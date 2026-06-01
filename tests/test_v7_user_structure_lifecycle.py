"""End-to-end check that suppressing the subscribe-side user/structure push
does not strand a non-v3 (v7) device.

The subscribe handler no longer pushes user.*/structure.* to a device that
didn't list those keys (see "don't push user/structure unless the client
listed them"). The replacement delivery path is the /entry inline buckets at
bootstrap. This test walks a v7 device through that lifecycle:

  1. /entry (v7-style request) -> must carry user.* and structure.* inline
  2. subscribe WITHOUT listing user.*/structure.* (v7 Format-1 style),
     not on the v3 path -> handler must NOT re-push them

so the bucket the device relies on still arrives, just from /entry instead of
the subscribe channel.
"""

import json
import time
from base64 import b64encode
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.lib.types import DeviceObject, DeviceOwner, UserInfo
from nolongerevil.routes.nest.entry import handle_entry
from nolongerevil.routes.nest.transport import handle_transport_subscribe
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService
from nolongerevil.services.subscription_manager import SubscriptionManager
from nolongerevil.utils.structure_assignment import derive_structure_id

SERIAL = "02AA01AB501203EQ"
OWNER = "test_user"
AUTH_HEADER = "Basic " + b64encode(f"{SERIAL}:password".encode()).decode()


@pytest.fixture(autouse=True)
def _fast_long_poll_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from nolongerevil.config import settings

    monkeypatch.setattr(type(settings), "connection_hold_timeout", property(lambda _self: 0.1))


async def _pair_with_buckets(
    state_service: DeviceStateService, sqlmodel_service: SQLModelService
) -> str:
    await sqlmodel_service.create_user(
        UserInfo(clerk_id=OWNER, email=f"{OWNER}@example.com", created_at=datetime.now())
    )
    await sqlmodel_service.set_device_owner(
        DeviceOwner(serial=SERIAL, user_id=OWNER, created_at=datetime.now())
    )
    structure_id = derive_structure_id(OWNER)
    now_ts = int(time.time() * 1000)
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"user.{OWNER}",
            object_revision=7,
            object_timestamp=now_ts,
            value={"name": OWNER},
            updated_at=datetime.now(),
        )
    )
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"structure.{structure_id}",
            object_revision=3,
            object_timestamp=now_ts,
            value={"name": "Home", "devices": [SERIAL]},
            updated_at=datetime.now(),
        )
    )
    return structure_id


def _entry_request(state_service: DeviceStateService, sqlmodel_service: SQLModelService) -> Mock:
    req = Mock(spec=web.Request)
    req.headers = {"X-nl-device-id": SERIAL}
    req.query = {}
    req.match_info = {}
    req.remote = "127.0.0.1"
    req.content_type = "application/octet-stream"
    req.post = AsyncMock(return_value={})
    req.app = {"state_service": state_service, "storage": sqlmodel_service}
    return req


def _subscribe_request(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
    body: dict,
) -> Mock:
    req = Mock(spec=web.Request)
    req.headers = {"Authorization": AUTH_HEADER}
    req.json = AsyncMock(return_value=body)
    req.app = {
        "state_service": state_service,
        "subscription_manager": subscription_manager,
        "storage": sqlmodel_service,
    }
    # v7-style: no version in match_info (real v7 hits /nest/transport or /v7/*)
    req.match_info = {}
    return req


async def _subscribe_immediate_keys(req: Mock) -> list[str]:
    written = bytearray()

    class StubResponse:
        def __init__(self, headers: dict | None = None) -> None:
            self.status = 200
            self.headers = dict(headers or {})

        async def prepare(self, _req: Mock) -> None:
            pass

        async def write(self, data: bytes) -> None:
            written.extend(data)

        async def write_eof(self) -> None:
            pass

    import nolongerevil.routes.nest.transport as transport_mod

    original = transport_mod.web.StreamResponse
    transport_mod.web.StreamResponse = lambda **kw: StubResponse(kw.get("headers"))
    try:
        await handle_transport_subscribe(req)
    finally:
        transport_mod.web.StreamResponse = original

    if not written:
        return []
    try:
        return [o["object_key"] for o in json.loads(written).get("objects", [])]
    except json.JSONDecodeError:
        return []


@pytest.mark.asyncio
async def test_v7_gets_user_structure_from_entry_not_subscribe(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
) -> None:
    structure_id = await _pair_with_buckets(state_service, sqlmodel_service)

    # 1. /entry must carry the buckets inline (the delivery path that replaces
    #    the suppressed subscribe push).
    resp = await handle_entry(_entry_request(state_service, sqlmodel_service))
    assert resp.status == 200
    entry_body = json.loads(resp.body)
    assert f"user.{OWNER}" in entry_body
    assert f"structure.{structure_id}" in entry_body

    # 2. v7-style subscribe that does NOT list user.*/structure.* must not
    #    re-push them on the subscribe channel.
    keys = await _subscribe_immediate_keys(
        _subscribe_request(
            state_service,
            sqlmodel_service,
            subscription_manager,
            body={
                "device": {"object_key": f"device.{SERIAL}", "object_timestamp": 0},
                "shared": {"object_key": f"shared.{SERIAL}", "object_timestamp": 0},
            },
        )
    )
    assert not any(k.startswith("user.") for k in keys), keys
    assert not any(k.startswith("structure.") for k in keys), keys
