"""Tests for v3-firmware subscribe behavior around auto-injected
user/structure buckets.

v3 devices subscribe with `{"keys": [{"key": "device.SERIAL"},
{"key": "shared.SERIAL"}]}` and never list user.* or structure.*.
The subscribe handler's user/structure auto-include block used to push
those buckets every cycle (client_ts=0 < server_ts), closing the
long-poll immediately and producing a busy reconnect loop.

Init delivery now happens via /entry inline buckets at bootstrap, so
the subscribe handler can safely skip the auto-push entirely when the
client didn't list those keys.
"""

import time
from base64 import b64encode
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.lib.types import DeviceObject, DeviceOwner, UserInfo
from nolongerevil.routes.nest.transport import handle_transport_subscribe
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService
from nolongerevil.services.subscription_manager import SubscriptionManager

SERIAL = "02AA01AB501203EQ"
AUTH_HEADER = "Basic " + b64encode(f"{SERIAL}:password".encode()).decode()
OWNER = "test_user"


@pytest.fixture(autouse=True)
def _fast_long_poll_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """The handler waits up to settings.connection_hold_timeout (~290s by
    default) on each held subscribe. Shrink so tests don't add minutes to
    the suite. connection_hold_timeout is a property, so patch it on the
    class with a constant-returning property."""
    from nolongerevil.config import settings

    monkeypatch.setattr(
        type(settings),
        "connection_hold_timeout",
        property(lambda _self: 0.1),
    )


def _make_request(
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
    req.match_info = {}
    return req


async def _make_paired_device(sqlmodel_service: SQLModelService) -> None:
    await sqlmodel_service.create_user(
        UserInfo(clerk_id=OWNER, email=f"{OWNER}@example.com", created_at=datetime.now())
    )
    await sqlmodel_service.set_device_owner(
        DeviceOwner(serial=SERIAL, user_id=OWNER, created_at=datetime.now())
    )


async def _seed_user_and_structure_buckets(state_service: DeviceStateService) -> None:
    now_ts = int(time.time() * 1000)
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"user.{OWNER}",
            object_revision=1,
            object_timestamp=now_ts,
            value={"name": OWNER},
            updated_at=datetime.now(),
        )
    )
    from nolongerevil.utils.structure_assignment import derive_structure_id

    structure_id = derive_structure_id(OWNER)
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"structure.{structure_id}",
            object_revision=1,
            object_timestamp=now_ts,
            value={"name": "Home", "devices": [SERIAL]},
            updated_at=datetime.now(),
        )
    )


async def _immediate_response_keys(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
    body: dict,
) -> list[str]:
    """Run handle_transport_subscribe and return object_keys in the immediate
    body, or [] if the handler held the long-poll (no body written)."""
    req = _make_request(state_service, sqlmodel_service, subscription_manager, body)
    written = bytearray()

    class StubResponse:
        def __init__(self) -> None:
            self.status = 200
            self.headers: dict[str, str] = {}

        async def prepare(self, _req: Mock) -> None:
            pass

        async def write(self, data: bytes) -> None:
            written.extend(data)

        async def write_eof(self) -> None:
            pass

    import nolongerevil.routes.nest.transport as transport_mod

    original = transport_mod.web.StreamResponse
    transport_mod.web.StreamResponse = lambda **_kw: StubResponse()  # type: ignore[assignment]
    try:
        await handle_transport_subscribe(req)
    finally:
        transport_mod.web.StreamResponse = original

    if not written:
        return []
    import json

    try:
        return [obj["object_key"] for obj in json.loads(written).get("objects", [])]
    except json.JSONDecodeError:
        return []


@pytest.mark.asyncio
async def test_v3_subscribe_does_not_push_user_or_structure(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
) -> None:
    """v3 subscribe lists only device.* and shared.* — the handler must
    NOT push user/structure to bootstrap pairing on this channel. /entry
    inline buckets handle that earlier in the lifecycle."""
    await _make_paired_device(sqlmodel_service)
    await _seed_user_and_structure_buckets(state_service)

    keys = await _immediate_response_keys(
        state_service,
        sqlmodel_service,
        subscription_manager,
        body={"keys": [{"key": f"device.{SERIAL}"}, {"key": f"shared.{SERIAL}"}]},
    )

    assert not any(k.startswith("user.") for k in keys), keys
    assert not any(k.startswith("structure.") for k in keys), keys
