"""Tests for the v3 SKV-header push on data-carrying subscribe responses.

v3 firmware reads a pushed object's identity from X-nl-skv-key/-version/
-timestamp response headers and applies the body as the bucket's raw value
(see notes/subscribe-push-framing.md). A {"objects":[...]} body with no SKV
headers is rejected by the device ("bad header data"). v7 keeps the
objects-array body.
"""

import json
import time
from base64 import b64encode
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.lib.types import DeviceObject
from nolongerevil.routes.nest.transport import handle_transport_subscribe
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService
from nolongerevil.services.subscription_manager import SubscriptionManager

SERIAL = "02AA01AB501203EQ"
AUTH_HEADER = "Basic " + b64encode(f"{SERIAL}:password".encode()).decode()


@pytest.fixture(autouse=True)
def _fast_long_poll_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from nolongerevil.config import settings
    from nolongerevil.routes.nest import transport

    monkeypatch.setattr(type(settings), "connection_hold_timeout", property(lambda _self: 0.1))
    # _v3_pushed is a module-level session tracker; reset it so each test sees
    # a clean "first contact" and isn't affected by other tests' ordering.
    transport._v3_pushed.clear()


async def _seed(state_service: DeviceStateService) -> DeviceObject:
    obj = DeviceObject(
        serial=SERIAL,
        object_key=f"shared.{SERIAL}",
        object_revision=5,
        object_timestamp=int(time.time() * 1000),
        value={"target_temperature": 21.5},
        updated_at=datetime.now(),
    )
    await state_service.upsert_object(obj)
    return obj


async def _subscribe(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
    body: dict,
    version: str | None,
) -> tuple[dict, bytes]:
    """Run the subscribe handler, return (response_headers, body_bytes)."""
    req = Mock(spec=web.Request)
    req.headers = {"Authorization": AUTH_HEADER}
    req.json = AsyncMock(return_value=body)
    req.app = {
        "state_service": state_service,
        "subscription_manager": subscription_manager,
        "storage": sqlmodel_service,
    }
    req.match_info = {"version": version} if version else {}

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
        resp = await handle_transport_subscribe(req)
    finally:
        transport_mod.web.StreamResponse = original
    return resp.headers, bytes(written)


@pytest.mark.asyncio
async def test_v3_push_uses_skv_headers_and_raw_value_body(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
) -> None:
    obj = await _seed(state_service)
    # v3 keys-only subscribe = first contact for the key => server pushes it.
    headers, body = await _subscribe(
        state_service,
        sqlmodel_service,
        subscription_manager,
        body={"keys": [{"key": f"shared.{SERIAL}"}]},
        version="v3",
    )

    assert headers.get("X-nl-skv-key") == f"shared.{SERIAL}"
    assert headers.get("X-nl-skv-version") == str(obj.object_revision)
    assert headers.get("X-nl-skv-timestamp") == str(obj.object_timestamp)
    # Body is the bare value, NOT a {"objects":[...]} envelope.
    assert json.loads(body) == {"target_temperature": 21.5}
    assert b"objects" not in body


@pytest.mark.asyncio
async def test_v3_held_notify_tickles_without_objects_body(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
) -> None:
    """When data arrives mid-hold on v3, the handler can't add SKV headers
    (already sent at prepare), so it must tickle-close (no body) to trigger an
    SKV re-push on resubscribe — NOT stream a {"objects":[...]} body."""
    obj = await _seed(state_service)
    # Queue a pending push so the held connection is notified immediately.
    await subscription_manager.store_pending_push(
        SERIAL, [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 22.0}}]
    )
    # Subscribe "synced" (client ts == server ts) so there's no immediate push
    # and we reach the hold path, where the queued notify then fires.
    headers, body = await _subscribe(
        state_service,
        sqlmodel_service,
        subscription_manager,
        body={
            "chunked": True,
            "objects": [{"object_key": f"shared.{SERIAL}", "object_timestamp": obj.object_timestamp}],
        },
        version="v3",
    )
    assert b"objects" not in body
    assert b"target_temperature" not in body  # the queued data is NOT streamed inline


@pytest.mark.asyncio
async def test_v7_push_keeps_objects_array_and_no_skv_headers(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager: SubscriptionManager,
) -> None:
    await _seed(state_service)
    headers, body = await _subscribe(
        state_service,
        sqlmodel_service,
        subscription_manager,
        body={"chunked": True, "objects": [{"object_key": f"shared.{SERIAL}", "object_timestamp": 0}]},
        version="v7",
    )

    assert "X-nl-skv-key" not in headers
    payload = json.loads(body)
    assert "objects" in payload
