"""Tests for tolerance of firmware-corrupted bucket keys.

The v3 firmware appends ` .$version`/` ._sync` fragments to a bucket's
composite key and echoes the mutated key back in every request. The server
canonicalizes inbound keys (so corrupted requests resolve to the real bucket
and no phantom buckets get stored) and purges any existing phantom rows on
cache load.
"""

import json
import time
from base64 import b64encode
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.lib.types import DeviceObject
from nolongerevil.routes.nest.transport import handle_transport_get, handle_transport_put
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService
from nolongerevil.utils.bucket_key import canonical_object_key, is_corrupted_key

SERIAL = "02AA01AB501203EQ"
AUTH_HEADER = "Basic " + b64encode(f"{SERIAL}:password".encode()).decode()


# ---------------------------------------------------------------------------
# helper unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        (f"device.{SERIAL}", f"device.{SERIAL}"),
        (f"device.{SERIAL} ._sync", f"device.{SERIAL}"),
        (f"device.{SERIAL} .$version", f"device.{SERIAL}"),
        (f"device.{SERIAL} .$version .$version .$version", f"device.{SERIAL}"),
        ("", ""),
    ],
)
def test_canonical_object_key(raw: str, expected: str) -> None:
    assert canonical_object_key(raw) == expected


def test_is_corrupted_key() -> None:
    assert not is_corrupted_key(f"device.{SERIAL}")
    assert is_corrupted_key(f"device.{SERIAL} ._sync")
    assert is_corrupted_key(f"device.{SERIAL} .$version")


# ---------------------------------------------------------------------------
# inbound normalization: PUT
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_with_corrupted_key_stores_under_canonical(
    state_service: DeviceStateService,
) -> None:
    """A PUT carrying a corrupted object_key must update the canonical bucket,
    never create a phantom `… ._sync` bucket."""
    req = Mock(spec=web.Request)
    req.headers = {"Authorization": AUTH_HEADER}
    req.json = AsyncMock(
        return_value={
            "objects": [
                {
                    "object_key": f"shared.{SERIAL} ._sync",
                    "value": {"target_temperature": 21.5},
                }
            ]
        }
    )
    req.app = {"state_service": state_service}
    req.match_info = {"version": "v3"}

    resp = await handle_transport_put(req)
    assert resp.status == 200

    assert state_service.get_object(SERIAL, f"shared.{SERIAL}") is not None
    assert state_service.get_object(SERIAL, f"shared.{SERIAL} ._sync") is None
    # No phantom bucket leaked into storage.
    assert all(not is_corrupted_key(o.object_key) for o in state_service.get_objects_by_serial(SERIAL))


@pytest.mark.asyncio
async def test_put_v3_nested_corrupted_inner_serial_stores_under_canonical(
    state_service: DeviceStateService,
) -> None:
    """Format 3 reconstructs object_key as `<type>.<serial>`; a corrupted inner
    serial yields a key with a space, which canonicalization must still strip."""
    req = Mock(spec=web.Request)
    req.headers = {"Authorization": AUTH_HEADER}
    req.json = AsyncMock(
        return_value={"shared": {f"{SERIAL} ._sync": {"target_temperature": 20.0}}}
    )
    req.app = {"state_service": state_service}
    req.match_info = {"version": "v3"}

    resp = await handle_transport_put(req)
    assert resp.status == 200
    assert state_service.get_object(SERIAL, f"shared.{SERIAL}") is not None
    assert all(not is_corrupted_key(o.object_key) for o in state_service.get_objects_by_serial(SERIAL))


# ---------------------------------------------------------------------------
# inbound normalization: subscribe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_with_corrupted_key_resolves_real_bucket(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
    subscription_manager,
) -> None:
    """A v3 subscribe carrying a corrupted key must resolve to the canonical
    bucket: the first-contact push echoes the real `shared.SERIAL`, not the
    corrupted variant."""
    from nolongerevil.routes.nest.transport import handle_transport_subscribe

    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"shared.{SERIAL}",
            object_revision=2,
            object_timestamp=int(time.time() * 1000),
            value={"target_temperature": 21.5},
            updated_at=datetime.now(),
        )
    )

    req = Mock(spec=web.Request)
    req.headers = {"Authorization": AUTH_HEADER}
    req.json = AsyncMock(return_value={"keys": [{"key": f"shared.{SERIAL} ._sync"}]})
    req.app = {
        "state_service": state_service,
        "subscription_manager": subscription_manager,
        "storage": sqlmodel_service,
    }
    req.match_info = {}

    written = bytearray()

    class StubResponse:
        status = 200
        headers: dict = {}

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

    keys = [o["object_key"] for o in json.loads(written).get("objects", [])]
    assert f"shared.{SERIAL}" in keys
    assert all(not is_corrupted_key(k) for k in keys)


# ---------------------------------------------------------------------------
# inbound normalization: GET (BigGet)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_with_corrupted_path_resolves_to_real_device(
    state_service: DeviceStateService,
) -> None:
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"device.{SERIAL}",
            object_revision=4,
            object_timestamp=int(time.time() * 1000),
            value={"current_temperature": 20.5},
            updated_at=datetime.now(),
        )
    )
    req = Mock(spec=web.Request)
    req.headers = {}
    req.path = f"/nest/transport/v3/device/device.{SERIAL} ._sync"
    req.match_info = {}
    req.app = {"state_service": state_service}

    resp = await handle_transport_get(req)
    assert resp.status == 200
    body = json.loads(resp.body)
    keys = [o["key"] for o in body["objects"]]
    assert f"device.{SERIAL}" in keys


# ---------------------------------------------------------------------------
# self-healing purge on cache load
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_cache_purges_existing_corrupted_buckets(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
) -> None:
    """Phantom buckets persisted by older builds are dropped on the next load."""
    now = int(time.time() * 1000)
    for key in (f"device.{SERIAL}", f"device.{SERIAL} ._sync", f"device.{SERIAL} .$version .$version"):
        await state_service.upsert_object(
            DeviceObject(
                serial=SERIAL,
                object_key=key,
                object_revision=1,
                object_timestamp=now,
                value={"x": 1},
                updated_at=datetime.now(),
            )
        )

    reloaded = DeviceStateService(sqlmodel_service)
    await reloaded.initialize()
    try:
        assert reloaded.get_object(SERIAL, f"device.{SERIAL}") is not None
        assert reloaded.get_object(SERIAL, f"device.{SERIAL} ._sync") is None
        remaining = await sqlmodel_service.get_all_objects()
        assert remaining, "canonical bucket must survive"
        assert all(not is_corrupted_key(o.object_key) for o in remaining)
    finally:
        await reloaded.close()
