"""Tests for the v3-firmware BigGet reply shape at GET /nest/transport/v3/device/...

v3 firmware parses the BigGet reply with nlCZGetParser, which keys each entry
by a field literally named "key" and reads "$version"/"$timestamp" off it to
set the bucket's cloud version/timestamp. The v7 field names
object_key/object_revision/object_timestamp do not exist in the v3 binary, so
the v7 shape leaves the bucket's cloud timestamp at 0 and CompareVersions takes
the "forcing device update" branch — the subscribe gate never opens.

v7/legacy GET paths MUST keep the {"objects": [{object_revision, ...}]} shape.
"""

import json
import time
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.lib.types import DeviceObject
from nolongerevil.routes.nest.transport import handle_transport_get
from nolongerevil.services.device_state_service import DeviceStateService

SERIAL = "02AA01AB501203EQ"


def _make_get_request(state_service: DeviceStateService, path: str) -> Mock:
    req = Mock(spec=web.Request)
    req.headers = {}
    req.path = path
    req.match_info = {}
    req.app = {"state_service": state_service}
    return req


async def _seed_buckets(state_service: DeviceStateService) -> None:
    ts = int(time.time() * 1000)
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"device.{SERIAL}",
            object_revision=4,
            object_timestamp=ts,
            value={"current_temperature": 20.5},
            updated_at=datetime.now(),
        )
    )
    await state_service.upsert_object(
        DeviceObject(
            serial=SERIAL,
            object_key=f"shared.{SERIAL}",
            object_revision=2,
            object_timestamp=ts,
            value={"target_temperature": 21.5},
            updated_at=datetime.now(),
        )
    )


async def _get(state_service: DeviceStateService, path: str) -> dict:
    resp = await handle_transport_get(_make_get_request(state_service, path))
    assert resp.status == 200
    return json.loads(resp.body)


@pytest.mark.asyncio
async def test_v3_get_uses_bigget_key_dollar_fields(
    state_service: DeviceStateService,
) -> None:
    await _seed_buckets(state_service)
    body = await _get(state_service, f"/nest/transport/v3/device/device.{SERIAL}")

    entries = {e["key"]: e for e in body["objects"]}
    assert f"device.{SERIAL}" in entries
    assert f"shared.{SERIAL}" in entries

    dev = entries[f"device.{SERIAL}"]
    # The fields nlCZGetParser actually reads: "key", "$version", "$timestamp".
    assert dev["$version"] == 4
    assert dev["$timestamp"] > 0
    # The v7 names must NOT be the carriers on v3 — the firmware ignores them.
    assert "object_key" not in dev
    assert "object_revision" not in dev
    assert "object_timestamp" not in dev


@pytest.mark.asyncio
async def test_legacy_get_still_uses_object_key_shape(
    state_service: DeviceStateService,
) -> None:
    await _seed_buckets(state_service)
    body = await _get(state_service, f"/nest/transport/v7/device/device.{SERIAL}")

    obj = body["objects"][0]
    assert "object_key" in obj
    assert "object_revision" in obj
    assert "object_timestamp" in obj
    # v3 BigGet field names must not leak into the v7 shape.
    assert "key" not in obj
    assert "$version" not in obj
