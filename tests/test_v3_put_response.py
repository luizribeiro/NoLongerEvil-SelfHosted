"""Tests for v3-firmware PUT response formatting at /nest/transport/v3/put.

v3-firmware devices expect:
1. A nested envelope `{"<type>": {"<serial>": {rev, ts, key, value?}}}`
   instead of the v7 `{"objects": [...]}` array.
2. The merged `value` echoed on changed buckets — the device parser drops
   the response otherwise.

v7 behavior at /v7/put (and unversioned /put) MUST be unchanged: no value
echo, {"objects": [...]} envelope. The existing tests in test_transport_put.py
already cover the v7 invariants; this file adds the v3 path and pins the
v7-regression guards from the same handler.
"""

import json
from base64 import b64encode
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.routes.nest.transport import handle_transport_put
from nolongerevil.services.device_state_service import DeviceStateService

SERIAL = "02AA01AB501203EQ"
AUTH_HEADER = "Basic " + b64encode(f"{SERIAL}:password".encode()).decode()


def _make_request(
    state_service: DeviceStateService,
    body: dict,
    version: str | None,
) -> Mock:
    req = Mock(spec=web.Request)
    req.headers = {"Authorization": AUTH_HEADER}
    req.json = AsyncMock(return_value=body)
    req.app = {"state_service": state_service}
    req.match_info = {"version": version} if version else {}
    return req


async def _put(
    state_service: DeviceStateService,
    body: dict,
    version: str | None,
) -> dict:
    req = _make_request(state_service, body, version)
    resp = await handle_transport_put(req)
    assert resp.status == 200
    return json.loads(resp.body)


# ---------------------------------------------------------------------------
# v3 envelope shape
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_v3_put_response_is_flat_envelope_keyed_by_bucket_id(
    state_service: DeviceStateService,
) -> None:
    """Top-level keys MUST be the full `<bucket_type>.<serial>` string, not
    nested under bucket-type. The device's bucket-store hashtable is keyed
    by that exact string; nlclient looks the bucket up via PL_HashTableLookup
    on the top-level response key. A nested envelope (top-level "shared"
    with inner "<serial>") makes that lookup miss → parser logs "no bucket
    exists for payload" → device deadlocks in PUT-retry."""
    body = await _put(
        state_service,
        {
            "shared": {SERIAL: {"target_temperature": 21.5}},
            "device": {SERIAL: {"away": False}},
        },
        version="v3",
    )
    assert "objects" not in body
    assert f"shared.{SERIAL}" in body
    assert f"device.{SERIAL}" in body
    # No bucket-type-only keys
    assert "shared" not in body
    assert "device" not in body


# ---------------------------------------------------------------------------
# v3 value echo on change
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_v3_put_response_contains_dollar_version_and_timestamp_substrings(
    state_service: DeviceStateService,
) -> None:
    """The firmware's bucket synchroniser (FUN_00069be8) does literal strstr()
    searches for `"$version":` and `"$timestamp":` substrings inside the
    per-bucket value object. They can be at ANY nesting depth — strstr is
    byte-scan, not JSON-aware. The current envelope nests them inside a
    `_sync` wrapper so the node-walker (FUN_00068068) doesn't see them as
    top-level keys (which would trigger the bucket-key-append corruption
    via FUN_0004f460). The strstr arm still finds them, so the subscribe
    gate still arms."""
    body = await _put(
        state_service,
        {"shared": {SERIAL: {"target_temperature": 21.5}}},
        version="v3",
    )
    inner = body[f"shared.{SERIAL}"]
    # The corruption-avoidance rule: $version/$timestamp must NOT be
    # top-level keys of the per-bucket value object. Test both halves:
    assert "$version" not in inner, (
        "$version as a top-level key triggers the firmware's bucket+0x2c "
        f"append corruption: {inner}"
    )
    assert "$timestamp" not in inner, inner
    # But the strstr arm needs to find the literal substrings somewhere
    # in the serialised per-bucket value JSON.
    serialised = json.dumps(inner)
    assert '"$version":' in serialised, serialised
    assert '"$timestamp":' in serialised, serialised


@pytest.mark.asyncio
async def test_v3_put_response_echoes_value_on_change(
    state_service: DeviceStateService,
) -> None:
    body = await _put(
        state_service,
        {"shared": {SERIAL: {"target_temperature": 21.5}}},
        version="v3",
    )
    inner = body[f"shared.{SERIAL}"]
    assert "value" in inner
    assert inner["value"]["target_temperature"] == 21.5


@pytest.mark.asyncio
async def test_v3_put_response_omits_value_on_duplicate_put(
    state_service: DeviceStateService,
) -> None:
    """If the same value is PUT twice, the second response has no `value`
    field (values_changed=False), even on v3."""
    first = await _put(
        state_service,
        {"shared": {SERIAL: {"target_temperature": 21.5}}},
        version="v3",
    )
    assert "value" in first[f"shared.{SERIAL}"]

    second = await _put(
        state_service,
        {"shared": {SERIAL: {"target_temperature": 21.5}}},
        version="v3",
    )
    assert "value" not in second[f"shared.{SERIAL}"]


# ---------------------------------------------------------------------------
# v7 regression guards
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_v7_put_response_still_uses_objects_array(
    state_service: DeviceStateService,
) -> None:
    body = await _put(
        state_service,
        {"objects": [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 21.5}}]},
        version="v7",
    )
    assert "objects" in body
    assert "shared" not in body  # not nested


@pytest.mark.asyncio
async def test_v7_put_response_still_omits_value(
    state_service: DeviceStateService,
) -> None:
    body = await _put(
        state_service,
        {"objects": [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 21.5}}]},
        version="v7",
    )
    for obj in body["objects"]:
        assert "value" not in obj


@pytest.mark.asyncio
async def test_unversioned_put_response_still_uses_objects_array(
    state_service: DeviceStateService,
) -> None:
    """The /nest/transport/put route has no {version} match_info; it must
    keep the v7 envelope."""
    body = await _put(
        state_service,
        {"objects": [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 21.5}}]},
        version=None,
    )
    assert "objects" in body
    assert "shared" not in body
