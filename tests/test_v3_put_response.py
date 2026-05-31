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
# v3 PUT response is a parse-safe no-op
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_v3_put_response_is_empty_no_op(
    state_service: DeviceStateService,
) -> None:
    """Any bucket-keyed v3 PUT response with child fields ($version/$timestamp/
    _sync/value) corrupts the device's composite key (parser appends a fragment
    → next BigGet GET URL gains a space → HTTP 400 → device backs off). The PUT
    must return a parse-safe no-op; version/timestamp sync over the BigGet path
    and the subscribe gate stays open via CompareVersions."""
    body = await _put(
        state_service,
        {
            "shared": {SERIAL: {"target_temperature": 21.5}},
            "device": {SERIAL: {"away": False}},
        },
        version="v3",
    )
    assert body == {}
    # None of the key-corrupting shapes may appear.
    serialised = json.dumps(body)
    assert "$version" not in serialised
    assert "_sync" not in serialised
    assert f"shared.{SERIAL}" not in serialised


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
