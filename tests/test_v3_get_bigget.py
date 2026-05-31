"""Regression guard for the v3 device GET (BigGet) reply shape.

An earlier attempt served `{"objects":[{"key","$version","$timestamp"}]}` to v3
firmware on the theory that nlCZGetParser reads those field names. On real
hardware that shape faults the parser ("nlCZParser: parent object format
incorrect: 3 for $version" → "cannot find bucket under key $version") and
crash-loops nlclient. The GET reply must use the objects-array metadata shape
(object_revision/object_timestamp/object_key) for every firmware version.
"""

import json
import time
from datetime import datetime
from unittest.mock import Mock

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


async def _seed(state_service: DeviceStateService) -> None:
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


async def _get(state_service: DeviceStateService, path: str) -> dict:
    resp = await handle_transport_get(_make_get_request(state_service, path))
    assert resp.status == 200
    return json.loads(resp.body)


@pytest.mark.parametrize("version", ["v3", "v7"])
@pytest.mark.asyncio
async def test_get_uses_object_key_shape_never_dollar_fields(
    state_service: DeviceStateService, version: str
) -> None:
    await _seed(state_service)
    body = await _get(state_service, f"/nest/transport/{version}/device/device.{SERIAL}")

    obj = body["objects"][0]
    assert "object_key" in obj
    assert "object_revision" in obj
    assert "object_timestamp" in obj
    # The shapes that crash v3 nlclient must never be emitted.
    assert "$version" not in obj
    assert "$timestamp" not in obj
    assert "key" not in obj
