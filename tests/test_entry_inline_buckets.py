"""Tests for the /entry inline-bucket injection.

For paired devices, the /entry response must include the device's
user.<owner> and structure.<id> buckets inline (top-level keys with
{object_revision, object_timestamp, value}). v3-firmware devices need
them in the bootstrap response to arm their subscribe loop; v7 devices
ignore the extra fields harmlessly.

Unpaired devices and pre-storage-initialization requests must keep the
plain service-URL response intact.
"""

import json
import time
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp import web

from nolongerevil.lib.types import DeviceObject, DeviceOwner, UserInfo
from nolongerevil.routes.nest.entry import handle_entry
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService

SERIAL = "02AA01AB501203EQ"
OWNER = "test_user"

SERVICE_URL_KEYS = {
    "czfe_url",
    "transport_url",
    "direct_transport_url",
    "passphrase_url",
    "ping_url",
    "pro_info_url",
    "weather_url",
    "upload_url",
    "software_update_url",
    "server_version",
    "tier_name",
}


def _make_request(
    state_service: DeviceStateService | None,
    sqlmodel_service: SQLModelService | None,
    serial: str | None,
) -> Mock:
    req = Mock(spec=web.Request)
    req.headers = {"X-nl-device-id": serial} if serial else {}
    req.query = {}
    req.match_info = {}
    req.remote = "127.0.0.1"
    req.content_type = "application/octet-stream"
    req.post = AsyncMock(return_value={})
    app: dict = {}
    if state_service is not None:
        app["state_service"] = state_service
    if sqlmodel_service is not None:
        app["storage"] = sqlmodel_service
    req.app = app
    return req


async def _make_paired_device_with_buckets(
    state_service: DeviceStateService, sqlmodel_service: SQLModelService
) -> str:
    await sqlmodel_service.create_user(
        UserInfo(clerk_id=OWNER, email=f"{OWNER}@example.com", created_at=datetime.now())
    )
    await sqlmodel_service.set_device_owner(
        DeviceOwner(serial=SERIAL, user_id=OWNER, created_at=datetime.now())
    )

    from nolongerevil.utils.structure_assignment import derive_structure_id

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


async def _entry(req: Mock) -> dict:
    resp = await handle_entry(req)
    assert resp.status == 200
    return json.loads(resp.body)


@pytest.mark.asyncio
async def test_entry_includes_inline_buckets_for_paired_device(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
) -> None:
    structure_id = await _make_paired_device_with_buckets(state_service, sqlmodel_service)
    body = await _entry(_make_request(state_service, sqlmodel_service, SERIAL))

    assert f"user.{OWNER}" in body
    assert f"structure.{structure_id}" in body
    user_bucket = body[f"user.{OWNER}"]
    assert user_bucket["object_revision"] == 7
    assert user_bucket["value"] == {"name": OWNER}


@pytest.mark.asyncio
async def test_entry_service_urls_still_present_for_paired_device(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
) -> None:
    await _make_paired_device_with_buckets(state_service, sqlmodel_service)
    body = await _entry(_make_request(state_service, sqlmodel_service, SERIAL))
    assert SERVICE_URL_KEYS.issubset(body.keys())


@pytest.mark.asyncio
async def test_entry_for_unpaired_device_has_no_inline_buckets(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
) -> None:
    body = await _entry(_make_request(state_service, sqlmodel_service, SERIAL))
    assert set(body.keys()) == SERVICE_URL_KEYS


@pytest.mark.asyncio
async def test_entry_without_storage_keeps_original_shape(
    state_service: DeviceStateService,
) -> None:
    body = await _entry(_make_request(state_service, None, SERIAL))
    assert set(body.keys()) == SERVICE_URL_KEYS


@pytest.mark.asyncio
async def test_entry_without_serial_keeps_original_shape(
    state_service: DeviceStateService,
    sqlmodel_service: SQLModelService,
) -> None:
    body = await _entry(_make_request(state_service, sqlmodel_service, serial=None))
    assert set(body.keys()) == SERVICE_URL_KEYS
