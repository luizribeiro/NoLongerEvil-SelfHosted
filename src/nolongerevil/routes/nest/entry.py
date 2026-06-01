"""Nest entry endpoint - service discovery."""

from typing import Any

from aiohttp import web

from nolongerevil.config import settings
from nolongerevil.lib.logger import get_logger
from nolongerevil.lib.serial_parser import extract_serial_from_request
from nolongerevil.lib.types import DeviceObject
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService
from nolongerevil.utils.structure_assignment import derive_structure_id

logger = get_logger(__name__)


def _serialize_bucket(obj: DeviceObject) -> dict[str, Any]:
    return {
        "object_revision": obj.object_revision,
        "object_timestamp": obj.object_timestamp,
        "value": obj.value,
    }


async def _collect_inline_buckets(
    serial: str | None, app: web.Application
) -> dict[str, dict[str, Any]]:
    """Return top-level bucket-keyed entries to merge into the /entry response
    for a paired device, or {} if the device is unknown or storage isn't wired
    up.

    v3-firmware (Display-2.14 / firmware 4.3.3) gates its subscribe loop on
    the bucket-store population state (nlCZFetch::IsActive checks a flag at
    this+0x74 that the bucket-store layer sets when a bucket first arrives).
    Bucket bodies injected via PUT responses are rejected by the firmware
    ("nlCZUpdateParser: no bucket exists for payload"), but inline buckets
    in the bootstrap /entry response are accepted and populate the store,
    arming subscribe. Without this, subscribe-state stays "inactive" forever
    on v3 devices.

    v7 devices ignore the extra fields harmlessly.
    """
    if not serial:
        return {}
    storage: SQLModelService | None = app.get("storage")
    state_service: DeviceStateService | None = app.get("state_service")
    if not storage or not state_service:
        return {}
    owner = await storage.get_device_owner(serial)
    if not owner:
        return {}

    buckets: dict[str, dict[str, Any]] = {}
    structure_id = derive_structure_id(owner.user_id)
    for key in (f"user.{owner.user_id}", f"structure.{structure_id}"):
        obj = state_service.get_object(serial, key)
        if obj:
            buckets[key] = _serialize_bucket(obj)
    return buckets


async def handle_entry(request: web.Request) -> web.Response:
    """Handle Nest service discovery request.

    Returns URLs for all Nest services that the device needs to communicate with.

    Per spec, entry request may include:
    - reset: Reset reason (optional)
    - mac: MAC address (optional)
    - model: Device model (optional)
    - request_id: Request identifier (optional)
    - software_version: Firmware version (optional)

    Returns:
        JSON response with service URLs
    """
    serial = extract_serial_from_request(request)
    origin = settings.api_origin_with_port

    # Parse entry request fields (form-urlencoded per spec)
    entry_info = {}
    if request.content_type == "application/x-www-form-urlencoded":
        try:
            form_data = await request.post()
            entry_info = {
                "reset": form_data.get("reset"),
                "mac": form_data.get("mac"),
                "model": form_data.get("model"),
                "software_version": form_data.get("software_version"),
                "request_id": form_data.get("request_id"),
            }
            # Filter out None values for cleaner logging
            entry_info = {k: v for k, v in entry_info.items() if v is not None}
        except Exception as e:
            logger.debug(f"Could not parse entry form data: {e}")

    # Build service URLs
    response_data: dict[str, Any] = {
        "czfe_url": f"{origin}/nest/transport",
        "transport_url": f"{origin}/nest/transport",
        "direct_transport_url": f"{origin}/nest/transport",
        "passphrase_url": f"{origin}/nest/passphrase",
        "ping_url": f"{origin}/nest/transport",
        "pro_info_url": f"{origin}/nest/pro_info",
        "weather_url": f"{origin}/nest/weather/v1?query=",
        "upload_url": f"{origin}/nest/upload",
        "software_update_url": "",
        "server_version": "1.0.0",
        "tier_name": "local",
    }

    # Inline user/structure buckets for paired devices — required for v3
    # firmware to arm its subscribe loop. See _collect_inline_buckets.
    inline = await _collect_inline_buckets(serial, request.app)
    if inline:
        response_data.update(inline)
        logger.debug(
            f"Entry response for {serial} includes inline buckets: {list(inline)}"
        )

    if entry_info:
        logger.debug(f"Entry request from {serial or request.remote}: {entry_info}")
    else:
        logger.debug(f"Entry request from {serial or request.remote}")

    return web.json_response(response_data)


def create_entry_routes(app: web.Application) -> None:
    """Register entry routes.

    Args:
        app: aiohttp application
    """
    # Handle both GET and POST for /nest/entry - devices may use either
    app.router.add_get("/nest/entry", handle_entry)
    app.router.add_post("/nest/entry", handle_entry)
