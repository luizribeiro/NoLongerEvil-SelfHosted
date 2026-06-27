"""Tests for tolerance of firmware-corrupted request lines.

The v3 firmware injects raw spaces into the request *target* of its BigGet sync
(`GET /…/device.SERIAL ._sync HTTP/1.1`), which aiohttp's parser rejects with
400 before routing. sanitize_request_line percent-encodes those spaces so the
request parses and reaches the handler with the space restored in request.path.

The socket tests below verify the end-to-end contract directly — stock aiohttp
400s, TolerantAppRunner serves 200 — without relying on any route handler.
"""

import asyncio

import pytest
from aiohttp import web

from nolongerevil.lib.tolerant_http import TolerantAppRunner, sanitize_request_line

SERIAL = "02AA01AB501203EQ"

CORRUPTED_REQUEST = (
    f"GET /nest/transport/v3/device/device.{SERIAL} ._sync HTTP/1.1\r\n"
    "Host: 127.0.0.1\r\nConnection: close\r\n\r\n"
).encode()


# ---------------------------------------------------------------------------
# pure-function unit tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        pytest.param(
            f"GET /nest/transport/v3/device/device.{SERIAL} ._sync HTTP/1.1\r\n".encode(),
            f"GET /nest/transport/v3/device/device.{SERIAL}%20._sync HTTP/1.1\r\n".encode(),
            id="corrupted_bigget_single_space",
        ),
        pytest.param(
            f"GET /d/device.{SERIAL} .$version .$version HTTP/1.1\r\n".encode(),
            f"GET /d/device.{SERIAL}%20.$version%20.$version HTTP/1.1\r\n".encode(),
            id="corrupted_multiple_fragments",
        ),
        pytest.param(
            b"GET /nest/transport/v3/device/device.X HTTP/1.1\r\n",
            b"GET /nest/transport/v3/device/device.X HTTP/1.1\r\n",
            id="well_formed_get_untouched",
        ),
        pytest.param(
            b"POST /nest/upload HTTP/1.1\r\n",
            b"POST /nest/upload HTTP/1.1\r\n",
            id="well_formed_post_untouched",
        ),
        pytest.param(
            b'{"objects": [{"object_key": "device.X ._sync"}]}',
            b'{"objects": [{"object_key": "device.X ._sync"}]}',
            id="body_chunk_untouched",
        ),
    ],
)
def test_sanitize_request_line(raw: bytes, expected: bytes) -> None:
    assert sanitize_request_line(raw) == expected


def test_sanitize_preserves_headers_and_body() -> None:
    out = sanitize_request_line(CORRUPTED_REQUEST)
    assert out.endswith(b"\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    assert f"device.{SERIAL}%20._sync".encode() in out


def test_sanitize_leaves_method_prefixed_non_request_alone() -> None:
    # starts with "GET " but is not a request line (no HTTP/ version token)
    assert sanitize_request_line(b"GET well actually\r\nmore") == b"GET well actually\r\nmore"


def test_sanitize_leaves_split_request_line_alone() -> None:
    # request line not yet terminated by CRLF in this chunk: left as-is
    chunk = f"GET /d/device.{SERIAL} ._sync HTTP/1.1".encode()
    assert sanitize_request_line(chunk) == chunk


# ---------------------------------------------------------------------------
# end-to-end socket tests (no route handler narrative trusted)
# ---------------------------------------------------------------------------


async def _send_raw(runner: web.BaseRunner, payload: bytes) -> bytes:
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = runner.addresses[0][1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(payload)
        await writer.drain()
        data = await reader.read()
        writer.close()
        await writer.wait_closed()
        return data
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_stock_runner_rejects_corrupted_request_line() -> None:
    """Reproduces the bug: default aiohttp 400s the corrupted line pre-routing."""
    async def handler(_request: web.Request) -> web.Response:
        return web.Response()

    app = web.Application()
    app.router.add_route("GET", "/nest/transport/v3/device/{tail:.*}", handler)
    resp = await _send_raw(web.AppRunner(app), CORRUPTED_REQUEST)
    assert b"400" in resp.split(b"\r\n", 1)[0], resp[:200]


@pytest.mark.asyncio
async def test_tolerant_runner_serves_corrupted_request_line() -> None:
    """The fix: TolerantAppRunner parses the line and the handler sees the
    space-restored path (so canonical_object_key can resolve the real bucket)."""
    seen: dict[str, str] = {}

    async def handler(request: web.Request) -> web.Response:
        seen["path"] = request.path
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_route("GET", "/nest/transport/v3/device/{tail:.*}", handler)

    resp = await _send_raw(TolerantAppRunner(app), CORRUPTED_REQUEST)

    assert b"200" in resp.split(b"\r\n", 1)[0], resp[:200]
    assert seen["path"] == f"/nest/transport/v3/device/device.{SERIAL} ._sync"
