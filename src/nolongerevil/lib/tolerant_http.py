"""Tolerate firmware-corrupted request lines at the HTTP-parse layer.

The v3 firmware corrupts a bucket's composite key with a ` ._sync`/` .$version`
fragment (see utils.bucket_key) and echoes the mutated key back in the request
*target* of its BigGet sync:

    GET /nest/transport/v3/device/device.<serial> ._sync HTTP/1.1

The raw space makes the request line four tokens instead of three, so aiohttp's
parser rejects it with 400 BadHttpMessage before any route or middleware runs.
The device's contact never reaches device_heartbeat, so it times out and goes
unavailable in Home Assistant.

The transport handler already canonicalizes the key (utils.bucket_key), but only
for requests that reach it. This module gets the request *to* the handler by
percent-encoding the injected spaces in the request target before the parser
sees them. aiohttp decodes %20 back to a space in request.path, so the existing
canonical_object_key handling resolves the real bucket unchanged.
"""

from aiohttp import web
from aiohttp.web_protocol import RequestHandler
from aiohttp.web_server import Server

_METHODS = (b"GET", b"POST", b"PUT", b"HEAD", b"DELETE", b"OPTIONS", b"PATCH")


def sanitize_request_line(data: bytes) -> bytes:
    """Percent-encode spaces injected into a corrupted request target.

    Only a chunk that begins a request whose request line carries extra spaces
    between the target and the HTTP version (the corruption signature) is
    rewritten; request bodies and well-formed lines pass through untouched. Only
    the first request line in a chunk is examined, and a request line split
    across TCP segments (no CRLF yet) is left alone — the firmware's tiny BigGet
    requests arrive whole and unpipelined in practice.
    """
    if not any(data.startswith(method + b" ") for method in _METHODS):
        return data

    eol = data.find(b"\r\n")
    if eol == -1:
        return data

    line, rest = data[:eol], data[eol:]
    parts = line.split(b" ")
    if len(parts) <= 3 or not parts[-1].startswith(b"HTTP/"):
        return data

    method, version = parts[0], parts[-1]
    target = b"%20".join(parts[1:-1])
    return method + b" " + target + b" " + version + rest


class _TolerantRequestHandler(RequestHandler):
    __slots__ = ()

    def data_received(self, data: bytes) -> None:
        super().data_received(sanitize_request_line(data))


class _TolerantServer(Server):
    def __call__(self) -> RequestHandler:
        # Mirrors aiohttp's Server.__call__: if RequestHandler rejects an unknown
        # kwarg, retry with only the args its signature has kept across versions.
        try:
            return _TolerantRequestHandler(self, loop=self._loop, **self._kwargs)
        except TypeError:
            kwargs = {
                k: v for k, v in self._kwargs.items() if k in ("debug", "access_log_class")
            }
            return _TolerantRequestHandler(self, loop=self._loop, **kwargs)


class TolerantAppRunner(web.AppRunner):
    """AppRunner whose connections tolerate firmware-corrupted request lines."""

    async def _make_server(self) -> Server:
        server = await super()._make_server()
        server.__class__ = _TolerantServer
        return server
