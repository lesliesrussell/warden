# warden-09k
"""Async client for the Warden daemon's control plane.

Distinct from the worker SDK (``warden.BeamCtx`` / ``run_loop``), which speaks
the *bridge* protocol for processes already running on a beam. ``Client`` is a
client of the *control* socket (``WARDEN_CTRL_SOCKET``) — the same protocol
``wardenctl`` speaks — used to create beams and spawn/message supervised workers.
"""

from __future__ import annotations

import asyncio
import json
import os
import struct
import time
from typing import Any, Awaitable, Callable

_DEFAULT_SOCKET = "~/.warden/ctrl.sock"
_RETRY_INITIAL = 0.05
_RETRY_CAP = 2.0
_VALID_RESTART = ("permanent", "transient", "temporary")


class WardenError(Exception):
    """A request returned ``ok=false``; the message is the server's error string."""


class WardenUnavailable(Exception):
    """An explicit ``connect(timeout=...)`` elapsed before the daemon was reachable."""


def _encode_frame(obj: dict) -> bytes:
    body = json.dumps(obj).encode("utf-8")
    return struct.pack(">I", len(body)) + body


async def _read_frame(reader: asyncio.StreamReader) -> dict:
    header = await reader.readexactly(4)
    (length,) = struct.unpack(">I", header)
    body = await reader.readexactly(length)
    return json.loads(body.decode("utf-8"))


def _resolve_path(path: str | None) -> str:
    raw = path or os.environ.get("WARDEN_CTRL_SOCKET") or _DEFAULT_SOCKET
    return os.path.expanduser(raw)


# warden-09k
async def _connect_with_retry(
    connector: Callable[[], Awaitable[Any]],
    *,
    timeout: float | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> Any:
    """Call ``connector`` until it succeeds, backing off 50ms→2s between tries.

    Retries on the errors that mean "daemon not up yet" (``FileNotFoundError`` /
    ``ConnectionRefusedError``). Blocks forever by default; if ``timeout`` is set
    and the elapsed wait passes it, raises ``WardenUnavailable``. ``sleep`` and
    ``monotonic`` are injectable for deterministic tests.
    """
    sleep = sleep if sleep is not None else asyncio.sleep
    monotonic = monotonic if monotonic is not None else time.monotonic
    deadline = None if timeout is None else monotonic() + timeout
    delay = _RETRY_INITIAL
    while True:
        try:
            return await connector()
        except (FileNotFoundError, ConnectionRefusedError) as exc:
            if deadline is not None and monotonic() >= deadline:
                raise WardenUnavailable(
                    f"warden daemon not reachable within {timeout}s"
                ) from exc
            await sleep(delay)
            delay = min(delay * 2, _RETRY_CAP)


# warden-09k
class Client:
    """Async client of the Warden control socket. One connection, serialized
    requests. Use ``await Client.connect()`` to construct."""

    def __init__(self, path: str, *, _connector=None):
        self._path = path
        self._connector = _connector  # test seam: async (path) -> (reader, writer)
        self._reader: asyncio.StreamReader | None = None
        self._writer = None
        self._counter = 0
        self._connected = False
        self._lock = asyncio.Lock()

    @classmethod
    async def connect(
        cls, path: str | None = None, *, timeout: float | None = None, _connector=None
    ) -> "Client":
        self = cls(_resolve_path(path), _connector=_connector)
        await self._ensure_connected(timeout=timeout)
        return self

    async def _open(self):
        if self._connector is not None:
            return await self._connector(self._path)
        return await asyncio.open_unix_connection(self._path)

    async def _ensure_connected(self, *, timeout: float | None = None) -> None:
        if self._connected:
            return
        reader, writer = await _connect_with_retry(self._open, timeout=timeout)
        self._reader, self._writer, self._connected = reader, writer, True

    async def _request(self, action: str, payload: Any) -> Any:
        async with self._lock:
            await self._ensure_connected()
            self._counter += 1
            req_id = str(self._counter)
            frame = _encode_frame(
                {"req_id": req_id, "action": action, "payload": payload}
            )
            try:
                self._writer.write(frame)
                await self._writer.drain()
                resp = await _read_frame(self._reader)
            except (ConnectionError, asyncio.IncompleteReadError, OSError) as exc:
                self._connected = False
                raise ConnectionError(
                    f"warden connection lost during {action!r}"
                ) from exc
            if resp.get("req_id") != req_id:
                raise WardenError(
                    f"req_id mismatch: sent {req_id}, got {resp.get('req_id')!r}"
                )
            if not resp.get("ok"):
                raise WardenError(resp.get("error") or "unknown error")
            return resp.get("payload")

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except (ConnectionError, OSError):
                pass
        self._reader = self._writer = None
        self._connected = False

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()
