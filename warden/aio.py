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
        self._writer: asyncio.StreamWriter | None = None
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
        # warden-09k: close any stale connection from a prior request before
        # reconnecting (one-shot protocol reconnects every request).
        # we don't await wait_closed() here — the daemon already closed its end
        # (one-shot protocol), and blocking the reconnect hot path on FIN is
        # needless; Client.close() awaits wait_closed() for the final teardown.
        if self._writer is not None:
            try:
                self._writer.close()
            except (ConnectionError, OSError):
                pass
        reader, writer = await _connect_with_retry(self._open, timeout=timeout)
        self._reader, self._writer, self._connected = reader, writer, True

    async def _request(self, action: str, payload: Any) -> Any:
        async with self._lock:
            await self._ensure_connected()
            # warden-4sx: capture the connection locally so a concurrent close()
            # that nulls self._writer/_reader can't turn an in-flight write/read
            # into an AttributeError. The request operates on its captured handles;
            # a severed connection surfaces as a clean ConnectionError below.
            writer = self._writer
            reader = self._reader
            if writer is None or reader is None:
                self._connected = False
                raise ConnectionError(f"warden client closed during {action!r}")
            self._counter += 1
            req_id = str(self._counter)
            frame = _encode_frame(
                {"req_id": req_id, "action": action, "payload": payload}
            )
            try:
                writer.write(frame)
                await writer.drain()
                resp = await _read_frame(reader)
            except (ConnectionError, asyncio.IncompleteReadError, OSError) as exc:
                self._connected = False
                raise ConnectionError(
                    f"warden connection lost during {action!r}"
                ) from exc
            # warden-09k: daemon closes the connection after each response;
            # mark disconnected so the next _request reconnects transparently.
            self._connected = False
            if resp.get("req_id") != req_id:
                raise WardenError(
                    f"req_id mismatch: sent {req_id}, got {resp.get('req_id')!r}"
                )
            if not resp.get("ok"):
                raise WardenError(resp.get("error") or "unknown error")
            return resp.get("payload")

    # warden-09k
    @staticmethod
    def _field(result, key: str, action: str):
        if not isinstance(result, dict) or key not in result:
            raise WardenError(
                f"{action}: malformed response, missing {key!r}: {result!r}"
            )
        return result[key]

    async def beam_create(self, beam: int | None = None) -> int:
        payload = {} if beam is None else {"beam": beam}
        result = await self._request("beam.create", payload)
        return self._field(result, "beam_id", "beam.create")

    async def proc_spawn(
        self,
        cmd: "str | list[str]",
        *,
        beam: int | None = None,
        parent: str | None = None,
        restart: str | None = None,
    ) -> str:
        if restart is not None and restart not in _VALID_RESTART:
            raise ValueError(
                f"invalid restart policy {restart!r}; "
                f"expected one of {_VALID_RESTART}"
            )
        cmd_list = [cmd] if isinstance(cmd, str) else list(cmd)
        payload: dict[str, Any] = {"cmd": cmd_list}
        if beam is not None:
            payload["beam"] = beam
        if parent is not None:
            payload["parent"] = parent
        if restart is not None:
            payload["restart"] = restart
        result = await self._request("proc.spawn", payload)
        return self._field(result, "pid", "proc.spawn")

    async def proc_send(self, pid: str, msg_type: str, body: Any) -> None:
        await self._request(
            "proc.send", {"pid": pid, "type": msg_type, "body": body}
        )
        return None

    async def proc_call(
        self, pid: str, msg_type: str, body: Any, *, timeout_ms: int = 5000
    ) -> dict:
        return await self._request(
            "proc.call",
            {"pid": pid, "type": msg_type, "body": body, "timeout_ms": timeout_ms},
        )

    async def proc_list(self, beam: int | None = None) -> list:
        payload = {} if beam is None else {"beam": beam}
        result = await self._request("proc.list", payload)
        return self._field(result, "processes", "proc.list")

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
