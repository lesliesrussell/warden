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
    raise NotImplementedError  # implemented in Task 2


class Client:  # implemented in Task 3
    pass
