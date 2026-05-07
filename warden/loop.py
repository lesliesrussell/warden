# warden-942
from __future__ import annotations

import uuid
from typing import Callable

from .client import BeamCtx, _new_id

SHUTDOWN_TYPE = "sys.shutdown"


def run_loop(
    ctx: BeamCtx,
    handlers: dict[str, Callable],
    *,
    recv_timeout_ms: int = 1000,
) -> None:
    """Receive messages and dispatch to registered handlers by message type.

    Each handler has signature ``handler(ctx, msg) -> reply_body``.
    On success, sends a ``res.ok`` reply carrying the return value.
    On exception, sends a ``res.error`` reply with the error string — no crash.
    Exits cleanly when a ``sys.shutdown`` message arrives.

    Example::

        beam.run_loop(ctx, {
            "req.search": search_web,
            "req.read_file": read_file,
        })
    """
    ctx.note("run_loop: started")

    while True:
        msg = ctx.recv(timeout_ms=recv_timeout_ms)
        if msg is None:
            continue

        msg_type = msg.get("type", "")
        msg_id = msg.get("id", _new_id())
        reply_to = msg.get("reply_to") or msg.get("from", "")

        if msg_type == SHUTDOWN_TYPE:
            ctx.note("run_loop: received shutdown, exiting")
            break

        handler = handlers.get(msg_type)
        if handler is None:
            ctx.warn(f"run_loop: no handler for {msg_type!r}, ignoring")
            continue

        try:
            result = handler(ctx, msg)
            if reply_to:
                ctx.reply(reply_to, "res.ok", result, corr=msg_id)
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            ctx.warn(f"run_loop: handler {msg_type!r} raised: {error_msg}")
            if reply_to:
                ctx.reply(reply_to, "res.error", error_msg, corr=msg_id)

    ctx.note("run_loop: stopped")
