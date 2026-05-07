#!/usr/bin/env python3
# warden-45x
"""
Hanging tool worker — demonstrates what happens in today's stacks vs. Warden.

TODAY'S STACKS (LangChain, CrewAI, raw asyncio):
  A tool calls an HTTP endpoint with no timeout.
  The event loop or thread pool blocks forever.
  The entire agent silently stalls — no recovery possible.

WITH WARDEN:
  1. This worker connects and receives task_1.
  2. It deliberately blocks on a socket.connect() with no timeout.
  3. Warden's watchdog detects no log activity past idle_timeout_ms.
  4. The policy engine quarantines this process's PID.
  5. The supervisor kills this worker and starts a fresh one.
  6. task_2 arrives at the new worker and completes normally.
  7. The session supervisor PID never changed.

Run this with a live ForeignBridge:
  WARDEN_SOCKET=/tmp/warden.sock python hanging_worker.py
"""

import socket
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import warden


@warden.tool
def fetch_data(ctx, msg):
    """Fetch data from an external API — with no timeout (the bug)."""
    url = str(msg.get("body", ""))
    ctx.note(f"fetch_data: connecting to {url}")

    # This is the bug: socket.connect() with no timeout on an unreachable host.
    # The thread blocks here indefinitely. No exception, no return.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("192.0.2.1", 80))   # RFC 5737 TEST-NET — unreachable by design
    return s.recv(4096).decode()    # never reached


@warden.tool
def echo(ctx, msg):
    """Simple echo — works fine, used by task_2 on the replacement worker."""
    return {"echoed": msg.get("body")}


def main():
    ctx = warden.BeamCtx()
    ctx.note("hanging_worker: connected — will hang on first fetch_data call")

    warden.run_loop(ctx, {
        "req.fetch": fetch_data,
        "req.echo": echo,
    })


if __name__ == "__main__":
    main()
