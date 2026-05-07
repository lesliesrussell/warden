#!/usr/bin/env python3
# warden-942
"""Example Python tool worker.

Boots against the Warden runtime via $WARDEN_SOCKET, registers two tools
(shell and read_file), and serves requests until it receives sys.shutdown.

Run via ForeignBridge or standalone for development:
    WARDEN_SOCKET=/tmp/warden.sock python worker.py
"""

import subprocess
import sys
import os

# Allow running from the repo root without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import warden


@warden.tool
def run_shell(ctx, msg):
    """Execute a shell command; body should be the command string."""
    command = str(msg.get("body", ""))
    ctx.decision("run shell command", context={"command": command})
    proc = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return {
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


@warden.tool
def read_file(ctx, msg):
    """Read a file from the local filesystem; body should be the path."""
    path = str(msg.get("body", ""))
    ctx.note(f"read_file: {path}")
    with open(path) as f:
        return f.read()


def main():
    ctx = warden.BeamCtx()
    ctx.note("python_worker: connected")

    warden.run_loop(ctx, {
        "req.shell": run_shell,
        "req.read_file": read_file,
    })


if __name__ == "__main__":
    main()
