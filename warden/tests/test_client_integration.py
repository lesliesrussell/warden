# warden-09k
"""Integration test: drive the real warden daemon over the control socket.

Gated — skips if zig-out/bin/warden or python3 is unavailable, so it stays fast
and hermetic. CI builds the daemon (zig build) before running this.
"""

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from warden.aio import Client, WardenUnavailable

_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
_DAEMON = os.path.join(_REPO_ROOT, "zig-out", "bin", "warden")
_WORKER = os.path.join(_REPO_ROOT, "examples", "live_demo", "math_worker.py")
_BLOCKING_WAIT_S = 0.3  # long enough to prove connect() is still retrying, short enough not to stall the suite


def _daemon_available() -> bool:
    return (
        os.path.exists(_DAEMON)
        and os.access(_DAEMON, os.X_OK)
        and shutil.which("python3") is not None
        and os.path.exists(_WORKER)
    )


def _wait_for_socket(path: str, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.02)
    return False


@unittest.skipUnless(_daemon_available(), "zig-out/bin/warden or python3 absent")
class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="warden-itest-")
        self._sock = os.path.join(self._tmp, "sockets", "ctrl.sock")
        self._log_dir = os.path.join(self._tmp, "logs")
        os.makedirs(self._log_dir, exist_ok=True)
        self._proc = None

    def tearDown(self):
        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGTERM)
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _start_daemon(self):
        env = dict(os.environ)
        env["WARDEN_CTRL_SOCKET"] = self._sock
        env["WARDEN_BEAM_ID"] = "1"
        env["WARDEN_LOG_DIR"] = self._log_dir
        self._proc = subprocess.Popen(
            [_DAEMON],
            env=env,
            cwd=_REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    async def test_spawn_call_list_roundtrip(self):
        self._start_daemon()
        self.assertTrue(
            _wait_for_socket(self._sock), "daemon never bound the control socket"
        )

        async with await Client.connect(self._sock, timeout=10) as client:
            # beam.create with the primary beam id returns the existing beam.
            beam_id = await client.beam_create(beam=1)
            self.assertIsInstance(beam_id, int)
            self.assertEqual(beam_id, 1)

            pid = await client.proc_spawn(
                [sys.executable, _WORKER],
                beam=beam_id,
                restart="permanent",
            )
            self.assertRegex(pid, r"^\d+/\d+$")

            reply = await client.proc_call(pid, "req.fib", 10, timeout_ms=5000)
            self.assertEqual(reply["body"], 55)  # fib(10) == 55

            procs = await client.proc_list(beam=beam_id)
            # proc_list returns {"beam": int, "pid": int, ...}; reconstruct compound pid.
            self.assertTrue(
                any(f"{p.get('beam')}/{p.get('pid')}" == pid for p in procs)
            )

    async def test_retry_connect_blocks_until_daemon_up(self):
        # Connect BEFORE the daemon exists; it must block, then succeed.
        connect_task = asyncio.create_task(Client.connect(self._sock, timeout=10))
        await asyncio.sleep(_BLOCKING_WAIT_S)  # still waiting, not errored
        self.assertFalse(connect_task.done())

        self._start_daemon()
        # let the in-flight connect carry the wait — don't block the loop with a poll
        client = await asyncio.wait_for(connect_task, timeout=10)
        try:
            beam_id = await client.beam_create(beam=1)
            self.assertIsInstance(beam_id, int)
        finally:
            await client.close()

    async def test_connect_timeout_raises_when_daemon_never_starts(self):
        with self.assertRaises(WardenUnavailable):
            await Client.connect(self._sock, timeout=0.5)


if __name__ == "__main__":
    unittest.main()
