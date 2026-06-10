# Warden Python Client SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an async `warden.Client` that drives the Warden daemon's control plane over its Unix socket, with a resilient retry-connect contract and wrappers for the management verbs.

**Architecture:** A new `warden/aio.py` module exposes `Client` (an `asyncio`-based client of the *control* socket — distinct from the existing `BeamCtx` worker SDK which speaks the *bridge* protocol). The client frames requests as a 4-byte big-endian length prefix + UTF-8 JSON, serializes requests behind an `asyncio.Lock`, and reconnects lazily after a daemon bounce. Connection establishment uses an exponential-backoff retry loop (50ms→2s cap) that blocks indefinitely by default.

**Tech Stack:** Python ≥ 3.11, stdlib only (`asyncio`, `json`, `struct`, `os`, `time`). Tests use stdlib `unittest.IsolatedAsyncioTestCase` (the existing `warden/tests/test_sdk.py` uses `unittest`, and `pytest-asyncio` is not installed — this is a deliberate, dependency-free deviation from the spec's pytest assumption).

**Spec:** `docs/superpowers/specs/2026-06-09-warden-client-sdks-design.md` (Python portion only; TypeScript is warden-3yd, a separate plan).

**Bead:** warden-09k. Every new contiguous code block carries a `# warden-09k` comment.

---

## File Structure

- `warden/aio.py` — **NEW.** The async `Client`, framing helpers (`_encode_frame`, `_read_frame`), path resolution (`_resolve_path`), the retry-connect helper (`_connect_with_retry`), and exceptions (`WardenError`, `WardenUnavailable`). One module, one responsibility: the control-plane client.
- `warden/__init__.py` — **MODIFY.** Re-export `Client`, `WardenError`, `WardenUnavailable`.
- `warden/tests/test_client_unit.py` — **NEW.** Framing round-trip, retry schedule, error mapping, validation — all in-process, no daemon.
- `warden/tests/test_client_integration.py` — **NEW.** Drives the real `zig-out/bin/warden` daemon; gated (skips if the binary or `python3` is absent).

The existing `warden/client.py`, `warden/decorators.py`, `warden/loop.py` (the worker SDK) are **untouched**.

---

## Task 1: Framing helpers and exceptions

**Files:**
- Create: `warden/aio.py`
- Test: `warden/tests/test_client_unit.py`

- [ ] **Step 1: Write the failing test**

Create `warden/tests/test_client_unit.py`:

```python
# warden-09k
"""Unit tests for warden.aio (no live daemon required)."""

import asyncio
import json
import struct
import unittest

from warden.aio import (
    Client,
    WardenError,
    WardenUnavailable,
    _connect_with_retry,
    _encode_frame,
    _read_frame,
    _resolve_path,
)


class FramingTests(unittest.IsolatedAsyncioTestCase):
    async def test_encode_frame_is_length_prefixed_json(self):
        obj = {"req_id": "1", "action": "beam.create", "payload": {}}
        frame = _encode_frame(obj)
        length = struct.unpack(">I", frame[:4])[0]
        self.assertEqual(length, len(frame) - 4)
        self.assertEqual(json.loads(frame[4:].decode("utf-8")), obj)

    async def test_read_frame_roundtrip(self):
        obj = {"req_id": "7", "ok": True, "error": None, "payload": {"beam_id": 2}}
        reader = asyncio.StreamReader()
        reader.feed_data(_encode_frame(obj))
        reader.feed_eof()
        self.assertEqual(await _read_frame(reader), obj)

    async def test_read_frame_accumulates_split_reads(self):
        # multi-byte UTF-8 body delivered in two chunks across the length boundary
        obj = {"msg": "café — ümlaut"}
        frame = _encode_frame(obj)
        reader = asyncio.StreamReader()
        reader.feed_data(frame[:3])      # partial length prefix
        reader.feed_data(frame[3:])      # the rest
        reader.feed_eof()
        self.assertEqual(await _read_frame(reader), obj)

    async def test_read_frame_empty_payload(self):
        obj = {}
        reader = asyncio.StreamReader()
        reader.feed_data(_encode_frame(obj))
        reader.feed_eof()
        self.assertEqual(await _read_frame(reader), obj)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'warden.aio'` (or `ImportError`).

- [ ] **Step 3: Write minimal implementation**

Create `warden/aio.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit -v`
Expected: PASS (4 framing tests). The retry/error imports at the top of the test file resolve to names that don't exist yet — so this step will still fail on import. **Add stubs** so imports resolve: append to `warden/aio.py`:

```python
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
```

Re-run. Expected: PASS (4 framing tests; the stubs satisfy the imports).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add warden/aio.py warden/tests/test_client_unit.py
git commit -m "warden-09k: framing helpers + exceptions for control-plane client"
```

---

## Task 2: Retry-connect helper

**Files:**
- Modify: `warden/aio.py` (replace the `_connect_with_retry` stub)
- Test: `warden/tests/test_client_unit.py` (add a test class)

- [ ] **Step 1: Write the failing test**

Append to `warden/tests/test_client_unit.py` (before the `if __name__` block):

```python
# warden-09k
class RetryConnectTests(unittest.IsolatedAsyncioTestCase):
    def _fake_clock(self):
        """Return (sleep, monotonic, delays) where sleep advances a fake clock."""
        state = {"now": 0.0}
        delays: list[float] = []

        async def sleep(d: float) -> None:
            delays.append(d)
            state["now"] += d

        def monotonic() -> float:
            return state["now"]

        return sleep, monotonic, delays

    async def test_backoff_schedule_then_success(self):
        sleep, monotonic, delays = self._fake_clock()
        attempts = {"n": 0}

        async def connector():
            attempts["n"] += 1
            if attempts["n"] <= 8:
                raise FileNotFoundError()
            return ("reader", "writer")

        result = await _connect_with_retry(
            connector, timeout=None, sleep=sleep, monotonic=monotonic
        )
        self.assertEqual(result, ("reader", "writer"))
        # 50ms doubling, capped at 2s: one sleep per failed attempt (8 failures)
        self.assertEqual(delays, [0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 2.0, 2.0])

    async def test_connection_refused_also_retries(self):
        sleep, monotonic, delays = self._fake_clock()
        attempts = {"n": 0}

        async def connector():
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise ConnectionRefusedError()
            return "ok"

        self.assertEqual(
            await _connect_with_retry(connector, sleep=sleep, monotonic=monotonic),
            "ok",
        )
        self.assertEqual(delays, [0.05])

    async def test_timeout_raises_warden_unavailable(self):
        sleep, monotonic, _ = self._fake_clock()

        async def connector():
            raise FileNotFoundError()

        with self.assertRaises(WardenUnavailable):
            await _connect_with_retry(
                connector, timeout=1.0, sleep=sleep, monotonic=monotonic
            )

    async def test_no_timeout_never_raises_unavailable(self):
        sleep, monotonic, _ = self._fake_clock()
        attempts = {"n": 0}

        async def connector():
            attempts["n"] += 1
            if attempts["n"] <= 50:
                raise FileNotFoundError()
            return "ok"

        # Even after 50 failures, with no timeout it keeps going and succeeds.
        self.assertEqual(
            await _connect_with_retry(connector, sleep=sleep, monotonic=monotonic),
            "ok",
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit.RetryConnectTests -v`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Write minimal implementation**

In `warden/aio.py`, replace the `_connect_with_retry` stub body with:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit -v`
Expected: PASS (framing + 4 retry tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add warden/aio.py warden/tests/test_client_unit.py
git commit -m "warden-09k: exponential-backoff retry-connect helper"
```

---

## Task 3: Client connect / close / _request

**Files:**
- Modify: `warden/aio.py` (replace the `Client` stub)
- Test: `warden/tests/test_client_unit.py` (add a test class)

- [ ] **Step 1: Write the failing test**

Append to `warden/tests/test_client_unit.py` (before the `if __name__` block):

```python
# warden-09k
class _FakeWriter:
    """Captures bytes written; no real socket."""

    def __init__(self):
        self.buffer = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        pass


def _reader_with(*responses: dict) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    for r in responses:
        reader.feed_data(_encode_frame(r))
    return reader


def _sent_requests(writer: _FakeWriter) -> list[dict]:
    """Decode every length-prefixed frame the client wrote."""
    out, buf, pos = [], bytes(writer.buffer), 0
    while pos < len(buf):
        (length,) = struct.unpack(">I", buf[pos : pos + 4])
        out.append(json.loads(buf[pos + 4 : pos + 4 + length].decode("utf-8")))
        pos += 4 + length
    return out


async def _connected_client(reader, writer):
    async def connector(path):
        return reader, writer

    return await Client.connect("/tmp/ignored.sock", _connector=connector)


class RequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_success_returns_payload(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"beam_id": 3}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        payload = await client._request("beam.create", {})
        self.assertEqual(payload, {"beam_id": 3})
        sent = _sent_requests(writer)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["req_id"], "1")
        self.assertEqual(sent[0]["action"], "beam.create")
        self.assertEqual(sent[0]["payload"], {})

    async def test_req_id_increments_per_request(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {}},
            {"req_id": "2", "ok": True, "error": None, "payload": {}},
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        await client._request("proc.list", {})
        await client._request("proc.list", {})
        self.assertEqual([r["req_id"] for r in _sent_requests(writer)], ["1", "2"])

    async def test_ok_false_raises_warden_error(self):
        reader = _reader_with(
            {"req_id": "1", "ok": False, "error": "unknown beam", "payload": None}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        with self.assertRaises(WardenError) as ctx:
            await client._request("proc.list", {"beam": 99})
        self.assertIn("unknown beam", str(ctx.exception))

    async def test_dropped_connection_marks_disconnected(self):
        reader = asyncio.StreamReader()
        reader.feed_eof()  # server hung up: readexactly raises IncompleteReadError
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        with self.assertRaises(ConnectionError):
            await client._request("proc.list", {})
        self.assertFalse(client._connected)

    async def test_close_is_idempotent_and_closes_writer(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        await client.close()
        self.assertTrue(writer.closed)
        await client.close()  # second close must not raise
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit.RequestTests -v`
Expected: FAIL — `Client.connect` does not exist (the stub is an empty class).

- [ ] **Step 3: Write minimal implementation**

In `warden/aio.py`, replace the `class Client: pass` stub with:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit -v`
Expected: PASS (framing + retry + 5 request tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add warden/aio.py warden/tests/test_client_unit.py
git commit -m "warden-09k: Client connect/close/_request with serialized lock + lazy-reconnect flag"
```

---

## Task 4: Verb methods (beam_create, proc_spawn, proc_send, proc_call, proc_list)

**Files:**
- Modify: `warden/aio.py` (add methods to `Client`)
- Test: `warden/tests/test_client_unit.py` (add a test class)

- [ ] **Step 1: Write the failing test**

Append to `warden/tests/test_client_unit.py` (before the `if __name__` block):

```python
# warden-09k
class VerbTests(unittest.IsolatedAsyncioTestCase):
    async def test_beam_create_default_payload_and_return(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"beam_id": 4}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        self.assertEqual(await client.beam_create(), 4)
        self.assertEqual(_sent_requests(writer)[0]["payload"], {})

    async def test_beam_create_with_explicit_beam(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"beam_id": 9}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        self.assertEqual(await client.beam_create(beam=9), 9)
        self.assertEqual(_sent_requests(writer)[0]["payload"], {"beam": 9})

    async def test_proc_spawn_builds_payload_and_returns_pid(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"pid": "2/5"}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        pid = await client.proc_spawn(
            ["python3", "w.py"], beam=2, parent="2/1", restart="permanent"
        )
        self.assertEqual(pid, "2/5")
        sent = _sent_requests(writer)[0]
        self.assertEqual(sent["action"], "proc.spawn")
        self.assertEqual(
            sent["payload"],
            {"cmd": ["python3", "w.py"], "beam": 2, "parent": "2/1", "restart": "permanent"},
        )

    async def test_proc_spawn_omits_unset_optionals(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"pid": "1/3"}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        await client.proc_spawn(["echo", "hi"])
        self.assertEqual(_sent_requests(writer)[0]["payload"], {"cmd": ["echo", "hi"]})

    async def test_proc_spawn_bad_restart_raises_before_io(self):
        reader = asyncio.StreamReader()  # no response queued — proves no I/O
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        with self.assertRaises(ValueError):
            await client.proc_spawn(["echo"], restart="bogus")
        self.assertEqual(len(writer.buffer), 0)

    async def test_proc_send_returns_none(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": None}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        self.assertIsNone(await client.proc_send("1/2", "req.ping", {"x": 1}))
        sent = _sent_requests(writer)[0]
        self.assertEqual(sent["action"], "proc.send")
        self.assertEqual(
            sent["payload"], {"pid": "1/2", "type": "req.ping", "body": {"x": 1}}
        )

    async def test_proc_call_returns_type_and_body(self):
        reader = _reader_with(
            {
                "req_id": "1",
                "ok": True,
                "error": None,
                "payload": {"type": "res.ok", "body": 55},
            }
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        reply = await client.proc_call("1/2", "req.fib", 10, timeout_ms=2000)
        self.assertEqual(reply, {"type": "res.ok", "body": 55})
        sent = _sent_requests(writer)[0]
        self.assertEqual(sent["action"], "proc.call")
        self.assertEqual(
            sent["payload"],
            {"pid": "1/2", "type": "req.fib", "body": 10, "timeout_ms": 2000},
        )

    async def test_proc_list_returns_processes_array(self):
        procs = [{"pid": "1/2", "kind": "foreign", "state": "running",
                  "policy": "permanent", "last_active_ms": 12}]
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"processes": procs}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        self.assertEqual(await client.proc_list(beam=1), procs)
        self.assertEqual(_sent_requests(writer)[0]["payload"], {"beam": 1})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit.VerbTests -v`
Expected: FAIL — `AttributeError: 'Client' object has no attribute 'beam_create'`.

- [ ] **Step 3: Write minimal implementation**

In `warden/aio.py`, add these methods to `Client` (place them after `_request`, before `close`):

```python
    # warden-09k
    async def beam_create(self, beam: int | None = None) -> int:
        payload = {} if beam is None else {"beam": beam}
        result = await self._request("beam.create", payload)
        return result["beam_id"]

    async def proc_spawn(
        self,
        cmd,
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
        payload: dict[str, Any] = {"cmd": list(cmd)}
        if beam is not None:
            payload["beam"] = beam
        if parent is not None:
            payload["parent"] = parent
        if restart is not None:
            payload["restart"] = restart
        result = await self._request("proc.spawn", payload)
        return result["pid"]

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
        return result["processes"]
```

Note: the wire field is `type` (matching the daemon's `proc.send`/`proc.call` payload); the Python parameter is named `msg_type` to avoid shadowing the `type` builtin.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit -v`
Expected: PASS (all unit tests: framing + retry + request + verb).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add warden/aio.py warden/tests/test_client_unit.py
git commit -m "warden-09k: control-plane verb methods (beam_create/proc_spawn/send/call/list)"
```

---

## Task 5: Package re-exports

**Files:**
- Modify: `warden/__init__.py`
- Test: `warden/tests/test_client_unit.py` (add a test class)

- [ ] **Step 1: Write the failing test**

Append to `warden/tests/test_client_unit.py` (before the `if __name__` block):

```python
# warden-09k
class PackageExportTests(unittest.TestCase):
    def test_client_and_errors_exported_from_warden(self):
        import warden

        self.assertIs(warden.Client, Client)
        self.assertIs(warden.WardenError, WardenError)
        self.assertIs(warden.WardenUnavailable, WardenUnavailable)
        for name in ("Client", "WardenError", "WardenUnavailable"):
            self.assertIn(name, warden.__all__)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit.PackageExportTests -v`
Expected: FAIL — `AttributeError: module 'warden' has no attribute 'Client'`.

- [ ] **Step 3: Write minimal implementation**

Replace the body of `warden/__init__.py` with:

```python
# warden-942
"""Warden Python SDK — connect a Python process to the Warden runtime."""

from .client import BeamCtx
from .decorators import tool
from .loop import run_loop

# warden-09k
from .aio import Client, WardenError, WardenUnavailable

__all__ = [
    "BeamCtx",
    "tool",
    "run_loop",
    # warden-09k
    "Client",
    "WardenError",
    "WardenUnavailable",
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit -v`
Expected: PASS (all unit tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add warden/__init__.py warden/tests/test_client_unit.py
git commit -m "warden-09k: re-export Client + errors from warden package"
```

---

## Task 6: Gated integration test against the real daemon

**Files:**
- Create: `warden/tests/test_client_integration.py`

This test drives the real `zig-out/bin/warden`. It reuses the proven
`examples/live_demo/math_worker.py` worker (handles `req.fib` → returns the
Fibonacci number via a `res.ok` reply). It does **not** run `zig build`; it skips
if `zig-out/bin/warden` or `python3` is missing.

- [ ] **Step 1: Write the test (it is also the implementation — no production code changes)**

Create `warden/tests/test_client_integration.py`:

```python
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
        self._sock = os.path.join(self._tmp, "ctrl.sock")
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
            beam_id = await client.beam_create()
            self.assertIsInstance(beam_id, int)

            pid = await client.proc_spawn(
                [sys.executable, _WORKER],
                beam=beam_id,
                restart="permanent",
            )
            self.assertRegex(pid, r"^\d+/\d+$")

            reply = await client.proc_call(pid, "req.fib", 10, timeout_ms=5000)
            self.assertEqual(reply["body"], 55)  # fib(10) == 55

            procs = await client.proc_list(beam=beam_id)
            self.assertTrue(any(p.get("pid") == pid for p in procs))

    async def test_retry_connect_blocks_until_daemon_up(self):
        # Connect BEFORE the daemon exists; it must block, then succeed.
        connect_task = asyncio.create_task(Client.connect(self._sock, timeout=10))
        await asyncio.sleep(0.3)  # prove it is still waiting, not errored
        self.assertFalse(connect_task.done())

        self._start_daemon()
        self.assertTrue(_wait_for_socket(self._sock))

        client = await connect_task
        try:
            beam_id = await client.beam_create()
            self.assertIsInstance(beam_id, int)
        finally:
            await client.close()

    async def test_connect_timeout_raises_when_daemon_never_starts(self):
        with self.assertRaises(WardenUnavailable):
            await Client.connect(self._sock, timeout=0.5)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_integration -v`
Expected: PASS (3 tests) if `zig-out/bin/warden` exists; otherwise the whole class reports as **skipped** — either outcome is acceptable for this step. If the binary is stale, rebuild first: `zig build` then re-run.

- [ ] **Step 3: Run the entire SDK test suite**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest discover -s warden/tests -v`
Expected: PASS — the new unit + integration tests plus the pre-existing `test_sdk.py` all green (integration may show skips if the daemon is absent).

- [ ] **Step 4: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add warden/tests/test_client_integration.py
git commit -m "warden-09k: gated integration test driving the real daemon"
```

---

## Task 7: Verify, merge, close

**Files:** none (verification + git)

- [ ] **Step 1: Full unit suite (must always pass, no daemon required)**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit warden.tests.test_sdk -v`
Expected: PASS, zero failures.

- [ ] **Step 2: Confirm the daemon still builds and Zig tests pass (no regressions)**

Run: `cd /Users/leslierussell/repo/warden && zig build test 2>&1 | tail -5`
Expected: build succeeds, all Zig tests pass (this plan touches no Zig — it is a smoke check that the worktree is clean).

- [ ] **Step 3: Confirm only intended files changed**

Run: `cd /Users/leslierussell/repo/warden && git diff --stat main...HEAD -- warden/`
Expected: only `warden/aio.py`, `warden/__init__.py`, `warden/tests/test_client_unit.py`, `warden/tests/test_client_integration.py` appear. No `.zig-cache`, `zig-out`, or other artifacts staged in any commit.

- [ ] **Step 4: Merge to master**

```bash
cd /Users/leslierussell/repo/warden
git checkout master
git merge --no-ff warden-09k -m "warden-09k: Python control-plane client SDK (warden.Client)"
git branch -d warden-09k
```

- [ ] **Step 5: Close the bead**

```bash
cd /Users/leslierussell/repo/warden
bd close warden-09k --reason="Implemented warden.Client async control-plane SDK: framing, retry-connect (50ms→2s, blocks forever, timeout escape hatch), serialized requests with lazy reconnect, verb wrappers (beam_create/proc_spawn/proc_send/proc_call/proc_list), package re-exports, full unit suite + gated integration test against the real daemon."
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** retry contract (Task 2), serialized `_request` + lazy reconnect (Task 3), all five verbs + client-side `restart` validation (Task 4), `WardenError`/`WardenUnavailable` (Tasks 1–4), file layout + re-exports (Task 5), gated integration incl. retry-before-daemon (Task 6). The spec's `pytest` assumption is intentionally replaced with stdlib `unittest.IsolatedAsyncioTestCase` (zero new deps; consistent with `test_sdk.py`).
- **Wire-field vs param name:** the JSON field is `type`; the Python kwarg is `msg_type` to avoid shadowing the builtin. Keep this consistent across `proc_send`/`proc_call`.
- **pid format** is the string `"beam/proc"` (e.g. `"2/5"`); never parse it client-side beyond passing it back.
