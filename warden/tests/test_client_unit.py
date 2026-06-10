# warden-09k
"""Unit tests for warden.aio (no live daemon required)."""

import asyncio
import json
import os
import struct
import unittest
from unittest import mock

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


class ResolvePathTests(unittest.TestCase):
    def test_explicit_arg_wins_and_is_expanded(self):
        self.assertEqual(_resolve_path("/tmp/x.sock"), "/tmp/x.sock")
        self.assertEqual(
            _resolve_path("~/y.sock"), os.path.expanduser("~/y.sock")
        )

    def test_env_var_used_when_arg_is_none(self):
        with mock.patch.dict(os.environ, {"WARDEN_CTRL_SOCKET": "~/env.sock"}):
            self.assertEqual(
                _resolve_path(None), os.path.expanduser("~/env.sock")
            )

    def test_default_used_when_arg_none_and_env_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                _resolve_path(None),
                os.path.expanduser("~/.warden/ctrl.sock"),
            )


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
        sleep, monotonic, delays = self._fake_clock()

        async def connector():
            raise FileNotFoundError()

        with self.assertRaises(WardenUnavailable) as ctx:
            await _connect_with_retry(
                connector, timeout=1.0, sleep=sleep, monotonic=monotonic
            )
        # deadline 1.0s: sleeps 0.05+0.1+0.2+0.4+0.8 = 1.55s cumulative, so the
        # deadline check fires before the 6th sleep — no wasted sleeps past it.
        self.assertEqual(delays, [0.05, 0.1, 0.2, 0.4, 0.8])
        self.assertIsInstance(ctx.exception.__cause__, FileNotFoundError)

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

    async def test_reconnect_after_drop_succeeds(self):
        # First connection drops (EOF before any response); second serves a reply.
        dropped = asyncio.StreamReader()
        dropped.feed_eof()
        good = _reader_with(
            {"req_id": "2", "ok": True, "error": None, "payload": {"beam_id": 5}}
        )
        writer = _FakeWriter()
        calls = {"n": 0}

        async def connector(path):
            calls["n"] += 1
            return (dropped, writer) if calls["n"] == 1 else (good, writer)

        client = await Client.connect("/tmp/ignored.sock", _connector=connector)
        with self.assertRaises(ConnectionError):
            await client._request("proc.list", {})
        self.assertFalse(client._connected)
        # next call transparently reconnects (connector invoked again) and succeeds
        payload = await client._request("beam.create", {})
        self.assertEqual(payload, {"beam_id": 5})
        self.assertEqual(calls["n"], 2)


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

    async def test_proc_spawn_string_cmd_becomes_single_element(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {"pid": "1/4"}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        await client.proc_spawn("worker.py")
        self.assertEqual(
            _sent_requests(writer)[0]["payload"], {"cmd": ["worker.py"]}
        )

    async def test_proc_call_default_timeout_ms(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None,
             "payload": {"type": "res.ok", "body": 1}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        await client.proc_call("1/2", "req.x", None)
        self.assertEqual(_sent_requests(writer)[0]["payload"]["timeout_ms"], 5000)

    async def test_malformed_response_raises_warden_error(self):
        reader = _reader_with(
            {"req_id": "1", "ok": True, "error": None, "payload": {}}
        )
        writer = _FakeWriter()
        client = await _connected_client(reader, writer)
        with self.assertRaises(WardenError):
            await client.beam_create()


if __name__ == "__main__":
    unittest.main()
