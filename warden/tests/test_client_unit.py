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


if __name__ == "__main__":
    unittest.main()
