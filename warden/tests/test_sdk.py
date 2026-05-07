# warden-942
"""Unit tests for the Warden Python SDK (no live socket required)."""

import json
import socket
import struct
import threading
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from warden.client import BeamCtx
from warden.decorators import tool
from warden.loop import run_loop


def _frame(obj: dict) -> bytes:
    payload = json.dumps(obj).encode()
    return struct.pack(">I", len(payload)) + payload


class FakeSocket:
    """In-process fake socket — feeds canned frames, records sent frames."""

    def __init__(self, frames_to_send: list[dict]):
        # pre-encode all frames the "runtime" will push to the client
        self._inbox = b"".join(_frame(f) for f in frames_to_send)
        self._pos = 0
        self.sent: list[dict] = []
        self._timeout = None

    def settimeout(self, t):
        self._timeout = t

    def recv(self, n: int) -> bytes:
        chunk = self._inbox[self._pos: self._pos + n]
        if not chunk:
            raise socket.timeout("no more data")
        self._pos += len(chunk)
        return chunk

    def sendall(self, data: bytes) -> None:
        # data may contain multiple concatenated frames; decode all of them
        pos = 0
        while pos + 4 <= len(data):
            n = struct.unpack(">I", data[pos: pos + 4])[0]
            end = pos + 4 + n
            if end > len(data):
                break
            self.sent.append(json.loads(data[pos + 4: end]))
            pos = end


def _make_ctx(frames: list[dict]) -> tuple[BeamCtx, FakeSocket]:
    sock = FakeSocket(frames)
    ctx = BeamCtx.__new__(BeamCtx)
    ctx.sock = sock
    ctx.pid = 1
    return ctx, sock


class TestToolDecorator(unittest.TestCase):

    def test_success_emits_tool_call_and_tool_result(self):
        # runtime will ack two log frames
        ctx, sock = _make_ctx([{"kind": "ok"}, {"kind": "ok"}])

        @tool
        def my_tool(ctx, msg):
            return "done"

        result = my_tool(ctx, {"id": "m1", "type": "req.t", "body": "inp", "reply_to": ""})

        self.assertEqual(result, "done")
        self.assertEqual(len(sock.sent), 2)
        self.assertEqual(sock.sent[0]["level"], "tool_call")
        self.assertEqual(sock.sent[0]["tool"], "my_tool")
        self.assertEqual(sock.sent[0]["corr"], "m1")
        self.assertEqual(sock.sent[1]["level"], "tool_result")
        self.assertTrue(sock.sent[1]["success"])
        self.assertEqual(sock.sent[1]["output"], "done")

    def test_failure_emits_tool_result_with_success_false(self):
        ctx, sock = _make_ctx([{"kind": "ok"}, {"kind": "ok"}])

        @tool
        def failing(ctx, msg):
            raise ValueError("oh no")

        with self.assertRaises(ValueError):
            failing(ctx, {"id": "m2", "type": "req.f", "body": None, "reply_to": ""})

        self.assertEqual(sock.sent[1]["level"], "tool_result")
        self.assertFalse(sock.sent[1]["success"])
        self.assertIn("oh no", sock.sent[1]["output"])

    def test_corr_propagated_from_msg_id(self):
        ctx, sock = _make_ctx([{"kind": "ok"}, {"kind": "ok"}])

        @tool
        def noop(ctx, msg):
            return None

        noop(ctx, {"id": "corr-xyz", "type": "req.n", "body": None, "reply_to": ""})

        self.assertEqual(sock.sent[0]["corr"], "corr-xyz")
        self.assertEqual(sock.sent[1]["corr"], "corr-xyz")


class TestRunLoop(unittest.TestCase):

    def test_dispatches_request_and_replies(self):
        # Sequence of frames the fake runtime delivers:
        #   1. ok  — ack for "run_loop: started" note
        #   2. msg req.greet
        #   3. ok  — ack for reply
        #   4. msg sys.shutdown
        #   5. ok  — ack for "received shutdown" note
        #   6. ok  — ack for "run_loop: stopped" note
        ctx, sock = _make_ctx([
            {"kind": "ok"},
            {"kind": "msg", "type": "req.greet", "id": "r1",
             "body": "world", "from": "test", "reply_to": "test"},
            {"kind": "ok"},
            {"kind": "msg", "type": "sys.shutdown", "id": "s1",
             "body": None, "from": "test"},
            {"kind": "ok"},
            {"kind": "ok"},
        ])

        def greet(ctx, msg):
            return f"hello {msg['body']}"

        run_loop(ctx, {"req.greet": greet}, recv_timeout_ms=50)

        reply = next((f for f in sock.sent if f.get("kind") == "reply"), None)
        self.assertIsNotNone(reply, "expected a reply frame")
        self.assertEqual(reply["type"], "res.ok")
        self.assertEqual(reply["body"], "hello world")
        self.assertEqual(reply["corr"], "r1")

    def test_error_in_handler_sends_res_error_not_crash(self):
        ctx, sock = _make_ctx([
            {"kind": "ok"},           # ack: "run_loop: started"
            {"kind": "msg", "type": "req.bad", "id": "r2",
             "body": None, "from": "test", "reply_to": "test"},
            {"kind": "ok"},           # ack: warn log
            {"kind": "ok"},           # ack: res.error reply
            {"kind": "msg", "type": "sys.shutdown", "id": "s2",
             "body": None, "from": "test"},
            {"kind": "ok"},           # ack: "received shutdown"
            {"kind": "ok"},           # ack: "run_loop: stopped"
        ])

        def crash(ctx, msg):
            raise RuntimeError("boom")

        run_loop(ctx, {"req.bad": crash}, recv_timeout_ms=50)

        error_reply = next((f for f in sock.sent if f.get("type") == "res.error"), None)
        self.assertIsNotNone(error_reply, "expected a res.error reply")
        self.assertIn("boom", error_reply.get("body", ""))

    def test_unknown_msg_type_is_skipped(self):
        ctx, sock = _make_ctx([
            {"kind": "ok"},           # ack: "run_loop: started"
            {"kind": "msg", "type": "req.unknown", "id": "r3",
             "body": None, "from": "test", "reply_to": ""},
            {"kind": "ok"},           # ack: warn log
            {"kind": "msg", "type": "sys.shutdown", "id": "s3",
             "body": None, "from": "test"},
            {"kind": "ok"},           # ack: "received shutdown"
            {"kind": "ok"},           # ack: "run_loop: stopped"
        ])

        run_loop(ctx, {}, recv_timeout_ms=50)

        # no reply should be sent for unknown message
        replies = [f for f in sock.sent if f.get("kind") == "reply"]
        self.assertEqual(len(replies), 0)


if __name__ == "__main__":
    unittest.main()
