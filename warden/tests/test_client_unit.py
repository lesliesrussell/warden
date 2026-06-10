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


if __name__ == "__main__":
    unittest.main()
