# warden-6f6
"""Bridge handshake protocol-version validation (no live daemon required)."""

import unittest

from warden.client import (
    SUPPORTED_PROTOCOL,
    WardenProtocolError,
    _hello_frame,
    _validate_handshake,
)


class HandshakeValidationTests(unittest.TestCase):
    def test_matching_version_passes(self):
        # A handshake advertising the supported version is accepted.
        _validate_handshake(
            {"kind": "handshake", "protocol_version": SUPPORTED_PROTOCOL, "pid": "1/2"}
        )

    def test_wrong_version_raises(self):
        with self.assertRaises(WardenProtocolError):
            _validate_handshake({"protocol_version": SUPPORTED_PROTOCOL + 1, "pid": "1/2"})

    def test_missing_version_raises(self):
        # An un-versioned (e.g. older) runtime is treated as skew.
        with self.assertRaises(WardenProtocolError):
            _validate_handshake({"pid": "1/2"})


    # warden-19i
    def test_hello_frame_reports_supported_version(self):
        self.assertEqual(
            _hello_frame(),
            {"kind": "hello", "protocol_version": SUPPORTED_PROTOCOL},
        )

if __name__ == "__main__":
    unittest.main()
