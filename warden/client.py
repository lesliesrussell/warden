# warden-942
import socket
import struct
import json
import base64
import os
import uuid


class BeamCtx:
    """Low-level connection to the Warden runtime over a Unix socket."""

    def __init__(self, sock_path: str | None = None):
        path = sock_path or os.environ.get("WARDEN_SOCKET", "/tmp/warden.sock")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        # warden-499
        try:
            self.sock.connect(path)
        except FileNotFoundError:
            raise RuntimeError(
                f"No Warden runtime listening at {path!r}. "
                "Start the runtime first, or set WARDEN_SOCKET to the correct socket path."
            ) from None
        hs = self._recv_frame()
        self.pid = hs["pid"]

    # ------------------------------------------------------------------ recv

    def recv(self, timeout_ms: int = 5000) -> dict | None:
        self.sock.settimeout(timeout_ms / 1000)
        try:
            frame = self._recv_frame()
            if frame.get("kind") == "msg":
                return frame
        except (socket.timeout, TimeoutError):
            return None
        return None

    # ------------------------------------------------------------------ send / reply

    def send(self, to: str, msg_type: str, body, *, msg_id: str | None = None) -> None:
        self._send_frame({
            "kind": "send",
            "to": to,
            "type": msg_type,
            "id": msg_id or _new_id(),
            "body": body,
        })
        self._recv_frame()

    def reply(self, reply_to: str, msg_type: str, body, *, msg_id: str | None = None, corr: str = "") -> None:
        self._send_frame({
            "kind": "reply",
            "reply_to": reply_to,
            "type": msg_type,
            "id": msg_id or _new_id(),
            "corr": corr,
            "body": body,
        })
        self._recv_frame()

    # ------------------------------------------------------------------ structured logging

    def log(self, level: str, message: str) -> None:
        self._send_frame({"kind": "log", "level": level, "message": message})
        self._recv_frame()

    def note(self, message: str) -> None:
        self.log("info", message)

    def warn(self, message: str) -> None:
        self.log("warn", message)

    def metric(self, name: str, value: float, unit: str | None = None) -> None:
        frame: dict = {"kind": "metric", "name": name, "value": value}
        if unit:
            frame["unit"] = unit
        self._send_frame(frame)
        self._recv_frame()

    def decision(self, message: str, *, context: dict | None = None) -> None:
        frame: dict = {"kind": "log", "level": "decision", "message": message}
        if context:
            frame["context"] = context
        self._send_frame(frame)
        self._recv_frame()

    def tool_call(self, tool: str, input, *, corr: str = "") -> None:
        self._send_frame({
            "kind": "log",
            "level": "tool_call",
            "tool": tool,
            "input": input,
            "corr": corr,
        })
        self._recv_frame()

    def tool_result(self, tool: str, *, success: bool, output=None, corr: str = "") -> None:
        self._send_frame({
            "kind": "log",
            "level": "tool_result",
            "tool": tool,
            "success": success,
            "output": output,
            "corr": corr,
        })
        self._recv_frame()

    # ------------------------------------------------------------------ filesystem

    def fs_write(self, ns: str, path: str, data: bytes) -> None:
        self._send_frame({
            "kind": "fs_write",
            "ns": ns,
            "path": path,
            "data": base64.b64encode(data).decode(),
        })
        self._recv_frame()

    def fs_read(self, ns: str, path: str) -> bytes:
        self._send_frame({"kind": "fs_read", "ns": ns, "path": path})
        resp = self._recv_frame()
        return base64.b64decode(resp["data"])

    # ------------------------------------------------------------------ wire

    def _send_frame(self, obj: dict) -> None:
        payload = json.dumps(obj).encode()
        self.sock.sendall(struct.pack(">I", len(payload)) + payload)

    def _recv_frame(self) -> dict:
        raw = self._recvn(4)
        length = struct.unpack(">I", raw)[0]
        return json.loads(self._recvn(length))

    def _recvn(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("warden socket closed")
            buf += chunk
        return buf


def _new_id() -> str:
    return str(uuid.uuid4())[:8]
