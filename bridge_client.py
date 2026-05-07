import socket, struct, json, base64, os

class BeamCtx:
    def __init__(self):
        sock_path = os.environ["WARDEN_SOCKET"]
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(sock_path)
        hs = self._recv_frame()
        self.pid = hs["pid"]

    def recv(self, timeout_ms=5000):
        self.sock.settimeout(timeout_ms / 1000)
        try:
            frame = self._recv_frame()
            if frame.get("kind") == "msg":
                return frame
        except socket.timeout:
            return None
        return None

    def send(self, to, msg_type, msg_id, body):
        self._send_frame({"kind": "send", "to": to, "type": msg_type, "id": msg_id, "body": body})
        self._recv_frame()

    def reply(self, reply_to, msg_type, msg_id, corr, body):
        self._send_frame({"kind": "reply", "reply_to": reply_to, "type": msg_type, "id": msg_id, "corr": corr, "body": body})
        self._recv_frame()

    def log(self, level, message):
        self._send_frame({"kind": "log", "level": level, "message": message})
        self._recv_frame()

    def note(self, message):
        self.log("info", message)

    def fs_write(self, ns, path, data: bytes):
        self._send_frame({"kind": "fs_write", "ns": ns, "path": path, "data": base64.b64encode(data).decode()})
        self._recv_frame()

    def fs_read(self, ns, path) -> bytes:
        self._send_frame({"kind": "fs_read", "ns": ns, "path": path})
        resp = self._recv_frame()
        return base64.b64decode(resp["data"])

    def _send_frame(self, obj):
        payload = json.dumps(obj).encode()
        self.sock.sendall(struct.pack(">I", len(payload)) + payload)

    def _recv_frame(self):
        raw = self._recvn(4)
        length = struct.unpack(">I", raw)[0]
        return json.loads(self._recvn(length))

    def _recvn(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("socket closed")
            buf += chunk
        return buf
