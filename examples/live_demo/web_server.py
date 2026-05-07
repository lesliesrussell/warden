#!/usr/bin/env python3
# warden-3cn
"""Web server worker — runs an HTTP server and handles Warden control messages.

Spawns an http.server in a background thread on a random port.
Warden messages:
  req.get_url  → {"url": "http://127.0.0.1:<port>"}
  req.get_hits → {"hits": <count>}
"""

import sys
import os
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import warden

_hit_count = 0
_server_port = 0


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass  # suppress default stderr logging

    def do_GET(self):
        global _hit_count
        _hit_count += 1
        if self.path == "/status":
            body = json.dumps({"status": "ok", "hits": _hit_count}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


def start_http_server() -> int:
    global _server_port
    srv = HTTPServer(("127.0.0.1", 0), Handler)
    _server_port = srv.server_address[1]
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return _server_port


def handle_get_url(ctx, msg):
    return {"url": f"http://127.0.0.1:{_server_port}"}


def handle_get_hits(ctx, msg):
    return {"hits": _hit_count}


def main():
    port = start_http_server()
    ctx = warden.BeamCtx()
    ctx.note(f"web_server: HTTP listening on port {port}")
    warden.run_loop(ctx, {
        "req.get_url": handle_get_url,
        "req.get_hits": handle_get_hits,
    })


if __name__ == "__main__":
    main()
