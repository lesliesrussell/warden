# live_demo example

Demonstrates heterogeneous process supervision: a Zig runtime managing two Python
workers — a math service and an HTTP server — as supervised children of a Zig
supervisor process.

## What runs

| Worker | Script | Handles |
|--------|--------|---------|
| `math_worker` | `math_worker.py` | `req.fib` → Fibonacci, `req.primes` → sieve of Eratosthenes |
| `web_server` | `web_server.py` | HTTP GET `/status` on a random port; `req.get_url` → port discovery |

Both workers are spawned under a native Zig supervisor and appear in the registry
and topology tree as supervised children.

## What the test verifies

```
[registry]  math_worker and web_server appear in proc.list (beam 43)
[topology]  supervisor has exactly 2 children
[messaging] req.fib(10) → res.ok body=55  (round-trip through ForeignBridge)
[http]      GET /status → HTTP 200 {"status":"ok","hits":1}
[lifecycle] pause math_worker → state=paused; resume → state=running
```

## Running the integration test

```bash
zig build test --filter "live demo"
```

## Running the workers manually

```bash
# Terminal 1 — math worker
WARDEN_SOCKET=/tmp/warden_math.sock python examples/live_demo/math_worker.py

# Terminal 2 — web server
WARDEN_SOCKET=/tmp/warden_web.sock python examples/live_demo/web_server.py
```

Send `req.fib` with body `10` to get back `55`.
Send `req.get_url` to discover the HTTP port, then `GET /status`.

## Message protocol

Workers speak the same length-prefixed JSON frame protocol as all Warden foreign
workers (see `src/bridge.zig` and `warden/` SDK):

```
[4-byte big-endian length][JSON frame]
```

Frames sent from runtime to worker:

```json
{"type":"req.fib","id":"abc","from":"43/1","reply_to":"43/2","body":10}
```

Frames sent from worker to runtime (`type` field determines routing):

```json
{"type":"res.ok","id":"abc","corr":"req-id","body":55}
{"type":"log","level":"note","msg":"fib(10) = 55"}
```
