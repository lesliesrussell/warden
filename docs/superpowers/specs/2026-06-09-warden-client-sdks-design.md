# Warden client SDKs (Python + TypeScript) — design

**Status:** approved design, pre-implementation
**Beads:** warden-09k (Python client SDK), warden-3yd (TypeScript client SDK)
**Date:** 2026-06-09

## Problem

Warden's daemon exposes a control plane over a Unix socket (the same protocol
`wardenctl` speaks). Today the only programmatic clients are `wardenctl` (a CLI)
and the in-tree Zig `ControlClient`. External programs that want to *drive*
Warden — create beams, spawn supervised workers, send/call them — have no
library. The existing `warden/` Python package is the **worker** SDK (`BeamCtx`
/ `run_loop`, for processes already running *on* a beam, speaking the bridge
protocol); it is not a client of the control plane.

This work adds two **async client** SDKs — Python and TypeScript — that connect
to the daemon's control socket, with a resilient retry-connect contract, and
wrap the management verbs so Python/Node/Deno/Bun programs can participate in
Warden-supervised topologies.

## Goals / non-goals

**Goals**
- Async client libraries (Python `asyncio`, TypeScript Promises) speaking the
  control protocol.
- A retry-connect contract: block until the daemon is up, never error on
  unavailability; transparently reconnect after a daemon bounce.
- Wrap: `beam_create`, `proc_spawn` (incl. the `restart` policy field),
  `proc_send`, `proc_call`, `proc_list`, plus `connect`/`close`.
- Idiomatic in each language; identical conceptual API; shared wire protocol.

**Non-goals**
- Full `wardenctl` parity (no `topology.get`, `proc.control`, `logs.stream`,
  `beam.reaper`/renice in v1 — those stay the CLI's job).
- req_id request multiplexing (requests are serialized — see §4).
- Changing the daemon / control protocol (clients only).
- The worker (bridge) SDK is untouched.

## Wire protocol (existing — clients conform to it)

Transport: a Unix stream socket. Each frame is a **4-byte big-endian u32 length
prefix** followed by that many bytes of UTF-8 JSON.

- Request: `{"req_id": "<string>", "action": "<verb>", "payload": { ... }}`
- Response: `{"req_id": "<string>", "ok": <bool>, "error": <string|null>, "payload": <obj|null>}`

`req_id` is a per-connection monotonically increasing counter (stringified).
The socket path defaults to `~/.warden/ctrl.sock`, overridable by the
`WARDEN_CTRL_SOCKET` env var or an explicit argument (matching `wardenctl`).
(Note: this is the **control** socket / `WARDEN_CTRL_SOCKET` — distinct from the
worker SDK's bridge socket / `WARDEN_SOCKET`.)

### Verbs used

| Verb | payload | success `payload` |
|---|---|---|
| `beam.create` | `{beam?: int}` | `{beam_id: int}` |
| `proc.spawn` | `{cmd: [str], beam?: int, parent?: "b/p", restart?: "permanent"|"transient"|"temporary"}` | `{pid: "b/p"}` |
| `proc.send` | `{pid: "b/p", type: str, body: any}` | `null` |
| `proc.call` | `{pid: "b/p", type: str, body: any, timeout_ms?: int}` | `{type: str, body: any}` |
| `proc.list` | `{beam?: int}` | `{processes: [{pid, kind, state, policy, last_active_ms}]}` |

On failure the daemon returns `ok=false` with an `error` string (e.g.
`proc.call` timeout → `error: "timeout"`).

## API

Both SDKs expose the same conceptual surface (snake_case in Python, camelCase
in TS):

```
Client.connect(path=None, *, timeout=None) -> Client      # classmethod / static, async
  # path: explicit socket path; default WARDEN_CTRL_SOCKET or ~/.warden/ctrl.sock
  # timeout: max seconds to wait for the daemon (None = forever)

  beam_create(beam=None) -> int                            # returns beam_id
  proc_spawn(cmd, *, beam=None, parent=None, restart=None) -> str   # returns pid
  proc_send(pid, type, body) -> None
  proc_call(pid, type, body, *, timeout_ms=5000) -> dict   # {"type", "body"}
  proc_list(beam=None) -> list[dict]                       # the processes array
  close() -> None
```
- Async context manager: Python `async with await Client.connect() as c:`;
  TS `await using c = await Warden.connect()` (or explicit `close()`).
- `restart` validated client-side against the three allowed strings (raise
  `ValueError`/`TypeError` early rather than round-tripping a bad value).
- TS method names: `beamCreate`, `procSpawn`, `procSend`, `procCall`, `procList`.
- TS `connect` options object: `Warden.connect({ path?, timeout? })`.

## Retry-connect contract (§ the headline)

`connect()`:
1. Resolve the socket path.
2. Attempt to open the Unix socket. On `FileNotFoundError`/`ENOENT`/
   `ECONNREFUSED` (daemon not up / socket not yet bound), wait `delay`, then
   retry. `delay` starts at **50ms**, doubles each attempt, capped at **2s**.
3. Block **indefinitely** by default; never raise on unavailability.
4. If `timeout` is set and the total wait exceeds it, raise
   `WardenUnavailable`. (Escape hatch; default `None` = forever.)
5. On the first successful connect, read nothing special — the control protocol
   has no server-initiated handshake frame (unlike the bridge protocol); the
   connection is immediately ready for requests.

**Lazy reconnect:** the client tracks a `connected` flag. If a request fails
because the socket is closed/reset (daemon restarted mid-session), the client
marks itself disconnected and **the next call re-runs the retry-connect loop**
before sending, so a daemon bounce is invisible to callers. The in-flight
request that hit the drop raises a connection error (the caller may retry it).

## Request/response handling (§4)

A single connection with **serialized requests** (one in-flight at a time),
guarded by an async lock (`asyncio.Lock` / a promise chain in TS):

1. acquire the lock,
2. ensure connected (lazy reconnect if needed),
3. `req_id = next(counter)`; write the framed request,
4. read exactly one length-prefixed response frame,
5. assert its `req_id` matches (defensive — serialization guarantees it),
6. release the lock,
7. if `ok` is false → raise `WardenError(error)`; else return the relevant slice
   of `payload`.

Serialization is intentional (req_id multiplexing buys nothing here per the
design review). A long `proc_call` blocks other calls on the same client until
it returns — callers needing concurrency open multiple clients.

## Errors

- `WardenError(message)` — a request returned `ok=false`; `message` is the
  server's `error` string. (Covers `proc.call` timeout, unknown beam, invalid
  pid, etc.)
- `WardenUnavailable` — only raised when an explicit `connect(timeout=…)`
  elapses before the daemon is reachable.
- Connection drop mid-request raises a connection error (Python: the underlying
  `ConnectionError`; TS: an `Error` with a clear message); the next call
  reconnects.
- Client-side validation errors (bad `restart` value, malformed pid) raise
  `ValueError`/`TypeError` before any I/O.

## File layout

**Python (warden-09k):**
```
warden/
  __init__.py        # add: from .aio import Client, WardenError, WardenUnavailable
  aio.py             # NEW — async Client + framing + retry loop + errors
  tests/
    test_client_unit.py    # NEW — framing, retry schedule, error mapping (no daemon)
    test_client_integration.py  # NEW — drives real zig-out/bin/warden (gated/skip)
```
Python ≥ 3.11 (matches the existing package's `str | None` syntax). Tests use
`pytest` + `pytest-asyncio`. No third-party runtime deps (stdlib `asyncio`,
`json`, `struct`).

**TypeScript (warden-3yd) — separate plan, built after Python merges:**
```
clients/typescript/
  package.json       # @warden/sdk, type: module, scripts: build/test
  tsconfig.json
  src/index.ts       # Client (camelCase API) + framing + retry + errors
  test/client.test.ts  # node:test — unit + gated integration
  README.md
```
Node ≥ 18 (built-in `node:test`, `node:net`). No runtime deps. Build with `tsc`.

## Testing

**Unit (no daemon, fast, always run):**
- Frame round-trip: encode `{req_id,action,payload}` → 4-byte BE length + JSON →
  decode back; boundary cases (empty payload, multi-byte UTF-8, a frame split
  across two socket reads — the reader must accumulate until it has the full
  length-prefixed frame).
- Retry schedule: with a stub "connect" that fails N times then succeeds, assert
  the backoff sequence (50,100,200,…,2000,2000) and that it never raises with no
  `timeout`; with `timeout` set, assert `WardenUnavailable` after the budget.
- Error mapping: an `ok=false` response → `WardenError` carrying the message;
  bad `restart` → `ValueError` before I/O.

**Integration (gated — skip if `zig-out/bin/warden` or `python3`/`node` absent):**
- The integration test does NOT run `zig build` itself; it checks for an existing
  `zig-out/bin/warden` and `return`s a skip if absent (keeps the test fast and
  hermetic; CI builds the daemon before running SDK tests).
- Start the real daemon on a temp `WARDEN_CTRL_SOCKET`, `connect()`, `beam_create()`,
  `proc_spawn(["python3", <tiny echo worker>], restart="permanent")`,
  `proc_call(pid, "req.echo", {...})` → assert the reply, `proc_list()` shows the
  worker, `close()`, stop the daemon.
- Retry-connect: start the client `connect()` *before* the daemon, confirm it
  blocks then succeeds once the daemon comes up (bounded by the test's own
  timeout, asserted via the escape-hatch).

## Implementation order

Python (warden-09k) first: spec → plan → implement → review → merge. **The
implementation plan that follows this spec covers the Python SDK only.** Then
TypeScript (warden-3yd) gets its own separate implementation plan from this same
spec and is built against the merged, proven Python contract (so the two are
guaranteed wire-compatible with the same daemon).
