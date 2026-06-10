# @warden/sdk

Async TypeScript client for the [Warden](../../README.md) daemon's control
plane. Connect to the daemon over its Unix control socket to create beams and
spawn / message supervised workers. Wire-compatible with the Python
`warden.Client`.

## Requirements

- Node ≥ 22.6 (uses `node:net`, `node:test`, and native TypeScript type-stripping; flagless on ≥ 22.18).
- No runtime dependencies.
- Distributed as TypeScript source (no build step) — consume under a Node with type-stripping or a TS-aware bundler/loader.

## Usage

```ts
import { Warden } from "@warden/sdk";

// Blocks (with 50ms→2s backoff) until the daemon is reachable.
const w = await Warden.connect({ path: "/path/to/ctrl.sock" }); // or rely on WARDEN_CTRL_SOCKET
try {
  const beam = await w.beamCreate();
  const pid = await w.procSpawn(["python3", "worker.py"], { beam, restart: "permanent" });
  const reply = await w.procCall(pid, "req.fib", 10);
  console.log(reply.body);
  for (const p of await w.procList(beam)) {
    console.log(`${p.beam}/${p.pid}`, p.kind, p.state);
  }
} finally {
  await w.close();
}

// Or with explicit resource management (Node ≥ 22.6):
await using c = await Warden.connect();
```

## API

- `Warden.connect({ path?, timeout? }) => Promise<Warden>` — `path` defaults to
  `WARDEN_CTRL_SOCKET` or `~/.warden/ctrl.sock`; `timeout` is **seconds** to wait
  for the daemon (omit = forever; on elapse throws `WardenUnavailable`).
- `beamCreate(beam?) => Promise<number>`
- `procSpawn(cmd, { beam?, parent?, restart? }) => Promise<string>` — `cmd` is a
  string or `string[]`; `restart` ∈ `"permanent" | "transient" | "temporary"`
  (validated client-side, throws `TypeError` otherwise). Returns the `"beam/proc"` pid.
- `procSend(pid, msgType, body) => Promise<void>`
- `procCall(pid, msgType, body, { timeoutMs? }) => Promise<{ type, body }>` —
  `timeoutMs` defaults to 5000.
- `procList(beam?) => Promise<Array<{ beam, pid, kind, state, policy, last_active_ms }>>`
  — note rows carry **separate `beam` and `pid` integers**.
- `close() => Promise<void>` (also via `Symbol.asyncDispose`).

Errors: `WardenError` (the daemon returned `ok=false`) and `WardenUnavailable`
(an explicit `connect` timeout elapsed).

## Protocol notes

The control protocol is **one request per connection** — the daemon closes the
socket after each response. The client opens a fresh connection per request
(serialized; the blocking retry-connect contract applies on every open), so a
daemon restart between calls is transparent.

## Testing

```sh
node --test test/*.test.ts
```

Unit + in-process fake-daemon tests always run. The integration test
(`test/integration.test.ts`) skips unless `zig-out/bin/warden` and `python3` are
present (run `zig build` at the repo root first).
