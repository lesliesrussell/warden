# Warden TypeScript Client SDK Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an async `@warden/sdk` TypeScript client (`Warden.connect`) that drives the Warden daemon's control plane over its Unix socket, wire-compatible with the merged Python SDK.

**Architecture:** A new standalone package `clients/typescript/` exposes a `Warden` client class (camelCase API) over `node:net`. It frames requests as a 4-byte big-endian length prefix + UTF-8 JSON, serializes requests behind a promise-chain mutex, and uses the **verified one-request-per-connection protocol** (the daemon closes the socket after each response — see the corrected spec) so every request opens a fresh connection via an exponential-backoff retry loop (50ms→2s cap, blocks forever by default).

**Tech Stack:** Node ≥ 20 (uses `node:net`, `node:test`, `node:assert`, and **native TypeScript type-stripping** — verified working on Node 22.22). No runtime dependencies. Source and tests are `.ts` run directly with `node --test test/*.test.ts`.

**Spec:** `docs/superpowers/specs/2026-06-09-warden-client-sdks-design.md` (TypeScript portion; the spec was corrected to the real daemon behavior during the Python build). This plan is built against the merged, proven Python contract so the two are wire-compatible.

**Bead:** warden-3yd. Every new contiguous code block carries a `// warden-3yd` comment.

---

## Deliberate deviations from the spec (and why)

1. **No `tsc` build step / no `npm install`.** The spec says "Build with tsc." Instead we run `.ts` directly via Node's native type-stripping (`node --test test/*.test.ts`). Rationale: it is zero-dependency and fully hermetic — a `tsc --noEmit` typecheck would require `@types/node` (a network install), which this environment may not have. `node --test` (which strips types and executes) is the authoritative gate. A `typecheck` npm script is included for environments that do have `@types/node`, but it is **not** a required gate.
2. **Native-strip constraints (must follow):** because Node only *erases* types (it does not transform), the source must avoid non-erasable TS — **no `enum`, no `namespace`, no constructor parameter-properties** (`constructor(private x)`), and type-only imports must use `import type`. Local imports MUST include the explicit `.ts` extension (e.g. `from "../src/index.ts"`). Use a `const` array + union type instead of an enum (as the API already specifies).
3. **Two test files** (`test/client.test.ts` for unit + in-process fake-daemon, `test/integration.test.ts` for the gated real-daemon test) instead of the spec's single `test/client.test.ts`, for separation of fast/always-run vs. gated.

---

## Wire contract (from the corrected spec — the client conforms)

- Frame: 4-byte big-endian u32 length prefix + that many bytes of UTF-8 JSON.
- Request: `{"req_id": "<str>", "action": "<verb>", "payload": {...}}`. Response: `{"req_id","ok","error","payload"}`.
- **One request per connection:** the daemon reads one frame, writes one response, then closes the socket. The client reconnects for each request.
- Verbs: `beam.create {beam?}`→`{beam_id}`; `proc.spawn {cmd:[str],beam?,parent?,restart?}`→`{pid:"b/p"}`; `proc.send {pid,type,body}`→`null`; `proc.call {pid,type,body,timeout_ms?}`→`{type,body}`; `proc.list {beam?}`→`{processes:[{beam,pid,kind,state,policy,last_active_ms}]}` (note: `proc.list` rows carry **separate `beam` and `pid` integers**, not a `"b/p"` string).
- Socket path default `~/.warden/ctrl.sock`, overridable by arg or `WARDEN_CTRL_SOCKET`.

---

## File Structure

- `clients/typescript/package.json` — **NEW.** `@warden/sdk`, `"type": "module"`, scripts (`test`, `typecheck`), no `dependencies`.
- `clients/typescript/tsconfig.json` — **NEW.** Editor/optional-typecheck config (`noEmit`, strict, `allowImportingTsExtensions`).
- `clients/typescript/src/index.ts` — **NEW.** Framing, `resolvePath`, `connectWithRetry`, `Warden` client, `WardenError`/`WardenUnavailable`. One module, one responsibility.
- `clients/typescript/test/client.test.ts` — **NEW.** Unit (framing, path, retry schedule) + in-process fake-daemon request/verb/error tests. Always run.
- `clients/typescript/test/integration.test.ts` — **NEW.** Gated test against the real `zig-out/bin/warden`.
- `clients/typescript/README.md` — **NEW.** Usage, install, the one-shot/wire notes.

---

## Task 1: Package scaffold

**Files:**
- Create: `clients/typescript/package.json`, `clients/typescript/tsconfig.json`, `clients/typescript/test/smoke.test.ts`

- [ ] **Step 1: Write a smoke test that proves the toolchain runs**

Create `clients/typescript/test/smoke.test.ts`:

```typescript
// warden-3yd
import test from "node:test";
import assert from "node:assert/strict";

test("node:test + native type-stripping works", () => {
  const n: number = 2 + 3;
  assert.equal(n, 5);
});
```

- [ ] **Step 2: Create `clients/typescript/package.json`**

```json
{
  "name": "@warden/sdk",
  "version": "0.1.0",
  "description": "Async client for the Warden daemon control plane",
  "type": "module",
  "exports": {
    ".": "./src/index.ts"
  },
  "scripts": {
    "test": "node --test test/*.test.ts",
    "typecheck": "tsc --noEmit"
  },
  "engines": {
    "node": ">=20"
  },
  "files": [
    "src"
  ],
  "license": "MIT"
}
```

- [ ] **Step 3: Create `clients/typescript/tsconfig.json`** (for editors / optional `npm run typecheck`; not required to run tests)

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "lib": ["ES2022"],
    "strict": true,
    "noEmit": true,
    "allowImportingTsExtensions": true,
    "verbatimModuleSyntax": true,
    "skipLibCheck": true,
    "types": []
  },
  "include": ["src/**/*.ts", "test/**/*.ts"]
}
```

- [ ] **Step 4: Run the smoke test**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: `# pass 1` / `# fail 0`.

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/package.json clients/typescript/tsconfig.json clients/typescript/test/smoke.test.ts
git commit -m "warden-3yd: TypeScript SDK package scaffold (node:test, native strip)"
```

---

## Task 2: Framing, errors, path resolution

**Files:**
- Create: `clients/typescript/src/index.ts`
- Create: `clients/typescript/test/client.test.ts`

- [ ] **Step 1: Write the failing test**

Create `clients/typescript/test/client.test.ts`:

```typescript
// warden-3yd
import test from "node:test";
import assert from "node:assert/strict";
import {
  WardenError,
  WardenUnavailable,
  encodeFrame,
  decodeFrame,
  resolvePath,
} from "../src/index.ts";

test("encodeFrame is a 4-byte BE length prefix + JSON", () => {
  const obj = { req_id: "1", action: "beam.create", payload: {} };
  const frame = encodeFrame(obj);
  const len = frame.readUInt32BE(0);
  assert.equal(len, frame.length - 4);
  assert.deepEqual(JSON.parse(frame.subarray(4).toString("utf8")), obj);
});

test("decodeFrame round-trips a body buffer", () => {
  const obj = { msg: "café — ümlaut", n: 7 };
  const frame = encodeFrame(obj);
  const body = frame.subarray(4);
  assert.deepEqual(decodeFrame(body), obj);
});

test("resolvePath: explicit arg wins, with ~ expansion", () => {
  assert.equal(resolvePath("/tmp/x.sock"), "/tmp/x.sock");
  const home = process.env.HOME!;
  assert.equal(resolvePath("~/y.sock"), `${home}/y.sock`);
});

test("resolvePath: env var used when arg omitted", () => {
  const prev = process.env.WARDEN_CTRL_SOCKET;
  process.env.WARDEN_CTRL_SOCKET = "/run/warden.sock";
  try {
    assert.equal(resolvePath(), "/run/warden.sock");
  } finally {
    if (prev === undefined) delete process.env.WARDEN_CTRL_SOCKET;
    else process.env.WARDEN_CTRL_SOCKET = prev;
  }
});

test("resolvePath: default when arg and env absent", () => {
  const prev = process.env.WARDEN_CTRL_SOCKET;
  delete process.env.WARDEN_CTRL_SOCKET;
  try {
    assert.equal(resolvePath(), `${process.env.HOME}/.warden/ctrl.sock`);
  } finally {
    if (prev !== undefined) process.env.WARDEN_CTRL_SOCKET = prev;
  }
});

test("error classes carry messages and instanceof works", () => {
  assert.ok(new WardenError("boom") instanceof Error);
  assert.equal(new WardenError("boom").message, "boom");
  assert.ok(new WardenUnavailable("nope") instanceof Error);
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: FAIL — `Cannot find module '../src/index.ts'`.

- [ ] **Step 3: Write minimal implementation**

Create `clients/typescript/src/index.ts`:

```typescript
// warden-3yd
// Async client for the Warden daemon's control plane.
//
// Distinct from any worker SDK: this is a client of the *control* socket
// (WARDEN_CTRL_SOCKET) — the same protocol wardenctl speaks — used to create
// beams and spawn/message supervised workers. Wire-compatible with the Python
// warden.Client.
import net from "node:net";
import os from "node:os";
import path from "node:path";

const DEFAULT_SOCKET = "~/.warden/ctrl.sock";
const RETRY_INITIAL_MS = 50;
const RETRY_CAP_MS = 2000;

export const VALID_RESTART = ["permanent", "transient", "temporary"] as const;
export type RestartPolicy = (typeof VALID_RESTART)[number];

export class WardenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WardenError";
  }
}

export class WardenUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WardenUnavailable";
  }
}

export function encodeFrame(obj: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(obj), "utf8");
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

export function decodeFrame(body: Buffer): any {
  return JSON.parse(body.toString("utf8"));
}

function expandHome(p: string): string {
  if (p === "~") return os.homedir();
  if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
  return p;
}

export function resolvePath(p?: string): string {
  const raw = p ?? process.env.WARDEN_CTRL_SOCKET ?? DEFAULT_SOCKET;
  return expandHome(raw);
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS (smoke + 6 framing/path/error tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/src/index.ts clients/typescript/test/client.test.ts
git commit -m "warden-3yd: framing, errors, path resolution"
```

---

## Task 3: Retry-connect helper

**Files:**
- Modify: `clients/typescript/src/index.ts` (add `connectWithRetry` + a small `Opener` type and socket opener)
- Modify: `clients/typescript/test/client.test.ts` (add tests)

- [ ] **Step 1: Write the failing test**

Append to `clients/typescript/test/client.test.ts`:

```typescript
// warden-3yd
import { connectWithRetry } from "../src/index.ts";

function fakeClock() {
  const state = { now: 0 };
  const delays: number[] = [];
  const sleep = async (ms: number) => {
    delays.push(ms);
    state.now += ms;
  };
  const now = () => state.now;
  return { sleep, now, delays };
}

test("retry backoff schedule then success (50ms→2s cap)", async () => {
  const { sleep, now, delays } = fakeClock();
  let attempts = 0;
  const open = async () => {
    attempts += 1;
    if (attempts <= 8) {
      const e: NodeJS.ErrnoException = new Error("nope");
      e.code = "ENOENT";
      throw e;
    }
    return "SOCK" as unknown as net.Socket;
  };
  const sock = await connectWithRetry(open as any, { sleep, now });
  assert.equal(sock, "SOCK");
  assert.deepEqual(delays, [50, 100, 200, 400, 800, 1600, 2000, 2000]);
});

test("ECONNREFUSED also retries", async () => {
  const { sleep, now, delays } = fakeClock();
  let attempts = 0;
  const open = async () => {
    attempts += 1;
    if (attempts === 1) {
      const e: NodeJS.ErrnoException = new Error("refused");
      e.code = "ECONNREFUSED";
      throw e;
    }
    return "OK" as any;
  };
  assert.equal(await connectWithRetry(open as any, { sleep, now }), "OK");
  assert.deepEqual(delays, [50]);
});

test("timeout raises WardenUnavailable at the boundary", async () => {
  const { sleep, now, delays } = fakeClock();
  const open = async () => {
    const e: NodeJS.ErrnoException = new Error("nope");
    e.code = "ENOENT";
    throw e;
  };
  await assert.rejects(
    connectWithRetry(open as any, { timeoutMs: 1000, sleep, now }),
    WardenUnavailable,
  );
  // deadline 1000ms: sleeps 50+100+200+400 = 750 (<1000), next check at 1550 fires
  assert.deepEqual(delays, [50, 100, 200, 400, 800]);
});

test("a non-connect error is not retried (propagates)", async () => {
  const { sleep, now } = fakeClock();
  const open = async () => {
    throw new Error("boom-other");
  };
  await assert.rejects(connectWithRetry(open as any, { sleep, now }), /boom-other/);
});

test("no timeout never raises (succeeds after many failures)", async () => {
  const { sleep, now } = fakeClock();
  let attempts = 0;
  const open = async () => {
    attempts += 1;
    if (attempts <= 50) {
      const e: NodeJS.ErrnoException = new Error("nope");
      e.code = "ENOENT";
      throw e;
    }
    return "OK" as any;
  };
  assert.equal(await connectWithRetry(open as any, { sleep, now }), "OK");
});
```

Add this import to the existing import block at the top of the test file (the `net` type is referenced in casts): add `import type net from "node:net";` near the top.

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: FAIL — `connectWithRetry` is not exported.

- [ ] **Step 3: Write minimal implementation**

Append to `clients/typescript/src/index.ts`:

```typescript
// warden-3yd
export type Opener = () => Promise<net.Socket>;

interface RetryClock {
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function connectWithRetry(
  open: Opener,
  opts: { timeoutMs?: number } & RetryClock = {},
): Promise<net.Socket> {
  const sleep = opts.sleep ?? defaultSleep;
  const now = opts.now ?? Date.now;
  const deadline = opts.timeoutMs == null ? null : now() + opts.timeoutMs;
  let delay = RETRY_INITIAL_MS;
  for (;;) {
    try {
      return await open();
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== "ENOENT" && code !== "ECONNREFUSED") throw err;
      if (deadline != null && now() >= deadline) {
        throw new WardenUnavailable(
          `warden daemon not reachable within ${opts.timeoutMs}ms`,
        );
      }
      await sleep(delay);
      delay = Math.min(delay * 2, RETRY_CAP_MS);
    }
  }
}

// warden-3yd
// Open a Unix-domain stream socket; rejects with the ENOENT/ECONNREFUSED that
// connectWithRetry treats as "daemon not up yet".
export function openUnixSocket(sockPath: string): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection({ path: sockPath });
    const onConnect = () => {
      sock.off("error", onError);
      resolve(sock);
    };
    const onError = (err: Error) => {
      sock.off("connect", onConnect);
      reject(err);
    };
    sock.once("connect", onConnect);
    sock.once("error", onError);
  });
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS (all prior + 5 retry tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/src/index.ts clients/typescript/test/client.test.ts
git commit -m "warden-3yd: exponential-backoff retry-connect + unix socket opener"
```

---

## Task 4: Client core — connect, request (one-shot), close, serialization

**Files:**
- Modify: `clients/typescript/src/index.ts` (add `readOneFrame` + `Warden` class)
- Modify: `clients/typescript/test/client.test.ts` (add an in-process fake-daemon harness + request tests)

- [ ] **Step 1: Write the failing test**

Append to `clients/typescript/test/client.test.ts`:

```typescript
// warden-3yd
import netReal from "node:net";
import os2 from "node:os";
import path2 from "node:path";
import fs from "node:fs";
import { Warden } from "../src/index.ts";

// Reads exactly one length-prefixed frame from a server-side socket.
function readServerFrame(sock: netReal.Socket): Promise<any> {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    let need = -1;
    const onData = (chunk: Buffer) => {
      buf = Buffer.concat([buf, chunk]);
      if (need < 0 && buf.length >= 4) need = buf.readUInt32BE(0);
      if (need >= 0 && buf.length >= 4 + need) {
        cleanup();
        try {
          resolve(JSON.parse(buf.subarray(4, 4 + need).toString("utf8")));
        } catch (e) {
          reject(e);
        }
      }
    };
    const onErr = (e: Error) => {
      cleanup();
      reject(e);
    };
    const cleanup = () => {
      sock.off("data", onData);
      sock.off("error", onErr);
    };
    sock.on("data", onData);
    sock.on("error", onErr);
  });
}

// Spin up an in-process one-shot "daemon": each connection gets one request,
// `responder(req, requests)` returns the response object, then the socket closes.
async function withFakeDaemon(
  responder: (req: any, all: any[]) => any | null,
  fn: (sockPath: string, requests: any[]) => Promise<void>,
): Promise<void> {
  const dir = fs.mkdtempSync(path2.join(os2.tmpdir(), "warden-tsd-"));
  const sockPath = path2.join(dir, "ctrl.sock");
  const requests: any[] = [];
  const server = netReal.createServer((sock) => {
    readServerFrame(sock)
      .then((req) => {
        requests.push(req);
        const resp = responder(req, requests);
        if (resp === null) {
          sock.destroy(); // simulate a drop with no response
          return;
        }
        sock.end(encodeFrame(resp));
      })
      .catch(() => sock.destroy());
  });
  await new Promise<void>((r) => server.listen(sockPath, () => r()));
  try {
    await fn(sockPath, requests);
  } finally {
    await new Promise<void>((r) => server.close(() => r()));
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

const okEcho = (payload: unknown) => (req: any) => ({
  req_id: req.req_id,
  ok: true,
  error: null,
  payload,
});

test("request: success returns payload, sends framed request", async () => {
  await withFakeDaemon(okEcho({ beam_id: 3 }), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    const payload = await (c as any).request("beam.create", {});
    assert.deepEqual(payload, { beam_id: 3 });
    assert.equal(requests.length, 1);
    assert.equal(requests[0].action, "beam.create");
    assert.equal(requests[0].req_id, "1");
    await c.close();
  });
});

test("request: req_id increments across the one-shot reconnects", async () => {
  await withFakeDaemon(okEcho({}), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await (c as any).request("proc.list", {});
    await (c as any).request("proc.list", {});
    assert.deepEqual(requests.map((r) => r.req_id), ["1", "2"]);
    await c.close();
  });
});

test("request: ok=false maps to WardenError with the server message", async () => {
  const responder = (req: any) => ({
    req_id: req.req_id,
    ok: false,
    error: "unknown beam",
    payload: null,
  });
  await withFakeDaemon(responder, async (sockPath) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await assert.rejects(
      (c as any).request("proc.list", { beam: 99 }),
      (e: Error) => e instanceof WardenError && /unknown beam/.test(e.message),
    );
    await c.close();
  });
});

test("request: a mid-request drop rejects and the next call reconnects", async () => {
  let first = true;
  const responder = (req: any) => {
    if (first) {
      first = false;
      return null; // drop the first request with no response
    }
    return { req_id: req.req_id, ok: true, error: null, payload: { ok: 1 } };
  };
  await withFakeDaemon(responder, async (sockPath) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await assert.rejects((c as any).request("proc.list", {}));
    // next call transparently reconnects and succeeds
    const payload = await (c as any).request("proc.list", {});
    assert.deepEqual(payload, { ok: 1 });
    await c.close();
  });
});

test("concurrent requests are serialized (req_ids 1..N in order)", async () => {
  await withFakeDaemon(okEcho({}), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await Promise.all(
      Array.from({ length: 5 }, () => (c as any).request("proc.list", {})),
    );
    assert.deepEqual(requests.map((r) => r.req_id), ["1", "2", "3", "4", "5"]);
    await c.close();
  });
});

test("close() is idempotent", async () => {
  await withFakeDaemon(okEcho({}), async (sockPath) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await c.close();
    await c.close(); // must not throw
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: FAIL — `Warden` is not exported.

- [ ] **Step 3: Write minimal implementation**

Append to `clients/typescript/src/index.ts`:

```typescript
// warden-3yd
// Read exactly one length-prefixed JSON frame from a client socket, accumulating
// chunks until the full frame arrives. Rejects if the socket closes/errs first.
function readOneFrame(sock: net.Socket): Promise<any> {
  return new Promise((resolve, reject) => {
    let buf = Buffer.alloc(0);
    let need = -1;
    const onData = (chunk: Buffer) => {
      buf = Buffer.concat([buf, chunk]);
      if (need < 0 && buf.length >= 4) need = buf.readUInt32BE(0);
      if (need >= 0 && buf.length >= 4 + need) {
        cleanup();
        try {
          resolve(decodeBody(buf.subarray(4, 4 + need)));
        } catch (e) {
          reject(e);
        }
      }
    };
    const onErr = (e: Error) => {
      cleanup();
      reject(e);
    };
    const onClose = () => {
      cleanup();
      reject(new Error("warden: connection closed before a full response frame"));
    };
    const cleanup = () => {
      sock.off("data", onData);
      sock.off("error", onErr);
      sock.off("close", onClose);
    };
    sock.on("data", onData);
    sock.on("error", onErr);
    sock.on("close", onClose);
  });
}

export interface ConnectOptions {
  path?: string;
  /** Max seconds to wait for the daemon (undefined = forever). */
  timeout?: number;
  /** Test seam: override how a socket is opened. */
  _open?: Opener;
}

// warden-3yd
export class Warden {
  #path: string;
  #opener: Opener;
  #sock: net.Socket | null = null;
  #connected = false;
  #counter = 0;
  // promise-chain mutex: each request waits for the previous to settle.
  #tail: Promise<unknown> = Promise.resolve();

  private constructor(sockPath: string, opener: Opener) {
    this.#path = sockPath;
    this.#opener = opener;
  }

  static async connect(opts: ConnectOptions = {}): Promise<Warden> {
    const sockPath = resolvePath(opts.path);
    const opener = opts._open ?? (() => openUnixSocket(sockPath));
    const c = new Warden(sockPath, opener);
    await c.#ensureConnected(opts.timeout);
    return c;
  }

  async #ensureConnected(timeoutSec?: number): Promise<void> {
    if (this.#connected && this.#sock) return;
    if (this.#sock) {
      this.#sock.destroy();
      this.#sock = null;
    }
    const timeoutMs = timeoutSec == null ? undefined : timeoutSec * 1000;
    this.#sock = await connectWithRetry(this.#opener, { timeoutMs });
    this.#connected = true;
  }

  #markDisconnected(): void {
    this.#connected = false;
    if (this.#sock) {
      this.#sock.destroy();
      this.#sock = null;
    }
  }

  #writeAll(frame: Buffer): Promise<void> {
    return new Promise((resolve, reject) => {
      this.#sock!.write(frame, (err) => (err ? reject(err) : resolve()));
    });
  }

  // Serialize: chain onto #tail so only one request runs at a time, regardless
  // of whether the previous one resolved or rejected.
  #runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.#tail.then(fn, fn);
    this.#tail = result.then(
      () => {},
      () => {},
    );
    return result;
  }

  /** @internal - exposed for tests; verb methods are the public surface. */
  request(action: string, payload: unknown): Promise<any> {
    return this.#runExclusive(async () => {
      await this.#ensureConnected();
      this.#counter += 1;
      const reqId = String(this.#counter);
      const frame = encodeFrame({ req_id: reqId, action, payload });
      let resp: any;
      try {
        await this.#writeAll(frame);
        resp = await readOneFrame(this.#sock!);
      } catch (err) {
        this.#markDisconnected();
        throw new Error(
          `warden connection lost during '${action}': ${(err as Error).message}`,
        );
      }
      // one-shot protocol: the daemon closes the socket after this response.
      this.#markDisconnected();
      if (resp.req_id !== reqId) {
        throw new WardenError(
          `req_id mismatch: sent ${reqId}, got ${JSON.stringify(resp.req_id)}`,
        );
      }
      if (!resp.ok) {
        throw new WardenError(resp.error ?? "unknown error");
      }
      return resp.payload;
    });
  }

  async close(): Promise<void> {
    this.#markDisconnected();
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS (all prior + 6 request tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/src/index.ts clients/typescript/test/client.test.ts
git commit -m "warden-3yd: Warden client core — one-shot request, serialization, reconnect"
```

---

## Task 5: Verb methods (camelCase)

**Files:**
- Modify: `clients/typescript/src/index.ts` (add verb methods + a `#field` helper to `Warden`)
- Modify: `clients/typescript/test/client.test.ts` (add tests)

- [ ] **Step 1: Write the failing test**

Append to `clients/typescript/test/client.test.ts`:

```typescript
// warden-3yd
test("beamCreate: default payload {} and returns beam_id", async () => {
  await withFakeDaemon(okEcho({ beam_id: 4 }), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    assert.equal(await c.beamCreate(), 4);
    assert.deepEqual(requests[0].payload, {});
    await c.close();
  });
});

test("beamCreate: explicit beam in payload", async () => {
  await withFakeDaemon(okEcho({ beam_id: 9 }), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    assert.equal(await c.beamCreate(9), 9);
    assert.deepEqual(requests[0].payload, { beam: 9 });
    await c.close();
  });
});

test("procSpawn: builds payload, returns pid", async () => {
  await withFakeDaemon(okEcho({ pid: "2/5" }), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    const pid = await c.procSpawn(["python3", "w.py"], {
      beam: 2,
      parent: "2/1",
      restart: "permanent",
    });
    assert.equal(pid, "2/5");
    assert.equal(requests[0].action, "proc.spawn");
    assert.deepEqual(requests[0].payload, {
      cmd: ["python3", "w.py"],
      beam: 2,
      parent: "2/1",
      restart: "permanent",
    });
    await c.close();
  });
});

test("procSpawn: omits unset optionals; string cmd becomes single element", async () => {
  await withFakeDaemon(okEcho({ pid: "1/3" }), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await c.procSpawn("worker.py");
    assert.deepEqual(requests[0].payload, { cmd: ["worker.py"] });
    await c.close();
  });
});

test("procSpawn: bad restart throws TypeError before any I/O", async () => {
  await withFakeDaemon(okEcho({}), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await assert.rejects(
      // @ts-expect-error intentionally invalid value
      c.procSpawn(["echo"], { restart: "bogus" }),
      TypeError,
    );
    assert.equal(requests.length, 0); // nothing sent
    await c.close();
  });
});

test("procSend: returns void; payload uses wire field 'type'", async () => {
  await withFakeDaemon(okEcho(null), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    const r = await c.procSend("1/2", "req.ping", { x: 1 });
    assert.equal(r, undefined);
    assert.equal(requests[0].action, "proc.send");
    assert.deepEqual(requests[0].payload, {
      pid: "1/2",
      type: "req.ping",
      body: { x: 1 },
    });
    await c.close();
  });
});

test("procCall: returns {type, body}; default timeout_ms 5000", async () => {
  await withFakeDaemon(
    okEcho({ type: "res.ok", body: 55 }),
    async (sockPath, requests) => {
      const c = await Warden.connect({ path: sockPath, timeout: 5 });
      const reply = await c.procCall("1/2", "req.fib", 10);
      assert.deepEqual(reply, { type: "res.ok", body: 55 });
      assert.deepEqual(requests[0].payload, {
        pid: "1/2",
        type: "req.fib",
        body: 10,
        timeout_ms: 5000,
      });
      await c.close();
    },
  );
});

test("procCall: explicit timeoutMs flows to wire timeout_ms", async () => {
  await withFakeDaemon(
    okEcho({ type: "res.ok", body: 1 }),
    async (sockPath, requests) => {
      const c = await Warden.connect({ path: sockPath, timeout: 5 });
      await c.procCall("1/2", "req.x", null, { timeoutMs: 2000 });
      assert.equal(requests[0].payload.timeout_ms, 2000);
      await c.close();
    },
  );
});

test("procList: returns processes array; passes beam filter", async () => {
  const procs = [
    { beam: 1, pid: 2, kind: "foreign", state: "running", policy: "permanent", last_active_ms: 12 },
  ];
  await withFakeDaemon(okEcho({ processes: procs }), async (sockPath, requests) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    assert.deepEqual(await c.procList(1), procs);
    assert.deepEqual(requests[0].payload, { beam: 1 });
    await c.close();
  });
});

test("malformed response (missing key) raises WardenError", async () => {
  await withFakeDaemon(okEcho({}), async (sockPath) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await assert.rejects(c.beamCreate(), WardenError); // payload {} has no beam_id
    await c.close();
  });
});
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: FAIL — `c.beamCreate` is not a function.

- [ ] **Step 3: Write minimal implementation**

In `clients/typescript/src/index.ts`, add these members to the `Warden` class (place after `request`, before `close`):

```typescript
  // warden-3yd
  #field<T>(result: any, key: string, action: string): T {
    if (result == null || typeof result !== "object" || !(key in result)) {
      throw new WardenError(
        `${action}: malformed response, missing '${key}': ${JSON.stringify(result)}`,
      );
    }
    return result[key] as T;
  }

  async beamCreate(beam?: number): Promise<number> {
    const payload = beam == null ? {} : { beam };
    const result = await this.request("beam.create", payload);
    return this.#field<number>(result, "beam_id", "beam.create");
  }

  async procSpawn(
    cmd: string | string[],
    opts: { beam?: number; parent?: string; restart?: RestartPolicy } = {},
  ): Promise<string> {
    const { beam, parent, restart } = opts;
    if (restart != null && !VALID_RESTART.includes(restart)) {
      throw new TypeError(
        `invalid restart policy ${JSON.stringify(restart)}; ` +
          `expected one of ${JSON.stringify(VALID_RESTART)}`,
      );
    }
    const cmdArr = typeof cmd === "string" ? [cmd] : [...cmd];
    const payload: Record<string, unknown> = { cmd: cmdArr };
    if (beam != null) payload.beam = beam;
    if (parent != null) payload.parent = parent;
    if (restart != null) payload.restart = restart;
    const result = await this.request("proc.spawn", payload);
    return this.#field<string>(result, "pid", "proc.spawn");
  }

  async procSend(pid: string, msgType: string, body: unknown): Promise<void> {
    await this.request("proc.send", { pid, type: msgType, body });
  }

  async procCall(
    pid: string,
    msgType: string,
    body: unknown,
    opts: { timeoutMs?: number } = {},
  ): Promise<{ type: string; body: unknown }> {
    const timeout_ms = opts.timeoutMs ?? 5000;
    return (await this.request("proc.call", {
      pid,
      type: msgType,
      body,
      timeout_ms,
    })) as { type: string; body: unknown };
  }

  async procList(beam?: number): Promise<any[]> {
    const payload = beam == null ? {} : { beam };
    const result = await this.request("proc.list", payload);
    return this.#field<any[]>(result, "processes", "proc.list");
  }
```

Note: the wire field is `type` (matching the daemon); the method parameter is `msgType` to avoid the `type` keyword/builtin overlap — consistent with the Python SDK's `msg_type`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS (all prior + 10 verb tests).

- [ ] **Step 5: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/src/index.ts clients/typescript/test/client.test.ts
git commit -m "warden-3yd: camelCase verb methods + malformed-response guard"
```

---

## Task 6: Gated integration test against the real daemon

**Files:**
- Create: `clients/typescript/test/integration.test.ts`

Mirrors the Python integration test: drives the real `zig-out/bin/warden`, reuses `examples/live_demo/math_worker.py` (`req.fib` → `res.ok` with the Fibonacci number). Gated — skips if the binary / `python3` / worker is absent. Targets the **primary beam** (`beamCreate(1)`) because `proc.spawn` on a freshly-minted beam currently fails (tracked: warden-95s), and reconstructs the compound pid from `proc.list`'s split `beam`/`pid` ints (warden-36j). Places the socket in a sub-directory the daemon creates itself (warden-w6n).

- [ ] **Step 1: Write the test**

Create `clients/typescript/test/integration.test.ts`:

```typescript
// warden-3yd
import test from "node:test";
import assert from "node:assert/strict";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import fs from "node:fs";
import { spawn, type ChildProcess } from "node:child_process";
import { Warden, WardenUnavailable } from "../src/index.ts";

const REPO_ROOT = path.resolve(import.meta.dirname, "..", "..", "..");
const DAEMON = path.join(REPO_ROOT, "zig-out", "bin", "warden");
const WORKER = path.join(REPO_ROOT, "examples", "live_demo", "math_worker.py");

function daemonAvailable(): boolean {
  try {
    fs.accessSync(DAEMON, fs.constants.X_OK);
    fs.accessSync(WORKER);
    return true;
  } catch {
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitForSocket(p: string, timeoutMs = 5000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fs.existsSync(p)) return true;
    await sleep(20);
  }
  return false;
}

type Daemon = { proc: ChildProcess; sock: string; dir: string };

function startDaemon(): Daemon {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "warden-tsi-"));
  // socket in a subdir the daemon creates itself (avoids warden-w6n)
  const sock = path.join(dir, "sockets", "ctrl.sock");
  const logDir = path.join(dir, "logs");
  fs.mkdirSync(logDir, { recursive: true });
  const proc = spawn(DAEMON, {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      WARDEN_CTRL_SOCKET: sock,
      WARDEN_BEAM_ID: "1",
      WARDEN_LOG_DIR: logDir,
    },
    stdio: "ignore",
  });
  return { proc, sock, dir };
}

async function stopDaemon(d: Daemon): Promise<void> {
  if (d.proc.exitCode === null && d.proc.signalCode === null) {
    d.proc.kill("SIGTERM");
    await new Promise<void>((resolve) => {
      const t = setTimeout(() => {
        d.proc.kill("SIGKILL");
        resolve();
      }, 5000);
      d.proc.once("exit", () => {
        clearTimeout(t);
        resolve();
      });
    });
  }
  fs.rmSync(d.dir, { recursive: true, force: true });
}

test(
  "integration: spawn → call → list round-trip",
  { skip: daemonAvailable() ? false : "zig-out/bin/warden or worker absent" },
  async () => {
    const d = startDaemon();
    try {
      assert.ok(await waitForSocket(d.sock), "daemon never bound the socket");
      const c = await Warden.connect({ path: d.sock, timeout: 10 });
      try {
        const beam = await c.beamCreate(1); // primary beam (warden-95s)
        assert.equal(typeof beam, "number");

        const pid = await c.procSpawn(["python3", WORKER], {
          beam,
          restart: "permanent",
        });
        assert.match(pid, /^\d+\/\d+$/);

        const reply = await c.procCall(pid, "req.fib", 10, { timeoutMs: 5000 });
        assert.equal(reply.body, 55); // fib(10)

        const procs = await c.procList(beam);
        // proc.list rows carry split beam/pid ints (warden-36j)
        assert.ok(procs.some((p) => `${p.beam}/${p.pid}` === pid));
      } finally {
        await c.close();
      }
    } finally {
      await stopDaemon(d);
    }
  },
);

test(
  "integration: retry-connect blocks until the daemon comes up",
  { skip: daemonAvailable() ? false : "zig-out/bin/warden absent" },
  async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "warden-tsi2-"));
    const sock = path.join(dir, "sockets", "ctrl.sock");
    let started: Daemon | null = null;
    try {
      const connectP = Warden.connect({ path: sock, timeout: 10 });
      await sleep(300);
      let settled = false;
      void connectP.then(
        () => (settled = true),
        () => (settled = true),
      );
      await sleep(0);
      assert.equal(settled, false, "connect should still be blocking");

      // start the daemon on the same socket
      const logDir = path.join(dir, "logs");
      fs.mkdirSync(logDir, { recursive: true });
      const proc = spawn(DAEMON, {
        cwd: REPO_ROOT,
        env: {
          ...process.env,
          WARDEN_CTRL_SOCKET: sock,
          WARDEN_BEAM_ID: "1",
          WARDEN_LOG_DIR: logDir,
        },
        stdio: "ignore",
      });
      started = { proc, sock, dir };

      const c = await connectP;
      try {
        assert.equal(typeof (await c.beamCreate(1)), "number");
      } finally {
        await c.close();
      }
    } finally {
      if (started) await stopDaemon(started);
      else fs.rmSync(dir, { recursive: true, force: true });
    }
  },
);

test("integration: connect timeout raises WardenUnavailable when no daemon", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "warden-tsi3-"));
  const sock = path.join(dir, "ctrl.sock");
  try {
    await assert.rejects(
      Warden.connect({ path: sock, timeout: 0.5 }),
      WardenUnavailable,
    );
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
```

- [ ] **Step 2: Run the integration test**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/integration.test.ts`
Expected: PASS (3 tests) since `zig-out/bin/warden` exists; the round-trip must report `fib == 55`. If `import.meta.dirname` is undefined on the running Node, replace `REPO_ROOT` with a `fileURLToPath(import.meta.url)`-based resolution (Node ≥ 20.11 has `import.meta.dirname`; Node 22.22 has it). If a test FAILS (not skips), investigate against the real daemon — do NOT weaken assertions to force a pass; report a genuine mismatch as DONE_WITH_CONCERNS/BLOCKED.

- [ ] **Step 3: Run the whole TS suite**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS — unit + fake-daemon + integration all green.

- [ ] **Step 4: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/test/integration.test.ts
git commit -m "warden-3yd: gated integration test driving the real daemon"
```

---

## Task 7: README

**Files:**
- Create: `clients/typescript/README.md`

- [ ] **Step 1: Write the README**

Create `clients/typescript/README.md`:

````markdown
# @warden/sdk

Async TypeScript client for the [Warden](../../README.md) daemon's control
plane. Connect to the daemon over its Unix control socket to create beams and
spawn / message supervised workers. Wire-compatible with the Python
`warden.Client`.

## Requirements

- Node ≥ 20 (uses `node:net`, `node:test`, and native TypeScript type-stripping).
- No runtime dependencies.

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

// Or with explicit resource management (Node ≥ 20):
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
````

- [ ] **Step 2: Sanity-check the suite still passes**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS (README change doesn't affect tests).

- [ ] **Step 3: Commit**

```bash
cd /Users/leslierussell/repo/warden
git add clients/typescript/README.md
git commit -m "warden-3yd: README for @warden/sdk"
```

---

## Task 8: Verify, merge, close

**Files:** none (verification + git)

- [ ] **Step 1: Full TS suite**

Run: `cd /Users/leslierussell/repo/warden/clients/typescript && node --test test/*.test.ts`
Expected: PASS, zero failures (integration green against the live daemon, or skipped if absent).

- [ ] **Step 2: Confirm the Python SDK still passes (no cross-contamination)**

Run: `cd /Users/leslierussell/repo/warden && python3 -m unittest warden.tests.test_client_unit warden.tests.test_client_integration`
Expected: PASS — this branch must not have touched the Python SDK.

- [ ] **Step 3: Confirm only intended files changed**

Run: `cd /Users/leslierussell/repo/warden && git diff --name-only master..HEAD`
Expected: only `clients/typescript/**` and `docs/superpowers/plans/2026-06-10-warden-typescript-client-sdk.md`. No `.zig-cache`, `zig-out`, `node_modules`, or other artifacts in any commit.

- [ ] **Step 4: Merge to master**

```bash
cd /Users/leslierussell/repo/warden
git checkout master
git merge --no-ff warden-3yd -m "warden-3yd: TypeScript control-plane client SDK (@warden/sdk)"
git branch -d warden-3yd
```

- [ ] **Step 5: Close the bead**

```bash
cd /Users/leslierussell/repo/warden
bd close warden-3yd --reason="Implemented @warden/sdk TypeScript control-plane client: framing, blocking retry-connect (50ms→2s, forever default, WardenUnavailable on timeout), serialized one-request-per-connection requests with transparent reconnect, camelCase verbs (beamCreate/procSpawn[restart-validated]/procSend/procCall/procList), Symbol.asyncDispose, README. Native type-stripping + node:test (no build/deps). Unit + fake-daemon + gated integration tests (live daemon, fib(10)==55). Wire-compatible with the Python warden.Client."
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** retry contract (Task 3), serialized one-shot `request` + reconnect (Task 4), all five camelCase verbs + client-side `restart` validation (Task 5), `WardenError`/`WardenUnavailable` (Tasks 2–5), `Symbol.asyncDispose` + README (Tasks 4, 7), gated integration incl. retry-before-daemon (Task 6). Deviations (native strip instead of `tsc` build; two test files) are documented at the top.
- **Native-strip discipline:** no `enum`/`namespace`/parameter-properties; `import type` for type-only imports; explicit `.ts` extensions on local imports. If `node --test` reports a `SyntaxError` about unsupported syntax, that constraint was violated.
- **Timeout units:** `connect({ timeout })` is in **seconds** (matches the Python conceptual API); `procCall({ timeoutMs })` is in **milliseconds** (matches the wire `timeout_ms`). Keep them distinct.
- **Wire-field vs param:** JSON field `type`, method param `msgType`.
- **proc.list shape:** rows are `{ beam, pid, ... }` separate ints — never assume a `"b/p"` string there.
