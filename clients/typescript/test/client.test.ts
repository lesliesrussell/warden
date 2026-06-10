// warden-3yd
import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import type net from "node:net";
import {
  WardenError,
  WardenUnavailable,
  encodeFrame,
  decodeBody,
  resolvePath,
  connectWithRetry,
} from "../src/index.ts";

test("encodeFrame is a 4-byte BE length prefix + JSON", () => {
  const obj = { req_id: "1", action: "beam.create", payload: {} };
  const frame = encodeFrame(obj);
  const len = frame.readUInt32BE(0);
  assert.equal(len, frame.length - 4);
  assert.deepEqual(JSON.parse(frame.subarray(4).toString("utf8")), obj);
});

test("encodeFrame length prefix counts UTF-8 bytes, not characters", () => {
  const obj = { s: "café — ümlaut" }; // multibyte: byte length > char length
  const frame = encodeFrame(obj);
  const expectedBytes = Buffer.byteLength(JSON.stringify(obj), "utf8");
  assert.equal(frame.readUInt32BE(0), expectedBytes);
  assert.ok(expectedBytes > JSON.stringify(obj).length); // sanity: it IS multibyte
});

test("decodeBody round-trips a body buffer", () => {
  const obj = { msg: "café — ümlaut", n: 7 };
  const frame = encodeFrame(obj);
  const body = frame.subarray(4);
  assert.deepEqual(decodeBody(body), obj);
});

test("resolvePath: explicit arg wins, with ~ expansion", () => {
  assert.equal(resolvePath("/tmp/x.sock"), "/tmp/x.sock");
  assert.equal(resolvePath("~/y.sock"), `${os.homedir()}/y.sock`);
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
    assert.equal(resolvePath(), `${os.homedir()}/.warden/ctrl.sock`);
  } finally {
    if (prev !== undefined) process.env.WARDEN_CTRL_SOCKET = prev;
  }
});

test("error classes carry messages and instanceof works", () => {
  assert.ok(new WardenError("boom") instanceof Error);
  assert.equal(new WardenError("boom").message, "boom");
  assert.ok(new WardenUnavailable("nope") instanceof Error);
});

// warden-3yd
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
  // sleeps 50+100+200+400+800; after the 800ms sleep clock=1550 ≥ deadline=1000 → throws
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

test("request: a non-object response frame raises WardenError", async () => {
  const responder = () => 42 as any; // sends JSON `42`, not an object
  await withFakeDaemon(responder, async (sockPath) => {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await assert.rejects((c as any).request("x", {}), WardenError);
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
