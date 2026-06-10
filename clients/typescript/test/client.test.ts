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

// warden-3yd
test("one-shot: each request opens a fresh connection", async () => {
  let connections = 0;
  const dir = fs.mkdtempSync(path2.join(os2.tmpdir(), "warden-reuse-"));
  const sockPath = path2.join(dir, "ctrl.sock");
  const server = netReal.createServer((sock) => {
    connections += 1;
    readServerFrame(sock)
      .then((req) =>
        sock.end(encodeFrame({ req_id: req.req_id, ok: true, error: null, payload: {} })),
      )
      .catch(() => sock.destroy());
  });
  await new Promise<void>((r) => server.listen(sockPath, () => r()));
  try {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    await (c as any).request("a", {});
    await (c as any).request("b", {});
    await (c as any).request("c", {});
    assert.equal(connections, 3); // connect's socket serves req a, then b & c reconnect
    await c.close();
  } finally {
    await new Promise<void>((r) => server.close(() => r()));
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("close() during an in-flight request rejects it cleanly and the client recovers", async () => {
  let firstSock: any = null;
  const dir = fs.mkdtempSync(path2.join(os2.tmpdir(), "warden-closeflight-"));
  const sockPath = path2.join(dir, "ctrl.sock");
  const server = netReal.createServer((sock) => {
    if (firstSock === null) {
      firstSock = sock; // never respond to the first connection
      return;
    }
    readServerFrame(sock)
      .then((req) =>
        sock.end(
          encodeFrame({ req_id: req.req_id, ok: true, error: null, payload: { ok: 2 } }),
        ),
      )
      .catch(() => sock.destroy());
  });
  await new Promise<void>((r) => server.listen(sockPath, () => r()));
  const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
  try {
    const c = await Warden.connect({ path: sockPath, timeout: 5 });
    const inflight = (c as any).request("a", {}); // hangs (server never responds)
    await wait(50); // let it reach readOneFrame
    await c.close(); // destroys the captured socket
    await assert.rejects(inflight); // in-flight request rejects, no crash
    const r = await (c as any).request("b", {}); // recovers on a fresh connection
    assert.deepEqual(r, { ok: 2 });
  } finally {
    if (firstSock) firstSock.destroy(); // never-answered socket would block server.close()
    await new Promise<void>((r) => server.close(() => r()));
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
