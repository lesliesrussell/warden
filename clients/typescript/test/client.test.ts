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
