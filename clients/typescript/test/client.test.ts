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
