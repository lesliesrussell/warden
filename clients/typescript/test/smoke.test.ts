// warden-3yd
import test from "node:test";
import assert from "node:assert/strict";

test("node:test + native type-stripping works", () => {
  const n: number = 2 + 3;
  assert.equal(n, 5);
});
