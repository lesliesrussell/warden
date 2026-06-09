# Foreign Worker Auto-Restart Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a foreign (Python) worker crashes, `BridgeSupervisor` detects it and respawns per a per-worker restart policy, with a runaway guard and a live-adjustable reaper cadence.

**Architecture:** A `crashed` atomic flag set by each bridge's reader thread on genuine death; a per-`BridgeSupervisor` reaper thread that polls workers, classifies the exit, and applies a pure restart-decision function (policy + 3-in-5s intensity) to respawn a new PID incarnation or retire the worker. Spawn inputs are retained in a new `ManagedWorker` record. A `beam.reaper` RPC + `wardenctl renice` adjust the poll interval on the fly.

**Tech Stack:** Zig 0.16, `std.Io` threaded executor, `std.Thread`, the existing `ForeignBridge`/`BridgeSupervisor`/`ControlServer` machinery, Python 3 worker fixtures.

**Bead:** warden-dmg. **Branch:** warden-dmg. Each task ends by committing with the `warden-dmg` tag in the message and `// warden-dmg` comment blocks on new code.

**Spec:** `docs/superpowers/specs/2026-06-09-foreign-worker-auto-restart-design.md`

**Build/test commands:**
- `zig build test` — run all tests (currently 110).
- `zig test src/<file>.zig` — run one file's inline tests standalone.
- `zig build` — compile `warden` + `wardenctl`.

---

## File structure

- `src/restart.zig` (new) — pure restart-decision logic + the `RestartStrategy` parse + per-worker intensity check. Self-contained, fully unit-testable, no threads/IO.
- `src/bridge.zig` (modify) — `crashed`/`last_exit` on `ForeignBridge`; reader sets the flag; `stop()` captures the child `Term`; `ManagedWorker`; reaper thread; `reapAndMaybeRespawn`; `renice`; `spawnWorkerUnder` retains inputs + takes a `restart` arg.
- `src/control.zig` (modify) — `handleProcSpawn` parses `restart`; new `handleBeamReaper` + router entry.
- `apps/wardenctl/src/commands/renice.zig` (new) + `apps/wardenctl/src/cli.zig` (modify) — `renice` command.
- `src/foreign_restart_test.zig` (new) + `src/root.zig` (modify) — integration tests; register in the test root.

---

## Task 1: Pure restart-decision logic (`restart.zig`)

**Files:**
- Create: `src/restart.zig`
- Test: inline tests in `src/restart.zig`

- [ ] **Step 1: Write `src/restart.zig` with the pure logic and failing tests**

```zig
// warden-dmg
//
// Pure restart-decision logic for foreign workers — no threads, no I/O, so it
// is fully unit-testable. The reaper thread (bridge.zig) wires it to real
// process teardown/respawn.

const std = @import("std");
const clock = @import("clock.zig");

/// Per-worker restart policy. Mirrors the meaning of supervisor.RestartStrategy's
/// transient/temporary; `permanent` = always restart on crash.
pub const Strategy = enum {
    permanent,
    transient,
    temporary,

    /// Parse the proc.spawn "restart" field. Defaults handled by the caller.
    pub fn parse(s: []const u8) ?Strategy {
        if (std.mem.eql(u8, s, "permanent")) return .permanent;
        if (std.mem.eql(u8, s, "transient")) return .transient;
        if (std.mem.eql(u8, s, "temporary")) return .temporary;
        return null;
    }
};

/// How the previous incarnation exited, as classified from the child Term.
pub const ExitClass = enum { normal, abnormal };

/// Runaway-guard constants — identical to src/supervisor.zig for consistency.
pub const max_restarts: u32 = 3;
pub const intensity_window_ms: u64 = 5000;

/// Reaper cadence bounds (ms). Default sits well below the old 100ms.
pub const default_interval_ms: u64 = 50;
pub const min_interval_ms: u64 = 10;
pub const max_interval_ms: u64 = 2000;

pub fn clampInterval(ms: u64) u64 {
    return std.math.clamp(ms, min_interval_ms, max_interval_ms);
}

pub const Decision = enum { restart, retire };

/// Decide what to do with a crashed worker. `timestamps` is the worker's rolling
/// list of prior restart times (ms); on a `.restart` decision the caller appends
/// `now`. Pruning of out-of-window entries happens here (in place) so the
/// caller's list stays bounded.
pub fn decide(
    strategy: Strategy,
    exit: ExitClass,
    timestamps: *std.ArrayList(i64),
    allocator: std.mem.Allocator,
    now: i64,
) !Decision {
    switch (strategy) {
        .temporary => return .retire,
        .transient => if (exit == .normal) return .retire,
        .permanent => {},
    }
    // Prune timestamps outside the window.
    var write: usize = 0;
    for (timestamps.items) |ts| {
        if (now - ts < @as(i64, @intCast(intensity_window_ms))) {
            timestamps.items[write] = ts;
            write += 1;
        }
    }
    timestamps.items.len = write;

    if (timestamps.items.len >= max_restarts) return .retire;
    try timestamps.append(allocator, now);
    return .restart;
}

test "temporary never restarts" {
    var ts: std.ArrayList(i64) = .empty;
    defer ts.deinit(std.testing.allocator);
    try std.testing.expectEqual(Decision.retire, try decide(.temporary, .abnormal, &ts, std.testing.allocator, 1000));
}

test "transient retires on normal exit, restarts on crash" {
    var ts: std.ArrayList(i64) = .empty;
    defer ts.deinit(std.testing.allocator);
    try std.testing.expectEqual(Decision.retire, try decide(.transient, .normal, &ts, std.testing.allocator, 1000));
    try std.testing.expectEqual(Decision.restart, try decide(.transient, .abnormal, &ts, std.testing.allocator, 1000));
}

test "permanent restarts until 3-in-5s, then retires" {
    var ts: std.ArrayList(i64) = .empty;
    defer ts.deinit(std.testing.allocator);
    // three quick crashes -> restart, restart, restart
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1000));
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1100));
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1200));
    // fourth within window -> retire
    try std.testing.expectEqual(Decision.retire, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1300));
    // far in the future -> window cleared -> restart again
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 20000));
}

test "clampInterval bounds" {
    try std.testing.expectEqual(@as(u64, 10), clampInterval(1));
    try std.testing.expectEqual(@as(u64, 2000), clampInterval(99999));
    try std.testing.expectEqual(@as(u64, 50), clampInterval(50));
}

test "Strategy.parse" {
    try std.testing.expectEqual(Strategy.permanent, Strategy.parse("permanent").?);
    try std.testing.expectEqual(Strategy.temporary, Strategy.parse("temporary").?);
    try std.testing.expect(Strategy.parse("bogus") == null);
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `zig test src/restart.zig`
Expected: `All 5 tests passed.`

- [ ] **Step 3: Register `restart.zig` in the test root**

Modify `src/root.zig` — add after the `pub const clock` line:

```zig
// warden-dmg
pub const restart = @import("restart.zig");
```

And inside the `comptime { ... }` block, after the `_ = @import("sync.zig");` line:

```zig
// warden-dmg
_ = @import("restart.zig");
```

- [ ] **Step 4: Verify the suite still builds + passes**

Run: `zig build test`
Expected: exit 0 (now 115 tests).

- [ ] **Step 5: Commit**

```bash
git add src/restart.zig src/root.zig
git commit -m "warden-dmg: pure restart-decision logic (policy + intensity + clamp)"
```

---

## Task 2: Crash detection on `ForeignBridge`

**Files:**
- Modify: `src/bridge.zig`

- [ ] **Step 1: Add the two fields to the `ForeignBridge` struct**

In `src/bridge.zig`, in the `ForeignBridge` struct definition, after `running: std.atomic.Value(bool),` add:

```zig
    // warden-dmg: set by the reader thread when the loop exits while still
    // running (the worker died, not a deliberate stop). The reaper consumes it.
    crashed: std.atomic.Value(bool),
    // warden-dmg: how the child exited, classified from its Term during teardown.
    last_exit: @import("restart.zig").ExitClass,
```

- [ ] **Step 2: Initialize them in `initWithParent`'s returned struct**

In the `return ForeignBridge{ ... }` literal, after `.running = std.atomic.Value(bool).init(false),` add:

```zig
            .crashed = std.atomic.Value(bool).init(false),
            .last_exit = .normal,
```

- [ ] **Step 3: Set `crashed` when the reader loop exits due to death**

In `readerThread`, the `while (self.running.load(.acquire)) { ... }` loop ends when `readFrame` returns an error. Immediately after the `while` loop's closing brace (before the function returns), add:

```zig
    // warden-dmg: if the loop ended while still "running", the worker died
    // (socket drop / process exit) rather than being deliberately stopped.
    if (self.running.load(.acquire)) self.crashed.store(true, .release);
```

- [ ] **Step 4: Capture the child Term in `stop()` and classify it**

In `stop()`, replace:

```zig
        if (self.child_proc) |*child| {
            _ = child.wait(self.runtime.io) catch {};
            self.child_proc = null;
        }
```

with:

```zig
        // warden-dmg: classify the exit so the reaper can apply transient policy.
        if (self.child_proc) |*child| {
            if (child.wait(self.runtime.io)) |term| {
                self.last_exit = switch (term) {
                    .exited => |code| if (code == 0) .normal else .abnormal,
                    else => .abnormal, // signal / stopped / unknown
                };
            } else |_| {
                self.last_exit = .abnormal;
            }
            self.child_proc = null;
        }
```

- [ ] **Step 5: Verify the suite still builds + passes (regression — no behavior change yet)**

Run: `zig build test`
Expected: exit 0, 115 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/bridge.zig
git commit -m "warden-dmg: ForeignBridge crash flag + exit classification"
```

---

## Task 3: `ManagedWorker` + retain spawn inputs in `BridgeSupervisor`

**Files:**
- Modify: `src/bridge.zig`

- [ ] **Step 1: Add the `ManagedWorker` type above `BridgeSupervisor`**

Immediately before `pub const BridgeSupervisor = struct {`, add:

```zig
// warden-dmg
const restart_mod = @import("restart.zig");

// warden-dmg
/// A foreign worker under supervision: its live bridge plus everything needed
/// to respawn it after a crash, and its per-worker runaway-guard state.
pub const ManagedWorker = struct {
    bridge: *ForeignBridge,
    // Retained respawn inputs (deep-owned copies).
    cmd: [][]u8,
    log_dir: []u8,
    storage_base: []u8,
    parent_pid: ?Pid,
    strategy: restart_mod.Strategy,
    // Runaway guard (per worker).
    restart_timestamps: std.ArrayList(i64),
    restart_count: u32,
    retired: bool,

    fn freeOwned(self: *ManagedWorker, allocator: std.mem.Allocator) void {
        for (self.cmd) |a| allocator.free(a);
        allocator.free(self.cmd);
        allocator.free(self.log_dir);
        allocator.free(self.storage_base);
        self.restart_timestamps.deinit(allocator);
    }
};

// warden-dmg: deep-copy an argv into owned memory.
fn dupeCmd(allocator: std.mem.Allocator, cmd: []const []const u8) ![][]u8 {
    const out = try allocator.alloc([]u8, cmd.len);
    var n: usize = 0;
    errdefer {
        for (out[0..n]) |a| allocator.free(a);
        allocator.free(out);
    }
    for (cmd) |a| {
        out[n] = try allocator.dupe(u8, a);
        n += 1;
    }
    return out;
}
```

- [ ] **Step 2: Change the `BridgeSupervisor` field from `bridges` to `workers`**

Replace `bridges: std.ArrayList(*ForeignBridge),` with:

```zig
    workers: std.ArrayList(*ManagedWorker),
    // warden-dmg: reaper cadence (ms), adjustable via renice().
    reaper_interval_ms: std.atomic.Value(u64),
    reaper_stopping: std.atomic.Value(bool),
    reaper_thread: ?std.Thread,
```

In `init`, replace `.bridges = .empty,` with:

```zig
            .workers = .empty,
            .reaper_interval_ms = std.atomic.Value(u64).init(restart_mod.default_interval_ms),
            .reaper_stopping = std.atomic.Value(bool).init(false),
            .reaper_thread = null,
```

- [ ] **Step 3: Update `deinit`, `findBridge`, and `spawnWorkerUnder` to use `workers`**

Replace `deinit`'s body with (reaper teardown comes in Task 4; for now just iterate workers):

```zig
    pub fn deinit(self: *BridgeSupervisor) void {
        for (self.workers.items) |w| {
            w.bridge.deinit();
            self.allocator.destroy(w.bridge);
            w.freeOwned(self.allocator);
            self.allocator.destroy(w);
        }
        self.workers.deinit(self.allocator);
    }
```

Replace `findBridge`'s loop:

```zig
    pub fn findBridge(self: *BridgeSupervisor, pid: Pid) ?*ForeignBridge {
        for (self.workers.items) |w| {
            if (w.bridge.pid.beam == pid.beam and w.bridge.pid.proc == pid.proc) return w.bridge;
        }
        return null;
    }
```

Replace `spawnWorker` and `spawnWorkerUnder`. `spawnWorker` keeps its signature (defaults strategy to permanent); `spawnWorkerUnder` gains a `strategy` parameter and builds a `ManagedWorker`:

```zig
    pub fn spawnWorker(
        self: *BridgeSupervisor,
        cmd: []const []const u8,
        log_dir: []const u8,
        storage_base: []const u8,
    ) !Pid {
        return self.spawnWorkerUnder(cmd, log_dir, storage_base, null, .permanent);
    }

    // warden-3cn, warden-dmg
    pub fn spawnWorkerUnder(
        self: *BridgeSupervisor,
        cmd: []const []const u8,
        log_dir: []const u8,
        storage_base: []const u8,
        parent_pid: ?Pid,
        strategy: restart_mod.Strategy,
    ) !Pid {
        const w = try self.allocator.create(ManagedWorker);
        errdefer self.allocator.destroy(w);

        const cmd_owned = try dupeCmd(self.allocator, cmd);
        errdefer {
            for (cmd_owned) |a| self.allocator.free(a);
            self.allocator.free(cmd_owned);
        }
        const log_owned = try self.allocator.dupe(u8, log_dir);
        errdefer self.allocator.free(log_owned);
        const store_owned = try self.allocator.dupe(u8, storage_base);
        errdefer self.allocator.free(store_owned);

        const bridge = try self.allocator.create(ForeignBridge);
        errdefer self.allocator.destroy(bridge);
        bridge.* = try ForeignBridge.initWithParent(self.allocator, self.runtime, cmd, log_dir, storage_base, parent_pid);
        errdefer bridge.deinit();
        try bridge.start(cmd);

        w.* = .{
            .bridge = bridge,
            .cmd = cmd_owned,
            .log_dir = log_owned,
            .storage_base = store_owned,
            .parent_pid = parent_pid,
            .strategy = strategy,
            .restart_timestamps = .empty,
            .restart_count = 0,
            .retired = false,
        };
        try self.workers.append(self.allocator, w);
        return bridge.pid;
    }
```

- [ ] **Step 4: Update every `spawnWorkerUnder` caller for the new `strategy` arg**

The signature gained a 5th parameter, so all callers must pass one or the build
breaks. Find them:

Run: `grep -rn 'spawnWorkerUnder\|\.bridges' src/ apps/`

- `src/control.zig` `handleProcSpawn` calls `sup.spawnWorkerUnder(cmd.items, log_dir, store_base, parent_pid)` — change it to pass `.permanent` for now (Task 5 replaces it with the parsed strategy):

```zig
    const pid = try sup.spawnWorkerUnder(cmd.items, log_dir, store_base, parent_pid, .permanent);
```

- Any other caller found (tests, etc.): if it's `spawnWorker` (4-arg) leave it — that wrapper is unchanged. If it's a direct `spawnWorkerUnder`, append `, .permanent`.
- Any `.bridges` references: as of this writing only `bridge.zig` used `bridges`, and Task 3 replaced them. If `grep` shows a stray read-only iteration, update it to `.workers` with `w.bridge`.

- [ ] **Step 5: Verify build + tests (regression)**

Run: `zig build test`
Expected: exit 0, 115 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/bridge.zig
git commit -m "warden-dmg: ManagedWorker — retain spawn inputs + per-worker policy"
```

---

## Task 4: Reaper thread + respawn

**Files:**
- Modify: `src/bridge.zig`

- [ ] **Step 1: Ensure `bridge.zig` imports `clock`, then add the reaper methods**

`bridge.zig` does not yet import `clock` (the reaper uses `clock.nowMs` and
`clock.sleepNs`). Add it near the top imports, after `const sync = @import("sync.zig");`:

```zig
const clock = @import("clock.zig");
```

Then add these methods inside `BridgeSupervisor` (after `spawnWorkerUnder`):

```zig
    // warden-dmg: adjust the reaper poll interval on the fly. Returns the
    // clamped value actually applied.
    pub fn renice(self: *BridgeSupervisor, interval_ms: u64) u64 {
        const v = restart_mod.clampInterval(interval_ms);
        self.reaper_interval_ms.store(v, .release);
        return v;
    }

    // warden-dmg: start the reaper. Call once after init.
    pub fn startReaper(self: *BridgeSupervisor) !void {
        self.reaper_thread = try std.Thread.spawn(.{}, reaperLoop, .{self});
    }

    // warden-dmg: background loop — poll workers, respawn crashed ones.
    fn reaperLoop(self: *BridgeSupervisor) void {
        while (!self.reaper_stopping.load(.acquire)) {
            const iv = self.reaper_interval_ms.load(.acquire);
            clock.sleepNs(iv * std.time.ns_per_ms);
            if (self.reaper_stopping.load(.acquire)) break;
            for (self.workers.items) |w| {
                if (w.retired) continue;
                if (!w.bridge.crashed.load(.acquire)) continue;
                self.reapAndMaybeRespawn(w) catch {};
            }
        }
    }

    // warden-dmg: tear down a crashed worker and respawn or retire per policy.
    fn reapAndMaybeRespawn(self: *BridgeSupervisor, w: *ManagedWorker) !void {
        const old = w.bridge;
        const old_pid = old.pid;

        // Reap the dead incarnation (shutdown -> join -> close -> wait), which
        // sets old.last_exit. stop() is idempotent and null-guarded.
        old.stop() catch {};
        const exit_class = old.last_exit;

        const now = clock.nowMs();
        const decision = try restart_mod.decide(
            w.strategy, exit_class, &w.restart_timestamps, self.allocator, now,
        );

        if (decision == .retire) {
            // Log the give-up via the old logger before we free it.
            old.ctx.logger.note("worker retired — not restarting", null) catch {};
            old.ctx.runtime.registry.transition(old_pid, .exiting) catch {};
            old.deinit();
            self.allocator.destroy(old);
            w.bridge = undefined; // never read again: w.retired gates it
            w.retired = true;
            return;
        }

        // Restart: tear down old, mark its registry entry dead, spawn a new
        // incarnation with a fresh PID.
        old.ctx.runtime.registry.transition(old_pid, .exiting) catch {};
        old.deinit();
        self.allocator.destroy(old);

        const nb = try self.allocator.create(ForeignBridge);
        errdefer self.allocator.destroy(nb);
        nb.* = try ForeignBridge.initWithParent(
            self.allocator, self.runtime, w.cmd, w.log_dir, w.storage_base, w.parent_pid,
        );
        errdefer nb.deinit();
        try nb.start(w.cmd);

        w.bridge = nb;
        w.restart_count += 1;
        nb.ctx.logger.emit(.{ .restart = .{ .attempt = w.restart_count, .reason = "crash" } }, null) catch {};
    }
```

- [ ] **Step 2: Start the reaper in `init` callers and stop it in `deinit`**

`BridgeSupervisor.init` returns by value (callers move it), so the reaper must be started *after* the supervisor is at its final heap address — i.e. by whoever owns the `*BridgeSupervisor`. Both owners are in `control.zig` (`ControlServer.init` seeds the primary supervisor; `handleBeamCreate` creates more). Add `try sup.startReaper();` immediately after each `BridgeSupervisor.init(...)` assignment in `control.zig`:

In `ControlServer.init`, after `primary_sup.* = bridge_mod.BridgeSupervisor.init(allocator, runtime);` add:
```zig
        try primary_sup.startReaper();
```
In `handleBeamCreate`, after `sup.* = bridge_mod.BridgeSupervisor.init(cs.allocator, rt);` add:
```zig
        try sup.startReaper();
```

In `BridgeSupervisor.deinit`, **before** the worker-freeing loop, stop and join the reaper:

```zig
    pub fn deinit(self: *BridgeSupervisor) void {
        // warden-dmg: stop the reaper first so it never races teardown.
        self.reaper_stopping.store(true, .release);
        if (self.reaper_thread) |t| {
            t.join();
            self.reaper_thread = null;
        }
        for (self.workers.items) |w| {
            if (!w.retired) {
                w.bridge.deinit();
                self.allocator.destroy(w.bridge);
            }
            w.freeOwned(self.allocator);
            self.allocator.destroy(w);
        }
        self.workers.deinit(self.allocator);
    }
```

(Note: retired workers already had their bridge freed in `reapAndMaybeRespawn`, so `deinit` skips them.)

- [ ] **Step 3: Verify build + existing tests still pass (reaper present but idle without crashes)**

Run: `zig build test`
Expected: exit 0, 115 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/bridge.zig src/control.zig
git commit -m "warden-dmg: reaper thread — respawn/retire crashed workers, renice"
```

---

## Task 5: `proc.spawn` restart field + `beam.reaper` RPC

**Files:**
- Modify: `src/control.zig`

- [ ] **Step 1: Parse `restart` in `handleProcSpawn` and pass it through**

In `handleProcSpawn`, find the line Task 3 left (now passing `.permanent`):

```zig
    const pid = try sup.spawnWorkerUnder(cmd.items, log_dir, store_base, parent_pid, .permanent);
```

Replace with:

```zig
    // warden-dmg: optional per-worker restart policy (default permanent).
    var strategy: @import("restart.zig").Strategy = .permanent;
    if (obj.get("restart")) |rv| {
        if (rv == .string) {
            strategy = @import("restart.zig").Strategy.parse(rv.string) orelse
                return sendErrResp(cs.runtime.io, allocator, req_id, stream, "invalid restart");
        }
    }
    const pid = try sup.spawnWorkerUnder(cmd.items, log_dir, store_base, parent_pid, strategy);
```

- [ ] **Step 2: Add `handleBeamReaper`**

Add this function near the other handlers (e.g. after `handleBeamCreate`):

```zig
// warden-dmg
fn handleBeamReaper(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const pv = payload_val orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing payload");
    if (pv != .object) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "payload must be object");
    const obj = pv.object;

    const beam_id: u32 = if (obj.get("beam")) |bv|
        if (bv == .integer) @intCast(bv.integer) else cs.runtime.beam_id
    else
        cs.runtime.beam_id;

    const interval_val = obj.get("interval_ms") orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing interval_ms");
    if (interval_val != .integer or interval_val.integer < 0)
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "interval_ms must be a non-negative integer");

    const sup = cs.supervisors.get(beam_id) orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "unknown beam");

    const applied = sup.renice(@intCast(interval_val.integer));

    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"interval_ms\":{d}}}}}",
        .{ req_id, applied });
    defer allocator.free(resp);
    try writeFrame(cs.runtime.io, stream, resp);
}
```

- [ ] **Step 3: Route `beam.reaper` in `handleConnection`**

In the `if/else if` action chain, after the `beam.create` arm, add:

```zig
    } else if (std.mem.eql(u8, action, "beam.reaper")) {
        try handleBeamReaper(cs, allocator, req_id, stream, payload_val);
```

- [ ] **Step 4: Verify build + tests**

Run: `zig build test`
Expected: exit 0, 115 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/control.zig
git commit -m "warden-dmg: proc.spawn restart field + beam.reaper RPC"
```

---

## Task 6: `wardenctl renice` command

**Files:**
- Create: `apps/wardenctl/src/commands/renice.zig`
- Modify: `apps/wardenctl/src/cli.zig`

- [ ] **Step 1: Create the command**

`apps/wardenctl/src/commands/renice.zig`:

```zig
// warden-dmg

const std = @import("std");
const term = @import("../term.zig");
const ControlClient = @import("../client.zig").ControlClient;

// warden-dmg: send beam.reaper to adjust a beam's reaper poll interval.
pub fn run(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    beam: u32,
    interval_ms: u64,
    json_output: bool,
) !void {
    const payload = try std.fmt.allocPrint(
        allocator, "{{\"beam\":{d},\"interval_ms\":{d}}}", .{ beam, interval_ms });
    defer allocator.free(payload);

    const resp_bytes = try client.requestWithPayload("beam.reaper", payload);
    defer allocator.free(resp_bytes);

    if (json_output) {
        term.outAll(resp_bytes);
        term.outAll("\n");
        return;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    switch (obj.get("ok") orelse return error.InvalidResponse) {
        .bool => |b| if (!b) {
            const msg = switch (obj.get("error") orelse .null) {
                .string => |s| s,
                else => "unknown error",
            };
            term.errAll("error: renice failed: ");
            term.errAll(msg);
            term.errAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }
    const applied = obj.get("payload").?.object.get("interval_ms").?.integer;
    const line = try std.fmt.allocPrint(allocator, "beam {d}: reaper interval = {d}ms\n", .{ beam, applied });
    defer allocator.free(line);
    term.outAll(line);
}
```

- [ ] **Step 2: Import the command + dispatch it in `cli.zig`**

In `apps/wardenctl/src/cli.zig`, add the import near the other command imports:

```zig
// warden-dmg
const renice_cmd = @import("commands/renice.zig");
```

In the `dispatch` function's command chain (the one with `control_cmd.run`), add an arm. `renice` takes `<beam> <interval_ms>`:

```zig
    } else if (std.mem.eql(u8, subcmd, "renice")) {
        if (sub_args.len < 2) return usageErr("renice requires <beam> <interval_ms>");
        const beam = std.fmt.parseInt(u32, sub_args[0], 10) catch return usageErr("beam must be an integer");
        const interval = std.fmt.parseInt(u64, sub_args[1], 10) catch return usageErr("interval_ms must be an integer");
        try renice_cmd.run(allocator, client, beam, interval, opts.json_output);
```

(Place it alongside the existing single-client commands like `pause`/`kill`, not in the fan-out set. Match the exact `else if` style and the `client`/`opts` names used by the surrounding arms.)

- [ ] **Step 3: Add `renice` to the usage text**

In `printUsage`, add a line under the command list:

```
        \\  renice <beam> <ms>   Adjust a beam's worker-reaper poll interval (10-2000)
```

- [ ] **Step 4: Verify both binaries build**

Run: `zig build`
Expected: exit 0; `zig-out/bin/warden` and `zig-out/bin/wardenctl` present.

- [ ] **Step 5: Commit**

```bash
git add apps/wardenctl/
git commit -m "warden-dmg: wardenctl renice command"
```

---

## Task 7: Integration test (real Python workers)

**Files:**
- Create: `src/foreign_restart_test.zig`
- Modify: `src/root.zig`

- [ ] **Step 1: Write the integration test**

`src/foreign_restart_test.zig` — drives `BridgeSupervisor` with throwaway Python
workers written into the test tmp dir. It starts the reaper, crashes a worker,
and asserts a new PID appears.

```zig
// warden-dmg

const std = @import("std");
const testutil = @import("testutil.zig");
const beam = @import("beam.zig");
const bridge = @import("bridge.zig");

// Write a tiny python script into `dir` and return its absolute path (owned).
fn writeScript(allocator: std.mem.Allocator, dir: []const u8, name: []const u8, body: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir, name });
    errdefer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = body });
    return path;
}

// A worker that connects, reads the handshake, then exits abnormally on the
// FIRST message it receives — simulating a crash mid-work.
const crash_on_msg =
    \\import socket, struct, os, sys
    \\s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    \\s.connect(os.environ["WARDEN_SOCKET"])
    \\def frame():
    \\    h = s.recv(4)
    \\    if len(h) < 4: sys.exit(0)
    \\    n = struct.unpack(">I", h)[0]
    \\    return s.recv(n)
    \\frame()            # handshake
    \\frame()            # first real message -> then die hard
    \\os._exit(1)
;

test "permanent worker respawns with a new pid after a crash" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var base_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try testutil.tmpAbs(&base_buf, &tmp);

    const script = try writeScript(allocator, base, "crash.py", crash_on_msg);
    defer allocator.free(script);

    const rt = try beam.Runtime.init(allocator, 77);
    defer rt.destroy();

    var sup = bridge.BridgeSupervisor.init(allocator, rt);
    try sup.startReaper();
    defer sup.deinit();
    // Fast reaper so the test is quick.
    _ = sup.renice(10);

    const cmd = [_][]const u8{ "python3", script };
    const pid1 = try sup.spawnWorkerUnder(&cmd, base, base, null, .permanent);

    // Wait for the worker to connect, then poke it so it crashes.
    clock_sleep(150);
    const b = sup.findBridge(pid1).?;
    try b.deliver(.{ .kind = .request, .@"type" = "go", .id = "1", .from = "test", .to = "x", .body = .null });

    // Reaper should detect the crash and respawn with a NEW pid within ~1s.
    var respawned = false;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        clock_sleep(20);
        if (sup.workers.items.len == 1) {
            const cur = sup.workers.items[0].bridge.pid;
            if (cur.proc != pid1.proc and !sup.workers.items[0].retired) {
                respawned = true;
                break;
            }
        }
    }
    try std.testing.expect(respawned);
}

fn clock_sleep(ms: u64) void {
    @import("clock.zig").sleepNs(ms * std.time.ns_per_ms);
}
```

- [ ] **Step 2: Register the test in the test root**

In `src/root.zig`, inside the `comptime { ... }` block, after the `_ = @import("sync.zig");` line (or near the other `warden-dmg` entries):

```zig
    // warden-dmg
    _ = @import("foreign_restart_test.zig");
```

- [ ] **Step 3: Run the test**

Run: `zig build test`
Expected: exit 0. The new test passes (a fresh PID appears after the crash).
If `python3` is unavailable the test should be skipped, not failed — if it fails
on environment, guard the top of the test with:
`std.process.Child` availability is not checkable cheaply, so instead detect a
spawn failure: if `spawnWorkerUnder` returns an error, `return error.SkipZigTest;`.
Wrap the `spawnWorkerUnder` call: `const pid1 = sup.spawnWorkerUnder(...) catch return error.SkipZigTest;`.

- [ ] **Step 4: Commit**

```bash
git add src/foreign_restart_test.zig src/root.zig
git commit -m "warden-dmg: integration test — crashed worker respawns with new pid"
```

---

## Task 8: Verify, finish, merge

- [ ] **Step 1: Full green build**

Run: `zig build test && zig build`
Expected: both exit 0; tests pass (116+); `warden` + `wardenctl` built.

- [ ] **Step 2: Manual smoke (optional but recommended)**

```bash
WARDEN_BEAM_ID=5 WARDEN_CTRL_SOCKET=/tmp/w5.sock ./zig-out/bin/warden &
./zig-out/bin/wardenctl --socket /tmp/w5.sock renice 5 25   # expect "beam 5: reaper interval = 25ms"
kill %1
```

- [ ] **Step 3: Merge to master + close the bead**

```bash
git checkout master
git merge --ff-only warden-dmg
git branch -d warden-dmg
bd close warden-dmg
```

---

## Self-review notes (resolved)

- **Spec coverage:** detection (Task 2), per-worker policy + retain inputs (Task 3),
  reaper + respawn + intensity + new-PID (Task 4), renice default 50 / clamp 10-2000
  (Tasks 1,4), `proc.spawn` restart field + `beam.reaper` (Task 5), `wardenctl renice`
  (Task 6), integration tests incl. runaway + renice (Task 1 unit + Task 7 integration).
- **Logging:** uses the existing `RestartEvent` (`.restart`) for respawns and `note()`
  for retire — no new logger variants needed (spec said field names would be pinned to
  the logger; done).
- **Out of scope (as designed):** monitor/DOWN notifications; one_for_all/rest_for_one
  for foreign workers.
