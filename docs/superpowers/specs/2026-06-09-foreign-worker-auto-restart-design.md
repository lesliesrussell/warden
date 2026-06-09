# ForeignBridge auto-restart — design (warden-dmg)

**Status:** approved design, pre-implementation
**Bead:** warden-dmg — *ForeignBridge auto-restart: crash detection and supervisor respawn*
**Date:** 2026-06-09

## Problem

`proc.spawn` launches a foreign (Python) worker as a `ForeignBridge` tracked by
`BridgeSupervisor`. The bridge's reader thread already *detects* worker death —
when the worker's socket drops or the process exits, `readFrame` returns
`ConnectionClosed` and the reader loop exits — but nothing acts on it. The dead
bridge just sits in the supervisor's list. There is no respawn, no policy, and
the spawn inputs (`cmd`/`log_dir`/`storage_base`/`parent`) are discarded after
the initial spawn, so a respawn is not even possible.

This makes "managed" hollow: a crashed worker stays dead with no human
intervention *and* no runtime intervention. `warden-dmg` closes that gap — the
supervisor detects death and respawns according to a per-worker restart policy,
with a runaway guard.

Native processes already have this via `Supervisor.childExited`
(`src/supervisor.zig`). This bead brings the equivalent to the *foreign* worker
path (`BridgeSupervisor` in `src/bridge.zig`), reusing the native supervisor's
proven restart-intensity values for consistency.

## Goals / non-goals

**Goals**
- Detect foreign-worker death (socket drop or process exit).
- Respawn per a per-worker restart policy: `permanent` | `transient` | `temporary`.
- Runaway protection: cap restarts at 3 within a rolling 5s window per worker.
- Restart = a new incarnation with a new PID (matches the README demo and OTP).
- A live-adjustable reaper cadence ("renice") via RPC + `wardenctl`.
- Log every restart and every give-up as structured events.

**Non-goals (separate beads)**
- Monitor/DOWN notifications to watcher processes when a worker dies/restarts.
- one_for_all / rest_for_one cascade strategies across foreign siblings (foreign
  workers spawned via `proc.spawn` are independent; only per-worker semantics apply).
- Preserving a stable identity/name across incarnations beyond `parent_pid`
  linkage + `proc.list` discovery.

## Design

### Data model — `ManagedWorker`

`BridgeSupervisor.bridges: ArrayList(*ForeignBridge)` becomes
`workers: ArrayList(*ManagedWorker)`:

```
ManagedWorker {
    bridge: *ForeignBridge,
    // respawn inputs — retained (currently discarded after spawnWorkerUnder):
    cmd: [][]u8,                  // owned deep copy
    log_dir: []u8,                // owned
    storage_base: []u8,           // owned
    parent_pid: ?Pid,
    restart: RestartStrategy,     // permanent | transient | temporary
    // runaway guard — per worker, mirrors native Supervisor's values:
    restart_timestamps: ArrayList(i64),  // ms, rolling 5s window
    restart_count: u32,           // lifetime restarts (the "attempt" in logs)
    retired: bool,                // gave up (temporary, normal-transient, or intensity exceeded)
}
```

`RestartStrategy` reuses `supervisor.RestartStrategy`'s `transient`/`temporary`
meaning; `permanent` maps to "always restart" (the `one_for_one` arm, minus the
sibling logic). The `max_restarts = 3` / `intensity_window_ms = 5000` constants
match `src/supervisor.zig` exactly.

### Crash detection — in the reader thread

`ForeignBridge` gains one field: `crashed: std.atomic.Value(bool)`, set by the
reader thread.

The reader loop already exits on `ConnectionClosed`. Today that happens for two
reasons: a deliberate `stop()` (which sets `running = false` first), or the
worker dying. The thread distinguishes them: **if the loop ends while `running`
is still `true`, the worker died** → set `crashed = true`.

The *reason* (normal vs abnormal) is not known to the reader thread — it comes
from the child process's exit status. The reaper classifies it during teardown:
`child.wait()` returns a `Term`, and `Term.Exited(0)` → normal, anything else
(non-zero exit or signal) → abnormal/crash. `stop()` currently discards the
`Term` (`_ = child.wait() catch {}`); it will store it on the bridge
(`last_exit: ExitReason`) so the reaper can read the classification after reaping.

`stop()` already sets `running = false` *before* `shutdown(.both)`, so a
deliberate stop never trips the `crashed` flag — the existing migration fix and
this detection compose cleanly.

### Reaper thread — one per `BridgeSupervisor`

Started in `BridgeSupervisor.init`, joined in `deinit`. Loop:

```
while (!reaper_stopping):
    iv = reaper_interval_ms.load(.acquire)   // live-adjustable
    sleep(iv ms)
    lock workers
    for w in workers where w.bridge.crashed and not w.retired:
        reapAndMaybeRespawn(w)
    unlock
```

`reapAndMaybeRespawn(w)`:
1. **Reap the dead incarnation:** `w.bridge.stop()` (shutdown → join reader →
   close → `child.wait()`), capturing the exit reason; then `deinit` + free the
   old bridge. (Reuses the migration's `shutdown→join→close` ordering, so no BADF.)
2. **Policy:**
   - `temporary` → retire.
   - `transient` and exit was normal → retire.
   - otherwise → candidate for restart.
3. **Runaway guard:** prune `w.restart_timestamps` to the 5s window; if
   `len >= 3` → retire + emit `restart_giving_up`. Else append `now`.
4. **Respawn:** create a fresh `ForeignBridge` from `w.cmd/log_dir/storage_base/
   parent_pid` → **new PID**. Old registry entry → `dead`; new entry → `running`.
   Point `w.bridge` at the new incarnation. Emit a `restart` event
   `{beam, old_pid, new_pid, reason}`.

"Retire" = leave the worker dead, `retired = true`, logged. The `ManagedWorker`
record stays in the list (so `proc.list`/introspection can still show a retired
worker) until supervisor `deinit`.

### Adjustable cadence — "renice"

`BridgeSupervisor.reaper_interval_ms: std.atomic.Value(u64)`, **default 50ms**.

```
pub fn renice(self, interval_ms: u64) u64 {   // returns the clamped value
    const v = clamp(interval_ms, 10, 2000);
    self.reaper_interval_ms.store(v, .release);
    return v;
}
```

The reaper reads it at the top of each loop, so a change lands within one cycle.

### API surface

- **`proc.spawn`** gains an optional `"restart"` field
  (`"permanent"`|`"transient"`|`"temporary"`, default `permanent`).
  `handleProcSpawn` parses it (unknown value → `sendErrResp "invalid restart"`)
  and threads it through `spawnWorkerUnder`. Existing callers unaffected.
- **`beam.reaper`** — new control RPC. Payload `{ "beam": N, "interval_ms": M }`
  → `sup.renice(M)` → response `{ "interval_ms": <clamped> }`. Unknown beam →
  `sendErrResp "unknown beam"`.
- **`wardenctl renice <beam> <interval_ms>`** — sends `beam.reaper`, prints the
  applied interval.

### PID identity

A restart is a new incarnation with a new monotonic PID (registry already
allocates these). Callers/watchdogs discover the live worker via
`proc.list`/the registry — consistent with the existing watchdog model in the
README. The old PID transitions to `dead`.

### Shutdown ordering

`BridgeSupervisor.deinit`: set `reaper_stopping = true`, **join the reaper
thread first**, then deinit each worker's bridge and free records. The reaper
holds the workers lock while reaping, and skips `retired` workers, so it never
races a concurrent `deinit` or double-reaps.

## Logging (structured events)

- `restart` — `{beam, old_pid, new_pid, reason, attempt}` on each respawn.
- `restart_giving_up` — `{beam, pid, restart_count, window_ms}` when intensity
  is exceeded or a `temporary`/normal-`transient` worker is retired.

## Testing

A new integration test in the spirit of `src/failure_recovery_test.zig`, driving
real Python workers through `BridgeSupervisor`:

1. **permanent restart:** worker exits after one message → reaper produces a
   *new* PID with the worker running again; `restart` event emitted.
2. **temporary stays dead:** a `temporary` worker exits → it is retired, no new PID.
3. **transient on normal exit:** a `transient` worker exits cleanly → retired.
4. **runaway guard:** a worker that crashes immediately on start → retired after
   exactly 3 attempts within the window; `restart_giving_up` emitted.
5. **renice:** `renice(10)` then `renice(5000)` → clamped values applied and the
   reaper continues to function at the new cadence.

Unit-level: `renice` clamping (below 10 → 10, above 2000 → 2000).

## Files touched

- `src/bridge.zig` — `ManagedWorker`, reaper thread, crash flag, `renice`,
  respawn logic; `spawnWorkerUnder` gains a `restart` parameter and retains inputs.
- `src/control.zig` — `handleProcSpawn` parses `restart`; new `handleBeamReaper`.
- `apps/wardenctl/src/cli.zig` + `commands/` — `renice` command.
- `src/foreign_restart_test.zig` (new) + register in `src/root.zig`.
- Python test fixtures (a worker that exits-on-message / crashes-on-start) under
  `examples/` or the test's tmp dir.
