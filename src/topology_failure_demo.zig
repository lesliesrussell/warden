// warden-45x
//
// Failure recovery demo topology.
//
// Shape:
//   session_supervisor  — never restarts; PID is stable across the demo
//   └── executor        — manages the tool worker slot
//       └── tool_worker — receives tasks; one instance hangs after task 1
//   watchdog            — monitors tool_worker.last_active_at; detects idle;
//                         kills hung worker and spawns a replacement
//
// Narrative:
//   1. task_1 dispatched → worker_1 accepts it, then stalls (simulates HTTP hang)
//   2. watchdog detects idle_timeout exceeded → quarantines worker_1 PID
//   3. executor spawns worker_2 (new PID) → mailbox role preserved
//   4. task_2 dispatched → worker_2 completes it
//   5. session_supervisor PID unchanged throughout

const std = @import("std");
const beam = @import("beam.zig");

pub const Pid = beam.Pid;
pub const Runtime = beam.Runtime;
pub const Ctx = beam.Ctx;
pub const MessageEnvelope = beam.MessageEnvelope;

// warden-45x
pub const MsgType = struct {
    pub const task_request = "fd.task_request";
    pub const task_done = "fd.task_done";
    pub const shutdown = "fd.shutdown";
};

// warden-45x
pub const Config = struct {
    log_dir: []const u8,
    storage_base: []const u8,
    idle_timeout_ms: i64 = 150,
    watchdog_poll_ms: u64 = 20,
};

// warden-45x
pub const WorkerCtx = struct {
    ctx: Ctx,
    demo: *FailureRecoveryDemo,
    ready: std.atomic.Value(bool),
    stopping: std.atomic.Value(bool),
    exited: std.atomic.Value(bool),
    thread: ?std.Thread,
    hang_after_first: bool,
};

fn matchAny(msg: *const MessageEnvelope) bool {
    _ = msg;
    return true;
}

// warden-45x
// Tool worker thread.
// If hang_after_first is true: handles first task then stops logging (simulates
// a blocking HTTP call with no timeout). The watchdog detects the idle gap.
fn toolWorkerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const demo = wctx.demo;

    ctx.note("tool_worker: ready", null) catch {};

    var tasks_handled: u32 = 0;

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;

        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.task_request)) {
            tasks_handled += 1;
            ctx.note("tool_worker: received task", null) catch {};

            if (wctx.hang_after_first and tasks_handled == 1) {
                // Simulate a hanging HTTP call — thread stays alive but
                // never logs again. last_active_at in the registry stops
                // advancing, which the watchdog detects as idle_timeout.
                ctx.note("tool_worker: calling external API (no timeout)...", null) catch {};
                while (!wctx.stopping.load(.acquire)) {
                    std.Thread.sleep(10 * std.time.ns_per_ms);
                }
                break;
            }

            // Normal path: complete the task and notify the demo.
            std.Thread.sleep(3 * std.time.ns_per_ms);
            ctx.note("tool_worker: task completed", null) catch {};
            demo.task_completed.store(true, .release);
        }
    }
    wctx.exited.store(true, .release);
}

// warden-45x
pub const WatchdogCtx = struct {
    demo: *FailureRecoveryDemo,
    stopping: std.atomic.Value(bool),
    exited: std.atomic.Value(bool),
    thread: ?std.Thread,
};

// warden-45x
// Watchdog thread.
// Polls the registry every watchdog_poll_ms. If the current tool worker's
// last_active_at is stale by more than idle_timeout_ms, it:
//   1. Quarantines the hung worker's PID via the policy engine
//   2. Signals the executor to spawn a fresh worker
fn watchdogThread(wdctx: *WatchdogCtx) void {
    const demo = wdctx.demo;
    const poll_ns = demo.config.watchdog_poll_ms * std.time.ns_per_ms;

    while (!wdctx.stopping.load(.acquire)) {
        std.Thread.sleep(poll_ns);
        // warden-iry: recheck after sleep — deinit() may have fired during sleep
        if (wdctx.stopping.load(.acquire)) break;

        const worker_pid = demo.current_worker_proc.load(.acquire);
        if (worker_pid == 0) continue;

        const pid = Pid{ .beam = demo.runtime.beam_id, .proc = worker_pid };
        const entry = demo.runtime.registry.lookup(pid) orelse continue;
        const idle_ms = std.time.milliTimestamp() - entry.last_active_at;

        if (idle_ms > demo.config.idle_timeout_ms) {
            // Claim the restart slot — skip if a restart is already in flight.
            if (demo.restarting.cmpxchgStrong(false, true, .acquire, .monotonic) != null) continue;
            // Zero out proc so the watchdog doesn't fire again on the stale PID.
            demo.current_worker_proc.store(0, .release);
            demo.runtime.policy.quarantine(pid, "idle_timeout") catch {};
            demo.restartWorker() catch {};
            demo.restarting.store(false, .release);
        }
    }
    wdctx.exited.store(true, .release);
}

// warden-45x
pub const FailureRecoveryDemo = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    config: Config,

    // The session supervisor PID never changes — it identifies the session.
    session_pid: Pid,

    // current_worker_proc is the proc field of the current worker's Pid.
    // Reconstruct the full Pid with runtime.beam_id when needed.
    current_worker_proc: std.atomic.Value(u64),

    // Set by worker_2 when task_2 completes.
    task_completed: std.atomic.Value(bool),

    // restart_count is incremented each time the executor spawns a new worker.
    restart_count: std.atomic.Value(u32),

    // restarting prevents concurrent restart calls from the watchdog.
    restarting: std.atomic.Value(bool),

    worker: ?*WorkerCtx,
    watchdog: WatchdogCtx,
    worker_mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, config: Config) !FailureRecoveryDemo {
        const session_pid = try runtime.registry.spawn(.native_worker, null, .{});
        return FailureRecoveryDemo{
            .allocator = allocator,
            .runtime = runtime,
            .config = config,
            .session_pid = session_pid,
            .current_worker_proc = std.atomic.Value(u64).init(0),
            .task_completed = std.atomic.Value(bool).init(false),
            .restart_count = std.atomic.Value(u32).init(0),
            .restarting = std.atomic.Value(bool).init(false),
            .worker = null,
            .watchdog = WatchdogCtx{
                .demo = undefined,
                .stopping = std.atomic.Value(bool).init(false),
                .exited = std.atomic.Value(bool).init(false),
                .thread = null,
            },
            .worker_mutex = .{},
        };
    }

    pub fn deinit(self: *FailureRecoveryDemo) void {
        // Stop watchdog
        self.watchdog.stopping.store(true, .release);
        if (self.watchdog.thread) |t| t.join();

        // Stop current worker
        self.worker_mutex.lock();
        defer self.worker_mutex.unlock();
        if (self.worker) |wctx| {
            wctx.stopping.store(true, .release);
            if (wctx.thread) |t| t.join();
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
            self.worker = null;
        }
    }

    // warden-45x
    // Spawn the initial tool worker (hang_after_first = true).
    pub fn start(self: *FailureRecoveryDemo) !void {
        try self.spawnWorker(true);

        // Start watchdog
        self.watchdog.demo = self;
        self.watchdog.thread = try std.Thread.spawn(.{}, watchdogThread, .{&self.watchdog});
    }

    fn spawnWorker(self: *FailureRecoveryDemo, hang_after_first: bool) !void {
        const wctx = try self.allocator.create(WorkerCtx);
        wctx.demo = self;
        wctx.hang_after_first = hang_after_first;
        wctx.ready = std.atomic.Value(bool).init(false);
        wctx.stopping = std.atomic.Value(bool).init(false);
        wctx.exited = std.atomic.Value(bool).init(false);
        wctx.thread = null;

        const pid = try self.runtime.registry.spawn(.native_worker, null, .{});
        try self.runtime.allocMailbox(pid, .{});
        wctx.ctx = try Ctx.init(self.runtime, pid, self.config.log_dir, self.config.storage_base);
        wctx.ready.store(true, .release);
        wctx.thread = try std.Thread.spawn(.{}, toolWorkerThread, .{wctx});

        self.current_worker_proc.store(pid.proc, .release);

        self.worker_mutex.lock();
        self.worker = wctx;
        self.worker_mutex.unlock();
    }

    // warden-45x
    // Called by the watchdog when idle_timeout exceeded.
    // Stops the hung worker thread, spawns a replacement (non-hanging).
    // current_worker_proc must be zeroed by the caller before calling this.
    pub fn restartWorker(self: *FailureRecoveryDemo) !void {
        // Swap out the hung worker under the mutex.
        self.worker_mutex.lock();
        const old = self.worker;
        self.worker = null;
        self.worker_mutex.unlock();

        if (old) |wctx| {
            wctx.stopping.store(true, .release);
            if (wctx.thread) |t| t.join();
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
        }

        _ = self.restart_count.fetchAdd(1, .monotonic);

        // Spawn the replacement; this also sets current_worker_proc.
        try self.spawnWorker(false);
    }

    // warden-45x
    // Dispatch a task message to the current worker.
    pub fn dispatchTask(self: *FailureRecoveryDemo, task_id: []const u8) !void {
        const worker_pid = Pid{ .beam = self.runtime.beam_id, .proc = self.current_worker_proc.load(.acquire) };
        const mb = self.runtime.getMailbox(worker_pid) orelse return error.NoMailbox;
        const msg = MessageEnvelope{
            .kind = .request,
            .@"type" = MsgType.task_request,
            .id = task_id,
            .from = "session",
            .to = "worker",
            .body = .{ .string = task_id },
        };
        _ = mb.enqueue(msg) catch {};
    }
};
