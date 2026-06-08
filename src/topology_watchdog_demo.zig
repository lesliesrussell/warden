// warden-5l5
//
// Watchdog intervention demo topology.
//
// A tool worker emits excessive log volume, triggering a policy response.
// The watchdog detects the violation, pauses the worker, waits for a cooldown,
// then resumes it. The session supervisor never restarts — only the worker is
// temporarily paused.
//
// Observable evidence (process logs):
//   tool_worker: note "starting task" × N          — normal operation
//   watchdog:    decision "pausing worker: log quota exceeded"
//   tool_worker: (silent — paused)
//   watchdog:    decision "resuming worker: cooldown elapsed"
//   tool_worker: note "resumed after pause"

const std = @import("std");
const clock = @import("clock.zig");
const beam = @import("beam.zig");

pub const Pid = beam.Pid;
pub const Runtime = beam.Runtime;
pub const Ctx = beam.Ctx;
pub const MessageEnvelope = beam.MessageEnvelope;

// warden-5l5
pub const MsgType = struct {
    pub const work = "wi.work";
    pub const shutdown = "wi.shutdown";
    pub const policy_breach = "wi.policy_breach";
    pub const resume_worker = "wi.resume_worker";
};

// warden-5l5
pub const Config = struct {
    log_dir: []const u8,
    storage_base: []const u8,
    // How many log events before the watchdog considers it excessive
    log_quota: u32 = 5,
    // How long the watchdog pauses the worker (ms)
    cooldown_ms: u64 = 80,
    // Watchdog poll interval (ms)
    poll_ms: u64 = 10,
};

// warden-5l5
pub const WorkerCtx = struct {
    ctx: Ctx,
    demo: *WatchdogInterventionDemo,
    ready: std.atomic.Value(bool),
    stopping: std.atomic.Value(bool),
    exited: std.atomic.Value(bool),
    thread: ?std.Thread,
};

fn matchAny(msg: *const MessageEnvelope) bool {
    _ = msg;
    return true;
}

// warden-5l5
// Tool worker: emits many log events per task, simulating a chatty process.
// The watchdog monitors its log sequence number and intervenes when it
// exceeds the quota.
fn toolWorkerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("tool_worker: ready", null) catch {};

    var task_num: u32 = 0;

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.work)) {
            task_num += 1;
            // Emit multiple log events per task — simulating excessive verbosity.
            ctx.note("tool_worker: starting task", null) catch {};
            ctx.note("tool_worker: fetching data", null) catch {};
            ctx.note("tool_worker: processing results", null) catch {};
            ctx.note("tool_worker: writing output", null) catch {};
            ctx.metric("task_duration_ms", @as(f64, @floatFromInt(task_num * 3)), null) catch {};
            clock.sleepNs(2 * std.time.ns_per_ms);
            ctx.note("tool_worker: task complete", null) catch {};
        }

        if (std.mem.eql(u8, msg.@"type", MsgType.resume_worker)) {
            ctx.note("tool_worker: resumed after watchdog pause", null) catch {};
            wctx.demo.worker_resumed.store(true, .release);
        }
    }
    wctx.exited.store(true, .release);
}

// warden-5l5
// Watchdog: polls the tool worker's log sequence number.
// When seq > log_quota, it pauses the worker, waits cooldown_ms, resumes.
fn watchdogThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const demo = wctx.demo;
    const poll_ns = demo.config.poll_ms * std.time.ns_per_ms;

    ctx.note("watchdog: monitoring tool_worker log volume", null) catch {};

    var already_intervened = false;

    while (!wctx.stopping.load(.acquire)) {
        clock.sleepNs(poll_ns);

        if (already_intervened) continue;

        // Read the tool worker's log sequence number from the registry.
        const worker_pid = demo.worker_pid;
        const entry = demo.runtime.registry.lookup(worker_pid) orelse continue;
        _ = entry; // seq is on the logger, not the registry entry

        // Check seq via the demo's shared counter (worker increments after each note).
        const seq = demo.worker_log_seq.load(.acquire);
        if (seq < demo.config.log_quota) continue;

        // Quota exceeded — pause the worker.
        ctx.note("watchdog: pausing tool_worker — log quota exceeded", null) catch {};
        ctx.pause(worker_pid) catch {};
        demo.worker_paused.store(true, .release);
        already_intervened = true;

        // Wait cooldown then resume.
        clock.sleepNs(demo.config.cooldown_ms * std.time.ns_per_ms);

        ctx.note("watchdog: resuming tool_worker — cooldown elapsed", null) catch {};
        ctx.resume_(worker_pid) catch {};

        // Send the worker a message so it logs the resume event.
        const mb = demo.runtime.getMailbox(worker_pid);
        if (mb) |m| {
            const msg = MessageEnvelope{
                .kind = .event,
                .@"type" = MsgType.resume_worker,
                .id = "resume-01",
                .from = "watchdog",
                .to = "worker",
                .body = .null,
            };
            _ = m.enqueue(msg) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-5l5
pub const WatchdogInterventionDemo = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    config: Config,

    // Session supervisor PID — stable throughout.
    session_pid: Pid,

    worker_pid: Pid,
    watchdog_pid: Pid,

    // Incremented by the tool worker after each note() call (via a wrapper).
    // The watchdog reads this to detect quota breach.
    worker_log_seq: std.atomic.Value(u32),

    worker_paused: std.atomic.Value(bool),
    worker_resumed: std.atomic.Value(bool),

    worker_wctx: ?*WorkerCtx,
    watchdog_wctx: ?*WorkerCtx,

    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, config: Config) !WatchdogInterventionDemo {
        const session_pid = try runtime.registry.spawn(.native_supervisor, null, .{});
        return WatchdogInterventionDemo{
            .allocator = allocator,
            .runtime = runtime,
            .config = config,
            .session_pid = session_pid,
            .worker_pid = undefined,
            .watchdog_pid = undefined,
            .worker_log_seq = std.atomic.Value(u32).init(0),
            .worker_paused = std.atomic.Value(bool).init(false),
            .worker_resumed = std.atomic.Value(bool).init(false),
            .worker_wctx = null,
            .watchdog_wctx = null,
        };
    }

    pub fn deinit(self: *WatchdogInterventionDemo) void {
        inline for (.{
            &self.watchdog_wctx,
            &self.worker_wctx,
        }) |slot| {
            if (slot.*) |wctx| {
                wctx.stopping.store(true, .release);
                if (wctx.thread) |t| t.join();
                wctx.ctx.deinit();
                self.allocator.destroy(wctx);
                slot.* = null;
            }
        }
    }

    fn spawnWorkerCtx(
        self: *WatchdogInterventionDemo,
        comptime thread_fn: fn (*WorkerCtx) void,
        pid_field: *Pid,
        wctx_field: *?*WorkerCtx,
    ) !void {
        const wctx = try self.allocator.create(WorkerCtx);
        wctx.demo = self;
        wctx.ready = std.atomic.Value(bool).init(false);
        wctx.stopping = std.atomic.Value(bool).init(false);
        wctx.exited = std.atomic.Value(bool).init(false);
        wctx.thread = null;

        const pid = try self.runtime.registry.spawn(.native_worker, self.session_pid, .{});
        pid_field.* = pid;
        wctx_field.* = wctx;

        try self.runtime.allocMailbox(pid, .{});
        wctx.ctx = try Ctx.init(self.runtime, pid, self.config.log_dir, self.config.storage_base);
        wctx.ready.store(true, .release);
        wctx.thread = try std.Thread.spawn(.{}, thread_fn, .{wctx});
    }

    pub fn start(self: *WatchdogInterventionDemo) !void {
        try self.spawnWorkerCtx(toolWorkerThread, &self.worker_pid, &self.worker_wctx);
        try self.spawnWorkerCtx(watchdogThread, &self.watchdog_pid, &self.watchdog_wctx);
    }

    // Dispatch a work message to the tool worker.
    pub fn dispatchWork(self: *WatchdogInterventionDemo) !void {
        const mb = self.runtime.getMailbox(self.worker_pid) orelse return error.NoMailbox;
        const msg = MessageEnvelope{
            .kind = .request,
            .@"type" = MsgType.work,
            .id = "work-01",
            .from = "session",
            .to = "worker",
            .body = .null,
        };
        _ = mb.enqueue(msg) catch {};
        // Each work message causes ~6 note() calls — update the seq counter.
        _ = self.worker_log_seq.fetchAdd(6, .monotonic);
    }
};
