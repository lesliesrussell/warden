// warden-1ud

const std = @import("std");
const clock = @import("clock.zig");
const beam = @import("beam.zig");
const supervisor_mod = @import("supervisor.zig");
const types = @import("types.zig");

pub const Pid = beam.Pid;
pub const Runtime = beam.Runtime;
pub const Ctx = beam.Ctx;
pub const Supervisor = supervisor_mod.Supervisor;
pub const ChildSpec = supervisor_mod.ChildSpec;
pub const MessageEnvelope = beam.MessageEnvelope;

// warden-1ud
pub const SessionId = []const u8;

// warden-1ud
pub const SessionConfig = struct {
    session_id: SessionId,
    tool_worker_count: usize = 2,
    log_dir: []const u8,
    storage_base: []const u8,
};

// warden-1ud
/// Canonical agent message type discriminators.
/// Passed over MessageEnvelope by setting msg.@"type" to one of these
/// string literals and encoding payload in msg.body.
pub const AgentMsgType = struct {
    pub const run_task = "agent.run_task";
    pub const task_result = "agent.task_result";
    pub const shutdown = "agent.shutdown";
    pub const health_check = "agent.health_check";
    pub const health_ok = "agent.health_ok";
};

// warden-1ud
/// Per-worker execution context.  Heap-allocated so the pointer is stable
/// for the lifetime of the spawned thread.
pub const WorkerCtx = struct {
    ctx: Ctx,
    topology: *Topology,
    /// Becomes true only after allocMailbox + Ctx.init have both completed.
    /// The entry function spin-waits on this before entering its main loop.
    ready: std.atomic.Value(bool),
    /// Set to true by Topology.shutdown() to ask the main loop to exit.
    stopping: std.atomic.Value(bool),
    /// Set to true by the worker just before its thread function returns.
    exited: std.atomic.Value(bool),
    /// The OS thread handle — joined in deinit.
    thread: ?std.Thread,
    /// Role-specific: index of this worker in tool_worker_pids (tool workers only).
    worker_index: usize,
};

// warden-1ud
/// Round-robin index used by the planner to select the next tool worker.
var planner_next_worker = std.atomic.Value(usize).init(0);

// warden-1ud
/// match_any predicate — accepts every message.
fn matchAny(msg: *const MessageEnvelope) bool {
    _ = msg;
    return true;
}

// ─── Thread entry functions ───────────────────────────────────────────────────
// Each role runs as a dedicated OS thread (not on the scheduler's thread pool)
// because they need long-lived blocking recv loops.
//
// The supervisor tree still registers these processes in the registry and
// owns their child specs; the actual execution happens on these dedicated
// threads rather than the Scheduler's worker pool.

// warden-1ud
/// Planner process thread.
/// Receives run_task, forwards to a tool_worker round-robin, collects the
/// task_result, then replies to the original caller.
fn plannerThread(wctx: *WorkerCtx) void {
    // Spin until topology.start() has finished wiring up the Ctx and mailbox.
    while (!wctx.ready.load(.acquire)) {
        clock.sleepNs(1 * std.time.ns_per_ms);
    }

    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    const alloc = topo.allocator;

    ctx.note("planner: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.run_task)) {
            const task_id = msg.task_id orelse "unknown";
            const session_id = msg.session_id orelse topo.config.session_id;
            const trace_id = msg.trace_id;

            // Emit a structured note carrying session_id and trace_id so they
            // appear in the log file.  The logger's send/recv events already
            // carry trace_id; session_id needs an explicit note here so it is
            // visible in the per-process log file that tests inspect.
            // Emit a structured note carrying session_id and trace_id so they
            // appear in the log file.  The logger's send/recv events carry
            // trace_id but not session_id, so we embed session_id in the
            // message string here and flush immediately so it is on disk.
            if (std.fmt.allocPrint(
                alloc,
                "planner: run_task session={s} trace={?s} task={s}",
                .{ session_id, trace_id, task_id },
            )) |note_msg| {
                defer alloc.free(note_msg);
                ctx.note(note_msg, null) catch {};
                ctx.logger.flush() catch {};
            } else |_| {
                ctx.note("planner: received run_task", null) catch {};
                ctx.logger.flush() catch {};
            }

            // Round-robin worker selection.
            const n = topo.tool_worker_pids.len;
            if (n == 0) continue;
            const idx = planner_next_worker.fetchAdd(1, .monotonic) % n;
            const worker_pid = topo.tool_worker_pids[idx];

            const worker_str = std.fmt.allocPrint(alloc, "{d}", .{worker_pid.proc}) catch continue;
            defer alloc.free(worker_str);
            const self_str = std.fmt.allocPrint(alloc, "{d}", .{ctx.pid.proc}) catch continue;
            defer alloc.free(self_str);

            const forward = MessageEnvelope{
                .kind = .request,
                .@"type" = AgentMsgType.run_task,
                .id = msg.id,
                .from = self_str,
                .to = worker_str,
                .trace_id = trace_id,
                .session_id = session_id,
                .task_id = task_id,
                .body = msg.body,
            };
            ctx.send(worker_pid, forward) catch continue;

            // Wait for task_result from the worker.
            const result_opt = ctx.recv(struct {
                fn f(m: *const MessageEnvelope) bool {
                    return std.mem.eql(u8, m.@"type", AgentMsgType.task_result);
                }
            }.f, 5000) catch null;

            if (result_opt) |result| {
                if (msg.reply_to) |reply_to_str| {
                    const reply_proc = std.fmt.parseInt(u64, reply_to_str, 10) catch continue;
                    const reply_pid = Pid{ .beam = topo.runtime.beam_id, .proc = reply_proc };
                    const from_str = std.fmt.allocPrint(alloc, "{d}", .{ctx.pid.proc}) catch continue;
                    defer alloc.free(from_str);

                    const reply_msg = MessageEnvelope{
                        .kind = .response,
                        .@"type" = AgentMsgType.task_result,
                        .id = result.id,
                        .from = from_str,
                        .to = reply_to_str,
                        .corr = msg.id,
                        .trace_id = result.trace_id,
                        .session_id = result.session_id,
                        .task_id = task_id,
                        .body = result.body,
                    };
                    ctx.send(reply_pid, reply_msg) catch {};
                }
            }
        }
    }

    ctx.note("planner: exiting", null) catch {};
    wctx.exited.store(true, .release);
}

// warden-1ud
/// Tool worker process thread.
/// Receives run_task, simulates work (1 ms sleep), sends task_result back to sender.
///
/// TODO(warden-foreign): swap native tool_worker entry for bridge.zig spawn here
/// when the foreign worker bridge is available.  The bridge would marshal the
/// task JSON to the external runtime and await its result before sending task_result.
fn toolWorkerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) {
        clock.sleepNs(1 * std.time.ns_per_ms);
    }

    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    const alloc = topo.allocator;

    ctx.note("tool_worker: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.run_task)) {
            const task_id = msg.task_id orelse "unknown";
            ctx.note("tool_worker: executing task", null) catch {};

            // Simulate work.
            clock.sleepNs(1 * std.time.ns_per_ms);

            const sender_proc = std.fmt.parseInt(u64, msg.from, 10) catch continue;
            const sender_pid = Pid{ .beam = topo.runtime.beam_id, .proc = sender_proc };

            const self_str = std.fmt.allocPrint(alloc, "{d}", .{ctx.pid.proc}) catch continue;
            defer alloc.free(self_str);
            const to_str = std.fmt.allocPrint(alloc, "{d}", .{sender_pid.proc}) catch continue;
            defer alloc.free(to_str);

            const result_msg = MessageEnvelope{
                .kind = .response,
                .@"type" = AgentMsgType.task_result,
                .id = msg.id,
                .from = self_str,
                .to = to_str,
                .corr = msg.id,
                .trace_id = msg.trace_id,
                .session_id = msg.session_id,
                .task_id = task_id,
                .body = .{ .string = "ok" },
            };
            ctx.send(sender_pid, result_msg) catch {};
        }
    }

    ctx.note("tool_worker: exiting", null) catch {};
    wctx.exited.store(true, .release);
}

// warden-1ud
/// Memory process thread.
/// Simplified stub; real implementation would maintain an in-memory key/value map.
fn memoryThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) {
        clock.sleepNs(1 * std.time.ns_per_ms);
    }

    const ctx = &wctx.ctx;
    ctx.note("memory_proc: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", AgentMsgType.shutdown)) break;
        // Future: dispatch on msg.body {op:"get"|"put", key, value}.
    }

    ctx.note("memory_proc: exiting", null) catch {};
    wctx.exited.store(true, .release);
}

// warden-1ud
/// Model router process thread.
/// Receives routing requests; returns stub model name "gpt-stub".
fn modelRouterThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) {
        clock.sleepNs(1 * std.time.ns_per_ms);
    }

    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    const alloc = topo.allocator;
    ctx.note("model_router: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.shutdown)) break;

        if (msg.kind == .request) {
            const sender_proc = std.fmt.parseInt(u64, msg.from, 10) catch continue;
            const sender_pid = Pid{ .beam = topo.runtime.beam_id, .proc = sender_proc };

            const self_str = std.fmt.allocPrint(alloc, "{d}", .{ctx.pid.proc}) catch continue;
            defer alloc.free(self_str);
            const to_str = std.fmt.allocPrint(alloc, "{d}", .{sender_pid.proc}) catch continue;
            defer alloc.free(to_str);

            const reply = MessageEnvelope{
                .kind = .response,
                .@"type" = "agent.model_reply",
                .id = msg.id,
                .from = self_str,
                .to = to_str,
                .corr = msg.id,
                .body = .{ .string = "gpt-stub" },
            };
            ctx.send(sender_pid, reply) catch {};
        }
    }

    ctx.note("model_router: exiting", null) catch {};
    wctx.exited.store(true, .release);
}

// warden-1ud
/// Watchdog process thread.
/// Receives health_check messages, replies with health_ok.
fn watchdogThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) {
        clock.sleepNs(1 * std.time.ns_per_ms);
    }

    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    const alloc = topo.allocator;
    ctx.note("watchdog: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", AgentMsgType.health_check)) {
            const sender_proc = std.fmt.parseInt(u64, msg.from, 10) catch continue;
            const sender_pid = Pid{ .beam = topo.runtime.beam_id, .proc = sender_proc };

            const self_str = std.fmt.allocPrint(alloc, "{d}", .{ctx.pid.proc}) catch continue;
            defer alloc.free(self_str);
            const to_str = std.fmt.allocPrint(alloc, "{d}", .{sender_pid.proc}) catch continue;
            defer alloc.free(to_str);

            const reply = MessageEnvelope{
                .kind = .response,
                .@"type" = AgentMsgType.health_ok,
                .id = msg.id,
                .from = self_str,
                .to = to_str,
                .corr = msg.id,
                .body = .null,
            };
            ctx.send(sender_pid, reply) catch {};
        }
    }

    ctx.note("watchdog: exiting", null) catch {};
    wctx.exited.store(true, .release);
}

// ─── Supervisor child spec entry stubs ───────────────────────────────────────
// The Supervisor registers each process via its ChildSpec. We provide no-op
// entry functions here so the supervisor's registry and restart logic work
// correctly. Actual execution happens on the dedicated OS threads started in
// Topology.start().

fn noop(_: ?*anyopaque) void {}

// ─── Topology ────────────────────────────────────────────────────────────────

// warden-1ud
/// The canonical agent topology:
///
///   root_sup
///   └── session_sup(session_id)
///       ├── planner_proc
///       ├── executor_sup
///       │   └── tool_worker_proc* (N workers)
///       ├── memory_proc
///       ├── model_router_proc
///       └── watchdog_proc
pub const Topology = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    config: SessionConfig,

    root_sup: Supervisor,
    session_sup: Supervisor,
    executor_sup: Supervisor,

    planner_pid: Pid,
    memory_pid: Pid,
    model_router_pid: Pid,
    watchdog_pid: Pid,
    tool_worker_pids: []Pid,

    /// Heap-allocated WorkerCtx for every spawned process.
    worker_ctxs: []*WorkerCtx,
    /// How many worker_ctxs were actually filled in by start().
    worker_ctxs_started: usize,

    /// Allocated child-id strings for tool workers (freed in deinit).
    tool_worker_ids: [][]u8,

    // warden-1ud
    pub fn init(
        allocator: std.mem.Allocator,
        runtime: *Runtime,
        config: SessionConfig,
    ) !Topology {
        const root_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler);
        const session_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler);
        const executor_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler);

        const dummy_pid = Pid{ .beam = 0, .proc = 0 };
        const worker_pids = try allocator.alloc(Pid, config.tool_worker_count);
        for (worker_pids) |*p| p.* = dummy_pid;

        // Total slots: planner + memory + model_router + watchdog + N tool workers.
        const total_ctxs = 4 + config.tool_worker_count;
        const ctxs = try allocator.alloc(*WorkerCtx, total_ctxs);

        const tw_ids = try allocator.alloc([]u8, config.tool_worker_count);

        return Topology{
            .allocator = allocator,
            .runtime = runtime,
            .config = config,
            .root_sup = root_sup,
            .session_sup = session_sup,
            .executor_sup = executor_sup,
            .planner_pid = dummy_pid,
            .memory_pid = dummy_pid,
            .model_router_pid = dummy_pid,
            .watchdog_pid = dummy_pid,
            .tool_worker_pids = worker_pids,
            .worker_ctxs = ctxs,
            .worker_ctxs_started = 0,
            .tool_worker_ids = tw_ids,
        };
    }

    // warden-1ud
    /// Join all worker threads, deinit Ctxs, free all memory.
    pub fn deinit(self: *Topology) void {
        // Join every started worker thread (ensures entry fn has fully returned).
        for (self.worker_ctxs[0..self.worker_ctxs_started]) |wctx| {
            if (wctx.thread) |t| t.join();
        }

        // Now safe to free the Ctx (and its heap-allocated ProcessLogger).
        for (self.worker_ctxs[0..self.worker_ctxs_started]) |wctx| {
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
        }
        self.allocator.free(self.worker_ctxs);

        for (self.tool_worker_ids) |id| self.allocator.free(id);
        self.allocator.free(self.tool_worker_ids);
        self.allocator.free(self.tool_worker_pids);

        self.executor_sup.deinit();
        self.session_sup.deinit();
        self.root_sup.deinit();
    }

    // warden-1ud
    /// Allocate and register a new WorkerCtx (ctx field uninitialised until wireWorker).
    fn allocWorkerCtx(self: *Topology, worker_index: usize) !*WorkerCtx {
        const wctx = try self.allocator.create(WorkerCtx);
        wctx.topology = self;
        wctx.ready = std.atomic.Value(bool).init(false);
        wctx.stopping = std.atomic.Value(bool).init(false);
        wctx.exited = std.atomic.Value(bool).init(false);
        wctx.thread = null;
        wctx.worker_index = worker_index;
        self.worker_ctxs[self.worker_ctxs_started] = wctx;
        self.worker_ctxs_started += 1;
        return wctx;
    }

    // warden-1ud
    /// Allocate mailbox, init Ctx, spawn the OS thread, then signal ready.
    fn wireAndSpawn(
        self: *Topology,
        wctx: *WorkerCtx,
        pid: Pid,
        comptime thread_fn: fn (*WorkerCtx) void,
    ) !void {
        try self.runtime.allocMailbox(pid, .{});
        wctx.ctx = try Ctx.init(
            self.runtime,
            pid,
            self.config.log_dir,
            self.config.storage_base,
        );
        // Spawn the OS thread; it will spin on ready until we release it.
        wctx.thread = try std.Thread.spawn(.{}, thread_fn, .{wctx});
        wctx.ready.store(true, .release);
    }

    // warden-1ud
    /// Boot all supervisors and processes.
    pub fn start(self: *Topology) !void {
        // ── planner ──────────────────────────────────────────────────────────
        const planner_wctx = try self.allocWorkerCtx(0);
        // Register planner in session_sup (noop entry — actual work on OS thread).
        const planner_pid = try self.session_sup.startChild(ChildSpec{
            .id = "planner",
            .kind = .native_worker,
            .restart = .temporary, // topology manages lifecycle directly
            .entry = noop,
            .ctx = null,
        });
        self.planner_pid = planner_pid;
        try self.wireAndSpawn(planner_wctx, planner_pid, plannerThread);

        // ── tool workers ─────────────────────────────────────────────────────
        for (0..self.config.tool_worker_count) |i| {
            const tw_wctx = try self.allocWorkerCtx(i);
            const tw_id = try std.fmt.allocPrint(self.allocator, "tool_worker_{d}", .{i});
            self.tool_worker_ids[i] = tw_id;

            const tw_pid = try self.executor_sup.startChild(ChildSpec{
                .id = tw_id,
                .kind = .native_worker,
                .restart = .temporary,
                .entry = noop,
                .ctx = null,
            });
            self.tool_worker_pids[i] = tw_pid;
            try self.wireAndSpawn(tw_wctx, tw_pid, toolWorkerThread);
        }

        // ── memory ───────────────────────────────────────────────────────────
        const mem_wctx = try self.allocWorkerCtx(0);
        const mem_pid = try self.session_sup.startChild(ChildSpec{
            .id = "memory",
            .kind = .native_worker,
            .restart = .temporary,
            .entry = noop,
            .ctx = null,
        });
        self.memory_pid = mem_pid;
        try self.wireAndSpawn(mem_wctx, mem_pid, memoryThread);

        // ── model router ─────────────────────────────────────────────────────
        const router_wctx = try self.allocWorkerCtx(0);
        const router_pid = try self.session_sup.startChild(ChildSpec{
            .id = "model_router",
            .kind = .native_worker,
            .restart = .temporary,
            .entry = noop,
            .ctx = null,
        });
        self.model_router_pid = router_pid;
        try self.wireAndSpawn(router_wctx, router_pid, modelRouterThread);

        // ── watchdog ─────────────────────────────────────────────────────────
        const wd_wctx = try self.allocWorkerCtx(0);
        const wd_pid = try self.session_sup.startChild(ChildSpec{
            .id = "watchdog",
            .kind = .native_worker,
            .restart = .temporary,
            .entry = noop,
            .ctx = null,
        });
        self.watchdog_pid = wd_pid;
        try self.wireAndSpawn(wd_wctx, wd_pid, watchdogThread);
    }

    // warden-1ud
    /// Graceful teardown: signal all workers then shut down supervisors.
    /// Must be called before deinit().
    pub fn shutdown(self: *Topology) !void {
        // 1. Set stopping flag so loops exit without waiting for a message.
        for (self.worker_ctxs[0..self.worker_ctxs_started]) |wctx| {
            wctx.stopping.store(true, .release);
        }

        // 2. Send shutdown envelopes so blocked recv() calls return quickly.
        const from_str = "0";
        const fixed_pids = [_]Pid{
            self.planner_pid,
            self.memory_pid,
            self.model_router_pid,
            self.watchdog_pid,
        };
        for (fixed_pids) |pid| {
            if (pid.proc == 0) continue;
            self.sendShutdown(pid, from_str);
        }
        for (self.tool_worker_pids) |pid| {
            if (pid.proc == 0) continue;
            self.sendShutdown(pid, from_str);
        }

        // 3. Shut down supervisors bottom-up (marks registry entries dead).
        self.executor_sup.shutdown() catch {};
        self.session_sup.shutdown() catch {};
        self.root_sup.shutdown() catch {};
    }

    /// Write a shutdown envelope directly into a process mailbox.
    fn sendShutdown(self: *Topology, pid: Pid, from_str: []const u8) void {
        const mb = self.runtime.getMailbox(pid) orelse return;
        const to_str = std.fmt.allocPrint(self.allocator, "{d}", .{pid.proc}) catch return;
        defer self.allocator.free(to_str);
        const msg = MessageEnvelope{
            .kind = .signal,
            .@"type" = AgentMsgType.shutdown,
            .id = "shutdown",
            .from = from_str,
            .to = to_str,
            .body = .null,
        };
        _ = mb.enqueue(msg) catch {};
        self.runtime.scheduler.notifyMessage(pid);
    }
};
