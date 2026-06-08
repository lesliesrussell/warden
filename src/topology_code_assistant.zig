// warden-7hi
//
// Code-assistant session topology.
//
// Shape:
//   root_sup
//   └── session_sup(session_id)
//       ├── planner_proc          — decomposes user requests into tool calls
//       ├── executor_sup
//       │   ├── shell_worker      — runs shell commands
//       │   ├── lsp_worker        — language server queries (hover, refs, diag)
//       │   └── file_worker       — read / write / patch files
//       ├── memory_proc           — context window manager; evicts oldest entries
//       └── watchdog_proc         — monitors token budget; pauses planner on overflow
//
// Design notes:
//   - memory_proc stores conversation history in proc-state so a supervisor
//     restart does not lose context.
//   - watchdog_proc reads a token_count metric from memory_proc and pauses
//     the planner when the budget is exceeded, resuming after the user acks.
//   - Tool workers are one_for_one under executor_sup: a crashed shell command
//     does not affect LSP or file ops.

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

// warden-7hi
pub const MsgType = struct {
    pub const user_request = "ca.user_request";
    pub const tool_call = "ca.tool_call";
    pub const tool_result = "ca.tool_result";
    pub const context_push = "ca.context_push";
    pub const context_query = "ca.context_query";
    pub const context_reply = "ca.context_reply";
    pub const token_count = "ca.token_count";
    pub const budget_exceeded = "ca.budget_exceeded";
    pub const budget_ok = "ca.budget_ok";
    pub const shutdown = "ca.shutdown";
    pub const health_check = "ca.health_check";
    pub const health_ok = "ca.health_ok";
};

// warden-7hi
pub const Config = struct {
    session_id: []const u8,
    token_budget: u64 = 128_000,
    log_dir: []const u8,
    storage_base: []const u8,
};

// warden-7hi
pub const WorkerCtx = struct {
    ctx: Ctx,
    topology: *CodeAssistantTopology,
    ready: std.atomic.Value(bool),
    stopping: std.atomic.Value(bool),
    exited: std.atomic.Value(bool),
    thread: ?std.Thread,
};

fn matchAny(msg: *const MessageEnvelope) bool {
    _ = msg;
    return true;
}

// warden-7hi
// Planner: receives user_request, decomposes into tool_call messages,
// collects results, returns synthesized response.
fn plannerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;

    ctx.note("planner: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;

        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.user_request)) {
            // Push context entry to memory_proc
            const push = beam.MessageEnvelope{
                .kind = .request,
                .@"type" = MsgType.context_push,
                .id = "ctx-push",
                .from = "planner",
                .to = "memory",
                .body = msg.body,
                .session_id = topo.config.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.memory_pid, push) catch {};

            // Forward tool call to shell_worker (simplified: one tool per request)
            const call = beam.MessageEnvelope{
                .kind = .request,
                .@"type" = MsgType.tool_call,
                .id = "tool-call",
                .from = "planner",
                .to = "shell",
                .body = msg.body,
                .reply_to = "planner",
                .corr = msg.id,
                .session_id = topo.config.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.shell_pid, call) catch {};

            // Await tool_result
            const result_opt = ctx.recv(matchAny, 5000) catch null;
            if (result_opt) |result| {
                ctx.note("planner: got tool result", null) catch {};
                // Reply to original caller if reply_to is set
                if (msg.reply_to) |reply_pid_str| {
                    _ = reply_pid_str;
                    // In a real system: parse reply_pid_str → Pid, send result
                    _ = result;
                }
            }
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Shell worker: receives tool_call, simulates shell execution, replies with result.
// In production: wrap std.process.Child to run the command.
fn shellWorkerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;

    ctx.note("shell_worker: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;
        if (!std.mem.eql(u8, msg.@"type", MsgType.tool_call)) continue;

        {
            // TODO(warden-foreign): replace with real shell execution via std.process.Child
            ctx.note("shell_worker: executing command", null) catch {};
            clock.sleepNs(2 * std.time.ns_per_ms); // simulate work

            const result = beam.MessageEnvelope{
                .kind = .response,
                .@"type" = MsgType.tool_result,
                .id = "tool-result",
                .from = "shell",
                .to = msg.reply_to orelse "planner",
                .corr = msg.id,
                .body = .{ .string = "exit_code=0\noutput=ok" },
                .session_id = msg.session_id,
                .task_id = msg.task_id,
            };
            if (msg.reply_to != null) {
                ctx.send(topo.planner_pid, result) catch {};
            }
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// LSP worker: language server queries. Stub — replace with real LSP protocol.
fn lspWorkerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("lsp_worker: ready", null) catch {};
    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;
        // TODO(warden-foreign): real LSP over bridge_client.py
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// File worker: read / write / patch files via proc-state namespace.
fn fileWorkerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("file_worker: ready", null) catch {};
    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;
        // TODO(warden-foreign): real file ops
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Memory proc: manages context window. Stores entries in proc-state so
// they survive supervisor restarts. Evicts oldest when approaching limit.
fn memoryThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("memory: ready", null) catch {};

    var entry_count: u32 = 0;
    const max_entries: u32 = 200;

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.context_push)) {
            entry_count += 1;
            if (entry_count > max_entries) {
                // Evict oldest — in production: remove earliest proc-state entry
                entry_count = max_entries;
                ctx.note("memory: evicted oldest context entry", null) catch {};
            }
            // Persist to proc-state for restart durability
            const key = std.fmt.allocPrint(ctx.runtime.allocator, "entry-{d}", .{entry_count}) catch continue;
            defer ctx.runtime.allocator.free(key);
            ctx.fsWrite(.proc_state, key, "context") catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Watchdog: monitors token budget. Pauses planner when budget exceeded.
// Resumes planner when budget_ok received (e.g. after user acks or context pruned).
fn watchdogThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) clock.sleepNs(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("watchdog: ready", null) catch {};

    var planner_paused = false;

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 100) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.budget_exceeded) and !planner_paused) {
            ctx.note("watchdog: token budget exceeded — pausing planner", null) catch {};
            ctx.pause(topo.planner_pid) catch {};
            planner_paused = true;
        }

        if (std.mem.eql(u8, msg.@"type", MsgType.budget_ok) and planner_paused) {
            ctx.note("watchdog: budget restored — resuming planner", null) catch {};
            ctx.resume_(topo.planner_pid) catch {};
            planner_paused = false;
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
pub const CodeAssistantTopology = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    config: Config,

    root_sup: Supervisor,
    session_sup: Supervisor,
    executor_sup: Supervisor,

    planner_pid: Pid,
    shell_pid: Pid,
    lsp_pid: Pid,
    file_pid: Pid,
    memory_pid: Pid,
    watchdog_pid: Pid,

    workers: [6]*WorkerCtx,

    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, config: Config) !CodeAssistantTopology {
        return CodeAssistantTopology{
            .allocator = allocator,
            .runtime = runtime,
            .config = config,
            .root_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler),
            .session_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler),
            .executor_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler),
            .planner_pid = undefined,
            .shell_pid = undefined,
            .lsp_pid = undefined,
            .file_pid = undefined,
            .memory_pid = undefined,
            .watchdog_pid = undefined,
            .workers = undefined,
        };
    }

    pub fn deinit(self: *CodeAssistantTopology) void {
        for (self.workers) |wctx| {
            wctx.stopping.store(true, .release);
        }
        for (self.workers) |wctx| {
            if (wctx.thread) |t| t.join();
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
        }
        self.executor_sup.deinit();
        self.session_sup.deinit();
        self.root_sup.deinit();
    }

    pub fn start(self: *CodeAssistantTopology) !void {
        // std.Thread.spawn requires comptime-known functions — spawn each role explicitly.
        const spawnWorker = struct {
            fn do(topo: *CodeAssistantTopology, idx: usize, pid_field: *Pid, comptime thread_fn: fn (*WorkerCtx) void) !void {
                const wctx = try topo.allocator.create(WorkerCtx);
                wctx.topology = topo;
                wctx.ready = std.atomic.Value(bool).init(false);
                wctx.stopping = std.atomic.Value(bool).init(false);
                wctx.exited = std.atomic.Value(bool).init(false);
                wctx.thread = null;
                const pid = try topo.runtime.registry.spawn(.native_worker, null, .{});
                pid_field.* = pid;
                topo.workers[idx] = wctx;
                try topo.runtime.allocMailbox(pid, .{});
                wctx.ctx = try Ctx.init(topo.runtime, pid, topo.config.log_dir, topo.config.storage_base);
                wctx.ready.store(true, .release);
                wctx.thread = try std.Thread.spawn(.{}, thread_fn, .{wctx});
            }
        }.do;

        try spawnWorker(self, 0, &self.planner_pid, plannerThread);
        try spawnWorker(self, 1, &self.shell_pid, shellWorkerThread);
        try spawnWorker(self, 2, &self.lsp_pid, lspWorkerThread);
        try spawnWorker(self, 3, &self.file_pid, fileWorkerThread);
        try spawnWorker(self, 4, &self.memory_pid, memoryThread);
        try spawnWorker(self, 5, &self.watchdog_pid, watchdogThread);
    }

    pub fn shutdown(self: *CodeAssistantTopology) void {
        for (self.workers) |wctx| {
            wctx.stopping.store(true, .release);
        }
        for (self.workers) |wctx| {
            if (wctx.thread) |t| t.join();
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
        }
        self.executor_sup.deinit();
        self.session_sup.deinit();
        self.root_sup.deinit();
    }
};
