// warden-7q1

const std = @import("std");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");
const mailbox_mod = @import("mailbox.zig");
const scheduler_mod = @import("scheduler.zig");
const policy_mod = @import("policy.zig");
const logger_mod = @import("logger.zig");
const storage_mod = @import("storage.zig");
const supervisor_mod = @import("supervisor.zig");

// Re-export key types for callers
pub const Pid = types.Pid;
pub const ProcessKind = types.ProcessKind;
pub const ProcessState = types.ProcessState;
pub const ActivityClass = types.ActivityClass;
pub const PolicyEnvelope = types.PolicyEnvelope;
pub const MessageEnvelope = types.MessageEnvelope;
pub const MessageKind = types.MessageKind;
pub const MessagePriority = types.MessagePriority;
pub const ExitReason = supervisor_mod.ExitReason;
pub const Namespace = storage_mod.Namespace;
pub const Stat = storage_mod.Stat;

const Registry = registry_mod.Registry;
const Mailbox = mailbox_mod.Mailbox;
const Scheduler = scheduler_mod.Scheduler;
const PolicyEngine = policy_mod.PolicyEngine;
const ProcessLogger = logger_mod.ProcessLogger;
const StorageView = storage_mod.StorageView;

// warden-7q1
/// A monitor reference tracking a watcher→target relationship.
pub const MonitorRef = struct {
    id: u64,
    watcher: Pid,
    target: Pid,
};

// warden-7q1
/// Options for spawning a new process.
pub const SpawnOpts = struct {
    supervisor_pid: ?Pid = null,
    policy: PolicyEnvelope = .{},
};

// warden-7q1
/// A patch to apply to a process policy.
pub const PolicyPatch = struct {
    activity_class: ?ActivityClass = null,
    max_mailbox_len: ?u32 = null,
};

// warden-7q1
/// Internal trampoline adapter — bridges the entry fn pointer convention
/// required by Scheduler.submit (fn(*anyopaque) void) to the beam.* API
/// convention (fn(?*anyopaque) void).
/// The trampoline owns its own heap allocation and frees itself on completion.
const TrampolineCtx = struct {
    allocator: std.mem.Allocator,
    entry: *const fn (ctx: ?*anyopaque) void,
    ctx: ?*anyopaque,
};

fn trampolineRun(raw: *anyopaque) void {
    // warden-7q1
    const tc: *TrampolineCtx = @ptrCast(@alignCast(raw));
    const allocator = tc.allocator;
    tc.entry(tc.ctx);
    // Free self after the entry function returns.
    allocator.destroy(tc);
}

// warden-7q1
/// The central runtime context.
///
/// Runtime is heap-allocated (via `init`) so that internal subsystems
/// can hold stable pointers to each other (e.g. PolicyEngine holds *Registry,
/// Scheduler holds *Registry). The caller must call `deinit()` and
/// `allocator.destroy(rt)` when done, or use the helper `destroy()`.
pub const Runtime = struct {
    allocator: std.mem.Allocator,
    beam_id: u32,
    registry: Registry,
    scheduler: *Scheduler,
    policy: PolicyEngine,
    /// Per-process mailboxes, keyed by pid.proc.
    mailboxes: std.AutoHashMap(u64, *Mailbox),
    mailboxes_mutex: std.Thread.Mutex,

    // warden-7q1
    /// Allocate a Runtime on the heap. Returns an owned pointer.
    /// `registry` and `policy` are stored by value inside this allocation,
    /// so pointers into them are stable for the Runtime's lifetime.
    pub fn init(allocator: std.mem.Allocator, beam_id: u32) !*Runtime {
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.beam_id = beam_id;
        self.registry = Registry.init(allocator, beam_id);
        self.mailboxes_mutex = .{};
        self.mailboxes = std.AutoHashMap(u64, *Mailbox).init(allocator);

        // Scheduler self-allocates — it holds *Registry so must come after registry init.
        self.scheduler = try Scheduler.init(allocator, &self.registry, 1);
        errdefer self.scheduler.deinit();

        // PolicyEngine holds *Registry — must come after registry is in place.
        self.policy = PolicyEngine.init(allocator, &self.registry);

        return self;
    }

    // warden-7q1
    pub fn deinit(self: *Runtime) void {
        self.policy.deinit();
        self.scheduler.deinit();

        // Free all mailboxes.
        self.mailboxes_mutex.lock();
        var it = self.mailboxes.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.mailboxes.deinit();
        self.mailboxes_mutex.unlock();

        self.registry.deinit();
    }

    // warden-7q1
    /// Convenience: deinit then free the heap allocation.
    pub fn destroy(self: *Runtime) void {
        const allocator = self.allocator;
        self.deinit();
        allocator.destroy(self);
    }

    // warden-7q1
    /// Start the scheduler thread pool (no-op — threads started in init).
    pub fn start(self: *Runtime, worker_count: usize) !void {
        _ = self;
        _ = worker_count;
    }

    // warden-7q1
    /// Allocate a mailbox for a process and register it.
    pub fn allocMailbox(self: *Runtime, pid: Pid, policy: PolicyEnvelope) !void {
        const mb = try self.allocator.create(Mailbox);
        errdefer self.allocator.destroy(mb);
        mb.* = Mailbox.init(self.allocator, policy);

        self.mailboxes_mutex.lock();
        defer self.mailboxes_mutex.unlock();
        try self.mailboxes.put(pid.proc, mb);
    }

    // warden-7q1
    /// Look up the mailbox for a pid. Returns null if not found.
    pub fn getMailbox(self: *Runtime, pid: Pid) ?*Mailbox {
        self.mailboxes_mutex.lock();
        defer self.mailboxes_mutex.unlock();
        return self.mailboxes.get(pid.proc);
    }
};

// warden-7q1
/// Per-process execution context. Every running process has one.
pub const Ctx = struct {
    pid: Pid,
    runtime: *Runtime,
    logger: *ProcessLogger,
    storage: StorageView,

    // warden-7q1
    pub fn init(
        runtime: *Runtime,
        pid: Pid,
        log_dir: []const u8,
        storage_base: []const u8,
    ) !Ctx {
        // Open (or create) the log directory.
        try std.fs.cwd().makePath(log_dir);
        const dir = try std.fs.cwd().openDir(log_dir, .{});

        // ProcessLogger must not be moved after init; heap-allocate it.
        const pl = try runtime.allocator.create(ProcessLogger);
        errdefer runtime.allocator.destroy(pl);
        pl.* = try ProcessLogger.init(runtime.allocator, runtime.beam_id, pid.proc, dir);
        errdefer pl.deinit();

        const sv = try StorageView.init(
            runtime.allocator,
            storage_base,
            pid,
            PolicyEnvelope{},
        );
        // sv.deinit() can be called if later steps fail;

        return Ctx{
            .pid = pid,
            .runtime = runtime,
            .logger = pl,
            .storage = sv,
        };
    }

    // warden-7q1
    pub fn deinit(self: *Ctx) void {
        self.storage.deinit();
        self.logger.deinit();
        self.runtime.allocator.destroy(self.logger);
    }

    // ─── Process APIs ────────────────────────────────────────────────────────

    // warden-7q1
    /// Return own PID.
    pub fn self_(ctx: *Ctx) Pid {
        return ctx.pid;
    }

    // warden-7q1
    /// Send a message to another process (fire-and-forget).
    pub fn send(ctx: *Ctx, to: Pid, msg: MessageEnvelope) !void {
        const mb = ctx.runtime.getMailbox(to) orelse return error.NoSuchProcess;
        const result = try mb.enqueue(msg);
        switch (result) {
            .ok => {},
            .dropped_for_room => {},
            .rejected => return error.MailboxFull,
            .escalate => return error.MailboxFull,
            .throttle => return error.MailboxFull,
        }
        // Emit send event to own logger.
        try ctx.logger.emit(.{
            .send = .{
                .msg_id = msg.id,
                .to = msg.to,
                .msg_type = msg.@"type",
                .corr = msg.corr,
                .trace_id = msg.trace_id,
                .task_id = msg.task_id,
            },
        }, null);
        // Notify scheduler that recipient has mail.
        ctx.runtime.scheduler.notifyMessage(to);
    }

    // warden-7q1
    /// Blocking call: send a message and wait for the correlated reply.
    /// Returns the reply envelope, or error.Timeout if no reply arrives.
    pub fn call(ctx: *Ctx, to: Pid, msg: MessageEnvelope, timeout_ms: u64) !MessageEnvelope {
        // Use msg.id as the correlation ID that the reply should echo.
        var tagged_msg = msg;
        tagged_msg.corr = msg.id;
        try ctx.send(to, tagged_msg);

        // Spin-wait for a reply with matching corr.
        const start_ms = @as(u64, @intCast(std.time.milliTimestamp()));
        while (true) {
            const elapsed_ms = @as(u64, @intCast(std.time.milliTimestamp())) -| start_ms;
            if (elapsed_ms >= timeout_ms) break;

            const mb = ctx.runtime.getMailbox(ctx.pid) orelse return error.NoSuchProcess;
            const reply_opt = scanMailboxCorr(mb, msg.id);
            if (reply_opt) |r| return r;

            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
        return error.Timeout;
    }

    // warden-7q1
    /// Send a reply to reply_to pid.
    pub fn reply(ctx: *Ctx, reply_to: Pid, msg: MessageEnvelope) !void {
        try ctx.send(reply_to, msg);
    }

    // warden-7q1
    /// Receive the next message matching match_fn from own mailbox.
    /// Returns null on timeout.
    pub fn recv(
        ctx: *Ctx,
        match_fn: *const fn (msg: *const MessageEnvelope) bool,
        timeout_ms: u64,
    ) !?MessageEnvelope {
        const start_ms = @as(u64, @intCast(std.time.milliTimestamp()));
        while (true) {
            const elapsed_ms = @as(u64, @intCast(std.time.milliTimestamp())) -| start_ms;
            if (elapsed_ms >= timeout_ms) return null;

            const mb = ctx.runtime.getMailbox(ctx.pid) orelse return error.NoSuchProcess;
            const msg_opt = scanMailboxFn(mb, match_fn);
            if (msg_opt) |msg| {
                try ctx.logger.emit(.{
                    .recv = .{
                        .msg_id = msg.id,
                        .from = msg.from,
                        .msg_type = msg.@"type",
                        .corr = msg.corr,
                        .trace_id = msg.trace_id,
                        .task_id = msg.task_id,
                    },
                }, null);
                return msg;
            }

            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }

    // warden-7q1
    /// Spawn a new process under an optional supervisor.
    pub fn spawn(
        ctx: *Ctx,
        kind: ProcessKind,
        entry: *const fn (?*anyopaque) void,
        opts: SpawnOpts,
    ) !Pid {
        const rt = ctx.runtime;
        const child_pid = try rt.registry.spawn(kind, opts.supervisor_pid, opts.policy);
        try rt.allocMailbox(child_pid, opts.policy);

        // Trampoline: Scheduler.submit wants fn(*anyopaque) void.
        const tc = try rt.allocator.create(TrampolineCtx);
        tc.* = .{ .allocator = rt.allocator, .entry = entry, .ctx = null };

        // makeReady drives starting→ready and adds to ready list.
        try rt.scheduler.makeReady(child_pid);
        try rt.scheduler.submit(child_pid, trampolineRun, tc);

        return child_pid;
    }

    // warden-7q1
    /// Record a monitor reference (simplified — no exit propagation yet).
    pub fn monitor(ctx: *Ctx, pid: Pid) !MonitorRef {
        const id = std.crypto.random.int(u64);
        try ctx.logger.emit(.{
            .monitor = .{
                .target_pid = pid.proc,
                .target_beam = pid.beam,
            },
        }, null);
        return MonitorRef{ .id = id, .watcher = ctx.pid, .target = pid };
    }

    // warden-7q1
    /// Record a link (simplified — no bidirectional exit propagation yet).
    pub fn link(ctx: *Ctx, pid: Pid) !void {
        _ = ctx;
        _ = pid;
    }

    // warden-7q1
    /// Signal a process to exit.
    pub fn exit(ctx: *Ctx, pid: Pid, reason: ExitReason) !void {
        _ = reason;
        ctx.runtime.registry.transition(pid, .exiting) catch {};
        ctx.runtime.registry.transition(pid, .dead) catch {};
    }

    // warden-7q1
    /// Pause a process.
    pub fn pause(ctx: *Ctx, pid: Pid) !void {
        try ctx.runtime.scheduler.pause(pid);
    }

    // warden-7q1
    /// Resume a paused process.
    pub fn resume_(ctx: *Ctx, pid: Pid) !void {
        try ctx.runtime.scheduler.resume_(pid);
    }

    // warden-7q1
    /// Apply a policy patch to a process entry.
    pub fn setPolicy(ctx: *Ctx, pid: Pid, patch: PolicyPatch) !void {
        const entry = ctx.runtime.registry.lookup(pid) orelse return error.NoSuchProcess;
        if (patch.activity_class) |ac| {
            entry.policy.activity_class = ac;
        }
        if (patch.max_mailbox_len) |ml| {
            entry.policy.max_mailbox_len = ml;
        }
    }

    // warden-7q1
    /// Promote a process to a higher activity class via the policy engine.
    pub fn promote(ctx: *Ctx, pid: Pid, class: ActivityClass, ttl_ms: u64, reason: []const u8) !void {
        try ctx.runtime.policy.promote(pid, class, ttl_ms, reason);
    }

    // warden-7q1
    /// Demote a process to normal activity class via the policy engine.
    pub fn demote(ctx: *Ctx, pid: Pid, reason: []const u8) !void {
        try ctx.runtime.policy.demote(pid, reason);
    }

    // ─── Logging APIs ────────────────────────────────────────────────────────

    // warden-7q1
    /// Emit a log note at the given level.
    pub fn log(ctx: *Ctx, level: []const u8, message: []const u8, fields: ?std.json.ObjectMap) !void {
        try ctx.logger.note(level, message, fields);
    }

    // warden-7q1
    /// Emit an info note.
    pub fn note(ctx: *Ctx, message: []const u8, fields: ?std.json.ObjectMap) !void {
        try ctx.logger.note("info", message, fields);
    }

    // warden-7q1
    /// Emit a metric event.
    pub fn metric(ctx: *Ctx, name: []const u8, value: f64, fields: ?std.json.ObjectMap) !void {
        try ctx.logger.metric(name, value, fields);
    }

    // warden-7q1
    /// Emit a warning.
    pub fn warning(ctx: *Ctx, message: []const u8, fields: ?std.json.ObjectMap) !void {
        try ctx.logger.warn(message, fields);
    }

    // warden-7q1
    /// Emit an error event.
    pub fn err(ctx: *Ctx, message: []const u8, fields: ?std.json.ObjectMap) !void {
        try ctx.logger.err(message, fields);
    }

    // ─── Storage APIs ────────────────────────────────────────────────────────

    // warden-7q1
    /// Read a file from the given namespace.
    pub fn fsRead(ctx: *Ctx, ns: Namespace, path: []const u8) ![]u8 {
        return ctx.storage.read(ns, path);
    }

    // warden-7q1
    /// Write a file in the given namespace.
    pub fn fsWrite(ctx: *Ctx, ns: Namespace, path: []const u8, data: []const u8) !void {
        return ctx.storage.write(ns, path, data);
    }

    // warden-7q1
    /// Append to a file in the given namespace.
    pub fn fsAppend(ctx: *Ctx, ns: Namespace, path: []const u8, data: []const u8) !void {
        return ctx.storage.append(ns, path, data);
    }

    // warden-7q1
    /// List entries in a directory within the given namespace.
    pub fn fsList(ctx: *Ctx, ns: Namespace, path: []const u8) ![][]u8 {
        return ctx.storage.list(ns, path);
    }

    // warden-7q1
    /// Delete a file in the given namespace.
    pub fn fsDelete(ctx: *Ctx, ns: Namespace, path: []const u8) !void {
        return ctx.storage.delete(ns, path);
    }

    // warden-7q1
    /// Stat a path in the given namespace.
    pub fn fsStat(ctx: *Ctx, ns: Namespace, path: []const u8) !Stat {
        return ctx.storage.stat(ns, path);
    }
};

// ─── Internal mailbox scan helpers ───────────────────────────────────────────

// warden-7q1
/// Scan mailbox for first message whose corr matches corr_id.
/// Removes and returns it, or returns null.
fn scanMailboxCorr(mb: *Mailbox, corr_id: []const u8) ?MessageEnvelope {
    mb.mu.lock();
    defer mb.mu.unlock();
    for (mb.queue.items, 0..) |msg, i| {
        if (msg.corr) |c| {
            if (std.mem.eql(u8, c, corr_id)) {
                return mb.queue.orderedRemove(i);
            }
        }
    }
    return null;
}

// warden-7q1
/// Scan mailbox using a runtime fn-pointer predicate.
/// Removes and returns the first matching message, or returns null.
fn scanMailboxFn(
    mb: *Mailbox,
    match_fn: *const fn (msg: *const MessageEnvelope) bool,
) ?MessageEnvelope {
    mb.mu.lock();
    defer mb.mu.unlock();
    for (mb.queue.items, 0..) |*msg, i| {
        if (match_fn(msg)) {
            return mb.queue.orderedRemove(i);
        }
    }
    return null;
}
