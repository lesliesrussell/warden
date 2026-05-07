// warden-dhx

const std = @import("std");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");
const scheduler_mod = @import("scheduler.zig");

const Pid = types.Pid;
const ProcessKind = types.ProcessKind;
const PolicyEnvelope = types.PolicyEnvelope;
const Registry = registry_mod.Registry;
const Scheduler = scheduler_mod.Scheduler;

// warden-dhx
pub const RestartStrategy = enum {
    one_for_one,
    one_for_all,
    rest_for_one,
    transient,
    temporary,
};

// warden-dhx
pub const ExitReason = enum {
    normal,
    abnormal,
    killed,
    shutdown,
};

// warden-dhx
pub const ChildSpec = struct {
    id: []const u8,
    kind: ProcessKind,
    restart: RestartStrategy,
    shutdown_timeout_ms: u64 = 5000,
    entry: *const fn (ctx: ?*anyopaque) void,
    ctx: ?*anyopaque = null,
};

// warden-dhx
pub const ChildState = enum { starting, running, exiting, dead };

// warden-dhx
pub const Child = struct {
    spec: ChildSpec,
    pid: Pid,
    state: ChildState,
    restart_count: u32,
    started_at_ms: i64,
};

// warden-dhx
pub const SupervisorError = error{
    ChildNotFound,
    MaxRestartIntensityExceeded,
    AlreadyStarted,
};

// warden-dhx
/// Trampoline: scheduler.submit requires `*const fn(*anyopaque) void`, but
/// ChildSpec.entry uses `?*anyopaque`. This adapter bridges the gap.
const TrampolineCtx = struct {
    entry: *const fn (ctx: ?*anyopaque) void,
    ctx: ?*anyopaque,
};

fn trampolineRun(raw: *anyopaque) void {
    const tc: *TrampolineCtx = @ptrCast(@alignCast(raw));
    tc.entry(tc.ctx);
}

// warden-dhx
// Note: in Zig 0.15, std.ArrayList is the *unmanaged* variant.
// Initialise with `.empty`; pass allocator to every mutating call.
pub const Supervisor = struct {
    allocator: std.mem.Allocator,
    pid: Pid,
    registry: *Registry,
    scheduler: *Scheduler,
    /// Ordered list of children; insertion order matters for rest_for_one.
    children: std.ArrayList(Child),
    mutex: std.Thread.Mutex,

    /// Max restart intensity parameters.
    max_restarts: u32,
    intensity_window_ms: u64,
    /// Rolling window of restart timestamps (milliseconds).
    restart_timestamps: std.ArrayList(i64),

    /// Heap-allocated trampoline contexts for child tasks.
    trampoline_pool: std.ArrayList(*TrampolineCtx),

    // warden-dhx
    pub fn init(allocator: std.mem.Allocator, registry: *Registry, scheduler: *Scheduler) !Supervisor {
        const sup_pid = try registry.spawn(.native_supervisor, null, PolicyEnvelope{});
        return Supervisor{
            .allocator = allocator,
            .pid = sup_pid,
            .registry = registry,
            .scheduler = scheduler,
            .children = .empty,
            .mutex = .{},
            .max_restarts = 3,
            .intensity_window_ms = 5000,
            .restart_timestamps = .empty,
            .trampoline_pool = .empty,
        };
    }

    // warden-dhx
    pub fn deinit(self: *Supervisor) void {
        for (self.trampoline_pool.items) |tc| {
            self.allocator.destroy(tc);
        }
        self.trampoline_pool.deinit(self.allocator);
        self.children.deinit(self.allocator);
        self.restart_timestamps.deinit(self.allocator);
    }

    // warden-dhx
    /// Start a child from spec. Allocates PID via registry, submits to scheduler.
    pub fn startChild(self: *Supervisor, spec: ChildSpec) !Pid {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Reject duplicate live child IDs.
        for (self.children.items) |c| {
            if (std.mem.eql(u8, c.spec.id, spec.id) and c.state != .dead) {
                return SupervisorError.AlreadyStarted;
            }
        }

        const child_pid = try self.registry.spawn(spec.kind, self.pid, PolicyEnvelope{});

        const child = Child{
            .spec = spec,
            .pid = child_pid,
            .state = .starting,
            .restart_count = 0,
            .started_at_ms = std.time.milliTimestamp(),
        };
        try self.children.append(self.allocator, child);

        const idx = self.children.items.len - 1;
        try self.submitChildLocked(idx);
        self.children.items[idx].state = .running;

        return child_pid;
    }

    // warden-dhx
    /// Called when a child exits. Applies restart strategy.
    pub fn childExited(self: *Supervisor, pid: Pid, reason: ExitReason) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const child_idx = self.findChildIndex(pid) orelse return SupervisorError.ChildNotFound;
        self.children.items[child_idx].state = .dead;

        const strategy = self.children.items[child_idx].spec.restart;

        switch (strategy) {
            .temporary => {
                // Never restart regardless of reason.
                return;
            },
            .transient => {
                // Restart only on abnormal exit.
                if (reason == .normal or reason == .shutdown) return;
                try self.checkIntensityLocked();
                try self.restartOneLocked(child_idx);
            },
            .one_for_one => {
                try self.checkIntensityLocked();
                try self.restartOneLocked(child_idx);
            },
            .one_for_all => {
                try self.checkIntensityLocked();
                // Terminate all other live children.
                var i = self.children.items.len;
                while (i > 0) {
                    i -= 1;
                    if (i == child_idx) continue;
                    self.markDeadLocked(i);
                }
                // Restart all children in original order.
                for (0..self.children.items.len) |j| {
                    self.children.items[j].state = .starting;
                    try self.launchChildLocked(j);
                    self.children.items[j].state = .running;
                }
            },
            .rest_for_one => {
                try self.checkIntensityLocked();
                // Terminate children started AFTER the failed one (reverse order).
                var i = self.children.items.len;
                while (i > 0) {
                    i -= 1;
                    if (i <= child_idx) break;
                    self.markDeadLocked(i);
                }
                // Restart failed child + all later children in original order.
                for (child_idx..self.children.items.len) |j| {
                    self.children.items[j].state = .starting;
                    try self.launchChildLocked(j);
                    self.children.items[j].state = .running;
                }
            },
        }
    }

    // warden-dhx
    /// Terminate a child gracefully.
    pub fn terminateChild(self: *Supervisor, pid: Pid) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const idx = self.findChildIndex(pid) orelse return SupervisorError.ChildNotFound;
        self.markDeadLocked(idx);
    }

    // warden-dhx
    /// Terminate all children in reverse start order, then mark supervisor dead.
    pub fn shutdown(self: *Supervisor) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var i = self.children.items.len;
        while (i > 0) {
            i -= 1;
            self.markDeadLocked(i);
        }
        self.registry.transition(self.pid, .exiting) catch {};
        self.registry.transition(self.pid, .dead) catch {};
    }

    // ── Internal helpers (all must be called with mutex held) ─────────────

    fn findChildIndex(self: *Supervisor, pid: Pid) ?usize {
        for (self.children.items, 0..) |c, i| {
            if (c.pid.proc == pid.proc and c.pid.beam == pid.beam) return i;
        }
        return null;
    }

    /// Mark child dead and drive registry state machine to .dead.
    fn markDeadLocked(self: *Supervisor, idx: usize) void {
        self.children.items[idx].state = .dead;
        const p = self.children.items[idx].pid;
        self.registry.transition(p, .exiting) catch {};
        self.registry.transition(p, .dead) catch {};
    }

    // warden-dhx
    /// Prune old timestamps and check intensity. Records a restart if allowed.
    fn checkIntensityLocked(self: *Supervisor) !void {
        const now = std.time.milliTimestamp();
        const window: i64 = @intCast(self.intensity_window_ms);

        // Compact: keep only timestamps within the window.
        var write: usize = 0;
        for (self.restart_timestamps.items) |ts| {
            if (now - ts < window) {
                self.restart_timestamps.items[write] = ts;
                write += 1;
            }
        }
        self.restart_timestamps.items.len = write;

        if (self.restart_timestamps.items.len >= self.max_restarts) {
            return SupervisorError.MaxRestartIntensityExceeded;
        }
        try self.restart_timestamps.append(self.allocator, now);
    }

    /// Allocate a new trampoline and submit the child to the scheduler.
    fn submitChildLocked(self: *Supervisor, idx: usize) !void {
        const spec = self.children.items[idx].spec;
        const tc = try self.allocator.create(TrampolineCtx);
        tc.* = TrampolineCtx{ .entry = spec.entry, .ctx = spec.ctx };
        try self.trampoline_pool.append(self.allocator, tc);
        try self.scheduler.submit(self.children.items[idx].pid, trampolineRun, tc);
    }

    /// Spawn a fresh PID for children[idx] and submit it to the scheduler.
    fn launchChildLocked(self: *Supervisor, idx: usize) !void {
        const spec = self.children.items[idx].spec;
        const new_pid = try self.registry.spawn(spec.kind, self.pid, PolicyEnvelope{});
        self.children.items[idx].pid = new_pid;
        self.children.items[idx].restart_count += 1;
        self.children.items[idx].started_at_ms = std.time.milliTimestamp();
        try self.submitChildLocked(idx);
    }

    /// Restart a single child (one_for_one / transient path).
    fn restartOneLocked(self: *Supervisor, idx: usize) !void {
        self.children.items[idx].state = .starting;
        try self.launchChildLocked(idx);
        self.children.items[idx].state = .running;
    }
};
