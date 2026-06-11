// warden-3rc

const std = @import("std");
const clock = @import("clock.zig");
const sync = @import("sync.zig");
const types = @import("types.zig");

const Pid = types.Pid;
const ProcessKind = types.ProcessKind;
const ProcessState = types.ProcessState;
const PolicyEnvelope = types.PolicyEnvelope;

/// Monotonic PID counter. Each Registry instance uses this to allocate
/// unique proc IDs within its beam. Thread-safe via atomic fetch-add.
var global_proc_counter = std.atomic.Value(u64).init(1);

pub const ProcessEntry = struct {
    pid: Pid,
    kind: ProcessKind,
    state: ProcessState,
    supervisor: ?Pid,
    policy: PolicyEnvelope,
    started_at: i64,
    last_active_at: i64,
    /// Placeholder for arbitrary metadata (caller-managed).
    metadata: ?*anyopaque,
};

pub const RegistryError = error{
    InvalidTransition,
    ProcessNotFound,
    ProcessNotDead,
};

/// Thread-safe map of Pid → ProcessEntry.
///
/// Concurrency contract for `lookup`:
///   The returned `*ProcessEntry` points into the HashMap. The pointer is
///   valid only while the caller holds no concurrent writes (spawn/remove).
///   For production use, callers should operate under the Registry's mutex
///   or copy the entry out. In tests and single-writer scenarios this is safe.
pub const Registry = struct {
    allocator: std.mem.Allocator,
    beam_id: u32,
    mutex: sync.Mutex,
    map: std.AutoHashMap(u64, ProcessEntry),

    pub fn init(allocator: std.mem.Allocator, beam_id: u32) Registry {
        return .{
            .allocator = allocator,
            .beam_id = beam_id,
            .mutex = .{},
            .map = std.AutoHashMap(u64, ProcessEntry).init(allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        self.map.deinit();
    }

    /// Allocate a new Pid, insert a ProcessEntry in `.starting` state, return the Pid.
    pub fn spawn(
        self: *Registry,
        kind: ProcessKind,
        supervisor: ?Pid,
        policy: PolicyEnvelope,
    ) !Pid {
        const proc_id = global_proc_counter.fetchAdd(1, .monotonic);
        const pid = Pid{ .beam = self.beam_id, .proc = proc_id };
        const now = clock.nowMs();
        const entry = ProcessEntry{
            .pid = pid,
            .kind = kind,
            .state = .starting,
            .supervisor = supervisor,
            .policy = policy,
            .started_at = now,
            .last_active_at = now,
            .metadata = null,
        };
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.map.put(proc_id, entry);
        return pid;
    }

    /// Returns a pointer to the entry, or null if not found.
    /// See concurrency contract on Registry.
    pub fn lookup(self: *Registry, pid: Pid) ?*ProcessEntry {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.map.getPtr(pid.proc);
    }

    /// Enforce valid state transition via ProcessState.canTransitionTo.
    /// Returns RegistryError.InvalidTransition if the transition is illegal,
    /// RegistryError.ProcessNotFound if the pid is unknown.
    pub fn transition(self: *Registry, pid: Pid, next_state: ProcessState) RegistryError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const entry = self.map.getPtr(pid.proc) orelse return RegistryError.ProcessNotFound;
        if (!entry.state.canTransitionTo(next_state)) return RegistryError.InvalidTransition;
        entry.state = next_state;
        entry.last_active_at = clock.nowMs();
    }

    /// Remove a dead process from the registry. Returns an error if the
    /// process is not in `.dead` state or does not exist.
    pub fn remove(self: *Registry, pid: Pid) RegistryError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const entry = self.map.getPtr(pid.proc) orelse return RegistryError.ProcessNotFound;
        if (entry.state != .dead) return RegistryError.ProcessNotDead;
        _ = self.map.remove(pid.proc);
    }

    // warden-f19
    /// Number of registered processes (acquires the lock). Lets callers read the
    /// count without reaching into `mutex`/`map` directly.
    pub fn count(self: *Registry) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.map.count();
    }

    // warden-zl0
    /// Read a process's activity class under the lock (used by the scheduler for
    /// admission priority). Returns null if the pid is unknown.
    pub fn activityClass(self: *Registry, pid: Pid) ?types.ActivityClass {
        self.mutex.lock();
        defer self.mutex.unlock();
        const entry = self.map.getPtr(pid.proc) orelse return null;
        return entry.policy.activity_class;
    }

    // warden-f19
    /// Set a process's activity class under the lock (used for quarantine).
    /// Leaves promotion_ttl_ms untouched. Returns ProcessNotFound if unknown.
    /// Safe where `lookup` is not: it never hands a map pointer back to the
    /// caller, so the mutation cannot race a concurrent spawn/remove the way a
    /// lookup-then-mutate would.
    pub fn setActivityClass(self: *Registry, pid: Pid, class: types.ActivityClass) RegistryError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const entry = self.map.getPtr(pid.proc) orelse return RegistryError.ProcessNotFound;
        entry.policy.activity_class = class;
    }

    // warden-f19
    /// Set a process's activity class and promotion TTL together under the lock
    /// (used for promote). Returns ProcessNotFound if unknown.
    pub fn setPromotion(self: *Registry, pid: Pid, class: types.ActivityClass, ttl_ms: ?u64) RegistryError!void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const entry = self.map.getPtr(pid.proc) orelse return RegistryError.ProcessNotFound;
        entry.policy.activity_class = class;
        entry.policy.promotion_ttl_ms = ttl_ms;
    }
};
