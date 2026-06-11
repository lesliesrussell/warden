// warden-u8y

const std = @import("std");
const clock = @import("clock.zig");
const sync = @import("sync.zig");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");

const Pid = types.Pid;
const ActivityClass = types.ActivityClass;
const PolicyEnvelope = types.PolicyEnvelope;
const Registry = registry_mod.Registry;

// warden-u8y
pub const StorageNs = enum { temp, cache, state };

// warden-u8y
pub const PolicyError = error{
    ProcessNotFound,
    QuotaExceeded,
    UnauthorizedPromotion,
    NotPaused,
};

// warden-u8y
pub const PromotionRecord = struct {
    prior_class: ActivityClass,
    promoted_class: ActivityClass,
    expires_at_ms: i64,
    reason: []const u8, // heap-allocated, owned by PolicyEngine
};

// warden-u8y
pub const PolicyEvent = struct {
    pid: Pid,
    action: []const u8,
    reason: []const u8,
    ts: i64,
};

// warden-u8y
pub const PolicyEngine = struct {
    registry: *Registry,
    allocator: std.mem.Allocator,
    promotions: std.AutoHashMap(u64, PromotionRecord),
    mutex: sync.Mutex,
    events: std.ArrayList(PolicyEvent),

    // warden-u8y
    pub fn init(allocator: std.mem.Allocator, registry: *Registry) PolicyEngine {
        return .{
            .registry = registry,
            .allocator = allocator,
            .promotions = std.AutoHashMap(u64, PromotionRecord).init(allocator),
            .mutex = .{},
            .events = .empty,
        };
    }

    // warden-u8y
    pub fn deinit(self: *PolicyEngine) void {
        // Free all heap-allocated reason strings in promotions
        var it = self.promotions.valueIterator();
        while (it.next()) |rec| {
            self.allocator.free(rec.reason);
        }
        self.promotions.deinit();

        // Free all heap-allocated reason strings in events
        for (self.events.items) |ev| {
            self.allocator.free(ev.reason);
            self.allocator.free(ev.action);
        }
        self.events.deinit(self.allocator);
    }

    // warden-u8y
    /// Emit a policy event. Caller must hold self.mutex.
    fn emitEvent(self: *PolicyEngine, pid: Pid, action: []const u8, reason: []const u8) !void {
        const action_copy = try self.allocator.dupe(u8, action);
        errdefer self.allocator.free(action_copy);
        const reason_copy = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(reason_copy);
        try self.events.append(self.allocator, PolicyEvent{
            .pid = pid,
            .action = action_copy,
            .reason = reason_copy,
            .ts = clock.nowMs(),
        });
    }

    // warden-u8y
    /// Promote process to class for ttl_ms.
    /// TODO: Authorization check not yet implemented — any caller can promote.
    pub fn promote(self: *PolicyEngine, pid: Pid, class: ActivityClass, ttl_ms: u64, reason: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Look up the process in the registry
        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;

        // Determine prior class: if already promoted, preserve the original prior
        const prior_class = if (self.promotions.get(pid.proc)) |existing| existing.prior_class else entry.policy.activity_class;

        // Free old promotion reason if overwriting
        if (self.promotions.getPtr(pid.proc)) |old_rec| {
            self.allocator.free(old_rec.reason);
        }

        const reason_copy = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(reason_copy);

        const expires_at = clock.nowMs() + @as(i64, @intCast(ttl_ms));
        const rec = PromotionRecord{
            .prior_class = prior_class,
            .promoted_class = class,
            .expires_at_ms = expires_at,
            .reason = reason_copy,
        };
        try self.promotions.put(pid.proc, rec);

        // Update the registry entry's policy
        entry.policy.activity_class = class;

        try self.emitEvent(pid, "promote", reason);
    }

    // warden-u8y
    /// Demote process back to its prior class.
    pub fn demote(self: *PolicyEngine, pid: Pid, reason: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;

        const rec = self.promotions.get(pid.proc) orelse {
            // Not promoted — demote to .normal as baseline
            entry.policy.activity_class = .normal;
            try self.emitEvent(pid, "demote", reason);
            return;
        };

        const prior = rec.prior_class;

        // Free reason string and remove promotion record
        self.allocator.free(rec.reason);
        _ = self.promotions.remove(pid.proc);

        // Restore prior class
        entry.policy.activity_class = prior;

        try self.emitEvent(pid, "demote", reason);
    }

    // warden-u8y
    /// Expire promotions whose TTL has elapsed. Call periodically.
    pub fn tick(self: *PolicyEngine) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const now = clock.nowMs();

        // Collect expired keys — can't remove during iteration
        var expired_keys: [64]u64 = undefined;
        var expired_count: usize = 0;

        var it = self.promotions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.expires_at_ms <= now) {
                if (expired_count < expired_keys.len) {
                    expired_keys[expired_count] = entry.key_ptr.*;
                    expired_count += 1;
                }
            }
        }

        // Process expirations
        for (expired_keys[0..expired_count]) |proc_id| {
            const rec = self.promotions.get(proc_id) orelse continue;
            const pid = Pid{ .beam = self.registry.beam_id, .proc = proc_id };

            // Restore prior class in registry
            if (self.registry.lookup(pid)) |reg_entry| {
                reg_entry.policy.activity_class = rec.prior_class;
            }

            // Emit expire event
            try self.emitEvent(pid, "expire", rec.reason);

            // Free promotion reason and remove record
            self.allocator.free(rec.reason);
            _ = self.promotions.remove(proc_id);
        }
    }

    // warden-u8y
    /// Check if a proposed mailbox enqueue would violate policy.
    pub fn checkMailboxQuota(self: *PolicyEngine, pid: Pid, current_len: u32, current_bytes: u64, msg_bytes: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;
        const policy = entry.policy;

        if (current_len + 1 > policy.max_mailbox_len) return PolicyError.QuotaExceeded;
        if (current_bytes + msg_bytes > policy.max_mailbox_bytes) return PolicyError.QuotaExceeded;
    }

    // warden-u8y
    /// Check log volume quota (simplified ceiling check).
    pub fn checkLogQuota(self: *PolicyEngine, pid: Pid, bytes: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;
        if (bytes > entry.policy.max_log_bytes_per_min) return PolicyError.QuotaExceeded;
    }

    // warden-u8y
    /// Check storage quota for a namespace.
    pub fn checkStorageQuota(self: *PolicyEngine, pid: Pid, ns: StorageNs, bytes: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;
        const limit = switch (ns) {
            .temp => entry.policy.max_temp_bytes,
            .cache => entry.policy.max_cache_bytes,
            .state => entry.policy.max_state_bytes,
        };
        if (bytes > limit) return PolicyError.QuotaExceeded;
    }

    // warden-u8y
    // warden-092: quarantine demotes a process to the .tiny activity class (the
    // same class the control/wardenctl path uses) — an advisory label, not a
    // hard stop. The mutation goes through Registry.setActivityClass (lock-held)
    // rather than a lookup-then-mutate pointer, avoiding the use-after-unlock
    // pattern fixed in warden-f19.
    /// Quarantine a process: demote to .tiny and record prior class for restore.
    pub fn quarantine(self: *PolicyEngine, pid: Pid, reason: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;

        // Read prior class (copy) before mutating; never expires (maxInt).
        const prior_class = if (self.promotions.get(pid.proc)) |existing| existing.prior_class else entry.policy.activity_class;

        // Free old promotion reason if overwriting
        if (self.promotions.getPtr(pid.proc)) |old_rec| {
            self.allocator.free(old_rec.reason);
        }

        const reason_copy = try self.allocator.dupe(u8, reason);
        errdefer self.allocator.free(reason_copy);

        const rec = PromotionRecord{
            .prior_class = prior_class,
            .promoted_class = .tiny,
            .expires_at_ms = std.math.maxInt(i64),
            .reason = reason_copy,
        };
        try self.promotions.put(pid.proc, rec);

        try self.registry.setActivityClass(pid, .tiny);

        try self.emitEvent(pid, "quarantine", reason);
    }

    // warden-u8y
    /// Resume a paused process (restore prior class).
    pub fn unquarantine(self: *PolicyEngine, pid: Pid) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.registry.lookup(pid) orelse return PolicyError.ProcessNotFound;

        // warden-092: quarantine demotes to .tiny; only a quarantined process can
        // be unquarantined. (Error name kept as NotPaused for compatibility.)
        if (entry.policy.activity_class != .tiny) return PolicyError.NotPaused;

        const rec = self.promotions.get(pid.proc) orelse {
            // Quarantined but no promotion record — restore to .normal
            try self.registry.setActivityClass(pid, .normal);
            try self.emitEvent(pid, "unquarantine", "");
            return;
        };

        const prior = rec.prior_class;
        self.allocator.free(rec.reason);
        _ = self.promotions.remove(pid.proc);

        try self.registry.setActivityClass(pid, prior);

        try self.emitEvent(pid, "unquarantine", "");
    }
};
