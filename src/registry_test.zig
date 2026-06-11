// warden-3rc

const std = @import("std");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");

const Registry = registry_mod.Registry;
const RegistryError = registry_mod.RegistryError;
const Pid = types.Pid;
const PolicyEnvelope = types.PolicyEnvelope;
const ProcessState = types.ProcessState;

fn defaultPolicy() PolicyEnvelope {
    return .{};
}

test "spawn returns unique PIDs" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const p1 = try reg.spawn(.native_worker, null, defaultPolicy());
    const p2 = try reg.spawn(.native_worker, null, defaultPolicy());
    const p3 = try reg.spawn(.foreign_worker, null, defaultPolicy());

    try std.testing.expect(!p1.eql(p2));
    try std.testing.expect(!p2.eql(p3));
    try std.testing.expect(!p1.eql(p3));
}

test "lookup returns correct entry or null" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_supervisor, null, defaultPolicy());
    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expect(entry.pid.eql(pid));
    try std.testing.expectEqual(types.ProcessKind.native_supervisor, entry.kind);
    try std.testing.expectEqual(ProcessState.starting, entry.state);

    // lookup a non-existent pid returns null
    const fake = Pid{ .beam = 1, .proc = 999_999 };
    try std.testing.expectEqual(@as(?*registry_mod.ProcessEntry, null), reg.lookup(fake));
}

test "valid state transitions succeed" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    try reg.transition(pid, .ready);
    try reg.transition(pid, .running);
    try reg.transition(pid, .waiting);
    try reg.transition(pid, .ready);
    try reg.transition(pid, .exiting);
    try reg.transition(pid, .dead);

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ProcessState.dead, entry.state);
}

test "invalid transitions return error" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    // starting → running is invalid (must go through ready)
    try std.testing.expectError(RegistryError.InvalidTransition, reg.transition(pid, .running));
    // State should still be .starting after the failed transition
    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ProcessState.starting, entry.state);
}

test "dead process removal works" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    try reg.transition(pid, .ready);
    try reg.transition(pid, .exiting);
    try reg.transition(pid, .dead);
    try reg.remove(pid);
    try std.testing.expectEqual(@as(?*registry_mod.ProcessEntry, null), reg.lookup(pid));
}

test "remove non-dead process returns error" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    try std.testing.expectError(RegistryError.ProcessNotDead, reg.remove(pid));
}

test "remove non-existent process returns error" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const fake = Pid{ .beam = 1, .proc = 888_888 };
    try std.testing.expectError(RegistryError.ProcessNotFound, reg.remove(fake));
}

test "concurrent spawn produces no duplicate PIDs" {
    const thread_count = 8;
    const spawns_per_thread = 64;
    const total = thread_count * spawns_per_thread;

    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    var all_pids: [total]Pid = undefined;

    const Context = struct {
        registry: *Registry,
        out: []Pid,

        fn run(ctx: @This()) void {
            for (ctx.out) |*slot| {
                slot.* = ctx.registry.spawn(.native_worker, null, .{}) catch unreachable;
            }
        }
    };

    var threads: [thread_count]std.Thread = undefined;
    for (&threads, 0..) |*t, i| {
        const ctx = Context{
            .registry = &reg,
            .out = all_pids[i * spawns_per_thread .. (i + 1) * spawns_per_thread],
        };
        t.* = try std.Thread.spawn(.{}, Context.run, .{ctx});
    }
    for (&threads) |t| t.join();

    // Verify uniqueness: sort proc IDs and check for consecutive duplicates.
    var procs: [total]u64 = undefined;
    for (all_pids, 0..) |p, i| procs[i] = p.proc;
    std.mem.sort(u64, &procs, {}, std.sort.asc(u64));
    for (1..total) |i| {
        try std.testing.expect(procs[i] != procs[i - 1]);
    }
}

// warden-f19
test "setActivityClass sets class under lock and leaves ttl untouched" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_worker, null, .{ .promotion_ttl_ms = 1234 });
    try reg.setActivityClass(pid, .tiny);

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(types.ActivityClass.tiny, entry.policy.activity_class);
    try std.testing.expectEqual(@as(?u64, 1234), entry.policy.promotion_ttl_ms);
}

// warden-f19
test "setPromotion sets class and ttl together under lock" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    try reg.setPromotion(pid, .elevated, 5000);

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(types.ActivityClass.elevated, entry.policy.activity_class);
    try std.testing.expectEqual(@as(?u64, 5000), entry.policy.promotion_ttl_ms);
}

// warden-f19
test "mutate-under-lock methods report ProcessNotFound; count tracks size" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const ghost = Pid{ .beam = 1, .proc = 999999 };
    try std.testing.expectError(RegistryError.ProcessNotFound, reg.setActivityClass(ghost, .tiny));
    try std.testing.expectError(RegistryError.ProcessNotFound, reg.setPromotion(ghost, .elevated, null));

    try std.testing.expectEqual(@as(usize, 0), reg.count());
    _ = try reg.spawn(.native_worker, null, defaultPolicy());
    _ = try reg.spawn(.native_worker, null, defaultPolicy());
    try std.testing.expectEqual(@as(usize, 2), reg.count());
}
