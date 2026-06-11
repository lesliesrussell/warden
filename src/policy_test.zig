// warden-u8y

const std = @import("std");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");
const policy_mod = @import("policy.zig");

const Pid = types.Pid;
const ActivityClass = types.ActivityClass;
const PolicyEnvelope = types.PolicyEnvelope;
const Registry = registry_mod.Registry;
const PolicyEngine = policy_mod.PolicyEngine;
const PolicyError = policy_mod.PolicyError;

fn defaultPolicy() PolicyEnvelope {
    return .{};
}

// Helper: spawn a process and advance it to .ready state
fn spawnReady(reg: *Registry) !Pid {
    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    try reg.transition(pid, .ready);
    return pid;
}

// warden-u8y
test "promote changes activity_class in registry entry" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    try engine.promote(pid, .elevated, 5000, "test promote");

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ActivityClass.elevated, entry.policy.activity_class);
}

// warden-u8y
test "promotion expires after TTL" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    try engine.promote(pid, .elevated, 5000, "expires soon");

    // Manually backdate the expires_at to simulate TTL elapsed
    engine.mutex.lock();
    const rec = engine.promotions.getPtr(pid.proc) orelse {
        engine.mutex.unlock();
        return error.TestUnexpectedNull;
    };
    rec.expires_at_ms = 0; // in the past
    engine.mutex.unlock();

    try engine.tick();

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ActivityClass.normal, entry.policy.activity_class);
}

// warden-u8y
test "demote returns to prior class" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    // Verify baseline is .normal
    {
        const e = reg.lookup(pid) orelse return error.TestUnexpectedNull;
        try std.testing.expectEqual(ActivityClass.normal, e.policy.activity_class);
    }

    try engine.promote(pid, .system, 10000, "temp elevation");
    try engine.demote(pid, "back to normal");

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ActivityClass.normal, entry.policy.activity_class);
}

// warden-u8y
test "policy_change events recorded for promote, demote, expire" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    try engine.promote(pid, .elevated, 5000, "reason-promote");
    try engine.demote(pid, "reason-demote");

    // Check promote and demote events
    try std.testing.expectEqual(@as(usize, 2), engine.events.items.len);
    try std.testing.expectEqualStrings("promote", engine.events.items[0].action);
    try std.testing.expectEqualStrings("demote", engine.events.items[1].action);

    // Promote again and expire via tick
    try engine.promote(pid, .elevated, 5000, "reason-expire");

    engine.mutex.lock();
    const rec = engine.promotions.getPtr(pid.proc) orelse {
        engine.mutex.unlock();
        return error.TestUnexpectedNull;
    };
    rec.expires_at_ms = 0;
    engine.mutex.unlock();

    try engine.tick();

    try std.testing.expectEqual(@as(usize, 4), engine.events.items.len);
    try std.testing.expectEqualStrings("expire", engine.events.items[3].action);
}

// warden-u8y
test "quarantine sets activity_class to .tiny" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    try engine.quarantine(pid, "misbehaving");

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ActivityClass.tiny, entry.policy.activity_class);

    // Verify quarantine event recorded
    try std.testing.expectEqual(@as(usize, 1), engine.events.items.len);
    try std.testing.expectEqualStrings("quarantine", engine.events.items[0].action);
}

// warden-u8y
test "unquarantine restores prior class" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    try engine.quarantine(pid, "quarantined");
    try engine.unquarantine(pid);

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ActivityClass.normal, entry.policy.activity_class);

    try std.testing.expectEqual(@as(usize, 2), engine.events.items.len);
    try std.testing.expectEqualStrings("unquarantine", engine.events.items[1].action);
}

// warden-u8y
test "unquarantine on non-quarantined process returns NotPaused" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    try std.testing.expectError(PolicyError.NotPaused, engine.unquarantine(pid));
}

// warden-jq6: the PolicyEngine.check{Mailbox,Log,Storage}Quota helpers and their
// tests were removed — they were never called by the runtime (mailbox limits are
// enforced in mailbox.zig's overflow path, storage limits in storage.zig), so
// they implied a second enforcement path that did not exist.

// warden-u8y
test "promote on unknown pid returns ProcessNotFound" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const fake = Pid{ .beam = 1, .proc = 999_999 };
    try std.testing.expectError(
        PolicyError.ProcessNotFound,
        engine.promote(fake, .elevated, 1000, "ghost"),
    );
}

// warden-u8y
test "double promote preserves original prior_class" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    var engine = PolicyEngine.init(std.testing.allocator, &reg);
    defer engine.deinit();

    const pid = try spawnReady(&reg);
    // First promote from .normal → .elevated
    try engine.promote(pid, .elevated, 10000, "first");
    // Second promote from .elevated → .system (should preserve .normal as prior)
    try engine.promote(pid, .system, 10000, "second");

    // Demote should return to .normal, not .elevated
    try engine.demote(pid, "demote after double promote");

    const entry = reg.lookup(pid) orelse return error.TestUnexpectedNull;
    try std.testing.expectEqual(ActivityClass.normal, entry.policy.activity_class);
}
