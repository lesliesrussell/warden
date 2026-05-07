// warden-52t

const std = @import("std");
const types = @import("types.zig");

const ProcessState = types.ProcessState;
const Pid = types.Pid;
const PolicyEnvelope = types.PolicyEnvelope;

test "Pid equality" {
    const a = Pid{ .beam = 1, .proc = 42 };
    const b = Pid{ .beam = 1, .proc = 42 };
    const c = Pid{ .beam = 1, .proc = 99 };
    try std.testing.expect(a.eql(b));
    try std.testing.expect(!a.eql(c));
}

test "Pid format" {
    const pid = Pid{ .beam = 1, .proc = 42 };
    var buf: [64]u8 = undefined;
    const s = try std.fmt.bufPrint(&buf, "{f}", .{pid});
    try std.testing.expectEqualStrings("pid:1/42", s);
}

test "ProcessState valid transitions" {
    try std.testing.expect(ProcessState.starting.canTransitionTo(.ready));
    try std.testing.expect(ProcessState.ready.canTransitionTo(.running));
    try std.testing.expect(ProcessState.running.canTransitionTo(.waiting));
    try std.testing.expect(ProcessState.running.canTransitionTo(.hibernating));
    try std.testing.expect(ProcessState.running.canTransitionTo(.paused));
    try std.testing.expect(ProcessState.waiting.canTransitionTo(.ready));
    try std.testing.expect(ProcessState.hibernating.canTransitionTo(.ready));
    try std.testing.expect(ProcessState.paused.canTransitionTo(.ready));
    try std.testing.expect(ProcessState.exiting.canTransitionTo(.dead));
}

test "ProcessState invalid transitions" {
    try std.testing.expect(!ProcessState.dead.canTransitionTo(.ready));
    try std.testing.expect(!ProcessState.dead.canTransitionTo(.starting));
    try std.testing.expect(!ProcessState.ready.canTransitionTo(.dead));
    try std.testing.expect(!ProcessState.starting.canTransitionTo(.running));
    try std.testing.expect(!ProcessState.waiting.canTransitionTo(.running));
}

test "PolicyEnvelope defaults are sane" {
    const p = PolicyEnvelope{};
    try std.testing.expectEqual(types.ActivityClass.normal, p.activity_class);
    try std.testing.expectEqual(types.PriorityClass.normal, p.priority_class);
    try std.testing.expect(p.max_reductions_per_slice > 0);
    try std.testing.expect(p.max_mailbox_len > 0);
    try std.testing.expect(p.max_mailbox_bytes > 0);
    try std.testing.expect(p.hibernation_enabled);
    try std.testing.expectEqual(@as(?u64, null), p.promotion_ttl_ms);
}

test "PolicyEnvelope overflow strategy default" {
    const p = PolicyEnvelope{};
    try std.testing.expectEqual(types.OverflowStrategy.reject_new, p.overflow_strategy);
}

test "MessageEnvelope required fields compile" {
    // Verify the struct has the fields the spec requires — pure comptime check
    const required = [_][]const u8{ "v", "kind", "id", "from", "to", "priority", "body" };
    comptime {
        const fields = @typeInfo(types.MessageEnvelope).@"struct".fields;
        for (required) |req| {
            var found = false;
            for (fields) |f| {
                if (std.mem.eql(u8, f.name, req)) {
                    found = true;
                    break;
                }
            }
            if (!found) @compileError("MessageEnvelope missing required field: " ++ req);
        }
    }
    try std.testing.expect(true);
}
