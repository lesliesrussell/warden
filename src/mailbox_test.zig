// warden-6v5

const std = @import("std");
const types = @import("types.zig");
const mailbox_mod = @import("mailbox.zig");

const MessageEnvelope = types.MessageEnvelope;
const MessagePriority = types.MessagePriority;
const PolicyEnvelope = types.PolicyEnvelope;
const Mailbox = mailbox_mod.Mailbox;
const EnqueueResult = mailbox_mod.EnqueueResult;

// warden-6v5
// Helper: build a minimal MessageEnvelope for testing.
fn makeMsg(kind_tag: types.MessageKind, type_str: []const u8, priority: MessagePriority) MessageEnvelope {
    return .{
        .kind = kind_tag,
        .@"type" = type_str,
        .id = "test-id",
        .from = "from",
        .to = "to",
        .priority = priority,
        .body = .{ .null = {} },
    };
}

// warden-6v5
fn defaultPolicy() PolicyEnvelope {
    return .{};
}

fn limitedPolicy(max_len: u32) PolicyEnvelope {
    var p = PolicyEnvelope{};
    p.max_mailbox_len = max_len;
    return p;
}

// warden-6v5
test "enqueue and receive round trip" {
    var mb = Mailbox.init(std.testing.allocator, defaultPolicy());
    defer mb.deinit();

    const msg = makeMsg(.event, "ping", .normal);
    const result = try mb.enqueue(msg);
    try std.testing.expectEqual(EnqueueResult.ok, result);
    try std.testing.expectEqual(@as(usize, 1), mb.len());

    const got = mb.receive(struct {
        fn match(_: MessageEnvelope) bool {
            return true;
        }
    }.match);
    try std.testing.expect(got != null);
    try std.testing.expectEqualStrings("ping", got.?.@"type");
    try std.testing.expectEqual(@as(usize, 0), mb.len());
}

// warden-6v5
test "selective receive returns first matching message in arrival order" {
    var mb = Mailbox.init(std.testing.allocator, defaultPolicy());
    defer mb.deinit();

    // Enqueue three messages: alpha (event), beta (request), gamma (event)
    _ = try mb.enqueue(makeMsg(.event, "alpha", .normal));
    _ = try mb.enqueue(makeMsg(.request, "beta", .normal));
    _ = try mb.enqueue(makeMsg(.event, "gamma", .normal));

    // Match only .request messages — should get "beta" even though it's second.
    const got = mb.receive(struct {
        fn match(m: MessageEnvelope) bool {
            return m.kind == .request;
        }
    }.match);
    try std.testing.expect(got != null);
    try std.testing.expectEqualStrings("beta", got.?.@"type");

    // Remaining messages are alpha and gamma (in order).
    try std.testing.expectEqual(@as(usize, 2), mb.len());
}

// warden-6v5
test "receive returns null when no match found" {
    var mb = Mailbox.init(std.testing.allocator, defaultPolicy());
    defer mb.deinit();

    _ = try mb.enqueue(makeMsg(.event, "something", .normal));

    const got = mb.receive(struct {
        fn match(m: MessageEnvelope) bool {
            return m.kind == .signal; // nothing is .signal
        }
    }.match);
    try std.testing.expect(got == null);
    // The non-matching message is still in the mailbox.
    try std.testing.expectEqual(@as(usize, 1), mb.len());
}

// warden-6v5
test "received message is absent from mailbox after receipt" {
    var mb = Mailbox.init(std.testing.allocator, defaultPolicy());
    defer mb.deinit();

    _ = try mb.enqueue(makeMsg(.event, "first", .normal));
    _ = try mb.enqueue(makeMsg(.event, "second", .normal));

    _ = mb.receive(struct {
        fn match(m: MessageEnvelope) bool {
            return std.mem.eql(u8, m.@"type", "first");
        }
    }.match);

    try std.testing.expectEqual(@as(usize, 1), mb.len());

    // The remaining message should be "second".
    const got = mb.receive(struct {
        fn match(_: MessageEnvelope) bool {
            return true;
        }
    }.match);
    try std.testing.expect(got != null);
    try std.testing.expectEqualStrings("second", got.?.@"type");
    try std.testing.expectEqual(@as(usize, 0), mb.len());
}

// warden-6v5
test "reject_new overflow: 6th message rejected when limit is 5" {
    var policy = limitedPolicy(5);
    policy.overflow_strategy = .reject_new;

    var mb = Mailbox.init(std.testing.allocator, policy);
    defer mb.deinit();

    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        const result = try mb.enqueue(makeMsg(.event, "msg", .normal));
        try std.testing.expectEqual(EnqueueResult.ok, result);
    }

    const result = try mb.enqueue(makeMsg(.event, "overflow", .normal));
    try std.testing.expectEqual(EnqueueResult.rejected, result);
    try std.testing.expectEqual(@as(usize, 5), mb.len());
}

// warden-6v5
test "drop_oldest_low: oldest low-priority message dropped when full" {
    var policy = limitedPolicy(3);
    policy.overflow_strategy = .drop_oldest_low;

    var mb = Mailbox.init(std.testing.allocator, policy);
    defer mb.deinit();

    // Fill with: low, normal, normal
    _ = try mb.enqueue(makeMsg(.event, "low1", .low));
    _ = try mb.enqueue(makeMsg(.event, "normal1", .normal));
    _ = try mb.enqueue(makeMsg(.event, "normal2", .normal));
    try std.testing.expectEqual(@as(usize, 3), mb.len());

    // 4th message should displace "low1".
    const result = try mb.enqueue(makeMsg(.event, "new", .normal));
    switch (result) {
        .dropped_for_room => |displaced| {
            try std.testing.expectEqualStrings("low1", displaced.@"type");
        },
        else => return error.UnexpectedResult,
    }
    // Still 3 messages (low1 removed, new added).
    try std.testing.expectEqual(@as(usize, 3), mb.len());

    // "low1" must not be reachable anymore.
    const low1 = mb.receive(struct {
        fn match(m: MessageEnvelope) bool {
            return std.mem.eql(u8, m.@"type", "low1");
        }
    }.match);
    try std.testing.expect(low1 == null);
}

// warden-6v5
test "drop_oldest_low falls back to rejected when no low-priority message exists" {
    var policy = limitedPolicy(2);
    policy.overflow_strategy = .drop_oldest_low;

    var mb = Mailbox.init(std.testing.allocator, policy);
    defer mb.deinit();

    _ = try mb.enqueue(makeMsg(.event, "normal1", .normal));
    _ = try mb.enqueue(makeMsg(.event, "normal2", .normal));

    // No .low messages — should fall back to rejected.
    const result = try mb.enqueue(makeMsg(.event, "new", .high));
    try std.testing.expectEqual(EnqueueResult.rejected, result);
    try std.testing.expectEqual(@as(usize, 2), mb.len());
}

// warden-6v5
test "byte limit enforced independently from count limit" {
    // Allow up to 10 messages but only ~150 bytes.
    // approxMsgBytes = type.len + 64. "msg" is 3 chars → 67 bytes per message.
    // Two messages = 134 bytes. Three would be 201, exceeding 150.
    var policy = PolicyEnvelope{};
    policy.max_mailbox_len = 10;
    policy.max_mailbox_bytes = 150;
    policy.overflow_strategy = .reject_new;

    var mb = Mailbox.init(std.testing.allocator, policy);
    defer mb.deinit();

    const r1 = try mb.enqueue(makeMsg(.event, "msg", .normal)); // 67 bytes
    try std.testing.expectEqual(EnqueueResult.ok, r1);
    const r2 = try mb.enqueue(makeMsg(.event, "msg", .normal)); // 134 bytes
    try std.testing.expectEqual(EnqueueResult.ok, r2);
    const r3 = try mb.enqueue(makeMsg(.event, "msg", .normal)); // would be 201 > 150
    try std.testing.expectEqual(EnqueueResult.rejected, r3);

    try std.testing.expectEqual(@as(usize, 2), mb.len());
    try std.testing.expectEqual(@as(u64, 134), mb.byte_size());
}
