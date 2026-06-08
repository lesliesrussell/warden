// warden-6v5

const std = @import("std");
const sync = @import("sync.zig");
const types = @import("types.zig");

const MessageEnvelope = types.MessageEnvelope;
const MessagePriority = types.MessagePriority;
const PolicyEnvelope = types.PolicyEnvelope;
const OverflowStrategy = types.OverflowStrategy;

// warden-6v5
// Byte-size accounting: approximate each message as its `type` field length
// plus a fixed overhead of 64 bytes for all other scalar fields.
fn approxMsgBytes(msg: MessageEnvelope) u64 {
    return @as(u64, msg.@"type".len) + 64;
}

// warden-6v5
// Result returned from enqueue(). Callers inspect this to decide whether to
// log a warning, notify a supervisor, or apply backpressure to the sender.
pub const EnqueueResult = union(enum) {
    /// Message was accepted.
    ok,
    /// Rejected due to count or byte limit with `reject_new` strategy.
    rejected,
    /// A `.low`-priority message was displaced to make room. Caller may
    /// inspect or free the displaced envelope.
    dropped_for_room: MessageEnvelope,
    /// Caller must escalate to a supervisor.
    escalate,
    /// Caller must apply throttle / backpressure to the sender.
    throttle,
};

// warden-6v5
pub const Mailbox = struct {
    mu: sync.Mutex,
    allocator: std.mem.Allocator,
    queue: std.ArrayList(MessageEnvelope),
    policy: PolicyEnvelope,
    total_bytes: u64,

    pub fn init(allocator: std.mem.Allocator, policy: PolicyEnvelope) Mailbox {
        return .{
            .mu = .{},
            .allocator = allocator,
            .queue = .empty,
            .policy = policy,
            .total_bytes = 0,
        };
    }

    pub fn deinit(self: *Mailbox) void {
        // Mailbox does not own the string data inside each MessageEnvelope;
        // it only owns the ArrayList backing array.
        self.queue.deinit(self.allocator);
    }

    // warden-6v5
    /// Add `msg` to the back of the queue.
    ///
    /// Enforces `policy.max_mailbox_len` and `policy.max_mailbox_bytes`.
    /// When either limit would be exceeded the configured `overflow_strategy`
    /// determines the returned `EnqueueResult`:
    ///
    ///   - `reject_new`           → `.rejected`   (msg not added)
    ///   - `drop_oldest_low`      → drops oldest `.low` message to make room,
    ///                              then enqueues `msg`; returns
    ///                              `.dropped_for_room(displaced)`.
    ///                              If no `.low` message exists falls back to
    ///                              `.rejected`.
    ///   - `escalate_supervisor`  → `.escalate`   (msg not added)
    ///   - `throttle_sender`      → `.throttle`   (msg not added)
    pub fn enqueue(self: *Mailbox, msg: MessageEnvelope) !EnqueueResult {
        self.mu.lock();
        defer self.mu.unlock();

        const msg_bytes = approxMsgBytes(msg);
        const over_count = self.queue.items.len >= self.policy.max_mailbox_len;
        const over_bytes = (self.total_bytes + msg_bytes) > self.policy.max_mailbox_bytes;

        if (over_count or over_bytes) {
            return switch (self.policy.overflow_strategy) {
                .reject_new => .rejected,

                .drop_oldest_low => blk: {
                    // Find the oldest (lowest-index) `.low` priority message.
                    var drop_idx: ?usize = null;
                    for (self.queue.items, 0..) |m, i| {
                        if (m.priority == .low) {
                            drop_idx = i;
                            break;
                        }
                    }
                    const idx = drop_idx orelse break :blk .rejected;
                    const displaced = self.queue.orderedRemove(idx);
                    self.total_bytes -= approxMsgBytes(displaced);
                    // Now there is room — fall through to append below.
                    self.total_bytes += msg_bytes;
                    try self.queue.append(self.allocator, msg);
                    break :blk EnqueueResult{ .dropped_for_room = displaced };
                },

                .escalate_supervisor => .escalate,
                .throttle_sender => .throttle,
            };
        }

        // No overflow — normal path.
        self.total_bytes += msg_bytes;
        try self.queue.append(self.allocator, msg);
        return .ok;
    }

    // warden-6v5
    /// Scan messages in arrival order (oldest first).  Return and remove the
    /// first message for which `match_fn` returns `true`, or `null` if none
    /// match.
    ///
    /// Callers are responsible for transitioning the process to `.waiting`
    /// state when `null` is returned.
    pub fn receive(
        self: *Mailbox,
        comptime match_fn: fn (MessageEnvelope) bool,
    ) ?MessageEnvelope {
        self.mu.lock();
        defer self.mu.unlock();

        for (self.queue.items, 0..) |msg, i| {
            if (match_fn(msg)) {
                const removed = self.queue.orderedRemove(i);
                self.total_bytes -= approxMsgBytes(removed);
                return removed;
            }
        }
        return null;
    }

    // warden-6v5
    /// Number of messages currently in the mailbox.
    pub fn len(self: *Mailbox) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return self.queue.items.len;
    }

    // warden-6v5
    /// Approximate total byte size of all pending messages.
    pub fn byte_size(self: *Mailbox) u64 {
        self.mu.lock();
        defer self.mu.unlock();
        return self.total_bytes;
    }
};
