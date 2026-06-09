// warden-dmg
//
// Pure restart-decision logic for foreign workers — no threads, no I/O, so it
// is fully unit-testable. The reaper thread (bridge.zig) wires it to real
// process teardown/respawn.

const std = @import("std");

/// Per-worker restart policy. Mirrors the meaning of supervisor.RestartStrategy's
/// transient/temporary; `permanent` = always restart on crash.
pub const Strategy = enum {
    permanent,
    transient,
    temporary,

    /// Parse the proc.spawn "restart" field. Defaults handled by the caller.
    pub fn parse(s: []const u8) ?Strategy {
        if (std.mem.eql(u8, s, "permanent")) return .permanent;
        if (std.mem.eql(u8, s, "transient")) return .transient;
        if (std.mem.eql(u8, s, "temporary")) return .temporary;
        return null;
    }
};

/// How the previous incarnation exited, as classified from the child Term.
pub const ExitClass = enum { normal, abnormal };

/// Runaway-guard constants — identical to src/supervisor.zig for consistency.
pub const max_restarts: u32 = 3;
pub const intensity_window_ms: u64 = 5000;

/// Reaper cadence bounds (ms). Default sits well below the old 100ms.
pub const default_interval_ms: u64 = 50;
pub const min_interval_ms: u64 = 10;
pub const max_interval_ms: u64 = 2000;

pub fn clampInterval(ms: u64) u64 {
    return std.math.clamp(ms, min_interval_ms, max_interval_ms);
}

pub const Decision = enum { restart, retire };

/// Decide what to do with a crashed worker. `timestamps` is the worker's rolling
/// list of prior restart times (ms); on a `.restart` decision this function
/// appends `now` to the list. Pruning of out-of-window entries happens here (in
/// place) so the caller's list stays bounded. `allocator` must be the same
/// allocator the caller uses for all operations on `timestamps` (Zig 0.16
/// unmanaged ArrayList).
pub fn decide(
    strategy: Strategy,
    exit: ExitClass,
    timestamps: *std.ArrayList(i64),
    allocator: std.mem.Allocator,
    now: i64,
) !Decision {
    switch (strategy) {
        .temporary => return .retire,
        .transient => if (exit == .normal) return .retire,
        .permanent => {},
    }
    // Prune timestamps outside the window.
    var write: usize = 0;
    for (timestamps.items) |ts| {
        if (now - ts < @as(i64, @intCast(intensity_window_ms))) {
            timestamps.items[write] = ts;
            write += 1;
        }
    }
    timestamps.items.len = write;

    if (timestamps.items.len >= max_restarts) return .retire;
    try timestamps.append(allocator, now);
    return .restart;
}

test "temporary never restarts" {
    var ts: std.ArrayList(i64) = .empty;
    defer ts.deinit(std.testing.allocator);
    try std.testing.expectEqual(Decision.retire, try decide(.temporary, .abnormal, &ts, std.testing.allocator, 1000));
}

test "transient retires on normal exit, restarts on crash" {
    var ts: std.ArrayList(i64) = .empty;
    defer ts.deinit(std.testing.allocator);
    try std.testing.expectEqual(Decision.retire, try decide(.transient, .normal, &ts, std.testing.allocator, 1000));
    try std.testing.expectEqual(Decision.restart, try decide(.transient, .abnormal, &ts, std.testing.allocator, 1000));
}

test "permanent restarts until 3-in-5s, then retires" {
    var ts: std.ArrayList(i64) = .empty;
    defer ts.deinit(std.testing.allocator);
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1000));
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1100));
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1200));
    try std.testing.expectEqual(Decision.retire, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 1300));
    try std.testing.expectEqual(Decision.restart, try decide(.permanent, .abnormal, &ts, std.testing.allocator, 20000));
}

test "clampInterval bounds" {
    try std.testing.expectEqual(@as(u64, 10), clampInterval(1));
    try std.testing.expectEqual(@as(u64, 2000), clampInterval(99999));
    try std.testing.expectEqual(@as(u64, 50), clampInterval(50));
}

test "Strategy.parse" {
    try std.testing.expectEqual(Strategy.permanent, Strategy.parse("permanent").?);
    try std.testing.expectEqual(Strategy.transient, Strategy.parse("transient").?);
    try std.testing.expectEqual(Strategy.temporary, Strategy.parse("temporary").?);
    try std.testing.expect(Strategy.parse("bogus") == null);
}
