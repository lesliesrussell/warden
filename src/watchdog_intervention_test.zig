// warden-5l5

const std = @import("std");
const beam = @import("beam.zig");
const wi = @import("topology_watchdog_demo.zig");

test "watchdog intervention: worker paused on quota breach, resumed after cooldown" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var demo = try wi.WatchdogInterventionDemo.init(allocator, rt, .{
        .log_dir = log_path,
        .storage_base = log_path,
        .log_quota = 5,
        .cooldown_ms = 80,
        .poll_ms = 10,
    });

    const session_pid = demo.session_pid;

    try demo.start();
    std.Thread.sleep(20 * std.time.ns_per_ms);

    // Dispatch one work batch — 6 log events, exceeding the quota of 5.
    try demo.dispatchWork();

    // Wait for watchdog to detect breach and pause, then resume after cooldown.
    // Cooldown is 80ms + poll_ms overhead.
    std.Thread.sleep(200 * std.time.ns_per_ms);

    // Worker was paused.
    try std.testing.expect(demo.worker_paused.load(.acquire));

    // Worker was resumed.
    try std.testing.expect(demo.worker_resumed.load(.acquire));

    // Session supervisor never restarted — PID is unchanged.
    try std.testing.expectEqual(session_pid, demo.session_pid);

    demo.deinit();
}

test "watchdog intervention: session supervisor PID stable, only worker affected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var demo = try wi.WatchdogInterventionDemo.init(allocator, rt, .{
        .log_dir = log_path,
        .storage_base = log_path,
        .log_quota = 100,  // high quota — watchdog never fires
        .cooldown_ms = 50,
        .poll_ms = 10,
    });

    const session_pid_before = demo.session_pid;
    try demo.start();

    // Run a few work batches without triggering quota breach.
    for (0..3) |_| {
        try demo.dispatchWork();
        std.Thread.sleep(15 * std.time.ns_per_ms);
    }

    std.Thread.sleep(50 * std.time.ns_per_ms);

    // No intervention occurred.
    try std.testing.expect(!demo.worker_paused.load(.acquire));

    // Session supervisor PID unchanged.
    try std.testing.expectEqual(session_pid_before, demo.session_pid);

    demo.deinit();
}
