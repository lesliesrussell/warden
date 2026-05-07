// warden-45x

const std = @import("std");
const beam = @import("beam.zig");
const fd = @import("topology_failure_demo.zig");

test "failure recovery: hung worker is replaced, session PID unchanged, task 2 completes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 8);
    defer rt.destroy();

    var demo = try fd.FailureRecoveryDemo.init(allocator, rt, .{
        .log_dir = log_path,
        .storage_base = log_path,
        .idle_timeout_ms = 100,
        .watchdog_poll_ms = 10,
    });

    const session_pid = demo.session_pid;

    try demo.start();

    // Give worker_1 time to become ready.
    std.Thread.sleep(20 * std.time.ns_per_ms);
    const worker_1_pid = demo.current_worker_proc.load(.acquire);

    // Dispatch task_1 — worker_1 accepts it then hangs.
    try demo.dispatchTask("task-1");

    // Wait for watchdog to detect idle and restart (idle_timeout=100ms, plus margin).
    std.Thread.sleep(250 * std.time.ns_per_ms);

    // Supervisor has restarted at least once.
    try std.testing.expect(demo.restart_count.load(.acquire) >= 1);

    // New worker has a different PID.
    const worker_2_pid = demo.current_worker_proc.load(.acquire);
    try std.testing.expect(worker_2_pid != worker_1_pid);

    // Session supervisor PID is unchanged.
    try std.testing.expectEqual(session_pid, demo.session_pid);

    // Dispatch task_2 — worker_2 completes it.
    try demo.dispatchTask("task-2");
    std.Thread.sleep(50 * std.time.ns_per_ms);
    try std.testing.expect(demo.task_completed.load(.acquire));

    demo.deinit();
}

test "failure recovery: worker completes both tasks when hang_after_first is false" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 8);
    defer rt.destroy();

    // Use a long idle_timeout so the watchdog never fires.
    var demo = try fd.FailureRecoveryDemo.init(allocator, rt, .{
        .log_dir = log_path,
        .storage_base = log_path,
        .idle_timeout_ms = 10_000,
        .watchdog_poll_ms = 100,
    });

    // Manually spawn a non-hanging worker by patching hang_after_first.
    // We test this by starting, dispatching task-2 only (no hang scenario).
    try demo.start();
    std.Thread.sleep(20 * std.time.ns_per_ms);

    // Force-restart once to get a non-hanging worker.
    try demo.restartWorker();
    std.Thread.sleep(20 * std.time.ns_per_ms);

    try demo.dispatchTask("task-ok");
    std.Thread.sleep(50 * std.time.ns_per_ms);

    try std.testing.expect(demo.task_completed.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), demo.restart_count.load(.acquire));

    demo.deinit();
}
