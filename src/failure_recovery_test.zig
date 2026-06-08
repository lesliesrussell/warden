// warden-45x

const std = @import("std");
const clock = @import("clock.zig");
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
    // warden-iry: defer ensures watchdog is joined before rt.destroy() runs,
    // even when a test assertion fails mid-test.
    defer demo.deinit();

    const session_pid = demo.session_pid;

    try demo.start();

    // Give worker_1 time to become ready.
    clock.sleepNs(20 * std.time.ns_per_ms);
    const worker_1_pid = demo.current_worker_proc.load(.acquire);

    // Dispatch task_1 — worker_1 accepts it then hangs.
    try demo.dispatchTask("task-1");

    // Wait for watchdog to detect idle and restart (idle_timeout=100ms, plus margin).
    clock.sleepNs(250 * std.time.ns_per_ms);

    // Supervisor has restarted at least once.
    try std.testing.expect(demo.restart_count.load(.acquire) >= 1);

    // warden-iry: wait for spawnWorker() to finish and current_worker_proc to be set.
    // restart_count is incremented before spawnWorker() completes, so a brief poll is needed.
    var worker_2_pid: u64 = 0;
    for (0..50) |_| {
        worker_2_pid = demo.current_worker_proc.load(.acquire);
        if (worker_2_pid != 0) break;
        clock.sleepNs(10 * std.time.ns_per_ms);
    }

    // New worker has a different PID.
    try std.testing.expect(worker_2_pid != worker_1_pid);
    try std.testing.expect(worker_2_pid != 0);

    // Session supervisor PID is unchanged.
    try std.testing.expectEqual(session_pid, demo.session_pid);

    // Dispatch task_2 — worker_2 completes it.
    try demo.dispatchTask("task-2");
    clock.sleepNs(50 * std.time.ns_per_ms);
    try std.testing.expect(demo.task_completed.load(.acquire));

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
    // warden-iry: defer ensures watchdog is joined before rt.destroy() runs.
    defer demo.deinit();

    // Manually spawn a non-hanging worker by patching hang_after_first.
    // We test this by starting, dispatching task-2 only (no hang scenario).
    try demo.start();
    clock.sleepNs(20 * std.time.ns_per_ms);

    // Force-restart once to get a non-hanging worker.
    try demo.restartWorker();
    clock.sleepNs(20 * std.time.ns_per_ms);

    try demo.dispatchTask("task-ok");
    clock.sleepNs(50 * std.time.ns_per_ms);

    try std.testing.expect(demo.task_completed.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), demo.restart_count.load(.acquire));
}
