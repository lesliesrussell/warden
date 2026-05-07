// warden-dhx

const std = @import("std");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");
const scheduler_mod = @import("scheduler.zig");
const supervisor_mod = @import("supervisor.zig");

const Pid = types.Pid;
const ProcessKind = types.ProcessKind;
const PolicyEnvelope = types.PolicyEnvelope;
const Registry = registry_mod.Registry;
const Scheduler = scheduler_mod.Scheduler;
const Supervisor = supervisor_mod.Supervisor;
const ChildSpec = supervisor_mod.ChildSpec;
const ExitReason = supervisor_mod.ExitReason;
const RestartStrategy = supervisor_mod.RestartStrategy;
const SupervisorError = supervisor_mod.SupervisorError;

// warden-dhx
fn noop(ctx: ?*anyopaque) void {
    _ = ctx;
}

fn makeSpec(id: []const u8, strategy: RestartStrategy) ChildSpec {
    return ChildSpec{
        .id = id,
        .kind = .native_worker,
        .restart = strategy,
        .entry = noop,
        .ctx = null,
    };
}

// warden-dhx
test "startChild: spawns child with correct PID and running state" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    const spec = makeSpec("worker-a", .one_for_one);
    const pid = try sup.startChild(spec);

    // PID should be in registry.
    const entry = reg.lookup(pid);
    try std.testing.expect(entry != null);

    // Child should appear in supervisor's list with running state.
    sup.mutex.lock();
    defer sup.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 1), sup.children.items.len);
    try std.testing.expect(sup.children.items[0].pid.proc == pid.proc);
    try std.testing.expect(sup.children.items[0].state == .running);
}

// warden-dhx
test "childExited: temporary strategy — child not restarted" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    const pid = try sup.startChild(makeSpec("tmp", .temporary));
    try sup.childExited(pid, .abnormal);

    sup.mutex.lock();
    defer sup.mutex.unlock();
    // Child should remain dead, not restarted.
    try std.testing.expectEqual(@as(usize, 1), sup.children.items.len);
    try std.testing.expect(sup.children.items[0].state == .dead);
    try std.testing.expectEqual(@as(u32, 0), sup.children.items[0].restart_count);
}

// warden-dhx
test "childExited: transient + normal exit — not restarted" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    const pid = try sup.startChild(makeSpec("tr-normal", .transient));
    try sup.childExited(pid, .normal);

    sup.mutex.lock();
    defer sup.mutex.unlock();
    try std.testing.expect(sup.children.items[0].state == .dead);
    try std.testing.expectEqual(@as(u32, 0), sup.children.items[0].restart_count);
}

// warden-dhx
test "childExited: transient + abnormal exit — restarted" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    const pid = try sup.startChild(makeSpec("tr-abnormal", .transient));
    try sup.childExited(pid, .abnormal);

    sup.mutex.lock();
    defer sup.mutex.unlock();
    try std.testing.expect(sup.children.items[0].state == .running);
    try std.testing.expectEqual(@as(u32, 1), sup.children.items[0].restart_count);
}

// warden-dhx
test "childExited: one_for_one — only failed child restarted, siblings untouched" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    const pid_a = try sup.startChild(makeSpec("a", .one_for_one));
    const pid_b = try sup.startChild(makeSpec("b", .one_for_one));
    const pid_c = try sup.startChild(makeSpec("c", .one_for_one));

    // Record sibling PIDs before exit.
    sup.mutex.lock();
    const orig_b_proc = sup.children.items[1].pid.proc;
    const orig_c_proc = sup.children.items[2].pid.proc;
    sup.mutex.unlock();

    // Kill child a.
    try sup.childExited(pid_a, .abnormal);

    sup.mutex.lock();
    defer sup.mutex.unlock();

    // a should be restarted (new pid, running).
    try std.testing.expect(sup.children.items[0].state == .running);
    try std.testing.expectEqual(@as(u32, 1), sup.children.items[0].restart_count);

    // b and c should be untouched (same proc id, running state).
    try std.testing.expectEqual(orig_b_proc, sup.children.items[1].pid.proc);
    try std.testing.expect(sup.children.items[1].state == .running);
    try std.testing.expectEqual(@as(u32, 0), sup.children.items[1].restart_count);

    try std.testing.expectEqual(orig_c_proc, sup.children.items[2].pid.proc);
    try std.testing.expect(sup.children.items[2].state == .running);

    _ = pid_b;
    _ = pid_c;
}

// warden-dhx
test "childExited: one_for_all — all children restarted" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    const pid_a = try sup.startChild(makeSpec("a", .one_for_all));
    _ = try sup.startChild(makeSpec("b", .one_for_all));
    _ = try sup.startChild(makeSpec("c", .one_for_all));

    try sup.childExited(pid_a, .abnormal);

    sup.mutex.lock();
    defer sup.mutex.unlock();

    // All 3 children should be running with restart_count incremented.
    try std.testing.expectEqual(@as(usize, 3), sup.children.items.len);
    for (sup.children.items) |c| {
        try std.testing.expect(c.state == .running);
        try std.testing.expectEqual(@as(u32, 1), c.restart_count);
    }
}

// warden-dhx
test "childExited: rest_for_one — only failed + later children restarted" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    _ = try sup.startChild(makeSpec("a", .rest_for_one));
    const pid_b = try sup.startChild(makeSpec("b", .rest_for_one));
    _ = try sup.startChild(makeSpec("c", .rest_for_one));

    // Record original pid for a.
    sup.mutex.lock();
    const orig_a_proc = sup.children.items[0].pid.proc;
    sup.mutex.unlock();

    // Kill child b (index 1).
    try sup.childExited(pid_b, .abnormal);

    sup.mutex.lock();
    defer sup.mutex.unlock();

    // a should be untouched.
    try std.testing.expectEqual(orig_a_proc, sup.children.items[0].pid.proc);
    try std.testing.expect(sup.children.items[0].state == .running);
    try std.testing.expectEqual(@as(u32, 0), sup.children.items[0].restart_count);

    // b and c should be restarted.
    try std.testing.expect(sup.children.items[1].state == .running);
    try std.testing.expectEqual(@as(u32, 1), sup.children.items[1].restart_count);
    try std.testing.expect(sup.children.items[2].state == .running);
    try std.testing.expectEqual(@as(u32, 1), sup.children.items[2].restart_count);
}

// warden-dhx
test "max restart intensity exceeded returns error" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    var sup = try Supervisor.init(std.testing.allocator, &reg, sched);
    defer sup.deinit();

    // Lower threshold to 2 for this test.
    sup.max_restarts = 2;

    // First child exits and restarts (count = 1).
    const pid1 = try sup.startChild(makeSpec("w1", .one_for_one));
    try sup.childExited(pid1, .abnormal);

    // Second exit after restart.
    sup.mutex.lock();
    const pid1_new = sup.children.items[0].pid;
    sup.mutex.unlock();
    try sup.childExited(pid1_new, .abnormal);

    // Third exit should exceed intensity.
    sup.mutex.lock();
    const pid1_new2 = sup.children.items[0].pid;
    sup.mutex.unlock();

    const result = sup.childExited(pid1_new2, .abnormal);
    try std.testing.expectError(SupervisorError.MaxRestartIntensityExceeded, result);
}
