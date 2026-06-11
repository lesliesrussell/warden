// warden-7a1

const std = @import("std");
const clock = @import("clock.zig");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");
const scheduler_mod = @import("scheduler.zig");

const Pid = types.Pid;
const ProcessKind = types.ProcessKind;
const PolicyEnvelope = types.PolicyEnvelope;
const ProcessState = types.ProcessState;
const Registry = registry_mod.Registry;
const Scheduler = scheduler_mod.Scheduler;

fn defaultPolicy() PolicyEnvelope {
    return .{};
}

/// Spawn a process via the registry in .starting state, then transition to .ready
/// so that the scheduler can operate on it (e.g. makeWaiting, pause, etc.).
fn spawnReady(reg: *Registry) !Pid {
    const pid = try reg.spawn(.native_worker, null, defaultPolicy());
    try reg.transition(pid, .ready);
    return pid;
}

// warden-7a1
test "makeReady: process appears in ready list" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 1);
    defer sched.deinit();

    const pid = try spawnReady(&reg);
    try sched.makeReady(pid);

    sched.mutex.lock();
    defer sched.mutex.unlock();
    var found = false;
    for (sched.ready.items) |p| {
        if (p.proc == pid.proc and p.beam == pid.beam) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

// warden-7a1
test "submit: task runs on worker thread" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    // 2 workers so the task can execute without deadlocking the test thread.
    const sched = try Scheduler.init(std.testing.allocator, &reg, 2);
    defer sched.deinit();

    const pid = try spawnReady(&reg);

    // Use an atomic flag to detect completion.
    var done = std.atomic.Value(bool).init(false);

    const Ctx = struct {
        flag: *std.atomic.Value(bool),
    };
    var ctx = Ctx{ .flag = &done };

    const entryFn = struct {
        fn run(raw: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(raw));
            c.flag.store(true, .release);
        }
    }.run;

    try sched.submit(pid, entryFn, &ctx);

    // Poll with a 100ms deadline.
    const deadline = clock.nowMs() + 100;
    while (!done.load(.acquire)) {
        if (clock.nowMs() > deadline) break;
        clock.sleepNs(1 * std.time.ns_per_ms);
    }
    try std.testing.expect(done.load(.acquire));
}

// warden-7a1
test "makeWaiting: process removed from ready list and placed in waiting" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 1);
    defer sched.deinit();

    const pid = try spawnReady(&reg);
    try sched.makeReady(pid);
    try sched.makeWaiting(pid, null);

    sched.mutex.lock();
    defer sched.mutex.unlock();

    // Should not be in ready list.
    for (sched.ready.items) |p| {
        try std.testing.expect(!(p.proc == pid.proc and p.beam == pid.beam));
    }
    // Should be in waiting map.
    try std.testing.expect(sched.waiting.contains(pid.proc));
}

// warden-7a1
test "notifyMessage: waiting process moves back to ready" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 1);
    defer sched.deinit();

    const pid = try spawnReady(&reg);
    try sched.makeReady(pid);
    try sched.makeWaiting(pid, null);

    sched.notifyMessage(pid);

    sched.mutex.lock();
    defer sched.mutex.unlock();

    // Should no longer be in waiting.
    try std.testing.expect(!sched.waiting.contains(pid.proc));

    // Should be in ready.
    var found = false;
    for (sched.ready.items) |p| {
        if (p.proc == pid.proc and p.beam == pid.beam) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

// warden-7a1
test "pause: prevents task from being submitted" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 1);
    defer sched.deinit();

    const pid = try spawnReady(&reg);
    try sched.makeReady(pid);
    try sched.pause(pid);

    // Verify process is in the paused set.
    sched.mutex.lock();
    const is_paused = sched.paused.contains(pid.proc);
    sched.mutex.unlock();
    try std.testing.expect(is_paused);

    // Submit should be a no-op for a paused process.
    const entryFn = struct {
        fn run(_: *anyopaque) void {}
    }.run;
    var dummy: u8 = 0;
    try sched.submit(pid, entryFn, &dummy);

    sched.mutex.lock();
    const queue_len = sched.task_queue.items.len;
    sched.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 0), queue_len);
}

// warden-7a1
test "resume_: paused process moves to ready" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 1);
    defer sched.deinit();

    const pid = try spawnReady(&reg);
    try sched.makeReady(pid);
    try sched.pause(pid);
    try sched.resume_(pid);

    sched.mutex.lock();
    defer sched.mutex.unlock();

    // Should not be paused.
    try std.testing.expect(!sched.paused.contains(pid.proc));

    // Should be in ready.
    var found = false;
    for (sched.ready.items) |p| {
        if (p.proc == pid.proc and p.beam == pid.beam) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

// warden-7a1
test "tick: expires timed-out waiter and moves to ready" {
    var reg = Registry.init(std.testing.allocator, 42);
    defer reg.deinit();

    const sched = try Scheduler.init(std.testing.allocator, &reg, 1);
    defer sched.deinit();

    const pid = try reg.spawn(.native_worker, null, defaultPolicy());

    // Manually insert into waiting with a 0ms timeout and past enqueued_at_ms,
    // bypassing the registry state machine so we don't need the process to be
    // in .ready state first.
    {
        sched.mutex.lock();
        defer sched.mutex.unlock();
        const entry = scheduler_mod.WaitEntry{
            .pid = pid,
            .timeout_ms = 0,
            .enqueued_at_ms = clock.nowMs() - 1, // 1ms in the past
        };
        try sched.waiting.put(pid.proc, entry);
    }

    try sched.tick();

    sched.mutex.lock();
    defer sched.mutex.unlock();

    // No longer waiting.
    try std.testing.expect(!sched.waiting.contains(pid.proc));

    // Now in ready.
    var found = false;
    for (sched.ready.items) |p| {
        if (p.proc == pid.proc and p.beam == pid.beam) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

// warden-zl0
fn zl0Noop(_: *anyopaque) void {}

test "scheduler admission picks highest activity class first" {
    var reg = Registry.init(std.testing.allocator, 1);
    defer reg.deinit();
    const p_normal = try reg.spawn(.native_worker, null, .{});
    const p_other = try reg.spawn(.native_worker, null, .{});

    var dummy: u8 = 0;
    const items = [_]scheduler_mod.Task{
        .{ .pid = p_normal, .entry_fn = zl0Noop, .ctx = &dummy, .reductions = 0 },
        .{ .pid = p_other, .entry_fn = zl0Noop, .ctx = &dummy, .reductions = 0 },
    };

    // elevated (index 1) outranks normal (index 0)
    try reg.setActivityClass(p_other, .elevated);
    try std.testing.expectEqual(@as(usize, 1), scheduler_mod.pickHighestClass(&items, &reg));

    // quarantined (.tiny) is deprioritized below normal -> index 0 wins
    try reg.setActivityClass(p_other, .tiny);
    try std.testing.expectEqual(@as(usize, 0), scheduler_mod.pickHighestClass(&items, &reg));

    // equal class -> FIFO tie-break (earliest index)
    try reg.setActivityClass(p_other, .normal);
    try std.testing.expectEqual(@as(usize, 0), scheduler_mod.pickHighestClass(&items, &reg));
}
