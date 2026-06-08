// warden-1ud

const std = @import("std");
const testutil = @import("testutil.zig");
const clock = @import("clock.zig");
const beam = @import("beam.zig");
const topology = @import("topology.zig");

const Topology = topology.Topology;
const SessionConfig = topology.SessionConfig;
const AgentMsgType = topology.AgentMsgType;
const Pid = beam.Pid;
const MessageEnvelope = beam.MessageEnvelope;

// warden-1ud
test "Topology.init + start boots without panic" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 100);
    defer rt.destroy();

    const config = SessionConfig{
        .session_id = "test-session",
        .tool_worker_count = 2,
        .log_dir = log_dir,
        .storage_base = st_dir,
    };

    var topo = try Topology.init(allocator, rt, config);
    try topo.start();

    // Brief pause so workers can reach ready state.
    clock.sleepNs(20 * std.time.ns_per_ms);

    try topo.shutdown();
    topo.deinit();
}

// warden-1ud
test "planner -> tool_worker round trip: send run_task, verify task_result arrives" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 101);
    defer rt.destroy();

    const config = SessionConfig{
        .session_id = "roundtrip-session",
        .tool_worker_count = 2,
        .log_dir = log_dir,
        .storage_base = st_dir,
    };

    var topo = try Topology.init(allocator, rt, config);
    try topo.start();

    // Allow workers to reach their main loops.
    clock.sleepNs(30 * std.time.ns_per_ms);

    // Allocate a "caller" pid with a mailbox so the planner can reply to us.
    const caller_pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(caller_pid, .{});

    const caller_str = try std.fmt.allocPrint(allocator, "{d}", .{caller_pid.proc});
    defer allocator.free(caller_str);
    const planner_str = try std.fmt.allocPrint(allocator, "{d}", .{topo.planner_pid.proc});
    defer allocator.free(planner_str);

    // Send run_task to planner with reply_to = caller.
    const task_msg = MessageEnvelope{
        .kind = .request,
        .@"type" = AgentMsgType.run_task,
        .id = "task-001",
        .from = caller_str,
        .to = planner_str,
        .reply_to = caller_str,
        .trace_id = "trace-abc",
        .session_id = "roundtrip-session",
        .task_id = "task-001",
        .body = .{ .string = "do something" },
    };

    const planner_mb = rt.getMailbox(topo.planner_pid) orelse return error.NoPlannerMailbox;
    _ = try planner_mb.enqueue(task_msg);
    rt.scheduler.notifyMessage(topo.planner_pid);

    // Wait up to 3s for the task_result to arrive in caller's mailbox.
    const match_result = struct {
        fn f(msg: *const MessageEnvelope) bool {
            return std.mem.eql(u8, msg.@"type", AgentMsgType.task_result);
        }
    }.f;

    const caller_mb = rt.getMailbox(caller_pid) orelse return error.NoCallerMailbox;
    const deadline_ms: u64 = 3000;
    const start_ms = @as(u64, @intCast(clock.nowMs()));
    var result_arrived = false;
    while (true) {
        const elapsed = @as(u64, @intCast(clock.nowMs())) -| start_ms;
        if (elapsed >= deadline_ms) break;

        caller_mb.mu.lock();
        var found_idx: ?usize = null;
        for (caller_mb.queue.items, 0..) |*m, i| {
            if (match_result(m)) {
                found_idx = i;
                break;
            }
        }
        const result_opt: ?MessageEnvelope = if (found_idx) |idx|
            caller_mb.queue.orderedRemove(idx)
        else
            null;
        caller_mb.mu.unlock();

        if (result_opt) |_| {
            result_arrived = true;
            break;
        }
        clock.sleepNs(5 * std.time.ns_per_ms);
    }

    try topo.shutdown();
    topo.deinit();

    try std.testing.expect(result_arrived);
}

// warden-1ud
test "watchdog responds to health_check with health_ok" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 102);
    defer rt.destroy();

    const config = SessionConfig{
        .session_id = "watchdog-session",
        .tool_worker_count = 1,
        .log_dir = log_dir,
        .storage_base = st_dir,
    };

    var topo = try Topology.init(allocator, rt, config);
    try topo.start();

    clock.sleepNs(30 * std.time.ns_per_ms);

    // Allocate caller.
    const caller_pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(caller_pid, .{});

    const caller_str = try std.fmt.allocPrint(allocator, "{d}", .{caller_pid.proc});
    defer allocator.free(caller_str);
    const wd_str = try std.fmt.allocPrint(allocator, "{d}", .{topo.watchdog_pid.proc});
    defer allocator.free(wd_str);

    // Send health_check.
    const hc_msg = MessageEnvelope{
        .kind = .request,
        .@"type" = AgentMsgType.health_check,
        .id = "hc-001",
        .from = caller_str,
        .to = wd_str,
        .body = .null,
    };

    const wd_mb = rt.getMailbox(topo.watchdog_pid) orelse return error.NoWatchdogMailbox;
    _ = try wd_mb.enqueue(hc_msg);
    rt.scheduler.notifyMessage(topo.watchdog_pid);

    // Wait for health_ok.
    const caller_mb = rt.getMailbox(caller_pid) orelse return error.NoCallerMailbox;
    const deadline_ms: u64 = 2000;
    const start_ms = @as(u64, @intCast(clock.nowMs()));
    var health_ok = false;
    while (true) {
        const elapsed = @as(u64, @intCast(clock.nowMs())) -| start_ms;
        if (elapsed >= deadline_ms) break;

        caller_mb.mu.lock();
        var found_idx: ?usize = null;
        for (caller_mb.queue.items, 0..) |*m, i| {
            if (std.mem.eql(u8, m.@"type", AgentMsgType.health_ok)) {
                found_idx = i;
                break;
            }
        }
        const result_opt: ?MessageEnvelope = if (found_idx) |idx|
            caller_mb.queue.orderedRemove(idx)
        else
            null;
        caller_mb.mu.unlock();

        if (result_opt) |_| {
            health_ok = true;
            break;
        }
        clock.sleepNs(5 * std.time.ns_per_ms);
    }

    try topo.shutdown();
    topo.deinit();

    try std.testing.expect(health_ok);
}

// warden-1ud
test "Topology.shutdown tears down cleanly (no hanging threads)" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 103);
    defer rt.destroy();

    const config = SessionConfig{
        .session_id = "shutdown-session",
        .tool_worker_count = 3,
        .log_dir = log_dir,
        .storage_base = st_dir,
    };

    var topo = try Topology.init(allocator, rt, config);
    try topo.start();

    clock.sleepNs(20 * std.time.ns_per_ms);

    // shutdown + deinit must complete without hanging.
    try topo.shutdown();
    topo.deinit();
    // If we reach here without a deadlock, the test passes.
}

// warden-1ud
test "trace_id and session_id appear in log output" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 104);
    defer rt.destroy();

    const config = SessionConfig{
        .session_id = "log-trace-session",
        .tool_worker_count = 1,
        .log_dir = log_dir,
        .storage_base = st_dir,
    };

    var topo = try Topology.init(allocator, rt, config);
    try topo.start();

    clock.sleepNs(30 * std.time.ns_per_ms);

    // Send a run_task with trace_id and session_id set so they flow through logs.
    const caller_pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(caller_pid, .{});

    const caller_str = try std.fmt.allocPrint(allocator, "{d}", .{caller_pid.proc});
    defer allocator.free(caller_str);
    const planner_str = try std.fmt.allocPrint(allocator, "{d}", .{topo.planner_pid.proc});
    defer allocator.free(planner_str);

    const task_msg = MessageEnvelope{
        .kind = .request,
        .@"type" = AgentMsgType.run_task,
        .id = "trace-task-001",
        .from = caller_str,
        .to = planner_str,
        .reply_to = caller_str,
        .trace_id = "trace-xyz-unique",
        .session_id = "log-trace-session",
        .task_id = "trace-task-001",
        .body = .{ .string = "traced task" },
    };

    const planner_mb = rt.getMailbox(topo.planner_pid) orelse return error.NoPlannerMailbox;
    _ = try planner_mb.enqueue(task_msg);
    rt.scheduler.notifyMessage(topo.planner_pid);

    // Wait for round-trip to complete.
    clock.sleepNs(500 * std.time.ns_per_ms);

    try topo.shutdown();
    topo.deinit();

    // Read all log files in log_dir and verify trace_id + session_id appear.
    var log_d = try std.Io.Dir.openDirAbsolute(std.testing.io, log_dir, .{ .iterate = true });
    defer log_d.close(std.testing.io);

    var found_trace = false;
    var found_session = false;
    var it = log_d.iterate();
    while (try it.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        const content = log_d.readFileAlloc(std.testing.io, entry.name, allocator, .limited(1024 * 1024)) catch continue;
        defer allocator.free(content);
        if (std.mem.indexOf(u8, content, "trace-xyz-unique") != null) found_trace = true;
        if (std.mem.indexOf(u8, content, "log-trace-session") != null) found_session = true;
        if (found_trace and found_session) break;
    }

    try std.testing.expect(found_trace);
    try std.testing.expect(found_session);
}
