// warden-7q1

const std = @import("std");
const testutil = @import("testutil.zig");
const beam = @import("beam.zig");

const Pid = beam.Pid;
const MessageEnvelope = beam.MessageEnvelope;
const MessageKind = beam.MessageKind;
const Namespace = beam.Namespace;

// warden-7q1
/// Build a minimal MessageEnvelope suitable for tests.
fn testMsg(
    allocator: std.mem.Allocator,
    id: []const u8,
    from_pid: Pid,
    to_pid: Pid,
) !MessageEnvelope {
    return MessageEnvelope{
        .kind = .request,
        .@"type" = "test.ping",
        .id = id,
        .from = try std.fmt.allocPrint(allocator, "{d}", .{from_pid.proc}),
        .to = try std.fmt.allocPrint(allocator, "{d}", .{to_pid.proc}),
        .body = .null,
    };
}

// warden-7q1
test "Runtime.init and start succeed" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 42);
    defer rt.destroy();

    try rt.start(0);
}

// warden-7q1
test "send + recv round trip between two Ctx instances" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 1);
    defer rt.destroy();

    // Spawn two pids in the registry.
    const pid_a = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid_a, .{});
    const pid_b = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid_b, .{});

    var ctx_a = try beam.Ctx.init(rt, pid_a, log_dir, st_dir);
    defer ctx_a.deinit();
    var ctx_b = try beam.Ctx.init(rt, pid_b, log_dir, st_dir);
    defer ctx_b.deinit();

    // A sends to B.
    const msg_id = "msg-001";
    const from_str = try std.fmt.allocPrint(allocator, "{d}", .{pid_a.proc});
    defer allocator.free(from_str);
    const to_str = try std.fmt.allocPrint(allocator, "{d}", .{pid_b.proc});
    defer allocator.free(to_str);

    const msg = MessageEnvelope{
        .kind = .request,
        .@"type" = "test.hello",
        .id = msg_id,
        .from = from_str,
        .to = to_str,
        .body = .null,
    };
    try ctx_a.send(pid_b, msg);

    // B receives — match anything.
    const match_any = struct {
        fn f(m: *const MessageEnvelope) bool {
            _ = m;
            return true;
        }
    }.f;

    const received = try ctx_b.recv(match_any, 500);
    try std.testing.expect(received != null);
    try std.testing.expectEqualStrings("msg-001", received.?.id);
}

// warden-7q1
test "call + reply: A calls B, B replies, A gets response" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 2);
    defer rt.destroy();

    const pid_a = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid_a, .{});
    const pid_b = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid_b, .{});

    var ctx_a = try beam.Ctx.init(rt, pid_a, log_dir, st_dir);
    defer ctx_a.deinit();
    var ctx_b = try beam.Ctx.init(rt, pid_b, log_dir, st_dir);
    defer ctx_b.deinit();

    const from_a = try std.fmt.allocPrint(allocator, "{d}", .{pid_a.proc});
    defer allocator.free(from_a);
    const to_b = try std.fmt.allocPrint(allocator, "{d}", .{pid_b.proc});
    defer allocator.free(to_b);
    const from_b = try std.fmt.allocPrint(allocator, "{d}", .{pid_b.proc});
    defer allocator.free(from_b);
    const to_a = try std.fmt.allocPrint(allocator, "{d}", .{pid_a.proc});
    defer allocator.free(to_a);

    const request_id = "req-abc";

    // Spawn a thread to act as process B: recv request, send reply.
    const ThreadCtx = struct {
        ctx_b: *beam.Ctx,
        pid_a: Pid,
        from_b: []const u8,
        to_a: []const u8,
        request_id: []const u8,
    };
    var tc = ThreadCtx{
        .ctx_b = &ctx_b,
        .pid_a = pid_a,
        .from_b = from_b,
        .to_a = to_a,
        .request_id = request_id,
    };
    const thread = try std.Thread.spawn(.{}, struct {
        fn run(t: *ThreadCtx) void {
            // B waits for a request.
            const match_any = struct {
                fn f(m: *const MessageEnvelope) bool {
                    _ = m;
                    return true;
                }
            }.f;
            const req = (t.ctx_b.recv(match_any, 2000) catch return) orelse return;
            // B sends reply with corr = req.id
            const reply_msg = MessageEnvelope{
                .kind = .response,
                .@"type" = "test.pong",
                .id = "rep-xyz",
                .from = t.from_b,
                .to = t.to_a,
                .corr = req.id,
                .body = .null,
            };
            t.ctx_b.send(t.pid_a, reply_msg) catch {};
        }
    }.run, .{&tc});

    // A calls B with timeout 2s.
    const req_msg = MessageEnvelope{
        .kind = .request,
        .@"type" = "test.ping",
        .id = request_id,
        .from = from_a,
        .to = to_b,
        .body = .null,
    };
    const response = try ctx_a.call(pid_b, req_msg, 2000);
    thread.join();

    try std.testing.expectEqualStrings("test.pong", response.@"type");
    try std.testing.expectEqualStrings(request_id, response.corr.?);
}

// warden-7q1
test "spawn allocates a new PID and it appears in registry" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 3);
    defer rt.destroy();

    const pid_parent = try rt.registry.spawn(.native_supervisor, null, .{});
    try rt.allocMailbox(pid_parent, .{});
    var ctx = try beam.Ctx.init(rt, pid_parent, log_dir, st_dir);
    defer ctx.deinit();

    const noop = struct {
        fn f(_: ?*anyopaque) void {}
    }.f;

    const child_pid = try ctx.spawn(.native_worker, noop, .{});

    // Child must be in registry.
    const entry = rt.registry.lookup(child_pid);
    try std.testing.expect(entry != null);
}

// warden-7q1
test "promote changes activity class via policy engine" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 4);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid, .{});
    var ctx = try beam.Ctx.init(rt, pid, log_dir, st_dir);
    defer ctx.deinit();

    // Promote to elevated class.
    try ctx.promote(pid, .elevated, 5000, "test promotion");

    // Verify via registry entry that policy was updated.
    const entry = rt.registry.lookup(pid);
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(beam.ActivityClass.elevated, entry.?.policy.activity_class);
}

// warden-7q1
test "fsWrite + fsRead round trip via storage" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 5);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid, .{});
    var ctx = try beam.Ctx.init(rt, pid, log_dir, st_dir);
    defer ctx.deinit();

    try ctx.fsWrite(.proc_temp, "data.txt", "hello storage");
    const data = try ctx.fsRead(.proc_temp, "data.txt");
    defer allocator.free(data);

    try std.testing.expectEqualStrings("hello storage", data);
}

// warden-7q1
test "log/note/warn emit events to logger without error" {
    const allocator = std.testing.allocator;

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_st = std.testing.tmpDir(.{});
    defer tmp_st.cleanup();

    var log_buf: [std.fs.max_path_bytes]u8 = undefined;
    const log_dir = try testutil.tmpAbs(&log_buf, &tmp_log);
    var st_buf: [std.fs.max_path_bytes]u8 = undefined;
    const st_dir = try testutil.tmpAbs(&st_buf, &tmp_st);

    const rt = try beam.Runtime.init(allocator, 6);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.allocMailbox(pid, .{});
    var ctx = try beam.Ctx.init(rt, pid, log_dir, st_dir);
    defer ctx.deinit();

    try ctx.log("info", "a log message", null);
    try ctx.note("a note", null);
    try ctx.metric("throughput", 42.5, null);
    try ctx.warning("a warning", null);
    try ctx.err("an error", null);
}
