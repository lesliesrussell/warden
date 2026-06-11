// warden-554

const std = @import("std");
const logger = @import("logger.zig");

// warden-554
test "seq is strictly monotonic" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, 1, 100, tmp.dir);
    defer log.deinit();

    const n = 10;
    var i: u64 = 0;
    while (i < n) : (i += 1) {
        try log.emit(.{ .spawn = .{} }, null);
    }

    try std.testing.expectEqual(@as(u64, n), log.seq);
}

// warden-554
test "log file is named <beam>-<pid>.log" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const beam_id: u32 = 7;
    const pid: u64 = 42;

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, beam_id, pid, tmp.dir);
    defer log.deinit();

    // File should exist with the right name
    const f = try tmp.dir.openFile(std.testing.io, "7-42.log", .{});
    f.close(std.testing.io);
}

// warden-554
test "note event appears in file with correct fields" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, 1, 55, tmp.dir);
    try log.note("info", "hello from test", null);
    try log.flush();
    log.deinit();

    // Read back
    const content = try tmp.dir.readFileAlloc(std.testing.io, "1-55.log", allocator, .limited(64 * 1024));
    defer allocator.free(content);

    try std.testing.expect(std.mem.indexOf(u8, content, "\"event\":\"note\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"level\":\"info\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"message\":\"hello from test\"") != null);
}

// warden-554
test "recv event appears with msg_id field" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, 2, 77, tmp.dir);
    try log.emit(.{ .recv = .{
        .msg_id = "msg-001",
        .from = "pid:1/10",
        .msg_type = "request",
        .corr = "corr-xyz",
        .trace_id = "trace-abc",
        .task_id = "task-1",
    } }, null);
    try log.flush();
    log.deinit();

    const content = try tmp.dir.readFileAlloc(std.testing.io, "2-77.log", allocator, .limited(64 * 1024));
    defer allocator.free(content);

    try std.testing.expect(std.mem.indexOf(u8, content, "\"event\":\"recv\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"msg_id\":\"msg-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, content, "\"from\":\"pid:1/10\"") != null);
}

// warden-554
test "flush produces readable NDJSON" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, 3, 99, tmp.dir);
    try log.emit(.{ .spawn = .{ .kind = "native_worker", .parent_pid = 1 } }, null);
    try log.note("info", "started", null);
    try log.metric("reductions", 1234.0, null);
    try log.warn("low memory", null);
    try log.err("fatal error", null);
    try log.flush();
    log.deinit();

    // Read the file and parse each line as JSON
    const content = try tmp.dir.readFileAlloc(std.testing.io, "3-99.log", allocator, .limited(64 * 1024));
    defer allocator.free(content);

    var line_count: usize = 0;
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        // Each line must be valid JSON
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
        defer parsed.deinit();

        const obj = parsed.value.object;
        // Must have mandatory fields
        try std.testing.expect(obj.get("ts") != null);
        try std.testing.expect(obj.get("beam") != null);
        try std.testing.expect(obj.get("pid") != null);
        try std.testing.expect(obj.get("seq") != null);
        try std.testing.expect(obj.get("event") != null);

        line_count += 1;
    }

    // We emitted 5 events
    try std.testing.expectEqual(@as(usize, 5), line_count);
}

// warden-554
test "seq starts at 1 and increments" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, 4, 200, tmp.dir);
    try log.emit(.{ .spawn = .{} }, null);
    try log.emit(.{ .exit = .{} }, null);
    try log.emit(.{ .note = .{ .message = "third" } }, null);
    try log.flush();
    log.deinit();

    const content = try tmp.dir.readFileAlloc(std.testing.io, "4-200.log", allocator, .limited(64 * 1024));
    defer allocator.free(content);

    var seq: u64 = 1;
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
        defer parsed.deinit();
        const got_seq = parsed.value.object.get("seq").?.integer;
        try std.testing.expectEqual(@as(i64, @intCast(seq)), got_seq);
        seq += 1;
    }
}

// warden-ga2
// The active log rolls to <name>.log.1 once it passes rotate_at, bounding
// per-process disk usage instead of growing without limit.
test "log rotates past rotate_at threshold" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var log = try logger.ProcessLogger.init(std.testing.io, allocator, 1, 100, tmp.dir);
    defer log.deinit();
    log.rotate_at = 200; // tiny threshold so a few records trigger a roll

    var i: usize = 0;
    while (i < 40) : (i += 1) {
        try log.emit(.{ .spawn = .{} }, null);
    }
    try log.flush();

    // A rotated file must exist (rotation happened at least once).
    const rolled = tmp.dir.openFile(std.testing.io, "1-100.log.1", .{}) catch
        return error.RotationDidNotHappen;
    rolled.close(std.testing.io);

    // The active log was reset, so it is far smaller than the whole stream.
    const cur = try tmp.dir.openFile(std.testing.io, "1-100.log", .{});
    defer cur.close(std.testing.io);
    const st = try cur.stat(std.testing.io);
    try std.testing.expect(st.size < 40 * 200);
}
