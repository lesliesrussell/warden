// warden-r28
// logs.stream RPC handler + its log-parsing helper.

const std = @import("std");
const clock = @import("../clock.zig");
const transport = @import("transport.zig");
const control = @import("../control.zig");

const HandlerCtx = control.HandlerCtx;
const parseBeamProc = transport.parseBeamProc;
const writeJsonEscapedString = transport.writeJsonEscapedString;
const writeFrame = transport.writeFrame;

fn extractTs(line: []const u8) f64 {
    const key = "\"ts\":";
    const start = std.mem.indexOf(u8, line, key) orelse return 0;
    const num_start = start + key.len;
    var end = num_start;
    while (end < line.len and line[end] != ',' and line[end] != '}') : (end += 1) {}
    return std.fmt.parseFloat(f64, line[num_start..end]) catch 0;
}

pub fn handleLogsStream(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const stream = h.stream;
    const r = h.r;
    const log_dir = cs.log_dir orelse return r.err("log_dir not configured");

    // Extract payload fields.
    var pid_str: []const u8 = "";
    var since_ms: u64 = 0;
    var grep_pattern: ?[]const u8 = null;
    var follow = false;

    if (payload_val) |pv| {
        switch (pv) {
            .object => |p| {
                if (p.get("pid")) |pv2| pid_str = switch (pv2) {
                    .string => |s| s,
                    else => "",
                };
                if (p.get("since_ms")) |sv| since_ms = switch (sv) {
                    .integer => |i| @intCast(i),
                    else => 0,
                };
                if (p.get("grep")) |gv| grep_pattern = switch (gv) {
                    .string => |s| s,
                    else => null,
                };
                if (p.get("follow")) |fv| follow = switch (fv) {
                    .bool => |b| b,
                    else => false,
                };
            },
            else => {},
        }
    }

    // Parse PID: "beam/proc".
    // warden-y3s: shared beam/proc parse; logs.stream maps both numeric failures
    // to the same "invalid pid" message it historically used.
    const parsed_pid = switch (parseBeamProc(pid_str)) {
        .ok => |p| p,
        .no_slash => return r.err("invalid pid format, expected beam/proc"),
        .bad_beam => return r.err("invalid pid"),
        .bad_proc => return r.err("invalid pid"),
    };
    const beam_id = parsed_pid.beam;
    const proc_id = parsed_pid.proc;

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{d}-{d}.log", .{ log_dir, beam_id, proc_id });
    defer allocator.free(log_path);

    const initial = std.Io.Dir.cwd().readFileAlloc(cs.runtime.io, log_path, allocator, .limited(64 * 1024 * 1024)) catch
        return r.err("log file not found");
    defer allocator.free(initial);

    const cutoff_ts: f64 = if (since_ms > 0)
        @as(f64, @floatFromInt(clock.nowMs() - @as(i64, @intCast(since_ms)))) / 1000.0
    else
        0.0;

    if (!follow) {
        // Read entire file, filter, return as JSON array.
        const content = initial;

        var buf: std.Io.Writer.Allocating = .init(allocator);
        defer buf.deinit();
        const w = &buf.writer;

        try r.okPrefix(w);
        try w.writeAll("{\"lines\":[");

        var count: usize = 0;
        var lines = std.mem.splitScalar(u8, content, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trimEnd(u8, line, "\r");
            if (trimmed.len == 0) continue;
            if (cutoff_ts > 0 and extractTs(trimmed) < cutoff_ts) continue;
            if (grep_pattern) |pat| {
                if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
            }
            if (count > 0) try w.writeByte(',');
            try writeJsonEscapedString(w, trimmed);
            count += 1;
        }

        try w.print("],\"count\":{d}}}", .{count});
        try r.okSuffix(w);
        return writeFrame(cs.runtime.io, stream, buf.writer.buffered());
    }

    // Follow mode: send streaming header, then tail the file.
    try r.ok("{\"streaming\":true}");

    // Send existing content first.
    const content = initial;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimEnd(u8, line, "\r");
        if (trimmed.len == 0) continue;
        if (cutoff_ts > 0 and extractTs(trimmed) < cutoff_ts) continue;
        if (grep_pattern) |pat| {
            if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
        }
        var line_buf: std.Io.Writer.Allocating = .init(allocator);
        defer line_buf.deinit();
        const lw = &line_buf.writer;
        try lw.writeAll("{\"kind\":\"line\",\"data\":");
        try writeJsonEscapedString(lw, trimmed);
        try lw.writeByte('}');
        writeFrame(cs.runtime.io, stream, line_buf.writer.buffered()) catch return;
    }

    // Poll for new content.
    var pos: usize = initial.len;
    while (!cs.stopping.load(.acquire)) {
        clock.sleepNs(100 * std.time.ns_per_ms);
        const whole = std.Io.Dir.cwd().readFileAlloc(cs.runtime.io, log_path, allocator, .limited(64 * 1024 * 1024)) catch break;
        defer allocator.free(whole);
        if (whole.len <= pos) continue;
        const new_content = whole[pos..];
        pos = whole.len;

        var new_lines = std.mem.splitScalar(u8, new_content, '\n');
        while (new_lines.next()) |line| {
            const trimmed = std.mem.trimEnd(u8, line, "\r");
            if (trimmed.len == 0) continue;
            if (grep_pattern) |pat| {
                if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
            }
            var line_buf: std.Io.Writer.Allocating = .init(allocator);
            defer line_buf.deinit();
            const lw = &line_buf.writer;
            try lw.writeAll("{\"kind\":\"line\",\"data\":");
            try writeJsonEscapedString(lw, trimmed);
            try lw.writeByte('}');
            writeFrame(cs.runtime.io, stream, line_buf.writer.buffered()) catch return;
        }
    }
}

