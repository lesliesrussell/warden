// warden-39v
//
// Control server — accepts RPC requests over a Unix domain socket and routes
// them to the runtime's registry, policy engine, and logger.
//
// Transport: length-prefixed JSON frames (4-byte big-endian u32 length).
// Activation: call ControlServer.init() then .start().

const std = @import("std");
const beam_mod = @import("beam.zig");
const registry_mod = @import("registry.zig");
const types = @import("types.zig");

const Runtime = beam_mod.Runtime;
const ProcessEntry = registry_mod.ProcessEntry;

// warden-39v
/// Write a length-prefixed JSON frame to a stream.
pub fn writeFrame(stream: std.net.Stream, json: []const u8) !void {
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(json.len), .big);
    try stream.writeAll(&hdr);
    try stream.writeAll(json);
}

// warden-39v
/// Read a length-prefixed JSON frame. Caller owns the returned slice.
pub fn readFrame(allocator: std.mem.Allocator, stream: std.net.Stream) ![]u8 {
    var hdr: [4]u8 = undefined;
    const n = try stream.readAtLeast(&hdr, 4);
    if (n < 4) return error.ConnectionClosed;
    const length = std.mem.readInt(u32, &hdr, .big);
    if (length == 0) return error.EmptyFrame;
    const buf = try allocator.alloc(u8, length);
    errdefer allocator.free(buf);
    const read = try stream.readAtLeast(buf, length);
    if (read < length) return error.ConnectionClosed;
    return buf;
}

// warden-39v
fn handleBeamList(cs: *ControlServer, allocator: std.mem.Allocator, req_id: []const u8, stream: std.net.Stream) !void {
    const uptime_ms = std.time.milliTimestamp() - cs.started_at;

    cs.runtime.registry.mutex.lock();
    const proc_count = cs.runtime.registry.map.count();
    cs.runtime.registry.mutex.unlock();

    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null," ++
        "\"payload\":{{\"beams\":[{{\"beam_id\":{d},\"version\":\"0.1.0\"," ++
        "\"uptime_ms\":{d},\"process_count\":{d}}}]}}}}",
        .{ req_id, cs.runtime.beam_id, uptime_ms, proc_count },
    );
    defer allocator.free(resp);
    try writeFrame(stream, resp);
}

// warden-di6
fn handleProcList(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    // Extract optional filters from payload.
    var filter_beam: ?u32 = null;
    var filter_kind: ?[]const u8 = null;
    var filter_state: ?[]const u8 = null;

    if (payload_val) |pv| {
        switch (pv) {
            .object => |p| {
                if (p.get("beam")) |bv| {
                    filter_beam = switch (bv) {
                        .integer => |i| @intCast(i),
                        else => null,
                    };
                }
                if (p.get("kind")) |kv| {
                    filter_kind = switch (kv) {
                        .string => |s| s,
                        else => null,
                    };
                }
                if (p.get("state")) |sv| {
                    filter_state = switch (sv) {
                        .string => |s| s,
                        else => null,
                    };
                }
            },
            else => {},
        }
    }

    // Snapshot matching entries under the registry lock.
    var entries: std.ArrayList(ProcessEntry) = .empty;
    defer entries.deinit(allocator);

    cs.runtime.registry.mutex.lock();
    var it = cs.runtime.registry.map.iterator();
    while (it.next()) |kv| {
        const e = kv.value_ptr.*;
        if (filter_beam) |fb| {
            if (e.pid.beam != fb) continue;
        }
        if (filter_kind) |fk| {
            if (!std.mem.eql(u8, @tagName(e.kind), fk)) continue;
        }
        if (filter_state) |fs| {
            if (!std.mem.eql(u8, @tagName(e.state), fs)) continue;
        }
        try entries.append(allocator, e);
    }
    cs.runtime.registry.mutex.unlock();

    const now = std.time.milliTimestamp();

    // Build JSON response.
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"processes\":[", .{req_id});

    for (entries.items, 0..) |e, idx| {
        if (idx > 0) try w.writeByte(',');
        try w.print(
            "{{\"beam\":{d},\"pid\":{d},\"kind\":\"{s}\",\"state\":\"{s}\"," ++
            "\"policy\":\"{s}\",\"last_active_ms\":{d}}}",
            .{
                e.pid.beam,
                e.pid.proc,
                @tagName(e.kind),
                @tagName(e.state),
                @tagName(e.policy.activity_class),
                now - e.last_active_at,
            },
        );
    }

    try w.writeAll("]}}");
    try writeFrame(stream, buf.items);
}

// warden-mf3
fn writeTreeNode(entries: []const ProcessEntry, node: ProcessEntry, w: anytype) !void {
    try w.print(
        "{{\"beam\":{d},\"pid\":{d},\"kind\":\"{s}\",\"state\":\"{s}\",\"children\":[",
        .{ node.pid.beam, node.pid.proc, @tagName(node.kind), @tagName(node.state) },
    );
    var first = true;
    for (entries) |e| {
        const sv = e.supervisor orelse continue;
        if (sv.beam == node.pid.beam and sv.proc == node.pid.proc) {
            if (!first) try w.writeByte(',');
            try writeTreeNode(entries, e, w);
            first = false;
        }
    }
    try w.writeAll("]}");
}

// warden-mf3
fn handleTopologyGet(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    var filter_beam: ?u32 = null;
    if (payload_val) |pv| {
        switch (pv) {
            .object => |p| {
                if (p.get("beam")) |bv| {
                    filter_beam = switch (bv) {
                        .integer => |i| @intCast(i),
                        else => null,
                    };
                }
            },
            else => {},
        }
    }

    var entries: std.ArrayList(ProcessEntry) = .empty;
    defer entries.deinit(allocator);

    cs.runtime.registry.mutex.lock();
    var it = cs.runtime.registry.map.iterator();
    while (it.next()) |kv| {
        const e = kv.value_ptr.*;
        if (filter_beam) |fb| {
            if (e.pid.beam != fb) continue;
        }
        try entries.append(allocator, e);
    }
    cs.runtime.registry.mutex.unlock();

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    const w = buf.writer(allocator);

    try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"roots\":[", .{req_id});

    var first = true;
    for (entries.items) |e| {
        if (e.supervisor != null) continue; // only roots
        if (!first) try w.writeByte(',');
        try writeTreeNode(entries.items, e, w);
        first = false;
    }

    try w.writeAll("]}}");
    try writeFrame(stream, buf.items);
}

// warden-39v
fn handleConnection(cs: *ControlServer, stream: std.net.Stream) !void {
    const allocator = cs.allocator;

    const frame = try readFrame(allocator, stream);
    defer allocator.free(frame);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, frame, .{});
    defer parsed.deinit();

    const obj = switch (parsed.value) {
        .object => |o| o,
        else => return error.InvalidRequest,
    };

    const req_id = switch (obj.get("req_id") orelse return error.MissingReqId) {
        .string => |s| s,
        else => return error.InvalidReqId,
    };
    const action = switch (obj.get("action") orelse return error.MissingAction) {
        .string => |s| s,
        else => return error.InvalidAction,
    };

    const payload_val = obj.get("payload");

    if (std.mem.eql(u8, action, "beam.list")) {
        try handleBeamList(cs, allocator, req_id, stream);
    } else if (std.mem.eql(u8, action, "proc.list")) {
        // warden-di6
        try handleProcList(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "topology.get")) {
        // warden-mf3
        try handleTopologyGet(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "logs.stream")) {
        // warden-9jm
        try handleLogsStream(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "proc.control")) {
        // warden-aai
        try handleProcControl(cs, allocator, req_id, stream, payload_val);
    } else {
        const err_resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"unknown action: {s}\",\"payload\":null}}",
            .{ req_id, action },
        );
        defer allocator.free(err_resp);
        try writeFrame(stream, err_resp);
    }
}

// warden-39v
fn serverThread(cs: *ControlServer) void {
    while (!cs.stopping.load(.acquire)) {
        const conn = cs.server.accept() catch break;
        defer conn.stream.close();
        handleConnection(cs, conn.stream) catch {};
    }
}

// warden-9jm
fn writeJsonEscapedString(w: anytype, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            0x00...0x08, 0x0B, 0x0C, 0x0E...0x1F => try w.print("\\u{x:0>4}", .{c}),
            else => try w.writeByte(c),
        }
    }
    try w.writeByte('"');
}

fn extractTs(line: []const u8) f64 {
    const key = "\"ts\":";
    const start = std.mem.indexOf(u8, line, key) orelse return 0;
    const num_start = start + key.len;
    var end = num_start;
    while (end < line.len and line[end] != ',' and line[end] != '}') : (end += 1) {}
    return std.fmt.parseFloat(f64, line[num_start..end]) catch 0;
}

// warden-aai
fn handleProcControl(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const payload = payload_val orelse {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"missing payload\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(stream, resp);
    };

    const pid_str = switch (payload.object.get("pid") orelse .null) {
        .string => |s| s,
        else => {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"missing pid\",\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(stream, resp);
        },
    };

    const op_str = switch (payload.object.get("op") orelse .null) {
        .string => |s| s,
        else => {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"missing op\",\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(stream, resp);
        },
    };

    const slash = std.mem.indexOf(u8, pid_str, "/") orelse {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid format, expected beam/proc\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(stream, resp);
    };

    const beam_id = std.fmt.parseInt(u32, pid_str[0..slash], 10) catch {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid beam id\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(stream, resp);
    };
    const proc_id = std.fmt.parseInt(u64, pid_str[slash + 1 ..], 10) catch {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid proc id\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(stream, resp);
    };

    const target_state: types.ProcessState = if (std.mem.eql(u8, op_str, "pause"))
        .paused
    else if (std.mem.eql(u8, op_str, "resume"))
        .ready
    else {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"unknown op: {s}\",\"payload\":null}}",
            .{ req_id, op_str });
        defer allocator.free(resp);
        return writeFrame(stream, resp);
    };

    const pid = types.Pid{ .beam = beam_id, .proc = proc_id };

    const transition_err = cs.runtime.registry.transition(pid, target_state);

    if (transition_err) |_| {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"pid\":\"{s}\",\"op\":\"{s}\"}}}}",
            .{ req_id, pid_str, op_str });
        defer allocator.free(resp);
        try writeFrame(stream, resp);
    } else |err| {
        const msg = switch (err) {
            error.ProcessNotFound => "process not found",
            error.InvalidTransition => "invalid state transition",
            else => "internal error",
        };
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"{s}\",\"payload\":null}}",
            .{ req_id, msg });
        defer allocator.free(resp);
        try writeFrame(stream, resp);
    }
}

fn handleLogsStream(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const log_dir = cs.log_dir orelse {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"log_dir not configured\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(stream, err);
    };

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
    const slash = std.mem.indexOfScalar(u8, pid_str, '/') orelse {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid format, expected beam/proc\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(stream, err);
    };
    const beam_id = std.fmt.parseInt(u32, pid_str[0..slash], 10) catch {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(stream, err);
    };
    const proc_id = std.fmt.parseInt(u64, pid_str[slash + 1 ..], 10) catch {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(stream, err);
    };

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{d}-{d}.log", .{ log_dir, beam_id, proc_id });
    defer allocator.free(log_path);

    const file = std.fs.openFileAbsolute(log_path, .{}) catch {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"log file not found\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(stream, err);
    };
    defer file.close();

    const cutoff_ts: f64 = if (since_ms > 0)
        @as(f64, @floatFromInt(std.time.milliTimestamp() - @as(i64, @intCast(since_ms)))) / 1000.0
    else
        0.0;

    if (!follow) {
        // Read entire file, filter, return as JSON array.
        const content = try file.readToEndAlloc(allocator, 64 * 1024 * 1024);
        defer allocator.free(content);

        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(allocator);
        const w = buf.writer(allocator);

        try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"lines\":[", .{req_id});

        var count: usize = 0;
        var lines = std.mem.splitScalar(u8, content, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trimRight(u8, line, "\r");
            if (trimmed.len == 0) continue;
            if (cutoff_ts > 0 and extractTs(trimmed) < cutoff_ts) continue;
            if (grep_pattern) |pat| {
                if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
            }
            if (count > 0) try w.writeByte(',');
            try writeJsonEscapedString(w, trimmed);
            count += 1;
        }

        try w.print("],\"count\":{d}}}}}", .{count});
        return writeFrame(stream, buf.items);
    }

    // Follow mode: send streaming header, then tail the file.
    const header = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"streaming\":true}}}}",
        .{req_id});
    defer allocator.free(header);
    try writeFrame(stream, header);

    // Send existing content first.
    const content = try file.readToEndAlloc(allocator, 64 * 1024 * 1024);
    defer allocator.free(content);

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimRight(u8, line, "\r");
        if (trimmed.len == 0) continue;
        if (cutoff_ts > 0 and extractTs(trimmed) < cutoff_ts) continue;
        if (grep_pattern) |pat| {
            if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
        }
        var line_buf: std.ArrayList(u8) = .empty;
        defer line_buf.deinit(allocator);
        const lw = line_buf.writer(allocator);
        try lw.writeAll("{\"kind\":\"line\",\"data\":");
        try writeJsonEscapedString(lw, trimmed);
        try lw.writeByte('}');
        writeFrame(stream, line_buf.items) catch return;
    }

    // Poll for new content.
    var pos = try file.getPos();
    while (!cs.stopping.load(.acquire)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
        const new_content = blk: {
            try file.seekTo(pos);
            break :blk file.readToEndAlloc(allocator, 1 * 1024 * 1024) catch break;
        };
        defer allocator.free(new_content);
        if (new_content.len == 0) continue;
        pos += new_content.len;

        var new_lines = std.mem.splitScalar(u8, new_content, '\n');
        while (new_lines.next()) |line| {
            const trimmed = std.mem.trimRight(u8, line, "\r");
            if (trimmed.len == 0) continue;
            if (grep_pattern) |pat| {
                if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
            }
            var line_buf: std.ArrayList(u8) = .empty;
            defer line_buf.deinit(allocator);
            const lw = line_buf.writer(allocator);
            try lw.writeAll("{\"kind\":\"line\",\"data\":");
            try writeJsonEscapedString(lw, trimmed);
            try lw.writeByte('}');
            writeFrame(stream, line_buf.items) catch return;
        }
    }
}

// warden-39v
pub const ControlServer = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    socket_path: []u8,
    log_dir: ?[]u8,
    server: std.net.Server,
    thread: ?std.Thread,
    stopping: std.atomic.Value(bool),
    started_at: i64,

    /// Bind the control socket and prepare to accept connections.
    /// Call start() to begin serving.
    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, socket_path: []const u8) !ControlServer {
        const owned_path = try allocator.dupe(u8, socket_path);
        errdefer allocator.free(owned_path);
        std.fs.deleteFileAbsolute(socket_path) catch {};
        const addr = try std.net.Address.initUnix(socket_path);
        const server = try addr.listen(.{});
        return .{
            .allocator = allocator,
            .runtime = runtime,
            .socket_path = owned_path,
            .log_dir = null,
            .server = server,
            .thread = null,
            .stopping = std.atomic.Value(bool).init(false),
            .started_at = std.time.milliTimestamp(),
        };
    }

    // warden-9jm
    /// Configure the log directory so logs.stream can locate per-process log files.
    pub fn setLogDir(self: *ControlServer, log_dir: []const u8) !void {
        if (self.log_dir) |old| self.allocator.free(old);
        self.log_dir = try self.allocator.dupe(u8, log_dir);
    }

    /// Spawn the server thread. Must be called after init().
    pub fn start(self: *ControlServer) !void {
        self.thread = try std.Thread.spawn(.{}, serverThread, .{self});
    }

    /// Signal the server to stop, close the listen socket, and join the thread.
    pub fn stop(self: *ControlServer) void {
        self.stopping.store(true, .release);
        // Closing the listen socket unblocks accept() in the server thread.
        self.server.deinit();
        if (self.thread) |t| t.join();
        self.thread = null;
        std.fs.deleteFileAbsolute(self.socket_path) catch {};
        self.allocator.free(self.socket_path);
        if (self.log_dir) |d| self.allocator.free(d);
    }
};
