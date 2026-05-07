// warden-39v
//
// Control server — accepts RPC requests over a Unix domain socket and routes
// them to the runtime's registry, policy engine, and logger.
//
// Transport: length-prefixed JSON frames (4-byte big-endian u32 length).
// Activation: call ControlServer.init() then .start().

const std = @import("std");
const beam_mod = @import("beam.zig");

const Runtime = beam_mod.Runtime;

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

    if (std.mem.eql(u8, action, "beam.list")) {
        try handleBeamList(cs, allocator, req_id, stream);
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

// warden-39v
pub const ControlServer = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    socket_path: []u8,
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
            .server = server,
            .thread = null,
            .stopping = std.atomic.Value(bool).init(false),
            .started_at = std.time.milliTimestamp(),
        };
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
    }
};
