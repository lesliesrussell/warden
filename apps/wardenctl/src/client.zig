// warden-39v
//
// RPC client for the Warden control server.
// Speaks the same length-prefixed JSON framing as control.zig and bridge.zig.
// Does NOT import the warden core module — only uses stdlib + this file.

const std = @import("std");

fn writeFrame(stream: std.net.Stream, json: []const u8) !void {
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(json.len), .big);
    try stream.writeAll(&hdr);
    try stream.writeAll(json);
}

fn readFrame(allocator: std.mem.Allocator, stream: std.net.Stream) ![]u8 {
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
pub const ControlClient = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    req_counter: u32,

    pub fn connect(allocator: std.mem.Allocator, socket_path: []const u8) !ControlClient {
        const stream = std.net.connectUnixSocket(socket_path) catch |err| switch (err) {
            error.FileNotFound => {
                const stderr = std.fs.File.stderr();
                stderr.writeAll("error: no Warden runtime listening at '") catch {};
                stderr.writeAll(socket_path) catch {};
                stderr.writeAll("'\n  Start the runtime with WARDEN_CTRL_SOCKET set, or use --socket.\n") catch {};
                return error.RuntimeNotListening;
            },
            else => return err,
        };
        return .{
            .allocator = allocator,
            .stream = stream,
            .req_counter = 0,
        };
    }

    pub fn close(self: *ControlClient) void {
        self.stream.close();
    }

    /// Send an action request and return the raw JSON response. Caller owns the slice.
    pub fn request(self: *ControlClient, action: []const u8) ![]u8 {
        self.req_counter += 1;
        const req_json = try std.fmt.allocPrint(
            self.allocator,
            "{{\"req_id\":\"{d}\",\"action\":\"{s}\",\"payload\":{{}}}}",
            .{ self.req_counter, action },
        );
        defer self.allocator.free(req_json);
        try writeFrame(self.stream, req_json);
        return readFrame(self.allocator, self.stream);
    }
};
