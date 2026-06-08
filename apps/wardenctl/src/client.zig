// warden-39v
//
// RPC client for the Warden control server.
// Speaks the same length-prefixed JSON framing as control.zig and bridge.zig.
// Does NOT import the warden core module — only uses stdlib + this file.

const std = @import("std");
const term = @import("term.zig");

// warden-3qh: Zig 0.16 — sockets are std.Io.net; frame I/O via buffered
// Stream.Writer/Reader over the global executor.
fn writeFrame(stream: std.Io.net.Stream, json: []const u8) !void {
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(json.len), .big);
    var wbuf: [4096]u8 = undefined;
    var w = stream.writer(term.gio(), &wbuf);
    try w.interface.writeAll(&hdr);
    try w.interface.writeAll(json);
    try w.interface.flush();
}

/// Read one length-prefixed JSON frame from a persistent reader. Caller owns
/// the returned slice. The reader must persist across frames (streaming logs)
/// so bytes already buffered off the socket are not lost.
fn readFrame(allocator: std.mem.Allocator, r: *std.Io.Reader) ![]u8 {
    var hdr: [4]u8 = undefined;
    r.readSliceAll(&hdr) catch return error.ConnectionClosed;
    const length = std.mem.readInt(u32, &hdr, .big);
    if (length == 0) return error.EmptyFrame;
    const buf = try allocator.alloc(u8, length);
    errdefer allocator.free(buf);
    r.readSliceAll(buf) catch return error.ConnectionClosed;
    return buf;
}

// warden-39v
pub const ControlClient = struct {
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    req_counter: u32,
    // warden-3qh: lazily-initialized persistent reader (points into rbuf, so it
    // is created only once `self` is at its final stable address).
    rbuf: [4096]u8 = undefined,
    reader: ?std.Io.net.Stream.Reader = null,

    pub fn connect(allocator: std.mem.Allocator, socket_path: []const u8) !ControlClient {
        var ua = std.Io.net.UnixAddress.init(socket_path) catch return error.RuntimeNotListening;
        const stream = ua.connect(term.gio()) catch |err| switch (err) {
            error.FileNotFound => {
                term.errAll("error: no Warden runtime listening at '");
                term.errAll(socket_path);
                term.errAll("'\n  Start the runtime with WARDEN_CTRL_SOCKET set, or use --socket.\n");
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
        self.stream.close(term.gio());
    }

    fn rd(self: *ControlClient) *std.Io.Reader {
        if (self.reader == null) self.reader = self.stream.reader(term.gio(), &self.rbuf);
        return &self.reader.?.interface;
    }

    /// Send an action request and return the raw JSON response. Caller owns the slice.
    pub fn request(self: *ControlClient, action: []const u8) ![]u8 {
        return self.requestWithPayload(action, "{}");
    }

    // warden-di6
    /// Send a request with an explicit JSON payload string. Caller owns the returned slice.
    pub fn requestWithPayload(self: *ControlClient, action: []const u8, payload_json: []const u8) ![]u8 {
        try self.sendRequest(action, payload_json);
        return self.recvFrame();
    }

    // warden-9jm
    /// Send a request frame without reading the response (for streaming responses).
    pub fn sendRequest(self: *ControlClient, action: []const u8, payload_json: []const u8) !void {
        self.req_counter += 1;
        const req_json = try std.fmt.allocPrint(
            self.allocator,
            "{{\"req_id\":\"{d}\",\"action\":\"{s}\",\"payload\":{s}}}",
            .{ self.req_counter, action, payload_json },
        );
        defer self.allocator.free(req_json);
        try writeFrame(self.stream, req_json);
    }

    /// Read one response frame. Caller owns the returned slice.
    pub fn recvFrame(self: *ControlClient) ![]u8 {
        return readFrame(self.allocator, self.rd());
    }
};
