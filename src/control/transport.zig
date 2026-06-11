// warden-7zc
//
// Control-plane transport primitives, extracted from control.zig: the
// length-prefixed JSON frame I/O, the response-envelope Responder, JSON string
// escaping, and the "beam/proc" pid parser. control.zig re-exports writeFrame
// and readFrame so existing importers (main.zig, control_test.zig,
// live_demo_test.zig) keep compiling unchanged.

const std = @import("std");
const types = @import("../types.zig");

// warden-39v
// warden-3qh: Zig 0.16 — std.Io.net.Stream has no writeAll/readAtLeast; frame I/O
// goes through buffered Stream.Writer/Reader, which take the runtime io.
/// Write a length-prefixed JSON frame to a stream.
pub fn writeFrame(io: std.Io, stream: std.Io.net.Stream, json: []const u8) !void {
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(json.len), .big);
    var wbuf: [4096]u8 = undefined;
    var w = stream.writer(io, &wbuf);
    try w.interface.writeAll(&hdr);
    try w.interface.writeAll(json);
    try w.interface.flush();
}

// warden-39v
/// Read a length-prefixed JSON frame. Caller owns the returned slice.
pub fn readFrame(io: std.Io, allocator: std.mem.Allocator, stream: std.Io.net.Stream) ![]u8 {
    var rbuf: [4096]u8 = undefined;
    var r = stream.reader(io, &rbuf);
    var hdr: [4]u8 = undefined;
    r.interface.readSliceAll(&hdr) catch return error.ConnectionClosed;
    const length = std.mem.readInt(u32, &hdr, .big);
    if (length == 0) return error.EmptyFrame;
    const buf = try allocator.alloc(u8, length);
    errdefer allocator.free(buf);
    r.interface.readSliceAll(buf) catch return error.ConnectionClosed;
    return buf;
}

// warden-9jm
/// Write `s` as a JSON string literal (surrounding quotes included), escaping
/// quotes, backslashes, and control characters.
pub fn writeJsonEscapedString(w: anytype, s: []const u8) !void {
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

// warden-y3s
// Responder centralizes the control-plane response envelope so handlers stop
// hand-rolling `{"req_id":..,"ok":..,"error":..,"payload":..}` at every call
// site.
// warden-veb: req_id and error messages are JSON-escaped here — the single spot
// the whole control plane funnels through — so a req_id/error/action containing
// a quote or control char can no longer emit malformed JSON. Payload fragments
// are pre-rendered valid JSON by callers and pass through verbatim.
pub const Responder = struct {
    io: std.Io,
    allocator: std.mem.Allocator,
    stream: std.Io.net.Stream,
    req_id: []const u8,

    /// Frame an error response: ok=false, the given message, null payload.
    pub fn err(self: Responder, msg: []const u8) !void {
        var buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer buf.deinit();
        const w = &buf.writer;
        try w.writeAll("{\"req_id\":");
        try writeJsonEscapedString(w, self.req_id);
        try w.writeAll(",\"ok\":false,\"error\":");
        try writeJsonEscapedString(w, msg);
        try w.writeAll(",\"payload\":null}");
        try writeFrame(self.io, self.stream, buf.writer.buffered());
    }

    /// Frame a success response wrapping an already-rendered payload fragment.
    /// Pass "null" for an empty payload, or use okEmpty().
    pub fn ok(self: Responder, payload_json: []const u8) !void {
        var buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer buf.deinit();
        const w = &buf.writer;
        try self.okPrefix(w);
        try w.writeAll(payload_json);
        try self.okSuffix(w);
        try writeFrame(self.io, self.stream, buf.writer.buffered());
    }

    /// Frame a success response with a null payload.
    pub fn okEmpty(self: Responder) !void {
        try self.ok("null");
    }

    /// Write the success-envelope prefix up to `"payload":` into a caller-owned
    /// writer, for large/streamed payloads that should not be copied through an
    /// intermediate buffer. Caller appends the payload value, then okSuffix(),
    /// then frames the buffer once via writeFrame.
    pub fn okPrefix(self: Responder, w: anytype) !void {
        try w.writeAll("{\"req_id\":");
        try writeJsonEscapedString(w, self.req_id);
        try w.writeAll(",\"ok\":true,\"error\":null,\"payload\":");
    }

    pub fn okSuffix(_: Responder, w: anytype) !void {
        try w.writeByte('}');
    }
};

// warden-y3s
// parseBeamProc parses a "beam/proc" pid string into its parts, distinguishing
// the three failure modes so each caller can map them to its own error text
// (proc.control and logs.stream historically differ on the beam/proc messages).
pub const BeamProc = union(enum) {
    ok: types.Pid,
    no_slash,
    bad_beam,
    bad_proc,
};

pub fn parseBeamProc(s: []const u8) BeamProc {
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse return .no_slash;
    const beam_id = std.fmt.parseInt(u32, s[0..slash], 10) catch return .bad_beam;
    const proc_id = std.fmt.parseInt(u64, s[slash + 1 ..], 10) catch return .bad_proc;
    return .{ .ok = .{ .beam = beam_id, .proc = proc_id } };
}
