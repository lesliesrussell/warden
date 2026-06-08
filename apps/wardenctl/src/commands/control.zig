// warden-aai

const std = @import("std");
const term = @import("../term.zig");
const ControlClient = @import("../client.zig").ControlClient;

// warden-aai
pub fn run(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    pid: []const u8,
    op: []const u8,
    json_output: bool,
) !void {
    const payload = try std.fmt.allocPrint(
        allocator,
        "{{\"pid\":\"{s}\",\"op\":\"{s}\"}}",
        .{ pid, op },
    );
    defer allocator.free(payload);

    try runWithPayload(allocator, client, op, pid, payload, json_output);
}

// warden-h0j
pub const PromoteOpts = struct {
    class: []const u8 = "elevated",
    ttl_ms: ?u64 = null,
    reason: ?[]const u8 = null,
};

pub fn runPromote(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    pid: []const u8,
    opts: PromoteOpts,
    json_output: bool,
) !void {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;

    try w.print("{{\"pid\":\"{s}\",\"op\":\"promote\",\"class\":\"{s}\"", .{ pid, opts.class });
    if (opts.ttl_ms) |t| try w.print(",\"ttl_ms\":{d}", .{t});
    if (opts.reason) |r| {
        try w.writeAll(",\"reason\":\"");
        for (r) |c| {
            if (c == '"' or c == '\\') try w.writeByte('\\');
            try w.writeByte(c);
        }
        try w.writeByte('"');
    }
    try w.writeByte('}');

    try runWithPayload(allocator, client, "promote", pid, buf.writer.buffered(), json_output);
}

pub fn runWithReason(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    op: []const u8,
    pid: []const u8,
    reason: ?[]const u8,
    json_output: bool,
) !void {
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;

    try w.print("{{\"pid\":\"{s}\",\"op\":\"{s}\"", .{ pid, op });
    if (reason) |r| {
        try w.writeAll(",\"reason\":\"");
        for (r) |c| {
            if (c == '"' or c == '\\') try w.writeByte('\\');
            try w.writeByte(c);
        }
        try w.writeByte('"');
    }
    try w.writeByte('}');

    try runWithPayload(allocator, client, op, pid, buf.writer.buffered(), json_output);
}

fn runWithPayload(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    op: []const u8,
    pid: []const u8,
    payload: []const u8,
    json_output: bool,
) !void {
    const resp_bytes = try client.requestWithPayload("proc.control", payload);
    defer allocator.free(resp_bytes);


    if (json_output) {
        term.outAll(resp_bytes);
        term.outAll("\n");
        return;
    }

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    switch (obj.get("ok") orelse return error.InvalidResponse) {
        .bool => |b| if (!b) {
            const msg = switch (obj.get("error") orelse .null) {
                .string => |s| s,
                else => "unknown error",
            };
            term.errAll("error: proc.control failed: ");
            term.errAll(msg);
            term.errAll("\n");
            std.process.exit(1);
        },
        else => return error.InvalidResponse,
    }

    const line = try std.fmt.allocPrint(allocator, "{s} {s}\n", .{ op, pid });
    defer allocator.free(line);
    term.outAll(line);
}
