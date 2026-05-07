// warden-aai

const std = @import("std");
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
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    const w = buf.writer(allocator);

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

    try runWithPayload(allocator, client, "promote", pid, buf.items, json_output);
}

pub fn runWithReason(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    op: []const u8,
    pid: []const u8,
    reason: ?[]const u8,
    json_output: bool,
) !void {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);
    const w = buf.writer(allocator);

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

    try runWithPayload(allocator, client, op, pid, buf.items, json_output);
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

    const stdout = std.fs.File.stdout();

    if (json_output) {
        try stdout.writeAll(resp_bytes);
        try stdout.writeAll("\n");
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
            const stderr = std.fs.File.stderr();
            try stderr.writeAll("error: proc.control failed: ");
            try stderr.writeAll(msg);
            try stderr.writeAll("\n");
            std.process.exit(1);
        },
        else => return error.InvalidResponse,
    }

    const line = try std.fmt.allocPrint(allocator, "{s} {s}\n", .{ op, pid });
    defer allocator.free(line);
    try stdout.writeAll(line);
}
