// warden-mf3

const std = @import("std");
const ControlClient = @import("../client.zig").ControlClient;

pub const Filter = struct {
    beam: ?u32 = null,
};

// warden-mf3
pub fn run(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    filter: Filter,
    json_output: bool,
) !void {
    var payload_buf: std.ArrayList(u8) = .empty;
    defer payload_buf.deinit(allocator);
    const pw = payload_buf.writer(allocator);

    try pw.writeByte('{');
    if (filter.beam) |b| try pw.print("\"beam\":{d}", .{b});
    try pw.writeByte('}');

    const resp_bytes = try client.requestWithPayload("topology.get", payload_buf.items);
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
            try stderr.writeAll("error: topology.get failed: ");
            try stderr.writeAll(msg);
            try stderr.writeAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }

    const roots = obj.get("payload").?.object.get("roots").?.array;

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(allocator);
    const w = out.writer(allocator);

    for (roots.items) |root| {
        try renderRoot(allocator, root, w);
    }

    try stdout.writeAll(out.items);
}

fn renderRoot(allocator: std.mem.Allocator, node: std.json.Value, w: anytype) !void {
    const kind = node.object.get("kind").?.string;
    const beam = node.object.get("beam").?.integer;
    const pid = node.object.get("pid").?.integer;
    const state = node.object.get("state").?.string;
    try w.print("{s} [pid={d}/{d}, {s}]\n", .{ kind, beam, pid, state });

    const children = node.object.get("children").?.array;
    for (children.items, 0..) |child, i| {
        const is_last = i == children.items.len - 1;
        try renderChild(allocator, child, "", is_last, w);
    }
}

fn renderChild(
    allocator: std.mem.Allocator,
    node: std.json.Value,
    prefix: []const u8,
    is_last: bool,
    w: anytype,
) !void {
    const connector = if (is_last) "└── " else "├── ";
    const kind = node.object.get("kind").?.string;
    const beam = node.object.get("beam").?.integer;
    const pid = node.object.get("pid").?.integer;
    const state = node.object.get("state").?.string;

    try w.print("{s}{s}{s} [pid={d}/{d}, {s}]\n", .{ prefix, connector, kind, beam, pid, state });

    const children = node.object.get("children").?.array;
    const child_prefix = if (is_last)
        try std.mem.concat(allocator, u8, &.{ prefix, "    " })
    else
        try std.mem.concat(allocator, u8, &.{ prefix, "│   " });
    defer allocator.free(child_prefix);

    for (children.items, 0..) |child, i| {
        const child_is_last = i == children.items.len - 1;
        try renderChild(allocator, child, child_prefix, child_is_last, w);
    }
}
