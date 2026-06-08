// warden-mf3

const std = @import("std");
const term = @import("../term.zig");
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
    beam_label: ?u32,
) !void {
    var payload_buf: std.Io.Writer.Allocating = .init(allocator);
    defer payload_buf.deinit();
    const pw = &payload_buf.writer;

    try pw.writeByte('{');
    if (filter.beam) |b| try pw.print("\"beam\":{d}", .{b});
    try pw.writeByte('}');

    const resp_bytes = try client.requestWithPayload("topology.get", payload_buf.writer.buffered());
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
            term.errAll("error: topology.get failed: ");
            term.errAll(msg);
            term.errAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }

    const roots = obj.get("payload").?.object.get("roots").?.array;

    // warden-xh7: print beam label when fan-out across multiple beams
    if (beam_label) |bid| {
        const lbl = try std.fmt.allocPrint(allocator, "beam {d}:\n", .{bid});
        defer allocator.free(lbl);
        term.outAll(lbl);
    }

    if (roots.items.len == 0) {
        term.outAll("  (no processes)\n\n");
        return;
    }

    var out: std.Io.Writer.Allocating = .init(allocator);
    defer out.deinit();
    const w = &out.writer;

    for (roots.items) |root| {
        try renderRoot(allocator, root, w);
    }
    try w.writeByte('\n');

    term.outAll(out.writer.buffered());
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
