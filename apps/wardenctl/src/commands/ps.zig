// warden-di6

const std = @import("std");
const ControlClient = @import("../client.zig").ControlClient;

pub const Filter = struct {
    beam: ?u32 = null,
    kind: ?[]const u8 = null,
    state: ?[]const u8 = null,
};

// warden-di6
pub fn run(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    filter: Filter,
    json_output: bool,
) !void {
    // Build payload JSON from active filters.
    var payload_buf: std.ArrayList(u8) = .empty;
    defer payload_buf.deinit(allocator);
    const pw = payload_buf.writer(allocator);

    try pw.writeByte('{');
    var sep = false;
    if (filter.beam) |b| {
        try pw.print("\"beam\":{d}", .{b});
        sep = true;
    }
    if (filter.kind) |k| {
        if (sep) try pw.writeByte(',');
        try pw.print("\"kind\":\"{s}\"", .{k});
        sep = true;
    }
    if (filter.state) |s| {
        if (sep) try pw.writeByte(',');
        try pw.print("\"state\":\"{s}\"", .{s});
    }
    try pw.writeByte('}');

    const resp_bytes = try client.requestWithPayload("proc.list", payload_buf.items);
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
            try stderr.writeAll("error: proc.list failed: ");
            try stderr.writeAll(msg);
            try stderr.writeAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }

    const procs = obj.get("payload").?.object.get("processes").?.array;

    const header = try std.fmt.allocPrint(allocator,
        "{s:<6} {s:<6} {s:<20} {s:<12} {s:<10} {s}\n",
        .{ "BEAM", "PID", "KIND", "STATE", "POLICY", "LAST ACTIVE" });
    defer allocator.free(header);
    try stdout.writeAll(header);

    for (procs.items) |proc_val| {
        const p = proc_val.object;
        const beam_id = p.get("beam").?.integer;
        const pid = p.get("pid").?.integer;
        const kind = p.get("kind").?.string;
        const state = p.get("state").?.string;
        const policy = p.get("policy").?.string;
        const last_ms: u64 = @intCast(p.get("last_active_ms").?.integer);

        const last_str = try formatAge(allocator, last_ms);
        defer allocator.free(last_str);

        const row = try std.fmt.allocPrint(allocator,
            "{d:<6} {d:<6} {s:<20} {s:<12} {s:<10} {s}\n",
            .{ beam_id, pid, kind, state, policy, last_str });
        defer allocator.free(row);
        try stdout.writeAll(row);
    }
}

fn formatAge(allocator: std.mem.Allocator, ms: u64) ![]u8 {
    if (ms < 1000) return std.fmt.allocPrint(allocator, "{d}ms ago", .{ms});
    const secs = ms / 1000;
    if (secs < 60) return std.fmt.allocPrint(allocator, "{d}s ago", .{secs});
    const mins = secs / 60;
    return std.fmt.allocPrint(allocator, "{d}m ago", .{mins});
}
