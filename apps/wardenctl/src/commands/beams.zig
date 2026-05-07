// warden-39v

const std = @import("std");
const ControlClient = @import("../client.zig").ControlClient;

// warden-39v
pub fn run(allocator: std.mem.Allocator, client: *ControlClient, json_output: bool) !void {
    const resp_bytes = try client.request("beam.list");
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
            try stderr.writeAll("error: beam.list failed: ");
            try stderr.writeAll(msg);
            try stderr.writeAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }

    const beams = obj.get("payload").?.object.get("beams").?.array;

    const header = try std.fmt.allocPrint(allocator, "{s:<6} {s:<9} {s:<9} {s}\n",
        .{ "BEAM", "VERSION", "UPTIME", "PROCS" });
    defer allocator.free(header);
    try stdout.writeAll(header);

    for (beams.items) |beam_val| {
        const b = beam_val.object;
        const beam_id = b.get("beam_id").?.integer;
        const version = b.get("version").?.string;
        const uptime_ms: u64 = @intCast(b.get("uptime_ms").?.integer);
        const proc_count = b.get("process_count").?.integer;

        const uptime_str = try formatUptime(allocator, uptime_ms);
        defer allocator.free(uptime_str);

        const row = try std.fmt.allocPrint(allocator, "{d:<6} {s:<9} {s:<9} {d}\n",
            .{ beam_id, version, uptime_str, proc_count });
        defer allocator.free(row);
        try stdout.writeAll(row);
    }
}

fn formatUptime(allocator: std.mem.Allocator, ms: u64) ![]u8 {
    const secs = ms / 1000;
    if (secs < 60) return std.fmt.allocPrint(allocator, "{d}s", .{secs});
    const mins = secs / 60;
    const rem_secs = secs % 60;
    if (mins < 60) return std.fmt.allocPrint(allocator, "{d}m{d}s", .{ mins, rem_secs });
    const hours = mins / 60;
    const rem_mins = mins % 60;
    return std.fmt.allocPrint(allocator, "{d}h{d}m", .{ hours, rem_mins });
}
