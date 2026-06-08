// warden-39v

const std = @import("std");
const term = @import("../term.zig");
const ControlClient = @import("../client.zig").ControlClient;

// warden-39v
pub fn run(allocator: std.mem.Allocator, client: *ControlClient, json_output: bool, show_header: bool) !void {
    const resp_bytes = try client.request("beam.list");
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
            term.errAll("error: beam.list failed: ");
            term.errAll(msg);
            term.errAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }

    const beams = obj.get("payload").?.object.get("beams").?.array;

    // warden-xh7
    if (show_header) {
        const header = try std.fmt.allocPrint(allocator, "{s:<6} {s:<9} {s:<9} {s}\n",
            .{ "BEAM", "VERSION", "UPTIME", "PROCS" });
        defer allocator.free(header);
        term.outAll(header);
    }

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
        term.outAll(row);
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
