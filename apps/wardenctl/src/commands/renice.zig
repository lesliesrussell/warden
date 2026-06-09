// warden-dmg

const std = @import("std");
const term = @import("../term.zig");
const ControlClient = @import("../client.zig").ControlClient;

// warden-dmg: send beam.reaper to adjust a beam's reaper poll interval.
pub fn run(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    beam: u32,
    interval_ms: u64,
    json_output: bool,
) !void {
    const payload = try std.fmt.allocPrint(
        allocator, "{{\"beam\":{d},\"interval_ms\":{d}}}", .{ beam, interval_ms });
    defer allocator.free(payload);

    const resp_bytes = try client.requestWithPayload("beam.reaper", payload);
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
            term.errAll("error: renice failed: ");
            term.errAll(msg);
            term.errAll("\n");
            return error.RequestFailed;
        },
        else => return error.InvalidResponse,
    }
    const applied = obj.get("payload").?.object.get("interval_ms").?.integer;
    const line = try std.fmt.allocPrint(allocator, "beam {d}: reaper interval = {d}ms\n", .{ beam, applied });
    defer allocator.free(line);
    term.outAll(line);
}
