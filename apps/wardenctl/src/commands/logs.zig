// warden-9jm

const std = @import("std");
const ControlClient = @import("../client.zig").ControlClient;

pub const Options = struct {
    pid: []const u8,        // "beam/proc"
    since_ms: u64 = 0,
    grep: ?[]const u8 = null,
    follow: bool = false,
};

// warden-9jm
pub fn run(
    allocator: std.mem.Allocator,
    client: *ControlClient,
    opts: Options,
    json_output: bool,
) !void {
    var payload_buf: std.ArrayList(u8) = .empty;
    defer payload_buf.deinit(allocator);
    const pw = payload_buf.writer(allocator);

    try pw.print("{{\"pid\":\"{s}\"", .{opts.pid});
    if (opts.since_ms > 0) try pw.print(",\"since_ms\":{d}", .{opts.since_ms});
    if (opts.grep) |g| {
        try pw.writeAll(",\"grep\":\"");
        for (g) |c| {
            if (c == '"' or c == '\\') try pw.writeByte('\\');
            try pw.writeByte(c);
        }
        try pw.writeByte('"');
    }
    if (opts.follow) try pw.writeAll(",\"follow\":true");
    try pw.writeByte('}');

    const stdout = std.fs.File.stdout();

    if (!opts.follow) {
        // Single-shot: one request, one response.
        const resp_bytes = try client.requestWithPayload("logs.stream", payload_buf.items);
        defer allocator.free(resp_bytes);

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
                try stderr.writeAll("error: logs.stream failed: ");
                try stderr.writeAll(msg);
                try stderr.writeAll("\n");
                return error.RequestFailed;
            },
            else => return error.InvalidResponse,
        }

        const lines = obj.get("payload").?.object.get("lines").?.array;
        for (lines.items) |line_val| {
            const raw = line_val.string;
            try printLine(allocator, stdout, raw, opts.pid);
        }
        return;
    }

    // Follow mode: streaming frames.
    try client.sendRequest("logs.stream", payload_buf.items);

    // Read initial streaming confirmation frame.
    const init_bytes = try client.recvFrame();
    defer allocator.free(init_bytes);

    const init_parsed = try std.json.parseFromSlice(std.json.Value, allocator, init_bytes, .{});
    defer init_parsed.deinit();

    switch (init_parsed.value.object.get("ok") orelse return error.InvalidResponse) {
        .bool => |b| if (!b) return error.RequestFailed,
        else => return error.InvalidResponse,
    }

    // Read line frames until connection closes.
    while (true) {
        const frame = client.recvFrame() catch break;
        defer allocator.free(frame);

        const parsed = std.json.parseFromSlice(std.json.Value, allocator, frame, .{}) catch break;
        defer parsed.deinit();

        const data = switch (parsed.value.object.get("data") orelse break) {
            .string => |s| s,
            else => break,
        };
        if (json_output) {
            try stdout.writeAll(data);
            try stdout.writeAll("\n");
        } else {
            try printLine(allocator, stdout, data, opts.pid);
        }
    }
}

fn printLine(allocator: std.mem.Allocator, stdout: std.fs.File, raw: []const u8, pid: []const u8) !void {
    // Parse a few key fields for human-readable output.
    // Fall back to raw if parsing fails.
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, raw, .{}) catch {
        try stdout.writeAll(raw);
        try stdout.writeAll("\n");
        return;
    };
    defer parsed.deinit();

    const obj = parsed.value.object;

    const ts = switch (obj.get("ts") orelse .null) {
        .float => |f| f,
        .integer => |i| @as(f64, @floatFromInt(i)),
        else => 0.0,
    };
    const event = switch (obj.get("event") orelse .null) {
        .string => |s| s,
        else => "?",
    };

    // Print msg, name, or tool field depending on event type.
    const detail: []const u8 = blk: {
        if (obj.get("msg")) |mv| {
            if (mv == .string) break :blk mv.string;
        }
        if (obj.get("name")) |nv| {
            if (nv == .string) break :blk nv.string;
        }
        if (obj.get("tool")) |tv| {
            if (tv == .string) break :blk tv.string;
        }
        break :blk "";
    };

    const line = try std.fmt.allocPrint(allocator,
        "{d:.3}  pid={s}  event={s}  {s}\n",
        .{ ts, pid, event, detail });
    defer allocator.free(line);
    try stdout.writeAll(line);
}
