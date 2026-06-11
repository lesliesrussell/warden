// warden-r28
// beam.* RPC handlers (beam.create, beam.reaper, beam.list).

const std = @import("std");
const clock = @import("../clock.zig");
const beam_mod = @import("../beam.zig");
const bridge_mod = @import("../bridge.zig");
const transport = @import("transport.zig");
const control = @import("../control.zig");

const Runtime = beam_mod.Runtime;
const HandlerCtx = control.HandlerCtx;
const writeFrame = transport.writeFrame;

// warden-7oi
pub fn handleBeamCreate(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const r = h.r;
    var requested_id: ?u32 = null;
    if (payload_val) |pv| {
        if (pv == .object) {
            if (pv.object.get("beam")) |bv| {
                if (bv == .integer) requested_id = @intCast(bv.integer);
            }
        }
    }

    if (requested_id) |id| {
        if (cs.runtimes.get(id) != null) {
            const payload = try std.fmt.allocPrint(allocator, "{{\"beam_id\":{d}}}", .{id});
            defer allocator.free(payload);
            return r.ok(payload);
        }
    }

    const new_id = requested_id orelse cs.next_beam_id.fetchAdd(1, .monotonic);

    const rt = try Runtime.init(cs.allocator, new_id);
    errdefer rt.destroy();
    try rt.start(2);

    const sup = try cs.allocator.create(bridge_mod.BridgeSupervisor);
    errdefer cs.allocator.destroy(sup);
    sup.* = bridge_mod.BridgeSupervisor.init(cs.allocator, rt);
    try sup.startReaper(); // warden-dmg

    try cs.runtimes.put(new_id, rt);
    try cs.supervisors.put(new_id, sup);

    const payload = try std.fmt.allocPrint(allocator, "{{\"beam_id\":{d}}}", .{new_id});
    defer allocator.free(payload);
    try r.ok(payload);
}

// warden-dmg
pub fn handleBeamReaper(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const r = h.r;
    const pv = payload_val orelse return r.err("missing payload");
    if (pv != .object) return r.err("payload must be object");
    const obj = pv.object;

    const beam_id: u32 = if (obj.get("beam")) |bv|
        if (bv == .integer) @intCast(bv.integer) else cs.runtime.beam_id
    else
        cs.runtime.beam_id;

    const interval_val = obj.get("interval_ms") orelse
        return r.err("missing interval_ms");
    if (interval_val != .integer or interval_val.integer < 0)
        return r.err("interval_ms must be a non-negative integer");

    const sup = cs.supervisors.get(beam_id) orelse
        return r.err("unknown beam");

    const applied = sup.renice(@intCast(interval_val.integer));

    const payload = try std.fmt.allocPrint(allocator, "{{\"interval_ms\":{d}}}", .{applied});
    defer allocator.free(payload);
    try r.ok(payload);
}

pub fn handleBeamList(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const stream = h.stream;
    const r = h.r;
    const uptime_ms = clock.nowMs() - cs.started_at;

    // warden-f9s: list every beam (each with its own registry count), not just
    // the primary. Collect and sort beam ids for stable output.
    var ids: std.ArrayList(u32) = .empty;
    defer ids.deinit(allocator);
    var rit = cs.runtimes.iterator();
    while (rit.next()) |kv| try ids.append(allocator, kv.key_ptr.*);
    std.mem.sort(u32, ids.items, {}, std.sort.asc(u32));

    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try r.okPrefix(w);
    try w.writeAll("{\"beams\":[");
    for (ids.items, 0..) |bid, idx| {
        if (idx > 0) try w.writeByte(',');
        const brt = cs.runtimes.get(bid).?;
        const proc_count = brt.registry.count(); // warden-f19
        try w.print(
            "{{\"beam_id\":{d},\"version\":\"0.1.0\",\"uptime_ms\":{d},\"process_count\":{d}}}",
            .{ bid, uptime_ms, proc_count });
    }
    try w.writeAll("]}");
    try r.okSuffix(w);
    try writeFrame(cs.runtime.io, stream, buf.writer.buffered());
}

