// warden-r28
// proc.control RPC handler (pause/resume/kill/quarantine/promote).

const std = @import("std");
const types = @import("../types.zig");
const transport = @import("transport.zig");
const control = @import("../control.zig");

const HandlerCtx = control.HandlerCtx;
const parseBeamProc = transport.parseBeamProc;

// warden-aai
pub fn handleProcControl(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const r = h.r;

    const payload = payload_val orelse return r.err("missing payload");
    // warden-h6u: guard before payload.object accesses below, matching
    // proc.spawn/proc.send — a non-object payload must not panic the server thread.
    if (payload != .object) return r.err("payload must be object");

    const pid_str = switch (payload.object.get("pid") orelse .null) {
        .string => |s| s,
        else => return r.err("missing pid"),
    };

    const op_str = switch (payload.object.get("op") orelse .null) {
        .string => |s| s,
        else => return r.err("missing op"),
    };

    // warden-y3s: shared beam/proc parse; per-failure messages preserved (warden-aai).
    const pid = switch (parseBeamProc(pid_str)) {
        .ok => |p| p,
        .no_slash => return r.err("invalid pid format, expected beam/proc"),
        .bad_beam => return r.err("invalid beam id"),
        .bad_proc => return r.err("invalid proc id"),
    };
    const beam_id = pid.beam;

    // warden-0uj: resolve the target beam's registry. proc.control previously
    // operated on cs.runtime (the primary beam) regardless of the pid's beam,
    // so a foreign-beam pid hit the wrong process or none at all.
    const target_rt = cs.runtimes.get(beam_id) orelse return r.err("unknown beam");
    const reg = &target_rt.registry;

    // warden-h0j
    if (std.mem.eql(u8, op_str, "pause") or std.mem.eql(u8, op_str, "resume")) {
        const target_state: types.ProcessState = if (std.mem.eql(u8, op_str, "pause")) .paused else .ready;
        if (reg.transition(pid, target_state)) |_| {
            const out = try std.fmt.allocPrint(allocator, "{{\"pid\":\"{s}\",\"op\":\"{s}\"}}", .{ pid_str, op_str });
            defer allocator.free(out);
            try r.ok(out);
        } else |err| {
            try r.err(switch (err) {
                error.ProcessNotFound => "process not found",
                error.InvalidTransition => "invalid state transition",
                else => "internal error",
            });
        }
    } else if (std.mem.eql(u8, op_str, "kill")) {
        if (reg.transition(pid, .exiting)) |_| {
            const out = try std.fmt.allocPrint(allocator, "{{\"pid\":\"{s}\",\"op\":\"kill\"}}", .{pid_str});
            defer allocator.free(out);
            try r.ok(out);
        } else |err| {
            try r.err(switch (err) {
                error.ProcessNotFound => "process not found",
                error.InvalidTransition => "invalid state transition",
                else => "internal error",
            });
        }
    } else if (std.mem.eql(u8, op_str, "quarantine")) {
        // warden-qj2: route through the policy engine so quarantine emits a
        // structured policy event and records the prior class for restore.
        const reason = switch (payload.object.get("reason") orelse .null) {
            .string => |s| s,
            else => "wardenctl",
        };
        target_rt.policy.quarantine(pid, reason) catch return r.err("process not found");
        const out = try std.fmt.allocPrint(allocator, "{{\"pid\":\"{s}\",\"op\":\"quarantine\"}}", .{pid_str});
        defer allocator.free(out);
        try r.ok(out);
    } else if (std.mem.eql(u8, op_str, "promote")) {
        const class_str = switch (payload.object.get("class") orelse .null) {
            .string => |s| s,
            else => "elevated",
        };
        const ttl_ms = switch (payload.object.get("ttl_ms") orelse .null) {
            .integer => |n| @as(?u64, @intCast(n)),
            else => null,
        };
        const new_class = std.meta.stringToEnum(types.ActivityClass, class_str) orelse {
            const m = try std.fmt.allocPrint(allocator, "unknown activity class: {s}", .{class_str});
            defer allocator.free(m);
            return r.err(m);
        };
        // warden-qj2: route through the policy engine — emits a policy event and
        // registers the TTL for auto-expiry (reg.setPromotion alone never expired).
        const reason = switch (payload.object.get("reason") orelse .null) {
            .string => |s| s,
            else => "wardenctl",
        };
        target_rt.policy.promote(pid, new_class, ttl_ms, reason) catch return r.err("process not found");
        const out = try std.fmt.allocPrint(allocator, "{{\"pid\":\"{s}\",\"op\":\"promote\",\"class\":\"{s}\"}}", .{ pid_str, class_str });
        defer allocator.free(out);
        try r.ok(out);
    } else {
        const m = try std.fmt.allocPrint(allocator, "unknown op: {s}", .{op_str});
        defer allocator.free(m);
        try r.err(m);
    }
}


// warden-2rb
/// policy.events — return the target beam's recorded policy events
/// (promote/demote/quarantine/expire) so external orchestrators can observe
/// control-plane policy actions. Optional payload `beam` selects the beam
/// (default: the primary beam).
pub fn handlePolicyEvents(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const r = h.r;

    var beam_id: u32 = cs.runtime.beam_id;
    if (h.payload) |pv| {
        if (pv == .object) {
            if (pv.object.get("beam")) |bv| {
                if (bv == .integer) beam_id = @intCast(bv.integer);
            }
        }
    }
    const target_rt = cs.runtimes.get(beam_id) orelse return r.err("unknown beam");

    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;
    try w.writeAll("{\"events\":[");
    {
        // Hold the policy lock only while reading the events into the buffer.
        target_rt.policy.mutex.lock();
        defer target_rt.policy.mutex.unlock();
        for (target_rt.policy.events.items, 0..) |e, i| {
            if (i > 0) try w.writeByte(',');
            try w.print("{{\"pid\":\"{d}/{d}\",\"action\":", .{ e.pid.beam, e.pid.proc });
            try transport.writeJsonEscapedString(w, e.action);
            try w.writeAll(",\"reason\":");
            try transport.writeJsonEscapedString(w, e.reason);
            try w.print(",\"ts\":{d}}}", .{e.ts});
        }
    }
    try w.writeAll("]}");
    try r.ok(buf.writer.buffered());
}
