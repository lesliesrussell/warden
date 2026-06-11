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
        // warden-f19: mutate under the registry's own lock via its API.
        reg.setActivityClass(pid, .tiny) catch return r.err("process not found");
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
        // warden-f19: mutate under the registry's own lock via its API.
        reg.setPromotion(pid, new_class, ttl_ms) catch return r.err("process not found");
        const out = try std.fmt.allocPrint(allocator, "{{\"pid\":\"{s}\",\"op\":\"promote\",\"class\":\"{s}\"}}", .{ pid_str, class_str });
        defer allocator.free(out);
        try r.ok(out);
    } else {
        const m = try std.fmt.allocPrint(allocator, "unknown op: {s}", .{op_str});
        defer allocator.free(m);
        try r.err(m);
    }
}

