// warden-r28
// proc.* RPC handlers (spawn/send/call/list) + their local helpers.

const std = @import("std");
const clock = @import("../clock.zig");
const beam_mod = @import("../beam.zig");
const bridge_mod = @import("../bridge.zig");
const registry_mod = @import("../registry.zig");
const types = @import("../types.zig");
const transport = @import("transport.zig");
const control = @import("../control.zig");

const Runtime = beam_mod.Runtime;
const Pid = types.Pid;
const ProcessEntry = registry_mod.ProcessEntry;
const HandlerCtx = control.HandlerCtx;
const parseBeamProc = transport.parseBeamProc;
const writeJsonEscapedString = transport.writeJsonEscapedString;
const writeFrame = transport.writeFrame;

// warden-3qh: Zig 0.16 removed std.crypto.random; fill via std.Io.random.
fn randomU32(io: std.Io) u32 {
    var b: [4]u8 = undefined;
    std.Io.random(io, &b);
    return std.mem.readInt(u32, &b, .little);
}

// warden-7oi
fn matchAny(_: types.MessageEnvelope) bool {
    return true;
}

// warden-di6
// warden-36j: append a beam runtime's registry entries (matching the kind/state
// filters) to `entries`, holding only that registry's lock for the scan.
pub fn collectProcEntries(
    rt: *Runtime,
    filter_kind: ?[]const u8,
    filter_state: ?[]const u8,
    entries: *std.ArrayList(ProcessEntry),
    allocator: std.mem.Allocator,
) !void {
    rt.registry.mutex.lock();
    defer rt.registry.mutex.unlock();
    var it = rt.registry.map.iterator();
    while (it.next()) |kv| {
        const e = kv.value_ptr.*;
        if (filter_kind) |fk| {
            if (!std.mem.eql(u8, @tagName(e.kind), fk)) continue;
        }
        if (filter_state) |fs| {
            if (!std.mem.eql(u8, @tagName(e.state), fs)) continue;
        }
        try entries.append(allocator, e);
    }
}

// warden-7oi
pub fn handleProcSpawn(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const r = h.r;
    const pv = payload_val orelse return r.err("missing payload");
    if (pv != .object) return r.err("payload must be object");
    const obj = pv.object;

    const cmd_val = obj.get("cmd") orelse return r.err("missing cmd");
    if (cmd_val != .array) return r.err("cmd must be array");

    var cmd: std.ArrayList([]const u8) = .empty;
    defer cmd.deinit(allocator);
    for (cmd_val.array.items) |item| {
        if (item != .string) return r.err("cmd entries must be strings");
        try cmd.append(allocator, item.string);
    }

    const beam_id: u32 = if (obj.get("beam")) |bv|
        if (bv == .integer) @intCast(bv.integer) else cs.runtime.beam_id
    else
        cs.runtime.beam_id;

    var parent_pid: ?Pid = null;
    if (obj.get("parent")) |pv2| {
        if (pv2 == .string) parent_pid = bridge_mod.parsePidStr(pv2.string) catch null;
    }

    const sup = cs.supervisors.get(beam_id) orelse
        return r.err("unknown beam");
    const log_dir = cs.log_dir orelse "/tmp";
    const store_base = cs.store_base orelse "/tmp";

    // warden-dmg: optional per-worker restart policy (default permanent).
    var strategy: @import("../restart.zig").Strategy = .permanent;
    if (obj.get("restart")) |rv| {
        if (rv == .string) {
            strategy = @import("../restart.zig").Strategy.parse(rv.string) orelse
                return r.err("invalid restart");
        }
    }
    const pid = try sup.spawnWorkerUnder(cmd.items, log_dir, store_base, parent_pid, strategy);

    const pid_str = try std.fmt.allocPrint(allocator, "{d}/{d}", .{ pid.beam, pid.proc });
    defer allocator.free(pid_str);
    const payload = try std.fmt.allocPrint(allocator, "{{\"pid\":\"{s}\"}}", .{pid_str});
    defer allocator.free(payload);
    try r.ok(payload);
}

// warden-7oi
pub fn handleProcSend(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const r = h.r;
    const pv = payload_val orelse return r.err("missing payload");
    if (pv != .object) return r.err("payload must be object");
    const obj = pv.object;

    const pid_val = obj.get("pid") orelse return r.err("missing pid");
    if (pid_val != .string) return r.err("pid must be string");
    const target = bridge_mod.parsePidStr(pid_val.string) catch
        return r.err("invalid pid");

    const type_val = obj.get("type") orelse return r.err("missing type");
    if (type_val != .string) return r.err("type must be string");

    const body = obj.get("body") orelse std.json.Value.null;
    const msg_id = try std.fmt.allocPrint(allocator, "ext-{x}", .{randomU32(cs.runtime.io)});
    defer allocator.free(msg_id);

    const msg = types.MessageEnvelope{
        .kind = .request,
        .@"type" = type_val.string,
        .id = msg_id,
        .from = "external",
        .to = pid_val.string,
        .body = body,
    };

    if (cs.supervisors.get(target.beam)) |sup| {
        if (try sup.deliver(target, msg)) { // warden-dmg
            return r.okEmpty();
        }
    }
    const rt = cs.runtimes.get(target.beam) orelse
        return r.err("unknown beam");
    // warden-47g: deliver under the mailbox lock so a reaper reclaim can't free
    // the mailbox between lookup and enqueue.
    if (!try rt.tryDeliverMailbox(target, msg))
        return r.err("no mailbox for pid");
    try r.okEmpty();
}

// warden-7oi
pub fn handleProcCall(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const r = h.r;
    const pv = payload_val orelse return r.err("missing payload");
    if (pv != .object) return r.err("payload must be object");
    const obj = pv.object;

    const pid_val = obj.get("pid") orelse return r.err("missing pid");
    if (pid_val != .string) return r.err("pid must be string");
    const target = bridge_mod.parsePidStr(pid_val.string) catch
        return r.err("invalid pid");

    const type_val = obj.get("type") orelse return r.err("missing type");
    if (type_val != .string) return r.err("type must be string");

    const body = obj.get("body") orelse std.json.Value.null;
    const timeout_ms: u64 = if (obj.get("timeout_ms")) |tv|
        if (tv == .integer) @intCast(tv.integer) else 5000
    else
        5000;

    const rt = cs.runtimes.get(target.beam) orelse
        return r.err("unknown beam");

    const caller_pid = try rt.registry.spawn(.native_worker, null, .{});
    // warden-hiz: reclaim the ephemeral caller pid + mailbox on every exit path
    // (reply found, timeout, or delivery error). Registered right after spawn so a
    // failing allocMailbox is also covered (freeMailbox no-ops if absent). Runs
    // only after the function returns, so the receive loop below still has
    // caller_pid's mailbox; a late reply after cleanup is dropped safely
    // (tryDeliverMailbox and freeMailbox share mailboxes_mutex).
    defer {
        rt.registry.transition(caller_pid, .exiting) catch {};
        rt.registry.transition(caller_pid, .dead) catch {};
        rt.registry.remove(caller_pid) catch {};
        rt.freeMailbox(caller_pid);
    }
    try rt.allocMailbox(caller_pid, .{});

    const caller_str = try std.fmt.allocPrint(allocator, "{d}/{d}", .{ caller_pid.beam, caller_pid.proc });
    defer allocator.free(caller_str);
    const msg_id = try std.fmt.allocPrint(allocator, "ext-{x}", .{randomU32(cs.runtime.io)});
    defer allocator.free(msg_id);

    const msg = types.MessageEnvelope{
        .kind = .request,
        .@"type" = type_val.string,
        .id = msg_id,
        .from = caller_str,
        .to = pid_val.string,
        .reply_to = caller_str,
        .body = body,
    };

    if (cs.supervisors.get(target.beam)) |sup| {
        if (!try sup.deliver(target, msg)) { // warden-dmg
            // warden-47g: deliver under the mailbox lock (reclaim-safe).
            if (!try rt.tryDeliverMailbox(target, msg))
                return r.err("no mailbox for pid");
        }
    } else {
        return r.err("unknown beam");
    }

    const max_attempts: u32 = @intCast(timeout_ms / 10 + 1);
    var attempts: u32 = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        // warden-hiz: caller_pid stays non-terminal for the duration of this
        // loop (the cleanup defer at the top runs only after the function
        // returns), so this getMailbox+use-after-unlock cannot race a reclaim.
        const mb = rt.getMailbox(caller_pid) orelse break;
        if (mb.receive(matchAny)) |reply| {
            var body_buf: std.Io.Writer.Allocating = .init(allocator);
            defer body_buf.deinit();
            try bridge_mod.writeJsonValue(&body_buf.writer, reply.body);
            const payload = try std.fmt.allocPrint(allocator,
                "{{\"type\":\"{s}\",\"body\":{s}}}",
                .{ reply.@"type", body_buf.writer.buffered() });
            defer allocator.free(payload);
            return r.ok(payload);
        }
        clock.sleepNs(10 * std.time.ns_per_ms);
    }
    try r.err("timeout");
}

pub fn handleProcList(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const stream = h.stream;
    const r = h.r;
    // Extract optional filters from payload.
    var filter_beam: ?u32 = null;
    var filter_kind: ?[]const u8 = null;
    var filter_state: ?[]const u8 = null;

    if (payload_val) |pv| {
        switch (pv) {
            .object => |p| {
                if (p.get("beam")) |bv| {
                    filter_beam = switch (bv) {
                        .integer => |i| @intCast(i),
                        else => null,
                    };
                }
                if (p.get("kind")) |kv| {
                    filter_kind = switch (kv) {
                        .string => |s| s,
                        else => null,
                    };
                }
                if (p.get("state")) |sv| {
                    filter_state = switch (sv) {
                        .string => |s| s,
                        else => null,
                    };
                }
            },
            else => {},
        }
    }

    // Snapshot matching entries under the registry lock.
    var entries: std.ArrayList(ProcessEntry) = .empty;
    defer entries.deinit(allocator);

    // warden-36j: scan the target beam's registry (or every beam when
    // unfiltered). Previously only cs.runtime (the primary beam) was scanned,
    // so workers on beams created via beam.create were invisible to proc.list.
    if (filter_beam) |fb| {
        if (cs.runtimes.get(fb)) |rt| {
            try collectProcEntries(rt, filter_kind, filter_state, &entries, allocator);
        }
        // unknown beam -> empty list
    } else {
        var rit = cs.runtimes.iterator();
        while (rit.next()) |kv| {
            try collectProcEntries(kv.value_ptr.*, filter_kind, filter_state, &entries, allocator);
        }
    }

    const now = clock.nowMs();

    // Build JSON response.
    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;

    try r.okPrefix(w);
    try w.writeAll("{\"processes\":[");

    for (entries.items, 0..) |e, idx| {
        if (idx > 0) try w.writeByte(',');
        try w.print(
            "{{\"beam\":{d},\"pid\":{d},\"kind\":\"{s}\",\"state\":\"{s}\"," ++
            "\"policy\":\"{s}\",\"last_active_ms\":{d}}}",
            .{
                e.pid.beam,
                e.pid.proc,
                @tagName(e.kind),
                @tagName(e.state),
                @tagName(e.policy.activity_class),
                now - e.last_active_at,
            },
        );
    }

    try w.writeAll("]}");
    try r.okSuffix(w);
    try writeFrame(cs.runtime.io, stream, buf.writer.buffered());
}

