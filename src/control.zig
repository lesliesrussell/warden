// warden-39v
//
// Control server — accepts RPC requests over a Unix domain socket and routes
// them to the runtime's registry, policy engine, and logger.
//
// Transport: length-prefixed JSON frames (4-byte big-endian u32 length).
// Activation: call ControlServer.init() then .start().

const std = @import("std");
const env = @import("env.zig");
const clock = @import("clock.zig");
const beam_mod = @import("beam.zig");
const bridge_mod = @import("bridge.zig");
const registry_mod = @import("registry.zig");
const types = @import("types.zig");

const Runtime = beam_mod.Runtime;
const Pid = types.Pid;
const ProcessEntry = registry_mod.ProcessEntry;

// warden-7zc: transport primitives now live in control/transport.zig. Re-export
// the frame I/O (external API used by main.zig + the test files) and alias the
// internal helpers so handler bodies need no changes.
const transport = @import("control/transport.zig");
pub const writeFrame = transport.writeFrame;
pub const readFrame = transport.readFrame;
const Responder = transport.Responder;
const parseBeamProc = transport.parseBeamProc;
const writeJsonEscapedString = transport.writeJsonEscapedString;

// warden-3qh: Zig 0.16 removed std.crypto.random; fill via std.Io.random.
fn randomU32(io: std.Io) u32 {
    var b: [4]u8 = undefined;
    std.Io.random(io, &b);
    return std.mem.readInt(u32, &b, .little);
}

// warden-0i6
// HandlerCtx bundles everything an RPC handler needs into one value so every
// handler shares the signature `fn(*HandlerCtx) !void` — the precondition for
// the action->handler dispatch table (warden-r28). Built once per connection.
const HandlerCtx = struct {
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload: ?std.json.Value,
    r: Responder,
};

// warden-7oi
fn handleBeamCreate(h: *const HandlerCtx) !void {
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
fn handleBeamReaper(h: *const HandlerCtx) !void {
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

// warden-7oi
fn handleProcSpawn(h: *const HandlerCtx) !void {
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
    var strategy: @import("restart.zig").Strategy = .permanent;
    if (obj.get("restart")) |rv| {
        if (rv == .string) {
            strategy = @import("restart.zig").Strategy.parse(rv.string) orelse
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
fn handleProcSend(h: *const HandlerCtx) !void {
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
fn matchAny(_: types.MessageEnvelope) bool {
    return true;
}

// warden-7oi
fn handleProcCall(h: *const HandlerCtx) !void {
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

fn handleBeamList(h: *const HandlerCtx) !void {
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

// warden-di6
// warden-36j: append a beam runtime's registry entries (matching the kind/state
// filters) to `entries`, holding only that registry's lock for the scan.
fn collectProcEntries(
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

fn handleProcList(h: *const HandlerCtx) !void {
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

// warden-mf3
fn writeTreeNode(entries: []const ProcessEntry, node: ProcessEntry, w: anytype) !void {
    try w.print(
        "{{\"beam\":{d},\"pid\":{d},\"kind\":\"{s}\",\"state\":\"{s}\",\"children\":[",
        .{ node.pid.beam, node.pid.proc, @tagName(node.kind), @tagName(node.state) },
    );
    var first = true;
    for (entries) |e| {
        const sv = e.supervisor orelse continue;
        if (sv.beam == node.pid.beam and sv.proc == node.pid.proc) {
            if (!first) try w.writeByte(',');
            try writeTreeNode(entries, e, w);
            first = false;
        }
    }
    try w.writeAll("]}");
}

// warden-mf3
fn handleTopologyGet(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const stream = h.stream;
    const r = h.r;
    var filter_beam: ?u32 = null;
    if (payload_val) |pv| {
        switch (pv) {
            .object => |p| {
                if (p.get("beam")) |bv| {
                    filter_beam = switch (bv) {
                        .integer => |i| @intCast(i),
                        else => null,
                    };
                }
            },
            else => {},
        }
    }

    var entries: std.ArrayList(ProcessEntry) = .empty;
    defer entries.deinit(allocator);

    // warden-f9s: scan the target beam's registry (or every beam when
    // unfiltered), mirroring handleProcList — previously only the primary beam
    // was scanned, hiding foreign-beam processes from topology.get.
    if (filter_beam) |fb| {
        if (cs.runtimes.get(fb)) |rt| {
            try collectProcEntries(rt, null, null, &entries, allocator);
        }
    } else {
        var rit = cs.runtimes.iterator();
        while (rit.next()) |kv| {
            try collectProcEntries(kv.value_ptr.*, null, null, &entries, allocator);
        }
    }

    var buf: std.Io.Writer.Allocating = .init(allocator);
    defer buf.deinit();
    const w = &buf.writer;

    try r.okPrefix(w);
    try w.writeAll("{\"roots\":[");

    var first = true;
    for (entries.items) |e| {
        if (e.supervisor != null) continue; // only roots
        if (!first) try w.writeByte(',');
        try writeTreeNode(entries.items, e, w);
        first = false;
    }

    try w.writeAll("]}");
    try r.okSuffix(w);
    try writeFrame(cs.runtime.io, stream, buf.writer.buffered());
}

// warden-39v
fn handleConnection(cs: *ControlServer, stream: std.Io.net.Stream) !void {
    const allocator = cs.allocator;

    const frame = try readFrame(cs.runtime.io, allocator, stream);
    defer allocator.free(frame);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, frame, .{});
    defer parsed.deinit();

    const obj = switch (parsed.value) {
        .object => |o| o,
        else => return error.InvalidRequest,
    };

    const req_id = switch (obj.get("req_id") orelse return error.MissingReqId) {
        .string => |s| s,
        else => return error.InvalidReqId,
    };
    const action = switch (obj.get("action") orelse return error.MissingAction) {
        .string => |s| s,
        else => return error.InvalidAction,
    };

    // warden-0i6: build the per-request handler context once, then dispatch.
    const h = HandlerCtx{
        .cs = cs,
        .allocator = allocator,
        .req_id = req_id,
        .stream = stream,
        .payload = obj.get("payload"),
        .r = .{ .io = cs.runtime.io, .allocator = allocator, .stream = stream, .req_id = req_id },
    };

    if (std.mem.eql(u8, action, "beam.create")) {
        // warden-7oi
        try handleBeamCreate(&h);
    } else if (std.mem.eql(u8, action, "beam.reaper")) {
        // warden-dmg
        try handleBeamReaper(&h);
    } else if (std.mem.eql(u8, action, "proc.spawn")) {
        // warden-7oi
        try handleProcSpawn(&h);
    } else if (std.mem.eql(u8, action, "proc.send")) {
        // warden-7oi
        try handleProcSend(&h);
    } else if (std.mem.eql(u8, action, "proc.call")) {
        // warden-7oi
        try handleProcCall(&h);
    } else if (std.mem.eql(u8, action, "beam.list")) {
        try handleBeamList(&h);
    } else if (std.mem.eql(u8, action, "proc.list")) {
        // warden-di6
        try handleProcList(&h);
    } else if (std.mem.eql(u8, action, "topology.get")) {
        // warden-mf3
        try handleTopologyGet(&h);
    } else if (std.mem.eql(u8, action, "logs.stream")) {
        // warden-9jm
        try handleLogsStream(&h);
    } else if (std.mem.eql(u8, action, "proc.control")) {
        // warden-aai
        try handleProcControl(&h);
    } else {
        const m = try std.fmt.allocPrint(allocator, "unknown action: {s}", .{action});
        defer allocator.free(m);
        try h.r.err(m);
    }
}

// warden-39v
fn serverThread(cs: *ControlServer) void {
    while (!cs.stopping.load(.acquire)) {
        const stream = cs.server.accept(cs.runtime.io) catch break;
        defer stream.close(cs.runtime.io);
        handleConnection(cs, stream) catch {};
    }
}

fn extractTs(line: []const u8) f64 {
    const key = "\"ts\":";
    const start = std.mem.indexOf(u8, line, key) orelse return 0;
    const num_start = start + key.len;
    var end = num_start;
    while (end < line.len and line[end] != ',' and line[end] != '}') : (end += 1) {}
    return std.fmt.parseFloat(f64, line[num_start..end]) catch 0;
}

// warden-aai
fn handleProcControl(h: *const HandlerCtx) !void {
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

fn handleLogsStream(h: *const HandlerCtx) !void {
    const cs = h.cs;
    const allocator = h.allocator;
    const payload_val = h.payload;
    const stream = h.stream;
    const r = h.r;
    const log_dir = cs.log_dir orelse return r.err("log_dir not configured");

    // Extract payload fields.
    var pid_str: []const u8 = "";
    var since_ms: u64 = 0;
    var grep_pattern: ?[]const u8 = null;
    var follow = false;

    if (payload_val) |pv| {
        switch (pv) {
            .object => |p| {
                if (p.get("pid")) |pv2| pid_str = switch (pv2) {
                    .string => |s| s,
                    else => "",
                };
                if (p.get("since_ms")) |sv| since_ms = switch (sv) {
                    .integer => |i| @intCast(i),
                    else => 0,
                };
                if (p.get("grep")) |gv| grep_pattern = switch (gv) {
                    .string => |s| s,
                    else => null,
                };
                if (p.get("follow")) |fv| follow = switch (fv) {
                    .bool => |b| b,
                    else => false,
                };
            },
            else => {},
        }
    }

    // Parse PID: "beam/proc".
    // warden-y3s: shared beam/proc parse; logs.stream maps both numeric failures
    // to the same "invalid pid" message it historically used.
    const parsed_pid = switch (parseBeamProc(pid_str)) {
        .ok => |p| p,
        .no_slash => return r.err("invalid pid format, expected beam/proc"),
        .bad_beam => return r.err("invalid pid"),
        .bad_proc => return r.err("invalid pid"),
    };
    const beam_id = parsed_pid.beam;
    const proc_id = parsed_pid.proc;

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{d}-{d}.log", .{ log_dir, beam_id, proc_id });
    defer allocator.free(log_path);

    const initial = std.Io.Dir.cwd().readFileAlloc(cs.runtime.io, log_path, allocator, .limited(64 * 1024 * 1024)) catch
        return r.err("log file not found");
    defer allocator.free(initial);

    const cutoff_ts: f64 = if (since_ms > 0)
        @as(f64, @floatFromInt(clock.nowMs() - @as(i64, @intCast(since_ms)))) / 1000.0
    else
        0.0;

    if (!follow) {
        // Read entire file, filter, return as JSON array.
        const content = initial;

        var buf: std.Io.Writer.Allocating = .init(allocator);
        defer buf.deinit();
        const w = &buf.writer;

        try r.okPrefix(w);
        try w.writeAll("{\"lines\":[");

        var count: usize = 0;
        var lines = std.mem.splitScalar(u8, content, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trimEnd(u8, line, "\r");
            if (trimmed.len == 0) continue;
            if (cutoff_ts > 0 and extractTs(trimmed) < cutoff_ts) continue;
            if (grep_pattern) |pat| {
                if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
            }
            if (count > 0) try w.writeByte(',');
            try writeJsonEscapedString(w, trimmed);
            count += 1;
        }

        try w.print("],\"count\":{d}}}", .{count});
        try r.okSuffix(w);
        return writeFrame(cs.runtime.io, stream, buf.writer.buffered());
    }

    // Follow mode: send streaming header, then tail the file.
    try r.ok("{\"streaming\":true}");

    // Send existing content first.
    const content = initial;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        const trimmed = std.mem.trimEnd(u8, line, "\r");
        if (trimmed.len == 0) continue;
        if (cutoff_ts > 0 and extractTs(trimmed) < cutoff_ts) continue;
        if (grep_pattern) |pat| {
            if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
        }
        var line_buf: std.Io.Writer.Allocating = .init(allocator);
        defer line_buf.deinit();
        const lw = &line_buf.writer;
        try lw.writeAll("{\"kind\":\"line\",\"data\":");
        try writeJsonEscapedString(lw, trimmed);
        try lw.writeByte('}');
        writeFrame(cs.runtime.io, stream, line_buf.writer.buffered()) catch return;
    }

    // Poll for new content.
    var pos: usize = initial.len;
    while (!cs.stopping.load(.acquire)) {
        clock.sleepNs(100 * std.time.ns_per_ms);
        const whole = std.Io.Dir.cwd().readFileAlloc(cs.runtime.io, log_path, allocator, .limited(64 * 1024 * 1024)) catch break;
        defer allocator.free(whole);
        if (whole.len <= pos) continue;
        const new_content = whole[pos..];
        pos = whole.len;

        var new_lines = std.mem.splitScalar(u8, new_content, '\n');
        while (new_lines.next()) |line| {
            const trimmed = std.mem.trimEnd(u8, line, "\r");
            if (trimmed.len == 0) continue;
            if (grep_pattern) |pat| {
                if (std.mem.indexOf(u8, trimmed, pat) == null) continue;
            }
            var line_buf: std.Io.Writer.Allocating = .init(allocator);
            defer line_buf.deinit();
            const lw = &line_buf.writer;
            try lw.writeAll("{\"kind\":\"line\",\"data\":");
            try writeJsonEscapedString(lw, trimmed);
            try lw.writeByte('}');
            writeFrame(cs.runtime.io, stream, line_buf.writer.buffered()) catch return;
        }
    }
}

// warden-39v
pub const ControlServer = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    socket_path: []u8,
    log_dir: ?[]u8,
    // warden-4ga
    sidecar_path: ?[]u8,
    server: std.Io.net.Server,
    thread: ?std.Thread,
    stopping: std.atomic.Value(bool),
    started_at: i64,
    // warden-7oi: multi-beam management
    runtimes: std.AutoHashMap(u32, *Runtime),
    supervisors: std.AutoHashMap(u32, *bridge_mod.BridgeSupervisor),
    next_beam_id: std.atomic.Value(u32),
    store_base: ?[]u8,

    /// Bind the control socket and prepare to accept connections.
    /// Call start() to begin serving.
    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, socket_path: []const u8) !ControlServer {
        const owned_path = try allocator.dupe(u8, socket_path);
        errdefer allocator.free(owned_path);
        std.Io.Dir.deleteFileAbsolute(runtime.io, socket_path) catch {};
        const addr = try std.Io.net.UnixAddress.init(socket_path);
        const server = try addr.listen(runtime.io, .{});

        // warden-7oi: seed the beam and supervisor maps with the primary runtime
        var runtimes = std.AutoHashMap(u32, *Runtime).init(allocator);
        errdefer runtimes.deinit();
        try runtimes.put(runtime.beam_id, runtime);

        const primary_sup = try allocator.create(bridge_mod.BridgeSupervisor);
        primary_sup.* = bridge_mod.BridgeSupervisor.init(allocator, runtime);
        try primary_sup.startReaper(); // warden-dmg
        var supervisors = std.AutoHashMap(u32, *bridge_mod.BridgeSupervisor).init(allocator);
        errdefer {
            primary_sup.deinit();
            allocator.destroy(primary_sup);
            supervisors.deinit();
        }
        try supervisors.put(runtime.beam_id, primary_sup);

        return .{
            .allocator = allocator,
            .runtime = runtime,
            .socket_path = owned_path,
            .log_dir = null,
            .sidecar_path = null,
            .server = server,
            .thread = null,
            .stopping = std.atomic.Value(bool).init(false),
            .started_at = clock.nowMs(),
            .runtimes = runtimes,
            .supervisors = supervisors,
            .next_beam_id = std.atomic.Value(u32).init(runtime.beam_id + 1),
            .store_base = null,
        };
    }

    // warden-9jm
    /// Configure the log directory so logs.stream can locate per-process log files.
    pub fn setLogDir(self: *ControlServer, log_dir: []const u8) !void {
        if (self.log_dir) |old| self.allocator.free(old);
        self.log_dir = try self.allocator.dupe(u8, log_dir);
    }

    /// Spawn the server thread and register a discovery sidecar. Must be called after init().
    pub fn start(self: *ControlServer) !void {
        self.thread = try std.Thread.spawn(.{}, serverThread, .{self});
        // warden-4ga: write sidecar so wardenctl can discover this runtime
        self.writeSidecar() catch {};
    }

    /// Signal the server to stop, close the listen socket, and join the thread.
    pub fn stop(self: *ControlServer) void {
        self.stopping.store(true, .release);
        // Closing the listen socket unblocks accept() in the server thread.
        self.server.deinit(self.runtime.io);
        if (self.thread) |t| t.join();
        self.thread = null;
        std.Io.Dir.deleteFileAbsolute(self.runtime.io, self.socket_path) catch {};
        self.allocator.free(self.socket_path);
        if (self.log_dir) |d| self.allocator.free(d);
        // warden-4ga: remove sidecar on shutdown
        if (self.sidecar_path) |p| {
            std.Io.Dir.deleteFileAbsolute(self.runtime.io, p) catch {};
            self.allocator.free(p);
            self.sidecar_path = null;
        }
        // warden-7oi: deinit all supervisors and non-primary runtimes
        {
            var it = self.supervisors.valueIterator();
            while (it.next()) |sup_ptr| {
                sup_ptr.*.deinit();
                self.allocator.destroy(sup_ptr.*);
            }
            self.supervisors.deinit();
        }
        {
            var it = self.runtimes.valueIterator();
            while (it.next()) |rt_ptr| {
                if (rt_ptr.* != self.runtime) rt_ptr.*.destroy();
            }
            self.runtimes.deinit();
        }
        if (self.store_base) |s| self.allocator.free(s);
    }

    // warden-7oi
    pub fn setStoreBase(self: *ControlServer, store_base: []const u8) !void {
        if (self.store_base) |old| self.allocator.free(old);
        self.store_base = try self.allocator.dupe(u8, store_base);
    }

    // warden-4ga
    fn writeSidecar(self: *ControlServer) !void {
        const home = env.get("HOME") orelse return error.NoHome;
        const dir_path = try std.fmt.allocPrint(self.allocator, "{s}/.warden/sockets", .{home});
        defer self.allocator.free(dir_path);

        // Ensure ~/.warden and ~/.warden/sockets exist.
        const warden_dir = try std.fmt.allocPrint(self.allocator, "{s}/.warden", .{home});
        defer self.allocator.free(warden_dir);
        try std.Io.Dir.cwd().createDirPath(self.runtime.io, warden_dir);
        try std.Io.Dir.cwd().createDirPath(self.runtime.io, dir_path);

        const sidecar = try std.fmt.allocPrint(
            self.allocator, "{s}/{d}.json", .{ dir_path, self.runtime.beam_id });
        self.sidecar_path = sidecar;

        const json = try std.fmt.allocPrint(self.allocator,
            "{{\"socket_path\":\"{s}\",\"beam_id\":{d}}}\n",
            .{ self.socket_path, self.runtime.beam_id });
        defer self.allocator.free(json);

        try std.Io.Dir.cwd().writeFile(self.runtime.io, .{ .sub_path = sidecar, .data = json });
    }
};
