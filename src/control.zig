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

// warden-39v
// warden-3qh: Zig 0.16 — std.Io.net.Stream has no writeAll/readAtLeast; frame I/O
// goes through buffered Stream.Writer/Reader, which take the runtime io.
/// Write a length-prefixed JSON frame to a stream.
pub fn writeFrame(io: std.Io, stream: std.Io.net.Stream, json: []const u8) !void {
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(json.len), .big);
    var wbuf: [4096]u8 = undefined;
    var w = stream.writer(io, &wbuf);
    try w.interface.writeAll(&hdr);
    try w.interface.writeAll(json);
    try w.interface.flush();
}

// warden-39v
/// Read a length-prefixed JSON frame. Caller owns the returned slice.
pub fn readFrame(io: std.Io, allocator: std.mem.Allocator, stream: std.Io.net.Stream) ![]u8 {
    var rbuf: [4096]u8 = undefined;
    var r = stream.reader(io, &rbuf);
    var hdr: [4]u8 = undefined;
    r.interface.readSliceAll(&hdr) catch return error.ConnectionClosed;
    const length = std.mem.readInt(u32, &hdr, .big);
    if (length == 0) return error.EmptyFrame;
    const buf = try allocator.alloc(u8, length);
    errdefer allocator.free(buf);
    r.interface.readSliceAll(buf) catch return error.ConnectionClosed;
    return buf;
}

// warden-39v
// warden-7oi
fn sendErrResp(io: std.Io, allocator: std.mem.Allocator, req_id: []const u8, stream: std.Io.net.Stream, msg: []const u8) !void {
    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"{s}\",\"payload\":null}}",
        .{ req_id, msg });
    defer allocator.free(resp);
    try writeFrame(io, stream, resp);
}

// warden-3qh: Zig 0.16 removed std.crypto.random; fill via std.Io.random.
fn randomU32(io: std.Io) u32 {
    var b: [4]u8 = undefined;
    std.Io.random(io, &b);
    return std.mem.readInt(u32, &b, .little);
}

// warden-7oi
fn handleBeamCreate(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
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
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"beam_id\":{d}}}}}",
                .{ req_id, id });
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
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

    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"beam_id\":{d}}}}}",
        .{ req_id, new_id });
    defer allocator.free(resp);
    try writeFrame(cs.runtime.io, stream, resp);
}

// warden-dmg
fn handleBeamReaper(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const pv = payload_val orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing payload");
    if (pv != .object) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "payload must be object");
    const obj = pv.object;

    const beam_id: u32 = if (obj.get("beam")) |bv|
        if (bv == .integer) @intCast(bv.integer) else cs.runtime.beam_id
    else
        cs.runtime.beam_id;

    const interval_val = obj.get("interval_ms") orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing interval_ms");
    if (interval_val != .integer or interval_val.integer < 0)
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "interval_ms must be a non-negative integer");

    const sup = cs.supervisors.get(beam_id) orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "unknown beam");

    const applied = sup.renice(@intCast(interval_val.integer));

    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"interval_ms\":{d}}}}}",
        .{ req_id, applied });
    defer allocator.free(resp);
    try writeFrame(cs.runtime.io, stream, resp);
}

// warden-7oi
fn handleProcSpawn(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const pv = payload_val orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing payload");
    if (pv != .object) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "payload must be object");
    const obj = pv.object;

    const cmd_val = obj.get("cmd") orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing cmd");
    if (cmd_val != .array) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "cmd must be array");

    var cmd: std.ArrayList([]const u8) = .empty;
    defer cmd.deinit(allocator);
    for (cmd_val.array.items) |item| {
        if (item != .string) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "cmd entries must be strings");
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
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "unknown beam");
    const log_dir = cs.log_dir orelse "/tmp";
    const store_base = cs.store_base orelse "/tmp";

    // warden-dmg: optional per-worker restart policy (default permanent).
    var strategy: @import("restart.zig").Strategy = .permanent;
    if (obj.get("restart")) |rv| {
        if (rv == .string) {
            strategy = @import("restart.zig").Strategy.parse(rv.string) orelse
                return sendErrResp(cs.runtime.io, allocator, req_id, stream, "invalid restart");
        }
    }
    const pid = try sup.spawnWorkerUnder(cmd.items, log_dir, store_base, parent_pid, strategy);

    const pid_str = try std.fmt.allocPrint(allocator, "{d}/{d}", .{ pid.beam, pid.proc });
    defer allocator.free(pid_str);
    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"pid\":\"{s}\"}}}}",
        .{ req_id, pid_str });
    defer allocator.free(resp);
    try writeFrame(cs.runtime.io, stream, resp);
}

// warden-7oi
fn handleProcSend(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const pv = payload_val orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing payload");
    if (pv != .object) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "payload must be object");
    const obj = pv.object;

    const pid_val = obj.get("pid") orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing pid");
    if (pid_val != .string) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "pid must be string");
    const target = bridge_mod.parsePidStr(pid_val.string) catch
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "invalid pid");

    const type_val = obj.get("type") orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing type");
    if (type_val != .string) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "type must be string");

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
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        }
    }
    const rt = cs.runtimes.get(target.beam) orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "unknown beam");
    const mb = rt.getMailbox(target) orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "no mailbox for pid");
    _ = try mb.enqueue(msg);
    const resp = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":null}}",
        .{req_id});
    defer allocator.free(resp);
    try writeFrame(cs.runtime.io, stream, resp);
}

// warden-7oi
fn matchAny(_: types.MessageEnvelope) bool {
    return true;
}

// warden-7oi
fn handleProcCall(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const pv = payload_val orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing payload");
    if (pv != .object) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "payload must be object");
    const obj = pv.object;

    const pid_val = obj.get("pid") orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing pid");
    if (pid_val != .string) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "pid must be string");
    const target = bridge_mod.parsePidStr(pid_val.string) catch
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "invalid pid");

    const type_val = obj.get("type") orelse return sendErrResp(cs.runtime.io, allocator, req_id, stream, "missing type");
    if (type_val != .string) return sendErrResp(cs.runtime.io, allocator, req_id, stream, "type must be string");

    const body = obj.get("body") orelse std.json.Value.null;
    const timeout_ms: u64 = if (obj.get("timeout_ms")) |tv|
        if (tv == .integer) @intCast(tv.integer) else 5000
    else
        5000;

    const rt = cs.runtimes.get(target.beam) orelse
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "unknown beam");

    const caller_pid = try rt.registry.spawn(.native_worker, null, .{});
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
            const mb_t = rt.getMailbox(target) orelse
                return sendErrResp(cs.runtime.io, allocator, req_id, stream, "no mailbox for pid");
            _ = try mb_t.enqueue(msg);
        }
    } else {
        return sendErrResp(cs.runtime.io, allocator, req_id, stream, "unknown beam");
    }

    const max_attempts: u32 = @intCast(timeout_ms / 10 + 1);
    var attempts: u32 = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        const mb = rt.getMailbox(caller_pid) orelse break;
        if (mb.receive(matchAny)) |reply| {
            var body_buf: std.Io.Writer.Allocating = .init(allocator);
            defer body_buf.deinit();
            try bridge_mod.writeJsonValue(&body_buf.writer, reply.body);
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null," ++
                "\"payload\":{{\"type\":\"{s}\",\"body\":{s}}}}}",
                .{ req_id, reply.@"type", body_buf.writer.buffered() });
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        }
        clock.sleepNs(10 * std.time.ns_per_ms);
    }
    try sendErrResp(cs.runtime.io, allocator, req_id, stream, "timeout");
}

fn handleBeamList(cs: *ControlServer, allocator: std.mem.Allocator, req_id: []const u8, stream: std.Io.net.Stream) !void {
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
    try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"beams\":[", .{req_id});
    for (ids.items, 0..) |bid, idx| {
        if (idx > 0) try w.writeByte(',');
        const brt = cs.runtimes.get(bid).?;
        brt.registry.mutex.lock();
        const proc_count = brt.registry.map.count();
        brt.registry.mutex.unlock();
        try w.print(
            "{{\"beam_id\":{d},\"version\":\"0.1.0\",\"uptime_ms\":{d},\"process_count\":{d}}}",
            .{ bid, uptime_ms, proc_count });
    }
    try w.writeAll("]}}");
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

fn handleProcList(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
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

    try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"processes\":[", .{req_id});

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

    try w.writeAll("]}}");
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
fn handleTopologyGet(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
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

    try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"roots\":[", .{req_id});

    var first = true;
    for (entries.items) |e| {
        if (e.supervisor != null) continue; // only roots
        if (!first) try w.writeByte(',');
        try writeTreeNode(entries.items, e, w);
        first = false;
    }

    try w.writeAll("]}}");
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

    const payload_val = obj.get("payload");

    if (std.mem.eql(u8, action, "beam.create")) {
        // warden-7oi
        try handleBeamCreate(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "beam.reaper")) {
        // warden-dmg
        try handleBeamReaper(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "proc.spawn")) {
        // warden-7oi
        try handleProcSpawn(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "proc.send")) {
        // warden-7oi
        try handleProcSend(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "proc.call")) {
        // warden-7oi
        try handleProcCall(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "beam.list")) {
        try handleBeamList(cs, allocator, req_id, stream);
    } else if (std.mem.eql(u8, action, "proc.list")) {
        // warden-di6
        try handleProcList(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "topology.get")) {
        // warden-mf3
        try handleTopologyGet(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "logs.stream")) {
        // warden-9jm
        try handleLogsStream(cs, allocator, req_id, stream, payload_val);
    } else if (std.mem.eql(u8, action, "proc.control")) {
        // warden-aai
        try handleProcControl(cs, allocator, req_id, stream, payload_val);
    } else {
        const err_resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"unknown action: {s}\",\"payload\":null}}",
            .{ req_id, action },
        );
        defer allocator.free(err_resp);
        try writeFrame(cs.runtime.io, stream, err_resp);
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

// warden-9jm
fn writeJsonEscapedString(w: anytype, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            0x00...0x08, 0x0B, 0x0C, 0x0E...0x1F => try w.print("\\u{x:0>4}", .{c}),
            else => try w.writeByte(c),
        }
    }
    try w.writeByte('"');
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
fn handleProcControl(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const payload = payload_val orelse {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"missing payload\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(cs.runtime.io, stream, resp);
    };

    const pid_str = switch (payload.object.get("pid") orelse .null) {
        .string => |s| s,
        else => {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"missing pid\",\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        },
    };

    const op_str = switch (payload.object.get("op") orelse .null) {
        .string => |s| s,
        else => {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"missing op\",\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        },
    };

    const slash = std.mem.indexOf(u8, pid_str, "/") orelse {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid format, expected beam/proc\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(cs.runtime.io, stream, resp);
    };

    const beam_id = std.fmt.parseInt(u32, pid_str[0..slash], 10) catch {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid beam id\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(cs.runtime.io, stream, resp);
    };
    const proc_id = std.fmt.parseInt(u64, pid_str[slash + 1 ..], 10) catch {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid proc id\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(cs.runtime.io, stream, resp);
    };

    const pid = types.Pid{ .beam = beam_id, .proc = proc_id };

    // warden-0uj: resolve the target beam's registry. proc.control previously
    // operated on cs.runtime (the primary beam) regardless of the pid's beam,
    // so a foreign-beam pid hit the wrong process or none at all.
    const target_rt = cs.runtimes.get(beam_id) orelse {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"unknown beam\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(resp);
        return writeFrame(cs.runtime.io, stream, resp);
    };
    const reg = &target_rt.registry;

    // warden-h0j
    if (std.mem.eql(u8, op_str, "pause") or std.mem.eql(u8, op_str, "resume")) {
        const target_state: types.ProcessState = if (std.mem.eql(u8, op_str, "pause")) .paused else .ready;
        if (reg.transition(pid, target_state)) |_| {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"pid\":\"{s}\",\"op\":\"{s}\"}}}}",
                .{ req_id, pid_str, op_str });
            defer allocator.free(resp);
            try writeFrame(cs.runtime.io, stream, resp);
        } else |err| {
            const msg: []const u8 = switch (err) {
                error.ProcessNotFound => "process not found",
                error.InvalidTransition => "invalid state transition",
                else => "internal error",
            };
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"{s}\",\"payload\":null}}",
                .{ req_id, msg });
            defer allocator.free(resp);
            try writeFrame(cs.runtime.io, stream, resp);
        }
    } else if (std.mem.eql(u8, op_str, "kill")) {
        if (reg.transition(pid, .exiting)) |_| {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"pid\":\"{s}\",\"op\":\"kill\"}}}}",
                .{ req_id, pid_str });
            defer allocator.free(resp);
            try writeFrame(cs.runtime.io, stream, resp);
        } else |err| {
            const msg: []const u8 = switch (err) {
                error.ProcessNotFound => "process not found",
                error.InvalidTransition => "invalid state transition",
                else => "internal error",
            };
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"{s}\",\"payload\":null}}",
                .{ req_id, msg });
            defer allocator.free(resp);
            try writeFrame(cs.runtime.io, stream, resp);
        }
    } else if (std.mem.eql(u8, op_str, "quarantine")) {
        reg.mutex.lock();
        const entry = reg.map.getPtr(pid.proc);
        if (entry) |e| e.policy.activity_class = .tiny;
        reg.mutex.unlock();
        if (entry == null) {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"process not found\",\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        }
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"pid\":\"{s}\",\"op\":\"quarantine\"}}}}",
            .{ req_id, pid_str });
        defer allocator.free(resp);
        try writeFrame(cs.runtime.io, stream, resp);
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
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"unknown activity class: {s}\",\"payload\":null}}",
                .{ req_id, class_str });
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        };
        reg.mutex.lock();
        const entry = reg.map.getPtr(pid.proc);
        if (entry) |e| {
            e.policy.activity_class = new_class;
            e.policy.promotion_ttl_ms = ttl_ms;
        }
        reg.mutex.unlock();
        if (entry == null) {
            const resp = try std.fmt.allocPrint(allocator,
                "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"process not found\",\"payload\":null}}",
                .{req_id});
            defer allocator.free(resp);
            return writeFrame(cs.runtime.io, stream, resp);
        }
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"pid\":\"{s}\",\"op\":\"promote\",\"class\":\"{s}\"}}}}",
            .{ req_id, pid_str, class_str });
        defer allocator.free(resp);
        try writeFrame(cs.runtime.io, stream, resp);
    } else {
        const resp = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"unknown op: {s}\",\"payload\":null}}",
            .{ req_id, op_str });
        defer allocator.free(resp);
        try writeFrame(cs.runtime.io, stream, resp);
    }
}

fn handleLogsStream(
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload_val: ?std.json.Value,
) !void {
    const log_dir = cs.log_dir orelse {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"log_dir not configured\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(cs.runtime.io, stream, err);
    };

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
    const slash = std.mem.indexOfScalar(u8, pid_str, '/') orelse {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid format, expected beam/proc\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(cs.runtime.io, stream, err);
    };
    const beam_id = std.fmt.parseInt(u32, pid_str[0..slash], 10) catch {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(cs.runtime.io, stream, err);
    };
    const proc_id = std.fmt.parseInt(u64, pid_str[slash + 1 ..], 10) catch {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"invalid pid\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(cs.runtime.io, stream, err);
    };

    const log_path = try std.fmt.allocPrint(allocator, "{s}/{d}-{d}.log", .{ log_dir, beam_id, proc_id });
    defer allocator.free(log_path);

    const initial = std.Io.Dir.cwd().readFileAlloc(cs.runtime.io, log_path, allocator, .limited(64 * 1024 * 1024)) catch {
        const err = try std.fmt.allocPrint(allocator,
            "{{\"req_id\":\"{s}\",\"ok\":false,\"error\":\"log file not found\",\"payload\":null}}",
            .{req_id});
        defer allocator.free(err);
        return writeFrame(cs.runtime.io, stream, err);
    };
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

        try w.print("{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"lines\":[", .{req_id});

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

        try w.print("],\"count\":{d}}}}}", .{count});
        return writeFrame(cs.runtime.io, stream, buf.writer.buffered());
    }

    // Follow mode: send streaming header, then tail the file.
    const header = try std.fmt.allocPrint(allocator,
        "{{\"req_id\":\"{s}\",\"ok\":true,\"error\":null,\"payload\":{{\"streaming\":true}}}}",
        .{req_id});
    defer allocator.free(header);
    try writeFrame(cs.runtime.io, stream, header);

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
