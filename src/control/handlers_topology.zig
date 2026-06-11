// warden-r28
// topology.get RPC handler + tree serialization.

const std = @import("std");
const registry_mod = @import("../registry.zig");
const transport = @import("transport.zig");
const control = @import("../control.zig");

const ProcessEntry = registry_mod.ProcessEntry;
const HandlerCtx = control.HandlerCtx;
const writeFrame = transport.writeFrame;
const handlers_proc = @import("handlers_proc.zig");
const collectProcEntries = handlers_proc.collectProcEntries;

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
pub fn handleTopologyGet(h: *const HandlerCtx) !void {
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

