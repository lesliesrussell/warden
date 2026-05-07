// warden-39v

const std = @import("std");
const beam = @import("beam.zig");
const control = @import("control.zig");

test "control server: beam.list returns beam_id and process_count" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 42);
    defer rt.destroy();

    _ = try rt.registry.spawn(.native_worker, null, .{});
    _ = try rt.registry.spawn(.native_worker, null, .{});

    const socket_path = "/tmp/warden_ctrl_test.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    // Give the server thread a moment to reach accept().
    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"t1\",\"action\":\"beam.list\",\"payload\":{}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;

    // ok == true
    const ok = obj.get("ok").?.bool;
    try std.testing.expect(ok);

    const payload = obj.get("payload").?.object;
    const beams = payload.get("beams").?.array;
    try std.testing.expectEqual(@as(usize, 1), beams.items.len);

    const b = beams.items[0].object;
    try std.testing.expectEqual(@as(i64, 42), b.get("beam_id").?.integer);
    // 2 native_workers spawned above
    try std.testing.expect(b.get("process_count").?.integer >= 2);
}

test "control server: unknown action returns ok=false" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 99);
    defer rt.destroy();

    const socket_path = "/tmp/warden_ctrl_test2.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"t2\",\"action\":\"no.such.action\",\"payload\":{}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    try std.testing.expect(!obj.get("ok").?.bool);
}

// warden-di6
test "control server: proc.list returns all spawned processes" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 7);
    defer rt.destroy();

    _ = try rt.registry.spawn(.native_worker, null, .{});
    _ = try rt.registry.spawn(.native_supervisor, null, .{});

    const socket_path = "/tmp/warden_ctrl_test3.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"p1\",\"action\":\"proc.list\",\"payload\":{}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    try std.testing.expect(obj.get("ok").?.bool);

    const procs = obj.get("payload").?.object.get("processes").?.array;
    try std.testing.expectEqual(@as(usize, 2), procs.items.len);
}

test "control server: proc.list with kind filter" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 8);
    defer rt.destroy();

    _ = try rt.registry.spawn(.native_worker, null, .{});
    _ = try rt.registry.spawn(.native_worker, null, .{});
    _ = try rt.registry.spawn(.native_supervisor, null, .{});

    const socket_path = "/tmp/warden_ctrl_test4.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"p2\",\"action\":\"proc.list\",\"payload\":{\"kind\":\"native_worker\"}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const procs = parsed.value.object.get("payload").?.object.get("processes").?.array;
    // Only the 2 native_workers, not the supervisor.
    try std.testing.expectEqual(@as(usize, 2), procs.items.len);
    for (procs.items) |pv| {
        try std.testing.expectEqualStrings("native_worker", pv.object.get("kind").?.string);
    }
}

// warden-mf3
test "control server: topology.get returns tree with parent-child relationships" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 55);
    defer rt.destroy();

    // supervisor → two workers
    const sup_pid = try rt.registry.spawn(.native_supervisor, null, .{});
    const w1_pid = try rt.registry.spawn(.native_worker, sup_pid, .{});
    const w2_pid = try rt.registry.spawn(.native_worker, sup_pid, .{});
    _ = w1_pid;
    _ = w2_pid;

    const socket_path = "/tmp/warden_ctrl_test5.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"topo1\",\"action\":\"topology.get\",\"payload\":{}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    try std.testing.expect(obj.get("ok").?.bool);

    const roots = obj.get("payload").?.object.get("roots").?.array;
    // One root: the supervisor.
    try std.testing.expectEqual(@as(usize, 1), roots.items.len);

    const root = roots.items[0].object;
    try std.testing.expectEqualStrings("native_supervisor", root.get("kind").?.string);

    // Two children.
    const children = root.get("children").?.array;
    try std.testing.expectEqual(@as(usize, 2), children.items.len);
    for (children.items) |child| {
        try std.testing.expectEqualStrings("native_worker", child.object.get("kind").?.string);
    }
}
