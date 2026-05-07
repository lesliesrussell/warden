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
