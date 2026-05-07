// warden-39v

const std = @import("std");
const beam = @import("beam.zig");
const control = @import("control.zig");
const types = @import("types.zig");

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

// warden-9jm
test "control server: logs.stream reads log file by pid" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    // Write a synthetic log file for beam=33, proc=7.
    {
        const lf = try tmp.dir.createFile("33-7.log", .{});
        defer lf.close();
        try lf.writeAll("{\"ts\":1000.0,\"beam\":33,\"pid\":7,\"seq\":1,\"event\":\"note\",\"msg\":\"hello\"}\n");
        try lf.writeAll("{\"ts\":1001.0,\"beam\":33,\"pid\":7,\"seq\":2,\"event\":\"note\",\"msg\":\"world\"}\n");
        try lf.writeAll("{\"ts\":1002.0,\"beam\":33,\"pid\":7,\"seq\":3,\"event\":\"metric\",\"name\":\"latency_ms\",\"value\":3}\n");
    }

    const rt = try beam.Runtime.init(allocator, 33);
    defer rt.destroy();

    const socket_path = "/tmp/warden_ctrl_test7.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.setLogDir(log_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"lg1\",\"action\":\"logs.stream\",\"payload\":{\"pid\":\"33/7\"}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    try std.testing.expect(obj.get("ok").?.bool);

    const lines = obj.get("payload").?.object.get("lines").?.array;
    try std.testing.expectEqual(@as(usize, 3), lines.items.len);

    // First line should contain "hello".
    try std.testing.expect(std.mem.indexOf(u8, lines.items[0].string, "hello") != null);
}

test "control server: logs.stream grep filter" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    {
        const lf = try tmp.dir.createFile("34-8.log", .{});
        defer lf.close();
        try lf.writeAll("{\"ts\":1000.0,\"event\":\"note\",\"msg\":\"tool_call started\"}\n");
        try lf.writeAll("{\"ts\":1001.0,\"event\":\"note\",\"msg\":\"unrelated log\"}\n");
        try lf.writeAll("{\"ts\":1002.0,\"event\":\"note\",\"msg\":\"tool_call finished\"}\n");
    }

    const rt = try beam.Runtime.init(allocator, 34);
    defer rt.destroy();

    const socket_path = "/tmp/warden_ctrl_test8.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.setLogDir(log_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = "{\"req_id\":\"lg2\",\"action\":\"logs.stream\",\"payload\":{\"pid\":\"34/8\",\"grep\":\"tool_call\"}}";
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();

    const lines = parsed.value.object.get("payload").?.object.get("lines").?.array;
    // Only the 2 lines containing "tool_call".
    try std.testing.expectEqual(@as(usize, 2), lines.items.len);
}

// warden-aai
test "control server: proc.control pause and resume" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 77);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});
    // Spawn starts in .starting; transition to .ready so pause is a valid transition.
    try rt.registry.transition(pid, .ready);

    const socket_path = "/tmp/warden_ctrl_test9.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    // Pause — new connection per request (server handles one frame per connection).
    {
        const stream = try std.net.connectUnixSocket(socket_path);
        defer stream.close();

        const pause_req = try std.fmt.allocPrint(
            allocator,
            "{{\"req_id\":\"pc1\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"77/{d}\",\"op\":\"pause\"}}}}",
            .{pid.proc},
        );
        defer allocator.free(pause_req);
        try control.writeFrame(stream, pause_req);

        const pause_resp = try control.readFrame(allocator, stream);
        defer allocator.free(pause_resp);

        const p_parsed = try std.json.parseFromSlice(std.json.Value, allocator, pause_resp, .{});
        defer p_parsed.deinit();
        try std.testing.expectEqual(true, p_parsed.value.object.get("ok").?.bool);
    }

    rt.registry.mutex.lock();
    const after_pause = rt.registry.map.get(pid.proc).?.state;
    rt.registry.mutex.unlock();
    try std.testing.expectEqual(types.ProcessState.paused, after_pause);

    // Resume — new connection.
    {
        const stream = try std.net.connectUnixSocket(socket_path);
        defer stream.close();

        const resume_req = try std.fmt.allocPrint(
            allocator,
            "{{\"req_id\":\"pc2\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"77/{d}\",\"op\":\"resume\"}}}}",
            .{pid.proc},
        );
        defer allocator.free(resume_req);
        try control.writeFrame(stream, resume_req);

        const resume_resp = try control.readFrame(allocator, stream);
        defer allocator.free(resume_resp);

        const r_parsed = try std.json.parseFromSlice(std.json.Value, allocator, resume_resp, .{});
        defer r_parsed.deinit();
        try std.testing.expectEqual(true, r_parsed.value.object.get("ok").?.bool);
    }

    rt.registry.mutex.lock();
    const after_resume = rt.registry.map.get(pid.proc).?.state;
    rt.registry.mutex.unlock();
    try std.testing.expectEqual(types.ProcessState.ready, after_resume);
}

// warden-h0j
test "control server: proc.control kill transitions to exiting" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 88);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});
    try rt.registry.transition(pid, .ready);

    const socket_path = "/tmp/warden_ctrl_test10.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = try std.fmt.allocPrint(
        allocator,
        "{{\"req_id\":\"k1\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"88/{d}\",\"op\":\"kill\",\"reason\":\"test\"}}}}",
        .{pid.proc},
    );
    defer allocator.free(req);
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(true, parsed.value.object.get("ok").?.bool);

    rt.registry.mutex.lock();
    const state = rt.registry.map.get(pid.proc).?.state;
    rt.registry.mutex.unlock();
    try std.testing.expectEqual(types.ProcessState.exiting, state);
}

test "control server: proc.control quarantine sets activity_class to tiny" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 89);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});

    const socket_path = "/tmp/warden_ctrl_test11.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = try std.fmt.allocPrint(
        allocator,
        "{{\"req_id\":\"q1\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"89/{d}\",\"op\":\"quarantine\"}}}}",
        .{pid.proc},
    );
    defer allocator.free(req);
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(true, parsed.value.object.get("ok").?.bool);

    rt.registry.mutex.lock();
    const class = rt.registry.map.get(pid.proc).?.policy.activity_class;
    rt.registry.mutex.unlock();
    try std.testing.expectEqual(types.ActivityClass.tiny, class);
}

test "control server: proc.control promote sets activity_class and ttl" {
    const allocator = std.testing.allocator;

    const rt = try beam.Runtime.init(allocator, 90);
    defer rt.destroy();

    const pid = try rt.registry.spawn(.native_worker, null, .{});

    const socket_path = "/tmp/warden_ctrl_test12.sock";
    var cs = try control.ControlServer.init(allocator, rt, socket_path);
    try cs.start();
    defer cs.stop();

    std.Thread.sleep(5 * std.time.ns_per_ms);

    const stream = try std.net.connectUnixSocket(socket_path);
    defer stream.close();

    const req = try std.fmt.allocPrint(
        allocator,
        "{{\"req_id\":\"pr1\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"90/{d}\",\"op\":\"promote\",\"class\":\"elevated\",\"ttl_ms\":30000}}}}",
        .{pid.proc},
    );
    defer allocator.free(req);
    try control.writeFrame(stream, req);

    const resp_bytes = try control.readFrame(allocator, stream);
    defer allocator.free(resp_bytes);

    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, resp_bytes, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(true, parsed.value.object.get("ok").?.bool);

    rt.registry.mutex.lock();
    const entry = rt.registry.map.get(pid.proc).?;
    const class = entry.policy.activity_class;
    const ttl = entry.policy.promotion_ttl_ms;
    rt.registry.mutex.unlock();
    try std.testing.expectEqual(types.ActivityClass.elevated, class);
    try std.testing.expectEqual(@as(?u64, 30000), ttl);
}
