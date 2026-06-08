// warden-3cn
//
// Live demo integration test.
//
// Topology (beam 43):
//   demo_supervisor  [supervisor — virtual Zig process, root of the tree]
//   ├── math_worker  [foreign_worker — Python, handles req.fib / req.primes]
//   └── web_server   [foreign_worker — Python, HTTP on random port + req.get_url]
//
// What this tests:
//   1. Python workers are spawned and registered in the process registry.
//   2. Topology shows parent-child relationship.
//   3. math_worker handles req.fib(10) → replies res.ok with body 55.
//   4. web_server serves HTTP GET /status → {"status":"ok",...}.
//   5. Pausing math_worker transitions state to "paused".
//   6. Resuming transitions back to "running".

const std = @import("std");
const clock = @import("clock.zig");
const beam_mod = @import("beam.zig");
const bridge = @import("bridge.zig");
const control = @import("control.zig");
const types = @import("types.zig");

const Runtime = beam_mod.Runtime;
const BridgeSupervisor = bridge.BridgeSupervisor;
const Ctx = beam_mod.Ctx;
const Pid = beam_mod.Pid;
const MessageEnvelope = beam_mod.MessageEnvelope;
const SpawnOpts = beam_mod.SpawnOpts;

// Path to the demo Python scripts relative to the repo root.
// `zig build test` is invoked from the repo root, so these are relative to cwd.
const math_script = "examples/live_demo/math_worker.py";
const web_script = "examples/live_demo/web_server.py";

const ctrl_sock = "/tmp/warden_live_demo_43.sock";

fn pidStr(allocator: std.mem.Allocator, pid: Pid) ![]u8 {
    return std.fmt.allocPrint(allocator, "{d}/{d}", .{ pid.beam, pid.proc });
}

fn msgId(allocator: std.mem.Allocator) ![]u8 {
    const n = std.crypto.random.int(u32);
    return std.fmt.allocPrint(allocator, "t{x}", .{n});
}

fn matchResOk(msg: MessageEnvelope) bool {
    return std.mem.eql(u8, msg.@"type", "res.ok");
}

// warden-3cn
test "live demo: topology and messaging" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // ── runtime ──────────────────────────────────────────────────────────────
    const rt = try Runtime.init(allocator, 43);
    defer rt.destroy();
    try rt.start(2);

    // ── control server ───────────────────────────────────────────────────────
    var cs = try control.ControlServer.init(allocator, rt, ctrl_sock);
    try cs.start();
    defer cs.stop();

    // ── virtual supervisor process (root of demo tree) ────────────────────────
    const sup_pid = try rt.registry.spawn(.native_supervisor, null, .{});
    try rt.allocMailbox(sup_pid, .{});
    var sup_ctx = try Ctx.init(rt, sup_pid, "/tmp", "/tmp");
    defer sup_ctx.deinit();

    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_store = std.testing.tmpDir(.{});
    defer tmp_store.cleanup();
    const log_path = try tmp_log.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);
    const store_path = try tmp_store.dir.realpathAlloc(allocator, ".");
    defer allocator.free(store_path);

    // ── bridge supervisor ─────────────────────────────────────────────────────
    var sup = BridgeSupervisor.init(allocator, rt);
    defer sup.deinit();

    // python3 path
    const py = "python3";

    // spawn math_worker as child of demo_supervisor
    const math_pid = try sup.spawnWorkerUnder(
        &.{ py, math_script },
        log_path,
        store_path,
        sup_pid,
    );

    // spawn web_server as child of demo_supervisor
    const web_pid = try sup.spawnWorkerUnder(
        &.{ py, web_script },
        log_path,
        store_path,
        sup_pid,
    );

    // ── 1. verify both workers appear in proc.list ────────────────────────────
    {
        const stream = try std.net.connectUnixSocket(ctrl_sock);
        defer stream.close();
        const req = "{\"req_id\":\"t1\",\"action\":\"proc.list\",\"payload\":{\"beam\":43}}";
        var hdr: [4]u8 = undefined;
        std.mem.writeInt(u32, &hdr, @intCast(req.len), .big);
        try stream.writeAll(&hdr);
        try stream.writeAll(req);

        var rhdr: [4]u8 = undefined;
        _ = try stream.readAtLeast(&rhdr, 4);
        const rlen = std.mem.readInt(u32, &rhdr, .big);
        const rbuf = try allocator.alloc(u8, rlen);
        defer allocator.free(rbuf);
        _ = try stream.readAtLeast(rbuf, rlen);

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, rbuf, .{});
        defer parsed.deinit();
        const procs = parsed.value.object.get("payload").?.object.get("processes").?.array;
        try std.testing.expect(procs.items.len >= 2);
    }

    // ── 2. verify topology shows math_worker and web_server as children ───────
    {
        const stream = try std.net.connectUnixSocket(ctrl_sock);
        defer stream.close();
        const req = "{\"req_id\":\"t2\",\"action\":\"topology.get\",\"payload\":{\"beam\":43}}";
        var hdr: [4]u8 = undefined;
        std.mem.writeInt(u32, &hdr, @intCast(req.len), .big);
        try stream.writeAll(&hdr);
        try stream.writeAll(req);

        var rhdr: [4]u8 = undefined;
        _ = try stream.readAtLeast(&rhdr, 4);
        const rlen = std.mem.readInt(u32, &rhdr, .big);
        const rbuf = try allocator.alloc(u8, rlen);
        defer allocator.free(rbuf);
        _ = try stream.readAtLeast(rbuf, rlen);

        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, rbuf, .{});
        defer parsed.deinit();
        const roots = parsed.value.object.get("payload").?.object.get("roots").?.array;
        // supervisor is the root; it should have 2 children
        try std.testing.expect(roots.items.len >= 1);
        const root = roots.items[0];
        const children = root.object.get("children").?.array;
        try std.testing.expect(children.items.len == 2);
    }

    // ── 3. send req.fib(10) to math_worker, await res.ok ─────────────────────
    {
        // Test process needs its own Ctx and mailbox to receive replies.
        const test_pid = try rt.registry.spawn(.native_worker, null, .{});
        try rt.allocMailbox(test_pid, .{});
        var test_ctx = try Ctx.init(rt, test_pid, "/tmp", "/tmp");
        defer test_ctx.deinit();

        const test_pid_str = try pidStr(allocator, test_pid);
        defer allocator.free(test_pid_str);
        const math_pid_str = try pidStr(allocator, math_pid);
        defer allocator.free(math_pid_str);
        const id = try msgId(allocator);
        defer allocator.free(id);

        // Deliver req.fib with body=10 directly to the bridge
        const fib_msg = MessageEnvelope{
            .kind = .request,
            .@"type" = "req.fib",
            .id = id,
            .from = test_pid_str,
            .to = math_pid_str,
            .reply_to = test_pid_str,
            .body = .{ .integer = 10 },
        };
        // Find the math_worker bridge and deliver
        try sup.bridges.items[0].deliver(fib_msg);

        // Poll for reply (res.ok) with up to 3s timeout
        var got_reply = false;
        var attempts: u32 = 0;
        while (attempts < 300) : (attempts += 1) {
            const mb = rt.getMailbox(test_pid) orelse break;
            if (mb.receive(matchResOk)) |reply| {
                // Verify body is 55 (fib(10))
                switch (reply.body) {
                    .integer => |v| try std.testing.expectEqual(@as(i64, 55), v),
                    else => return error.UnexpectedBodyType,
                }
                got_reply = true;
                break;
            }
            clock.sleepNs(10 * std.time.ns_per_ms);
        }
        try std.testing.expect(got_reply);
    }

    // ── 4. HTTP GET /status to web_server ─────────────────────────────────────
    {
        // Ask web_server for its URL via req.get_url
        const test_pid2 = try rt.registry.spawn(.native_worker, null, .{});
        try rt.allocMailbox(test_pid2, .{});
        var test_ctx2 = try Ctx.init(rt, test_pid2, "/tmp", "/tmp");
        defer test_ctx2.deinit();

        const test_pid2_str = try pidStr(allocator, test_pid2);
        defer allocator.free(test_pid2_str);
        const web_pid_str = try pidStr(allocator, web_pid);
        defer allocator.free(web_pid_str);
        const id2 = try msgId(allocator);
        defer allocator.free(id2);

        const url_msg = MessageEnvelope{
            .kind = .request,
            .@"type" = "req.get_url",
            .id = id2,
            .from = test_pid2_str,
            .to = web_pid_str,
            .reply_to = test_pid2_str,
            .body = .null,
        };
        try sup.bridges.items[1].deliver(url_msg);

        // Poll for reply
        var port_found: u16 = 0;
        var attempts: u32 = 0;
        while (attempts < 300) : (attempts += 1) {
            const mb = rt.getMailbox(test_pid2) orelse break;
            if (mb.receive(matchResOk)) |reply| {
                // body = {"url": "http://127.0.0.1:<port>"}
                const url = reply.body.object.get("url").?.string;
                // parse port from "http://127.0.0.1:NNNNN"
                const colon_pos = std.mem.lastIndexOfScalar(u8, url, ':') orelse return error.NoPort;
                port_found = try std.fmt.parseInt(u16, url[colon_pos + 1 ..], 10);
                break;
            }
            clock.sleepNs(10 * std.time.ns_per_ms);
        }
        try std.testing.expect(port_found > 0);

        // Make HTTP GET /status — read until EOF (HTTP/1.0 closes after response)
        const addr = try std.net.Address.parseIp4("127.0.0.1", port_found);
        const sock = try std.net.tcpConnectToAddress(addr);
        defer sock.close();
        try sock.writeAll("GET /status HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n");
        var resp_buf: std.ArrayList(u8) = .empty;
        defer resp_buf.deinit(allocator);
        var chunk: [512]u8 = undefined;
        while (true) {
            const nr = try sock.read(&chunk);
            if (nr == 0) break;
            try resp_buf.appendSlice(allocator, chunk[0..nr]);
        }
        const resp = resp_buf.items;
        try std.testing.expect(std.mem.indexOf(u8, resp, "200") != null);
        try std.testing.expect(std.mem.indexOf(u8, resp, "\"status\"") != null);
    }

    // ── 5. pause math_worker, verify state, resume ───────────────────────────
    {
        const math_str = try pidStr(allocator, math_pid);
        defer allocator.free(math_str);

        // Pause via ControlServer RPC
        {
            const stream = try std.net.connectUnixSocket(ctrl_sock);
            defer stream.close();
            const req_body = try std.fmt.allocPrint(
                allocator,
                "{{\"req_id\":\"t3\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"{s}\",\"op\":\"pause\"}}}}",
                .{math_str},
            );
            defer allocator.free(req_body);
            var hdr: [4]u8 = undefined;
            std.mem.writeInt(u32, &hdr, @intCast(req_body.len), .big);
            try stream.writeAll(&hdr);
            try stream.writeAll(req_body);
            var rhdr: [4]u8 = undefined;
            _ = try stream.readAtLeast(&rhdr, 4);
            const rlen = std.mem.readInt(u32, &rhdr, .big);
            const rbuf = try allocator.alloc(u8, rlen);
            defer allocator.free(rbuf);
            _ = try stream.readAtLeast(rbuf, rlen);
        }

        // Verify paused state
        {
            const entry = rt.registry.lookup(math_pid) orelse return error.NoEntry;
            try std.testing.expectEqual(types.ProcessState.paused, entry.state);
        }

        // Resume
        {
            const stream = try std.net.connectUnixSocket(ctrl_sock);
            defer stream.close();
            const req_body = try std.fmt.allocPrint(
                allocator,
                "{{\"req_id\":\"t4\",\"action\":\"proc.control\",\"payload\":{{\"pid\":\"{s}\",\"op\":\"resume\"}}}}",
                .{math_str},
            );
            defer allocator.free(req_body);
            var hdr: [4]u8 = undefined;
            std.mem.writeInt(u32, &hdr, @intCast(req_body.len), .big);
            try stream.writeAll(&hdr);
            try stream.writeAll(req_body);
            var rhdr: [4]u8 = undefined;
            _ = try stream.readAtLeast(&rhdr, 4);
            const rlen = std.mem.readInt(u32, &rhdr, .big);
            const rbuf = try allocator.alloc(u8, rlen);
            defer allocator.free(rbuf);
            _ = try stream.readAtLeast(rbuf, rlen);
        }

        // Verify resumed
        {
            const entry = rt.registry.lookup(math_pid) orelse return error.NoEntry;
            try std.testing.expect(entry.state != types.ProcessState.paused);
        }
    }
}
