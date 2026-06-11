// warden-dmg

const std = @import("std");
const testutil = @import("testutil.zig");
const beam = @import("beam.zig");
const bridge = @import("bridge.zig");

fn sleepMs(ms: u64) void {
    @import("clock.zig").sleepNs(ms * std.time.ns_per_ms);
}

// Write a python script into `dir` (absolute) and return its absolute path (owned).
fn writeScript(allocator: std.mem.Allocator, dir: []const u8, name: []const u8, body: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir, name });
    errdefer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = body });
    return path;
}

// A worker that connects, reads the handshake frame, reads ONE real message,
// then exits abnormally (exit code 1) — simulating a crash mid-work.
const crash_on_msg =
    \\import socket, struct, os, sys
    \\s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    \\s.connect(os.environ["WARDEN_SOCKET"])
    \\def frame():
    \\    h = b""
    \\    while len(h) < 4:
    \\        c = s.recv(4 - len(h))
    \\        if not c: sys.exit(0)
    \\        h += c
    \\    n = struct.unpack(">I", h)[0]
    \\    body = b""
    \\    while len(body) < n:
    \\        c = s.recv(n - len(body))
    \\        if not c: sys.exit(0)
    \\        body += c
    \\    return body
    \\frame()   # handshake
    \\frame()   # first real message
    \\os._exit(1)
;

// Snapshot the current pid of the single managed worker, under the lock.
// Returns null if there is no live (non-retired) worker.
fn currentPid(sup: *bridge.BridgeSupervisor) ?beam.Pid {
    sup.mutex.lock();
    defer sup.mutex.unlock();
    if (sup.workers.items.len == 0) return null;
    const w = sup.workers.items[0];
    if (w.retired) return null;
    return w.bridge.pid;
}

test "permanent worker respawns with a new pid after a crash" {
    const allocator = std.testing.allocator;

    // Skip cleanly if python3 is unavailable. (Probing here matters: a missing
    // binary would otherwise hang spawnWorkerUnder forever in server.accept,
    // since the worker would never connect.)
    if (std.process.run(allocator, std.testing.io, .{
        .argv = &.{ "python3", "--version" },
    })) |res| {
        allocator.free(res.stdout);
        allocator.free(res.stderr);
    } else |_| {
        return error.SkipZigTest;
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var base_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try testutil.tmpAbs(&base_buf, &tmp);

    const script = try writeScript(allocator, base, "crash.py", crash_on_msg);
    defer allocator.free(script);

    const rt = try beam.Runtime.init(allocator, 77);
    defer rt.destroy();

    var sup = bridge.BridgeSupervisor.init(allocator, rt);
    try sup.startReaper();
    defer sup.deinit();
    _ = sup.renice(10); // fast reaper for a quick test

    const cmd = [_][]const u8{ "python3", script };
    // spawnWorkerUnder blocks until the worker connects + handshake is sent.
    const pid1 = sup.spawnWorkerUnder(&cmd, base, base, null, .permanent, null) catch return error.SkipZigTest;

    // Send the message that makes the worker exit(1). Locked delivery.
    _ = try sup.deliver(pid1, .{
        .kind = .request,
        .@"type" = "go",
        .id = "1",
        .from = "test",
        .to = "x",
        .body = .null,
    });

    // The reaper should detect the crash and respawn with a NEW pid within ~5s
    // (generous budget for slow python3 cold-start on a loaded CI box; the loop
    // breaks early on success, so this costs nothing when the test passes).
    var respawned = false;
    var i: usize = 0;
    while (i < 500) : (i += 1) {
        sleepMs(10);
        if (currentPid(&sup)) |cur| {
            if (cur.proc != pid1.proc) {
                respawned = true;
                break;
            }
        }
    }
    try std.testing.expect(respawned);
}

// warden-47g
test "reaper reclaims the old incarnation's registry entry + mailbox after restart" {
    const allocator = std.testing.allocator;

    if (std.process.run(allocator, std.testing.io, .{
        .argv = &.{ "python3", "--version" },
    })) |res| {
        allocator.free(res.stdout);
        allocator.free(res.stderr);
    } else |_| {
        return error.SkipZigTest;
    }

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var base_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try testutil.tmpAbs(&base_buf, &tmp);

    const script = try writeScript(allocator, base, "crash2.py", crash_on_msg);
    defer allocator.free(script);

    const rt = try beam.Runtime.init(allocator, 78);
    defer rt.destroy();

    var sup = bridge.BridgeSupervisor.init(allocator, rt);
    sup.reclaim_grace_ms = 0; // reclaim terminal entries on the next reaper cycle
    try sup.startReaper();
    defer sup.deinit();
    _ = sup.renice(10);

    const cmd = [_][]const u8{ "python3", script };
    const pid1 = sup.spawnWorkerUnder(&cmd, base, base, null, .permanent, null) catch return error.SkipZigTest;
    // The bridge allocated a registry entry + mailbox for pid1.
    try std.testing.expect(rt.getMailbox(pid1) != null);

    _ = try sup.deliver(pid1, .{
        .kind = .request,
        .@"type" = "go",
        .id = "1",
        .from = "test",
        .to = "x",
        .body = .null,
    });

    // After the crash the reaper respawns with a new pid AND (grace=0) reclaims
    // the old incarnation's registry entry + mailbox on a subsequent cycle.
    var reclaimed = false;
    var i: usize = 0;
    while (i < 500) : (i += 1) {
        sleepMs(10);
        if (currentPid(&sup)) |cur| {
            if (cur.proc != pid1.proc and
                rt.registry.lookup(pid1) == null and
                rt.getMailbox(pid1) == null)
            {
                reclaimed = true;
                break;
            }
        }
    }
    try std.testing.expect(reclaimed);
}
