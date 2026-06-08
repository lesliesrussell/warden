// warden-x4e
//
// Warden daemon — start a Runtime and ControlServer, block until SIGINT/SIGTERM.
//
// Environment variables:
//   WARDEN_BEAM_ID      Beam identifier (default: 1)
//   WARDEN_CTRL_SOCKET  Control socket path (default: ~/.warden/ctrl.sock)
//   WARDEN_LOG_DIR      Per-process log directory (optional)

const std = @import("std");
const warden = @import("warden");

var g_stopping = std.atomic.Value(bool).init(false);

fn sigHandler(_: c_int) callconv(.c) void {
    g_stopping.store(true, .release);
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Config from environment.
    const beam_id_str = warden.env.get("WARDEN_BEAM_ID") orelse "1";
    const beam_id = std.fmt.parseInt(u32, beam_id_str, 10) catch {
        std.debug.print("error: WARDEN_BEAM_ID must be a positive integer\n", .{});
        std.process.exit(1);
    };

    const log_dir = warden.env.get("WARDEN_LOG_DIR");

    // Socket path: explicit env > default per-beam path in ~/.warden/sockets/.
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const socket_path: []const u8 = if (warden.env.get("WARDEN_CTRL_SOCKET")) |s|
        resolveHome(&path_buf, s)
    else blk: {
        const home = warden.env.get("HOME") orelse {
            std.debug.print("error: HOME not set and WARDEN_CTRL_SOCKET not provided\n", .{});
            std.process.exit(1);
        };
        break :blk std.fmt.bufPrint(&path_buf, "{s}/.warden/sockets/{d}.sock", .{ home, beam_id }) catch {
            std.debug.print("error: socket path too long\n", .{});
            std.process.exit(1);
        };
    };

    // Ensure parent directory exists.
    if (std.fs.path.dirname(socket_path)) |parent| {
        const warden_dir = std.fs.path.dirname(parent) orelse parent;
        std.fs.makeDirAbsolute(warden_dir) catch |e| if (e != error.PathAlreadyExists) {
            std.debug.print("error: cannot create {s}: {}\n", .{ warden_dir, e });
            std.process.exit(1);
        };
        std.fs.makeDirAbsolute(parent) catch |e| if (e != error.PathAlreadyExists) {
            std.debug.print("error: cannot create {s}: {}\n", .{ parent, e });
            std.process.exit(1);
        };
    }

    // Start runtime.
    const rt = try warden.beam.Runtime.init(allocator, beam_id);
    defer rt.destroy();

    // Start control server.
    var cs = try warden.control.ControlServer.init(allocator, rt, socket_path);
    if (log_dir) |d| try cs.setLogDir(d);
    try cs.start();
    defer cs.stop();

    std.debug.print("warden: beam {d} ready  socket={s}\n", .{ beam_id, socket_path });
    if (log_dir) |d| std.debug.print("warden: log_dir={s}\n", .{d});

    // Install signal handlers.
    const act = std.posix.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    // Block until signal.
    while (!g_stopping.load(.acquire)) {
        warden.clock.sleepNs(100 * std.time.ns_per_ms);
    }

    std.debug.print("warden: beam {d} shutting down\n", .{beam_id});
}

fn resolveHome(buf: []u8, path: []const u8) []const u8 {
    if (!std.mem.startsWith(u8, path, "~/")) return path;
    const home = warden.env.get("HOME") orelse return path;
    return std.fmt.bufPrint(buf, "{s}{s}", .{ home, path[1..] }) catch return path;
}
