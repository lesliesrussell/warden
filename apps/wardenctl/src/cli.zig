// warden-39v

const std = @import("std");
const ControlClient = @import("client.zig").ControlClient;
const beams_cmd = @import("commands/beams.zig");
// warden-di6
const ps_cmd = @import("commands/ps.zig");
// warden-mf3
const topology_cmd = @import("commands/topology.zig");
// warden-9jm
const logs_cmd = @import("commands/logs.zig");
// warden-aai
const control_cmd = @import("commands/control.zig");

// warden-39v
pub const GlobalOpts = struct {
    socket_path: []const u8,
    // warden-4ga: true when set via --socket or $WARDEN_CTRL_SOCKET
    socket_explicit: bool,
    json_output: bool,
    quiet: bool,
};

const default_socket = "~/.warden/ctrl.sock";

// warden-l3i: Zig 0.16 removed std.posix.getenv; the start code populates the
// global executor's environ block from envp. getPosix is a non-allocating,
// runtime-key scan of that block — a drop-in for the old getenv. wardenctl is a
// standalone module with no access to the warden lib's env helper, so this is a
// small local copy.
fn getenv(name: []const u8) ?[:0]const u8 {
    return std.Io.Threaded.global_single_threaded.environ.process_environ.getPosix(name);
}

// warden-4ga
const SocketEntry = struct {
    socket_path: []u8, // heap-owned
    beam_id: u32,
};

// warden-4ga
// Scan ~/.warden/sockets/*.json and return all valid, reachable socket entries.
fn discoverSockets(allocator: std.mem.Allocator) ![]SocketEntry {
    const home = getenv("HOME") orelse return &.{};
    const dir_path = try std.fmt.allocPrint(allocator, "{s}/.warden/sockets", .{home});
    defer allocator.free(dir_path);

    var dir = std.fs.openDirAbsolute(dir_path, .{ .iterate = true }) catch return &.{};
    defer dir.close();

    var entries: std.ArrayList(SocketEntry) = .empty;
    errdefer {
        for (entries.items) |e| allocator.free(e.socket_path);
        entries.deinit(allocator);
    }

    var it = dir.iterate();
    while (try it.next()) |de| {
        if (!std.mem.endsWith(u8, de.name, ".json")) continue;

        const content = dir.readFileAlloc(allocator, de.name, 4096) catch continue;
        defer allocator.free(content);

        const parsed = std.json.parseFromSlice(std.json.Value, allocator, content, .{}) catch continue;
        defer parsed.deinit();

        const obj = parsed.value.object;
        const sp = switch (obj.get("socket_path") orelse continue) {
            .string => |s| s,
            else => continue,
        };
        const bid: u32 = switch (obj.get("beam_id") orelse continue) {
            .integer => |n| @intCast(n),
            else => continue,
        };

        // Verify the socket is actually reachable; skip stale sidecars.
        const test_stream = std.net.connectUnixSocket(sp) catch continue;
        test_stream.close();

        const owned_sp = try allocator.dupe(u8, sp);
        try entries.append(allocator, .{ .socket_path = owned_sp, .beam_id = bid });
    }

    return entries.toOwnedSlice(allocator);
}

fn freeSocketEntries(allocator: std.mem.Allocator, entries: []SocketEntry) void {
    for (entries) |e| allocator.free(e.socket_path);
    allocator.free(entries);
}

// warden-4ga: parse beam_id from "beam/proc" pid string
fn beamIdFromPid(pid: []const u8) ?u32 {
    const slash = std.mem.indexOf(u8, pid, "/") orelse return null;
    return std.fmt.parseInt(u32, pid[0..slash], 10) catch null;
}

// warden-39v
pub fn run(allocator: std.mem.Allocator, args: []const []const u8) !void {
    const env_socket = getenv("WARDEN_CTRL_SOCKET");
    var opts = GlobalOpts{
        .socket_path = env_socket orelse default_socket,
        .socket_explicit = env_socket != null,
        .json_output = false,
        .quiet = false,
    };

    var i: usize = 0;
    // Parse global flags before the subcommand.
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--socket") or std.mem.eql(u8, arg, "-s")) {
            i += 1;
            if (i >= args.len) return usageErr("--socket requires a path argument");
            opts.socket_path = args[i];
            opts.socket_explicit = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            opts.json_output = true;
        } else if (std.mem.eql(u8, arg, "--quiet") or std.mem.eql(u8, arg, "-q")) {
            opts.quiet = true;
        } else if (std.mem.eql(u8, arg, "--no-color")) {
            // no-op for now
        } else if (std.mem.startsWith(u8, arg, "-")) {
            return usageErr("unknown flag");
        } else {
            break; // first non-flag = subcommand
        }
    }

    if (i >= args.len) {
        return printUsage();
    }

    const subcmd = args[i];
    i += 1;
    const sub_args = args[i..];

    if (opts.socket_explicit) {
        // warden-39v: single explicit socket — existing behavior
        var resolved_buf: [std.fs.max_path_bytes]u8 = undefined;
        const socket_path = resolveHome(&resolved_buf, opts.socket_path);

        var client = ControlClient.connect(allocator, socket_path) catch |err| {
            if (err == error.RuntimeNotListening) std.process.exit(1);
            return err;
        };
        defer client.close();

        try dispatch(allocator, subcmd, sub_args, &client, opts);
    } else {
        // warden-4ga: auto-discover all live runtimes
        const sockets = try discoverSockets(allocator);
        defer freeSocketEntries(allocator, sockets);

        if (sockets.len == 0) {
            const stderr = std.fs.File.stderr();
            try stderr.writeAll("wardenctl: no Warden runtimes found\n");
            try stderr.writeAll("  Start a runtime, or use --socket to specify a path.\n");
            std.process.exit(1);
        }

        // Fan-out commands: query all sockets, print each (with header if >1).
        const is_fanout = std.mem.eql(u8, subcmd, "beams") or
            std.mem.eql(u8, subcmd, "ps") or
            std.mem.eql(u8, subcmd, "topology");

        // warden-xh7: fan-out with merged table (single header, all rows)
        if (is_fanout) {
            var first = true;
            for (sockets) |entry| {
                var client = ControlClient.connect(allocator, entry.socket_path) catch continue;
                defer client.close();
                try dispatchFanout(allocator, subcmd, sub_args, &client, opts, first, entry.beam_id);
                first = false;
            }
            return;
        }

        // Targeted command: route by beam_id encoded in the pid argument.
        const pid_arg = if (sub_args.len > 0) sub_args[0] else null;
        const target_beam = if (pid_arg) |p| beamIdFromPid(p) else null;

        if (sockets.len == 1) {
            var client = ControlClient.connect(allocator, sockets[0].socket_path) catch |err| {
                if (err == error.RuntimeNotListening) std.process.exit(1);
                return err;
            };
            defer client.close();
            try dispatch(allocator, subcmd, sub_args, &client, opts);
        } else if (target_beam) |beam_id| {
            // Find the socket for this beam.
            const entry = for (sockets) |e| {
                if (e.beam_id == beam_id) break e;
            } else {
                const stderr = std.fs.File.stderr();
                const msg = try std.fmt.allocPrint(
                    allocator, "wardenctl: no runtime found for beam {d}\n", .{beam_id});
                defer allocator.free(msg);
                try stderr.writeAll(msg);
                std.process.exit(1);
            };
            var client = ControlClient.connect(allocator, entry.socket_path) catch |err| {
                if (err == error.RuntimeNotListening) std.process.exit(1);
                return err;
            };
            defer client.close();
            try dispatch(allocator, subcmd, sub_args, &client, opts);
        } else {
            const stderr = std.fs.File.stderr();
            try stderr.writeAll("wardenctl: multiple runtimes running — use --socket or include beam id in pid (beam/proc)\n");
            std.process.exit(1);
        }
    }
}

// warden-xh7: fan-out variant — single header on first call, beam label for topology
fn dispatchFanout(
    allocator: std.mem.Allocator,
    subcmd: []const u8,
    sub_args: []const []const u8,
    client: *ControlClient,
    opts: GlobalOpts,
    first: bool,
    beam_id: u32,
) !void {
    if (std.mem.eql(u8, subcmd, "beams")) {
        try beams_cmd.run(allocator, client, opts.json_output, first);
    } else if (std.mem.eql(u8, subcmd, "ps")) {
        const filter = parsePsFilter(sub_args) catch return;
        try ps_cmd.run(allocator, client, filter, opts.json_output, first);
    } else if (std.mem.eql(u8, subcmd, "topology")) {
        const filter = parseTopologyFilter(sub_args) catch return;
        try topology_cmd.run(allocator, client, filter, opts.json_output, beam_id);
    }
}

fn dispatch(
    allocator: std.mem.Allocator,
    subcmd: []const u8,
    sub_args: []const []const u8,
    client: *ControlClient,
    opts: GlobalOpts,
) !void {
    if (std.mem.eql(u8, subcmd, "beams")) {
        try beams_cmd.run(allocator, client, opts.json_output, true);
    } else if (std.mem.eql(u8, subcmd, "ps")) {
        // warden-di6
        const filter = try parsePsFilter(sub_args);
        try ps_cmd.run(allocator, client, filter, opts.json_output, true);
    } else if (std.mem.eql(u8, subcmd, "topology")) {
        // warden-mf3
        const filter = try parseTopologyFilter(sub_args);
        try topology_cmd.run(allocator, client, filter, opts.json_output, null);
    } else if (std.mem.eql(u8, subcmd, "logs")) {
        // warden-9jm
        if (sub_args.len == 0) return usageErr("logs requires a pid argument (beam/proc)");
        var log_opts = logs_cmd.Options{ .pid = sub_args[0] };
        var j: usize = 1;
        while (j < sub_args.len) : (j += 1) {
            const arg = sub_args[j];
            if (std.mem.eql(u8, arg, "--follow") or std.mem.eql(u8, arg, "-f")) {
                log_opts.follow = true;
            } else if (std.mem.eql(u8, arg, "--grep")) {
                j += 1;
                if (j >= sub_args.len) return usageErr("--grep requires a pattern");
                log_opts.grep = sub_args[j];
            } else if (std.mem.eql(u8, arg, "--since")) {
                j += 1;
                if (j >= sub_args.len) return usageErr("--since requires a duration");
                log_opts.since_ms = parseDuration(sub_args[j]) catch
                    return usageErr("--since: invalid duration (use 10s, 5m, 1h)");
            }
        }
        try logs_cmd.run(allocator, client, log_opts, opts.json_output);
    } else if (std.mem.eql(u8, subcmd, "pause") or std.mem.eql(u8, subcmd, "resume")) {
        // warden-aai
        if (sub_args.len == 0) return usageErr("pause/resume requires a pid argument (beam/proc)");
        try control_cmd.run(allocator, client, sub_args[0], subcmd, opts.json_output);
    } else if (std.mem.eql(u8, subcmd, "kill") or std.mem.eql(u8, subcmd, "quarantine")) {
        // warden-h0j
        if (sub_args.len == 0) return usageErr("kill/quarantine requires a pid argument (beam/proc)");
        var force = false;
        var reason: ?[]const u8 = null;
        var j: usize = 1;
        while (j < sub_args.len) : (j += 1) {
            const arg = sub_args[j];
            if (std.mem.eql(u8, arg, "--force") or std.mem.eql(u8, arg, "-f")) {
                force = true;
            } else if (std.mem.eql(u8, arg, "--reason")) {
                j += 1;
                if (j >= sub_args.len) return usageErr("--reason requires a value");
                reason = sub_args[j];
            }
        }
        if (!force) return usageErr("kill/quarantine require --force to confirm");
        try control_cmd.runWithReason(allocator, client, subcmd, sub_args[0], reason, opts.json_output);
    } else if (std.mem.eql(u8, subcmd, "promote")) {
        // warden-h0j
        if (sub_args.len == 0) return usageErr("promote requires a pid argument (beam/proc)");
        var promote_opts = control_cmd.PromoteOpts{};
        var j: usize = 1;
        while (j < sub_args.len) : (j += 1) {
            const arg = sub_args[j];
            if (std.mem.eql(u8, arg, "--class")) {
                j += 1;
                if (j >= sub_args.len) return usageErr("--class requires a value");
                promote_opts.class = sub_args[j];
            } else if (std.mem.eql(u8, arg, "--ttl")) {
                j += 1;
                if (j >= sub_args.len) return usageErr("--ttl requires a duration");
                promote_opts.ttl_ms = parseDuration(sub_args[j]) catch
                    return usageErr("--ttl: invalid duration (use 10s, 5m, 1h)");
            } else if (std.mem.eql(u8, arg, "--reason")) {
                j += 1;
                if (j >= sub_args.len) return usageErr("--reason requires a value");
                promote_opts.reason = sub_args[j];
            }
        }
        try control_cmd.runPromote(allocator, client, sub_args[0], promote_opts, opts.json_output);
    } else {
        return usageErr("unknown subcommand");
    }
}

// warden-xh7
fn parsePsFilter(sub_args: []const []const u8) !ps_cmd.Filter {
    var filter = ps_cmd.Filter{};
    var j: usize = 0;
    while (j < sub_args.len) : (j += 1) {
        const arg = sub_args[j];
        if (std.mem.eql(u8, arg, "--beam")) {
            j += 1;
            if (j >= sub_args.len) return usageErr("--beam requires an id");
            filter.beam = std.fmt.parseInt(u32, sub_args[j], 10) catch
                return usageErr("--beam must be a number");
        } else if (std.mem.eql(u8, arg, "--kind")) {
            j += 1;
            if (j >= sub_args.len) return usageErr("--kind requires a value");
            filter.kind = sub_args[j];
        } else if (std.mem.eql(u8, arg, "--state")) {
            j += 1;
            if (j >= sub_args.len) return usageErr("--state requires a value");
            filter.state = sub_args[j];
        }
    }
    return filter;
}

fn parseTopologyFilter(sub_args: []const []const u8) !topology_cmd.Filter {
    var filter = topology_cmd.Filter{};
    var j: usize = 0;
    while (j < sub_args.len) : (j += 1) {
        const arg = sub_args[j];
        if (std.mem.eql(u8, arg, "--beam")) {
            j += 1;
            if (j >= sub_args.len) return usageErr("--beam requires an id");
            filter.beam = std.fmt.parseInt(u32, sub_args[j], 10) catch
                return usageErr("--beam must be a number");
        }
    }
    return filter;
}

fn parseDuration(s: []const u8) !u64 {
    if (s.len == 0) return error.InvalidDuration;
    const unit = s[s.len - 1];
    const num = try std.fmt.parseInt(u64, s[0 .. s.len - 1], 10);
    return switch (unit) {
        's' => num * 1000,
        'm' => num * 60 * 1000,
        'h' => num * 3600 * 1000,
        else => error.InvalidDuration,
    };
}

fn resolveHome(buf: []u8, path: []const u8) []const u8 {
    if (!std.mem.startsWith(u8, path, "~/")) return path;
    const home = getenv("HOME") orelse return path;
    const result = std.fmt.bufPrint(buf, "{s}{s}", .{ home, path[1..] }) catch return path;
    return result;
}

fn usageErr(msg: []const u8) error{UsageError} {
    const stderr = std.fs.File.stderr();
    stderr.writeAll("wardenctl: ") catch {};
    stderr.writeAll(msg) catch {};
    stderr.writeAll("\nRun 'wardenctl --help' for usage.\n") catch {};
    std.process.exit(1);
}

fn printUsage() void {
    const stderr = std.fs.File.stderr();
    stderr.writeAll(
        \\Usage: wardenctl [--socket <path>] [--json] <command>
        \\
        \\Commands:
        \\  beams          List active beams (all runtimes when no --socket)
        \\  ps             List processes (--beam, --kind, --state)
        \\  topology       Show supervisor tree (--beam)
        \\  logs <pid>     Stream process logs (--since, --grep, --follow)
        \\  pause <pid>          Pause a process
        \\  resume <pid>         Resume a paused process
        \\  kill <pid>           Kill a process (requires --force)
        \\  quarantine <pid>     Restrict process to minimum resources (requires --force)
        \\  promote <pid>        Promote process activity class (--class, --ttl, --reason)
        \\
        \\Global options:
        \\  --socket <path>   Control socket path (default: auto-discover from ~/.warden/sockets/)
        \\  --json            Machine-readable JSON output
        \\  -q, --quiet       Suppress non-essential output
        \\
    ) catch {};
    std.process.exit(0);
}
