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

// warden-39v
pub const GlobalOpts = struct {
    socket_path: []const u8,
    json_output: bool,
    quiet: bool,
};

const default_socket = "~/.warden/ctrl.sock";

// warden-39v
pub fn run(allocator: std.mem.Allocator, args: []const []const u8) !void {
    var opts = GlobalOpts{
        .socket_path = std.posix.getenv("WARDEN_CTRL_SOCKET") orelse default_socket,
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

    // Expand ~/ in socket path.
    var resolved_buf: [std.fs.max_path_bytes]u8 = undefined;
    const socket_path = resolveHome(&resolved_buf, opts.socket_path);

    var client = ControlClient.connect(allocator, socket_path) catch |err| {
        if (err == error.RuntimeNotListening) std.process.exit(1);
        return err;
    };
    defer client.close();

    if (std.mem.eql(u8, subcmd, "beams")) {
        try beams_cmd.run(allocator, &client, opts.json_output);
    } else if (std.mem.eql(u8, subcmd, "ps")) {
        // warden-di6
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
        try ps_cmd.run(allocator, &client, filter, opts.json_output);
    } else if (std.mem.eql(u8, subcmd, "topology")) {
        // warden-mf3
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
        try topology_cmd.run(allocator, &client, filter, opts.json_output);
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
        try logs_cmd.run(allocator, &client, log_opts, opts.json_output);
    } else {
        return usageErr("unknown subcommand");
    }
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
    const home = std.posix.getenv("HOME") orelse return path;
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
        \\  beams       List active beams
        \\  ps          List processes (--beam, --kind, --state)
        \\  topology    Show supervisor tree (--beam)
        \\  logs <pid>  Stream process logs (--since, --grep, --follow)
        \\
        \\Global options:
        \\  --socket <path>   Control socket path (default: $WARDEN_CTRL_SOCKET or ~/.warden/ctrl.sock)
        \\  --json            Machine-readable JSON output
        \\  -q, --quiet       Suppress non-essential output
        \\
    ) catch {};
    std.process.exit(0);
}
