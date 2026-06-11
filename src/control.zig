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

const Runtime = beam_mod.Runtime;

// warden-7zc: transport primitives now live in control/transport.zig. Re-export
// the frame I/O (external API used by main.zig + the test files) and alias the
// internal helpers so handler bodies need no changes.
const transport = @import("control/transport.zig");
pub const writeFrame = transport.writeFrame;
pub const readFrame = transport.readFrame;
const Responder = transport.Responder;

// warden-r28: per-domain RPC handler modules, dispatched via the table below.
const handlers_beam = @import("control/handlers_beam.zig");
const handlers_proc = @import("control/handlers_proc.zig");
const handlers_control = @import("control/handlers_control.zig");
const handlers_logs = @import("control/handlers_logs.zig");
const handlers_topology = @import("control/handlers_topology.zig");

// warden-0i6
// HandlerCtx bundles everything an RPC handler needs into one value so every
// handler shares the signature `fn(*const HandlerCtx) !void` — the precondition
// for the action->handler dispatch table (warden-r28). Built once per connection.
pub const HandlerCtx = struct {
    cs: *ControlServer,
    allocator: std.mem.Allocator,
    req_id: []const u8,
    stream: std.Io.net.Stream,
    payload: ?std.json.Value,
    r: Responder,
};

// warden-39v
// warden-r28: action -> handler dispatch table. Adding an RPC means adding one
// row here plus the handler in its domain module — handleConnection is untouched.
const DispatchEntry = struct {
    action: []const u8,
    handler: *const fn (*const HandlerCtx) anyerror!void,
};
const dispatch_table = [_]DispatchEntry{
    .{ .action = "beam.create", .handler = handlers_beam.handleBeamCreate },
    .{ .action = "beam.reaper", .handler = handlers_beam.handleBeamReaper },
    .{ .action = "beam.list", .handler = handlers_beam.handleBeamList },
    .{ .action = "proc.spawn", .handler = handlers_proc.handleProcSpawn },
    .{ .action = "proc.send", .handler = handlers_proc.handleProcSend },
    .{ .action = "proc.call", .handler = handlers_proc.handleProcCall },
    .{ .action = "proc.list", .handler = handlers_proc.handleProcList },
    .{ .action = "proc.control", .handler = handlers_control.handleProcControl },
    .{ .action = "policy.events", .handler = handlers_control.handlePolicyEvents },
    .{ .action = "topology.get", .handler = handlers_topology.handleTopologyGet },
    .{ .action = "logs.stream", .handler = handlers_logs.handleLogsStream },
};

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

    // warden-0i6: build the per-request handler context once, then dispatch.
    const h = HandlerCtx{
        .cs = cs,
        .allocator = allocator,
        .req_id = req_id,
        .stream = stream,
        .payload = obj.get("payload"),
        .r = .{ .io = cs.runtime.io, .allocator = allocator, .stream = stream, .req_id = req_id },
    };

    for (dispatch_table) |entry| {
        if (std.mem.eql(u8, action, entry.action)) return entry.handler(&h);
    }

    // No row matched: unknown action.
    const m = try std.fmt.allocPrint(allocator, "unknown action: {s}", .{action});
    defer allocator.free(m);
    try h.r.err(m);
}

// warden-39v
fn serverThread(cs: *ControlServer) void {
    while (!cs.stopping.load(.acquire)) {
        const stream = cs.server.accept(cs.runtime.io) catch break;
        defer stream.close(cs.runtime.io);
        handleConnection(cs, stream) catch {};
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
