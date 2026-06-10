// warden-eet

const std = @import("std");
const sync = @import("sync.zig");
// warden-dmg
const clock = @import("clock.zig");
const env = @import("env.zig");
const beam_mod = @import("beam.zig");
const types = @import("types.zig");

const Runtime = beam_mod.Runtime;
const Ctx = beam_mod.Ctx;
const Pid = types.Pid;
const MessageEnvelope = types.MessageEnvelope;
const MessageKind = types.MessageKind;
const MessagePriority = types.MessagePriority;
const Namespace = beam_mod.Namespace;

// warden-eet
/// Write a length-prefixed JSON frame to a stream.
/// Format: 4-byte big-endian u32 length + UTF-8 JSON payload.
pub fn writeFrameTest(io: std.Io, stream: std.Io.net.Stream, json: []const u8) !void {
    return writeFrame(io, stream, json);
}

/// Read a length-prefixed JSON frame from a stream (exported for tests).
pub fn readFrameTest(io: std.Io, allocator: std.mem.Allocator, stream: std.Io.net.Stream) ![]u8 {
    var rbuf: [4096]u8 = undefined;
    var reader = stream.reader(io, &rbuf);
    return readFrame(allocator, &reader.interface);
}

// warden-3qh: Zig 0.16 — std.Io.net.Stream has no writeAll/readAtLeast; frame I/O
// goes through buffered Stream.Writer/Reader.
fn writeFrame(io: std.Io, stream: std.Io.net.Stream, json: []const u8) !void {
    var hdr: [4]u8 = undefined;
    std.mem.writeInt(u32, &hdr, @intCast(json.len), .big);
    var wbuf: [4096]u8 = undefined;
    var w = stream.writer(io, &wbuf);
    try w.interface.writeAll(&hdr);
    try w.interface.writeAll(json);
    try w.interface.flush();
}

// warden-eet
/// Read a length-prefixed JSON frame from a persistent reader.
/// Caller owns the returned slice. The reader must persist across frames so
/// bytes already pulled off the socket into its buffer are not lost.
fn readFrame(allocator: std.mem.Allocator, r: *std.Io.Reader) ![]u8 {
    var hdr: [4]u8 = undefined;
    r.readSliceAll(&hdr) catch return error.ConnectionClosed;
    const length = std.mem.readInt(u32, &hdr, .big);
    if (length == 0) return error.EmptyFrame;
    const buf = try allocator.alloc(u8, length);
    errdefer allocator.free(buf);
    r.readSliceAll(buf) catch return error.ConnectionClosed;
    return buf;
}

// warden-eet
/// Parse a Namespace from a string.
fn parseNamespace(ns_str: []const u8) !Namespace {
    if (std.mem.eql(u8, ns_str, "proc_temp")) return .proc_temp;
    if (std.mem.eql(u8, ns_str, "proc_cache")) return .proc_cache;
    return error.UnknownNamespace;
}

// warden-eet
/// Send a simple {"kind":"ok"} response.
fn sendOk(io: std.Io, stream: std.Io.net.Stream) !void {
    try writeFrame(io, stream, "{\"kind\":\"ok\"}");
}

// warden-eet
/// Send a {"kind":"error","message":"..."} response.
fn sendError(io: std.Io, stream: std.Io.net.Stream, allocator: std.mem.Allocator, message: []const u8) !void {
    const json = try std.fmt.allocPrint(allocator, "{{\"kind\":\"error\",\"message\":\"{s}\"}}", .{message});
    defer allocator.free(json);
    try writeFrame(io, stream, json);
}

// warden-3cn: serialize a std.json.Value to an anytype writer (std.json.stringify unavailable in 0.15)
pub fn writeJsonValue(w: anytype, val: std.json.Value) !void {
    switch (val) {
        .null => try w.writeAll("null"),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .integer => |i| try w.print("{d}", .{i}),
        .float => |f| try w.print("{d}", .{f}),
        .number_string => |s| try w.writeAll(s),
        .string => |s| {
            try w.writeByte('"');
            for (s) |c| {
                switch (c) {
                    '"' => try w.writeAll("\\\""),
                    '\\' => try w.writeAll("\\\\"),
                    '\n' => try w.writeAll("\\n"),
                    '\r' => try w.writeAll("\\r"),
                    '\t' => try w.writeAll("\\t"),
                    else => try w.writeByte(c),
                }
            }
            try w.writeByte('"');
        },
        .array => |arr| {
            try w.writeByte('[');
            for (arr.items, 0..) |item, i| {
                if (i > 0) try w.writeByte(',');
                try writeJsonValue(w, item);
            }
            try w.writeByte(']');
        },
        .object => |obj| {
            try w.writeByte('{');
            var it = obj.iterator();
            var first = true;
            while (it.next()) |entry| {
                if (!first) try w.writeByte(',');
                first = false;
                try w.writeByte('"');
                try w.writeAll(entry.key_ptr.*);
                try w.writeByte('"');
                try w.writeByte(':');
                try writeJsonValue(w, entry.value_ptr.*);
            }
            try w.writeByte('}');
        },
    }
}

// warden-eet
/// Reader thread context — passed to the spawned thread.
const ReaderCtx = struct {
    bridge: *ForeignBridge,
};

// warden-eet
/// Thread entry: read frames from the worker and dispatch by kind.
fn readerThread(rc: ReaderCtx) void {
    const self = rc.bridge;
    const allocator = self.allocator;

    // warden-3qh: one persistent reader for the connection's lifetime — a fresh
    // reader per frame would drop bytes already buffered off the socket.
    const conn = self.conn orelse return;
    var rbuf: [4096]u8 = undefined;
    var reader = conn.reader(self.runtime.io, &rbuf);

    while (self.running.load(.acquire)) {
        const frame = readFrame(allocator, &reader.interface) catch {
            // EOF or connection closed — exit loop.
            break;
        };
        defer allocator.free(frame);

        // Parse JSON frame.
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            allocator,
            frame,
            .{},
        ) catch continue;
        defer parsed.deinit();

        const root = parsed.value;
        const obj = switch (root) {
            .object => |o| o,
            else => continue,
        };

        const kind_val = obj.get("kind") orelse continue;
        const kind_str = switch (kind_val) {
            .string => |s| s,
            else => continue,
        };

        self.handleFrame(kind_str, obj) catch {
            // Log failure best-effort; continue.
        };
    }
    // warden-dmg: if the loop ended while still "running", the worker died
    // (socket drop / process exit) rather than being deliberately stopped.
    if (self.running.load(.acquire)) self.crashed.store(true, .release);
}

// warden-eet
/// Unix socket bridge between the Warden runtime and a foreign (e.g. Python) worker.
pub const ForeignBridge = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    pid: Pid,
    socket_path: []const u8, // owned
    server: std.Io.net.Server,
    child_proc: ?std.process.Child,
    conn: ?std.Io.net.Stream,
    reader_thread: ?std.Thread,
    running: std.atomic.Value(bool),
    // warden-dmg: set by the reader thread when the loop exits while still
    // running (the worker died, not a deliberate stop). The reaper consumes it.
    crashed: std.atomic.Value(bool),
    // warden-dmg: how the child exited, classified from its Term during teardown.
    last_exit: @import("restart.zig").ExitClass,
    ctx: Ctx,
    write_mutex: sync.Mutex,
    // warden-3cn: arena for reply message strings; freed on deinit
    msg_arena: std.heap.ArenaAllocator,

    // warden-eet
    /// Initialise a ForeignBridge.  Does NOT spawn the child process yet.
    pub fn init(
        allocator: std.mem.Allocator,
        runtime: *Runtime,
        cmd: []const []const u8,
        log_dir: []const u8,
        storage_base: []const u8,
    ) !ForeignBridge {
        return initWithParent(allocator, runtime, cmd, log_dir, storage_base, null);
    }

    // warden-3cn
    pub fn initWithParent(
        allocator: std.mem.Allocator,
        runtime: *Runtime,
        cmd: []const []const u8,
        log_dir: []const u8,
        storage_base: []const u8,
        parent_pid: ?Pid,
    ) !ForeignBridge {
        _ = cmd; // stored in start() via child_proc field — kept here for API symmetry

        // Allocate a PID for the foreign worker.
        const pid = try runtime.registry.spawn(.foreign_worker, parent_pid, .{});
        try runtime.allocMailbox(pid, .{});

        // Build socket path.
        const socket_path = try std.fmt.allocPrint(
            allocator,
            "/tmp/warden-{d}.sock",
            .{pid.proc},
        );
        errdefer allocator.free(socket_path);

        // Bind Unix socket.
        // Delete any stale socket file first.
        std.Io.Dir.cwd().deleteFile(runtime.io, socket_path) catch {};
        const addr = try std.Io.net.UnixAddress.init(socket_path);
        const server = try addr.listen(runtime.io, .{});

        // Build Ctx for the bridge to use when dispatching API calls.
        const ctx = try Ctx.init(runtime, pid, log_dir, storage_base);

        return ForeignBridge{
            .allocator = allocator,
            .runtime = runtime,
            .pid = pid,
            .socket_path = socket_path,
            .server = server,
            .child_proc = null,
            .conn = null,
            .reader_thread = null,
            .running = std.atomic.Value(bool).init(false),
            .crashed = std.atomic.Value(bool).init(false),
            .last_exit = .normal,
            .ctx = ctx,
            .write_mutex = .{},
            .msg_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    // warden-eet
    pub fn deinit(self: *ForeignBridge) void {
        // warden-3qh: stop() shuts down + joins the reader thread before closing
        // the fd — avoids a use-after-close (BADF) panic in the reader.
        self.stop() catch {};
        // Close server.
        self.server.deinit(self.runtime.io);
        // Remove socket file.
        std.Io.Dir.cwd().deleteFile(self.runtime.io, self.socket_path) catch {};
        // Free socket path.
        self.allocator.free(self.socket_path);
        // Deinit ctx.
        self.ctx.deinit();
        // warden-3cn: free all reply message strings allocated in the arena
        self.msg_arena.deinit();
    }

    // warden-eet
    /// Spawn the child process, wait for it to connect, send the handshake,
    /// then start the reader thread.
    pub fn start(self: *ForeignBridge, cmd: []const []const u8) !void {
        // warden-3qh: Zig 0.16 — build child env (current + WARDEN_SOCKET) and
        // spawn via std.process.spawn(io, options).
        var env_map = try env.createMap(self.allocator);
        defer env_map.deinit();
        try env_map.put("WARDEN_SOCKET", self.socket_path);

        const child = try std.process.spawn(self.runtime.io, .{
            .argv = cmd,
            .environ_map = &env_map,
            .stderr = .ignore, // suppress worker exit tracebacks
        });
        self.child_proc = child;

        // Accept the worker connection (blocks until worker connects).
        const accepted = try self.server.accept(self.runtime.io);
        self.conn = accepted;

        // Send handshake frame.
        const handshake = try std.fmt.allocPrint(
            self.allocator,
            "{{\"kind\":\"handshake\",\"pid\":\"{d}/{d}\",\"socket\":\"{s}\"}}",
            .{ self.pid.beam, self.pid.proc, self.socket_path },
        );
        defer self.allocator.free(handshake);
        try writeFrame(self.runtime.io, accepted, handshake);

        // warden-3cn: transition to running so the process can be paused/resumed
        try self.runtime.registry.transition(self.pid, .ready);
        try self.runtime.registry.transition(self.pid, .running);

        // Start reader thread.
        self.running.store(true, .release);
        const rc = ReaderCtx{ .bridge = self };
        self.reader_thread = try std.Thread.spawn(.{}, readerThread, .{rc});
    }

    // warden-eet
    /// Deliver a message envelope to the connected worker via a msg frame.
    pub fn deliver(self: *ForeignBridge, msg: MessageEnvelope) !void {
        const conn = self.conn orelse return error.NotConnected;

        // warden-3cn: serialize body to JSON
        var body_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer body_buf.deinit();
        try writeJsonValue(&body_buf.writer, msg.body);

        const json = try std.fmt.allocPrint(
            self.allocator,
            "{{\"kind\":\"msg\",\"type\":\"{s}\",\"id\":\"{s}\",\"from\":\"{s}\",\"to\":\"{s}\",\"body\":{s}}}",
            .{ msg.@"type", msg.id, msg.from, msg.to, body_buf.writer.buffered() },
        );
        defer self.allocator.free(json);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try writeFrame(self.runtime.io, conn, json);
    }

    // warden-eet
    /// Stop the bridge: signal stop, close connection, join reader thread.
    pub fn stop(self: *ForeignBridge) !void {
        self.running.store(false, .release);
        // warden-3qh: shutdown (not close) to unblock the reader's in-flight read —
        // closing the fd out from under it panics with BADF in 0.16's Io. The fd
        // stays valid; the read returns EOF. Close after the thread has exited.
        if (self.conn) |c| {
            c.shutdown(self.runtime.io, .both) catch {};
        }
        // Join reader thread (now unblocked by the shutdown).
        if (self.reader_thread) |t| {
            t.join();
            self.reader_thread = null;
        }
        // Safe to close now that no thread reads the fd.
        if (self.conn) |c| {
            c.close(self.runtime.io);
            self.conn = null;
        }
        // warden-dmg: classify the exit so the reaper can apply transient policy.
        if (self.child_proc) |*child| {
            if (child.wait(self.runtime.io)) |term| {
                self.last_exit = switch (term) {
                    .exited => |code| if (code == 0) .normal else .abnormal,
                    else => .abnormal, // signal / stopped / unknown
                };
            } else |_| {
                self.last_exit = .abnormal;
            }
            self.child_proc = null;
        }
    }

    // warden-eet
    /// Dispatch a parsed frame by its "kind" field.
    fn handleFrame(self: *ForeignBridge, kind: []const u8, obj: std.json.ObjectMap) !void {
        const conn = self.conn orelse return error.NotConnected;

        if (std.mem.eql(u8, kind, "send")) {
            try self.handleSend(conn, obj);
        } else if (std.mem.eql(u8, kind, "reply")) {
            try self.handleReply(conn, obj);
        } else if (std.mem.eql(u8, kind, "log")) {
            try self.handleLog(conn, obj);
        } else if (std.mem.eql(u8, kind, "fs_write")) {
            try self.handleFsWrite(conn, obj);
        } else if (std.mem.eql(u8, kind, "fs_read")) {
            try self.handleFsRead(conn, obj);
        } else {
            // Unknown kind — log and skip.
            self.ctx.note("unknown bridge frame kind", null) catch {};
        }
    }

    // warden-eet
    fn handleSend(self: *ForeignBridge, conn: std.Io.net.Stream, obj: std.json.ObjectMap) !void {
        const to_str = strField(obj, "to") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing to");
            return;
        };
        const msg_type = strField(obj, "type") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing type");
            return;
        };
        const msg_id = strField(obj, "id") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing id");
            return;
        };

        // Parse destination PID.
        const to_pid = parsePidStr(to_str) catch {
            try sendError(self.runtime.io, conn, self.allocator, "invalid to pid");
            return;
        };

        const from_str = try std.fmt.allocPrint(
            self.allocator,
            "{d}/{d}",
            .{ self.pid.beam, self.pid.proc },
        );
        defer self.allocator.free(from_str);

        const to_pid_str = try self.allocator.dupe(u8, to_str);
        defer self.allocator.free(to_pid_str);

        const type_dup = try self.allocator.dupe(u8, msg_type);
        defer self.allocator.free(type_dup);

        const id_dup = try self.allocator.dupe(u8, msg_id);
        defer self.allocator.free(id_dup);

        const from_dup = try self.allocator.dupe(u8, from_str);
        defer self.allocator.free(from_dup);

        const msg = MessageEnvelope{
            .kind = .request,
            .@"type" = type_dup,
            .id = id_dup,
            .from = from_dup,
            .to = to_pid_str,
            .body = .null,
        };

        try self.ctx.send(to_pid, msg);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try sendOk(self.runtime.io, conn);
    }

    // warden-eet
    fn handleReply(self: *ForeignBridge, conn: std.Io.net.Stream, obj: std.json.ObjectMap) !void {
        const reply_to_str = strField(obj, "reply_to") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing reply_to");
            return;
        };
        const msg_type = strField(obj, "type") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing type");
            return;
        };
        const msg_id = strField(obj, "id") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing id");
            return;
        };
        const corr = strField(obj, "corr");

        const reply_to_pid = parsePidStr(reply_to_str) catch {
            try sendError(self.runtime.io, conn, self.allocator, "invalid reply_to pid");
            return;
        };

        // warden-3cn: all reply message strings are allocated in msg_arena, freed on bridge.deinit()
        const ma = self.msg_arena.allocator();
        const from_str = try std.fmt.allocPrint(ma, "{d}/{d}", .{ self.pid.beam, self.pid.proc });
        const reply_to_dup = try ma.dupe(u8, reply_to_str);
        const type_dup = try ma.dupe(u8, msg_type);
        const id_dup = try ma.dupe(u8, msg_id);
        const from_dup = try ma.dupe(u8, from_str);
        const corr_dup: ?[]const u8 = if (corr) |c| try ma.dupe(u8, c) else null;

        // Serialize body to a string, then re-parse into the arena
        const body_raw = obj.get("body") orelse std.json.Value.null;
        var body_buf: std.Io.Writer.Allocating = .init(self.allocator);
        defer body_buf.deinit();
        try writeJsonValue(&body_buf.writer, body_raw);
        const body_parsed = try std.json.parseFromSlice(std.json.Value, ma, body_buf.writer.buffered(), .{});

        const msg = MessageEnvelope{
            .kind = .response,
            .@"type" = type_dup,
            .id = id_dup,
            .from = from_dup,
            .to = reply_to_dup,
            .corr = corr_dup,
            .body = body_parsed.value,
        };

        try self.ctx.send(reply_to_pid, msg);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try sendOk(self.runtime.io, conn);
    }

    // warden-eet
    fn handleLog(self: *ForeignBridge, conn: std.Io.net.Stream, obj: std.json.ObjectMap) !void {
        const level = strField(obj, "level") orelse "info";
        const message = strField(obj, "message") orelse "";

        try self.ctx.log(level, message, null);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try sendOk(self.runtime.io, conn);
    }

    // warden-eet
    fn handleFsWrite(self: *ForeignBridge, conn: std.Io.net.Stream, obj: std.json.ObjectMap) !void {
        const ns_str = strField(obj, "ns") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing ns");
            return;
        };
        const path = strField(obj, "path") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing path");
            return;
        };
        const data_b64 = strField(obj, "data") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing data");
            return;
        };

        const ns = parseNamespace(ns_str) catch {
            try sendError(self.runtime.io, conn, self.allocator, "unknown namespace");
            return;
        };

        // Decode base64 data.
        const decoded_len = std.base64.standard.Decoder.calcSizeForSlice(data_b64) catch {
            try sendError(self.runtime.io, conn, self.allocator, "invalid base64");
            return;
        };
        const decoded = try self.allocator.alloc(u8, decoded_len);
        defer self.allocator.free(decoded);
        std.base64.standard.Decoder.decode(decoded, data_b64) catch {
            try sendError(self.runtime.io, conn, self.allocator, "base64 decode failed");
            return;
        };

        self.ctx.fsWrite(ns, path, decoded) catch |err| {
            const msg = @errorName(err);
            try sendError(self.runtime.io, conn, self.allocator, msg);
            return;
        };

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try sendOk(self.runtime.io, conn);
    }

    // warden-eet
    fn handleFsRead(self: *ForeignBridge, conn: std.Io.net.Stream, obj: std.json.ObjectMap) !void {
        const ns_str = strField(obj, "ns") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing ns");
            return;
        };
        const path = strField(obj, "path") orelse {
            try sendError(self.runtime.io, conn, self.allocator, "missing path");
            return;
        };

        const ns = parseNamespace(ns_str) catch {
            try sendError(self.runtime.io, conn, self.allocator, "unknown namespace");
            return;
        };

        const data = self.ctx.fsRead(ns, path) catch |err| {
            const msg = @errorName(err);
            try sendError(self.runtime.io, conn, self.allocator, msg);
            return;
        };
        defer self.allocator.free(data);

        // Encode to base64.
        const enc_len = std.base64.standard.Encoder.calcSize(data.len);
        const encoded = try self.allocator.alloc(u8, enc_len);
        defer self.allocator.free(encoded);
        _ = std.base64.standard.Encoder.encode(encoded, data);

        const response = try std.fmt.allocPrint(
            self.allocator,
            "{{\"kind\":\"ok\",\"data\":\"{s}\"}}",
            .{encoded},
        );
        defer self.allocator.free(response);

        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try writeFrame(self.runtime.io, conn, response);
    }
};

// warden-eet
/// Parse a "beam/proc" PID string.
pub fn parsePidStr(s: []const u8) !Pid {
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse return error.InvalidPid;
    const beam = std.fmt.parseInt(u32, s[0..slash], 10) catch return error.InvalidPid;
    const proc = std.fmt.parseInt(u64, s[slash + 1 ..], 10) catch return error.InvalidPid;
    return Pid{ .beam = beam, .proc = proc };
}

// warden-eet
/// Extract a string field from a JSON object map.
fn strField(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const val = obj.get(key) orelse return null;
    return switch (val) {
        .string => |s| s,
        else => null,
    };
}

// warden-dmg
const restart_mod = @import("restart.zig");

// warden-dmg
/// A foreign worker under supervision: its live bridge plus everything needed
/// to respawn it after a crash, and its per-worker runaway-guard state.
pub const ManagedWorker = struct {
    bridge: *ForeignBridge,
    // Retained respawn inputs (deep-owned copies).
    cmd: [][]u8,
    log_dir: []u8,
    storage_base: []u8,
    parent_pid: ?Pid,
    strategy: restart_mod.Strategy,
    // Runaway guard (per worker).
    restart_timestamps: std.ArrayList(i64),
    restart_count: u32,
    retired: bool,

    fn freeOwned(self: *ManagedWorker, allocator: std.mem.Allocator) void {
        for (self.cmd) |a| allocator.free(a);
        allocator.free(self.cmd);
        allocator.free(self.log_dir);
        allocator.free(self.storage_base);
        self.restart_timestamps.deinit(allocator);
    }
};

// warden-dmg: deep-copy an argv into owned memory.
fn dupeCmd(allocator: std.mem.Allocator, cmd: []const []const u8) ![][]u8 {
    const out = try allocator.alloc([]u8, cmd.len);
    var n: usize = 0;
    errdefer {
        for (out[0..n]) |a| allocator.free(a);
        allocator.free(out);
    }
    for (cmd) |a| {
        out[n] = try allocator.dupe(u8, a);
        n += 1;
    }
    return out;
}

// warden-eet
/// Supervisor that manages a pool of ForeignBridge instances.
pub const BridgeSupervisor = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    workers: std.ArrayList(*ManagedWorker),
    // warden-dmg: reaper cadence (ms), adjustable via renice().
    reaper_interval_ms: std.atomic.Value(u64),
    reaper_stopping: std.atomic.Value(bool),
    reaper_thread: ?std.Thread,
    // warden-47g: grace before a terminal registry entry + its mailbox are
    // reclaimed by the reaper sweep. Plain field (set at init; tests may lower it).
    reclaim_grace_ms: u64 = 30_000,
    // warden-dmg: protects `workers` and per-worker `bridge` swaps against the
    // reaper thread. The reader thread never takes it (no deadlock on join).
    mutex: sync.Mutex,

    // warden-eet
    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime) BridgeSupervisor {
        return BridgeSupervisor{
            .allocator = allocator,
            .runtime = runtime,
            // warden-dmg
            .workers = .empty,
            .reaper_interval_ms = std.atomic.Value(u64).init(restart_mod.default_interval_ms),
            .reaper_stopping = std.atomic.Value(bool).init(false),
            .reaper_thread = null,
            // warden-47g
            .reclaim_grace_ms = 30_000,
            // warden-dmg
            .mutex = .{},
        };
    }

    // warden-eet, warden-dmg
    pub fn deinit(self: *BridgeSupervisor) void {
        // warden-dmg: stop the reaper first so it never races teardown.
        self.reaper_stopping.store(true, .release);
        if (self.reaper_thread) |t| {
            t.join();
            self.reaper_thread = null;
        }
        for (self.workers.items) |w| {
            if (!w.retired) {
                w.bridge.deinit();
                self.allocator.destroy(w.bridge);
            }
            w.freeOwned(self.allocator);
            self.allocator.destroy(w);
        }
        self.workers.deinit(self.allocator);
    }

    // warden-eet
    /// Spawn a new foreign worker and return its PID.
    pub fn spawnWorker(
        self: *BridgeSupervisor,
        cmd: []const []const u8,
        log_dir: []const u8,
        storage_base: []const u8,
    ) !Pid {
        return self.spawnWorkerUnder(cmd, log_dir, storage_base, null, .permanent);
    }

    // warden-3cn, warden-dmg
    /// Spawn a new foreign worker as a child of parent_pid, return its PID.
    pub fn spawnWorkerUnder(
        self: *BridgeSupervisor,
        cmd: []const []const u8,
        log_dir: []const u8,
        storage_base: []const u8,
        parent_pid: ?Pid,
        strategy: restart_mod.Strategy,
    ) !Pid {
        const w = try self.allocator.create(ManagedWorker);
        errdefer self.allocator.destroy(w);

        const cmd_owned = try dupeCmd(self.allocator, cmd);
        errdefer {
            for (cmd_owned) |a| self.allocator.free(a);
            self.allocator.free(cmd_owned);
        }
        const log_owned = try self.allocator.dupe(u8, log_dir);
        errdefer self.allocator.free(log_owned);
        const store_owned = try self.allocator.dupe(u8, storage_base);
        errdefer self.allocator.free(store_owned);

        const bridge = try self.allocator.create(ForeignBridge);
        errdefer self.allocator.destroy(bridge);
        bridge.* = try ForeignBridge.initWithParent(self.allocator, self.runtime, cmd, log_dir, storage_base, parent_pid);
        errdefer bridge.deinit();
        try bridge.start(cmd);

        w.* = .{
            .bridge = bridge,
            .cmd = cmd_owned,
            .log_dir = log_owned,
            .storage_base = store_owned,
            .parent_pid = parent_pid,
            .strategy = strategy,
            .restart_timestamps = .empty,
            .restart_count = 0,
            .retired = false,
        };
        // warden-dmg
        self.mutex.lock();
        self.workers.append(self.allocator, w) catch |e| {
            self.mutex.unlock();
            return e;
        };
        self.mutex.unlock();
        return bridge.pid;
    }

    // warden-dmg: deliver a message to a foreign worker under the lock, so the
    // reaper cannot free the bridge between lookup and use. Returns true if a
    // live (non-retired) worker for `pid` was found.
    pub fn deliver(self: *BridgeSupervisor, pid: Pid, msg: MessageEnvelope) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.workers.items) |w| {
            if (w.retired) continue;
            if (w.bridge.pid.beam == pid.beam and w.bridge.pid.proc == pid.proc) {
                try w.bridge.deliver(msg);
                return true;
            }
        }
        return false;
    }

    // warden-dmg: adjust the reaper poll interval on the fly; returns the
    // clamped value actually applied.
    pub fn renice(self: *BridgeSupervisor, interval_ms: u64) u64 {
        const v = restart_mod.clampInterval(interval_ms);
        self.reaper_interval_ms.store(v, .release);
        return v;
    }

    // warden-dmg: start the reaper. Call once after the supervisor is at its
    // final (stable) address.
    pub fn startReaper(self: *BridgeSupervisor) !void {
        self.reaper_thread = try std.Thread.spawn(.{}, reaperLoop, .{self});
    }

    // warden-dmg: background loop — poll workers, respawn crashed ones.
    fn reaperLoop(self: *BridgeSupervisor) void {
        while (!self.reaper_stopping.load(.acquire)) {
            const iv = self.reaper_interval_ms.load(.acquire);
            clock.sleepNs(iv * std.time.ns_per_ms);
            if (self.reaper_stopping.load(.acquire)) break;
            {
                self.mutex.lock();
                defer self.mutex.unlock();
                for (self.workers.items) |w| {
                    if (w.retired) continue;
                    if (!w.bridge.crashed.load(.acquire)) continue;
                    self.reapAndMaybeRespawn(w) catch {};
                }
            }
            // warden-47g: reclaim terminal registry entries + mailboxes. Done
            // without the supervisor lock — it touches the runtime registry/
            // mailboxes, not the worker list.
            self.runtime.reclaimTerminal(self.reclaim_grace_ms, clock.nowMs());
        }
    }

    // warden-dmg: tear down a crashed worker and respawn or retire per policy.
    // MUST be called with self.mutex held (reaperLoop holds it).
    fn reapAndMaybeRespawn(self: *BridgeSupervisor, w: *ManagedWorker) !void {
        const old = w.bridge;
        const old_pid = old.pid;

        // Reap the dead incarnation (shutdown -> join reader -> close -> wait),
        // which sets old.last_exit. stop() is idempotent and null-guarded.
        old.stop() catch {};
        const exit_class = old.last_exit;

        const now = clock.nowMs();
        const decision = try restart_mod.decide(
            w.strategy, exit_class, &w.restart_timestamps, self.allocator, now,
        );

        if (decision == .retire) {
            old.ctx.logger.note("info", "worker retired — not restarting", null) catch {};
            self.runtime.registry.transition(old_pid, .exiting) catch {};
            old.deinit();
            self.allocator.destroy(old);
            w.bridge = undefined; // never read again: w.retired gates it
            w.retired = true;
            return;
        }

        // Restart: tear down old, mark its registry entry exiting, spawn a new
        // incarnation with a fresh PID.
        self.runtime.registry.transition(old_pid, .exiting) catch {};
        old.deinit();
        self.allocator.destroy(old);

        w.bridge = undefined; // old is freed; must be reassigned before any read

        const nb = self.allocator.create(ForeignBridge) catch {
            w.retired = true; // cannot respawn — retire so the reaper skips it
            return;
        };
        nb.* = ForeignBridge.initWithParent(
            self.allocator, self.runtime, w.cmd, w.log_dir, w.storage_base, w.parent_pid,
        ) catch {
            self.allocator.destroy(nb);
            w.retired = true;
            return;
        };
        nb.start(w.cmd) catch {
            nb.deinit();
            self.allocator.destroy(nb);
            w.retired = true;
            return;
        };

        w.bridge = nb;
        w.restart_count += 1;
        nb.ctx.logger.emit(.{ .restart = .{ .attempt = w.restart_count, .reason = "crash" } }, null) catch {};
    }
};
