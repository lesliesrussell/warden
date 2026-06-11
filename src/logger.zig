// warden-554

const std = @import("std");
const clock = @import("clock.zig");

// warden-554
/// All runtime-emitted event types and process-authored event types.
pub const LogEvent = union(enum) {
    // Runtime-emitted events
    spawn: SpawnEvent,
    state_change: StateChangeEvent,
    enqueue: EnqueueEvent,
    recv: RecvEvent,
    send: SendEvent,
    reply: ReplyEvent,
    timeout: TimeoutEvent,
    monitor: MonitorEvent,
    down: DownEvent,
    policy_change: PolicyChangeEvent,
    mailbox_overflow: MailboxOverflowEvent,
    storage_write: StorageWriteEvent,
    storage_evict: StorageEvictEvent,
    restart: RestartEvent,
    exit: ExitEvent,
    // Process-authored events
    note: NoteEvent,
    metric: MetricEvent,
    decision: DecisionEvent,
    tool_call: ToolCallEvent,
    tool_result: ToolResultEvent,
    warning: WarningEvent,
    error_event: ErrorEvent,

    /// Returns the string tag name for this event variant.
    pub fn tag(self: LogEvent) []const u8 {
        return switch (self) {
            .spawn => "spawn",
            .state_change => "state_change",
            .enqueue => "enqueue",
            .recv => "recv",
            .send => "send",
            .reply => "reply",
            .timeout => "timeout",
            .monitor => "monitor",
            .down => "down",
            .policy_change => "policy_change",
            .mailbox_overflow => "mailbox_overflow",
            .storage_write => "storage_write",
            .storage_evict => "storage_evict",
            .restart => "restart",
            .exit => "exit",
            .note => "note",
            .metric => "metric",
            .decision => "decision",
            .tool_call => "tool_call",
            .tool_result => "tool_result",
            .warning => "warning",
            .error_event => "error_event",
        };
    }
};

// warden-554
pub const SpawnEvent = struct {
    kind: ?[]const u8 = null,
    parent_pid: ?u64 = null,
};

pub const StateChangeEvent = struct {
    from: []const u8,
    to: []const u8,
};

pub const EnqueueEvent = struct {
    msg_id: []const u8,
    from: []const u8,
    msg_type: []const u8,
    priority: ?[]const u8 = null,
    corr: ?[]const u8 = null,
    trace_id: ?[]const u8 = null,
    task_id: ?[]const u8 = null,
};

pub const RecvEvent = struct {
    msg_id: []const u8,
    from: []const u8,
    msg_type: []const u8,
    corr: ?[]const u8 = null,
    trace_id: ?[]const u8 = null,
    task_id: ?[]const u8 = null,
};

pub const SendEvent = struct {
    msg_id: []const u8,
    to: []const u8,
    msg_type: []const u8,
    corr: ?[]const u8 = null,
    trace_id: ?[]const u8 = null,
    task_id: ?[]const u8 = null,
};

pub const ReplyEvent = struct {
    msg_id: []const u8,
    to: []const u8,
    corr: ?[]const u8 = null,
    trace_id: ?[]const u8 = null,
};

pub const TimeoutEvent = struct {
    after_ms: u64,
    corr: ?[]const u8 = null,
    trace_id: ?[]const u8 = null,
};

pub const MonitorEvent = struct {
    target_pid: u64,
    target_beam: u32,
};

pub const DownEvent = struct {
    monitored_pid: u64,
    monitored_beam: u32,
    reason: ?[]const u8 = null,
};

pub const PolicyChangeEvent = struct {
    field: []const u8,
    old_value: ?[]const u8 = null,
    new_value: ?[]const u8 = null,
};

pub const MailboxOverflowEvent = struct {
    strategy: []const u8,
    mailbox_len: u64,
    msg_id: ?[]const u8 = null,
};

pub const StorageWriteEvent = struct {
    key: []const u8,
    bytes: u64,
};

pub const StorageEvictEvent = struct {
    key: []const u8,
    bytes: u64,
};

pub const RestartEvent = struct {
    attempt: u32,
    reason: ?[]const u8 = null,
};

pub const ExitEvent = struct {
    reason: ?[]const u8 = null,
    code: ?i32 = null,
};

pub const NoteEvent = struct {
    level: []const u8 = "info",
    message: []const u8,
};

pub const MetricEvent = struct {
    name: []const u8,
    value: f64,
    unit: ?[]const u8 = null,
};

pub const DecisionEvent = struct {
    message: []const u8,
    level: []const u8 = "info",
};

pub const ToolCallEvent = struct {
    tool: []const u8,
    input: ?[]const u8 = null,
    corr: ?[]const u8 = null,
};

pub const ToolResultEvent = struct {
    tool: []const u8,
    success: bool,
    corr: ?[]const u8 = null,
};

pub const WarningEvent = struct {
    message: []const u8,
};

pub const ErrorEvent = struct {
    message: []const u8,
    code: ?[]const u8 = null,
};

// warden-554
/// A fully-formed log record as written to disk.
/// Optional string fields are nullable slices; absent → not emitted in JSON.
pub const LogRecord = struct {
    ts: i64,
    beam: u32,
    pid: u64,
    seq: u64,
    event: []const u8,
    // Optional common fields
    level: ?[]const u8 = null,
    msg_id: ?[]const u8 = null,
    corr: ?[]const u8 = null,
    trace_id: ?[]const u8 = null,
    session_id: ?[]const u8 = null,
    task_id: ?[]const u8 = null,
};

// warden-554
/// Per-process append-only NDJSON log writer.
/// Each process gets its own file named `<beam>-<pid>.log`.
/// Uses Zig 0.15 std.fs.File.Writer with an embedded buffer.
///
/// IMPORTANT: `ProcessLogger` must not be moved after `init` returns — the
/// internal write buffer is referenced by address. Always store behind a
/// pointer or in a stable location (e.g. heap-allocated).
pub const ProcessLogger = struct {
    // warden-ga2: size-based rotation defaults. When the active log reaches
    // `rotate_at` bytes it is rolled to `<name>.log.1` (older rotations shift up
    // to `max_rotations`, oldest dropped), bounding per-process disk to roughly
    // (max_rotations + 1) * rotate_at. Tests lower `rotate_at`.
    pub const default_rotate_bytes: u64 = 1 << 20; // 1 MiB
    pub const max_rotations: usize = 3;

    allocator: std.mem.Allocator,
    beam_id: u32,
    pid: u64,
    // warden-lmm: Zig 0.16 — File I/O routes through std.Io; the writer caches io.
    io: std.Io,
    // warden-ga2: directory the log lives in, needed to rename on rotation.
    dir: std.Io.Dir,
    file: std.Io.File,
    buf: [4096]u8,
    file_writer: std.Io.File.Writer,
    seq: u64,
    // warden-ga2: rotate the active log once it reaches this many bytes.
    rotate_at: u64,

    // warden-554
    /// Initialise in-place.  Caller must ensure the returned value is never
    /// copied or moved (the writer holds a pointer into `self.buf`).
    /// Initialise a heap-allocated ProcessLogger in-place.
    /// Call on an already-allocated *ProcessLogger — never on a local var,
    /// because file_writer holds &self.buf which must not move after init.
    pub fn initInPlace(
        self: *ProcessLogger,
        io: std.Io,
        allocator: std.mem.Allocator,
        beam_id: u32,
        pid: u64,
        log_dir: std.Io.Dir,
    ) !void {
        const filename = try std.fmt.allocPrint(allocator, "{d}-{d}.log", .{ beam_id, pid });
        defer allocator.free(filename);

        const file = try log_dir.createFile(io, filename, .{
            .truncate = false,
            .exclusive = false,
        });
        errdefer file.close(io);

        // warden-lmm: 0.16 File has no seekFromEnd; use a positional writer and
        // start at end-of-file so the log stays append-only across restarts.
        const st = try file.stat(io);

        self.io = io;
        self.allocator = allocator;
        self.beam_id = beam_id;
        self.pid = pid;
        self.dir = log_dir;
        self.file = file;
        self.buf = undefined;
        self.seq = 0;
        self.rotate_at = default_rotate_bytes;
        // file_writer stores &self.buf — must be set after self is stable.
        self.file_writer = std.Io.File.Writer.init(file, io, &self.buf);
        self.file_writer.pos = st.size;
    }

    // Keep the old init for callers that rely on RLS (single-assignment, no errdefer).
    pub fn init(
        io: std.Io,
        allocator: std.mem.Allocator,
        beam_id: u32,
        pid: u64,
        log_dir: std.Io.Dir,
    ) !ProcessLogger {
        const filename = try std.fmt.allocPrint(allocator, "{d}-{d}.log", .{ beam_id, pid });
        defer allocator.free(filename);

        const file = try log_dir.createFile(io, filename, .{
            .truncate = false,
            .exclusive = false,
        });
        errdefer file.close(io);

        const st = try file.stat(io);

        // RLS places `self` at the caller's result location, so &self.buf is
        // stable. Safe ONLY when the caller does `var x: ProcessLogger = try init(...)`.
        var self: ProcessLogger = .{
            .io = io,
            .allocator = allocator,
            .beam_id = beam_id,
            .pid = pid,
            .dir = log_dir,
            .file = file,
            .buf = undefined,
            .file_writer = undefined,
            .seq = 0,
            .rotate_at = default_rotate_bytes,
        };
        self.file_writer = std.Io.File.Writer.init(file, io, &self.buf);
        self.file_writer.pos = st.size;
        return self;
    }

    // warden-554
    pub fn deinit(self: *ProcessLogger) void {
        self.file_writer.interface.flush() catch {};
        self.file.close(self.io);
    }

    // warden-554
    /// Flush buffered bytes to the OS.
    pub fn flush(self: *ProcessLogger) !void {
        try self.file_writer.interface.flush();
    }

    // warden-554
    /// Write one NDJSON record for `event`.
    /// `extra` is an optional `std.json.ObjectMap` of additional fields to
    /// append; values are serialised according to their `std.json.Value` type.
    pub fn emit(self: *ProcessLogger, event: LogEvent, extra: ?std.json.ObjectMap) !void {
        self.seq += 1;
        const ts = clock.nowMs();
        const w = &self.file_writer.interface;

        // Mandatory fields
        try w.print(
            "{{\"ts\":{d},\"beam\":{d},\"pid\":{d},\"seq\":{d},\"event\":\"{s}\"",
            .{ ts, self.beam_id, self.pid, self.seq, event.tag() },
        );

        // Event-specific fields
        try writeEventFields(w, event);

        // Caller-supplied extra fields (typed JSON values)
        if (extra) |map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try w.writeByte(',');
                try writeJsonString(w, entry.key_ptr.*);
                try w.writeByte(':');
                try writeJsonValue(w, entry.value_ptr.*);
            }
        }

        try w.writeAll("}\n");

        // warden-ga2: roll the log once it grows past the threshold. Count both
        // flushed bytes (writer.pos) and buffered-but-unflushed bytes
        // (interface.end), since small records sit in the 4 KiB buffer. Best-
        // effort: a rotation failure must not break logging.
        if (self.file_writer.pos + self.file_writer.interface.end >= self.rotate_at) self.rotate() catch {};
    }

    // warden-ga2
    /// Roll the active log to `<name>.log.1`, shifting older rotations up to
    /// `max_rotations` (oldest dropped), then reopen a fresh active log.
    /// Flushes first so the rolled file is complete.
    fn rotate(self: *ProcessLogger) !void {
        self.file_writer.interface.flush() catch {};
        self.file.close(self.io);

        var a: [64]u8 = undefined;
        var b: [64]u8 = undefined;

        // Drop the oldest rotation.
        const oldest = std.fmt.bufPrint(&a, "{d}-{d}.log.{d}", .{ self.beam_id, self.pid, max_rotations }) catch unreachable;
        self.dir.deleteFile(self.io, oldest) catch {};

        // Shift remaining rotations up: .log.(i-1) -> .log.i.
        var i: usize = max_rotations;
        while (i > 1) : (i -= 1) {
            const from = std.fmt.bufPrint(&a, "{d}-{d}.log.{d}", .{ self.beam_id, self.pid, i - 1 }) catch unreachable;
            const to = std.fmt.bufPrint(&b, "{d}-{d}.log.{d}", .{ self.beam_id, self.pid, i }) catch unreachable;
            self.dir.rename(from, self.dir, to, self.io) catch {};
        }

        // Active .log -> .log.1.
        const cur = std.fmt.bufPrint(&a, "{d}-{d}.log", .{ self.beam_id, self.pid }) catch unreachable;
        const one = std.fmt.bufPrint(&b, "{d}-{d}.log.1", .{ self.beam_id, self.pid }) catch unreachable;
        self.dir.rename(cur, self.dir, one, self.io) catch {};

        // Reopen a fresh active log and re-point the writer (self is stable, so
        // &self.buf is still valid).
        const name = std.fmt.bufPrint(&a, "{d}-{d}.log", .{ self.beam_id, self.pid }) catch unreachable;
        const file = try self.dir.createFile(self.io, name, .{ .truncate = true, .exclusive = false });
        self.file = file;
        self.file_writer = std.Io.File.Writer.init(file, self.io, &self.buf);
        self.file_writer.pos = 0;
    }

    // warden-554
    /// Emit a `note` event.
    pub fn note(
        self: *ProcessLogger,
        level: []const u8,
        message: []const u8,
        extra: ?std.json.ObjectMap,
    ) !void {
        try self.emit(.{ .note = .{ .level = level, .message = message } }, extra);
    }

    // warden-554
    /// Emit a `metric` event.
    pub fn metric(
        self: *ProcessLogger,
        name: []const u8,
        value: f64,
        extra: ?std.json.ObjectMap,
    ) !void {
        try self.emit(.{ .metric = .{ .name = name, .value = value } }, extra);
    }

    // warden-554
    /// Emit a `warning` event.
    pub fn warn(
        self: *ProcessLogger,
        message: []const u8,
        extra: ?std.json.ObjectMap,
    ) !void {
        try self.emit(.{ .warning = .{ .message = message } }, extra);
    }

    // warden-554
    /// Emit an `error_event` event.
    pub fn err(
        self: *ProcessLogger,
        message: []const u8,
        extra: ?std.json.ObjectMap,
    ) !void {
        try self.emit(.{ .error_event = .{ .message = message } }, extra);
    }
};

// warden-554
/// Serialise a `std.json.Value` to the writer.
fn writeJsonValue(w: anytype, val: std.json.Value) !void {
    switch (val) {
        .null => try w.writeAll("null"),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .integer => |i| try w.print("{d}", .{i}),
        .float => |f| try w.print("{d}", .{f}),
        .number_string => |s| try w.writeAll(s),
        .string => |s| try writeJsonString(w, s),
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
                try writeJsonString(w, entry.key_ptr.*);
                try w.writeByte(':');
                try writeJsonValue(w, entry.value_ptr.*);
            }
            try w.writeByte('}');
        },
    }
}

// warden-554
/// Write a JSON-escaped string including surrounding quotes.
fn writeJsonString(w: anytype, s: []const u8) !void {
    try w.writeByte('"');
    for (s) |c| {
        switch (c) {
            '"' => try w.writeAll("\\\""),
            '\\' => try w.writeAll("\\\\"),
            '\n' => try w.writeAll("\\n"),
            '\r' => try w.writeAll("\\r"),
            '\t' => try w.writeAll("\\t"),
            0x00...0x08, 0x0b...0x0c, 0x0e...0x1f => try w.print("\\u{x:0>4}", .{c}),
            else => try w.writeByte(c),
        }
    }
    try w.writeByte('"');
}

// warden-554
/// Write an optional string field: ,"key":"value"
fn writeOptStr(w: anytype, key: []const u8, val: ?[]const u8) !void {
    if (val) |v| {
        try w.writeByte(',');
        try writeJsonString(w, key);
        try w.writeByte(':');
        try writeJsonString(w, v);
    }
}

// warden-554
/// Dispatch event-specific field writing.
fn writeEventFields(w: anytype, event: LogEvent) !void {
    switch (event) {
        .spawn => |e| {
            try writeOptStr(w, "kind", e.kind);
            if (e.parent_pid) |pp| try w.print(",\"parent_pid\":{d}", .{pp});
        },
        .state_change => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "from");
            try w.writeByte(':');
            try writeJsonString(w, e.from);
            try w.writeByte(',');
            try writeJsonString(w, "to");
            try w.writeByte(':');
            try writeJsonString(w, e.to);
        },
        .enqueue => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "msg_id");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_id);
            try w.writeByte(',');
            try writeJsonString(w, "from");
            try w.writeByte(':');
            try writeJsonString(w, e.from);
            try w.writeByte(',');
            try writeJsonString(w, "msg_type");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_type);
            try writeOptStr(w, "priority", e.priority);
            try writeOptStr(w, "corr", e.corr);
            try writeOptStr(w, "trace_id", e.trace_id);
            try writeOptStr(w, "task_id", e.task_id);
        },
        .recv => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "msg_id");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_id);
            try w.writeByte(',');
            try writeJsonString(w, "from");
            try w.writeByte(':');
            try writeJsonString(w, e.from);
            try w.writeByte(',');
            try writeJsonString(w, "msg_type");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_type);
            try writeOptStr(w, "corr", e.corr);
            try writeOptStr(w, "trace_id", e.trace_id);
            try writeOptStr(w, "task_id", e.task_id);
        },
        .send => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "msg_id");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_id);
            try w.writeByte(',');
            try writeJsonString(w, "to");
            try w.writeByte(':');
            try writeJsonString(w, e.to);
            try w.writeByte(',');
            try writeJsonString(w, "msg_type");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_type);
            try writeOptStr(w, "corr", e.corr);
            try writeOptStr(w, "trace_id", e.trace_id);
            try writeOptStr(w, "task_id", e.task_id);
        },
        .reply => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "msg_id");
            try w.writeByte(':');
            try writeJsonString(w, e.msg_id);
            try w.writeByte(',');
            try writeJsonString(w, "to");
            try w.writeByte(':');
            try writeJsonString(w, e.to);
            try writeOptStr(w, "corr", e.corr);
            try writeOptStr(w, "trace_id", e.trace_id);
        },
        .timeout => |e| {
            try w.print(",\"after_ms\":{d}", .{e.after_ms});
            try writeOptStr(w, "corr", e.corr);
            try writeOptStr(w, "trace_id", e.trace_id);
        },
        .monitor => |e| {
            try w.print(",\"target_pid\":{d},\"target_beam\":{d}", .{ e.target_pid, e.target_beam });
        },
        .down => |e| {
            try w.print(",\"monitored_pid\":{d},\"monitored_beam\":{d}", .{ e.monitored_pid, e.monitored_beam });
            try writeOptStr(w, "reason", e.reason);
        },
        .policy_change => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "field");
            try w.writeByte(':');
            try writeJsonString(w, e.field);
            try writeOptStr(w, "old_value", e.old_value);
            try writeOptStr(w, "new_value", e.new_value);
        },
        .mailbox_overflow => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "strategy");
            try w.writeByte(':');
            try writeJsonString(w, e.strategy);
            try w.print(",\"mailbox_len\":{d}", .{e.mailbox_len});
            try writeOptStr(w, "msg_id", e.msg_id);
        },
        .storage_write => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "key");
            try w.writeByte(':');
            try writeJsonString(w, e.key);
            try w.print(",\"bytes\":{d}", .{e.bytes});
        },
        .storage_evict => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "key");
            try w.writeByte(':');
            try writeJsonString(w, e.key);
            try w.print(",\"bytes\":{d}", .{e.bytes});
        },
        .restart => |e| {
            try w.print(",\"attempt\":{d}", .{e.attempt});
            try writeOptStr(w, "reason", e.reason);
        },
        .exit => |e| {
            try writeOptStr(w, "reason", e.reason);
            if (e.code) |code| try w.print(",\"code\":{d}", .{code});
        },
        .note => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "level");
            try w.writeByte(':');
            try writeJsonString(w, e.level);
            try w.writeByte(',');
            try writeJsonString(w, "message");
            try w.writeByte(':');
            try writeJsonString(w, e.message);
        },
        .metric => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "name");
            try w.writeByte(':');
            try writeJsonString(w, e.name);
            try w.print(",\"value\":{d}", .{e.value});
            try writeOptStr(w, "unit", e.unit);
        },
        .decision => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "level");
            try w.writeByte(':');
            try writeJsonString(w, e.level);
            try w.writeByte(',');
            try writeJsonString(w, "message");
            try w.writeByte(':');
            try writeJsonString(w, e.message);
        },
        .tool_call => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "tool");
            try w.writeByte(':');
            try writeJsonString(w, e.tool);
            try writeOptStr(w, "input", e.input);
            try writeOptStr(w, "corr", e.corr);
        },
        .tool_result => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "tool");
            try w.writeByte(':');
            try writeJsonString(w, e.tool);
            try w.print(",\"success\":{}", .{e.success});
            try writeOptStr(w, "corr", e.corr);
        },
        .warning => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "message");
            try w.writeByte(':');
            try writeJsonString(w, e.message);
        },
        .error_event => |e| {
            try w.writeByte(',');
            try writeJsonString(w, "message");
            try w.writeByte(':');
            try writeJsonString(w, e.message);
            try writeOptStr(w, "code", e.code);
        },
    }
}
