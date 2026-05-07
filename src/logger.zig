// warden-554

const std = @import("std");

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
pub const ProcessLogger = struct {
    allocator: std.mem.Allocator,
    beam_id: u32,
    pid: u64,
    file: std.fs.File,
    buf: [4096]u8,
    file_writer: std.fs.File.Writer,
    seq: u64,

    // warden-554
    pub fn init(
        allocator: std.mem.Allocator,
        beam_id: u32,
        pid: u64,
        log_dir: std.fs.Dir,
    ) !ProcessLogger {
        const filename = try std.fmt.allocPrint(allocator, "{d}-{d}.log", .{ beam_id, pid });
        defer allocator.free(filename);

        const file = try log_dir.createFile(filename, .{
            .truncate = false,
            .exclusive = false,
        });
        errdefer file.close();

        // Seek to end for append behaviour
        try file.seekFromEnd(0);

        // We initialise buf and file_writer as undefined then fix them up.
        // The file_writer.interface.buffer pointer will be patched in fixup.
        var self: ProcessLogger = .{
            .allocator = allocator,
            .beam_id = beam_id,
            .pid = pid,
            .file = file,
            .buf = undefined,
            .file_writer = undefined,
            .seq = 0,
        };
        self.file_writer = std.fs.File.Writer.initStreaming(file, &self.buf);
        return self;
    }

    // warden-554
    pub fn deinit(self: *ProcessLogger) void {
        self.file_writer.interface.flush() catch {};
        self.file.close();
    }

    // warden-554
    /// Flush buffered bytes to the OS.
    pub fn flush(self: *ProcessLogger) !void {
        try self.file_writer.interface.flush();
    }

    // warden-554
    /// Write one NDJSON record for `event`.
    /// `extra` is an optional map of additional string→string fields to append.
    pub fn emit(self: *ProcessLogger, event: LogEvent, extra: ?std.StringHashMap([]const u8)) !void {
        self.seq += 1;
        const ts = std.time.milliTimestamp();
        const w = &self.file_writer.interface;

        // Mandatory fields
        try w.print(
            "{{\"ts\":{d},\"beam\":{d},\"pid\":{d},\"seq\":{d},\"event\":\"{s}\"",
            .{ ts, self.beam_id, self.pid, self.seq, event.tag() },
        );

        // Event-specific fields
        try writeEventFields(w, event);

        // Caller-supplied extra string fields
        if (extra) |map| {
            var it = map.iterator();
            while (it.next()) |entry| {
                try w.writeByte(',');
                try writeJsonString(w, entry.key_ptr.*);
                try w.writeByte(':');
                try writeJsonString(w, entry.value_ptr.*);
            }
        }

        try w.writeAll("}\n");
    }

    // warden-554
    /// Emit a `note` event.
    pub fn note(
        self: *ProcessLogger,
        level: []const u8,
        message: []const u8,
        extra: ?std.StringHashMap([]const u8),
    ) !void {
        try self.emit(.{ .note = .{ .level = level, .message = message } }, extra);
    }

    // warden-554
    /// Emit a `metric` event.
    pub fn metric(
        self: *ProcessLogger,
        name: []const u8,
        value: f64,
        extra: ?std.StringHashMap([]const u8),
    ) !void {
        try self.emit(.{ .metric = .{ .name = name, .value = value } }, extra);
    }

    // warden-554
    /// Emit a `warning` event.
    pub fn warn(
        self: *ProcessLogger,
        message: []const u8,
        extra: ?std.StringHashMap([]const u8),
    ) !void {
        try self.emit(.{ .warning = .{ .message = message } }, extra);
    }

    // warden-554
    /// Emit an `error_event` event.
    pub fn err(
        self: *ProcessLogger,
        message: []const u8,
        extra: ?std.StringHashMap([]const u8),
    ) !void {
        try self.emit(.{ .error_event = .{ .message = message } }, extra);
    }
};

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
