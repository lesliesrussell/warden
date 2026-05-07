// warden-52t

const std = @import("std");

pub const Pid = struct {
    beam: u32,
    proc: u64,

    pub fn eql(a: Pid, b: Pid) bool {
        return a.beam == b.beam and a.proc == b.proc;
    }

    pub fn format(self: Pid, writer: anytype) !void {
        try writer.print("pid:{d}/{d}", .{ self.beam, self.proc });
    }
};

pub const ProcessKind = enum {
    native_worker,
    native_supervisor,
    foreign_worker,
    system_service,
};

pub const ProcessState = enum {
    starting,
    ready,
    running,
    waiting,
    hibernating,
    paused,
    exiting,
    dead,

    pub fn canTransitionTo(self: ProcessState, next: ProcessState) bool {
        return switch (self) {
            .starting => next == .ready or next == .exiting,
            .ready => next == .running or next == .paused or next == .exiting,
            .running => next == .ready or next == .waiting or next == .hibernating or next == .paused or next == .exiting,
            .waiting => next == .ready or next == .paused or next == .exiting,
            .hibernating => next == .ready or next == .paused or next == .exiting,
            .paused => next == .ready or next == .exiting,
            .exiting => next == .dead,
            .dead => false,
        };
    }
};

pub const ActivityClass = enum {
    tiny,
    normal,
    elevated,
    system,
    paused,
};

pub const PriorityClass = enum {
    low,
    normal,
    high,
};

pub const OverflowStrategy = enum {
    reject_new,
    drop_oldest_low,
    escalate_supervisor,
    throttle_sender,
};

pub const PolicyEnvelope = struct {
    activity_class: ActivityClass = .normal,
    priority_class: PriorityClass = .normal,
    max_reductions_per_slice: u32 = 2000,
    max_mailbox_len: u32 = 1000,
    max_mailbox_bytes: u64 = 4 * 1024 * 1024,
    max_log_bytes_per_min: u64 = 1 * 1024 * 1024,
    max_temp_bytes: u64 = 256 * 1024 * 1024,
    max_cache_bytes: u64 = 256 * 1024 * 1024,
    max_state_bytes: u64 = 64 * 1024 * 1024,
    idle_timeout_ms: u64 = 30_000,
    overflow_strategy: OverflowStrategy = .reject_new,
    hibernation_enabled: bool = true,
    promotion_ttl_ms: ?u64 = null,
};

pub const MessageKind = enum {
    request,
    response,
    event,
    signal,
    system,
};

pub const MessagePriority = enum {
    high,
    normal,
    low,
};

pub const MessageEnvelope = struct {
    v: u8 = 1,
    kind: MessageKind,
    @"type": []const u8,
    id: []const u8,
    from: []const u8,
    to: []const u8,
    corr: ?[]const u8 = null,
    reply_to: ?[]const u8 = null,
    deadline: ?i64 = null,
    priority: MessagePriority = .normal,
    trace_id: ?[]const u8 = null,
    session_id: ?[]const u8 = null,
    task_id: ?[]const u8 = null,
    body: std.json.Value,
};

comptime {
    // ProcessState covers all 8 states from the spec
    const state_count = @typeInfo(ProcessState).@"enum".fields.len;
    if (state_count != 8) @compileError("ProcessState must have exactly 8 variants");

    // ActivityClass covers all 5 classes from the spec
    const class_count = @typeInfo(ActivityClass).@"enum".fields.len;
    if (class_count != 5) @compileError("ActivityClass must have exactly 5 variants");

    // OverflowStrategy covers all 4 strategies from the spec
    const overflow_count = @typeInfo(OverflowStrategy).@"enum".fields.len;
    if (overflow_count != 4) @compileError("OverflowStrategy must have exactly 4 variants");

    // ProcessKind covers all 4 kinds from the spec
    const kind_count = @typeInfo(ProcessKind).@"enum".fields.len;
    if (kind_count != 4) @compileError("ProcessKind must have exactly 4 variants");
}
