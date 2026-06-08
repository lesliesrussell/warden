// warden-foo
//
// Wall-clock access for Zig 0.16.
//
// Zig 0.16 removed std.time.milliTimestamp(); reading the wall clock now goes
// through the std.Io interface (`std.Io.Clock.now(.real, io)`). The read is
// non-blocking and instance-independent (the threaded executor's `now` vtable
// ignores its userdata and calls the OS clock directly), so a process-global
// executor is sufficient here — no need to thread a runtime Io into every call
// site that only wants a timestamp. Real I/O (filesystem, sockets, env) still
// uses the Runtime-owned Io; this helper is for timestamps only.

const std = @import("std");

inline fn cio() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

/// Milliseconds since the Unix epoch — drop-in for the removed
/// std.time.milliTimestamp().
pub fn nowMs() i64 {
    return std.Io.Clock.now(.real, cio()).toMilliseconds();
}

/// Block the calling thread for `ns` nanoseconds — drop-in for the removed
/// std.Thread.sleep(). Uses the monotonic clock; the global executor's sleep
/// is a plain clock_nanosleep on the calling thread and cancel is a no-op, so
/// no runtime Io is needed.
pub fn sleepNs(ns: u64) void {
    std.Io.sleep(cio(), .{ .nanoseconds = @intCast(ns) }, .awake) catch {};
}
