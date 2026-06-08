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
