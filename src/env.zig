// warden-l3i
//
// Environment-variable access for Zig 0.16.
//
// Zig 0.16 removed std.posix.getenv; the process environment now lives behind
// the std.Io interface. For an exe using std.start, the start code populates
// the global executor's environ block from the real envp at boot, and the test
// runner exposes the environment as std.testing.environ. process.Environ.
// getPosix does a non-allocating, runtime-key scan of that block — which is
// exactly what the old getenv did — so this is a drop-in returning the same
// ?[:0]const u8 the call sites expected.

const std = @import("std");
const builtin = @import("builtin");

fn processEnviron() std.process.Environ {
    if (builtin.is_test) return std.testing.environ;
    return std.Io.Threaded.global_single_threaded.environ.process_environ;
}

/// Look up an environment variable by name — drop-in for std.posix.getenv.
pub fn get(name: []const u8) ?[:0]const u8 {
    return processEnviron().getPosix(name);
}

/// Build a mutable copy of the current process environment — replaces the
/// removed std.process.getEnvMap. Caller owns the returned Map (deinit it).
pub fn createMap(allocator: std.mem.Allocator) !std.process.Environ.Map {
    return processEnviron().createMap(allocator);
}
