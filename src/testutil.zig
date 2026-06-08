// warden-0tc
//
// Test helpers for Zig 0.16.
//
// Zig 0.16 removed Io.Dir.realpath/realpathAlloc, which the tests used to turn a
// std.testing.tmpDir into an absolute path for log_dir/storage_base (StorageView
// builds absolute paths internally, so it needs a real absolute base). tmpDir
// places the directory at <cwd>/.zig-cache/tmp/<sub_path>, so we rebuild that
// path from the current directory (std.process.currentPath) and the tmp dir's
// sub_path. std.testing.io is the test runner's executor.

const std = @import("std");

/// Absolute path of `tmp`, written into `buf`. Replaces tmp.dir.realpath(".", buf).
pub fn tmpAbs(buf: []u8, tmp: *std.testing.TmpDir) ![]const u8 {
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const n = try std.process.currentPath(std.testing.io, &cwd_buf);
    return std.fmt.bufPrint(buf, "{s}/.zig-cache/tmp/{s}", .{ cwd_buf[0..n], &tmp.sub_path });
}

/// Allocating variant. Replaces tmp.dir.realpathAlloc(allocator, ".").
pub fn tmpAbsAlloc(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const n = try std.process.currentPath(std.testing.io, &cwd_buf);
    return std.fmt.allocPrint(allocator, "{s}/.zig-cache/tmp/{s}", .{ cwd_buf[0..n], &tmp.sub_path });
}
