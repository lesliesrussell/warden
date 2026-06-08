// warden-3qh
//
// Terminal output + the process-global std.Io executor for wardenctl.
//
// Zig 0.16 routes file/socket I/O through std.Io. wardenctl is a short-lived
// client with no Runtime, so it uses the global threaded executor (the same
// one std.start populates) for its socket and stdout/stderr writes.

const std = @import("std");

pub inline fn gio() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

fn writeTo(file: std.Io.File, bytes: []const u8) void {
    var buf: [4096]u8 = undefined;
    var w = file.writer(gio(), &buf);
    w.interface.writeAll(bytes) catch return;
    w.interface.flush() catch {};
}

/// Write to stdout (best-effort, flushed).
pub fn outAll(bytes: []const u8) void {
    writeTo(std.Io.File.stdout(), bytes);
}

/// Write to stderr (best-effort, flushed).
pub fn errAll(bytes: []const u8) void {
    writeTo(std.Io.File.stderr(), bytes);
}
