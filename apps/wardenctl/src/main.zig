// warden-39v

const std = @import("std");
const cli = @import("cli.zig");

// warden-3qh: Zig 0.16 removed std.process.argsAlloc; args arrive via
// std.process.Init.Minimal and are read through an Args iterator.
pub fn main(init: std.process.Init.Minimal) !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var argv: std.ArrayList([]const u8) = .empty;
    defer {
        for (argv.items) |a| allocator.free(a);
        argv.deinit(allocator);
    }
    var it = try std.process.Args.Iterator.initAllocator(init.args, allocator);
    defer it.deinit();
    while (it.next()) |a| try argv.append(allocator, try allocator.dupe(u8, a));

    // argv[0] is the binary name; pass the rest.
    const rest = if (argv.items.len > 1) argv.items[1..] else &[_][]const u8{};
    try cli.run(allocator, rest);
}
