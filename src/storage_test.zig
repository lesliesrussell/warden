// warden-h12

const std = @import("std");
const types = @import("types.zig");
const storage_mod = @import("storage.zig");

const Pid = types.Pid;
const PolicyEnvelope = types.PolicyEnvelope;
const StorageView = storage_mod.StorageView;
const StorageError = storage_mod.StorageError;
const Namespace = storage_mod.Namespace;

// warden-h12
fn testPid() Pid {
    return .{ .beam = 1, .proc = 42 };
}

fn defaultPolicy() PolicyEnvelope {
    return .{};
}

// warden-h12
// Wrapper that owns both the base_dir string and the StorageView.
const TestView = struct {
    base_dir: []u8,
    view: StorageView,
    allocator: std.mem.Allocator,

    fn init(
        allocator: std.mem.Allocator,
        tmp: *std.testing.TmpDir,
        policy: PolicyEnvelope,
    ) !TestView {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const base = try tmp.dir.realpath(".", &path_buf);
        const base_owned = try allocator.dupe(u8, base);
        errdefer allocator.free(base_owned);
        const view = try StorageView.init(allocator, base_owned, testPid(), policy);
        return TestView{
            .base_dir = base_owned,
            .view = view,
            .allocator = allocator,
        };
    }

    fn deinit(self: *TestView) void {
        self.view.deinit();
        self.allocator.free(self.base_dir);
    }
};

// warden-h12
test "proc-temp write and read round trip" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.write(.proc_temp, "hello.txt", "world");
    const data = try tv.view.read(.proc_temp, "hello.txt");
    defer std.testing.allocator.free(data);

    try std.testing.expectEqualStrings("world", data);
}

// warden-h12
test "proc-temp deleted by cleanupTemp" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.write(.proc_temp, "ephemeral.txt", "data");

    // Verify it exists.
    const data = try tv.view.read(.proc_temp, "ephemeral.txt");
    std.testing.allocator.free(data);

    try tv.view.cleanupTemp();

    // Should be gone now.
    const result = tv.view.read(.proc_temp, "ephemeral.txt");
    try std.testing.expectError(StorageError.NotFound, result);
}

// warden-h12
test "proc-cache eviction removes files down to limit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    // Write 3 files of 100 bytes each = 300 bytes total.
    const hundred = "x" ** 100;
    try tv.view.write(.proc_cache, "a.bin", hundred);
    try tv.view.write(.proc_cache, "b.bin", hundred);
    try tv.view.write(.proc_cache, "c.bin", hundred);
    try std.testing.expectEqual(@as(u64, 300), tv.view.bytes_cache);

    // Evict down to 150 bytes — should remove at least one file.
    try tv.view.evictCache(150);
    try std.testing.expect(tv.view.bytes_cache <= 150);
}

// warden-h12
test "proc-state write survives (no auto-cleanup)" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.write(.proc_state, "persistent.txt", "keep me");

    // Cleanup temp should not touch state.
    try tv.view.cleanupTemp();

    const data = try tv.view.read(.proc_state, "persistent.txt");
    defer std.testing.allocator.free(data);
    try std.testing.expectEqualStrings("keep me", data);
}

// warden-h12
test "shared-vol access denied without grant" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    const result = tv.view.read(.shared_vol, "myvol/secret.txt");
    try std.testing.expectError(StorageError.AccessDenied, result);
}

// warden-h12
test "shared-vol access succeeds after grantVolume" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.grantVolume("myvol");

    // Write and read back.
    try tv.view.write(.shared_vol, "myvol/data.txt", "shared content");
    const data = try tv.view.read(.shared_vol, "myvol/data.txt");
    defer std.testing.allocator.free(data);
    try std.testing.expectEqualStrings("shared content", data);
}

// warden-h12
test "quota exceeded error on over-limit write" {
    var policy = defaultPolicy();
    policy.max_temp_bytes = 50;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, policy);
    defer tv.deinit();

    // First write fits.
    try tv.view.write(.proc_temp, "small.txt", "x" ** 30);

    // Second write would push total past 50.
    const result = tv.view.write(.proc_temp, "big.txt", "x" ** 30);
    try std.testing.expectError(StorageError.QuotaExceeded, result);
}

// warden-h12
test "append adds to existing content and tracks bytes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.write(.proc_state, "log.txt", "line1\n");
    try tv.view.append(.proc_state, "log.txt", "line2\n");

    const data = try tv.view.read(.proc_state, "log.txt");
    defer std.testing.allocator.free(data);
    try std.testing.expectEqualStrings("line1\nline2\n", data);
}

// warden-h12
test "delete removes file and decrements quota tracking" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.write(.proc_temp, "to_delete.txt", "data");
    try std.testing.expect(tv.view.bytes_temp > 0);

    try tv.view.delete(.proc_temp, "to_delete.txt");
    try std.testing.expectEqual(@as(u64, 0), tv.view.bytes_temp);

    const result = tv.view.read(.proc_temp, "to_delete.txt");
    try std.testing.expectError(StorageError.NotFound, result);
}

// warden-h12
test "stat returns correct size and kind" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try tv.view.write(.proc_state, "measured.txt", "12345");

    const s = try tv.view.stat(.proc_state, "measured.txt");
    try std.testing.expectEqual(@as(u64, 5), s.size_bytes);
    try std.testing.expectEqual(false, s.is_dir);
}

// warden-h12
test "invalid path rejected" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var tv = try TestView.init(std.testing.allocator, &tmp, defaultPolicy());
    defer tv.deinit();

    try std.testing.expectError(StorageError.InvalidPath, tv.view.read(.proc_temp, "../escape.txt"));
    try std.testing.expectError(StorageError.InvalidPath, tv.view.read(.proc_temp, "/absolute"));
}
