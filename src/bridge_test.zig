// warden-eet

const std = @import("std");
const testutil = @import("testutil.zig");
const bridge = @import("bridge.zig");
const beam_mod = @import("beam.zig");

const ForeignBridge = bridge.ForeignBridge;
const BridgeSupervisor = bridge.BridgeSupervisor;

// warden-eet
// Test 1: Frame encode/decode round trip.
test "frame round trip" {
    // Create a Unix domain socket pair for bidirectional I/O.
    var sv: [2]std.c.fd_t = undefined;
    const rc = std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &sv);
    try std.testing.expect(rc == 0);
    const sender = std.Io.net.Stream{ .socket = .{ .handle = sv[0], .address = undefined } };
    const receiver = std.Io.net.Stream{ .socket = .{ .handle = sv[1], .address = undefined } };
    defer sender.close(std.testing.io);
    defer receiver.close(std.testing.io);

    const payload = "{\"kind\":\"ok\",\"hello\":\"world\"}";
    try bridge.writeFrameTest(std.testing.io, sender, payload);

    const allocator = std.testing.allocator;
    const received = try bridge.readFrameTest(std.testing.io, allocator, receiver);
    defer allocator.free(received);

    try std.testing.expectEqualStrings(payload, received);
}

// warden-eet
// Test 2: Base64 round trip for fs data encoding.
test "base64 round trip" {
    const allocator = std.testing.allocator;
    const original = "hello, warden!";

    const enc_len = std.base64.standard.Encoder.calcSize(original.len);
    const encoded = try allocator.alloc(u8, enc_len);
    defer allocator.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, original);

    const dec_len = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
    const decoded = try allocator.alloc(u8, dec_len);
    defer allocator.free(decoded);
    try std.base64.standard.Decoder.decode(decoded, encoded);

    try std.testing.expectEqualStrings(original, decoded);
}

// warden-eet
// Test 3: ForeignBridge.init allocates socket path and creates server without panicking.
test "ForeignBridge.init and deinit" {
    const allocator = std.testing.allocator;

    const rt = try beam_mod.Runtime.init(allocator, 99);
    defer rt.destroy();

    // Create temp directories for log_dir and storage_base.
    var tmp_log = std.testing.tmpDir(.{});
    defer tmp_log.cleanup();
    var tmp_storage = std.testing.tmpDir(.{});
    defer tmp_storage.cleanup();

    const log_path = try testutil.tmpAbsAlloc(allocator, &tmp_log);
    defer allocator.free(log_path);
    const storage_path = try testutil.tmpAbsAlloc(allocator, &tmp_storage);
    defer allocator.free(storage_path);

    const cmd = &[_][]const u8{"/bin/true"};

    var fb = try ForeignBridge.init(allocator, rt, cmd, log_path, storage_path);
    fb.deinit();
}

// warden-eet
// Test 4: BridgeSupervisor.init succeeds.
test "BridgeSupervisor.init succeeds" {
    const allocator = std.testing.allocator;

    const rt = try beam_mod.Runtime.init(allocator, 100);
    defer rt.destroy();

    var sup = BridgeSupervisor.init(allocator, rt);
    sup.deinit();
}

// warden-6f6
test "handshake frame advertises protocol version and capabilities" {
    const allocator = std.testing.allocator;
    const pid = @import("types.zig").Pid{ .beam = 3, .proc = 9 };
    const frame = try bridge.buildHandshakeFrame(allocator, pid, "/tmp/warden-9.sock");
    defer allocator.free(frame);

    // Parses as JSON and carries the expected fields.
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, frame, .{});
    defer parsed.deinit();
    const obj = parsed.value.object;
    try std.testing.expectEqualStrings("handshake", obj.get("kind").?.string);
    try std.testing.expectEqual(@as(i64, bridge.protocol_version), obj.get("protocol_version").?.integer);
    try std.testing.expectEqualStrings("3/9", obj.get("pid").?.string);
    const caps = obj.get("capabilities").?.array;
    try std.testing.expectEqual(@as(usize, 5), caps.items.len);
    try std.testing.expectEqualStrings("send", caps.items[0].string);
}
