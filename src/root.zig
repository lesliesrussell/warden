// warden-52t
pub const types = @import("types.zig");
// warden-3rc
pub const registry = @import("registry.zig");

comptime {
    _ = @import("types_test.zig");
    _ = @import("registry_test.zig");
}
