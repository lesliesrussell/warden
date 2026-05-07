// warden-52t
pub const types = @import("types.zig");
// warden-3rc
pub const registry = @import("registry.zig");
// warden-6v5
pub const mailbox = @import("mailbox.zig");
// warden-h12
pub const storage = @import("storage.zig");

comptime {
    _ = @import("types_test.zig");
    _ = @import("registry_test.zig");
    // warden-6v5
    _ = @import("mailbox_test.zig");
    // warden-h12
    _ = @import("storage_test.zig");
}
