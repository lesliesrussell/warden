// warden-h12

const std = @import("std");
const types = @import("types.zig");

const Pid = types.Pid;
const PolicyEnvelope = types.PolicyEnvelope;

// warden-h12
pub const StorageError = error{
    AccessDenied,
    QuotaExceeded,
    InvalidPath,
    NotFound,
};

// warden-h12
pub const Namespace = enum {
    proc_temp,
    proc_cache,
    proc_state,
    shared_vol,
};

// warden-h12
pub const Stat = struct {
    size_bytes: u64,
    is_dir: bool,
};

// warden-h12
// Validates that path is relative and contains no `..` segments.
// Returns StorageError.InvalidPath on violations.
fn validateRelPath(path: []const u8) StorageError!void {
    if (path.len == 0) return StorageError.InvalidPath;
    if (path[0] == '/') return StorageError.InvalidPath;

    var it = std.mem.splitScalar(u8, path, '/');
    while (it.next()) |seg| {
        if (std.mem.eql(u8, seg, "..")) return StorageError.InvalidPath;
        if (std.mem.eql(u8, seg, ".")) return StorageError.InvalidPath;
        if (seg.len == 0) return StorageError.InvalidPath;
    }
}

// warden-h12
// Per-process scoped view of the storage namespaces.
// All paths passed to operations are relative within the namespace.
// No raw host paths are exposed to callers.
//
// Directory layout under base_dir:
//   temp/<beam>/<proc>/    ← proc_temp
//   cache/<beam>/<proc>/   ← proc_cache
//   state/<beam>/<proc>/   ← proc_state
//   vol/<vol_name>/        ← shared_vol (ACL-checked)
//
// shared_vol routing: the first path segment is the volume name.
//   e.g. read(.shared_vol, "myvol/foo.txt") → <base>/vol/myvol/foo.txt
//   Access is denied unless "myvol" is in the granted set.
pub const StorageView = struct {
    // warden-lmm: Zig 0.16 routes filesystem I/O through std.Io.
    io: std.Io,
    allocator: std.mem.Allocator,
    base_dir: []const u8,
    pid: Pid,
    policy: PolicyEnvelope,
    granted_volumes: std.StringHashMap(void),

    // Cumulative current bytes per namespace (updated on write/delete).
    bytes_temp: u64,
    bytes_cache: u64,
    bytes_state: u64,

    // warden-h12
    pub fn init(
        io: std.Io,
        allocator: std.mem.Allocator,
        base_dir: []const u8,
        pid: Pid,
        policy: PolicyEnvelope,
    ) !StorageView {
        return StorageView{
            .io = io,
            .allocator = allocator,
            .base_dir = base_dir,
            .pid = pid,
            .policy = policy,
            .granted_volumes = std.StringHashMap(void).init(allocator),
            .bytes_temp = 0,
            .bytes_cache = 0,
            .bytes_state = 0,
        };
    }

    // warden-h12
    pub fn deinit(self: *StorageView) void {
        // Free keys stored in granted_volumes.
        var it = self.granted_volumes.keyIterator();
        while (it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.granted_volumes.deinit();
    }

    // warden-h12
    // Grant access to a shared volume by name.
    pub fn grantVolume(self: *StorageView, vol_name: []const u8) !void {
        const owned = try self.allocator.dupe(u8, vol_name);
        errdefer self.allocator.free(owned);
        try self.granted_volumes.put(owned, {});
    }

    // warden-h12
    // Resolve namespace + relative path to an absolute host path.
    // For shared_vol: first segment is vol_name; checks ACL.
    // Caller must free the returned slice.
    fn resolvePath(
        self: *StorageView,
        ns: Namespace,
        rel_path: []const u8,
    ) ![]u8 {
        try validateRelPath(rel_path);

        switch (ns) {
            .proc_temp => {
                return std.fmt.allocPrint(
                    self.allocator,
                    "{s}/temp/{d}/{d}/{s}",
                    .{ self.base_dir, self.pid.beam, self.pid.proc, rel_path },
                );
            },
            .proc_cache => {
                return std.fmt.allocPrint(
                    self.allocator,
                    "{s}/cache/{d}/{d}/{s}",
                    .{ self.base_dir, self.pid.beam, self.pid.proc, rel_path },
                );
            },
            .proc_state => {
                return std.fmt.allocPrint(
                    self.allocator,
                    "{s}/state/{d}/{d}/{s}",
                    .{ self.base_dir, self.pid.beam, self.pid.proc, rel_path },
                );
            },
            .shared_vol => {
                // Extract volume name (first segment).
                const slash = std.mem.indexOfScalar(u8, rel_path, '/') orelse {
                    // rel_path IS the vol_name with no sub-path — invalid usage.
                    return StorageError.InvalidPath;
                };
                const vol_name = rel_path[0..slash];
                if (!self.granted_volumes.contains(vol_name)) {
                    return StorageError.AccessDenied;
                }
                return std.fmt.allocPrint(
                    self.allocator,
                    "{s}/vol/{s}",
                    .{ self.base_dir, rel_path },
                );
            },
        }
    }

    // warden-h12
    // Returns and updates the appropriate bytes counter.
    fn bytesPtr(self: *StorageView, ns: Namespace) ?*u64 {
        return switch (ns) {
            .proc_temp => &self.bytes_temp,
            .proc_cache => &self.bytes_cache,
            .proc_state => &self.bytes_state,
            .shared_vol => null, // shared_vol not quota-tracked per process
        };
    }

    // warden-h12
    // Returns the policy limit for the namespace.
    fn quotaLimit(self: *StorageView, ns: Namespace) u64 {
        return switch (ns) {
            .proc_temp => self.policy.max_temp_bytes,
            .proc_cache => self.policy.max_cache_bytes,
            .proc_state => self.policy.max_state_bytes,
            .shared_vol => std.math.maxInt(u64),
        };
    }

    // warden-h12
    // Ensure parent directories exist for a file path.
    fn ensureParentDirs(io: std.Io, abs_path: []const u8) !void {
        const dir_end = std.mem.lastIndexOfScalar(u8, abs_path, '/') orelse return;
        const dir_path = abs_path[0..dir_end];
        std.Io.Dir.cwd().createDirPath(io, dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }

    // warden-h12
    // Read file contents. Caller owns the returned slice.
    pub fn read(self: *StorageView, ns: Namespace, path: []const u8) ![]u8 {
        const abs = try self.resolvePath(ns, path);
        defer self.allocator.free(abs);

        return std.Io.Dir.cwd().readFileAlloc(self.io, abs, self.allocator, .unlimited) catch |err| switch (err) {
            error.FileNotFound => return StorageError.NotFound,
            else => return err,
        };
    }

    // warden-h12
    // Write data to file, replacing existing content.
    // Enforces per-namespace quota.
    pub fn write(self: *StorageView, ns: Namespace, path: []const u8, data: []const u8) !void {
        const abs = try self.resolvePath(ns, path);
        defer self.allocator.free(abs);

        // Check quota: subtract old file size if exists, then check new size fits.
        const bytes_field = self.bytesPtr(ns);
        var old_size: u64 = 0;
        if (bytes_field != null) {
            old_size = fileSizeAbsolute(self.io, abs) catch 0;
            const new_total = (bytes_field.?.* -| old_size) + data.len;
            if (new_total > self.quotaLimit(ns)) {
                return StorageError.QuotaExceeded;
            }
        }

        try ensureParentDirs(self.io, abs);

        try std.Io.Dir.cwd().writeFile(self.io, .{
            .sub_path = abs,
            .data = data,
            .flags = .{ .truncate = true },
        });

        if (bytes_field) |bf| {
            bf.* = bf.* -| old_size;
            bf.* += data.len;
        }
    }

    // warden-h12
    // Append data to file, creating it if necessary.
    // Enforces per-namespace quota on the delta.
    pub fn append(self: *StorageView, ns: Namespace, path: []const u8, data: []const u8) !void {
        const abs = try self.resolvePath(ns, path);
        defer self.allocator.free(abs);

        const bytes_field = self.bytesPtr(ns);
        if (bytes_field) |bf| {
            const new_total = bf.* + data.len;
            if (new_total > self.quotaLimit(ns)) {
                return StorageError.QuotaExceeded;
            }
        }

        try ensureParentDirs(self.io, abs);

        const file = blk: {
            if (std.Io.Dir.openFileAbsolute(self.io, abs, .{ .mode = .read_write })) |f| {
                break :blk f;
            } else |err| {
                if (err == error.FileNotFound) {
                    break :blk try std.Io.Dir.createFileAbsolute(self.io, abs, .{});
                }
                return err;
            }
        };
        defer file.close(self.io);

        // warden-lmm: 0.16 File has no seekFromEnd; use a positional writer
        // anchored at end-of-file to append.
        const st = try file.stat(self.io);
        var wbuf: [4096]u8 = undefined;
        var w = std.Io.File.Writer.init(file, self.io, &wbuf);
        w.pos = st.size;
        try w.interface.writeAll(data);
        try w.interface.flush();

        if (bytes_field) |bf| {
            bf.* += data.len;
        }
    }

    // warden-h12
    // List entries in a directory. Caller owns the result slice and each string.
    pub fn list(self: *StorageView, ns: Namespace, path: []const u8) ![][]u8 {
        const abs = try self.resolvePath(ns, path);
        defer self.allocator.free(abs);

        var dir = std.Io.Dir.openDirAbsolute(self.io, abs, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return StorageError.NotFound,
            else => return err,
        };
        defer dir.close(self.io);

        // warden-lmm: 0.16 ArrayList is unmanaged; allocator passed per call.
        var entries: std.ArrayList([]u8) = .empty;
        errdefer {
            for (entries.items) |e| self.allocator.free(e);
            entries.deinit(self.allocator);
        }

        var it = dir.iterate();
        while (try it.next(self.io)) |entry| {
            const name = try self.allocator.dupe(u8, entry.name);
            try entries.append(self.allocator, name);
        }

        return entries.toOwnedSlice(self.allocator);
    }

    // warden-h12
    // Delete a file or empty directory.
    pub fn delete(self: *StorageView, ns: Namespace, path: []const u8) !void {
        const abs = try self.resolvePath(ns, path);
        defer self.allocator.free(abs);

        const bytes_field = self.bytesPtr(ns);
        var old_size: u64 = 0;
        if (bytes_field != null) {
            old_size = fileSizeAbsolute(self.io, abs) catch 0;
        }

        std.Io.Dir.deleteFileAbsolute(self.io, abs) catch |err| switch (err) {
            error.FileNotFound => return StorageError.NotFound,
            else => return err,
        };

        if (bytes_field) |bf| {
            bf.* -|= old_size;
        }
    }

    // warden-h12
    // Stat a file or directory.
    pub fn stat(self: *StorageView, ns: Namespace, path: []const u8) !Stat {
        const abs = try self.resolvePath(ns, path);
        defer self.allocator.free(abs);

        const st = std.Io.Dir.cwd().statFile(self.io, abs, .{}) catch |err| switch (err) {
            error.FileNotFound => return StorageError.NotFound,
            else => return err,
        };
        return Stat{
            .size_bytes = st.size,
            .is_dir = st.kind == .directory,
        };
    }

    // warden-h12
    // Delete the proc_temp subtree for this process.
    pub fn cleanupTemp(self: *StorageView) !void {
        const abs = try std.fmt.allocPrint(
            self.allocator,
            "{s}/temp/{d}/{d}",
            .{ self.base_dir, self.pid.beam, self.pid.proc },
        );
        defer self.allocator.free(abs);

        // warden-lmm: 0.16 deleteTree is idempotent (no FileNotFound).
        try std.Io.Dir.cwd().deleteTree(self.io, abs);
        self.bytes_temp = 0;
    }

    // warden-h12
    // Evict files from proc_cache until total usage is at or below bytes_limit.
    // Files are deleted in an arbitrary order (oldest-first via iteration order).
    pub fn evictCache(self: *StorageView, bytes_limit: u64) !void {
        if (self.bytes_cache <= bytes_limit) return;

        const cache_root = try std.fmt.allocPrint(
            self.allocator,
            "{s}/cache/{d}/{d}",
            .{ self.base_dir, self.pid.beam, self.pid.proc },
        );
        defer self.allocator.free(cache_root);

        var dir = std.Io.Dir.openDirAbsolute(self.io, cache_root, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close(self.io);

        var it = dir.iterate();
        while (try it.next(self.io)) |entry| {
            if (self.bytes_cache <= bytes_limit) break;
            if (entry.kind != .file) continue;

            const file_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}",
                .{ cache_root, entry.name },
            );
            defer self.allocator.free(file_path);

            const fsize = fileSizeAbsolute(self.io, file_path) catch 0;
            std.Io.Dir.deleteFileAbsolute(self.io, file_path) catch continue;
            self.bytes_cache -|= fsize;
        }
    }
};

// warden-h12
// Helper: get file size by absolute path, returns 0 if not found.
fn fileSizeAbsolute(io: std.Io, abs_path: []const u8) !u64 {
    const file = try std.Io.Dir.openFileAbsolute(io, abs_path, .{});
    defer file.close(io);
    const st = try file.stat(io);
    return st.size;
}
