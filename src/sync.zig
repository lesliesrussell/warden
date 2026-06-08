// warden-d8p
//
// Blocking synchronization primitives for Warden's OS-thread design.
//
// Zig 0.16 removed the zero-argument blocking `std.Thread.Mutex` and
// `std.Thread.Condition`. Blocking synchronization now lives behind the
// `std.Io` interface, whose `lock`/`wait`/`signal` operations take an `Io`
// parameter. Rather than thread an `Io` handle through every call site (and
// every struct that owns a lock), these thin wrappers preserve the previous
// zero-argument API by routing through the process-global threaded executor.
//
// The uncancelable futex paths (`lockUncancelable`, `waitUncancelable`,
// `unlock`, `signal`, `broadcast`) ignore the executor instance and dispatch
// straight to the OS futex, so the statically-initialized
// `global_single_threaded` executor is sufficient here. Its name refers to the
// executor's async/concurrency capability, not the build mode — the Warden
// binary is multi-threaded, so the futex actually parks and wakes threads.

const std = @import("std");

inline fn gio() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

/// Blocking mutual exclusion lock with a zero-argument lock/unlock API.
pub const Mutex = struct {
    inner: std.Io.Mutex = .init,

    pub fn lock(m: *Mutex) void {
        m.inner.lockUncancelable(gio());
    }

    pub fn tryLock(m: *Mutex) bool {
        return m.inner.tryLock();
    }

    pub fn unlock(m: *Mutex) void {
        m.inner.unlock(gio());
    }
};

/// Condition variable paired with `Mutex`, with a zero-argument wait/signal API.
pub const Condition = struct {
    inner: std.Io.Condition = .init,

    /// Atomically release `mutex` and block until signalled, then re-acquire it.
    pub fn wait(c: *Condition, mutex: *Mutex) void {
        c.inner.waitUncancelable(gio(), &mutex.inner);
    }

    pub fn signal(c: *Condition) void {
        c.inner.signal(gio());
    }

    pub fn broadcast(c: *Condition) void {
        c.inner.broadcast(gio());
    }
};

test "Mutex serializes concurrent increments" {
    const Shared = struct {
        m: Mutex = .{},
        counter: u64 = 0,
        fn bump(s: *@This()) void {
            var i: usize = 0;
            while (i < 10_000) : (i += 1) {
                s.m.lock();
                s.counter += 1;
                s.m.unlock();
            }
        }
    };
    var shared = Shared{};
    var threads: [8]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Shared.bump, .{&shared});
    for (threads) |t| t.join();
    try std.testing.expectEqual(@as(u64, 8 * 10_000), shared.counter);
}

test "tryLock reflects held state" {
    var m = Mutex{};
    try std.testing.expect(m.tryLock());
    try std.testing.expect(!m.tryLock());
    m.unlock();
    try std.testing.expect(m.tryLock());
    m.unlock();
}

test "Condition wakes a waiter" {
    const Box = struct {
        m: Mutex = .{},
        c: Condition = .{},
        ready: bool = false,
        fn waiter(b: *@This()) void {
            b.m.lock();
            defer b.m.unlock();
            while (!b.ready) b.c.wait(&b.m);
        }
    };
    var box = Box{};
    const t = try std.Thread.spawn(.{}, Box.waiter, .{&box});
    box.m.lock();
    box.ready = true;
    box.c.signal();
    box.m.unlock();
    t.join();
    try std.testing.expect(box.ready);
}
