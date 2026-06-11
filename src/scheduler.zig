// warden-7a1

const std = @import("std");
const clock = @import("clock.zig");
const sync = @import("sync.zig");
const types = @import("types.zig");
const registry_mod = @import("registry.zig");

const Pid = types.Pid;
const ProcessState = types.ProcessState;
const PolicyEnvelope = types.PolicyEnvelope;
const ActivityClass = types.ActivityClass;
const Registry = registry_mod.Registry;

// warden-zl0: admission priority. The scheduler is cooperative, not preemptive —
// a running task holds its worker thread until it returns — so activity class
// affects WHICH ready task starts next when workers are saturated, not the
// time-slicing of an already-running one.
pub fn classRank(c: ActivityClass) u8 {
    return switch (c) {
        .system => 4,
        .elevated => 3,
        .normal => 2,
        .tiny => 1,
        .paused => 0,
    };
}

// warden-zl0: index of the highest-activity-class task in `items` (FIFO
// tie-break: the earliest-enqueued among equal class). Caller passes a non-empty
// slice. Unknown pids are treated as `.normal`.
pub fn pickHighestClass(items: []const Task, registry: *Registry) usize {
    var best: usize = 0;
    var best_rank: u8 = classRank(registry.activityClass(items[0].pid) orelse .normal);
    var i: usize = 1;
    while (i < items.len) : (i += 1) {
        const r = classRank(registry.activityClass(items[i].pid) orelse .normal);
        if (r > best_rank) {
            best_rank = r;
            best = i;
        }
    }
    return best;
}

// warden-7a1
/// Default maximum reductions per scheduler slice.
/// When cooperative preemption detects a task has consumed this many
/// reductions, it logs a preemption event and requeues the process.
pub const DEFAULT_MAX_REDUCTIONS: u32 = 2000;

// warden-7a1
/// A task submitted to the scheduler for execution.
pub const Task = struct {
    pid: Pid,
    entry_fn: *const fn (ctx: *anyopaque) void,
    ctx: *anyopaque,
    reductions: u32,
};

// warden-7a1
pub const WaitEntry = struct {
    pid: Pid,
    timeout_ms: ?u64,
    enqueued_at_ms: i64,
};

// warden-7a1
pub const HibernateEntry = struct {
    pid: Pid,
    resume_at_ms: ?i64, // null = wake only on message
};

// warden-7a1
/// Thread-pool scheduler for the Warden process runtime.
///
/// # Memory model
/// `init` heap-allocates the Scheduler so that worker threads always have
/// a stable pointer. Call `deinit` to stop workers, join them, and free.
///
/// # Reduction accounting (cooperative)
/// Submitted tasks run to completion within one scheduling slice.
/// Full preemption (mid-function suspension) requires ucontext/signal handlers
/// and is out of scope for this implementation.
/// TODO: Implement true preemption via ucontext when platform support is added.
pub const Scheduler = struct {
    allocator: std.mem.Allocator,
    registry: *Registry,

    // Task queue: tasks submitted for execution on worker threads.
    // Unmanaged ArrayList — allocator stored in .allocator field.
    task_queue: std.ArrayListUnmanaged(Task),

    // Ready list: Pid bookkeeping (not the execution queue).
    ready: std.ArrayListUnmanaged(Pid),

    // Waiting processes blocked on receive, optionally with timeout.
    waiting: std.AutoHashMap(u64, WaitEntry),

    // Hibernating processes.
    hibernating: std.AutoHashMap(u64, HibernateEntry),

    // Paused processes — excluded from scheduling.
    paused: std.AutoHashMap(u64, void),

    // Mutex protects all queue/map fields above.
    mutex: sync.Mutex,

    // Condition variable signals worker threads when task_queue has work.
    cond: sync.Condition,

    // Worker thread pool.
    workers: []std.Thread,

    // Set to false to signal worker threads to stop.
    running: std.atomic.Value(bool),

    // warden-7a1
    /// Allocate a Scheduler on the heap and start worker threads.
    /// The caller owns the pointer and must call deinit() to free it.
    pub fn init(allocator: std.mem.Allocator, registry: *Registry, worker_count: usize) !*Scheduler {
        const self = try allocator.create(Scheduler);
        errdefer allocator.destroy(self);

        const workers = try allocator.alloc(std.Thread, worker_count);
        errdefer allocator.free(workers);

        self.* = Scheduler{
            .allocator = allocator,
            .registry = registry,
            .task_queue = .empty,
            .ready = .empty,
            .waiting = std.AutoHashMap(u64, WaitEntry).init(allocator),
            .hibernating = std.AutoHashMap(u64, HibernateEntry).init(allocator),
            .paused = std.AutoHashMap(u64, void).init(allocator),
            .mutex = .{},
            .cond = .{},
            .workers = workers,
            .running = std.atomic.Value(bool).init(true),
        };

        // Spawn worker threads — self is heap-allocated so the pointer is stable.
        for (0..worker_count) |i| {
            workers[i] = try std.Thread.spawn(.{}, workerLoop, .{self});
        }

        return self;
    }

    // warden-7a1
    /// Stop workers, join them, and free all memory.
    pub fn deinit(self: *Scheduler) void {
        // Signal workers to stop.
        self.running.store(false, .release);
        self.mutex.lock();
        self.cond.broadcast();
        self.mutex.unlock();

        // Join all worker threads.
        for (self.workers) |w| {
            w.join();
        }

        const allocator = self.allocator;
        allocator.free(self.workers);
        self.task_queue.deinit(allocator);
        self.ready.deinit(allocator);
        self.waiting.deinit();
        self.hibernating.deinit();
        self.paused.deinit();
        allocator.destroy(self);
    }

    // warden-7a1
    /// Enqueue a process into the ready bookkeeping list.
    /// Does NOT submit a task for execution — use submit() for that.
    pub fn makeReady(self: *Scheduler, pid: Pid) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        _ = self.waiting.remove(pid.proc);
        _ = self.hibernating.remove(pid.proc);
        _ = self.paused.remove(pid.proc);
        try self.ready.append(self.allocator, pid);
        self.registry.transition(pid, .ready) catch {};
    }

    // warden-7a1
    /// Move process to waiting state (blocked on recv).
    /// If timeout_ms is non-null, tick() will move it back to ready when elapsed.
    pub fn makeWaiting(self: *Scheduler, pid: Pid, timeout_ms: ?u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        removePid(&self.ready, pid);
        const entry = WaitEntry{
            .pid = pid,
            .timeout_ms = timeout_ms,
            .enqueued_at_ms = clock.nowMs(),
        };
        try self.waiting.put(pid.proc, entry);
        self.registry.transition(pid, .waiting) catch {};
    }

    // warden-7a1
    /// Move process to hibernating state.
    /// resume_at_ms: absolute epoch-ms when to auto-wake; null = wake only on notifyMessage.
    pub fn hibernate(self: *Scheduler, pid: Pid, resume_at_ms: ?i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        removePid(&self.ready, pid);
        const entry = HibernateEntry{
            .pid = pid,
            .resume_at_ms = resume_at_ms,
        };
        try self.hibernating.put(pid.proc, entry);
        self.registry.transition(pid, .hibernating) catch {};
    }

    // warden-7a1
    /// Move process to paused state.
    pub fn pause(self: *Scheduler, pid: Pid) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        removePid(&self.ready, pid);
        try self.paused.put(pid.proc, {});
        self.registry.transition(pid, .paused) catch {};
    }

    // warden-7a1
    /// Resume a paused or hibernating process → ready.
    pub fn resume_(self: *Scheduler, pid: Pid) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        _ = self.paused.remove(pid.proc);
        _ = self.hibernating.remove(pid.proc);
        try self.ready.append(self.allocator, pid);
        self.registry.transition(pid, .ready) catch {};
    }

    // warden-7a1
    /// Called by runtime when a message arrives for pid.
    /// If the process is waiting, moves it back to ready.
    pub fn notifyMessage(self: *Scheduler, pid: Pid) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.waiting.remove(pid.proc)) {
            self.ready.append(self.allocator, pid) catch {};
            self.registry.transition(pid, .ready) catch {};
        }
    }

    // warden-7a1
    /// Tick: expire timed-out waiters and auto-wake hibernating processes past resume time.
    /// Should be called periodically (e.g. by a timer thread or main loop).
    pub fn tick(self: *Scheduler) !void {
        const now = clock.nowMs();
        self.mutex.lock();
        defer self.mutex.unlock();

        // Collect expired waiters.
        var expired_wait = std.ArrayListUnmanaged(WaitEntry).empty;
        defer expired_wait.deinit(self.allocator);

        var wait_iter = self.waiting.iterator();
        while (wait_iter.next()) |kv| {
            const entry = kv.value_ptr.*;
            if (entry.timeout_ms) |tms| {
                const elapsed = now - entry.enqueued_at_ms;
                if (elapsed >= @as(i64, @intCast(tms))) {
                    try expired_wait.append(self.allocator, entry);
                }
            }
        }

        for (expired_wait.items) |entry| {
            _ = self.waiting.remove(entry.pid.proc);
            try self.ready.append(self.allocator, entry.pid);
            self.registry.transition(entry.pid, .ready) catch {};
        }

        // Collect hibernating processes past their resume time.
        var expired_hib = std.ArrayListUnmanaged(HibernateEntry).empty;
        defer expired_hib.deinit(self.allocator);

        var hib_iter = self.hibernating.iterator();
        while (hib_iter.next()) |kv| {
            const entry = kv.value_ptr.*;
            if (entry.resume_at_ms) |resume_at| {
                if (now >= resume_at) {
                    try expired_hib.append(self.allocator, entry);
                }
            }
        }

        for (expired_hib.items) |entry| {
            _ = self.hibernating.remove(entry.pid.proc);
            try self.ready.append(self.allocator, entry.pid);
            self.registry.transition(entry.pid, .ready) catch {};
        }
    }

    // warden-7a1
    /// Submit a task to run on the thread pool.
    /// entry_fn is called with ctx as argument. Reductions are tracked cooperatively.
    ///
    /// If the process is paused, the task is silently dropped.
    ///
    /// Known limitation: tasks run to completion within one slice.
    /// TODO: Real preemption requires ucontext — not implemented.
    pub fn submit(self: *Scheduler, pid: Pid, entry_fn: *const fn (ctx: *anyopaque) void, ctx: *anyopaque) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // If paused, silently discard.
        if (self.paused.contains(pid.proc)) {
            return;
        }

        const task = Task{
            .pid = pid,
            .entry_fn = entry_fn,
            .ctx = ctx,
            .reductions = 0,
        };
        try self.task_queue.append(self.allocator, task);
        self.cond.signal();
    }

    // warden-7a1
    /// Internal: worker thread main loop.
    fn workerLoop(self: *Scheduler) void {
        while (true) {
            self.mutex.lock();

            // Wait while running and queue is empty.
            while (self.running.load(.acquire) and self.task_queue.items.len == 0) {
                self.cond.wait(&self.mutex);
            }

            // Shutdown: not running and queue is empty → exit.
            if (!self.running.load(.acquire) and self.task_queue.items.len == 0) {
                self.mutex.unlock();
                break;
            }

            // warden-zl0: dequeue the highest-activity-class ready task
            // (admission priority); FIFO within a class.
            const idx = pickHighestClass(self.task_queue.items, self.registry);
            const task = self.task_queue.orderedRemove(idx);

            // Check if process is paused before running.
            const is_paused = self.paused.contains(task.pid.proc);
            self.mutex.unlock();

            if (is_paused) {
                // Process was paused — discard this task.
                continue;
            }

            // Transition to running state (best-effort).
            self.registry.transition(task.pid, .running) catch {};

            // Cooperative reduction accounting.
            var max_reductions: u32 = DEFAULT_MAX_REDUCTIONS;
            {
                // Brief lock to read policy.
                self.mutex.lock();
                if (self.registry.lookup(task.pid)) |entry| {
                    max_reductions = entry.policy.max_reductions_per_slice;
                }
                self.mutex.unlock();
            }

            const reductions = task.reductions + 1;

            if (reductions >= max_reductions) {
                // Preemption threshold hit — log and requeue.
                // TODO: Real preemption requires ucontext — not implemented.
                std.log.debug("scheduler: preempting pid {d}/{d} after {d} reductions (max={d})", .{
                    task.pid.beam,
                    task.pid.proc,
                    reductions,
                    max_reductions,
                });
                self.mutex.lock();
                self.ready.append(self.allocator, task.pid) catch {};
                self.mutex.unlock();
                self.registry.transition(task.pid, .ready) catch {};
                continue;
            }

            // Execute the task.
            task.entry_fn(task.ctx);

            // After completion, transition back to ready (best-effort).
            self.registry.transition(task.pid, .ready) catch {};
        }
    }
};

// warden-7a1
/// Remove first matching Pid from an unmanaged list.
fn removePid(list: *std.ArrayListUnmanaged(Pid), pid: Pid) void {
    for (list.items, 0..) |p, i| {
        if (p.proc == pid.proc and p.beam == pid.beam) {
            _ = list.swapRemove(i);
            return;
        }
    }
}
