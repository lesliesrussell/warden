// warden-7hi
//
// ETL pipeline topology — durable, restartable data pipeline.
//
// Shape:
//   root_sup
//   └── pipeline_sup (rest_for_one)
//       ├── extractor_proc     — pulls data from source, writes to proc-temp
//       ├── transformer_proc   — reads proc-temp, applies transforms, writes output
//       ├── loader_proc        — reads transformed data, loads to destination
//       └── progress_proc      — aggregates stage metrics, exposes health
//
// Design notes:
//   - rest_for_one strategy: if transformer fails, loader also restarts (it
//     can't load data that wasn't transformed). Extractor is unaffected.
//   - Each stage checkpoints its progress to proc-state. After a restart,
//     the stage reads its checkpoint and resumes from where it left off —
//     no data is re-extracted or re-transformed unnecessarily.
//   - progress_proc tracks rows_extracted / rows_transformed / rows_loaded
//     as metrics in its process log, giving an external observer a
//     pipeline health timeline queryable from logs alone.

const std = @import("std");
const beam = @import("beam.zig");
const supervisor_mod = @import("supervisor.zig");

pub const Pid = beam.Pid;
pub const Runtime = beam.Runtime;
pub const Ctx = beam.Ctx;
pub const Supervisor = supervisor_mod.Supervisor;
pub const MessageEnvelope = beam.MessageEnvelope;

// warden-7hi
pub const MsgType = struct {
    pub const extract_start = "etl.extract_start";
    pub const extract_done = "etl.extract_done";
    pub const transform_start = "etl.transform_start";
    pub const transform_done = "etl.transform_done";
    pub const load_start = "etl.load_start";
    pub const load_done = "etl.load_done";
    pub const progress_report = "etl.progress_report";
    pub const shutdown = "etl.shutdown";
};

// warden-7hi
pub const Config = struct {
    pipeline_id: []const u8,
    batch_size: u32 = 1000,
    log_dir: []const u8,
    storage_base: []const u8,
};

// warden-7hi
pub const WorkerCtx = struct {
    ctx: Ctx,
    topology: *EtlTopology,
    ready: std.atomic.Value(bool),
    stopping: std.atomic.Value(bool),
    exited: std.atomic.Value(bool),
    thread: ?std.Thread,
};

fn matchAny(msg: *const MessageEnvelope) bool {
    _ = msg;
    return true;
}

// warden-7hi
// Extractor: pulls data from source. Checkpoints offset to proc-state so
// a restart resumes from the last committed position, not from the beginning.
fn extractorThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("extractor: ready", null) catch {};

    // Recover checkpoint from proc-state (offset of last committed batch)
    // If no checkpoint exists, start from offset 0.
    var offset: u64 = 0;
    if (ctx.fsRead(.proc_state, "offset")) |saved| {
        offset = std.fmt.parseInt(u64, saved, 10) catch 0;
        ctx.runtime.allocator.free(saved);
        ctx.note("extractor: resumed from checkpoint", null) catch {};
    } else |_| {}

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.extract_start)) {
            // TODO: replace with real data source read
            ctx.note("extractor: extracting batch", null) catch {};
            std.Thread.sleep(5 * std.time.ns_per_ms);

            // Write extracted batch to proc-temp for transformer to read
            ctx.fsWrite(.proc_temp, "batch.json", "[]") catch {};

            // Commit checkpoint
            offset += topo.config.batch_size;
            const offset_str = std.fmt.allocPrint(topo.allocator, "{d}", .{offset}) catch continue;
            defer topo.allocator.free(offset_str);
            ctx.fsWrite(.proc_state, "offset", offset_str) catch {};

            // Signal transformer
            const done = MessageEnvelope{
                .kind = .event,
                .@"type" = MsgType.extract_done,
                .id = "extract-done",
                .from = "extractor",
                .to = "transformer",
                .body = .{ .string = "batch.json" },
                .task_id = msg.task_id,
            };
            ctx.send(topo.transformer_pid, done) catch {};
            ctx.metric("rows_extracted", @as(f64, @floatFromInt(offset)), null) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Transformer: reads extracted batch from proc-temp, applies transforms,
// writes output to proc-temp for loader. Checkpoints transform position.
fn transformerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("transformer: ready", null) catch {};

    var rows_transformed: u64 = 0;

    // Recover checkpoint
    if (ctx.fsRead(.proc_state, "rows_transformed")) |saved| {
        rows_transformed = std.fmt.parseInt(u64, saved, 10) catch 0;
        ctx.runtime.allocator.free(saved);
        ctx.note("transformer: resumed from checkpoint", null) catch {};
    } else |_| {}

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.extract_done)) {
            ctx.note("transformer: transforming batch", null) catch {};
            std.Thread.sleep(3 * std.time.ns_per_ms);

            // Read from proc-temp, write transformed output
            const data = ctx.fsRead(.proc_temp, "batch.json") catch continue;
            defer topo.allocator.free(data);
            ctx.fsWrite(.proc_temp, "transformed.json", data) catch {};

            rows_transformed += topo.config.batch_size;

            // Commit checkpoint
            const rows_str = std.fmt.allocPrint(topo.allocator, "{d}", .{rows_transformed}) catch continue;
            defer topo.allocator.free(rows_str);
            ctx.fsWrite(.proc_state, "rows_transformed", rows_str) catch {};

            // Signal loader
            const done = MessageEnvelope{
                .kind = .event,
                .@"type" = MsgType.transform_done,
                .id = "transform-done",
                .from = "transformer",
                .to = "loader",
                .body = .{ .string = "transformed.json" },
                .task_id = msg.task_id,
            };
            ctx.send(topo.loader_pid, done) catch {};
            ctx.metric("rows_transformed", @as(f64, @floatFromInt(rows_transformed)), null) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Loader: reads transformed data and loads to destination.
// Checkpoints rows_loaded so a restart doesn't re-load committed rows.
fn loaderThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("loader: ready", null) catch {};

    var rows_loaded: u64 = 0;

    // Recover checkpoint
    if (ctx.fsRead(.proc_state, "rows_loaded")) |saved| {
        rows_loaded = std.fmt.parseInt(u64, saved, 10) catch 0;
        ctx.runtime.allocator.free(saved);
        ctx.note("loader: resumed from checkpoint", null) catch {};
    } else |_| {}

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.transform_done)) {
            ctx.note("loader: loading batch to destination", null) catch {};
            // TODO: replace with real destination write (DB, S3, etc.)
            std.Thread.sleep(2 * std.time.ns_per_ms);

            rows_loaded += topo.config.batch_size;

            // Commit checkpoint
            const rows_str = std.fmt.allocPrint(topo.allocator, "{d}", .{rows_loaded}) catch continue;
            defer topo.allocator.free(rows_str);
            ctx.fsWrite(.proc_state, "rows_loaded", rows_str) catch {};

            ctx.metric("rows_loaded", @as(f64, @floatFromInt(rows_loaded)), null) catch {};

            // Notify progress proc
            const prog = MessageEnvelope{
                .kind = .event,
                .@"type" = MsgType.load_done,
                .id = "load-done",
                .from = "loader",
                .to = "progress",
                .body = .{ .integer = @as(i64, @intCast(rows_loaded)) },
                .task_id = msg.task_id,
            };
            ctx.send(topo.progress_pid, prog) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Progress proc: aggregates stage metrics and emits pipeline health to log.
// Queryable from logs alone — no separate metrics store needed.
fn progressThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("progress: ready", null) catch {};

    var total_loaded: u64 = 0;

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 100) catch null;
        const msg = msg_opt orelse {
            // Periodic health tick
            ctx.metric("pipeline_rows_loaded", @as(f64, @floatFromInt(total_loaded)), null) catch {};
            continue;
        };
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.load_done)) {
            if (msg.body == .integer) {
                total_loaded = @as(u64, @intCast(msg.body.integer));
            }
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
pub const EtlTopology = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    config: Config,

    pipeline_sup: Supervisor,

    extractor_pid: Pid,
    transformer_pid: Pid,
    loader_pid: Pid,
    progress_pid: Pid,

    workers: [4]*WorkerCtx,

    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, config: Config) !EtlTopology {
        return EtlTopology{
            .allocator = allocator,
            .runtime = runtime,
            .config = config,
            .pipeline_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler),
            .extractor_pid = undefined,
            .transformer_pid = undefined,
            .loader_pid = undefined,
            .progress_pid = undefined,
            .workers = undefined,
        };
    }

    pub fn deinit(self: *EtlTopology) void {
        for (self.workers) |wctx| {
            wctx.stopping.store(true, .release);
        }
        for (self.workers) |wctx| {
            if (wctx.thread) |t| t.join();
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
        }
        self.pipeline_sup.deinit();
    }

    pub fn start(self: *EtlTopology) !void {
        const spawnWorker = struct {
            fn do(topo: *EtlTopology, idx: usize, pid_field: *Pid, comptime thread_fn: fn (*WorkerCtx) void) !void {
                const wctx = try topo.allocator.create(WorkerCtx);
                wctx.topology = topo;
                wctx.ready = std.atomic.Value(bool).init(false);
                wctx.stopping = std.atomic.Value(bool).init(false);
                wctx.exited = std.atomic.Value(bool).init(false);
                wctx.thread = null;
                const pid = try topo.runtime.registry.spawn(.native_worker, null, .{});
                pid_field.* = pid;
                topo.workers[idx] = wctx;
                try topo.runtime.allocMailbox(pid, .{});
                wctx.ctx = try Ctx.init(topo.runtime, pid, topo.config.log_dir, topo.config.storage_base);
                wctx.ready.store(true, .release);
                wctx.thread = try std.Thread.spawn(.{}, thread_fn, .{wctx});
            }
        }.do;

        try spawnWorker(self, 0, &self.extractor_pid, extractorThread);
        try spawnWorker(self, 1, &self.transformer_pid, transformerThread);
        try spawnWorker(self, 2, &self.loader_pid, loaderThread);
        try spawnWorker(self, 3, &self.progress_pid, progressThread);
    }

    // Kick off a pipeline run by sending extract_start to the extractor.
    pub fn run(self: *EtlTopology, task_id: []const u8) !void {
        const mb = self.runtime.getMailbox(self.extractor_pid) orelse return;
        const msg = MessageEnvelope{
            .kind = .request,
            .@"type" = MsgType.extract_start,
            .id = "run-01",
            .from = "coordinator",
            .to = "extractor",
            .body = .null,
            .task_id = task_id,
        };
        _ = mb.enqueue(msg) catch {};
    }
};
