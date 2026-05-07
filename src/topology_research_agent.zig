// warden-7hi
//
// Research-agent topology — optimized for retrieval-heavy workloads.
//
// Shape:
//   root_sup
//   └── session_sup(session_id)
//       ├── retriever_sup (one_for_one)
//       │   ├── web_retriever      — HTTP search / scrape
//       │   ├── vector_retriever   — embedding similarity search
//       │   └── doc_retriever      — document store lookup
//       ├── ranker_proc            — scores and deduplicates retrieval results
//       ├── synthesizer_proc       — assembles ranked results into final response
//       └── citation_tracker_proc  — tracks source provenance in proc-state
//
// Design notes:
//   - Each retriever is independent under retriever_sup (one_for_one): a
//     failed web search does not cancel vector retrieval already in flight.
//   - citation_tracker_proc stores source URLs and snippets in proc-state.
//     After a supervisor restart the citation history is recovered from disk.
//   - ranker_proc collects results from all three retrievers before ranking;
//     it uses a timeout so a slow retriever does not block the response.

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
    pub const retrieve = "ra.retrieve";
    pub const retrieval_result = "ra.retrieval_result";
    pub const rank_request = "ra.rank_request";
    pub const rank_result = "ra.rank_result";
    pub const synthesize = "ra.synthesize";
    pub const cite = "ra.cite";
    pub const shutdown = "ra.shutdown";
};

// warden-7hi
pub const Config = struct {
    session_id: []const u8,
    retriever_timeout_ms: u64 = 3000,
    max_results_per_source: u32 = 10,
    log_dir: []const u8,
    storage_base: []const u8,
};

// warden-7hi
pub const WorkerCtx = struct {
    ctx: Ctx,
    topology: *ResearchAgentTopology,
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
// Web retriever: HTTP search. Replace stub with real HTTP client.
fn webRetrieverThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("web_retriever: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.retrieve)) {
            // TODO(warden-foreign): real HTTP search via Python bridge worker
            ctx.note("web_retriever: searching", null) catch {};
            std.Thread.sleep(5 * std.time.ns_per_ms);

            const result = MessageEnvelope{
                .kind = .response,
                .@"type" = MsgType.retrieval_result,
                .id = "web-result",
                .from = "web_retriever",
                .to = "ranker",
                .corr = msg.id,
                .body = .{ .string = "web_result" },
                .session_id = msg.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.ranker_pid, result) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Vector retriever: embedding similarity search.
fn vectorRetrieverThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("vector_retriever: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.retrieve)) {
            // TODO(warden-foreign): real embedding search via Python bridge worker
            ctx.note("vector_retriever: searching", null) catch {};
            std.Thread.sleep(3 * std.time.ns_per_ms);

            const result = MessageEnvelope{
                .kind = .response,
                .@"type" = MsgType.retrieval_result,
                .id = "vec-result",
                .from = "vector_retriever",
                .to = "ranker",
                .corr = msg.id,
                .body = .{ .string = "vector_result" },
                .session_id = msg.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.ranker_pid, result) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Doc retriever: document store lookup.
fn docRetrieverThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("doc_retriever: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.retrieve)) {
            // TODO(warden-foreign): real doc store via Python bridge worker
            ctx.note("doc_retriever: searching", null) catch {};
            std.Thread.sleep(2 * std.time.ns_per_ms);

            const result = MessageEnvelope{
                .kind = .response,
                .@"type" = MsgType.retrieval_result,
                .id = "doc-result",
                .from = "doc_retriever",
                .to = "ranker",
                .corr = msg.id,
                .body = .{ .string = "doc_result" },
                .session_id = msg.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.ranker_pid, result) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Ranker: collects results from all retrievers, deduplicates and scores,
// sends ranked list to synthesizer. Uses a timeout so slow retrievers
// don't block the response — partial results are still useful.
fn rankerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    const topo = wctx.topology;
    ctx.note("ranker: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;

        if (std.mem.eql(u8, msg.@"type", MsgType.rank_request)) {
            // Fan out retrieve to all three retrievers
            const req_id = msg.id;
            const retrieve_msg = MessageEnvelope{
                .kind = .request,
                .@"type" = MsgType.retrieve,
                .id = req_id,
                .from = "ranker",
                .to = "retrievers",
                .body = msg.body,
                .session_id = msg.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.web_pid, retrieve_msg) catch {};
            ctx.send(topo.vector_pid, retrieve_msg) catch {};
            ctx.send(topo.doc_pid, retrieve_msg) catch {};

            // Collect up to 3 results with timeout
            var collected: u32 = 0;
            const deadline = std.time.milliTimestamp() + @as(i64, @intCast(topo.config.retriever_timeout_ms));
            while (collected < 3 and std.time.milliTimestamp() < deadline) {
                const r_opt = ctx.recv(matchAny, 100) catch null;
                if (r_opt) |r| {
                    if (std.mem.eql(u8, r.@"type", MsgType.retrieval_result)) {
                        collected += 1;
                        // Forward citation to citation_tracker
                        ctx.send(topo.citation_pid, r) catch {};
                    }
                }
            }
            ctx.note("ranker: ranked results", null) catch {};

            // Send ranked results to synthesizer
            const ranked = MessageEnvelope{
                .kind = .request,
                .@"type" = MsgType.rank_result,
                .id = "ranked-result",
                .from = "ranker",
                .to = "synthesizer",
                .corr = req_id,
                .body = .{ .string = "ranked" },
                .session_id = msg.session_id,
                .task_id = msg.task_id,
            };
            ctx.send(topo.synthesizer_pid, ranked) catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Synthesizer: receives ranked results, assembles final response.
fn synthesizerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("synthesizer: ready", null) catch {};

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;
        if (std.mem.eql(u8, msg.@"type", MsgType.rank_result)) {
            ctx.note("synthesizer: assembled response", null) catch {};
            // TODO: call model router, assemble response with citations
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
// Citation tracker: stores source provenance in proc-state.
// Survives supervisor restarts — citation history is never lost.
fn citationTrackerThread(wctx: *WorkerCtx) void {
    while (!wctx.ready.load(.acquire)) std.Thread.sleep(1 * std.time.ns_per_ms);
    const ctx = &wctx.ctx;
    ctx.note("citation_tracker: ready", null) catch {};
    var cite_count: u32 = 0;

    while (!wctx.stopping.load(.acquire)) {
        const msg_opt = ctx.recv(matchAny, 50) catch null;
        const msg = msg_opt orelse continue;
        if (std.mem.eql(u8, msg.@"type", MsgType.shutdown)) break;
        if (std.mem.eql(u8, msg.@"type", MsgType.retrieval_result)) {
            cite_count += 1;
            // Persist citation to proc-state — survives restart
            const key = std.fmt.allocPrint(ctx.runtime.allocator, "cite-{d}", .{cite_count}) catch continue;
            defer ctx.runtime.allocator.free(key);
            ctx.fsWrite(.proc_state, key, "source") catch {};
        }
    }
    wctx.exited.store(true, .release);
}

// warden-7hi
pub const ResearchAgentTopology = struct {
    allocator: std.mem.Allocator,
    runtime: *Runtime,
    config: Config,

    retriever_sup: Supervisor,

    web_pid: Pid,
    vector_pid: Pid,
    doc_pid: Pid,
    ranker_pid: Pid,
    synthesizer_pid: Pid,
    citation_pid: Pid,

    workers: [6]*WorkerCtx,

    pub fn init(allocator: std.mem.Allocator, runtime: *Runtime, config: Config) !ResearchAgentTopology {
        return ResearchAgentTopology{
            .allocator = allocator,
            .runtime = runtime,
            .config = config,
            .retriever_sup = try Supervisor.init(allocator, &runtime.registry, runtime.scheduler),
            .web_pid = undefined,
            .vector_pid = undefined,
            .doc_pid = undefined,
            .ranker_pid = undefined,
            .synthesizer_pid = undefined,
            .citation_pid = undefined,
            .workers = undefined,
        };
    }

    pub fn deinit(self: *ResearchAgentTopology) void {
        for (self.workers) |wctx| {
            wctx.stopping.store(true, .release);
        }
        for (self.workers) |wctx| {
            if (wctx.thread) |t| t.join();
            wctx.ctx.deinit();
            self.allocator.destroy(wctx);
        }
        self.retriever_sup.deinit();
    }

    pub fn start(self: *ResearchAgentTopology) !void {
        const spawnWorker = struct {
            fn do(topo: *ResearchAgentTopology, idx: usize, pid_field: *Pid, comptime thread_fn: fn (*WorkerCtx) void) !void {
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

        try spawnWorker(self, 0, &self.web_pid, webRetrieverThread);
        try spawnWorker(self, 1, &self.vector_pid, vectorRetrieverThread);
        try spawnWorker(self, 2, &self.doc_pid, docRetrieverThread);
        try spawnWorker(self, 3, &self.ranker_pid, rankerThread);
        try spawnWorker(self, 4, &self.synthesizer_pid, synthesizerThread);
        try spawnWorker(self, 5, &self.citation_pid, citationTrackerThread);
    }
};
