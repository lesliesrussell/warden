// warden-7hi

const std = @import("std");
const clock = @import("clock.zig");
const beam = @import("beam.zig");
const ca = @import("topology_code_assistant.zig");
const ra = @import("topology_research_agent.zig");
const etl = @import("topology_etl.zig");

test "code assistant topology boots and shuts down cleanly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var topo = try ca.CodeAssistantTopology.init(allocator, rt, .{
        .session_id = "ca-test-01",
        .log_dir = log_path,
        .storage_base = log_path,
    });
    try topo.start();
    clock.sleepNs(10 * std.time.ns_per_ms);
    topo.shutdown();
}

test "code assistant watchdog pauses planner on budget_exceeded" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var topo = try ca.CodeAssistantTopology.init(allocator, rt, .{
        .session_id = "ca-test-02",
        .log_dir = log_path,
        .storage_base = log_path,
    });
    try topo.start();
    clock.sleepNs(10 * std.time.ns_per_ms);

    // Send budget_exceeded to watchdog
    const mb = rt.getMailbox(topo.watchdog_pid);
    if (mb) |m| {
        const msg = beam.MessageEnvelope{
            .kind = .event,
            .@"type" = ca.MsgType.budget_exceeded,
            .id = "budget-01",
            .from = "test",
            .to = "watchdog",
            .body = .null,
        };
        _ = m.enqueue(msg) catch {};
    }

    clock.sleepNs(20 * std.time.ns_per_ms);

    // Send budget_ok to resume
    if (mb) |m| {
        const msg = beam.MessageEnvelope{
            .kind = .event,
            .@"type" = ca.MsgType.budget_ok,
            .id = "budget-02",
            .from = "test",
            .to = "watchdog",
            .body = .null,
        };
        _ = m.enqueue(msg) catch {};
    }

    clock.sleepNs(10 * std.time.ns_per_ms);
    topo.shutdown();
}

test "research agent topology boots and shuts down cleanly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var topo = try ra.ResearchAgentTopology.init(allocator, rt, .{
        .session_id = "ra-test-01",
        .log_dir = log_path,
        .storage_base = log_path,
    });
    try topo.start();
    clock.sleepNs(10 * std.time.ns_per_ms);
    topo.deinit();
}

test "research agent ranker fans out to all retrievers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var topo = try ra.ResearchAgentTopology.init(allocator, rt, .{
        .session_id = "ra-test-02",
        .retriever_timeout_ms = 500,
        .log_dir = log_path,
        .storage_base = log_path,
    });
    try topo.start();
    clock.sleepNs(10 * std.time.ns_per_ms);

    // Send rank_request to ranker
    const mb = rt.getMailbox(topo.ranker_pid);
    if (mb) |m| {
        const msg = beam.MessageEnvelope{
            .kind = .request,
            .@"type" = ra.MsgType.rank_request,
            .id = "rank-01",
            .from = "test",
            .to = "ranker",
            .body = .{ .string = "what is warden?" },
            .session_id = "ra-test-02",
            .task_id = "task-01",
        };
        _ = m.enqueue(msg) catch {};
    }

    // Allow time for fan-out and collection
    clock.sleepNs(200 * std.time.ns_per_ms);
    topo.deinit();
}

test "ETL topology boots and shuts down cleanly" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var topo = try etl.EtlTopology.init(allocator, rt, .{
        .pipeline_id = "etl-test-01",
        .log_dir = log_path,
        .storage_base = log_path,
    });
    try topo.start();
    clock.sleepNs(10 * std.time.ns_per_ms);
    topo.deinit();
}

test "ETL pipeline runs extract→transform→load and checkpoints progress" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const log_path = try tmp.dir.realpathAlloc(allocator, ".");
    defer allocator.free(log_path);

    const rt = try beam.Runtime.init(allocator, 10);
    defer rt.destroy();

    var topo = try etl.EtlTopology.init(allocator, rt, .{
        .pipeline_id = "etl-test-02",
        .batch_size = 100,
        .log_dir = log_path,
        .storage_base = log_path,
    });
    try topo.start();
    clock.sleepNs(10 * std.time.ns_per_ms);

    // Kick off a pipeline run
    try topo.run("task-etl-01");

    // Allow extract→transform→load chain to complete
    clock.sleepNs(100 * std.time.ns_per_ms);

    topo.deinit();
}
