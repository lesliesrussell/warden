# Warden

A process virtual machine for supervised AI agents, implemented in Zig.

Warden runs agents, tools, models, memory services, and foreign workers as supervised, observable, policy-governed processes with bounded execution and durable operational narratives.

## Overview

Warden is modeled on the BEAM/OTP process model: every unit of work runs as an isolated process with a mailbox, a policy envelope, a structured log stream, and a storage namespace view. Supervisors own and restart children. Nothing shares mutable memory.

The runtime is optimized for AI-agent systems — orchestration services, tool execution, workflow engines, and durable background services that require concurrency, failure isolation, introspection, and bounded governance.

## Architecture

| Subsystem | File | Responsibility |
|---|---|---|
| Core types | `src/types.zig` | `Pid`, `ProcessKind`, `ProcessState`, `PolicyEnvelope`, `MessageEnvelope` |
| Process registry | `src/registry.zig` | PID allocation, lifecycle state machine, thread-safe process map |
| Mailbox | `src/mailbox.zig` | MPSC queue, selective receive, overflow strategies |
| Scheduler | `src/scheduler.zig` | Thread-pool, ready/wait/hibernating/paused queues, reduction-slice preemption |
| Supervision | `src/supervisor.zig` | Child specs, restart strategies, max restart intensity |
| Policy engine | `src/policy.zig` | Quota enforcement, activity classes, promotion/demotion with TTL |
| Logging engine | `src/logger.zig` | Per-process append-only NDJSON log stream |
| Storage engine | `src/storage.zig` | `proc-temp`, `proc-cache`, `proc-state`, `shared-vol` namespace mediation |
| Public API | `src/beam.zig` | `Runtime`, `Ctx`, and the full `beam.*` facade |
| Foreign bridge | `src/bridge.zig` | Unix socket protocol for Python/TypeScript workers |
| Agent topology | `src/topology.zig` | Canonical supervisor tree for AI-agent sessions |

## Process model

Every process has:

- An opaque `Pid` (`beam` + monotonic `proc` counter)
- A mailbox for asynchronous message passing
- A `PolicyEnvelope` governing quotas and activity class
- An append-only NDJSON log stream (`<beam>-<pid>.log`)
- A storage namespace view (`proc-temp`, `proc-cache`, `proc-state`)

Processes communicate by sending `MessageEnvelope` values to PIDs. The sender never blocks and never touches recipient memory. Received messages are removed from the mailbox and recorded in the process log.

## Public API

```zig
const beam = @import("beam.zig");

// Start the runtime
const rt = try beam.Runtime.init(allocator, beam_id);
defer rt.destroy();

// Create a per-process context
var ctx = try beam.Ctx.init(rt, pid, log_dir, storage_base);
defer ctx.deinit();

// Process APIs
const my_pid = ctx.self_();
try ctx.send(other_pid, msg);
const reply = try ctx.call(other_pid, msg, 5000);
const msg = try ctx.recv(match_fn, 1000);
const child = try ctx.spawn(.native_worker, entry_fn, .{});
try ctx.promote(pid, .elevated, 30_000, "handling request");

// Logging
try ctx.note("task started", null);
try ctx.metric("latency_ms", 42.0, null);
try ctx.warning("retrying", null);

// Storage
try ctx.fsWrite(.proc_temp, "scratch.bin", data);
const bytes = try ctx.fsRead(.proc_state, "checkpoint");
```

## Supervisor restart strategies

| Strategy | Behavior |
|---|---|
| `one_for_one` | Restart only the failed child |
| `one_for_all` | Restart all children when one fails |
| `rest_for_one` | Restart failed child and all children started after it |
| `transient` | Restart only on abnormal exit |
| `temporary` | Never restart |

## Storage namespaces

| Namespace | Lifecycle |
|---|---|
| `proc-temp` | Deleted automatically on process exit |
| `proc-cache` | Evictable by quota; no correctness guarantee |
| `proc-state` | Durable across restart per policy |
| `shared-vol` | Named shared datasets; ACL + volume quota governed |

## Foreign workers

Foreign workers (Python, TypeScript, etc.) participate through the same contracts as native processes. The bridge speaks a length-prefixed JSON frame protocol over a Unix domain socket.

```python
# bridge_client.py
from bridge_client import BeamCtx

beam = BeamCtx()  # connects via WARDEN_SOCKET env var
beam.note("worker_started")

while True:
    msg = beam.recv(timeout_ms=5000)
    if msg is None:
        continue
    result = run_task(msg)
    beam.reply(msg["reply_to"], "res.ok", new_id(), msg["id"], {"result": result})
```

Start a foreign worker from Zig:

```zig
var bridge_sup = try bridge.BridgeSupervisor.init(allocator, runtime);
const worker_pid = try bridge_sup.spawnWorker(&.{"python3", "worker.py"}, log_dir, storage_base);
```

## Canonical agent topology

```
root_sup
└── session_sup(session_id)
    ├── planner_proc
    ├── executor_sup
    │   └── tool_worker_proc × N
    ├── memory_proc
    ├── model_router_proc
    └── watchdog_proc
```

```zig
const topo = try topology.Topology.init(allocator, runtime, .{
    .session_id = "sess_01",
    .tool_worker_count = 4,
    .log_dir = "logs",
    .storage_base = "storage",
});
try topo.start();
defer topo.shutdown() catch {};
```

## Build and test

Requires Zig 0.15.2.

```bash
zig build          # build
zig build test     # run all 81 tests
zig build run      # run the example binary
```

## Specification

The full runtime specification is in [`warden-runtime-spec.md`](warden-runtime-spec.md).
