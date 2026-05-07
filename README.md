# Warden

**Execution-time control for AI agents** — implemented in Zig.

---

## The problem with today's agent stacks

Current agent frameworks (LangChain, CrewAI, raw asyncio) hand control to the model and hope for the best. When something goes wrong, there is no runtime authority to intervene:

- A tool worker calls an HTTP endpoint with no timeout → the entire agent silently stalls
- A context window grows without bound → the model hallucinates or the API returns an error
- A memory service leaks state between sessions → subtle data corruption, no observable cause
- A subprocess emits runaway logs → disk fills, no quota enforcement, no alerting

The only recovery is killing the whole process and starting over.

**Warden is the layer between your agent code and chaos.** Every process — planner, tool worker, memory service, model router — runs under supervision with bounded execution, structured observation, and runtime authority to pause, restart, or quarantine any component without disturbing the session.

---

## What you can observe

Every process emits an append-only NDJSON log stream. Every message received, tool call made, storage write issued, and policy change applied is recorded with a trace ID. No printf debugging — the full operational narrative is always queryable.

```jsonl
{"ts":1746582311.042,"beam":10,"pid":5,"seq":1,"event":"spawn","kind":"native_worker"}
{"ts":1746582311.043,"beam":10,"pid":5,"seq":2,"event":"recv","msg_id":"req-01","from":"planner","type":"req.shell","task_id":"task-42"}
{"ts":1746582311.043,"beam":10,"pid":5,"seq":3,"event":"tool_call","tool":"shell","input":"ls -la","corr":"req-01"}
{"ts":1746582311.051,"beam":10,"pid":5,"seq":4,"event":"tool_result","tool":"shell","success":true,"corr":"req-01"}
{"ts":1746582311.051,"beam":10,"pid":5,"seq":5,"event":"send","to":"planner","type":"res.ok","corr":"req-01"}
{"ts":1746582311.052,"beam":10,"pid":5,"seq":6,"event":"note","msg":"task complete","task_id":"task-42"}
```

A watchdog reading `last_active_at` from the registry can detect a hung tool worker in milliseconds — not after a user timeout.

---

## What you can control

The runtime exposes direct authority over any process. A watchdog proc can intervene without touching the session supervisor:

```zig
// Pause a worker that exceeded its log quota — without restarting the session.
try ctx.pause(worker_pid);
ctx.note("watchdog: worker paused — log quota exceeded", null) catch {};

// Wait for cooldown, then resume.
std.Thread.sleep(cooldown_ns);
ctx.note("watchdog: worker resumed — cooldown elapsed", null) catch {};
try ctx.resume_(worker_pid);

// Promote a planner to elevated priority for a latency-sensitive request.
try ctx.promote(planner_pid, .elevated, 30_000, "latency-sensitive request");

// Quarantine a foreign worker that stopped responding.
try ctx.runtime.policy.quarantine(worker_pid, "idle_timeout");
```

The session supervisor PID never changes. The user never sees an interruption.

---

## Runnable demos

### Hung worker recovery

A tool worker calls an HTTP endpoint with no timeout and blocks forever. Warden's watchdog detects the idle gap and restarts it:

```
[session]  task_1 dispatched → worker pid:10/5
[worker]   task_1 received — calling external API (no timeout)...
[watchdog] worker pid:10/5 idle 150ms > budget — quarantining
[executor] pid:10/5 killed — restarting (one_for_one)
[session]  new worker pid:10/8 ready
[session]  task_2 dispatched → worker pid:10/8
[worker]   task_2 completed in 3ms
[session]  session supervisor pid:10/3 unchanged throughout
```

See `examples/failure_recovery/` and `src/failure_recovery_test.zig`.

### Watchdog intervention (pause/resume)

A tool worker emits excessive log volume. The watchdog pauses it, waits for a cooldown, then resumes — without restarting the session:

```
[worker]   tool_worker: starting task
[worker]   tool_worker: fetching data
[worker]   tool_worker: processing results     ← quota exceeded after this
[watchdog] watchdog: pausing tool_worker — log quota exceeded
           (worker silent for 80ms cooldown)
[watchdog] watchdog: resuming tool_worker — cooldown elapsed
[worker]   tool_worker: resumed after watchdog pause
```

See `examples/watchdog_intervention/` and `src/watchdog_intervention_test.zig`.

### Live demo — Python workers under Zig supervision

Two Python workers run as supervised children of a Zig supervisor: a math service
(Fibonacci, primes) and an HTTP server. The test drives the full lifecycle:

```
[registry]  math_worker and web_server appear in proc.list
[topology]  supervisor has 2 children
[messaging] req.fib(10) → res.ok body=55   (round-trip through ForeignBridge)
[http]      GET /status → HTTP 200 {"status":"ok"}
[lifecycle] pause math_worker → state=paused; resume → state=running
```

See `examples/live_demo/` and `src/live_demo_test.zig`.

---

## wardenctl — runtime CLI

`wardenctl` is a standalone CLI for inspecting and controlling a live Warden runtime over its Unix socket.

```bash
wardenctl [--socket <path>] [--json] <command>

Commands:
  beams                    List active beams
  ps [--beam N] [--kind K] [--state S]
                           List processes with optional filters
  topology [--beam N]      Show supervisor tree (ASCII)
  logs <beam/proc> [--since 10s] [--grep pattern] [--follow]
                           Stream per-process NDJSON log
  pause <beam/proc>        Pause a process
  resume <beam/proc>       Resume a paused process
  kill <beam/proc> --force [--reason '...']
                           Transition process to exiting
  quarantine <beam/proc> --force [--reason '...']
                           Restrict process to minimum resources
  promote <beam/proc> [--class elevated] [--ttl 30s] [--reason '...']
                           Promote process activity class
```

The runtime exposes a Unix socket at `~/.warden/ctrl.sock` by default (override with `$WARDEN_CTRL_SOCKET` or `--socket`).

---

## Reference topologies

Three concrete supervisor trees showing how Warden maps to real agent architectures:

| Topology | File | Shape |
|---|---|---|
| Code assistant | `src/topology_code_assistant.zig` | planner + executor_sup (shell/lsp/file) + memory + watchdog |
| Research agent | `src/topology_research_agent.zig` | ranker + retriever_sup (web/vector/doc) + synthesizer + citations |
| ETL pipeline | `src/topology_etl.zig` | extractor → transformer → loader (rest_for_one, checkpointed) |

---

## Python SDK

Tool workers written in Python participate through the same contracts as native processes:

```python
import warden

ctx = warden.BeamCtx()          # connects via $WARDEN_SOCKET

@warden.tool                     # auto-logs tool_call / tool_result events
def run_shell(ctx, msg):
    import subprocess
    return subprocess.run(msg["body"], shell=True, capture_output=True, text=True).stdout

warden.run_loop(ctx, {
    "req.shell": run_shell,      # dispatches by type, sends res.ok / res.error
})
```

See `warden/` for the SDK and `examples/python_worker/` for a complete example.

---

## Internals

### Architecture

| Subsystem | File | Responsibility |
|---|---|---|
| Core types | `src/types.zig` | `Pid`, `ProcessKind`, `ProcessState`, `PolicyEnvelope`, `MessageEnvelope` |
| Process registry | `src/registry.zig` | PID allocation, lifecycle state machine, `last_active_at` tracking |
| Mailbox | `src/mailbox.zig` | MPSC queue, selective receive, overflow strategies |
| Scheduler | `src/scheduler.zig` | Thread pool, ready/wait/hibernating/paused queues |
| Supervision | `src/supervisor.zig` | Child specs, restart strategies, max restart intensity |
| Policy engine | `src/policy.zig` | Quota enforcement, activity classes, promotion/demotion/quarantine |
| Logging engine | `src/logger.zig` | Per-process append-only NDJSON stream |
| Storage engine | `src/storage.zig` | `proc-temp`, `proc-cache`, `proc-state`, `shared-vol` namespace mediation |
| Public API | `src/beam.zig` | `Runtime`, `Ctx`, and the full `beam.*` facade |
| Foreign bridge | `src/bridge.zig` | Length-prefixed JSON frames over Unix domain socket |

### Process model

Every process has:
- An opaque `Pid` (`beam` + monotonic `proc` counter)
- A mailbox for asynchronous message passing (sender never blocks, never touches recipient memory)
- A `PolicyEnvelope` governing quotas and activity class
- An append-only NDJSON log stream (`<beam>-<pid>.log`)
- A storage namespace view (`proc-temp`, `proc-cache`, `proc-state`)

### Supervisor restart strategies

| Strategy | Behavior |
|---|---|
| `one_for_one` | Restart only the failed child |
| `one_for_all` | Restart all children when one fails |
| `rest_for_one` | Restart failed child + all started after it |
| `transient` | Restart only on abnormal exit |
| `temporary` | Never restart |

### Storage namespaces

| Namespace | Lifecycle |
|---|---|
| `proc-temp` | Deleted on process exit |
| `proc-cache` | Evictable by quota; no correctness guarantee |
| `proc-state` | Durable across restart per policy |
| `shared-vol` | Named shared datasets; ACL + volume quota governed |

---

## Public API

```zig
const beam = @import("beam.zig");

const rt = try beam.Runtime.init(allocator, beam_id);
defer rt.destroy();

var ctx = try beam.Ctx.init(rt, pid, log_dir, storage_base);
defer ctx.deinit();

// Messaging
try ctx.send(other_pid, msg);
const reply = try ctx.call(other_pid, msg, 5000);
const incoming = try ctx.recv(match_fn, 1000);

// Supervision authority
try ctx.pause(worker_pid);
try ctx.resume_(worker_pid);
try ctx.promote(pid, .elevated, 30_000, "reason");

// Logging
try ctx.note("task started", null);
try ctx.metric("latency_ms", 42.0, null);
try ctx.warning("retrying", null);

// Storage
try ctx.fsWrite(.proc_temp, "scratch.bin", data);
const bytes = try ctx.fsRead(.proc_state, "checkpoint");
```

---

## Build and test

Requires Zig 0.15.2.

```bash
zig build test     # run all 106 tests
zig build          # compile wardenctl + runtime library
```

---

## Specification

[`warden-runtime-spec.md`](warden-runtime-spec.md)
