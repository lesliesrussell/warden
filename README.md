# Warden

An OTP-inspired runtime for supervising AI-agent processes — written in Zig.

---

## What it is

Warden is an OTP-inspired runtime for supervising AI-agent processes in Zig. It
borrows heavily, on purpose, from well-established supervision and
process-isolation ideas, then applies them to the kinds of mixed Zig/Python
worker trees common in modern agent systems.

A Warden system is a tree of supervised processes — planners, tool workers,
memory services, model routers — each with its own mailbox, policy envelope, and
structured log. Native processes run inside the Zig runtime; foreign processes
(today, Python) run in their own runtime and participate through a bridge using
the same process, messaging, logging, and supervision contracts. Warden is one
reasonable way to build this kind of runtime, not a claim to a new model.

---

## Prior art

Warden's core ideas are not new, and it does not claim them to be. The primary
influence is **Erlang/OTP**: supervision trees, restart strategies
(`one_for_one`, `rest_for_one`, …), per-process mailboxes, process isolation
with no shared mutable memory, and runtime-level intervention are all
established OTP concepts that Warden adopts more or less directly.

Adjacent traditions inform it as well — the **actor model** (Akka, Orleans, Ray)
for message-passing process design, and **external process/infrastructure
supervisors** (systemd, supervisord, Kubernetes) for the "something watches and
restarts your workers" pattern at a different layer.

What Warden contributes is not a new model but a particular implementation
choice: those ideas, in a small Zig runtime, with first-class supervision of
foreign-language workers, aimed at agent systems. It is one implementation among
many valid ones.

---

## Why this exists

Agent components fail in operational ways that are awkward to handle purely in
application code or purely at the infrastructure layer:

- a tool worker calls an HTTP endpoint with no timeout and stalls indefinitely,
- a context or log stream grows without bound,
- a worker leaks state across sessions,
- a subprocess crashes and has to be noticed and restarted.

Many teams already handle these — with request timeouts, orchestration, process
managers, retries, and careful application logic. Warden does not claim those
approaches are missing or wrong. It focuses on a specific slice:
**process-level supervision and control inside the runtime boundary**, so a
watchdog or supervisor can observe, restart, pause, or throttle a misbehaving
process — native or foreign — without tearing down the whole system, and so that
intervention is uniform across Zig and Python workers.

---

## What Warden does

- Runs application work as supervised processes in a **supervision tree**, with
  OTP-style restart strategies and bounded restart intensity.
- **Isolates** processes: no shared mutable memory; communication is
  asynchronous message passing through per-process **mailboxes**.
- Supervises **foreign workers** (e.g. Python) as first-class processes — the
  same PID, messaging, logging, and supervision contracts as native processes —
  and auto-restarts them when their bridge connection drops or the process exits.
- Enforces per-process **policy**: activity classes, quotas (mailbox, log
  volume, storage), and promotion/demotion.
- Records an **append-only NDJSON event log** per process, with trace and
  correlation fields.
- Exposes **runtime control** (inspect, pause, resume, quarantine, promote,
  restart, log-tail) over a Unix socket via the `wardenctl` CLI.

---

## What Warden does not do

Warden supervises processes. It does not, by itself, provide:

- **Application-level idempotency** — restarting or replaying a worker does not
  make its side effects safe to repeat; that is the application's responsibility.
- **Exactly-once side effects** — a restarted worker may re-run partially
  completed work.
- **LLM output quality** — Warden governs processes, not what a model returns.
- **Correctness of external APIs or tools** — it can restart a failing caller,
  not fix the callee.
- **Cross-machine distributed orchestration** — Warden supervises processes
  within a runtime/host boundary; it is not a cluster scheduler.
- **Invisible recovery** — restarting or pausing a process can have
  application-visible effects (dropped in-flight work, added latency, lost
  ephemeral state) unless the application is written to tolerate them.

Where user-visible continuity matters, it has to be designed at the application
level — idempotent handlers, checkpointing to `proc-state`, retriable requests.
Warden provides the supervision primitives; it does not silently paper over
failure.

---

## Foreign workers and the Python SDK

Supervising foreign-language workers as first-class processes is one of Warden's
clearest practical differentiators, so it comes first.

A foreign worker — today, a Python process — is spawned and supervised by the
Zig runtime through the **foreign worker bridge**: length-prefixed JSON frames
over a Unix domain socket. It receives the same treatment as a native process:
a PID, a mailbox, a policy envelope, an NDJSON log stream, and a place in a
supervision tree. From the runtime's point of view it is just another process.

Tool workers written in Python participate through the same contracts as native
processes:

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

### Foreign worker auto-restart

Foreign workers spawned via `proc.spawn` are supervised like native children: a
per-beam reaper thread watches each worker's bridge and, when the worker crashes
(socket drop or process exit), respawns it as a fresh incarnation (new PID)
without human intervention. The prior PID transitions to a terminal state — a
restart is a new process, not a resumed one.

Each worker carries its own restart policy, set with the optional `restart`
field on `proc.spawn` (default `permanent`):

| Policy | Behavior on exit |
|---|---|
| `permanent` | Always restart (default) |
| `transient` | Restart only on abnormal exit (non-zero / signal) |
| `temporary` | Never restart |

A runaway guard caps restarts at **3 within a 5s window** per worker; a worker
that exceeds it is retired and logged (`restart` / give-up events in its NDJSON
stream). The reaper's poll cadence (default 50ms) is adjustable on the fly per
beam via the `beam.reaper` RPC or `wardenctl renice <beam> <ms>`.

---

## What you can observe

Every process writes an **append-only NDJSON log stream** (`<beam>-<pid>.log`).
Warden records operational events — lifecycle transitions, messages received and
sent, tool calls, storage writes, policy changes — each stamped with a sequence
number and, where available, trace and correlation IDs:

```jsonl
{"ts":1746582311.042,"beam":10,"pid":5,"seq":1,"event":"spawn","kind":"native_worker"}
{"ts":1746582311.043,"beam":10,"pid":5,"seq":2,"event":"recv","msg_id":"req-01","from":"planner","type":"req.shell","task_id":"task-42"}
{"ts":1746582311.043,"beam":10,"pid":5,"seq":3,"event":"tool_call","tool":"shell","input":"ls -la","corr":"req-01"}
{"ts":1746582311.051,"beam":10,"pid":5,"seq":4,"event":"tool_result","tool":"shell","success":true,"corr":"req-01"}
{"ts":1746582311.051,"beam":10,"pid":5,"seq":5,"event":"send","to":"planner","type":"res.ok","corr":"req-01"}
{"ts":1746582311.052,"beam":10,"pid":5,"seq":6,"event":"note","msg":"task complete","task_id":"task-42"}
```

These are **raw events, not a causal explanation**. The correlation and trace
fields are what you join *on*; reconstructing *why* something happened is
analysis you do on top of the stream (for example with `wardenctl logs --grep`
or your own tooling). Warden captures the events and lets you tail, filter, and
correlate them — it does not index them or guarantee a queryable narrative on
its own.

Separately, a watchdog process can read `last_active_at` from the registry to
notice a worker that has gone idle past its budget, rather than waiting on a
downstream timeout.

---

## What you can control

The runtime exposes intervention authority over any process, so a watchdog can
act without restarting the session supervisor:

```zig
// Pause a worker that exceeded its log quota — stops scheduling it.
try ctx.pause(worker_pid);
ctx.note("watchdog: worker paused — log quota exceeded", null) catch {};

// Wait for cooldown, then resume (return it to the ready queue).
std.Thread.sleep(cooldown_ns);
ctx.note("watchdog: worker resumed — cooldown elapsed", null) catch {};
try ctx.resume_(worker_pid);

// Promote a planner to elevated activity class for a bounded TTL.
try ctx.promote(planner_pid, .elevated, 30_000, "latency-sensitive request");

// Quarantine a foreign worker that stopped responding.
try ctx.runtime.policy.quarantine(worker_pid, "idle_timeout");
```

What each intervention means at the runtime level:

| Operation | Runtime effect |
|---|---|
| `pause` | Process stops being scheduled; queued and in-flight-from-its-view work does not progress until resumed. |
| `resume` | Returns a paused process to the ready queue. |
| `promote` | Raises the process's activity class (e.g. to `elevated`) for a bounded TTL, then it reverts. |
| `quarantine` | Policy-defined throttling: moves the process to the `tiny` activity class (minimal scheduling/quota share). It is not a hard kill. |
| restart | The supervisor terminates the process and starts a fresh incarnation with a **new PID**. |

These act at the **process level**, and that can be application-visible. Pausing
a worker mid-request delays that request; restarting one abandons whatever it had
in flight and gives it a new PID. The session supervisor's own PID is unaffected
by intervening on a child, but "the supervisor survived" is not the same as "the
user saw nothing" — whether a given intervention is transparent depends on how
the application handles dropped, delayed, or replayed work.

---

## Runnable demos

These are **demonstrations of specific behaviors**, not proofs of end-to-end
guarantees. Each notes what it shows and what it does not.

### Hung worker recovery

A tool worker calls an HTTP endpoint with no timeout and blocks forever. Warden's
watchdog detects the idle gap and the supervisor restarts it:

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

- **Shows:** supervised detection and replacement of a stuck worker; the session
  supervisor PID is unchanged while the failed child is replaced.
- **Does not prove:** transparent request replay, exactly-once effects, or that
  the in-flight `task_1` had no user-visible impact. `task_1` was abandoned; only
  `task_2` ran on the new worker.

See `examples/failure_recovery/` and `src/failure_recovery_test.zig`.

### Watchdog intervention (pause/resume)

A tool worker emits excessive log volume. The watchdog pauses it, waits for a
cooldown, then resumes — without restarting the session:

```
[worker]   tool_worker: starting task
[worker]   tool_worker: fetching data
[worker]   tool_worker: processing results     ← quota exceeded after this
[watchdog] watchdog: pausing tool_worker — log quota exceeded
           (worker silent for 80ms cooldown)
[watchdog] watchdog: resuming tool_worker — cooldown elapsed
[worker]   tool_worker: resumed after watchdog pause
```

- **Shows:** runtime-level pause/resume of a single process driven by a policy
  signal, with the rest of the tree untouched.
- **Does not prove:** that the paused worker's pending work completed on time, or
  that pausing had no downstream latency effect.

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

- **Shows:** foreign (Python) workers spawned, registered, messaged, and
  lifecycle-controlled through the same contracts as native processes.
- **Does not prove:** crash-recovery semantics for these particular workers (see
  the hung-worker demo for that), or behavior under concurrent load.

See `examples/live_demo/` and `src/live_demo_test.zig`.

---

## `wardenctl` — runtime CLI

`wardenctl` is a standalone CLI for inspecting and controlling a live Warden
runtime over its Unix socket.

```bash
wardenctl [--socket <path>] [--json] <command>

Commands:
  beams                    List active beams
  ps [--beam N] [--kind K] [--state S]
                           List processes with optional filters
  topology [--beam N]      Show supervisor tree (ASCII)
  logs <beam/proc> [--since 10s] [--grep pattern] [--follow]
                           Stream per-process NDJSON log
  pause <beam/proc>        Pause a process (stop scheduling it)
  resume <beam/proc>       Resume a paused process
  kill <beam/proc> --force [--reason '...']
                           Transition process to exiting
  quarantine <beam/proc> --force [--reason '...']
                           Demote to the `tiny` activity class (policy-defined
                           minimal scheduling/quota; not a hard kill)
  promote <beam/proc> [--class elevated] [--ttl 30s] [--reason '...']
                           Raise process activity class for a bounded TTL
  renice <beam> <ms>       Adjust a beam's foreign-worker reaper poll interval
                           (10–2000ms; lower = faster crash detection)
```

The runtime exposes a Unix socket at `~/.warden/ctrl.sock` by default (override
with `$WARDEN_CTRL_SOCKET` or `--socket`).

---

## Reference topologies

Three concrete supervisor trees showing how Warden maps to real agent
architectures:

| Topology | File | Shape |
|---|---|---|
| Code assistant | `src/topology_code_assistant.zig` | planner + executor_sup (shell/lsp/file) + memory + watchdog |
| Research agent | `src/topology_research_agent.zig` | ranker + retriever_sup (web/vector/doc) + synthesizer + citations |
| ETL pipeline | `src/topology_etl.zig` | extractor → transformer → loader (rest_for_one, checkpointed) |

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
| Control server | `src/control.zig` + `src/control/` | Unix-socket RPC: framing/`Responder` transport, table-dispatched per-domain handlers |
| Foreign bridge | `src/bridge.zig` | Length-prefixed JSON frames over Unix domain socket; per-worker reaper that auto-restarts crashed foreign workers |
| Restart policy | `src/restart.zig` | Foreign-worker restart decision (permanent/transient/temporary) + runaway guard |

### Process model

Every process has:
- An opaque `Pid` (`beam` + monotonic `proc` counter)
- A **mailbox** for asynchronous message passing (sender never blocks, never touches recipient memory)
- A `PolicyEnvelope` governing quotas and activity class
- An append-only NDJSON **log stream** (`<beam>-<pid>.log`)
- A storage namespace view (`proc-temp`, `proc-cache`, `proc-state`)

### Supervisor restart strategies

Standard OTP strategies, adopted by name:

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

Requires Zig 0.16.0.

```bash
zig build test     # run the full test suite (139 tests)
zig build          # compile wardenctl + runtime library
```

---

## License

MIT — see [`LICENSE`](LICENSE).
