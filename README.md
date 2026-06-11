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

## Semantics

This section is the contract — the behavior you can write tests against. It
describes the runtime as implemented today, and is explicit about what is
*enforced* versus what is currently an *advisory* label.

### Message ordering and delivery

- Each process has one mailbox: a single FIFO queue. Messages from a given
  sender to a given recipient are delivered in send order (**per-sender FIFO**);
  messages from different senders interleave in arrival order. There is no
  priority reordering of the queue.
- `recv` is **selective**: it scans pending messages oldest-first and returns
  the first that matches.
- `send` **never blocks**. It returns `error.NoSuchProcess` if the recipient's
  mailbox no longer exists, or `error.MailboxFull` if the mailbox is at its
  count/byte limit and the overflow strategy rejects the message.
- `call(pid, msg, timeout)` sends, then polls its *own* mailbox for a reply
  correlated by message id, returning `error.Timeout` if none arrives.
- Delivery is **at-most-once and best-effort**: no acknowledgement, retry, or
  redelivery. A message enqueued but never received (e.g. the recipient exits
  first) is simply dropped.

### Process exit and restart

- Mailboxes are **ephemeral**. When a process exits, its mailbox and any pending
  messages are discarded. Nothing is forwarded to a replacement.
- A restart is a **new process with a new PID**, not a resumed one; the old PID
  transitions to a terminal state.
- An in-flight `call` to a process that exits or restarts **does not detect the
  death** — it simply times out (`error.Timeout`). It never rebinds to the new
  incarnation, and Warden never redelivers the original message.
- Consequently, if a request must survive a worker restart, the **caller** must
  retry and the work must be safe to repeat.

### Storage across restart

- Storage namespaces are keyed by PID: `<base>/<ns>/<beam>/<proc>/…`. Because a
  restart yields a **new PID**, the new incarnation gets a fresh, empty namespace
  view — including `proc-state`.
- **`proc-state` is therefore NOT automatically reattached across a restart.**
  Its bytes persist on disk under the old PID's path, but the new incarnation
  cannot see them through its own namespace. Durable state that must survive a
  restart has to be re-keyed and reloaded by the application (e.g. under a stable
  logical key in `shared-vol`), not assumed.
- `proc-temp` is scratch space — treat it as non-durable. `proc-cache` is
  evictable and must never be relied on for correctness.

### Quotas, classes, and quarantine — enforced vs. advisory

- **Mailbox quota is enforced.** `max_mailbox_len` / `max_mailbox_bytes` are
  applied on every enqueue via the configured overflow strategy
  (`reject_new`, `drop_oldest_low`, `escalate_supervisor`, `throttle_sender`);
  overflow surfaces to the sender as `error.MailboxFull` (or drops a
  low-priority message under `drop_oldest_low`).
- **Activity classes are advisory.** Classes (`tiny`/`normal`/`elevated`/…) and
  `promote`/`demote` set a label on the process and emit a policy event. The
  current scheduler is FIFO and **does not prioritize by class** — promoting to
  `elevated` does not, today, schedule a process ahead of others.
- **Quarantine is advisory.** It demotes a process to the `tiny` class and emits
  an event; it does **not**, by itself, stop the process from being scheduled or
  from accepting messages. A watchdog or supervisor is expected to react to the
  event/class. (`pause` is the mechanism that actually removes a process from
  scheduling.)
- **Log-volume quota is not enforced.** `max_log_bytes_per_min` is carried in the
  policy envelope but not applied by the runtime; there is no rotation,
  compaction, or dropping. Log files grow unbounded — manage them with external
  tooling (e.g. `logrotate`).

### Logging durability

- Per-process NDJSON is written through a **buffered** writer and is
  best-effort: a hard crash may lose the unflushed tail. The log is an
  operational event stream, not a transactional audit record.
- Correlation/trace IDs are a **convention**: the SDK helpers stamp them, but
  propagating IDs across process boundaries is the application's responsibility.
  Warden does not guarantee a message and its log events share an ID unless your
  code sets it.

### Control plane

- The control socket has **no authentication or authorization**: any process
  that can open `~/.warden/ctrl.sock` has full control authority. The model is
  **local, single-tenant by design** — secure it with filesystem permissions.
- Control operations are best-effort and applied per current process state
  (pausing an already-paused process is a no-op). Concurrent operations on the
  same process are serialized by the registry lock with **no conflict
  detection** (last writer wins).
- `pause` removes a process from scheduling and **discards tasks submitted while
  it is paused** — it is not a freeze-and-resume of in-flight work.

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
that exceeds it is retired (not respawned) and logged (`restart` / give-up
events in its NDJSON stream). A retired worker's PID is terminal: its mailbox is
reclaimed, so subsequent `send`/`call` to it fail (`error.NoSuchProcess` /
`error.Timeout`) rather than silently queuing. **A live supervisor does not
imply a live child** — check the child's state, not just the tree root. The
reaper's poll cadence (default 50ms) is adjustable on the fly per beam via the
`beam.reaper` RPC or `wardenctl renice <beam> <ms>`.

### Failure matrix

| Failure | What the bridge does | What the caller observes |
|---|---|---|
| Worker exits / socket drops | Reader detects EOF, marks the worker crashed; reaper respawns it as a **new PID** per restart policy (or retires it past the runaway guard) | In-flight `call` times out; messages in the old mailbox are lost; no redelivery |
| Worker hangs (no progress, socket open) | **Not detected by the bridge** — a watchdog reading `last_active_at` must notice and intervene (`pause`/`kill`) | `call` times out; nothing happens automatically without a watchdog |
| Protocol violation (malformed JSON, unknown frame kind, missing fields, stray reply id) | Frame is logged/dropped; the connection **stays open**; the worker is **not** killed or quarantined | No effect; a bad/unmatched reply is simply never delivered to a caller |
| Version skew (runtime vs SDK) | **No version negotiation** — the one-way handshake carries no version; mismatches surface only as per-frame field errors | Undefined; depends on which frames the worker emits |

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
| `pause` | Removes the process from scheduling. Tasks submitted while it is paused are **discarded, not queued** (see Semantics). |
| `resume` | Returns a paused process to the ready queue. |
| `promote` | **Advisory**: relabels the activity class (e.g. `elevated`) for a bounded TTL and emits an event. The current scheduler does **not** prioritize by class. |
| `quarantine` | **Advisory**: demotes to the `tiny` class and emits an event. Not a hard kill and does **not** stop scheduling — a watchdog must act on it. |
| restart | The supervisor terminates the process and starts a fresh incarnation with a **new PID** (no mailbox or per-PID state carried over). |

These act at the **process level**, and that can be application-visible. Pausing
a worker **drops the work queued for it** (it is not buffered for later);
restarting one abandons whatever it had in flight and gives it a new PID. The
session supervisor's own PID is unaffected by intervening on a child, but "the
supervisor survived" is not the same as "the user saw nothing" — whether a given
intervention is transparent depends on how the application handles dropped,
delayed, or replayed work. See **Semantics** for the precise contract.

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
with `$WARDEN_CTRL_SOCKET` or `--socket`). **There is no authentication** — any
peer that can open the socket has full control authority. The model is local and
single-tenant; secure it with filesystem permissions. See
[Semantics → Control plane](#semantics).

---

## Reference topologies

Three concrete supervisor trees showing how Warden maps to real agent
architectures:

| Topology | File | Shape |
|---|---|---|
| Code assistant | `src/topology_code_assistant.zig` | planner + executor_sup (shell/lsp/file) + memory + watchdog |
| Research agent | `src/topology_research_agent.zig` | ranker + retriever_sup (web/vector/doc) + synthesizer + citations |
| ETL pipeline | `src/topology_etl.zig` | extractor → transformer → loader (rest_for_one, checkpointed) |

A restart strategy only defines *which* processes are replaced — it does not
make the work correct. Given the [Semantics](#semantics) above (new PID on
restart, no `proc-state` reattach, no redelivery), each tree carries
application-level obligations:

- **ETL (`rest_for_one`)** — a transformer failure restarts transformer **and**
  loader (not the extractor). For this to not corrupt the run, the **loader must
  be idempotent** and the **transformer must checkpoint batch progress** to a
  stable, non-PID-keyed location (e.g. `shared-vol`), since the restarted
  incarnation starts with an empty `proc-state`.
- **Code assistant / research agent** — decide per child which failures are
  tolerable vs. fatal: a tool worker dying and being replaced is fine (the
  planner retries), but a persistent memory-service failure usually warrants
  failing the session fast rather than continuing against missing state. Warden
  gives you the restart mechanism; choosing the correctness envelope is yours.

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
| `proc-temp` | Scratch. `cleanupTemp()` exists but is **not auto-invoked on exit** today — treat as non-durable, not auto-deleted. |
| `proc-cache` | Recomputable. `evictCache()` exists but is **not auto-invoked** — never rely on its contents. |
| `proc-state` | Persists on disk, but keyed by PID — **not auto-reattached after restart** (new PID = new namespace). See [Semantics](#semantics). |
| `shared-vol` | Named shared datasets; access gated by explicit per-view grants (`grantVolume`). |

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
