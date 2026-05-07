# failure_recovery example

Demonstrates Warden's supervision advantage over today's agent stacks.

## The scenario

A Python tool worker calls an HTTP endpoint with **no timeout** and blocks forever.

| Stack | What happens |
|-------|-------------|
| LangChain / CrewAI / raw asyncio | Event loop or thread pool blocks. Entire agent silently stalls. No recovery. |
| **Warden** | Watchdog detects idle. Policy engine quarantines the hung PID. Supervisor restarts. Session continues. |

## Warden's response (observable from logs)

```
[session]  task_1 dispatched → worker pid:1/5
[worker]   task_1 received — calling external API (no timeout)...
[watchdog] worker pid:1/5 idle 150ms > budget — quarantining
[policy]   pid:1/5 quarantined (idle_timeout)
[executor] pid:1/5 exited (killed) — restarting (one_for_one)
[session]  new worker pid:1/8 ready
[session]  task_2 dispatched → worker pid:1/8
[worker]   task_2 completed in 3ms
[session]  session supervisor pid:1/3 unchanged throughout
```

## Running the Zig integration test

```bash
zig build test --filter "failure recovery"
```

This runs two scenarios end-to-end:
- **Test 1**: Hung worker is detected, quarantined, replaced; task_2 completes on new worker
- **Test 2**: Manual restart; non-hanging worker completes a task

## Running the Python worker (with a live ForeignBridge)

```bash
WARDEN_SOCKET=/tmp/warden.sock python examples/failure_recovery/hanging_worker.py
```

Send `req.fetch` to trigger the hang. The watchdog will kill and restart the worker.
Send `req.echo` to the replacement worker to verify recovery.
