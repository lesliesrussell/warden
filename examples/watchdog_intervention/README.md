# watchdog_intervention example

Demonstrates Warden's pause/resume authority — the watchdog can silence a
misbehaving worker without restarting the session.

## The scenario

A tool worker emits far more log events than its policy allows. In today's
stacks there is no enforcement mechanism — the logs just pile up. In Warden:

1. The tool worker processes a task and emits 6 log events
2. The watchdog detects the log quota (5 events) has been exceeded
3. The watchdog pauses the worker with `ctx.pause(worker_pid)`
4. After an 80 ms cooldown the watchdog resumes it with `ctx.resume_(worker_pid)`
5. The session supervisor PID never changes

## Observable log output

```
{"event":"note","msg":"tool_worker: starting task"}
{"event":"note","msg":"tool_worker: fetching data"}
{"event":"note","msg":"tool_worker: processing results"}
{"event":"note","msg":"tool_worker: writing output"}
{"event":"metric","name":"task_duration_ms","value":3}
{"event":"note","msg":"tool_worker: task complete"}
{"event":"note","msg":"watchdog: pausing tool_worker — log quota exceeded"}
  ... 80 ms cooldown ...
{"event":"note","msg":"watchdog: resuming tool_worker — cooldown elapsed"}
{"event":"note","msg":"tool_worker: resumed after watchdog pause"}
```

## Running the integration test

```bash
zig build test --filter "watchdog intervention"
```

Two tests run:
- **Test 1**: watchdog fires, worker paused and resumed, session PID unchanged
- **Test 2**: quota not exceeded, no intervention occurs
