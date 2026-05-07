# python_worker example

A Python tool worker connected to the Warden runtime over a Unix socket.

## Quickstart

```bash
# From the repo root — socket path set by ForeignBridge at runtime
WARDEN_SOCKET=/tmp/warden.sock python examples/python_worker/worker.py
```

The worker registers two tools:

| Message type   | Body         | Returns                                  |
|----------------|--------------|------------------------------------------|
| `req.shell`    | command str  | `{exit_code, stdout, stderr}`            |
| `req.read_file`| file path    | file contents as a string                |

Send `sys.shutdown` to stop the loop cleanly.

## SDK usage

```python
import warden

ctx = warden.BeamCtx()          # connects via $WARDEN_SOCKET

@warden.tool                     # auto-logs tool_call / tool_result events
def my_tool(ctx, msg):
    return "result"

warden.run_loop(ctx, {
    "req.my_tool": my_tool,      # dispatches by message type, handles errors
})
```
