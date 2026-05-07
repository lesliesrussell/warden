#!/usr/bin/env python3
# warden-3cn
"""Math worker — handles req.fib and req.primes messages.

Long-running: after handling a request it goes back to waiting.
Warden can pause/resume this process mid-wait.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import warden


def fib(n: int) -> int:
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def sieve(limit: int) -> list[int]:
    if limit < 2:
        return []
    composite = bytearray(limit + 1)
    for i in range(2, int(limit**0.5) + 1):
        if not composite[i]:
            for j in range(i * i, limit + 1, i):
                composite[j] = 1
    return [i for i in range(2, limit + 1) if not composite[i]]


@warden.tool
def handle_fib(ctx, msg):
    n = int(msg.get("body", 10))
    result = fib(n)
    ctx.note(f"fib({n}) = {result}")
    return result


@warden.tool
def handle_primes(ctx, msg):
    limit = int(msg.get("body", 50))
    result = sieve(limit)
    ctx.note(f"primes up to {limit}: {result}")
    return result


def main():
    ctx = warden.BeamCtx()
    ctx.note("math_worker: ready")
    warden.run_loop(ctx, {
        "req.fib": handle_fib,
        "req.primes": handle_primes,
    })


if __name__ == "__main__":
    main()
