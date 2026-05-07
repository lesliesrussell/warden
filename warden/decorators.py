# warden-942
from __future__ import annotations

import functools
import traceback
import uuid
from typing import Callable


def tool(fn: Callable) -> Callable:
    """Decorator that wraps a handler with tool_call / tool_result log events.

    Usage::

        @beam.tool
        def search_web(ctx, msg):
            query = msg["body"]
            ...
            return result_str
    """
    @functools.wraps(fn)
    def wrapper(ctx, msg):
        corr = msg.get("id", "")
        input_body = msg.get("body")
        ctx.tool_call(fn.__name__, input_body, corr=corr)
        try:
            result = fn(ctx, msg)
            ctx.tool_result(fn.__name__, success=True, output=result, corr=corr)
            return result
        except Exception as exc:
            ctx.tool_result(fn.__name__, success=False, output=str(exc), corr=corr)
            raise

    wrapper._is_beam_tool = True
    return wrapper
