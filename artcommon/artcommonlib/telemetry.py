import os
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, Sequence

import psutil
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.util.types import Attributes


def _sample_proc(proc: psutil.Process) -> dict:
    """Sample RSS and CPU for a process. Returns a sentinel dict with -1 values on NoSuchProcess."""
    try:
        mem = proc.memory_info().rss // 1024 // 1024
        cpu = proc.cpu_times()
        return {"rss_mb": mem, "cpu_user_s": round(cpu.user, 3), "cpu_sys_s": round(cpu.system, 3)}
    except psutil.Error:
        return {"rss_mb": -1, "cpu_user_s": -1.0, "cpu_sys_s": -1.0}


def start_as_current_span_async(
    tracer: trace.Tracer,
    name: str,
    context: Optional[Context] = None,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    attributes: Attributes = None,
    links: Optional[Sequence[trace.Link]] = None,
    start_time: Optional[int] = None,
    record_exception: bool = True,
    set_status_on_exception: bool = True,
    end_on_exit: bool = True,
    record_resources: bool = False,
):
    """A decorator like tracer.start_as_current_span, but works for async functions.

    When record_resources=True, samples process RSS and CPU before and after the
    wrapped function and writes process.rss_mb_start/end and cpu_user_s_start/end
    on the span, giving a true delta for the duration of that operation.
    """

    def decorator(function: Callable[..., Awaitable[Any]]):
        @wraps(function)
        async def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(
                name=name,
                context=context,
                kind=kind,
                attributes=attributes,
                links=links,
                start_time=start_time,
                record_exception=record_exception,
                set_status_on_exception=set_status_on_exception,
                end_on_exit=end_on_exit,
            ) as span:
                if record_resources:
                    proc = psutil.Process(os.getpid())
                    r0 = _sample_proc(proc)
                    span.set_attribute("process.rss_mb_start", r0["rss_mb"])
                    span.set_attribute("process.cpu_user_s_start", r0["cpu_user_s"])
                    span.set_attribute("process.cpu_sys_s_start", r0["cpu_sys_s"])
                try:
                    return await function(*args, **kwargs)
                finally:
                    if record_resources:
                        r1 = _sample_proc(proc)
                        span.set_attribute("process.rss_mb_end", r1["rss_mb"])
                        span.set_attribute("process.cpu_user_s_end", r1["cpu_user_s"])
                        span.set_attribute("process.cpu_sys_s_end", r1["cpu_sys_s"])

        return wrapper

    return decorator
