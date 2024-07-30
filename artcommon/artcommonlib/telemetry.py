from functools import wraps
from typing import Any, Awaitable, Callable, Optional, Sequence

from opentelemetry import context, metrics, trace
from opentelemetry.context import Context
from opentelemetry.util.types import Attributes


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
):
    """ A decorator like tracer.start_as_current_span, but works for async functions
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
            ):
                return await function(*args, **kwargs)

        return wrapper

    return decorator
