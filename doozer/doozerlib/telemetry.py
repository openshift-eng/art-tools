import os
import sys
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, Sequence

from opentelemetry import context, metrics, trace
from opentelemetry.context import Context
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (ConsoleMetricExporter,
                                              PeriodicExportingMetricReader)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (BatchSpanProcessor,
                                            ConsoleSpanExporter, SpanExporter)
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace.propagation.tracecontext import \
    TraceContextTextMapPropagator
from opentelemetry.util.types import Attributes

from doozerlib import __version__


def new_tracker_provider(resource: Resource, exporter: SpanExporter):
    """ Creates and initialize a TracerProvider for Doozer.
    Currently we only export traces to stderr for development purpose.
    """
    processor = BatchSpanProcessor(exporter)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(processor)
    return provider


def initialize_telemetry():
    # Initialize resource attributes;
    # Additional attributes can be specified in OTEL_RESOURCE_ATTRIBUTES env var
    resource = Resource.create(attributes={
        ResourceAttributes.SERVICE_NAME: "doozer",
        ResourceAttributes.SERVICE_VERSION: __version__,
    })

    otel_exporter_otlp_endpoint = os.environ.get('OTEL_EXPORTER_OTLP_ENDPOINT')
    otel_exporter_otlp_headers = os.environ.get('OTEL_EXPORTER_OTLP_HEADERS')
    if otel_exporter_otlp_endpoint:
        exporter = OTLPSpanExporter(endpoint=otel_exporter_otlp_endpoint, headers=otel_exporter_otlp_headers)
    else:
        # OTEL_EXPORTER_OTLP_ENDPOINT is not defined; export to stderr
        exporter = ConsoleSpanExporter(service_name="doozer", out=sys.stderr)

    tp = new_tracker_provider(resource, exporter)
    trace.set_tracer_provider(tp)
    # TRACEPARENT env var is used to propagate trace context
    traceparent = os.environ.get('TRACEPARENT')
    if traceparent:
        carrier = {'traceparent': traceparent}
        ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
        context.attach(ctx)
        print(f"Use TRACEPARENT {traceparent} for telemetry", file=sys.stderr)


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
