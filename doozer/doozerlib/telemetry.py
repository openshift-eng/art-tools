import os
import sys

from artcommonlib import constants
from opentelemetry import context, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from doozerlib import __version__


def new_tracker_provider(resource: Resource, exporter: SpanExporter):
    """Creates and initialize a TracerProvider for Doozer.
    Currently we only export traces to stderr for development purpose.
    """
    processor = BatchSpanProcessor(exporter)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(processor)
    return provider


def initialize_telemetry():
    # Initialize resource attributes;
    # Additional attributes can be specified in OTEL_RESOURCE_ATTRIBUTES env var
    resource = Resource.create(
        attributes={
            ResourceAttributes.SERVICE_NAME: "doozer",
            ResourceAttributes.SERVICE_VERSION: __version__,
        }
    )

    otel_exporter_otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", constants.OTEL_EXPORTER_OTLP_ENDPOINT)
    otel_exporter_otlp_headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS")

    exporter = OTLPSpanExporter(endpoint=otel_exporter_otlp_endpoint, headers=otel_exporter_otlp_headers)

    tp = new_tracker_provider(resource, exporter)
    trace.set_tracer_provider(tp)
    # TRACEPARENT env var is used to propagate trace context
    traceparent = os.environ.get("TRACEPARENT")
    if traceparent:
        carrier = {"traceparent": traceparent}
        ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
        context.attach(ctx)
        print(f"Use TRACEPARENT {traceparent} for telemetry", file=sys.stderr)
