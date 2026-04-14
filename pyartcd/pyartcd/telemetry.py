import os
import sys

from artcommonlib import constants
from opentelemetry import context, metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from pyartcd import __version__


def new_tracker_provider(resource: Resource, exporter: SpanExporter):
    """Creates and initialize a TracerProvider for Doozer.
    Currently we only export traces to stderr for development purpose.
    """
    processor = BatchSpanProcessor(exporter)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(processor)
    return provider


def new_meter_provider(resource: Resource, exporter: OTLPMetricExporter):
    """Creates and initialize a MeterProvider for pyartcd metrics."""
    reader = PeriodicExportingMetricReader(exporter)
    return MeterProvider(resource=resource, metric_readers=[reader])


def initialize_telemetry():
    # Initialize resource attributes;
    # Additional attributes can be specified in OTEL_RESOURCE_ATTRIBUTES env var
    resource = Resource.create(
        attributes={
            ResourceAttributes.SERVICE_NAME: "pyartcd",
            ResourceAttributes.SERVICE_VERSION: __version__,
        }
    )

    otel_exporter_otlp_endpoint = os.environ.get('OTEL_EXPORTER_OTLP_ENDPOINT', constants.OTEL_EXPORTER_OTLP_ENDPOINT)
    otel_exporter_otlp_headers = os.environ.get('OTEL_EXPORTER_OTLP_HEADERS')

    trace_exporter = OTLPSpanExporter(endpoint=otel_exporter_otlp_endpoint, headers=otel_exporter_otlp_headers)
    metric_exporter = OTLPMetricExporter(endpoint=otel_exporter_otlp_endpoint, headers=otel_exporter_otlp_headers)

    tp = new_tracker_provider(resource, trace_exporter)
    mp = new_meter_provider(resource, metric_exporter)
    trace.set_tracer_provider(tp)
    metrics.set_meter_provider(mp)
    # TRACEPARENT env var is used to propagate trace context
    traceparent = os.environ.get('TRACEPARENT')
    if traceparent:
        carrier = {'traceparent': traceparent}
        ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
        context.attach(ctx)
        print(f"Use TRACEPARENT {traceparent} for telemetry", file=sys.stderr)
