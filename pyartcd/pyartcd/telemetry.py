import logging
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

LOGGER = logging.getLogger(__name__)
_TRACER_PROVIDER: TracerProvider | None = None
_METER_PROVIDER: MeterProvider | None = None


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
    global _TRACER_PROVIDER, _METER_PROVIDER
    if _TRACER_PROVIDER is not None or _METER_PROVIDER is not None:
        return

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

    _TRACER_PROVIDER = new_tracker_provider(resource, trace_exporter)
    _METER_PROVIDER = new_meter_provider(resource, metric_exporter)
    trace.set_tracer_provider(_TRACER_PROVIDER)
    metrics.set_meter_provider(_METER_PROVIDER)
    # TRACEPARENT env var is used to propagate trace context
    traceparent = os.environ.get('TRACEPARENT')
    if traceparent:
        carrier = {'traceparent': traceparent}
        ctx = TraceContextTextMapPropagator().extract(carrier=carrier)
        context.attach(ctx)
        print(f"Use TRACEPARENT {traceparent} for telemetry", file=sys.stderr)


def shutdown_telemetry():
    global _TRACER_PROVIDER, _METER_PROVIDER

    meter_provider = _METER_PROVIDER
    tracer_provider = _TRACER_PROVIDER
    _METER_PROVIDER = None
    _TRACER_PROVIDER = None

    if meter_provider is not None:
        try:
            meter_provider.force_flush()
        except Exception:
            LOGGER.warning("Failed to flush OpenTelemetry metrics", exc_info=True)
        try:
            meter_provider.shutdown()
        except Exception:
            LOGGER.warning("Failed to shut down OpenTelemetry metrics", exc_info=True)

    if tracer_provider is not None:
        try:
            tracer_provider.force_flush()
        except Exception:
            LOGGER.warning("Failed to flush OpenTelemetry traces", exc_info=True)
        try:
            tracer_provider.shutdown()
        except Exception:
            LOGGER.warning("Failed to shut down OpenTelemetry traces", exc_info=True)
