import unittest
from unittest.mock import patch, sentinel

from pyartcd import telemetry


class TestTelemetry(unittest.TestCase):
    @patch("pyartcd.telemetry.context.attach")
    @patch("pyartcd.telemetry.metrics.set_meter_provider")
    @patch("pyartcd.telemetry.trace.set_tracer_provider")
    @patch("pyartcd.telemetry.new_meter_provider")
    @patch("pyartcd.telemetry.new_tracker_provider")
    @patch("pyartcd.telemetry.OTLPMetricExporter")
    @patch("pyartcd.telemetry.OTLPSpanExporter")
    def test_initialize_telemetry_initializes_trace_and_metric_providers(
        self,
        mock_span_exporter,
        mock_metric_exporter,
        mock_new_tracker_provider,
        mock_new_meter_provider,
        mock_set_tracer_provider,
        mock_set_meter_provider,
        mock_context_attach,
    ):
        mock_span_exporter.return_value = sentinel.trace_exporter
        mock_metric_exporter.return_value = sentinel.metric_exporter
        mock_new_tracker_provider.return_value = sentinel.tracer_provider
        mock_new_meter_provider.return_value = sentinel.meter_provider

        with patch.dict(
            "os.environ",
            {
                "OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.example:4317",
                "OTEL_EXPORTER_OTLP_HEADERS": "authorization=Bearer test",
            },
            clear=False,
        ):
            telemetry.initialize_telemetry()

        mock_span_exporter.assert_called_once_with(
            endpoint="https://otel.example:4317",
            headers="authorization=Bearer test",
        )
        mock_metric_exporter.assert_called_once_with(
            endpoint="https://otel.example:4317",
            headers="authorization=Bearer test",
        )
        mock_new_tracker_provider.assert_called_once()
        mock_new_meter_provider.assert_called_once()
        mock_set_tracer_provider.assert_called_once_with(sentinel.tracer_provider)
        mock_set_meter_provider.assert_called_once_with(sentinel.meter_provider)
        mock_context_attach.assert_not_called()
