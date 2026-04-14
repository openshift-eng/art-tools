import unittest
from unittest.mock import patch, sentinel

from pyartcd import telemetry


class TestTelemetry(unittest.TestCase):
    def tearDown(self):
        telemetry._TRACER_PROVIDER = None
        telemetry._METER_PROVIDER = None

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
        self.assertIs(telemetry._TRACER_PROVIDER, sentinel.tracer_provider)
        self.assertIs(telemetry._METER_PROVIDER, sentinel.meter_provider)

    @patch("pyartcd.telemetry.metrics.set_meter_provider")
    @patch("pyartcd.telemetry.trace.set_tracer_provider")
    @patch("pyartcd.telemetry.new_meter_provider")
    @patch("pyartcd.telemetry.new_tracker_provider")
    @patch("pyartcd.telemetry.OTLPMetricExporter")
    @patch("pyartcd.telemetry.OTLPSpanExporter")
    def test_initialize_telemetry_is_idempotent(
        self,
        mock_span_exporter,
        mock_metric_exporter,
        mock_new_tracker_provider,
        mock_new_meter_provider,
        mock_set_tracer_provider,
        mock_set_meter_provider,
    ):
        mock_new_tracker_provider.return_value = sentinel.tracer_provider
        mock_new_meter_provider.return_value = sentinel.meter_provider

        telemetry.initialize_telemetry()
        telemetry.initialize_telemetry()

        mock_span_exporter.assert_called_once()
        mock_metric_exporter.assert_called_once()
        mock_new_tracker_provider.assert_called_once()
        mock_new_meter_provider.assert_called_once()
        mock_set_tracer_provider.assert_called_once_with(sentinel.tracer_provider)
        mock_set_meter_provider.assert_called_once_with(sentinel.meter_provider)

    @patch("pyartcd.telemetry.LOGGER")
    def test_shutdown_telemetry_flushes_and_shuts_down_providers(self, mock_logger):
        telemetry._TRACER_PROVIDER = tracer_provider = unittest.mock.Mock()
        telemetry._METER_PROVIDER = meter_provider = unittest.mock.Mock()

        telemetry.shutdown_telemetry()

        meter_provider.force_flush.assert_called_once_with()
        meter_provider.shutdown.assert_called_once_with()
        tracer_provider.force_flush.assert_called_once_with()
        tracer_provider.shutdown.assert_called_once_with()
        mock_logger.warning.assert_not_called()
        self.assertIsNone(telemetry._TRACER_PROVIDER)
        self.assertIsNone(telemetry._METER_PROVIDER)

    @patch("pyartcd.telemetry.LOGGER")
    def test_shutdown_telemetry_swallows_provider_errors(self, mock_logger):
        telemetry._TRACER_PROVIDER = tracer_provider = unittest.mock.Mock()
        telemetry._METER_PROVIDER = meter_provider = unittest.mock.Mock()
        meter_provider.force_flush.side_effect = RuntimeError("metrics flush")
        meter_provider.shutdown.side_effect = RuntimeError("metrics shutdown")
        tracer_provider.force_flush.side_effect = RuntimeError("traces flush")
        tracer_provider.shutdown.side_effect = RuntimeError("traces shutdown")

        telemetry.shutdown_telemetry()

        self.assertEqual(mock_logger.warning.call_count, 4)
        self.assertIsNone(telemetry._TRACER_PROVIDER)
        self.assertIsNone(telemetry._METER_PROVIDER)
