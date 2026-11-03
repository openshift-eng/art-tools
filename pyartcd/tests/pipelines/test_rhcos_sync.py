import os
from contextlib import contextmanager
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pyartcd.pipelines.rhcos_sync import RhcosSyncPipeline

RESOLVED_AUTH = "/tmp/resolved-auth.json"


def _make_pipeline() -> RhcosSyncPipeline:
    runtime = MagicMock()
    runtime.logger = MagicMock()
    return RhcosSyncPipeline(runtime=runtime, release_tag="4.18.0-x86_64")


@contextmanager
def _fake_registry_config(*args, **kwargs):
    """Stand-in for RegistryConfig that yields a known resolved auth file path."""
    yield RESOLVED_AUTH


class TestRhcosSyncRegistryAuth(TestCase):
    """Verify ``_registry_auth`` exports the resolved auth file as QUAY_AUTH_FILE
    (so cosign's hostname-only lookup resolves the right quay credential) and
    restores the previous value on exit.
    """

    @patch("pyartcd.pipelines.rhcos_sync.RegistryConfig", _fake_registry_config)
    @patch.dict(os.environ, {"QUAY_AUTH_FILE": "/raw/multi-robot.json"}, clear=False)
    def test_exports_resolved_auth_and_restores_prior_value(self):
        pipeline = _make_pipeline()

        with pipeline._registry_auth() as auth_file:
            self.assertEqual(auth_file, RESOLVED_AUTH)
            # Inside the context, the resolved file must be the active credential.
            self.assertEqual(os.environ["QUAY_AUTH_FILE"], RESOLVED_AUTH)
            self.assertEqual(pipeline._registry_auth_file, RESOLVED_AUTH)

        # After exit, the prior raw value must be restored.
        self.assertEqual(os.environ["QUAY_AUTH_FILE"], "/raw/multi-robot.json")

    @patch("pyartcd.pipelines.rhcos_sync.RegistryConfig", _fake_registry_config)
    @patch.dict(os.environ, {}, clear=False)
    def test_exports_resolved_auth_and_pops_when_no_prior_value(self):
        # Ensure there is no prior value for this case.
        os.environ.pop("QUAY_AUTH_FILE", None)
        pipeline = _make_pipeline()

        with pipeline._registry_auth() as auth_file:
            self.assertEqual(auth_file, RESOLVED_AUTH)
            self.assertEqual(os.environ["QUAY_AUTH_FILE"], RESOLVED_AUTH)

        # After exit, with no prior value, the variable must be removed.
        self.assertNotIn("QUAY_AUTH_FILE", os.environ)
