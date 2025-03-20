from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.backend.konflux_client import API_VERSION, KIND_RELEASE
from elliottlib.cli.konflux_release_watch_cli import WatchReleaseCli
from artcommonlib.model import Model


class TestWatchReleaseCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.runtime.get_major_minor.return_value = (4, 18)

        self.dry_run = False

        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )

        self.konflux_client = AsyncMock()
        self.konflux_client.verify_connection.return_value = True

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        release = "test-release-prod"
        cli = WatchReleaseCli(
            release=release,
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            timeout=0,
            dry_run=self.dry_run,
        )

        release = {
            'apiVersion': API_VERSION,
            'kind': KIND_RELEASE,
            'metadata': {
                'name': release,
                'namespace': self.konflux_config['namespace']
            },
            'status': {
                'conditions': [
                    {
                        'type': 'Released',
                        'status': 'True',
                        'reason': 'Succeeded'
                    },
                ]
            }
        }
        self.konflux_client.wait_for_release.return_value = Model(release)

        status = await cli.run()
        self.assertEqual(status, True)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_skipped(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        release = "test-release-prod"
        cli = WatchReleaseCli(
            release=release,
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            timeout=0,
            dry_run=self.dry_run,
        )

        release = {
            'apiVersion': API_VERSION,
            'kind': KIND_RELEASE,
            'metadata': {
                'name': release,
                'namespace': self.konflux_config['namespace']
            },
            'status': {
                'conditions': [
                    {
                        'type': 'Released',
                        'status': 'True',
                        'reason': 'Skipped'
                    },
                ]
            }
        }
        self.konflux_client.wait_for_release.return_value = Model(release)

        status = await cli.run()
        self.assertEqual(status, False)

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_skipped(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        release = "test-release-prod"
        cli = WatchReleaseCli(
            release=release,
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            timeout=0,
            dry_run=self.dry_run,
        )

        release = {
            'apiVersion': API_VERSION,
            'kind': KIND_RELEASE,
            'metadata': {
                'name': release,
                'namespace': self.konflux_config['namespace']
            },
            'status': {
                'conditions': [
                    {
                        'type': 'Released',
                        'status': 'False',
                        'reason': 'Failed'
                    },
                ]
            }
        }
        self.konflux_client.wait_for_release.return_value = Model(release)

        status = await cli.run()
        self.assertEqual(status, False)
