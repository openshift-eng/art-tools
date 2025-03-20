from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch, ANY, Mock

from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT, KIND_RELEASE
from elliottlib.cli.konflux_release_cli import WatchReleaseCli
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
            'apiVersion': 'appstudio.redhat.com/v1alpha1',
            'kind': 'Snapshot',
            'metadata': {
                'labels': {
                    'test.appstudio.openshift.io/type': 'override'
                },
                'name': 'ose-4-18-timestamp',
                'namespace': 'test-namespace'
            },
            'spec': {
                'application': 'openshift-4-18',
                'components': [{'containerImage': 'registry/image@sha256:digest1',
                                'name': 'ose-4-18-component1',
                                'source': {'git': {'revision': 'foobar', 'url': 'https://github.com/test/repo1'}}}]
            }
        }
        self.konflux_client._get.return_value = Model(release)

        status = await cli.run()
        self.konflux_client._get.assert_called_once_with(API_VERSION, KIND_RELEASE, release)
