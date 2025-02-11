from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch, ANY, Mock
import os

from elliottlib.cli.release_snapshot_cli import CreateSnapshotCli


class TestCreateSnapshotCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.konflux_namespace = "test-namespace"
        self.konflux_kubeconfig = None
        self.konflux_context = None
        self.for_bundle = False
        self.dry_run = False

        os.environ['KONFLUX_ART_IMAGES_USERNAME'] = 'test_user'
        os.environ['KONFLUX_ART_IMAGES_PASSWORD'] = 'test_password'

        self.runtime.konflux_db.bind.return_value = None
        self.runtime.get_major_minor.return_value = (4, 18)
        self.runtime.konflux_db = MagicMock()

        self.konflux_client = AsyncMock()
        self.konflux_client.verify_connection.return_value = True

    def tearDown(self):
        del os.environ['KONFLUX_ART_IMAGES_USERNAME']
        del os.environ['KONFLUX_ART_IMAGES_PASSWORD']

    @patch("elliottlib.cli.release_snapshot_cli.CreateSnapshotCli.get_timestamp", return_value="timestamp")
    @patch("elliottlib.cli.release_snapshot_cli.oc_image_info_for_arch_async")
    @patch("doozerlib.backend.konflux_client.KonfluxClient")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_konflux_client, mock_oc_image_info, *_):
        mock_runtime.return_value = self.runtime
        mock_konflux_client.return_value = self.konflux_client
        mock_oc_image_info.return_value = AsyncMock()

        build_records = [
            Mock(
                nvr='test-nvr-1',
                image_pullspec='registry/image:tag1',
                image_tag='digest1',
                source_repo='https://github.com/test/repo1',
                commitish='main'
            ),
            Mock(
                nvr='test-nvr-2',
                image_pullspec='registry/image:tag2',
                image_tag='digest2',
                source_repo='https://github.com/test/repo2',
                commitish='main'
            )
        ]
        # `name` attribute is special so set it after creation
        # https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
        build_records[0].name = 'component1'
        build_records[1].name = 'component2'

        with patch.object(CreateSnapshotCli, 'fetch_build_records', return_value=build_records):
            cli = CreateSnapshotCli(
                runtime=self.runtime,
                konflux_kubeconfig=self.konflux_kubeconfig,
                konflux_context=self.konflux_context,
                konflux_namespace=self.konflux_namespace,
                for_bundle=self.for_bundle,
                builds=['test-nvr-1', 'test-nvr-2'],
                dry_run=self.dry_run
            )

            await cli.run()

            self.konflux_client._create.assert_called_once_with({
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
                                    'source': {'git': {'revision': 'main', 'url': 'https://github.com/test/repo1'}}},
                                   {'containerImage': 'registry/image@sha256:digest2',
                                    'name': 'ose-4-18-component2',
                                    'source': {'git': {'revision': 'main', 'url': 'https://github.com/test/repo2'}}}]
                }
            })

    @patch("doozerlib.backend.konflux_client.KonfluxClient")
    @patch('elliottlib.runtime.Runtime')
    async def test_fetch_build_records_validation(self, mock_runtime, mock_konflux_client):
        mock_runtime.return_value = self.runtime
        mock_konflux_client.from_kubeconfig.return_value = self.konflux_client

        mock_records = ['mock_row1', 'mock_row2']
        self.runtime.konflux_db.get_build_records_by_nvrs = AsyncMock(return_value=mock_records)
        builds = ['openshift-v4.18.0-4.el9', 'openshift-clients-v4.18.0-4.el8']

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_kubeconfig=self.konflux_kubeconfig,
            konflux_context=self.konflux_context,
            konflux_namespace=self.konflux_namespace,
            for_bundle=self.for_bundle,
            builds=builds,
            dry_run=self.dry_run,
        )

        records = await cli.fetch_build_records()
        self.assertEqual(records, mock_records)
        self.runtime.konflux_db.get_build_records_by_nvrs.assert_called_once_with(
            builds, where=ANY, strict=True)
