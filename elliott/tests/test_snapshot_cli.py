from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from artcommonlib.model import Model
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from elliottlib.cli.snapshot_cli import CreateSnapshotCli, GetSnapshotCli


class TestCreateSnapshotCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )
        self.image_repo_pull_secret = "/path/to/pull-secret"
        self.dry_run = False

        self.runtime.konflux_db.bind.return_value = None
        self.runtime.get_major_minor.return_value = (4, 18)
        self.runtime.konflux_db = MagicMock()

        self.konflux_client = AsyncMock()
        # Patch verify_connection to be a regular Mock, not AsyncMock
        self.konflux_client.verify_connection = Mock(return_value=True)

    @patch("elliottlib.cli.snapshot_cli.get_utc_now_formatted_str", return_value="timestamp")
    @patch("elliottlib.cli.snapshot_cli.oc_image_info_for_arch_async")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_konflux_client_init, mock_oc_image_info, *_):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client
        mock_oc_image_info.return_value = AsyncMock()

        build_records = [
            Mock(
                nvr='component1-container-v1.0.0-1',
                image_pullspec='registry/image@sha256:digest1',
                image_tag='tag1',
                rebase_repo_url='https://github.com/test/repo1',
                rebase_commitish='foobar',
                get_konflux_application_name=Mock(return_value='openshift-4-18'),
                get_konflux_component_name=Mock(return_value='ose-4-18-component1'),
            ),
            Mock(
                nvr='component2-container-v1.0.0-1',
                image_pullspec='registry/image@sha256:digest2',
                image_tag='tag2',
                rebase_repo_url='https://github.com/test/repo2',
                rebase_commitish='test',
                get_konflux_application_name=Mock(return_value='openshift-4-18'),
                get_konflux_component_name=Mock(return_value='ose-4-18-component2'),
            ),
            Mock(
                nvr='component3-container-v1.0.0-1',
                image_pullspec='registry/image@sha256:digest3',
                image_tag='tag3',
                rebase_repo_url='https://github.com/test/repo3',
                rebase_commitish='deadbeef',
                get_konflux_application_name=Mock(return_value='fbc-openshift-4-18'),
                get_konflux_component_name=Mock(return_value='fbc-ose-4-18-component3'),
            ),
        ]
        # `name` attribute is special so set it after creation
        # https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
        build_records[0].name = 'component1'
        build_records[1].name = 'component2'
        build_records[2].name = 'component3'

        self.konflux_client._create.side_effect = lambda data: data
        self.konflux_client.resource_url = MagicMock(return_value="https://whatever")

        with patch.object(CreateSnapshotCli, 'fetch_build_records', return_value=build_records):
            cli = CreateSnapshotCli(
                runtime=self.runtime,
                konflux_config=self.konflux_config,
                image_repo_pull_secret=self.image_repo_pull_secret,
                builds=['test-nvr-1', 'test-nvr-2', 'test-nvr-3'],
                dry_run=self.dry_run,
            )

            await cli.run()
            self.konflux_client._create.assert_any_await(
                {
                    'apiVersion': 'appstudio.redhat.com/v1alpha1',
                    'kind': 'Snapshot',
                    'metadata': {
                        'labels': {
                            'test.appstudio.openshift.io/type': 'override',
                            'appstudio.openshift.io/application': 'fbc-openshift-4-18',
                        },
                        'name': 'ose-4-18-timestamp-1',
                        'namespace': 'test-namespace',
                    },
                    'spec': {
                        'application': 'fbc-openshift-4-18',
                        'components': [
                            {
                                'containerImage': 'registry/image@sha256:digest3',
                                'name': 'fbc-ose-4-18-component3',
                                'source': {'git': {'revision': 'deadbeef', 'url': 'https://github.com/test/repo3'}},
                            },
                        ],
                    },
                }
            )
            self.konflux_client._create.assert_any_await(
                {
                    'apiVersion': 'appstudio.redhat.com/v1alpha1',
                    'kind': 'Snapshot',
                    'metadata': {
                        'labels': {
                            'test.appstudio.openshift.io/type': 'override',
                            'appstudio.openshift.io/application': 'openshift-4-18',
                        },
                        'name': 'ose-4-18-timestamp-2',
                        'namespace': 'test-namespace',
                    },
                    'spec': {
                        'application': 'openshift-4-18',
                        'components': [
                            {
                                'containerImage': 'registry/image@sha256:digest1',
                                'name': 'ose-4-18-component1',
                                'source': {'git': {'revision': 'foobar', 'url': 'https://github.com/test/repo1'}},
                            },
                            {
                                'containerImage': 'registry/image@sha256:digest2',
                                'name': 'ose-4-18-component2',
                                'source': {'git': {'revision': 'test', 'url': 'https://github.com/test/repo2'}},
                            },
                        ],
                    },
                }
            )

    @patch("elliottlib.cli.snapshot_cli.KonfluxDb")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch('elliottlib.runtime.Runtime')
    async def test_fetch_build_records_validation(self, mock_runtime, mock_konflux_client_init, MockKonfluxDb):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client
        db = MockKonfluxDb.return_value

        builds = ['openshift-container-v4.18.0-4.el9', 'openshift-clientst-container-v4.18.0-4.el8']
        mock_records = [Mock(nvr=builds[0]), Mock(nvr=builds[1])]
        db.get_build_records_by_nvrs = AsyncMock(return_value=mock_records)

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            builds=builds,
            dry_run=self.dry_run,
        )

        records = await cli.fetch_build_records()
        self.assertEqual(records, mock_records)
        db.get_build_records_by_nvrs.assert_called_once_with(builds, where=ANY, strict=True)


class TestGetSnapshotCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.major = 4
        self.minor = 18
        self.runtime.group = f"openshift-{self.major}.{self.minor}"
        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )
        self.image_repo_pull_secret = "/path/to/pull-secret"
        self.for_fbc = False
        self.dry_run = False

        self.runtime.konflux_db.bind.return_value = None
        self.runtime.get_major_minor.return_value = (self.major, self.minor)
        self.runtime.konflux_db = MagicMock()

        self.konflux_client = AsyncMock()
        # Patch verify_connection to be a regular Mock, not AsyncMock
        self.konflux_client.verify_connection = Mock(return_value=True)

    @patch("elliottlib.cli.snapshot_cli.KonfluxDb")
    @patch("elliottlib.cli.snapshot_cli.oc_image_info_for_arch_async")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_konflux_client_init, mock_oc_image_info, MockKonfluxDb):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client
        MockKonfluxDb.return_value = AsyncMock()

        snapshot = {
            'apiVersion': 'appstudio.redhat.com/v1alpha1',
            'kind': 'Snapshot',
            'metadata': {
                'labels': {
                    'test.appstudio.openshift.io/type': 'override',
                },
                'name': 'ose-4-18-timestamp',
                'namespace': 'test-namespace',
            },
            'spec': {
                'application': 'openshift-4-18',
                'components': [
                    {
                        'containerImage': 'registry/image@sha256:digest1',
                        'name': 'ose-4-18-component1',
                        'source': {'git': {'revision': 'foobar', 'url': 'https://github.com/test/repo1'}},
                    }
                ],
            },
        }
        # Ensure _get is an AsyncMock and returns Model(snapshot)
        self.konflux_client._get = AsyncMock(return_value=Model(snapshot))
        mock_oc_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "com.redhat.component": "test-component-container",
                        "version": f"v{self.major}.{self.minor}.0",
                        "release": "assembly.test.el8",
                    },
                },
            },
        }
        expected_nvrs = [f"test-component-container-v{self.major}.{self.minor}.0-assembly.test.el8"]
        cli = GetSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret='/path/to/pull-secret',
            for_fbc=self.for_fbc,
            snapshot='test-snapshot',
            dry_run=self.dry_run,
        )
        actual_nvrs = await cli.run()
        self.konflux_client._get.assert_awaited_once_with(API_VERSION, KIND_SNAPSHOT, 'test-snapshot')
        mock_oc_image_info.assert_called_once_with(
            "registry/image@sha256:digest1",
            registry_config=self.image_repo_pull_secret,
        )
        self.assertEqual(expected_nvrs, actual_nvrs)
