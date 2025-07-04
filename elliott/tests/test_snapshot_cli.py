from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from artcommonlib.model import Model
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
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
        self.for_fbc = False
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

        # Mock the necessary async methods that will be called
        self.konflux_client._get_api = AsyncMock()
        self.konflux_client._get = AsyncMock()
        self.konflux_client._create = AsyncMock()
        self.konflux_client.resource_url = Mock(return_value="http://test-url")

        build_records = [
            Mock(
                nvr='test-nvr-1',
                image_pullspec='registry/image@sha256:digest1',
                image_tag='tag1',
                rebase_repo_url='https://github.com/test/repo1',
                rebase_commitish='foobar',
                component='',  # Empty string to trigger fallback
                bundle_component='',
                fbc_component='',
            ),
            Mock(
                nvr='test-nvr-2',
                image_pullspec='registry/image@sha256:digest2',
                image_tag='tag2',
                rebase_repo_url='https://github.com/test/repo2',
                rebase_commitish='test',
                component='',  # Empty string to trigger fallback
                bundle_component='',
                fbc_component='',
            ),
        ]
        # `name` attribute is special so set it after creation
        # https://docs.python.org/3/library/unittest.mock.html#mock-names-and-the-name-attribute
        build_records[0].name = 'component1'
        build_records[1].name = 'component2'

        with (
            patch.object(CreateSnapshotCli, 'fetch_build_records', return_value=build_records),
            patch.object(KonfluxImageBuilder, 'get_component_name', side_effect=lambda app, name: f"{app}-{name}"),
        ):
            cli = CreateSnapshotCli(
                runtime=self.runtime,
                konflux_config=self.konflux_config,
                image_repo_pull_secret=self.image_repo_pull_secret,
                for_fbc=self.for_fbc,
                builds=['test-nvr-1', 'test-nvr-2'],
                dry_run=self.dry_run,
            )

            await cli.run()

            self.konflux_client._create.assert_called_once_with(
                {
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
                                'name': 'openshift-4-18-component1',
                                'source': {'git': {'revision': 'foobar', 'url': 'https://github.com/test/repo1'}},
                            },
                            {
                                'containerImage': 'registry/image@sha256:digest2',
                                'name': 'openshift-4-18-component2',
                                'source': {'git': {'revision': 'test', 'url': 'https://github.com/test/repo2'}},
                            },
                        ],
                    },
                }
            )

    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch('elliottlib.runtime.Runtime')
    async def test_fetch_build_records_validation(self, mock_runtime, mock_konflux_client_init):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client

        builds = ['openshift-v4.18.0-4.el9', 'openshift-clients-v4.18.0-4.el8']
        mock_records = [Mock(nvr=builds[0]), Mock(nvr=builds[1])]
        self.runtime.konflux_db.get_build_records_by_nvrs = AsyncMock(return_value=mock_records)

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            for_fbc=self.for_fbc,
            builds=builds,
            dry_run=self.dry_run,
        )

        records = await cli.fetch_build_records()
        self.assertEqual(records, mock_records)
        self.runtime.konflux_db.get_build_records_by_nvrs.assert_called_once_with(builds, where=ANY, strict=True)


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

    @patch("elliottlib.cli.snapshot_cli.oc_image_info_for_arch_async")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    @patch("elliottlib.runtime.Runtime")
    async def test_run_happy_path(self, mock_runtime, mock_konflux_client_init, mock_oc_image_info):
        mock_runtime.return_value = self.runtime
        mock_konflux_client_init.return_value = self.konflux_client
        self.runtime.konflux_db.get_build_records_by_nvrs = AsyncMock()

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
                        "com.redhat.component": "test-component",
                        "version": f"v{self.major}.{self.minor}.0",
                        "release": "assembly.test.el8",
                    },
                },
            },
        }
        expected_nvrs = [f"test-component-v{self.major}.{self.minor}.0-assembly.test.el8"]
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


class TestCompFunction(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.runtime.get_major_minor.return_value = (4, 18)
        self.application_name = "openshift-4-18"

        # Mock the konflux client before creating the CLI
        self.mock_konflux_client = AsyncMock()
        self.mock_konflux_client.verify_connection = Mock(return_value=True)
        self.mock_konflux_client._get_api = AsyncMock()
        self.mock_konflux_client._get = AsyncMock()

        with patch(
            "doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig", return_value=self.mock_konflux_client
        ):
            self.cli = CreateSnapshotCli(
                runtime=self.runtime,
                konflux_config={"namespace": "test-ns", "kubeconfig": "/fake/path", "context": None},
                image_repo_pull_secret="",
                for_fbc=False,
                builds=["test-build"],
                dry_run=True,
            )

    async def test_comp_priority_order_all_fields_present(self):
        """Test component name prioritization when all fields are present"""
        # Create record with all component name fields
        record = Mock()
        record.component = "primary-component"
        record.bundle_component = "bundle-component"
        record.fbc_component = "fbc-component"
        record.name = "base-name"
        record.rebase_repo_url = "https://github.com/test/repo"
        record.rebase_commitish = "abc123"
        record.image_tag = "latest"
        record.image_pullspec = "registry/image@sha256:digest"

        # Call the _comp function directly
        comp_result = await self.cli.new_snapshot([record])
        component = comp_result["spec"]["components"][0]

        # Should prioritize 'component' field first
        self.assertEqual(component["name"], "primary-component")
        self.assertEqual(component["containerImage"], "registry/image@sha256:digest")
        self.assertEqual(component["source"]["git"]["url"], "https://github.com/test/repo")
        self.assertEqual(component["source"]["git"]["revision"], "abc123")

    async def test_comp_priority_order_bundle_fallback(self):
        """Test component name falls back to bundle_component when component is empty"""
        record = Mock()
        record.component = ""  # Empty primary component
        record.bundle_component = "my-bundle-component"
        record.fbc_component = "fbc-component"
        record.name = "base-name"
        record.rebase_repo_url = "https://github.com/test/repo"
        record.rebase_commitish = "def456"
        record.image_tag = "v1.0"
        record.image_pullspec = "registry/bundle@sha256:digest"

        comp_result = await self.cli.new_snapshot([record])
        component = comp_result["spec"]["components"][0]

        # Should use bundle_component since component is empty
        self.assertEqual(component["name"], "my-bundle-component")

    async def test_comp_priority_order_fbc_fallback(self):
        """Test component name falls back to fbc_component when component and bundle_component are empty"""
        record = Mock()
        record.component = ""
        record.bundle_component = ""  # Empty bundle component
        record.fbc_component = "my-fbc-component"
        record.name = "base-name"
        record.rebase_repo_url = "https://github.com/test/repo"
        record.rebase_commitish = "ghi789"
        record.image_tag = "v2.0"
        record.image_pullspec = "registry/fbc@sha256:digest"

        comp_result = await self.cli.new_snapshot([record])
        component = comp_result["spec"]["components"][0]

        # Should use fbc_component since both component and bundle_component are empty
        self.assertEqual(component["name"], "my-fbc-component")

    async def test_comp_backward_compatibility_missing_attributes(self):
        """Test backward compatibility when build record lacks component name attributes"""

        # Create a more realistic record that doesn't have component attributes
        class LegacyRecord:
            def __init__(self):
                self.name = "legacy-component"
                self.rebase_repo_url = "https://github.com/legacy/repo"
                self.rebase_commitish = "legacy123"
                self.image_tag = "legacy-tag"
                self.image_pullspec = "registry/legacy@sha256:digest"
                # Intentionally don't set component, bundle_component, fbc_component

        record = LegacyRecord()

        with patch.object(
            KonfluxImageBuilder, 'get_component_name', return_value="openshift-4-18-legacy-component"
        ) as mock_get_component:
            comp_result = await self.cli.new_snapshot([record])
            component = comp_result["spec"]["components"][0]

            # Should fall back to default component name generation
            self.assertEqual(component["name"], "openshift-4-18-legacy-component")
            mock_get_component.assert_called_once_with(self.application_name, "legacy-component")

    async def test_comp_backward_compatibility_empty_strings(self):
        """Test backward compatibility when component fields exist but are empty strings"""
        record = Mock()
        record.component = ""
        record.bundle_component = ""
        record.fbc_component = ""
        record.name = "empty-component"
        record.rebase_repo_url = "https://github.com/empty/repo"
        record.rebase_commitish = "empty456"
        record.image_tag = "empty-tag"
        record.image_pullspec = "registry/empty@sha256:digest"

        with patch.object(
            KonfluxImageBuilder, 'get_component_name', return_value="openshift-4-18-empty-component"
        ) as mock_get_component:
            comp_result = await self.cli.new_snapshot([record])
            component = comp_result["spec"]["components"][0]

            # Should fall back to default when all fields are empty strings
            self.assertEqual(component["name"], "openshift-4-18-empty-component")
            mock_get_component.assert_called_once_with(self.application_name, "empty-component")
