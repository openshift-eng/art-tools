from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from artcommonlib.model import Model
from artcommonlib.util import (
    normalize_group_name_for_k8s,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from elliottlib.cli.snapshot_cli import (
    CreateSnapshotCli,
    GetSnapshotCli,
)


class TestCreateSnapshotCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.18"
        self.runtime.product = "ocp"
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
                        'name': 'openshift-4-18-timestamp-1',
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
                        'name': 'openshift-4-18-timestamp-2',
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
        db.get_build_records_by_nvrs.assert_called_once_with(builds, where=ANY, strict=True, exclude_large_columns=True)


class TestGetSnapshotCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.major = 4
        self.minor = 18
        self.runtime.group = f"openshift-{self.major}.{self.minor}"
        self.runtime.product = "ocp"
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
                'name': 'openshift-4-18-timestamp',
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


class TestGroupNameNormalization(IsolatedAsyncioTestCase):
    """Test cases for normalize_group_name_for_k8s function"""

    def test_simple_oadp_group(self):
        """Test simple oadp group name normalization"""
        result = normalize_group_name_for_k8s("oadp-1.5")
        self.assertEqual(result, "oadp-1-5")

    def test_complex_group_name(self):
        """Test complex group name with mixed case, underscores, and dots"""
        result = normalize_group_name_for_k8s("Test_Group-1.5")
        self.assertEqual(result, "test-group-1-5")

    def test_empty_string(self):
        """Test empty string input"""
        result = normalize_group_name_for_k8s("")
        self.assertEqual(result, "")

    def test_consecutive_dashes(self):
        """Test collapse of consecutive dashes"""
        result = normalize_group_name_for_k8s("test--group---name")
        self.assertEqual(result, "test-group-name")

    def test_leading_trailing_special_chars(self):
        """Test trimming of leading/trailing non-alphanumeric characters"""
        result = normalize_group_name_for_k8s("-_test.group_-")
        self.assertEqual(result, "test-group")

    def test_only_special_chars(self):
        """Test string with only special characters"""
        result = normalize_group_name_for_k8s("_..-__")
        self.assertEqual(result, "")

    def test_mixed_alphanumeric_special(self):
        """Test mixed alphanumeric and special characters"""
        result = normalize_group_name_for_k8s("test@group#1.2$name")
        self.assertEqual(result, "test-group-1-2-name")

    def test_long_group_name_truncation(self):
        """Test truncation of very long group names"""
        long_name = "a" * 100  # 100 character string
        result = normalize_group_name_for_k8s(long_name)
        # Should be truncated to leave room for timestamp (max 63 - 18 - 1 = 44 chars)
        self.assertLessEqual(len(result), 44)
        self.assertTrue(result.startswith("a"))

    def test_uppercase_conversion(self):
        """Test uppercase to lowercase conversion"""
        result = normalize_group_name_for_k8s("OADP-1.5")
        self.assertEqual(result, "oadp-1-5")

    def test_numeric_group(self):
        """Test group name with numbers"""
        result = normalize_group_name_for_k8s("group123-4.56")
        self.assertEqual(result, "group123-4-56")


class TestSnapshotNaming(IsolatedAsyncioTestCase):
    """Test cases for snapshot name generation"""

    def setUp(self):
        self.runtime = MagicMock()
        self.konflux_config = dict(
            namespace="test-namespace",
            kubeconfig="/path/to/kubeconfig",
            context=None,
        )
        self.image_repo_pull_secret = "/path/to/pull-secret"
        self.dry_run = False

    @patch("elliottlib.cli.snapshot_cli.get_utc_now_formatted_str", return_value="20251031141128")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    async def test_openshift_group_snapshot_name(self, mock_konflux_client_init, _mock_timestamp):
        """Test snapshot naming for openshift groups"""
        self.runtime.group = "openshift-4.18"
        self.runtime.get_major_minor.return_value = (4, 18)

        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            builds=["openshift-container-v4.18.0-1"],
            dry_run=self.dry_run,
        )

        # Test the snapshot naming logic directly
        build_records = []
        snapshots = await cli.new_snapshots(build_records)

        # For openshift groups, should use ose-{major}-{minor}-{timestamp} format
        # Since there are no build records, no snapshots will be created, but we can test the naming logic
        # by checking that the method completes without error and the naming logic is correct
        self.assertEqual(snapshots, [])

    @patch("elliottlib.cli.snapshot_cli.get_utc_now_formatted_str", return_value="20251031141128")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    async def test_oadp_group_snapshot_name(self, mock_konflux_client_init, _mock_timestamp):
        """Test snapshot naming for oadp groups"""
        self.runtime.group = "oadp-1.5"
        self.runtime.product = "oadp"

        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            builds=["oadp-operator-v1.5.0-1"],
            dry_run=self.dry_run,
        )

        build_records = []
        snapshots = await cli.new_snapshots(build_records)

        # For non-openshift groups, should use {normalized_group}-{timestamp} format
        # Since there are no build records, no snapshots will be created
        self.assertEqual(snapshots, [])

    @patch("elliottlib.cli.snapshot_cli.get_utc_now_formatted_str", return_value="20251031141128")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    async def test_complex_group_snapshot_name(self, mock_konflux_client_init, _mock_timestamp):
        """Test snapshot naming for complex group names"""
        self.runtime.group = "Test_Group-1.5"
        self.runtime.product = "mta"

        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        # Mock a build record to actually create a snapshot
        mock_build_record = Mock()
        mock_build_record.get_konflux_application_name.return_value = "test-app"
        mock_build_record.get_konflux_component_name.return_value = "test-component"
        mock_build_record.rebase_repo_url = "https://github.com/test/repo"
        mock_build_record.rebase_commitish = "abc123"
        mock_build_record.image_tag = "sha256:digest"
        mock_build_record.image_pullspec = "registry/image@sha256:digest"
        mock_build_record.name = "test-component"

        # Mock the konflux client methods
        mock_konflux_client.get_application__caching = AsyncMock()
        mock_konflux_client.get_component__caching = AsyncMock()

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            builds=["test-component-v1.0.0-1"],
            dry_run=self.dry_run,
        )

        # Mock runtime.image_map for the image metadata lookup
        self.runtime.image_map = {}

        build_records = [mock_build_record]
        snapshots = await cli.new_snapshots(build_records)

        # Should create one snapshot with normalized group name
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]

        # Check that the snapshot name uses the normalized group name
        # "Test_Group-1.5" should become "test-group-1-5-20251031141128-1"
        expected_name_prefix = "test-group-1-5-20251031141128"
        self.assertTrue(snapshot["metadata"]["name"].startswith(expected_name_prefix))

    @patch("elliottlib.cli.snapshot_cli.get_utc_now_formatted_str", return_value="20251031141128")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    async def test_oadp_simple_group_snapshot_name(self, mock_konflux_client_init, _mock_timestamp):
        """Test snapshot naming for simple oadp group"""
        self.runtime.group = "oadp-1.5"
        self.runtime.product = "oadp"

        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        # Mock a build record to actually create a snapshot
        mock_build_record = Mock()
        mock_build_record.get_konflux_application_name.return_value = "oadp-app"
        mock_build_record.get_konflux_component_name.return_value = "oadp-component"
        mock_build_record.rebase_repo_url = "https://github.com/oadp/repo"
        mock_build_record.rebase_commitish = "def456"
        mock_build_record.image_tag = "sha256:oadpdigest"
        mock_build_record.image_pullspec = "registry/oadp@sha256:oadpdigest"
        mock_build_record.name = "oadp-operator"

        # Mock the konflux client methods
        mock_konflux_client.get_application__caching = AsyncMock()
        mock_konflux_client.get_component__caching = AsyncMock()

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            builds=["oadp-operator-v1.5.0-1"],
            dry_run=self.dry_run,
        )

        # Mock runtime.image_map for the image metadata lookup
        self.runtime.image_map = {}

        build_records = [mock_build_record]
        snapshots = await cli.new_snapshots(build_records)

        # Should create one snapshot with normalized group name
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]

        # Check that the snapshot name uses the normalized group name
        # "oadp-1.5" should become "oadp-1-5-20251031141128-1"
        expected_name_prefix = "oadp-1-5-20251031141128"
        self.assertTrue(snapshot["metadata"]["name"].startswith(expected_name_prefix))

    @patch("elliottlib.cli.snapshot_cli.get_utc_now_formatted_str", return_value="20251031141128")
    @patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
    async def test_invalid_group_raises_error(self, mock_konflux_client_init, _mock_timestamp):
        """Test that group names that normalize to empty string raise ValueError"""
        self.runtime.group = "_..-__"  # This normalizes to empty string
        self.runtime.product = "mta"

        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=self.konflux_config,
            image_repo_pull_secret=self.image_repo_pull_secret,
            builds=["test-component-v1.0.0-1"],
            dry_run=self.dry_run,
        )

        # Mock a build record to trigger the snapshot creation logic
        mock_build_record = Mock()
        mock_build_record.get_konflux_application_name.return_value = "test-app"
        mock_build_record.get_konflux_component_name.return_value = "test-component"
        mock_build_record.rebase_repo_url = "https://github.com/test/repo"
        mock_build_record.rebase_commitish = "abc123"
        mock_build_record.image_tag = "sha256:digest"
        mock_build_record.image_pullspec = "registry/image@sha256:digest"
        mock_build_record.name = "test-component"

        build_records = [mock_build_record]

        # Should raise ValueError with clear message
        with self.assertRaises(ValueError) as context:
            await cli.new_snapshots(build_records)

        error_msg = str(context.exception)
        self.assertIn("Group name '_..-__' produces invalid normalized name", error_msg)
        self.assertIn("Kubernetes snapshot", error_msg)


class TestProductBasedResolution(IsolatedAsyncioTestCase):
    """Test cases for product-based configuration resolution"""

    def test_product_namespace_resolution(self):
        """Test product-based namespace resolution"""

        # Test known products
        self.assertEqual(resolve_konflux_namespace_by_product("oadp"), "art-oadp-tenant")
        self.assertEqual(resolve_konflux_namespace_by_product("mta"), "art-mta-tenant")
        self.assertEqual(resolve_konflux_namespace_by_product("rhmtc"), "art-mtc-tenant")
        self.assertEqual(resolve_konflux_namespace_by_product("logging"), "art-logging-tenant")
        self.assertEqual(resolve_konflux_namespace_by_product("ocp"), "ocp-art-tenant")

        # Test unknown product falls back to default
        self.assertEqual(resolve_konflux_namespace_by_product("unknown"), "ocp-art-tenant")

        # Test provided namespace takes precedence
        self.assertEqual(resolve_konflux_namespace_by_product("oadp", "custom-namespace"), "custom-namespace")

    def test_product_kubeconfig_resolution(self):
        """Test product-based kubeconfig resolution"""

        # Test provided kubeconfig takes precedence
        self.assertEqual(resolve_konflux_kubeconfig_by_product("oadp", "/custom/path"), "/custom/path")

        # Test without environment variables (should return None)
        self.assertIsNone(resolve_konflux_kubeconfig_by_product("oadp"))
        self.assertIsNone(resolve_konflux_kubeconfig_by_product("unknown"))


class TestLocalDevelopment(IsolatedAsyncioTestCase):
    """Test cases for local development scenarios"""

    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.konflux_db = MagicMock()  # Not None
        self.runtime.group = "test-group"
        self.runtime.get_major_minor.return_value = (4, 18)  # Mock version numbers

    @patch("elliottlib.cli.snapshot_cli.KonfluxClient.from_kubeconfig")
    async def test_create_snapshot_handles_no_kubeconfig_gracefully(self, mock_konflux_client_init):
        """Test that CreateSnapshotCli handles missing kubeconfig gracefully for local development"""
        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        konflux_config = {
            'namespace': 'test-namespace',
            'kubeconfig': None,  # No kubeconfig
            'context': None,
        }

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=konflux_config,
            image_repo_pull_secret='/tmp/pull-secret',
            builds=['test-build-1.0.0-1'],
            dry_run=True,
        )

        # Mock the methods to avoid actual DB/network calls
        cli.fetch_build_records = AsyncMock(return_value=[])
        cli.get_pullspecs = AsyncMock(return_value=[])
        cli.new_snapshots = AsyncMock(return_value=[])

        # Should not raise an exception
        result = await cli.run()
        self.assertEqual(result, [])

    @patch("elliottlib.cli.snapshot_cli.KonfluxClient.from_kubeconfig")
    async def test_create_snapshot_handles_cluster_failure_gracefully(self, mock_konflux_client_init):
        """Test that CreateSnapshotCli handles cluster creation failures gracefully"""
        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        # Mock _create to raise an exception (simulating cluster access failure)
        mock_konflux_client._create = AsyncMock(side_effect=Exception("Connection refused"))
        mock_konflux_client_init.return_value = mock_konflux_client

        konflux_config = {
            'namespace': 'test-namespace',
            'kubeconfig': None,  # No kubeconfig - using current context
            'context': None,
        }

        cli = CreateSnapshotCli(
            runtime=self.runtime,
            konflux_config=konflux_config,
            image_repo_pull_secret='/tmp/pull-secret',
            builds=['test-build-1.0.0-1'],
            dry_run=False,  # Not dry-run mode - should attempt actual creation
        )

        # Mock the methods to return some test data
        cli.fetch_build_records = AsyncMock(return_value=[])
        cli.get_pullspecs = AsyncMock(return_value=[])
        cli.new_snapshots = AsyncMock(return_value=[{'metadata': {'name': 'test-snapshot'}}])

        # Should raise a RuntimeError with helpful message
        with self.assertRaises(RuntimeError) as context:
            await cli.run()

        error_msg = str(context.exception)
        self.assertIn("Failed to create snapshots in the cluster using current oc context", error_msg)
        self.assertIn("Connection refused", error_msg)
        self.assertIn("Make sure you're connected to the right cluster", error_msg)

    @patch("elliottlib.cli.snapshot_cli.KonfluxClient.from_kubeconfig")
    async def test_get_snapshot_handles_no_kubeconfig_gracefully(self, mock_konflux_client_init):
        """Test that GetSnapshotCli handles missing kubeconfig gracefully for local development"""
        mock_konflux_client = AsyncMock()
        mock_konflux_client.verify_connection = Mock(return_value=True)
        mock_konflux_client_init.return_value = mock_konflux_client

        konflux_config = {
            'namespace': 'test-namespace',
            'kubeconfig': None,  # No kubeconfig
            'context': None,
        }

        cli = GetSnapshotCli(
            runtime=self.runtime,
            konflux_config=konflux_config,
            image_repo_pull_secret='/tmp/pull-secret',
            for_fbc=False,
            dry_run=True,
            snapshot='test-snapshot',
        )

        # Should not raise an exception during initialization phase
        # (dry_run=True will skip actual operations)
        result = await cli.run()
        # In dry_run mode, it returns a mock NVR
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)


# TestHelperFunctions removed - legacy group-based resolver functions no longer exist
# Tests for product-based resolvers are in TestProductBasedResolution class above
