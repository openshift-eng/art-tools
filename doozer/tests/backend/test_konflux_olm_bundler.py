import re
from datetime import datetime, timezone
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBundleBuildRecord
from artcommonlib.konflux.konflux_db import Engine
from doozerlib import constants
from doozerlib.backend.konflux_olm_bundler import KonfluxOlmBundleBuilder, KonfluxOlmBundleRebaser


class TestKonfluxOlmBundleRebaser(IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_dir = Path("/path/to/base/dir")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.group_config = MagicMock()
        self.konflux_db = MagicMock()
        self.source_resolver = MagicMock()
        self.rebaser = KonfluxOlmBundleRebaser(
            base_dir=self.base_dir,
            group=self.group,
            assembly=self.assembly,
            group_config=self.group_config,
            konflux_db=self.konflux_db,
            source_resolver=self.source_resolver,
        )

    @patch("aiofiles.open")
    @patch("pathlib.Path.mkdir")
    async def test_create_oit_files(self, mock_mkdir, mock_open):
        bundle_dir = Path("/path/to/bundle/dir")
        operator_nvr = "test-operator-1.0-1"
        operands = {
            "operand1": ("old_pullspec1", "new_pullspec1", "operand1-1.0-1"),
            "operand2": ("old_pullspec2", "new_pullspec2", "operand2-1.0-1"),
        }

        mock_file = mock_open.return_value.__aenter__.return_value

        await self.rebaser._create_oit_files(
            "test-operator", "test-operator.v1.0.1", bundle_dir, operator_nvr, operands
        )

        mock_mkdir.assert_called_once_with(exist_ok=True)
        mock_open.assert_called_once_with(bundle_dir / ".oit" / "olm_bundle_info.yaml", "w")
        mock_file.write.assert_called_once()
        written_content = yaml.safe_load(mock_file.write.call_args[0][0])
        self.assertEqual(written_content["operator"]["nvr"], operator_nvr)
        self.assertEqual(written_content["operands"]["operand1"]["nvr"], "operand1-1.0-1")
        self.assertEqual(written_content["operands"]["operand1"]["internal_pullspec"], "old_pullspec1")
        self.assertEqual(written_content["operands"]["operand1"]["public_pullspec"], "new_pullspec1")
        self.assertEqual(written_content["operands"]["operand2"]["nvr"], "operand2-1.0-1")
        self.assertEqual(written_content["operands"]["operand2"]["internal_pullspec"], "old_pullspec2")
        self.assertEqual(written_content["operands"]["operand2"]["public_pullspec"], "new_pullspec2")

    def test_get_image_reference_pattern(self):
        registry = "registry.example.com"
        pattern = KonfluxOlmBundleRebaser._get_image_reference_pattern(registry)
        self.assertIsInstance(pattern, re.Pattern)
        match = pattern.match("registry.example.com/namespace/image:tag")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(0), "registry.example.com/namespace/image:tag")
        self.assertEqual(match.group(1), "namespace/image")
        self.assertEqual(match.group(2), "tag")

    @patch("doozerlib.util.oc_image_info_for_arch_async__caching")
    async def test_replace_image_references(self, mock_oc_image_info):
        old_registry = "registry.example.com"
        content = """
        apiVersion: v1
        kind: Pod
        metadata:
            name: test-pod
        spec:
            containers:
            - name: test-container
            image: registry.example.com/namespace/image:tag
        """
        mock_image_info = {
            'config': {
                'config': {
                    'Labels': {
                        'com.redhat.component': 'test-brew-component',
                        'version': '1.0',
                        'release': '1',
                    },
                    'Env': {
                        '__doozer_key=test-component',
                    },
                },
            },
            'listDigest': 'sha256:1234567890abcdef',
            'contentDigest': 'sha256:abcdef1234567890',
        }
        mock_oc_image_info.return_value = mock_image_info
        self.rebaser._group_config.operator_image_ref_mode = 'manifest-list'
        self.rebaser._group_config.get.return_value = 'namespace'

        new_content, found_images = await self.rebaser._replace_image_references(old_registry, content, Engine.KONFLUX)

        expected_new_content = """
        apiVersion: v1
        kind: Pod
        metadata:
            name: test-pod
        spec:
            containers:
            - name: test-container
            image: registry.redhat.io/openshift4/image@sha256:1234567890abcdef
        """
        self.assertEqual(new_content.strip(), expected_new_content.strip())
        self.assertIn('image', found_images)
        self.assertEqual(
            found_images['image'],
            (
                'registry.example.com/namespace/image:tag',
                'registry.redhat.io/openshift4/image@sha256:1234567890abcdef',
                'test-brew-component-1.0-1',
            ),
        )

    def test_operator_index_mode(self):
        # Test when operator_index_mode is 'pre-release'
        self.rebaser._group_config.operator_index_mode = 'pre-release'
        self.assertEqual(self.rebaser._operator_index_mode, 'pre-release')

        # Test when operator_index_mode is 'ga'
        self.rebaser._group_config.operator_index_mode = 'ga'
        del self.rebaser._operator_index_mode
        self.assertEqual(self.rebaser._operator_index_mode, 'ga')

        # Test when operator_index_mode is 'ga-plus'
        self.rebaser._group_config.operator_index_mode = 'ga-plus'
        del self.rebaser._operator_index_mode
        self.assertEqual(self.rebaser._operator_index_mode, 'ga-plus')

        # Test when operator_index_mode is invalid
        self.rebaser._group_config.operator_index_mode = 'invalid-mode'
        del self.rebaser._operator_index_mode
        with self.assertLogs(self.rebaser._logger, level='WARNING') as cm:
            self.assertEqual(self.rebaser._operator_index_mode, 'ga')
            self.assertIn(
                'invalid-mode is not a valid group_config.operator_index_mode. Defaulting to "ga"', cm.output[0]
            )

        # Test when operator_index_mode is None (default to 'ga')
        self.rebaser._group_config.operator_index_mode = None
        del self.rebaser._operator_index_mode
        self.assertEqual(self.rebaser._operator_index_mode, 'ga')

    def test_redhat_delivery_tags(self):
        # Test when operator_index_mode is 'pre-release'
        self.rebaser._group_config.operator_index_mode = 'pre-release'
        self.rebaser._group_config.vars = {'MAJOR': '4', 'MINOR': '8'}
        expected_tags = {
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': '=v4.8',
            'com.redhat.prerelease': 'true',
        }
        self.assertEqual(self.rebaser._redhat_delivery_tags, expected_tags)

        # Test when operator_index_mode is 'ga'
        self.rebaser._group_config.operator_index_mode = 'ga'
        del self.rebaser._operator_index_mode
        del self.rebaser._redhat_delivery_tags
        expected_tags = {
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': '=v4.8',
        }
        self.assertEqual(self.rebaser._redhat_delivery_tags, expected_tags)

        # Test when operator_index_mode is 'ga-plus'
        self.rebaser._group_config.operator_index_mode = 'ga-plus'
        del self.rebaser._operator_index_mode
        del self.rebaser._redhat_delivery_tags
        expected_tags = {
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': 'v4.8',
        }
        self.assertEqual(self.rebaser._redhat_delivery_tags, expected_tags)

        # Test when operator_index_mode is invalid (should default to 'ga')
        self.rebaser._group_config.operator_index_mode = 'invalid-mode'
        del self.rebaser._operator_index_mode
        del self.rebaser._redhat_delivery_tags
        with self.assertLogs(self.rebaser._logger, level='WARNING') as cm:
            expected_tags = {
                'com.redhat.delivery.operator.bundle': 'true',
                'com.redhat.openshift.versions': '=v4.8',
            }
            self.assertEqual(self.rebaser._redhat_delivery_tags, expected_tags)
            self.assertIn(
                'invalid-mode is not a valid group_config.operator_index_mode. Defaulting to "ga"', cm.output[0]
            )

        # Test when operator_index_mode is None (default to 'ga')
        self.rebaser._group_config.operator_index_mode = None
        del self.rebaser._operator_index_mode
        del self.rebaser._redhat_delivery_tags
        expected_tags = {
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': '=v4.8',
        }
        self.assertEqual(self.rebaser._redhat_delivery_tags, expected_tags)

    def test_get_operator_framework_tags(self):
        # Test when operator_channel_stable is 'default'
        self.rebaser._group_config.operator_channel_stable = 'default'
        channel_name = "test-channel"
        package_name = "test-package"
        expected_tags = {
            'operators.operatorframework.io.bundle.channel.default.v1': 'stable',
            'operators.operatorframework.io.bundle.channels.v1': 'test-channel,stable',
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': 'test-package',
        }
        actual_tags = self.rebaser._get_operator_framework_tags(channel_name, package_name)
        self.assertEqual(actual_tags, expected_tags)

        # Test when operator_channel_stable is 'extra'
        self.rebaser._group_config.operator_channel_stable = 'extra'
        expected_tags = {
            'operators.operatorframework.io.bundle.channel.default.v1': 'test-channel',
            'operators.operatorframework.io.bundle.channels.v1': 'test-channel,stable',
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': 'test-package',
        }
        actual_tags = self.rebaser._get_operator_framework_tags(channel_name, package_name)
        self.assertEqual(actual_tags, expected_tags)

        # Test when operator_channel_stable is None
        self.rebaser._group_config.operator_channel_stable = None
        expected_tags = {
            'operators.operatorframework.io.bundle.channel.default.v1': 'test-channel',
            'operators.operatorframework.io.bundle.channels.v1': 'test-channel',
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': 'test-package',
        }
        actual_tags = self.rebaser._get_operator_framework_tags(channel_name, package_name)
        self.assertEqual(actual_tags, expected_tags)

    def test_create_dockerfile(self):
        metadata = MagicMock()
        metadata.get_olm_bundle_brew_component_name.return_value = "test-component"
        metadata.get_olm_bundle_image_name.return_value = "test-image"

        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        operator_framework_tags = {
            'operators.operatorframework.io.bundle.channel.default.v1': 'stable',
            'operators.operatorframework.io.bundle.channels.v1': 'test-channel,stable',
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': 'test-package',
        }
        input_release = "1.0-1"

        self.rebaser._group_config.vars = {'MAJOR': '4', 'MINOR': '8'}

        with patch("doozerlib.backend.konflux_olm_bundler.DockerfileParser") as mock_dockerfile_parser:
            mock_operator_df = MagicMock()
            mock_operator_df.labels = {
                'com.redhat.component': 'test-component',
                'version': '1.0',
                'release': '1',
                'distribution-scope': 'public',
                'url': 'https://example.com',
            }
            mock_bundle_df = MagicMock()
            mock_dockerfile_parser.side_effect = [mock_operator_df, mock_bundle_df]

            self.rebaser._create_dockerfile(metadata, operator_dir, bundle_dir, operator_framework_tags, input_release)

            mock_dockerfile_parser.assert_any_call(str(operator_dir.joinpath('Dockerfile')))
            mock_dockerfile_parser.assert_any_call(str(bundle_dir.joinpath('Dockerfile')))

            self.assertEqual(
                mock_bundle_df.content, 'FROM scratch\nCOPY ./manifests /manifests\nCOPY ./metadata /metadata'
            )
            self.assertEqual(
                mock_bundle_df.labels,
                {
                    'com.redhat.component': 'test-component',
                    'com.redhat.delivery.appregistry': '',
                    'name': 'test-image',
                    'version': '1.0.1',
                    'release': '1.0-1',
                    'com.redhat.delivery.operator.bundle': 'true',
                    'com.redhat.openshift.versions': '=v4.8',
                    'operators.operatorframework.io.bundle.channel.default.v1': 'stable',
                    'operators.operatorframework.io.bundle.channels.v1': 'test-channel,stable',
                    'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
                    'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
                    'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
                    'operators.operatorframework.io.bundle.package.v1': 'test-package',
                    'distribution-scope': 'public',
                    'url': 'https://example.com',
                },
            )

    @patch("pathlib.Path.iterdir")
    @patch("aiofiles.open")
    @patch("pathlib.Path.mkdir")
    @patch("glob.glob")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._replace_image_references")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._create_dockerfile")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._create_oit_files")
    async def test_rebase_dir(
        self,
        mock_create_oit_files,
        mock_create_dockerfile,
        mock_replace_image_references,
        mock_glob,
        mock_mkdir,
        mock_open,
        mock_iterdir,
    ):
        metadata = MagicMock()
        metadata.config = {
            'update-csv': {
                'manifests-dir': 'manifests',
                'bundle-dir': 'bundle',
                'valid-subscription-label': 'valid-subscription',
                'registry': 'registry.example.com',
            },
        }
        metadata.distgit_key = "test-distgit-key"
        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        mock_glob.return_value = ["/path/to/operator/dir/manifests/package.yaml"]
        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read.return_value = yaml.safe_dump(
            {
                'packageName': 'test-package',
                'channels': [{'name': 'test-channel', 'currentCSV': 'test-operator.v1.0.0'}],
            }
        )

        new_content = """
apiVersion: operators.coreos.com/v1alpha1
kind: ClusterServiceVersion
metadata:
  annotations: {}
  name: test-operator.v1.0.1
spec:
  description: This is a test operator
  displayName: Test Operator
        """
        mock_replace_image_references.return_value = (
            new_content,
            {
                'image': ('old_pullspec', 'new_pullspec', 'test-component-1.0-1'),
            },
        )

        bundle_files = [
            Path("/path/to/operator/dir/manifests/bundle/file.yaml"),
            Path("/path/to/operator/dir/manifests/bundle/another-file.yaml"),
            Path("/path/to/operator/dir/manifests/bundle/image-references"),
            Path("/path/to/operator/dir/manifests/bundle/test.clusterserviceversion.yaml"),
        ]
        mock_iterdir.side_effect = lambda: iter(bundle_files)

        operator_nvr = "test-component-1.0-1"
        await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, MagicMock(nvr=operator_nvr), input_release)

        mock_mkdir.assert_any_call(parents=True, exist_ok=True)
        mock_open.assert_any_call("/path/to/operator/dir/manifests/package.yaml", 'r')
        mock_open.assert_any_call(Path("/path/to/bundle/dir/manifests/file.yaml"), 'w')
        mock_open.assert_any_call(Path("/path/to/bundle/dir/manifests/another-file.yaml"), 'w')
        mock_open.assert_any_call(Path("/path/to/bundle/dir/manifests/test.clusterserviceversion.yaml"), 'w')
        mock_open.assert_any_call(Path("/path/to/bundle/dir/metadata/annotations.yaml"), 'w')
        mock_create_dockerfile.assert_called_once_with(
            metadata,
            operator_dir,
            bundle_dir,
            {
                'operators.operatorframework.io.bundle.channel.default.v1': 'test-channel',
                'operators.operatorframework.io.bundle.channels.v1': 'test-channel',
                'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
                'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
                'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
                'operators.operatorframework.io.bundle.package.v1': 'test-package',
            },
            input_release,
        )
        mock_create_oit_files.assert_called_once_with(
            'test-package',
            'test-operator.v1.0.0',
            bundle_dir,
            operator_nvr,
            {
                'image': ('old_pullspec', 'new_pullspec', 'test-component-1.0-1'),
            },
        )

    async def test_rebase_dir_no_update_csv(self):
        metadata = MagicMock()
        metadata.config = {}
        metadata.distgit_key = "test-distgit-key"
        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        with self.assertRaises(ValueError) as context:
            await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, MagicMock(), input_release)
        self.assertIn("No update-csv config found in the operator's metadata", str(context.exception))

    async def test_rebase_dir_no_manifests_dir(self):
        metadata = MagicMock()
        metadata.config = {
            'update-csv': {
                'bundle-dir': 'bundle',
                'valid-subscription-label': 'valid-subscription',
                'registry': 'registry.example.com',
            },
        }
        metadata.distgit_key = "test-distgit-key"
        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        with self.assertRaises(ValueError) as context:
            await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, MagicMock(), input_release)
        self.assertIn("No manifests-dir defined in the operator's update-csv", str(context.exception))

    async def test_rebase_dir_no_bundle_dir(self):
        metadata = MagicMock()
        metadata.config = {
            'update-csv': {
                'manifests-dir': 'manifests',
                'valid-subscription-label': 'valid-subscription',
                'registry': 'registry.example.com',
            },
        }
        metadata.distgit_key = "test-distgit-key"
        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        with self.assertRaises(ValueError) as context:
            await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, MagicMock(), input_release)
        self.assertIn("No bundle-dir defined in the operator's update-csv", str(context.exception))

    async def test_rebase_dir_no_valid_subscription_label(self):
        metadata = MagicMock()
        metadata.config = {
            'update-csv': {
                'manifests-dir': 'manifests',
                'bundle-dir': 'bundle',
                'registry': 'registry.example.com',
            },
        }
        metadata.distgit_key = "test-distgit-key"
        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        with self.assertRaises(ValueError) as context:
            await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, MagicMock(), input_release)
        self.assertIn("No valid-subscription-label defined in the operator's update-csv", str(context.exception))

    @patch("pathlib.Path.iterdir", return_value=iter([]))
    async def test_rebase_dir_no_files_in_bundle_dir(self, _):
        metadata = MagicMock()
        metadata.config = {
            'update-csv': {
                'manifests-dir': 'manifests',
                'bundle-dir': 'bundle',
                'valid-subscription-label': 'valid-subscription',
                'registry': 'registry.example.com',
            },
        }
        metadata.distgit_key = "test-distgit-key"
        operator_dir = Path("/path/to/base/dir/operator/dir")
        bundle_dir = Path("/path/to/base/dir/bundle/dir")
        input_release = "1.0-1"

        with self.assertRaises(FileNotFoundError) as context:
            await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, MagicMock(), input_release)
            self.assertIn("No files found in bundle directory", str(context.exception))


class TestKonfluxOlmBundleBuilder(IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_dir = Path("/path/to/base/dir")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.source_resolver = MagicMock()
        self.db = MagicMock(record_cls=KonfluxBundleBuildRecord)
        self.konflux_namespace = "test-namespace"
        self.konflux_kubeconfig = None
        self.konflux_context = None
        self.image_repo = "test-image-repo"
        self.skip_checks = False
        self.dry_run = False
        self.konflux_client = AsyncMock()
        with patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient") as mock_konflux_client:
            mock_konflux_client.return_value = self.konflux_client
            mock_konflux_client.from_kubeconfig.return_value = self.konflux_client
            self.builder = KonfluxOlmBundleBuilder(
                base_dir=self.base_dir,
                group=self.group,
                assembly=self.assembly,
                source_resolver=self.source_resolver,
                db=self.db,
                konflux_namespace=self.konflux_namespace,
                konflux_kubeconfig=self.konflux_kubeconfig,
                konflux_context=self.konflux_context,
                image_repo=self.image_repo,
                skip_checks=self.skip_checks,
                dry_run=self.dry_run,
            )

    @patch("doozerlib.backend.konflux_olm_bundler.DockerfileParser")
    async def test_start_build(self, mock_dockerfile_parser):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        bundle_build_repo = MagicMock()
        bundle_build_repo.commit_hash = "test-commit-hash"
        bundle_build_repo.branch = None
        bundle_build_repo.https_url = "https://example.com/repo.git"
        additional_tags = ["tag1", "tag2"]

        mock_dockerfile = MagicMock()
        mock_dockerfile.labels = {
            'com.redhat.component': 'test-component',
            'version': '1.0',
            'release': '1',
        }
        mock_dockerfile_parser.return_value = mock_dockerfile

        pipelinerun = MagicMock()
        pipelinerun.metadata.name = "test-pipelinerun"
        self.konflux_client.start_pipeline_run_for_image_build.return_value = pipelinerun
        self.konflux_client.build_pipeline_url = MagicMock(return_value="https://example.com/pipelinerun")

        pipelinerun, url = await self.builder._start_build(
            metadata,
            bundle_build_repo,
            f"{self.image_repo}:test-component-1.0-1",
            self.konflux_namespace,
            self.skip_checks,
            additional_tags,
        )

        self.konflux_client.ensure_application.assert_called_once_with(name="test-group", display_name="test-group")
        self.konflux_client.ensure_component.assert_called_once_with(
            name="test-group-test-bundle",
            application="test-group",
            component_name="test-group-test-bundle",
            image_repo=self.image_repo,
            source_url=bundle_build_repo.https_url,
            revision=bundle_build_repo.commit_hash,
        )
        self.konflux_client.start_pipeline_run_for_image_build.assert_called_once_with(
            generate_name="test-group-test-bundle-",
            namespace=self.konflux_namespace,
            application_name="test-group",
            component_name='test-group-test-bundle',
            git_url=bundle_build_repo.https_url,
            commit_sha=bundle_build_repo.commit_hash,
            target_branch=bundle_build_repo.commit_hash,
            output_image=f"{self.image_repo}:test-component-1.0-1",
            vm_override={},
            building_arches=["x86_64"],
            additional_tags=additional_tags,
            skip_checks=self.skip_checks,
            hermetic=True,
            pipelinerun_template_url=constants.KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL,
            artifact_type="operatorbundle",
        )
        self.assertEqual(url, "https://example.com/pipelinerun")

    async def test_start_build_no_commit_hash(self):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        bundle_build_repo = MagicMock()
        bundle_build_repo.commit_hash = None

        with self.assertRaises(IOError) as context:
            await self.builder._start_build(metadata, bundle_build_repo, self.image_repo, self.konflux_namespace)
        self.assertIn("Bundle repository must have a commit to build", str(context.exception))

    @patch("aiofiles.open")
    @patch("doozerlib.backend.konflux_olm_bundler.DockerfileParser")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.build_pipeline_url")
    async def test_update_konflux_db_success(self, mock_build_pipeline_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun = MagicMock()
        pipelinerun.status.results = [
            {'name': 'IMAGE_URL', 'value': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:test-image'},
            {
                'name': 'IMAGE_DIGEST',
                'value': 'sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807',
            },
        ]
        pipelinerun.metadata.name = "test-pipelinerun"
        pipelinerun.status.startTime = "2023-10-01T12:00:00Z"
        pipelinerun.status.completionTime = "2023-10-01T12:30:00Z"
        pipelinerun.metadata.name = "test-pipelinerun"

        mock_build_pipeline_url.return_value = "https://example.com/pipelinerun"

        mock_dockerfile = MagicMock()
        mock_dockerfile.labels = {
            'io.openshift.build.source-location': 'https://example.com/source-repo.git',
            'io.openshift.build.commit.id': 'source-commit-id',
            'com.redhat.component': 'test-component',
            'version': '1.0',
            'release': '1',
        }
        mock_dockerfile_parser.return_value = mock_dockerfile

        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read.return_value = yaml.safe_dump(
            {
                'operator': {'nvr': 'test-operator-1.0-1'},
                'operands': {
                    'operand1': {'nvr': 'operand1-1.0-1'},
                    'operand2': {'nvr': 'operand2-1.0-1'},
                },
            }
        )

        await self.builder._update_konflux_db(
            metadata,
            build_repo,
            'test-operator',
            'test-operator-1.0',
            pipelinerun,
            KonfluxBuildOutcome.SUCCESS,
            'test-operator-1.0-1',
            ["operand1-1.0-1", "operand2-1.0-1"],
        )

        self.db.add_build.assert_called_once()
        build_record = self.db.add_build.call_args[0][0]
        self.assertEqual(build_record.name, "test-bundle")
        self.assertEqual(build_record.version, "1.0")
        self.assertEqual(build_record.release, "1")
        self.assertEqual(build_record.nvr, "test-component-1.0-1")
        self.assertEqual(build_record.group, "test-group")
        self.assertEqual(build_record.assembly, "test-assembly")
        self.assertEqual(build_record.source_repo, "https://example.com/source-repo.git")
        self.assertEqual(build_record.commitish, "source-commit-id")
        self.assertEqual(build_record.rebase_repo_url, "https://example.com/repo.git")
        self.assertEqual(build_record.rebase_commitish, "test-commit-hash")
        self.assertEqual(build_record.engine, Engine.KONFLUX)
        self.assertEqual(build_record.outcome, KonfluxBuildOutcome.SUCCESS)
        self.assertEqual(build_record.art_job_url, 'n/a')
        self.assertEqual(build_record.build_id, "test-pipelinerun")
        self.assertEqual(build_record.build_pipeline_url, "https://example.com/pipelinerun")
        self.assertEqual(build_record.operator_nvr, "test-operator-1.0-1")
        self.assertEqual(build_record.operand_nvrs, ["operand1-1.0-1", "operand2-1.0-1"])
        self.assertEqual(
            build_record.image_pullspec,
            "quay.io/openshift-release-dev/ocp-v4.0-art-dev-test@sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807",
        )
        self.assertEqual(build_record.image_tag, "test-image")
        self.assertEqual(build_record.start_time, datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(build_record.end_time, datetime(2023, 10, 1, 12, 30, 0, tzinfo=timezone.utc))

    @patch("aiofiles.open")
    @patch("doozerlib.backend.konflux_olm_bundler.DockerfileParser")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.build_pipeline_url")
    async def test_update_konflux_db_failure(self, mock_build_pipeline_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun = MagicMock()
        pipelinerun.status.startTime = "2023-10-01T12:00:00Z"
        pipelinerun.status.completionTime = "2023-10-01T12:30:00Z"
        pipelinerun.metadata.name = "test-pipelinerun"

        mock_build_pipeline_url.return_value = "https://example.com/pipelinerun"

        mock_dockerfile = MagicMock()
        mock_dockerfile.labels = {
            'io.openshift.build.source-location': 'https://example.com/source-repo.git',
            'io.openshift.build.commit.id': 'source-commit-id',
            'com.redhat.component': 'test-component',
            'version': '1.0',
            'release': '1',
        }
        mock_dockerfile_parser.return_value = mock_dockerfile

        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read.return_value = yaml.safe_dump(
            {
                'operator': {'nvr': 'test-operator-1.0-1'},
                'operands': {
                    'operand1': {'nvr': 'operand1-1.0-1'},
                    'operand2': {'nvr': 'operand2-1.0-1'},
                },
            }
        )

        await self.builder._update_konflux_db(
            metadata,
            build_repo,
            'test-operator',
            'test-operator-1.0',
            pipelinerun,
            KonfluxBuildOutcome.FAILURE,
            'test-operator-1.0-1',
            ["operand1-1.0-1", "operand2-1.0-1"],
        )

        self.db.add_build.assert_called_once()
        build_record = self.db.add_build.call_args[0][0]
        self.assertEqual(build_record.name, "test-bundle")
        self.assertEqual(build_record.version, "1.0")
        self.assertEqual(build_record.release, "1")
        self.assertEqual(build_record.nvr, "test-component-1.0-1")
        self.assertEqual(build_record.group, "test-group")
        self.assertEqual(build_record.assembly, "test-assembly")
        self.assertEqual(build_record.source_repo, "https://example.com/source-repo.git")
        self.assertEqual(build_record.commitish, "source-commit-id")
        self.assertEqual(build_record.rebase_repo_url, "https://example.com/repo.git")
        self.assertEqual(build_record.rebase_commitish, "test-commit-hash")
        self.assertEqual(build_record.engine, Engine.KONFLUX)
        self.assertEqual(build_record.outcome, KonfluxBuildOutcome.FAILURE)
        self.assertEqual(build_record.art_job_url, 'n/a')
        self.assertEqual(build_record.build_id, "test-pipelinerun")
        self.assertEqual(build_record.build_pipeline_url, "https://example.com/pipelinerun")
        self.assertEqual(build_record.operator_nvr, "test-operator-1.0-1")
        self.assertEqual(build_record.operand_nvrs, ["operand1-1.0-1", "operand2-1.0-1"])
        self.assertEqual(build_record.start_time, datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(build_record.end_time, datetime(2023, 10, 1, 12, 30, 0, tzinfo=timezone.utc))

    @patch("aiofiles.open")
    @patch("doozerlib.backend.konflux_olm_bundler.DockerfileParser")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.build_pipeline_url")
    async def test_update_konflux_db_no_db_connection(self, mock_build_pipeline_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun = MagicMock()
        pipelinerun.status.results = [
            {'name': 'IMAGE_URL', 'value': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:test-image'},
            {
                'name': 'IMAGE_DIGEST',
                'value': 'sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807',
            },
        ]
        pipelinerun.status.startTime = "2023-10-01T12:00:00Z"
        pipelinerun.status.completionTime = "2023-10-01T12:30:00Z"
        pipelinerun.metadata.name = "test-pipelinerun"

        mock_build_pipeline_url.return_value = "https://example.com/pipelinerun"

        mock_dockerfile = MagicMock()
        mock_dockerfile.labels = {
            'io.openshift.build.source-location': 'https://example.com/source-repo.git',
            'io.openshift.build.commit.id': 'source-commit-id',
            'com.redhat.component': 'test-component',
            'version': '1.0',
            'release': '1',
        }
        mock_dockerfile_parser.return_value = mock_dockerfile

        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read.return_value = yaml.safe_dump(
            {
                'operator': {'nvr': 'test-operator-1.0-1'},
                'operands': {
                    'operand1': {'nvr': 'operand1-1.0-1'},
                    'operand2': {'nvr': 'operand2-1.0-1'},
                },
            }
        )

        self.builder._db = None

        with self.assertLogs(self.builder._logger, level='WARNING') as cm:
            await self.builder._update_konflux_db(
                metadata,
                build_repo,
                'test-operator',
                'test-operator-1.0',
                pipelinerun,
                KonfluxBuildOutcome.SUCCESS,
                'test-operator-1.0-1',
                ["operand1-1.0-1", "operand2-1.0-1"],
            )
            self.assertIn(
                'Konflux DB connection is not initialized, not writing build record to the Konflux DB.', cm.output[0]
            )

    @patch("aiofiles.open")
    @patch("doozerlib.backend.konflux_olm_bundler.DockerfileParser")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.build_pipeline_url")
    async def test_update_konflux_db_exception(self, mock_build_pipeline_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun = MagicMock()
        pipelinerun.status.results = [
            {'name': 'IMAGE_URL', 'value': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:test-image'},
            {
                'name': 'IMAGE_DIGEST',
                'value': 'sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807',
            },
        ]
        pipelinerun.status.startTime = "2023-10-01T12:00:00Z"
        pipelinerun.status.completionTime = "2023-10-01T12:30:00Z"
        pipelinerun.metadata.name = "test-pipelinerun"

        mock_build_pipeline_url.return_value = "https://example.com/pipelinerun"

        mock_dockerfile = MagicMock()
        mock_dockerfile.labels = {
            'io.openshift.build.source-location': 'https://example.com/source-repo.git',
            'io.openshift.build.commit.id': 'source-commit-id',
            'com.redhat.component': 'test-component',
            'version': '1.0',
            'release': '1',
        }
        mock_dockerfile_parser.return_value = mock_dockerfile

        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read.return_value = yaml.safe_dump(
            {
                'operator': {'nvr': 'test-operator-1.0-1'},
                'operands': {
                    'operand1': {'nvr': 'operand1-1.0-1'},
                    'operand2': {'nvr': 'operand2-1.0-1'},
                },
            }
        )

        self.db.add_build.side_effect = Exception("Test exception")

        with self.assertLogs(self.builder._logger, level='ERROR') as cm:
            await self.builder._update_konflux_db(
                metadata,
                build_repo,
                'test-operator',
                'test-operator-1.0',
                pipelinerun,
                KonfluxBuildOutcome.SUCCESS,
                'test-operator-1.0-1',
                ["operand1-1.0-1", "operand2-1.0-1"],
            )
            self.assertIn('Failed writing record to the konflux DB: Test exception', cm.output[0])
