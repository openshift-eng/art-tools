import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBundleBuildRecord
from artcommonlib.konflux.konflux_db import Engine
from artcommonlib.model import Model
from doozerlib import constants
from doozerlib.backend.konflux_client import ImageBuildParams
from doozerlib.backend.konflux_olm_bundler import (
    KonfluxOlmBundleBuilder,
    KonfluxOlmBundleRebaseError,
    KonfluxOlmBundleRebaser,
)
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo


class TestKonfluxOlmBundleRebaser(IsolatedAsyncioTestCase):
    def setUp(self):
        self.base_dir = Path("/path/to/base/dir")
        self.group = "test-group"
        self.assembly = "test-assembly"
        self.group_config = MagicMock()
        self.konflux_db = MagicMock()
        self.source_resolver = MagicMock()
        # Ensure fresh logger for proper assertLogs() behavior
        self.test_logger = logging.getLogger('doozerlib.backend.konflux_olm_bundler')
        self.rebaser = KonfluxOlmBundleRebaser(
            base_dir=self.base_dir,
            group=self.group,
            assembly=self.assembly,
            group_config=self.group_config,
            konflux_db=self.konflux_db,
            source_resolver=self.source_resolver,
            logger=self.test_logger,
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

    @patch("doozerlib.util.oc_image_info_for_arch_async")
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
                        # Public (non-embargoed) Konflux release, so the embargo-substitution
                        # path introduced for layered-product bundles is not exercised here.
                        'release': '202607151200.p2.g172d0b2.assembly.stream.el9',
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
        # operator_image_ref_mode defaults to 'manifest-list' (uses listDigest)
        self.rebaser._group_config.get.return_value = 'namespace'
        self.rebaser._group_config.vars = {'MAJOR': 4}
        metadata = MagicMock()
        metadata.runtime.group = "openshift-4.19"
        new_content, found_images = await self.rebaser._replace_image_references(
            old_registry, content, Engine.KONFLUX, metadata
        )

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
                'test-brew-component-1.0-202607151200.p2.g172d0b2.assembly.stream.el9',
            ),
        )

    @patch("doozerlib.util.oc_image_info_for_arch_async")
    async def test_replace_image_references_ocp4_namespace(self, mock_oc_image_info):
        """Test that OCP 4.x uses openshift4 namespace"""
        old_registry = "registry.example.com"
        content = """image: registry.example.com/namespace/image:tag"""
        mock_image_info = {
            'config': {
                'config': {
                    'Labels': {
                        'com.redhat.component': 'test-component',
                        'version': '1.0',
                        # Public (non-embargoed) Konflux release.
                        'release': '202607151200.p2.g172d0b2.assembly.stream.el9',
                    },
                },
            },
            'listDigest': 'sha256:listdigest123',
            'contentDigest': 'sha256:contentdigest456',
        }
        mock_oc_image_info.return_value = mock_image_info

        # Mock group_config with MAJOR=4
        self.rebaser._group_config.get.side_effect = lambda key, default=None: {
            'csv_namespace': 'namespace',
        }.get(key, default)
        self.rebaser._group_config.vars = {'MAJOR': 4}
        self.rebaser._group_config.operator_image_ref_mode = 'manifest-list'  # Uses listDigest

        metadata = MagicMock()
        metadata.runtime.group = "openshift-4.19"
        metadata.runtime.data_dir = "/tmp/nonexistent"

        new_content, _ = await self.rebaser._replace_image_references(old_registry, content, Engine.KONFLUX, metadata)

        # Should use openshift4 namespace and listDigest (manifest-list mode)
        self.assertIn('registry.redhat.io/openshift4/image@sha256:listdigest123', new_content)

    @patch("doozerlib.util.oc_image_info_for_arch_async")
    async def test_replace_image_references_ocp5_namespace(self, mock_oc_image_info):
        """Test that OCP 5.x uses openshift5 namespace"""
        old_registry = "registry.example.com"
        content = """image: registry.example.com/namespace/image:tag"""
        mock_image_info = {
            'config': {
                'config': {
                    'Labels': {
                        'com.redhat.component': 'test-component',
                        'version': '1.0',
                        # Public (non-embargoed) Konflux release.
                        'release': '202607151200.p2.g172d0b2.assembly.stream.el9',
                    },
                },
            },
            'listDigest': 'sha256:listdigest789',
            'contentDigest': 'sha256:contentdigestabc',
        }
        mock_oc_image_info.return_value = mock_image_info

        # Mock group_config with MAJOR=5
        self.rebaser._group_config.get.side_effect = lambda key, default=None: {
            'csv_namespace': 'namespace',
        }.get(key, default)
        self.rebaser._group_config.vars = {'MAJOR': 5}
        self.rebaser._group_config.operator_image_ref_mode = 'by-arch'  # Uses contentDigest

        metadata = MagicMock()
        metadata.runtime.group = "openshift-5.0"
        metadata.runtime.data_dir = "/tmp/nonexistent"

        new_content, _ = await self.rebaser._replace_image_references(old_registry, content, Engine.KONFLUX, metadata)

        # Should use openshift5 namespace and contentDigest (by-arch mode)
        self.assertIn('registry.redhat.io/openshift5/image@sha256:contentdigestabc', new_content)

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
        with self.assertLogs(self.rebaser._logger.name, level='WARNING') as cm:
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
        with self.assertLogs(self.rebaser._logger.name, level='WARNING') as cm:
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

    @patch("doozerlib.util.oc_image_info_for_arch_async")
    async def test_resolve_operands_from_db(self, mock_oc_image_info):
        """
        Verify that _resolve_operands_from_db resolves operand NVRs from the
        Konflux DB and returns correctly-formed pullspecs and NVRs.
        """
        metadata = MagicMock()
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"
        metadata.runtime.data_dir = "/nonexistent/data/dir"

        # Set up name_in_bundle_map and image_map
        operand_meta = MagicMock()
        operand_meta.image_name_short = "ose-operand"
        operand_meta.distgit_key = "operand"
        operand_meta.branch_el_target.return_value = 9

        mock_build = MagicMock()
        mock_build.version = "4.18.0"
        mock_build.release = "202506120000.p0.g1234567.assembly.stream.el9"
        mock_build.nvr = "operand-container-4.18.0-202506120000.p0.g1234567.assembly.stream.el9"
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=mock_build)

        metadata.runtime.name_in_bundle_map = {"operand-image": "operand"}
        metadata.runtime.image_map = {"operand": operand_meta}

        image_references = {
            "operand-image": {
                "name": "operand-image",
                "from": {"name": "registry.example.com/openshift/ose-operand:v4.18"},
            },
        }

        mock_oc_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "com.redhat.component": "operand-container",
                        "version": "4.18.0",
                        "release": "202506120000.p0.g1234567.assembly.stream.el9",
                    }
                }
            },
            "listDigest": "sha256:abc123def456",
            "contentDigest": "sha256:789xyz000111",
        }
        self.rebaser._group_config.get.return_value = "openshift"
        self.rebaser._group_config.operator_image_ref_mode = "manifest-list"
        self.rebaser._group_config.vars = {"MAJOR": 4}

        resolved, rebased_name_map = await self.rebaser._resolve_operands_from_db(metadata, image_references, {}, {})

        self.assertIn("ose-operand", resolved)
        old_spec, new_pullspec, nvr = resolved["ose-operand"]
        self.assertEqual(old_spec, "registry.example.com/openshift/ose-operand:v4.18")
        self.assertIn("registry.redhat.io/openshift4/ose-operand@sha256:abc123def456", new_pullspec)
        self.assertEqual(nvr, "operand-container-4.18.0-202506120000.p0.g1234567.assembly.stream.el9")
        self.assertEqual(rebased_name_map["ose-operand"], "ose-operand")

        # Verify DB query was made directly to Konflux
        operand_meta.get_latest_konflux_build.assert_called_once_with(
            el_target="el9",
            exclude_large_columns=True,
        )

    @patch("doozerlib.util.oc_image_info_for_arch_async")
    async def test_resolve_operands_from_db_by_arch_mode(self, mock_oc_image_info):
        """
        Verify that by-arch mode uses contentDigest instead of listDigest.
        """
        metadata = MagicMock()
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"
        metadata.runtime.data_dir = "/nonexistent/data/dir"

        operand_meta = MagicMock()
        operand_meta.image_name_short = "ose-operand"
        operand_meta.distgit_key = "operand"
        operand_meta.branch_el_target.return_value = 9
        mock_build = MagicMock()
        mock_build.version = "4.18.0"
        mock_build.release = "1.el9"
        mock_build.nvr = "operand-container-4.18.0-1.el9"
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=mock_build)

        metadata.runtime.name_in_bundle_map = {"operand-image": "operand"}
        metadata.runtime.image_map = {"operand": operand_meta}

        image_references = {
            "operand-image": {
                "name": "operand-image",
                "from": {"name": "registry.example.com/openshift/ose-operand:v4.18"},
            },
        }

        mock_oc_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "com.redhat.component": "operand-container",
                        "version": "4.18.0",
                        "release": "1.el9",
                    }
                }
            },
            "listDigest": "sha256:list111",
            "contentDigest": "sha256:content222",
        }
        self.rebaser._group_config.get.return_value = "openshift"
        self.rebaser._group_config.operator_image_ref_mode = "by-arch"
        self.rebaser._group_config.vars = {"MAJOR": 4}

        resolved, _rebased_name_map = await self.rebaser._resolve_operands_from_db(metadata, image_references, {}, {})

        _, new_pullspec, _ = resolved["ose-operand"]
        self.assertIn("sha256:content222", new_pullspec)

    async def test_resolve_operands_from_db_unknown_image(self):
        """
        Verify ValueError when image-references has an image not in name_in_bundle_map.
        """
        metadata = MagicMock()
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"
        metadata.runtime.data_dir = "/nonexistent/data/dir"
        metadata.runtime.name_in_bundle_map = {}

        image_references = {
            "unknown-image": {
                "name": "unknown-image",
                "from": {"name": "registry.example.com/openshift/unknown:v4.18"},
            },
        }

        with self.assertRaises(ValueError) as ctx:
            await self.rebaser._resolve_operands_from_db(metadata, image_references, {}, {})
        self.assertIn("Unable to find unknown-image in name_in_bundle_map", str(ctx.exception))

    async def test_resolve_operands_from_db_disabled_image(self):
        """
        Verify DoozerFatalError when operand image has mode disabled.
        """
        from doozerlib.exceptions import DoozerFatalError

        metadata = MagicMock()
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"
        metadata.runtime.data_dir = "/nonexistent/data/dir"
        metadata.runtime.name_in_bundle_map = {"disabled-image": "disabled-operand"}
        metadata.runtime.image_map = {}
        metadata.runtime.late_resolve_image.return_value = None

        image_references = {
            "disabled-image": {
                "name": "disabled-image",
                "from": {"name": "registry.example.com/openshift/disabled:v4.18"},
            },
        }

        with self.assertRaises(DoozerFatalError):
            await self.rebaser._resolve_operands_from_db(metadata, image_references, {}, {})

    async def test_resolve_operands_from_db_no_build(self):
        """
        Verify ValueError when no build found in DB for an operand.
        """
        metadata = MagicMock()
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"
        metadata.runtime.data_dir = "/nonexistent/data/dir"

        operand_meta = MagicMock()
        operand_meta.image_name_short = "ose-operand"
        operand_meta.distgit_key = "operand"
        operand_meta.branch_el_target.return_value = 9
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=None)

        metadata.runtime.name_in_bundle_map = {"operand-image": "operand"}
        metadata.runtime.image_map = {"operand": operand_meta}

        image_references = {
            "operand-image": {
                "name": "operand-image",
                "from": {"name": "registry.example.com/openshift/ose-operand:v4.18"},
            },
        }

        with self.assertRaises(ValueError) as ctx:
            await self.rebaser._resolve_operands_from_db(metadata, image_references, {}, {})
        self.assertIn("Could not find latest Konflux build", str(ctx.exception))

    @patch("doozerlib.util.oc_image_info_for_arch_async")
    async def test_resolve_operands_with_delivery_override(self, mock_oc_image_info):
        """
        Verify delivery_repo_name_override mapping works in _resolve_operands_from_db.
        """
        metadata = MagicMock()
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"
        metadata.runtime.data_dir = "/tmp/test-data-dir"

        operand_meta = MagicMock()
        operand_meta.image_name_short = "ose-csi-driver-4.18-rhel9"
        operand_meta.distgit_key = "ose-csi-driver"
        operand_meta.branch_el_target.return_value = 9
        mock_build = MagicMock()
        mock_build.version = "4.18.0"
        mock_build.release = "1.el9"
        mock_build.nvr = "ose-csi-driver-container-4.18.0-1.el9"
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=mock_build)

        metadata.runtime.name_in_bundle_map = {"csi-driver": "ose-csi-driver"}
        metadata.runtime.image_map = {"ose-csi-driver": operand_meta}

        image_references = {
            "csi-driver": {
                "name": "csi-driver",
                "from": {"name": "registry.example.com/openshift/ose-csi-driver-4.18-rhel9:v4.18"},
            },
        }

        mock_oc_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "com.redhat.component": "ose-csi-driver-container",
                        "version": "4.18.0",
                        "release": "1.el9",
                    }
                }
            },
            "listDigest": "sha256:abc123",
            "contentDigest": "sha256:def456",
        }
        self.rebaser._group_config.get.return_value = "openshift"
        self.rebaser._group_config.operator_image_ref_mode = "manifest-list"

        # Create a temporary YAML file with delivery_repo_name_override
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            metadata.runtime.data_dir = tmpdir
            images_dir = Path(tmpdir) / "images"
            images_dir.mkdir()
            img_yaml = images_dir / "ose-csi-driver.yml"
            img_yaml.write_text(
                yaml.safe_dump(
                    {
                        "name": "ose-csi-driver-4.18-rhel9",
                        "delivery": {
                            "delivery_repo_names": ["openshift4/ose-csi-driver-rhel9"],
                            "delivery_repo_name_override": True,
                        },
                    }
                )
            )

            delivery_override_map, delivery_namespace_map = self.rebaser._build_delivery_maps(metadata)
            resolved, _rebased_name_map = await self.rebaser._resolve_operands_from_db(
                metadata, image_references, delivery_override_map, delivery_namespace_map
            )

        # Should use the override name, not the versioned name
        self.assertIn("ose-csi-driver-rhel9", resolved)
        _, new_pullspec, _ = resolved["ose-csi-driver-rhel9"]
        self.assertIn("ose-csi-driver-rhel9", new_pullspec)

    @patch("doozerlib.util.oc_image_info_for_arch_async")
    async def test_resolve_operands_from_db_layered_upstream_registry(self, mock_oc_image_info):
        """
        Verify that for a layered product (e.g. OADP) whose image-references
        point to an upstream registry (quay.io/konveyor/*), the delivery
        pullspec uses the namespace and image name from ocp-build-data
        delivery_repo_names, not the upstream spec.

        Regression test: without the fix, the result would be
        registry.redhat.io/konveyor/velero@sha256:... instead of the correct
        registry.redhat.io/oadp/oadp-velero-rhel9@sha256:...
        """
        metadata = MagicMock()
        metadata.distgit_key = "oadp-operator"
        metadata.runtime.group = "oadp-1.5"
        metadata.runtime.data_dir = "/tmp/test-oadp-data"

        operand_meta = MagicMock()
        operand_meta.image_name_short = "oadp-velero-rhel9"
        operand_meta.distgit_key = "oadp-velero"
        operand_meta.branch_el_target.return_value = 9
        operand_meta.config = Model(
            {
                "delivery": {
                    "delivery_repo_names": ["oadp/oadp-velero-rhel9"],
                },
            }
        )

        mock_build = MagicMock()
        mock_build.version = "1.5.8"
        mock_build.release = "202507160000.p0.g1234567.assembly.stream.el9"
        mock_build.nvr = "oadp-velero-container-1.5.8-202507160000.p0.g1234567.assembly.stream.el9"
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=mock_build)

        metadata.runtime.name_in_bundle_map = {"oadp-velero-rhel9": "oadp-velero"}
        metadata.runtime.image_map = {"oadp-velero": operand_meta}

        image_references = {
            "oadp-velero-rhel9": {
                "name": "oadp-velero-rhel9",
                "from": {"name": "quay.io/konveyor/velero:oadp-1.5"},
            },
        }

        mock_oc_image_info.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "com.redhat.component": "oadp-velero-container",
                        "version": "1.5.8",
                        "release": "202507160000.p0.g1234567.assembly.stream.el9",
                    }
                }
            },
            "listDigest": "sha256:oadpvelerodigest111",
            "contentDigest": "sha256:oadpvelerocontentdigest222",
        }
        self.rebaser._group_config.get.return_value = "oadp"
        self.rebaser._group_config.operator_image_ref_mode = "manifest-list"
        self.rebaser._group_config.vars = {"MAJOR": 4}

        delivery_namespace_map = {"oadp-velero-rhel9": "oadp"}
        resolved, rebased_name_map = await self.rebaser._resolve_operands_from_db(
            metadata,
            image_references,
            {},
            delivery_namespace_map,
        )

        self.assertIn("oadp-velero-rhel9", resolved)
        old_spec, new_pullspec, nvr = resolved["oadp-velero-rhel9"]
        self.assertEqual(old_spec, "quay.io/konveyor/velero:oadp-1.5")
        self.assertEqual(new_pullspec, "registry.redhat.io/oadp/oadp-velero-rhel9@sha256:oadpvelerodigest111")
        self.assertNotIn("konveyor", new_pullspec)
        self.assertEqual(nvr, "oadp-velero-container-1.5.8-202507160000.p0.g1234567.assembly.stream.el9")
        self.assertEqual(rebased_name_map["oadp-velero-rhel9"], "oadp-velero-rhel9")

    @patch("pathlib.Path.iterdir")
    @patch("pathlib.Path.exists", autospec=True)
    @patch("pathlib.Path.glob")
    @patch("aiofiles.open")
    @patch("pathlib.Path.mkdir")
    @patch("glob.glob")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._build_delivery_maps")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._resolve_operands_from_db")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._create_dockerfile")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._create_oit_files")
    async def test_rebase_dir_konflux_uses_db_resolution(
        self,
        mock_create_oit_files,
        mock_create_dockerfile,
        mock_resolve_operands,
        mock_build_delivery_maps,
        mock_glob,
        mock_mkdir,
        mock_open,
        mock_path_glob,
        mock_path_exists,
        mock_iterdir,
    ):
        """
        Verify that _rebase_dir uses _resolve_operands_from_db for Konflux engine
        and that regex-based replacement correctly handles predicted tags in the CSV
        (which differ from the original specs in image-references).
        """
        metadata = MagicMock()
        metadata.config = {
            "update-csv": {
                "manifests-dir": "manifests",
                "bundle-dir": "bundle",
                "valid-subscription-label": "valid-subscription",
                "registry": "registry.example.com",
            },
        }
        metadata.distgit_key = "test-operator"
        # Use a layered product group — DB resolution is gated to non-OCP groups
        metadata.runtime.group = "mtc-1.8"

        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        operator_build = MagicMock()
        operator_build.engine = Engine.KONFLUX
        operator_build.nvr = "test-operator-1.0-1"

        mock_glob.return_value = ["/path/to/operator/dir/manifests/package.yaml"]

        image_refs_yaml = yaml.safe_dump(
            {
                "spec": {
                    "tags": [
                        {"name": "operand-a", "from": {"name": "registry.example.com/openshift/ose-operand-a:v4.18"}},
                    ]
                }
            }
        )
        package_yaml = yaml.safe_dump(
            {
                "packageName": "test-package",
                "channels": [{"name": "test-channel", "currentCSV": "test-operator.v1.0.0"}],
            }
        )

        # CSV content has PREDICTED tags from operator rebase, NOT the original specs
        csv_content = (
            "apiVersion: operators.coreos.com/v1alpha1\n"
            "kind: ClusterServiceVersion\n"
            "metadata:\n"
            "  annotations: {}\n"
            "  name: test-operator.v1.0.1\n"
            "spec:\n"
            "  install:\n"
            "    spec:\n"
            "      deployments:\n"
            "      - spec:\n"
            "          template:\n"
            "            spec:\n"
            "              containers:\n"
            "              - image: registry.example.com/openshift/ose-operand-a:4.18.0-202506120000.p0.assembly.stream.el9\n"
        )

        read_call_count = [0]
        file_contents = [package_yaml, image_refs_yaml, csv_content]

        async def mock_read():
            idx = read_call_count[0]
            read_call_count[0] += 1
            if idx < len(file_contents):
                return file_contents[idx]
            return ""

        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read = mock_read
        mock_file.write = AsyncMock()

        # Only image-references path should "exist"; dependencies/properties paths should not
        mock_path_exists.side_effect = lambda path_self: "image-references" in str(path_self)
        mock_path_glob.return_value = []

        bundle_files = [
            Path("/path/to/operator/dir/manifests/bundle/test.clusterserviceversion.yaml"),
            Path("/path/to/operator/dir/manifests/bundle/image-references"),
        ]
        mock_iterdir.side_effect = lambda: iter(bundle_files)

        mock_build_delivery_maps.return_value = ({}, {})
        mock_resolve_operands.return_value = (
            {
                "ose-operand-a": (
                    "registry.example.com/openshift/ose-operand-a:v4.18",
                    "registry.redhat.io/openshift4/ose-operand-a@sha256:abc123",
                    "operand-a-container-4.18.0-1.el9",
                ),
            },
            {"ose-operand-a": "ose-operand-a"},
        )

        await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, operator_build, input_release)

        mock_resolve_operands.assert_called_once()

        # Verify the predicted tag was replaced with the DB-resolved SHA pullspec
        # by checking what was written to the CSV file
        write_calls = mock_file.write.call_args_list
        csv_written = None
        for call in write_calls:
            written = call[0][0]
            if "ose-operand-a" in written or "sha256:abc123" in written:
                csv_written = written
                break
        self.assertIsNotNone(csv_written, "CSV content was not written")
        self.assertIn("sha256:abc123", csv_written)
        self.assertNotIn("4.18.0-202506120000.p0.assembly.stream.el9", csv_written)

        # Verify operands passed to _create_oit_files include DB-resolved data
        mock_create_oit_files.assert_called_once()
        operands_arg = mock_create_oit_files.call_args[0][4]
        self.assertIn("ose-operand-a", operands_arg)
        self.assertEqual(operands_arg["ose-operand-a"][2], "operand-a-container-4.18.0-1.el9")

    @patch("pathlib.Path.iterdir")
    @patch("pathlib.Path.exists")
    @patch("pathlib.Path.glob")
    @patch("aiofiles.open")
    @patch("pathlib.Path.mkdir")
    @patch("glob.glob")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._replace_image_references")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._create_dockerfile")
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxOlmBundleRebaser._create_oit_files")
    async def test_rebase_dir_brew_uses_legacy_resolution(
        self,
        mock_create_oit_files,
        mock_create_dockerfile,
        mock_replace_image_references,
        mock_glob,
        mock_mkdir,
        mock_open,
        mock_path_glob,
        mock_path_exists,
        mock_iterdir,
    ):
        """
        Verify that _rebase_dir still uses _replace_image_references for Brew engine.
        """
        metadata = MagicMock()
        metadata.config = {
            "update-csv": {
                "manifests-dir": "manifests",
                "bundle-dir": "bundle",
                "valid-subscription-label": "valid-subscription",
                "registry": "registry.example.com",
            },
        }
        metadata.distgit_key = "test-operator"
        metadata.runtime.group = "openshift-4.18"

        operator_dir = Path("/path/to/operator/dir")
        bundle_dir = Path("/path/to/bundle/dir")
        input_release = "1.0-1"

        # Operator build with Brew engine
        operator_build = MagicMock()
        operator_build.engine = Engine.BREW
        operator_build.nvr = "test-operator-1.0-1"

        mock_glob.return_value = ["/path/to/operator/dir/manifests/package.yaml"]

        package_yaml = yaml.safe_dump(
            {
                "packageName": "test-package",
                "channels": [{"name": "test-channel", "currentCSV": "test-operator.v1.0.0"}],
            }
        )

        mock_file = mock_open.return_value.__aenter__.return_value
        mock_file.read.return_value = package_yaml
        mock_file.write = AsyncMock()

        mock_path_exists.return_value = False
        mock_path_glob.return_value = []

        bundle_files = [
            Path("/path/to/operator/dir/manifests/bundle/file.yaml"),
            Path("/path/to/operator/dir/manifests/bundle/image-references"),
        ]
        mock_iterdir.side_effect = lambda: iter(bundle_files)

        content = "apiVersion: v1\nkind: Pod\n"
        mock_replace_image_references.return_value = (
            content,
            {"image": ("old_pullspec", "new_pullspec", "test-component-1.0-1")},
        )

        await self.rebaser._rebase_dir(metadata, operator_dir, bundle_dir, operator_build, input_release)

        # Verify _replace_image_references was called (Brew path)
        mock_replace_image_references.assert_called()


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
        # Ensure fresh logger for proper assertLogs() behavior
        self.test_logger = logging.getLogger('doozerlib.backend.konflux_olm_bundler')
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
                logger=self.test_logger,
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
        self.konflux_client.resource_url = MagicMock(return_value="https://example.com/pipelinerun")

        pipelinerun, url = await self.builder._start_build(
            metadata,
            bundle_build_repo,
            f"{self.image_repo}:test-component-1.0-1",
            self.konflux_namespace,
            self.skip_checks,
            additional_tags=additional_tags,
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
            building_arches=["x86_64"],
            pipelinerun_template_url=constants.KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL,
            build_params=ImageBuildParams(
                additional_tags=additional_tags,
                skip_checks=self.skip_checks,
                hermetic=True,
                fetch_tags=False,
                artifact_type="operatorbundle",
                build_priority="3",
            ),
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
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.resource_url")
    async def test_update_konflux_db_success(self, mock_resource_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun_dict = {
            'metadata': {'name': 'test-pipelinerun', 'labels': {'appstudio.openshift.io/component': 'test-component'}},
            'status': {
                'results': [
                    {'name': 'IMAGE_URL', 'value': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:test-image'},
                    {
                        'name': 'IMAGE_DIGEST',
                        'value': 'sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807',
                    },
                ],
                'startTime': '2023-10-01T12:00:00Z',
                'completionTime': '2023-10-01T12:30:00Z',
            },
        }
        pipelinerun = PipelineRunInfo(pipelinerun_dict, {})

        mock_resource_url.return_value = "https://example.com/pipelinerun"

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
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.resource_url")
    async def test_update_konflux_db_failure(self, mock_resource_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun_dict = {
            'metadata': {'name': 'test-pipelinerun', 'labels': {'appstudio.openshift.io/component': 'test-component'}},
            'status': {'startTime': '2023-10-01T12:00:00Z', 'completionTime': '2023-10-01T12:30:00Z'},
        }
        pipelinerun = PipelineRunInfo(pipelinerun_dict, {})

        mock_resource_url.return_value = "https://example.com/pipelinerun"

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
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.resource_url")
    async def test_update_konflux_db_no_db_connection(self, mock_resource_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun_dict = {
            'metadata': {'name': 'test-pipelinerun', 'labels': {'appstudio.openshift.io/component': 'test-component'}},
            'status': {
                'results': [
                    {'name': 'IMAGE_URL', 'value': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:test-image'},
                    {
                        'name': 'IMAGE_DIGEST',
                        'value': 'sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807',
                    },
                ],
                'startTime': '2023-10-01T12:00:00Z',
                'completionTime': '2023-10-01T12:30:00Z',
            },
        }
        pipelinerun = PipelineRunInfo(pipelinerun_dict, {})

        mock_resource_url.return_value = "https://example.com/pipelinerun"

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

        with self.assertLogs(self.builder._logger.name, level='WARNING') as cm:
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
    @patch("doozerlib.backend.konflux_olm_bundler.KonfluxClient.resource_url")
    async def test_update_konflux_db_exception(self, mock_resource_url, mock_dockerfile_parser, mock_open):
        metadata = MagicMock()
        metadata.distgit_key = "test-distgit-key"
        metadata.get_olm_bundle_short_name.return_value = "test-bundle"
        metadata.runtime.group = "test-group"
        metadata.runtime.assembly = "test-assembly"

        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path("/path/to/local/dir")

        pipelinerun_dict = {
            'metadata': {'name': 'test-pipelinerun', 'labels': {'appstudio.openshift.io/component': 'test-component'}},
            'status': {
                'results': [
                    {'name': 'IMAGE_URL', 'value': 'quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:test-image'},
                    {
                        'name': 'IMAGE_DIGEST',
                        'value': 'sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807',
                    },
                ],
                'startTime': '2023-10-01T12:00:00Z',
                'completionTime': '2023-10-01T12:30:00Z',
            },
        }
        pipelinerun = PipelineRunInfo(pipelinerun_dict, {})

        mock_resource_url.return_value = "https://example.com/pipelinerun"

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

        with self.assertLogs(self.builder._logger.name, level='ERROR') as cm:
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
            self.assertIn('Failed writing record to the konflux DB', cm.output[0])


class TestReplaceImageReferencesEmbargo(IsolatedAsyncioTestCase):
    """Covers the embargoed-operand substitution logic added to _replace_image_references()
    for layered-product operator bundles: embargoed operand references must never be shipped
    in a public bundle, so they are substituted with the latest public build, or the rebase
    fails clearly if no public substitute exists."""

    def setUp(self):
        self.rebaser = KonfluxOlmBundleRebaser(
            base_dir=Path('/tmp/nonexistent-base-dir'),
            group='openshift-4.18',
            assembly='stream',
            group_config=Model({}),
            konflux_db=MagicMock(),
            source_resolver=MagicMock(),
            image_repo='quay.io/example/repo',
            logger=MagicMock(),
        )

    def _make_metadata(self, image_metas=None):
        metadata = MagicMock()
        metadata.distgit_key = 'my-operator'
        metadata.runtime.group = 'openshift-4.18'
        metadata.runtime.data_dir = '/tmp/nonexistent-data-dir'
        metadata.runtime.image_metas.return_value = image_metas or []
        # By default, simulate a fully-loaded runtime where image_metas() already covers every
        # image in the group, so the late_resolve_image() fallback in
        # _build_component_to_meta_map() has nothing extra to find. Tests that specifically
        # exercise that fallback (e.g. because `doozer beta:images:konflux:bundle <nvr>` restricts
        # image_metas() to just the given operator) override these explicitly.
        metadata.runtime.image_map = {im.distgit_key: im for im in (image_metas or [])}
        metadata.runtime.image_name_map = {}
        return metadata

    @staticmethod
    def _image_info(component, version, release, digest_suffix):
        return {
            'config': {
                'config': {'Labels': {'version': version, 'release': release, 'com.redhat.component': component}}
            },
            'contentDigest': f'sha256:{digest_suffix}-content',
            'listDigest': f'sha256:{digest_suffix}-list',
        }

    async def test_non_embargoed_operand_resolves_normally(self):
        """A non-embargoed operand is resolved to its digest without any substitution."""
        content = 'image: registry.redhat.io/openshift4/mypublic-operand-rhel9:v1.0'
        metadata = self._make_metadata()

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            new_callable=AsyncMock,
            return_value=self._image_info(
                'mypublic-operand-container', 'v1.0', '202607151200.p2.g172d0b2.assembly.stream.el9', 'public'
            ),
        ) as mock_oc_info:
            new_content, found_images = await self.rebaser._replace_image_references(
                'registry.redhat.io', content, Engine.KONFLUX, metadata
            )

        mock_oc_info.assert_awaited_once()
        self.assertIn('registry.redhat.io/openshift4/mypublic-operand-rhel9@sha256:public-list', new_content)
        self.assertEqual(
            found_images['mypublic-operand-rhel9'][2],
            'mypublic-operand-container-v1.0-202607151200.p2.g172d0b2.assembly.stream.el9',
        )
        # The lazy component-metadata map should never be built when nothing is embargoed.
        metadata.runtime.image_metas.assert_not_called()

    async def test_embargoed_operand_is_substituted_with_public_build(self):
        """An embargoed operand is substituted with the latest public build of the same component."""
        content = 'image: registry.redhat.io/openshift4/my-embargoed-operand-rhel9:v1.0'
        metadata = self._make_metadata()

        public_build = MagicMock()
        public_build.nvr = 'my-embargoed-operand-container-v1.0-202607151201.p2.g8a1f3c4.assembly.stream.el9'
        public_build.image_pullspec = 'quay.io/example/repo@sha256:public-build-digest'

        operand_meta = MagicMock()
        operand_meta.distgit_key = 'my-embargoed-operand'
        operand_meta.get_component_name.return_value = 'my-embargoed-operand-container'
        operand_meta.branch_el_target.return_value = 9
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=public_build)
        metadata.runtime.image_metas.return_value = [operand_meta]

        embargoed_info = self._image_info(
            'my-embargoed-operand-container', 'v1.0', '202607151200.p3.g172d0b2.assembly.stream.el9', 'embargoed'
        )
        public_info = self._image_info(
            'my-embargoed-operand-container', 'v1.0', '202607151201.p2.g8a1f3c4.assembly.stream.el9', 'public-sub'
        )

        async def fake_oc_image_info(pullspec, registry_config=None):
            if pullspec == public_build.image_pullspec:
                return public_info
            return embargoed_info

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            side_effect=fake_oc_image_info,
        ) as mock_oc_info:
            new_content, found_images = await self.rebaser._replace_image_references(
                'registry.redhat.io', content, Engine.KONFLUX, metadata
            )

        self.assertEqual(mock_oc_info.call_count, 2)
        operand_meta.get_latest_konflux_build.assert_awaited_once_with(
            default=None, el_target=9, embargoed=False, exclude_large_columns=True
        )
        # The substituted (public) build's digest should be used in the final content, not the
        # embargoed build's digest.
        self.assertIn('registry.redhat.io/openshift4/my-embargoed-operand-rhel9@sha256:public-sub-list', new_content)
        self.assertNotIn('embargoed-list', new_content)
        self.assertEqual(
            found_images['my-embargoed-operand-rhel9'][2],
            public_build.nvr,
        )

    async def test_embargoed_operand_with_no_public_alternative_raises(self):
        """When an embargoed operand has no public build to substitute, the rebase fails clearly."""
        content = 'image: registry.redhat.io/openshift4/my-embargoed-operand-rhel9:v1.0'
        metadata = self._make_metadata()

        operand_meta = MagicMock()
        operand_meta.distgit_key = 'my-embargoed-operand'
        operand_meta.get_component_name.return_value = 'my-embargoed-operand-container'
        operand_meta.branch_el_target.return_value = 9
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=None)
        metadata.runtime.image_metas.return_value = [operand_meta]

        embargoed_info = self._image_info(
            'my-embargoed-operand-container', 'v1.0', '202607151200.p3.g172d0b2.assembly.stream.el9', 'embargoed'
        )

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            new_callable=AsyncMock,
            return_value=embargoed_info,
        ):
            with self.assertRaises(KonfluxOlmBundleRebaseError):
                await self.rebaser._replace_image_references('registry.redhat.io', content, Engine.KONFLUX, metadata)

    async def test_embargoed_operand_not_in_restricted_image_metas_is_found_via_late_resolve(self):
        """Reproduces a real production failure: `doozer beta:images:konflux:bundle <operator-nvr>`
        restricts runtime.image_metas() to just the given operator (see
        KonfluxBundleCli.get_operator_builds()), so an embargoed operand belonging to a different,
        unloaded image in the same group (e.g. a dependent image referenced by the operator's CSV)
        is invisible to image_metas() alone. _build_component_to_meta_map() must fall back to
        Runtime.late_resolve_image() to find it rather than failing with "no ART image metadata
        could be found"."""
        content = 'image: registry.redhat.io/openshift4/log-file-metric-exporter-rhel9:v1.0'
        # Only the operator itself is loaded into image_metas(); the operand ("dependent" image)
        # is not, mirroring the restricted `--images`-style loading done for bundle builds.
        metadata = self._make_metadata(image_metas=[])
        metadata.runtime.image_name_map = {
            'openshift-logging/log-file-metric-exporter-rhel9': 'log-file-metric-exporter',
            'log-file-metric-exporter-rhel9': 'log-file-metric-exporter',
        }
        metadata.runtime.image_map = {}  # not loaded for this invocation

        public_build = MagicMock()
        public_build.nvr = 'log-file-metric-exporter-container-6.6.1-202607160200.p2.g3c1201f.assembly.test.el9'
        public_build.image_pullspec = 'quay.io/example/repo@sha256:public-build-digest'

        operand_meta = MagicMock()
        operand_meta.distgit_key = 'log-file-metric-exporter'
        operand_meta.get_component_name.return_value = 'log-file-metric-exporter-container'
        operand_meta.branch_el_target.return_value = 9
        operand_meta.get_latest_konflux_build = AsyncMock(return_value=public_build)
        metadata.runtime.late_resolve_image = MagicMock(return_value=operand_meta)

        embargoed_info = self._image_info(
            'log-file-metric-exporter-container',
            '6.6.1',
            '202607160122.p3.g3c1201f.assembly.test.el9',
            'embargoed',
        )
        public_info = self._image_info(
            'log-file-metric-exporter-container',
            '6.6.1',
            '202607160200.p2.g3c1201f.assembly.test.el9',
            'public-sub',
        )

        async def fake_oc_image_info(pullspec, registry_config=None):
            if pullspec == public_build.image_pullspec:
                return public_info
            return embargoed_info

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            side_effect=fake_oc_image_info,
        ):
            new_content, found_images = await self.rebaser._replace_image_references(
                'registry.redhat.io', content, Engine.KONFLUX, metadata
            )

        metadata.runtime.late_resolve_image.assert_called_once_with(
            'log-file-metric-exporter', add=False, required=False
        )
        operand_meta.get_latest_konflux_build.assert_awaited_once_with(
            default=None, el_target=9, embargoed=False, exclude_large_columns=True
        )
        self.assertIn(
            'registry.redhat.io/openshift4/log-file-metric-exporter-rhel9@sha256:public-sub-list', new_content
        )
        self.assertEqual(
            found_images['log-file-metric-exporter-rhel9'][2],
            public_build.nvr,
        )

    async def test_embargoed_operand_with_unknown_component_raises(self):
        """When an embargoed operand's component can't be mapped to any ART ImageMetadata, fail clearly."""
        content = 'image: registry.redhat.io/openshift4/my-embargoed-operand-rhel9:v1.0'
        metadata = self._make_metadata(image_metas=[])  # no metadata registered for this component

        embargoed_info = self._image_info(
            'my-embargoed-operand-container', 'v1.0', '202607151200.p3.g172d0b2.assembly.stream.el9', 'embargoed'
        )

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            new_callable=AsyncMock,
            return_value=embargoed_info,
        ):
            with self.assertRaises(KonfluxOlmBundleRebaseError):
                await self.rebaser._replace_image_references('registry.redhat.io', content, Engine.KONFLUX, metadata)

    async def test_operand_with_ambiguous_embargo_status_raises(self):
        """When an operand's NVR matches more than one visibility suffix (ambiguous embargo
        status), the rebase fails clearly rather than guessing which suffix applies."""
        content = 'image: registry.redhat.io/openshift4/my-ambiguous-operand-rhel9:v1.0'
        metadata = self._make_metadata()

        # Synthetic/defensive case: version and release combine into an NVR carrying two
        # visibility suffixes (real ART NVRs never carry more than one).
        ambiguous_info = self._image_info(
            'my-ambiguous-operand-container',
            '1.0.202607151200.p2.g172d0b2.assembly.stream.el9',
            '1.p3',
            'ambiguous',
        )

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            new_callable=AsyncMock,
            return_value=ambiguous_info,
        ):
            with self.assertRaises(KonfluxOlmBundleRebaseError):
                await self.rebaser._replace_image_references('registry.redhat.io', content, Engine.KONFLUX, metadata)

    async def test_brew_engine_does_not_check_embargo(self):
        """Embargo substitution is only applied for Konflux-engine operands; Brew engine is unaffected."""
        content = 'image: registry.redhat.io/openshift4/my-embargoed-operand-rhel9:v1.0'
        metadata = self._make_metadata()
        embargoed_info = self._image_info(
            'my-embargoed-operand-container', 'v1.0', '202607151200.p3.g172d0b2.assembly.stream.el9', 'embargoed'
        )

        with patch(
            'doozerlib.backend.konflux_olm_bundler.util.oc_image_info_for_arch_async',
            new_callable=AsyncMock,
            return_value=embargoed_info,
        ) as mock_oc_info:
            new_content, found_images = await self.rebaser._replace_image_references(
                'registry.redhat.io', content, Engine.BREW, metadata
            )

        # No substitution attempted; the (embargoed) build's own digest is used as-is.
        mock_oc_info.assert_awaited_once()
        self.assertIn('registry.redhat.io/openshift4/my-embargoed-operand-rhel9@sha256:embargoed-list', new_content)
        # The lazy component-metadata map should never be built for the Brew engine.
        metadata.runtime.image_metas.assert_not_called()
