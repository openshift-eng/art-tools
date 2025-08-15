import base64
import json
import random
from datetime import datetime, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import yaml
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from artcommonlib.model import Model
from doozerlib.cli.scan_sources_konflux import ConfigScanSources
from doozerlib.constants import KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL
from doozerlib.image import ImageMetadata
from doozerlib.metadata import RebuildHint, RebuildHintCode
from doozerlib.runtime import Runtime


class TestScanSourcesKonflux(IsolatedAsyncioTestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.konflux_db = MagicMock()
        self.session = MagicMock(spec=aiohttp.ClientSession)
        self.ci_kubeconfig = "/tmp/test_kubeconfig"

        # Mock environment variables
        with patch.dict('os.environ', {'GITHUB_TOKEN': 'test_token'}):
            self.scanner = ConfigScanSources(
                runtime=self.runtime,
                ci_kubeconfig=self.ci_kubeconfig,
                session=self.session,
                as_yaml=False,
                rebase_priv=False,
                dry_run=False,
            )

        # Mock image metadata
        self.image_meta = MagicMock(spec=ImageMetadata)
        self.image_meta.distgit_key = "test-image"

        # Mock build record
        self.build_record = MagicMock(spec=KonfluxBuildRecord)
        self.build_record.image_pullspec = "quay.io/test/image@sha256:abc123"

        self.scanner.latest_image_build_records_map = {"test-image": self.build_record}

        # Mock changing_image_names to allow scanning
        self.scanner.changing_image_names = set()


class TestScanTaskBundleChanges(TestScanSourcesKonflux):
    """Test the scan_task_bundle_changes method."""

    def setUp(self):
        super().setUp()

        # Sample SLSA attestation with task bundles
        self.sample_attestation = json.dumps(
            {
                "payload": base64.b64encode(
                    json.dumps(
                        {
                            "predicate": {
                                "materials": [
                                    {
                                        "uri": "quay.io/konflux-ci/tekton-catalog/task-git-clone",
                                        "digest": {"sha256": "abc123def456"},
                                    },
                                    {
                                        "uri": "quay.io/konflux-ci/tekton-catalog/task-buildah",
                                        "digest": {"sha256": "def456ghi789"},
                                    },
                                    {"uri": "quay.io/other-registry/some-other-task", "digest": {"sha256": "xyz999"}},
                                ]
                            }
                        }
                    ).encode()
                ).decode()
            }
        )

        # Sample current task bundles from GitHub
        self.current_task_bundles = {
            "task-git-clone": "new123def456",  # Different SHA - newer version
            "task-buildah": "def456ghi789",  # Same SHA - up to date
        }

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    async def test_scan_task_bundle_changes_no_attestation(self, mock_get_attestation):
        """Test handling when SLSA attestation cannot be retrieved."""
        mock_get_attestation.side_effect = ChildProcessError("Failed to download")

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when attestation fails
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    async def test_scan_task_bundle_changes_invalid_attestation(self, mock_get_attestation):
        """Test handling when SLSA attestation is malformed."""
        mock_get_attestation.return_value = "invalid json"

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when attestation is invalid
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    async def test_scan_task_bundle_changes_no_task_bundles(self, mock_get_attestation):
        """Test handling when no tekton-catalog task bundles are found."""
        attestation_without_task_bundles = json.dumps(
            {
                "payload": base64.b64encode(
                    json.dumps(
                        {
                            "predicate": {
                                "materials": [
                                    {"uri": "quay.io/other-registry/some-image", "digest": {"sha256": "xyz999"}}
                                ]
                            }
                        }
                    ).encode()
                ).decode()
            }
        )
        mock_get_attestation.return_value = attestation_without_task_bundles

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when no task bundles found
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    async def test_scan_task_bundle_changes_github_fetch_fails(self, mock_get_current, mock_get_attestation):
        """Test handling when GitHub task bundle fetch fails."""
        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = {}  # Empty dict indicates failure

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when GitHub fetch fails
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    async def test_scan_task_bundle_changes_not_old_enough(self, mock_get_age, mock_get_current, mock_get_attestation):
        """Test that task bundles less than 10 days old don't trigger rebuilds."""
        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = self.current_task_bundles
        mock_get_age.return_value = 5  # Less than 10 days

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when task bundle is not old enough
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    @patch('random.randint')
    async def test_scan_task_bundle_changes_staggered_rebuild_triggers(
        self, mock_randint, mock_add_change, mock_get_age, mock_get_current, mock_get_attestation
    ):
        """Test that staggered rebuild logic triggers rebuild when random condition is met."""
        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = self.current_task_bundles
        mock_get_age.return_value = 35  # 35 days old
        mock_randint.return_value = 1  # This should trigger rebuild

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should add change when staggered rebuild condition is met
        mock_add_change.assert_called_once()

        # Verify the rebuild hint
        call_args = mock_add_change.call_args[0]
        rebuild_hint = call_args[1]
        self.assertEqual(rebuild_hint.code, RebuildHintCode.TASK_BUNDLE_OUTDATED)
        self.assertIn("task-git-clone", rebuild_hint.reason)
        self.assertIn("35 days old", rebuild_hint.reason)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    @patch('random.randint')
    async def test_scan_task_bundle_changes_staggered_rebuild_skips(
        self, mock_randint, mock_add_change, mock_get_age, mock_get_current, mock_get_attestation
    ):
        """Test that staggered rebuild logic skips rebuild when random condition is not met."""
        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = self.current_task_bundles
        mock_get_age.return_value = 15  # 15 days old
        mock_randint.return_value = 2  # With denominator of 15 (max(30-15, 1)), this should not trigger rebuild

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add change when staggered rebuild condition is not met
        mock_add_change.assert_not_called()

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    async def test_scan_task_bundle_changes_same_sha_no_rebuild(
        self, mock_get_age, mock_get_current, mock_get_attestation
    ):
        """Test that task bundles with same SHA don't trigger rebuilds."""
        # Modify current bundles to have same SHA as used SHA
        current_bundles_same = {
            "task-git-clone": "abc123def456",  # Same SHA as in attestation
            "task-buildah": "def456ghi789",  # Same SHA as in attestation
        }

        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = current_bundles_same

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not call get_age when SHAs match
        mock_get_age.assert_not_called()
        self.assertEqual(len(self.scanner.changing_image_names), 0)


class TestGetCurrentTaskBundleShas(TestScanSourcesKonflux):
    """Test the get_current_task_bundle_shas method."""

    def setUp(self):
        super().setUp()

        # Sample YAML content with task references
        self.sample_yaml = {
            "spec": {
                "tasks": [
                    {
                        "name": "git-clone",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "git-clone"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/git-clone@sha256:abc123def456",
                                },
                            ],
                        },
                    },
                    {
                        "name": "buildah",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "buildah"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/buildah@sha256:def456ghi789",
                                },
                            ],
                        },
                    },
                    {
                        "name": "other-task",
                        "taskRef": {
                            "resolver": "git",  # Different resolver
                            "params": [{"name": "url", "value": "https://github.com/example/tasks"}],
                        },
                    },
                ]
            }
        }

    async def test_get_current_task_bundle_shas_success(self):
        """Test successful fetching and parsing of task bundle SHAs."""
        # Mock successful HTTP response
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_response.text = AsyncMock(return_value=yaml.dump(self.sample_yaml))

        self.session.get.return_value.__aenter__.return_value = mock_response

        result = await self.scanner.get_current_task_bundle_shas()

        expected = {"git-clone@sha256": "abc123def456", "buildah@sha256": "def456ghi789"}
        self.assertEqual(result, expected)

        # Verify correct URL and headers were used
        self.session.get.assert_called_once_with(
            KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL, headers={'Authorization': 'Bearer test_token'}
        )

    async def test_get_current_task_bundle_shas_http_error(self):
        """Test handling of HTTP errors when fetching from GitHub."""
        # Mock HTTP error
        mock_response = AsyncMock()
        mock_response.raise_for_status.side_effect = aiohttp.ClientResponseError(
            request_info=MagicMock(), history=[], status=404
        )

        self.session.get.return_value.__aenter__.return_value = mock_response

        result = await self.scanner.get_current_task_bundle_shas()

        self.assertEqual(result, {})

    async def test_get_current_task_bundle_shas_yaml_parse_error(self):
        """Test handling of YAML parsing errors."""
        # Mock successful HTTP response with invalid YAML
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_response.text = AsyncMock(return_value="invalid: yaml: content: [unclosed")

        self.session.get.return_value.__aenter__.return_value = mock_response

        result = await self.scanner.get_current_task_bundle_shas()

        self.assertEqual(result, {})

    async def test_get_current_task_bundle_shas_no_task_refs(self):
        """Test handling when YAML contains no task references."""
        yaml_without_tasks = {"spec": {"resources": []}}

        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_response.text = AsyncMock(return_value=yaml.dump(yaml_without_tasks))

        self.session.get.return_value.__aenter__.return_value = mock_response

        result = await self.scanner.get_current_task_bundle_shas()

        self.assertEqual(result, {})

    async def test_get_current_task_bundle_shas_nested_structure(self):
        """Test parsing task references in deeply nested YAML structures."""
        nested_yaml = {
            "metadata": {"name": "pipeline"},
            "spec": {
                "tasks": [
                    {
                        "name": "nested-task",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "nested-task"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/nested-task@sha256:nested123",
                                },
                            ],
                        },
                    }
                ],
                "finally": [
                    {
                        "name": "cleanup",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "cleanup"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/cleanup@sha256:cleanup456",
                                },
                            ],
                        },
                    }
                ],
            },
        }

        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_response.text = AsyncMock(return_value=yaml.dump(nested_yaml))

        self.session.get.return_value.__aenter__.return_value = mock_response

        result = await self.scanner.get_current_task_bundle_shas()

        expected = {"nested-task@sha256": "nested123", "cleanup@sha256": "cleanup456"}
        self.assertEqual(result, expected)


class TestGetTaskBundleAgeDays(TestScanSourcesKonflux):
    """Test the get_task_bundle_age_days method."""

    @patch('doozerlib.cli.scan_sources_konflux.cmd_gather_async')
    async def test_get_task_bundle_age_days_success(self, mock_cmd_gather):
        """Test successful calculation of task bundle age."""
        # Mock oc image info output
        created_time = "2024-01-01T10:00:00Z"
        mock_image_info = {"config": {"created": created_time}}

        mock_cmd_gather.return_value = (0, json.dumps(mock_image_info), "")

        # Mock current time to be 35 days after creation
        with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
            mock_now = datetime(2024, 2, 5, 10, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            mock_datetime.fromisoformat = datetime.fromisoformat

            result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertEqual(result, 35)

        # Verify correct command was called
        expected_pullspec = "quay.io/konflux-ci/tekton-catalog/test-task@sha256:abc123"
        expected_cmd = f"oc image info {expected_pullspec} -o json"
        mock_cmd_gather.assert_called_once_with(expected_cmd)

    @patch('doozerlib.cli.scan_sources_konflux.cmd_gather_async')
    async def test_get_task_bundle_age_days_missing_created_field(self, mock_cmd_gather):
        """Test handling when created field is missing from image info."""
        mock_image_info = {"config": {}}  # Missing 'created' field

        mock_cmd_gather.return_value = (0, json.dumps(mock_image_info), "")

        result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertIsNone(result)

    @patch('doozerlib.cli.scan_sources_konflux.cmd_gather_async')
    async def test_get_task_bundle_age_days_command_failure(self, mock_cmd_gather):
        """Test handling when oc image info command fails."""
        mock_cmd_gather.side_effect = Exception("Command failed")

        result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertIsNone(result)

    @patch('doozerlib.cli.scan_sources_konflux.cmd_gather_async')
    async def test_get_task_bundle_age_days_invalid_json(self, mock_cmd_gather):
        """Test handling when oc command returns invalid JSON."""
        mock_cmd_gather.return_value = (0, "invalid json", "")

        result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertIsNone(result)

    @patch('doozerlib.cli.scan_sources_konflux.cmd_gather_async')
    async def test_get_task_bundle_age_days_timezone_handling(self, mock_cmd_gather):
        """Test proper timezone handling in age calculation."""
        # Test with different timezone formats
        test_cases = [
            "2024-01-01T10:00:00Z",  # UTC with Z
            "2024-01-01T10:00:00+00:00",  # UTC with offset
        ]

        for created_time in test_cases:
            with self.subTest(created_time=created_time):
                mock_image_info = {"config": {"created": created_time}}
                mock_cmd_gather.return_value = (0, json.dumps(mock_image_info), "")

                with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
                    mock_now = datetime(2024, 1, 11, 10, 0, 0, tzinfo=timezone.utc)
                    mock_datetime.now.return_value = mock_now
                    mock_datetime.fromisoformat = datetime.fromisoformat

                    result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

                self.assertEqual(result, 10)


class TestTaskBundleIntegration(TestScanSourcesKonflux):
    """Integration tests for the complete task bundle scanning workflow."""

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    @patch('random.randint')
    async def test_full_workflow_rebuild_triggered(
        self, mock_randint, mock_add_change, mock_get_age, mock_get_current, mock_get_attestation
    ):
        """Test the complete workflow when a rebuild should be triggered."""
        # Setup test data
        attestation = json.dumps(
            {
                "payload": base64.b64encode(
                    json.dumps(
                        {
                            "predicate": {
                                "materials": [
                                    {
                                        "uri": "quay.io/konflux-ci/tekton-catalog/git-clone",
                                        "digest": {"sha256": "old123"},
                                    }
                                ]
                            }
                        }
                    ).encode()
                ).decode()
            }
        )

        current_bundles = {"git-clone": "new456"}  # Different SHA

        mock_get_attestation.return_value = attestation
        mock_get_current.return_value = current_bundles
        mock_get_age.return_value = 35  # Old enough to trigger rebuild
        mock_randint.return_value = 1  # Will trigger rebuild

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Verify all methods were called in correct order
        mock_get_attestation.assert_called_once_with(
            pullspec=self.build_record.image_pullspec, registry_auth_file=self.scanner.registry_auth_file
        )
        mock_get_current.assert_called_once()
        mock_get_age.assert_called_once_with("git-clone", "old123")

        # Verify staggered rebuild logic
        mock_randint.assert_called_once_with(1, 1)  # max(30 - 35, 1) = 1

        # Verify rebuild hint was added
        mock_add_change.assert_called_once()
        rebuild_hint = mock_add_change.call_args[0][1]
        self.assertEqual(rebuild_hint.code, RebuildHintCode.TASK_BUNDLE_OUTDATED)

    @patch('artcommonlib.util.get_konflux_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    async def test_full_workflow_no_rebuild_needed(
        self, mock_add_change, mock_get_age, mock_get_current, mock_get_attestation
    ):
        """Test the complete workflow when no rebuild is needed."""
        # Setup test data with matching SHAs
        attestation = json.dumps(
            {
                "payload": base64.b64encode(
                    json.dumps(
                        {
                            "predicate": {
                                "materials": [
                                    {
                                        "uri": "quay.io/konflux-ci/tekton-catalog/git-clone",
                                        "digest": {"sha256": "same123"},
                                    }
                                ]
                            }
                        }
                    ).encode()
                ).decode()
            }
        )

        current_bundles = {"git-clone": "same123"}  # Same SHA

        mock_get_attestation.return_value = attestation
        mock_get_current.return_value = current_bundles

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Verify early exit when SHAs match
        mock_get_attestation.assert_called_once()
        mock_get_current.assert_called_once()
        mock_get_age.assert_not_called()  # Should not check age when SHAs match
        mock_add_change.assert_not_called()

    async def test_skip_check_if_changing_decorator(self):
        """Test that the skip_check_if_changing decorator works correctly."""
        # Add image to changing set
        self.scanner.changing_image_names.add("test-image")

        with patch('artcommonlib.util.get_konflux_slsa_attestation') as mock_get_attestation:
            await self.scanner.scan_task_bundle_changes(self.image_meta)

            # Should not call get_attestation when image is already changing
            mock_get_attestation.assert_not_called()
