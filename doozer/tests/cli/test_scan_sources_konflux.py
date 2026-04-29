import base64
import json
from datetime import datetime, timezone
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import yaml
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from artcommonlib.model import Missing
from doozerlib.cli.scan_sources_konflux import ConfigScanSources
from doozerlib.image import ImageMetadata
from doozerlib.metadata import RebuildHint, RebuildHintCode
from doozerlib.runtime import Runtime


class TestScanSourcesKonflux(IsolatedAsyncioTestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.konflux_db = MagicMock()
        self.runtime.group = 'openshift-4.20'  # Default to OCP group for tests
        self.runtime.load_disabled = False
        self.runtime.registry_config = None
        self.runtime.image_metas.return_value = []
        self.runtime.rpm_metas.return_value = []
        self.runtime.image_map = {}
        self.session = MagicMock(spec=aiohttp.ClientSession)
        self.ci_kubeconfig = "/tmp/test_kubeconfig"

        # Mock get_github_client_for_org (returns client directly; no Github class or GITHUB_TOKEN)
        # Patch must persist for test duration since get_current_task_bundle_shas calls it at runtime
        self._github_patch = patch('doozerlib.cli.scan_sources_konflux.get_github_client_for_org')
        mock_get_github_client = self._github_patch.start()
        self.mock_github_client = MagicMock()
        mock_get_github_client.return_value = self.mock_github_client
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
        # Mock config with for_release attribute
        self.image_meta.config = MagicMock()
        self.image_meta.config.for_release = True

        # Mock build record
        self.build_record = MagicMock(spec=KonfluxBuildRecord)
        self.build_record.image_pullspec = "quay.io/test/image@sha256:abc123"
        self.build_record.name = "test-image"

        self.scanner.latest_image_build_records_map = {"test-image": self.build_record}

        # Mock changing_image_names to allow scanning
        self.scanner.changing_image_names = set()

    def tearDown(self):
        if hasattr(self, '_github_patch'):
            self._github_patch.stop()
        super().tearDown()


class TestMinimalCrashIsolation(TestScanSourcesKonflux):
    async def test_scan_images_skips_broken_canonical_image_and_continues(self):
        broken_image = MagicMock(spec=ImageMetadata)
        broken_image.distgit_key = "broken-image"
        broken_image.enabled = True
        broken_image.ensure_canonical_builders_resolved.side_effect = RuntimeError("bad canonical")
        broken_image.get_latest_build = AsyncMock()

        good_image = MagicMock(spec=ImageMetadata)
        good_image.distgit_key = "good-image"
        good_image.enabled = True
        good_image.ensure_canonical_builders_resolved.return_value = None
        good_image.get_latest_build = AsyncMock(return_value=self.build_record)

        self.runtime.image_map = {
            "broken-image": broken_image,
            "good-image": good_image,
        }
        self.scanner.latest_image_build_records_map = {}

        with patch.object(self.scanner, 'scan_image', AsyncMock()) as mock_scan_image:
            await self.scanner.scan_images(["broken-image", "good-image"])

        self.assertEqual(
            self.scanner.issues,
            [
                {
                    'name': 'broken-image',
                    'issue': 'Failed resolving canonical builders before latest build lookup: bad canonical',
                }
            ],
        )
        self.assertIn('broken-image', self.scanner.skipped_image_names)
        broken_image.get_latest_build.assert_not_called()
        self.assertEqual(self.scanner.latest_image_build_records_map, {"good-image": self.build_record})
        mock_scan_image.assert_awaited_once_with(good_image)

    async def test_scan_image_records_issue_instead_of_raising(self):
        self.image_meta.config.konflux = Missing

        with (
            patch.object(self.scanner, 'scan_for_upstream_changes', AsyncMock(side_effect=RuntimeError('boom'))),
            patch.object(self.scanner, 'scan_dependency_changes', AsyncMock()),
            patch.object(self.scanner, 'scan_builders_changes', AsyncMock()),
            patch.object(self.scanner, 'scan_arch_changes', AsyncMock()),
            patch.object(self.scanner, 'scan_network_mode_changes', AsyncMock()),
            patch.object(self.scanner, 'scan_for_config_changes', AsyncMock()),
            patch.object(self.scanner, 'scan_rpm_changes', AsyncMock()),
            patch.object(self.scanner, 'scan_extra_packages', AsyncMock()),
            patch.object(self.scanner, 'scan_task_bundle_changes', AsyncMock()),
        ):
            await self.scanner.scan_image(self.image_meta)

        self.assertEqual(
            self.scanner.issues,
            [{'name': 'test-image', 'issue': 'Failed scanning image during upstream commit checks: boom'}],
        )

    def test_skip_image_removes_broken_image_from_change_tracking(self):
        broken_image = MagicMock(spec=ImageMetadata)
        broken_image.distgit_key = "broken-image"
        broken_image.qualified_key = "images:broken-image"
        broken_image.get_descendants.return_value = set()

        good_image = MagicMock(spec=ImageMetadata)
        good_image.distgit_key = "good-image"
        good_image.qualified_key = "images:good-image"
        good_image.get_descendants.return_value = [broken_image]

        broken_key = f"{broken_image.qualified_key}+True"
        self.scanner.changing_image_names.add("broken-image")
        self.scanner.assessment_reason[broken_key] = "broken"
        self.scanner.assessment_code[broken_key] = RebuildHintCode.CONFIG_CHANGE

        self.scanner._skip_image(broken_image, "broken")
        self.scanner.add_image_meta_change(good_image, RebuildHint(RebuildHintCode.ANCESTOR_CHANGING, "good"))

        self.assertEqual(self.scanner.changing_image_names, {"good-image"})
        self.assertNotIn(broken_key, self.scanner.assessment_reason)
        self.assertNotIn(broken_key, self.scanner.assessment_code)

    @patch('doozerlib.cli.scan_sources_konflux.Dir')
    @patch('doozerlib.cli.scan_sources_konflux.exectools.parallel_exec')
    def test_rebase_into_priv_records_issue_and_continues(self, mock_parallel_exec, mock_dir):
        broken_meta = MagicMock()
        broken_meta.meta_type = 'image'
        broken_meta.enabled = True
        broken_meta.name = 'broken-image'
        broken_meta.distgit_key = 'broken-image'
        broken_meta.config.content = MagicMock()
        broken_meta.config.content.source.git.url = 'https://example.com/org/broken.git'
        broken_meta.config.content.source.git.branch.target = 'main'

        good_meta = MagicMock()
        good_meta.meta_type = 'image'
        good_meta.enabled = True
        good_meta.name = 'good-image'
        good_meta.distgit_key = 'good-image'
        good_meta.config.content = MagicMock()
        good_meta.config.content.source.git.url = 'https://example.com/org/good.git'
        good_meta.config.content.source.git.branch.target = 'main'

        mock_parallel_exec.return_value.get.return_value = [
            (broken_meta, ('https://example.com/public-org/broken.git', 'main', True)),
            (good_meta, ('https://example.com/public-org/good.git', 'main', True)),
        ]
        mock_dir.return_value.__enter__.return_value = None
        mock_dir.return_value.__exit__.return_value = None

        self.runtime.source_resolver = MagicMock()
        self.runtime.source_resolver.resolve_source.side_effect = [
            MagicMock(source_path='/tmp/broken'),
            MagicMock(source_path='/tmp/good'),
        ]

        with (
            patch.object(self.scanner, '_is_image_enabled', return_value=True),
            patch.object(self.scanner, '_do_shas_match', return_value=False),
            patch.object(self.scanner, '_is_pub_ancestor_of_priv', side_effect=[RuntimeError('bad repo'), False]),
            patch.object(self.scanner, '_try_reconciliation') as mock_try_reconciliation,
        ):
            self.scanner.rebase_into_priv()

        self.assertEqual(
            self.scanner.issues,
            [{'name': 'broken-image', 'issue': 'Failed rebasing into -priv: bad repo'}],
        )
        mock_try_reconciliation.assert_called_once_with(
            good_meta, 'good', 'main', 'main', priv_url='https://example.com/org/good.git'
        )


class TestScanTaskBundleChanges(TestScanSourcesKonflux):
    """Test the scan_task_bundle_changes method."""

    def setUp(self):
        super().setUp()

        # Sample SLSA attestation with task bundles (already parsed)
        self.sample_attestation = {
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

        # Sample current task bundles from GitHub
        self.current_task_bundles = {
            "task-git-clone": "new123def456",  # Different SHA - newer version
            "task-buildah": "def456ghi789",  # Same SHA - up to date
        }

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    async def test_scan_task_bundle_changes_no_attestation(self, mock_get_attestation):
        """Test handling when SLSA attestation cannot be retrieved."""
        mock_get_attestation.return_value = None

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when attestation fails
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    async def test_scan_task_bundle_changes_invalid_attestation(self, mock_get_attestation):
        """Test handling when SLSA attestation is malformed."""
        mock_get_attestation.return_value = "invalid json"

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when attestation is invalid
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
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

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    async def test_scan_task_bundle_changes_github_fetch_fails(self, mock_get_current, mock_get_attestation):
        """Test handling when GitHub task bundle fetch fails."""
        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = {}  # Empty dict indicates failure

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should not add any changes when GitHub fetch fails
        self.assertEqual(len(self.scanner.changing_image_names), 0)

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
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

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    @patch('random.randint')
    async def test_scan_task_bundle_changes_staggered_rebuild_triggers(
        self, mock_randint, mock_add_change, mock_get_age, mock_get_attestation
    ):
        """Test that staggered rebuild logic triggers rebuild when random condition is met."""
        mock_get_attestation.return_value = self.sample_attestation
        mock_get_age.return_value = 35  # 35 days old
        mock_randint.return_value = 1  # This should trigger rebuild

        self.scanner.current_task_bundles = self.current_task_bundles

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should add change when staggered rebuild condition is met
        mock_add_change.assert_called_once()

        # Verify the rebuild hint
        call_args = mock_add_change.call_args[0]
        rebuild_hint = call_args[1]
        self.assertEqual(rebuild_hint.code, RebuildHintCode.TASK_BUNDLE_OUTDATED)
        self.assertIn("task-git-clone", rebuild_hint.reason)
        self.assertIn("35 days old", rebuild_hint.reason)

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
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

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
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

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    async def test_scan_task_bundle_changes_for_release_none(self, mock_get_current, mock_get_attestation):
        """Test that task bundle scanning proceeds when for_release is None."""
        # Set for_release to None
        self.image_meta.config.for_release = None

        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = {}

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should call fetch_slsa_attestation when for_release is None (not skipped)
        mock_get_attestation.assert_called_once_with(
            self.build_record.image_pullspec, self.build_record.name, self.scanner.runtime.registry_config
        )

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    async def test_scan_task_bundle_changes_for_release_missing(self, mock_get_current, mock_get_attestation):
        """Test that task bundle scanning proceeds when for_release is Missing."""
        # Set for_release to Missing
        self.image_meta.config.for_release = Missing

        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = {}

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should call fetch_slsa_attestation when for_release is Missing (not skipped)
        mock_get_attestation.assert_called_once_with(
            self.build_record.image_pullspec, self.build_record.name, self.scanner.runtime.registry_config
        )

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_current_task_bundle_shas')
    async def test_scan_task_bundle_changes_for_release_true(self, mock_get_current, mock_get_attestation):
        """Test that task bundle scanning proceeds when for_release is True."""
        # for_release is already set to True in setUp, but let's be explicit
        self.image_meta.config.for_release = True

        mock_get_attestation.return_value = self.sample_attestation
        mock_get_current.return_value = {}

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should call fetch_slsa_attestation when for_release is True (not skipped)
        mock_get_attestation.assert_called_once_with(
            self.build_record.image_pullspec, self.build_record.name, self.scanner.runtime.registry_config
        )

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    async def test_scan_task_bundle_changes_for_release_false(self, mock_get_attestation):
        """Test that task bundle scanning is skipped when for_release is False."""
        # Set for_release to False
        self.image_meta.config.for_release = False

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Should NOT call fetch_slsa_attestation when for_release is False (skipped)
        mock_get_attestation.assert_not_called()


class TestGetCurrentTaskBundleShas(TestScanSourcesKonflux):
    """Test the get_current_task_bundle_shas method."""

    def setUp(self):
        super().setUp()

        # Sample YAML content with task references
        self.sample_yaml = {
            "spec": {
                "tasks": [
                    {
                        "name": "init",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "init"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/task-init:0.2@sha256:abc123def456",
                                },
                            ],
                        },
                    },
                    {
                        "name": "git-clone-oci-ta",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "git-clone-oci-ta"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/task-git-clone-oci-ta:0.1@sha256:def456ghi789",
                                },
                            ],
                        },
                    },
                    {
                        "name": "buildah-remote-oci-ta",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "buildah-remote-oci-ta"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/task-buildah-remote-oci-ta:0.7@sha256:ghi789jkl012",
                                },
                            ],
                        },
                    },
                    {
                        "name": "non-task-bundle",
                        "taskRef": {
                            "resolver": "bundles",
                            "params": [
                                {"name": "name", "value": "non-task-bundle"},
                                {
                                    "name": "bundle",
                                    "value": "quay.io/konflux-ci/tekton-catalog/git-clone@sha256:shouldnotbeextracted",
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
        yaml_content = yaml.dump(self.sample_yaml)

        mock_content = MagicMock()
        mock_content.decoded_content = yaml_content.encode('utf-8')
        self.mock_github_client.get_repo.return_value.get_contents.return_value = mock_content

        result = await self.scanner.get_current_task_bundle_shas()

        expected = {
            "task-init": "abc123def456",
            "task-git-clone-oci-ta": "def456ghi789",
            "task-buildah-remote-oci-ta": "ghi789jkl012",
        }
        self.assertEqual(result, expected)

        self.mock_github_client.get_repo.assert_called_with("openshift-priv/art-konflux-template")
        self.mock_github_client.get_repo.return_value.get_contents.assert_called_with(
            ".tekton/art-konflux-template-push.yaml", ref="main"
        )

    async def test_get_current_task_bundle_shas_http_error(self):
        """Test that GitHub API errors propagate so @retry can handle them."""
        from github import GithubException

        self.mock_github_client.get_repo.return_value.get_contents.side_effect = GithubException(
            404, {"message": "Not Found"}, None
        )

        with self.assertRaises(GithubException):
            await self.scanner.get_current_task_bundle_shas()

    async def test_get_current_task_bundle_shas_yaml_parse_error(self):
        """Test handling of YAML parsing errors."""
        invalid_yaml = "invalid: yaml: content: [unclosed"

        mock_content = MagicMock()
        mock_content.decoded_content = invalid_yaml.encode('utf-8')
        self.mock_github_client.get_repo.return_value.get_contents.return_value = mock_content

        result = await self.scanner.get_current_task_bundle_shas()

        self.assertEqual(result, {})

    async def test_get_current_task_bundle_shas_no_task_refs(self):
        """Test handling when YAML contains no task references."""
        yaml_without_tasks = {"spec": {"resources": []}}
        yaml_content = yaml.dump(yaml_without_tasks)

        mock_content = MagicMock()
        mock_content.decoded_content = yaml_content.encode('utf-8')
        self.mock_github_client.get_repo.return_value.get_contents.return_value = mock_content

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
                                    "value": "quay.io/konflux-ci/tekton-catalog/task-nested-task:0.1@sha256:nested123",
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
                                    "value": "quay.io/konflux-ci/tekton-catalog/task-cleanup:0.2@sha256:cleanup456",
                                },
                            ],
                        },
                    }
                ],
            },
        }

        yaml_content = yaml.dump(nested_yaml)

        mock_content = MagicMock()
        mock_content.decoded_content = yaml_content.encode('utf-8')
        self.mock_github_client.get_repo.return_value.get_contents.return_value = mock_content

        result = await self.scanner.get_current_task_bundle_shas()

        expected = {"task-nested-task": "nested123", "task-cleanup": "cleanup456"}
        self.assertEqual(result, expected)


class TestGetTaskBundleAgeDays(TestScanSourcesKonflux):
    """Test the get_task_bundle_age_days method."""

    @patch('doozerlib.cli.scan_sources_konflux.oc_image_info__cached_async')
    async def test_get_task_bundle_age_days_success(self, mock_oc_image_info):
        """Test successful calculation of task bundle age."""
        # Mock oc image info output
        created_time = "2024-01-01T10:00:00Z"
        mock_image_info = {"config": {"created": created_time}}

        mock_oc_image_info.return_value = json.dumps(mock_image_info)

        # Mock current time to be 35 days after creation
        with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
            mock_now = datetime(2024, 2, 5, 10, 0, 0, tzinfo=timezone.utc)
            mock_datetime.now.return_value = mock_now
            mock_datetime.fromisoformat = datetime.fromisoformat

            result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertEqual(result, 35)

        # Verify correct pullspec was queried with registry_config
        expected_pullspec = "quay.io/konflux-ci/tekton-catalog/test-task@sha256:abc123"
        mock_oc_image_info.assert_called_once_with(expected_pullspec, registry_config=None)

    @patch('doozerlib.cli.scan_sources_konflux.oc_image_info__cached_async')
    async def test_get_task_bundle_age_days_missing_created_field(self, mock_oc_image_info):
        """Test handling when created field is missing from image info."""
        mock_image_info = {"config": {}}  # Missing 'created' field

        mock_oc_image_info.return_value = json.dumps(mock_image_info)

        result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertIsNone(result)

    @patch('doozerlib.cli.scan_sources_konflux.oc_image_info__cached_async')
    async def test_get_task_bundle_age_days_command_failure(self, mock_oc_image_info):
        """Test handling when oc image info command fails."""
        mock_oc_image_info.side_effect = Exception("Command failed")

        result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertIsNone(result)

    @patch('doozerlib.cli.scan_sources_konflux.oc_image_info__cached_async')
    async def test_get_task_bundle_age_days_invalid_json(self, mock_oc_image_info):
        """Test handling when oc command returns invalid JSON."""
        mock_oc_image_info.return_value = "invalid json"

        result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

        self.assertIsNone(result)

    @patch('doozerlib.cli.scan_sources_konflux.oc_image_info__cached_async')
    async def test_get_task_bundle_age_days_timezone_handling(self, mock_oc_image_info):
        """Test proper timezone handling in age calculation."""
        # Test with different timezone formats
        test_cases = [
            "2024-01-01T10:00:00Z",  # UTC with Z
            "2024-01-01T10:00:00+00:00",  # UTC with offset
        ]

        for created_time in test_cases:
            with self.subTest(created_time=created_time):
                mock_image_info = {"config": {"created": created_time}}
                mock_oc_image_info.return_value = json.dumps(mock_image_info)

                with patch('doozerlib.cli.scan_sources_konflux.datetime') as mock_datetime:
                    mock_now = datetime(2024, 1, 11, 10, 0, 0, tzinfo=timezone.utc)
                    mock_datetime.now.return_value = mock_now
                    mock_datetime.fromisoformat = datetime.fromisoformat

                    result = await self.scanner.get_task_bundle_age_days("test-task", "abc123")

                self.assertEqual(result, 10)


class TestTaskBundleIntegration(TestScanSourcesKonflux):
    """Integration tests for the complete task bundle scanning workflow."""

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    @patch('random.randint')
    async def test_full_workflow_rebuild_triggered(
        self, mock_randint, mock_add_change, mock_get_age, mock_get_attestation
    ):
        """Test the complete workflow when a rebuild should be triggered."""
        # Setup test data
        attestation = {
            "predicate": {
                "materials": [
                    {
                        "uri": "quay.io/konflux-ci/tekton-catalog/git-clone",
                        "digest": {"sha256": "old123"},
                    }
                ]
            }
        }

        current_bundles = {"git-clone": "new456"}  # Different SHA

        mock_get_attestation.return_value = attestation
        mock_get_age.return_value = 35  # Old enough to trigger rebuild
        mock_randint.return_value = 1  # Will trigger rebuild
        self.scanner.current_task_bundles = current_bundles

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Verify all methods were called in correct order
        mock_get_attestation.assert_called_once_with(
            self.build_record.image_pullspec, self.build_record.name, self.scanner.runtime.registry_config
        )
        mock_get_age.assert_called_once_with("git-clone", "old123")

        # Verify staggered rebuild logic
        mock_randint.assert_called_once_with(1, 1)  # max(30 - 35, 1) = 1

        # Verify rebuild hint was added
        mock_add_change.assert_called_once()
        rebuild_hint = mock_add_change.call_args[0][1]
        self.assertEqual(rebuild_hint.code, RebuildHintCode.TASK_BUNDLE_OUTDATED)

    @patch('doozerlib.cli.scan_sources_konflux.fetch_slsa_attestation')
    @patch.object(ConfigScanSources, 'get_task_bundle_age_days')
    @patch.object(ConfigScanSources, 'add_image_meta_change')
    async def test_full_workflow_no_rebuild_needed(self, mock_add_change, mock_get_age, mock_get_attestation):
        """Test the complete workflow when no rebuild is needed."""
        # Setup test data with matching SHAs
        current_bundles = {"git-clone": "same123"}  # Same SHA
        self.scanner.current_task_bundles = current_bundles

        await self.scanner.scan_task_bundle_changes(self.image_meta)

        # Verify early exit when SHAs match
        mock_get_attestation.assert_called_once()
        mock_get_age.assert_not_called()  # Should not check age when SHAs match
        mock_add_change.assert_not_called()

    async def test_skip_check_if_changing_decorator(self):
        """Test that the skip_check_if_changing decorator works correctly."""
        # Add image to changing set
        self.scanner.changing_image_names.add("test-image")

        with patch('artcommonlib.util.get_konflux_data') as mock_get_attestation:
            await self.scanner.scan_task_bundle_changes(self.image_meta)

            # Should not call get_attestation when image is already changing
            mock_get_attestation.assert_not_called()

    async def test_network_mode_override_end_to_end_integration(self):
        """Test complete CLI override flow through all components."""
        runtime = MagicMock()
        runtime.network_mode_override = "internal-only"
        runtime.assembly = "test"
        runtime.get_releases_config.return_value = {}

        group_config = MagicMock()
        group_config.konflux.get.return_value = "hermetic"
        runtime.group_config = group_config

        image_config = MagicMock()
        image_config.konflux.get.return_value = "open"

        data_obj = MagicMock()
        data_obj.key = "test"
        data_obj.filename = "test.yml"
        data_obj.data = {"name": "test"}

        from doozerlib.metadata import Metadata

        meta = Metadata("image", runtime, data_obj)
        meta.config = image_config

        result = meta.get_konflux_network_mode()
        self.assertEqual(result, "internal-only")


class TestFindImagesWithDisabledDependencies(TestScanSourcesKonflux):
    """Test the _find_images_with_disabled_dependencies method."""

    def setUp(self):
        super().setUp()
        self.runtime.group = 'oadp-1.4'
        self.runtime.gitdata = MagicMock()

    def _make_gitdata_entry(self, key, mode='enabled', dependents=None):
        entry = MagicMock()
        entry.key = key
        entry.data = {'mode': mode, 'name': f'test/{key}'}
        if dependents:
            entry.data['dependents'] = dependents
        return entry

    def test_no_disabled_images(self):
        """No disabled images means no images should be flagged."""
        self.scanner.changing_image_names = {'oadp-operator', 'oadp-velero'}
        self.runtime.gitdata.load_data.return_value = {
            'oadp-operator': self._make_gitdata_entry('oadp-operator'),
            'oadp-velero': self._make_gitdata_entry('oadp-velero', dependents=['oadp-operator']),
        }

        result = self.scanner._find_images_with_disabled_dependencies()
        self.assertEqual(result, set())

    def test_disabled_image_with_dependent(self):
        """A disabled image listing a changed image as dependent should flag the dependent."""
        self.scanner.changing_image_names = {'oadp-operator', 'oadp-velero-restic-restore-helper'}
        self.runtime.gitdata.load_data.return_value = {
            'oadp-operator': self._make_gitdata_entry('oadp-operator'),
            'oadp-velero': self._make_gitdata_entry('oadp-velero', mode='disabled', dependents=['oadp-operator']),
            'oadp-velero-restic-restore-helper': self._make_gitdata_entry('oadp-velero-restic-restore-helper'),
        }

        result = self.scanner._find_images_with_disabled_dependencies()
        self.assertEqual(result, {'oadp-operator'})

    def test_disabled_image_dependent_not_changing(self):
        """A disabled image whose dependent is not in the changing set should not flag anything."""
        self.scanner.changing_image_names = {'oadp-velero-restic-restore-helper'}
        self.runtime.gitdata.load_data.return_value = {
            'oadp-operator': self._make_gitdata_entry('oadp-operator'),
            'oadp-velero': self._make_gitdata_entry('oadp-velero', mode='disabled', dependents=['oadp-operator']),
            'oadp-velero-restic-restore-helper': self._make_gitdata_entry('oadp-velero-restic-restore-helper'),
        }

        result = self.scanner._find_images_with_disabled_dependencies()
        self.assertEqual(result, set())

    def test_multiple_disabled_images_same_dependent(self):
        """Multiple disabled images listing the same dependent should flag it once."""
        self.scanner.changing_image_names = {'my-operator'}
        self.runtime.gitdata.load_data.return_value = {
            'my-operator': self._make_gitdata_entry('my-operator'),
            'disabled-img-a': self._make_gitdata_entry('disabled-img-a', mode='disabled', dependents=['my-operator']),
            'disabled-img-b': self._make_gitdata_entry('disabled-img-b', mode='disabled', dependents=['my-operator']),
        }

        result = self.scanner._find_images_with_disabled_dependencies()
        self.assertEqual(result, {'my-operator'})

    def test_wip_images_not_treated_as_disabled(self):
        """Images with mode 'wip' should not be treated as disabled for dependency checking."""
        self.scanner.changing_image_names = {'my-operator'}
        self.runtime.gitdata.load_data.return_value = {
            'my-operator': self._make_gitdata_entry('my-operator'),
            'wip-image': self._make_gitdata_entry('wip-image', mode='wip', dependents=['my-operator']),
        }

        result = self.scanner._find_images_with_disabled_dependencies()
        self.assertEqual(result, set())

    def test_disabled_image_without_dependents(self):
        """A disabled image without dependents should not affect anything."""
        self.scanner.changing_image_names = {'my-operator'}
        self.runtime.gitdata.load_data.return_value = {
            'my-operator': self._make_gitdata_entry('my-operator'),
            'disabled-standalone': self._make_gitdata_entry('disabled-standalone', mode='disabled'),
        }

        result = self.scanner._find_images_with_disabled_dependencies()
        self.assertEqual(result, set())
