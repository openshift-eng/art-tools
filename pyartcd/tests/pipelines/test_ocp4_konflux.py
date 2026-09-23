#!/usr/bin/env python3

import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.variants import BuildVariant
from pyartcd.counter_models import BuildFailCounterContext
from pyartcd.pipelines.ocp4_konflux import KonfluxOcpPipeline
from pyartcd.util import update_build_fail_counters


def _make_konflux_ocp_pipeline(
    assembly='stream',
    version='4.21',
    *,
    dry_run=None,
    slack_and_registry=False,
):
    runtime = MagicMock()
    runtime.doozer_working = '/tmp/doozer-working'
    if dry_run is not None:
        runtime.dry_run = dry_run
    runtime.new_slack_client.return_value = MagicMock()
    with patch('pyartcd.pipelines.ocp4_konflux.util.default_release_suffix', return_value='202408190000'):
        pipeline = KonfluxOcpPipeline(
            runtime=runtime,
            assembly=assembly,
            version=version,
            image_build_strategy='all',
            rpm_build_strategy='none',
            build_priority='auto',
            data_path='https://github.com/openshift-eng/ocp-build-data',
        )
    if slack_and_registry:
        pipeline.slack_client = MagicMock()
        pipeline.slack_client.say = AsyncMock()
        pipeline._registry_auth_file = '/tmp/auth'
    return pipeline


class TestUpdateBuildFailCounters(unittest.IsolatedAsyncioTestCase):
    """Tests for the shared Konflux build counter utility used by OCP."""

    async def _update_build_fail_counters(
        self,
        assembly,
        group,
        built_images,
        failed_images,
        record_log,
        reset_counter,
        increment_counter,
    ):
        failed_entries = {
            entry["name"]: entry for entry in record_log.get("image_build_konflux", []) if int(entry["status"])
        }
        await update_build_fail_counters(
            context=BuildFailCounterContext(
                group=group,
                assembly=assembly,
                build_variant=BuildVariant.OCP,
                jenkins_url=os.getenv("BUILD_URL"),
                built_images=built_images,
                failed_images=failed_images,
                failed_entries=failed_entries,
                reset_counter=reset_counter,
                increment_counter=increment_counter,
            )
        )

    @patch.dict(os.environ, {"BUILD_URL": "https://jenkins.example.com/job/1"})
    async def test_infra_failure_still_resets_built_image_counters(self):
        """Infra failures do not prevent successful images from being reset."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()
        built_images = ["driver-toolkit", "base-images"]
        failed_images = ["enterprise-cluster-capacity"]
        record_log = {
            "image_build_konflux": [
                {
                    "name": "enterprise-cluster-capacity",
                    "status": "1",
                    "task_id": "n/a",
                    "task_url": "n/a",
                    "message": "infrastructure failure",
                    "outcome": "",
                    "nvrs": "",
                    "build_pipeline_url": "",
                }
            ]
        }

        await self._update_build_fail_counters(
            "stream",
            "openshift-4.21",
            built_images,
            failed_images,
            record_log,
            mock_reset,
            mock_increment,
        )

        self.assertEqual(mock_reset.call_count, len(built_images) * 3)
        reset_keys = {call.args[0] for call in mock_reset.call_args_list}
        expected_keys = {
            f"count:{counter_type}:konflux:openshift-4.21:{image}"
            for image in built_images
            for counter_type in ("build-failure", "ec-failure", "release-failure")
        }
        self.assertEqual(reset_keys, expected_keys)
        mock_increment.assert_not_called()

    async def test_non_stream_assembly_skips_all_counters(self):
        """Non-stream assemblies do not update counters."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()

        await self._update_build_fail_counters(
            "4.21.3",
            "openshift-4.21",
            ["some-image"],
            [],
            {},
            mock_reset,
            mock_increment,
        )

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()

    async def test_no_built_no_failed_images_noop(self):
        """Empty built and failed lists produce no counter operations."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()

        await self._update_build_fail_counters(
            "stream",
            "openshift-4.21",
            [],
            [],
            {},
            mock_reset,
            mock_increment,
        )

        mock_reset.assert_not_called()
        mock_increment.assert_not_called()

    @patch.dict(os.environ, {"BUILD_URL": "https://jenkins.example.com/job/1"})
    async def test_build_failure_counter_stores_ocp_variant(self):
        """OCP build failures include the OCP build variant metadata."""
        mock_increment = AsyncMock()
        mock_reset = AsyncMock()
        record_log = {
            "image_build_konflux": [
                {
                    "name": "ironic",
                    "status": "1",
                    "task_id": "plr-1",
                    "task_url": "https://konflux.example.com/plr-1",
                    "outcome": "build_error",
                    "nvrs": "ironic-1.0-1",
                    "build_pipeline_url": "https://konflux.example.com/plr-1",
                }
            ]
        }

        await self._update_build_fail_counters(
            "stream",
            "openshift-4.21",
            [],
            ["ironic"],
            record_log,
            mock_reset,
            mock_increment,
        )

        self.assertEqual(mock_increment.call_args.kwargs["build_variant"], "ocp")


class TestSweepGolangBugs(unittest.IsolatedAsyncioTestCase):
    """Tests for KonfluxOcpPipeline.sweep_golang_bugs()"""

    def _make_streams_yml(self, tag: str) -> bytes:
        """Return streams.yml bytes containing a golang-builder entry with *tag*."""
        content = {
            'golang': {
                'image': f'registry.redhat.io/openshift/golang-builder:{tag}',
            }
        }
        import io as _io

        import yaml as _yaml

        buf = _io.StringIO()
        _yaml.dump(content, buf)
        return buf.getvalue().encode()

    @patch('pyartcd.pipelines.ocp4_konflux.get_active_versions_for_golang_major_minor', return_value=['4.17', '4.18'])
    @patch('artcommonlib.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.get_github_client_for_org')
    async def test_sweep_multi_version_loop(self, mock_gh, mock_cmd_assert, mock_get_active):
        """Elliott is called once per active OCP version."""
        pipeline = _make_konflux_ocp_pipeline(version='4.18', dry_run=False, slack_and_registry=True)

        # Mock streams.yml with a floating tag encoding golang 1.22
        streams_bytes = self._make_streams_yml('golang-builder-v1.22-rhel9')
        mock_contents = MagicMock()
        mock_contents.decoded_content = streams_bytes
        mock_gh.return_value.get_repo.return_value.get_contents.return_value = mock_contents

        await pipeline.sweep_golang_bugs()

        # get_active_versions_for_golang_major_minor must be called with parsed major.minor
        mock_get_active.assert_called_once_with('1.22', pipeline.data_path)
        # elliott called once per returned version
        self.assertEqual(mock_cmd_assert.call_count, 2)
        groups = [call[0][0][1] for call in mock_cmd_assert.call_args_list]
        self.assertIn('--group=openshift-4.17', groups)
        self.assertIn('--group=openshift-4.18', groups)

    @patch('artcommonlib.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.get_github_client_for_org')
    async def test_sweep_floating_tag_parse(self, mock_gh, mock_cmd_assert):
        """Floating tag golang-builder-vX.Y-rhelN is parsed to X.Y correctly."""
        pipeline = _make_konflux_ocp_pipeline(version='4.18', dry_run=False, slack_and_registry=True)

        streams_bytes = self._make_streams_yml('golang-builder-v1.23-rhel9')
        mock_contents = MagicMock()
        mock_contents.decoded_content = streams_bytes
        mock_gh.return_value.get_repo.return_value.get_contents.return_value = mock_contents

        with patch(
            'pyartcd.pipelines.ocp4_konflux.get_active_versions_for_golang_major_minor',
            return_value=['4.19'],
        ) as mock_get_active:
            await pipeline.sweep_golang_bugs()

        # Verify the parsed major.minor matches the tag
        mock_get_active.assert_called_once()
        self.assertEqual(mock_get_active.call_args[0][0], '1.23')

    @patch('artcommonlib.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.get_github_client_for_org')
    async def test_sweep_skips_non_stream_assembly(self, mock_gh, mock_cmd_assert):
        """sweep_golang_bugs must be a no-op for non-stream assemblies."""
        pipeline = _make_konflux_ocp_pipeline(assembly='test', version='4.18', dry_run=False, slack_and_registry=True)

        await pipeline.sweep_golang_bugs()

        mock_gh.assert_not_called()
        mock_cmd_assert.assert_not_called()

    @patch('artcommonlib.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.get_github_client_for_org')
    async def test_sweep_no_active_versions(self, mock_gh, mock_cmd_assert):
        """When version discovery returns empty list, elliott is never called."""
        pipeline = _make_konflux_ocp_pipeline(version='4.18', dry_run=False, slack_and_registry=True)
        streams_bytes = self._make_streams_yml('golang-builder-v1.22-rhel9')
        mock_contents = MagicMock()
        mock_contents.decoded_content = streams_bytes
        mock_gh.return_value.get_repo.return_value.get_contents.return_value = mock_contents

        with patch(
            'pyartcd.pipelines.ocp4_konflux.get_active_versions_for_golang_major_minor',
            return_value=[],
        ):
            await pipeline.sweep_golang_bugs()

        mock_cmd_assert.assert_not_called()

    @patch('artcommonlib.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.get_github_client_for_org')
    async def test_sweep_continues_after_version_failure(self, mock_gh, mock_cmd_assert):
        """Elliott failure on one version does not abort sweep for remaining versions."""
        pipeline = _make_konflux_ocp_pipeline(version='4.18', dry_run=False, slack_and_registry=True)
        streams_bytes = self._make_streams_yml('golang-builder-v1.22-rhel9')
        mock_contents = MagicMock()
        mock_contents.decoded_content = streams_bytes
        mock_gh.return_value.get_repo.return_value.get_contents.return_value = mock_contents

        # First version fails, second succeeds
        mock_cmd_assert.side_effect = [ChildProcessError("elliott failed"), None]

        with patch(
            'pyartcd.pipelines.ocp4_konflux.get_active_versions_for_golang_major_minor',
            return_value=['4.17', '4.18'],
        ):
            await pipeline.sweep_golang_bugs()

        # Both versions were attempted
        self.assertEqual(mock_cmd_assert.call_count, 2)
        # Slack was notified for the failing version
        pipeline.slack_client.say.assert_awaited_once()

    @patch('artcommonlib.exectools.cmd_assert_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.ocp4_konflux.get_github_client_for_org')
    async def test_sweep_dry_run_continues_on_failure(self, mock_gh, mock_cmd_assert):
        """In dry_run mode a ChildProcessError is silently continued without Slack."""
        pipeline = _make_konflux_ocp_pipeline(version='4.18', dry_run=True, slack_and_registry=True)
        streams_bytes = self._make_streams_yml('golang-builder-v1.22-rhel9')
        mock_contents = MagicMock()
        mock_contents.decoded_content = streams_bytes
        mock_gh.return_value.get_repo.return_value.get_contents.return_value = mock_contents

        mock_cmd_assert.side_effect = [ChildProcessError("elliott failed"), None]

        with patch(
            'pyartcd.pipelines.ocp4_konflux.get_active_versions_for_golang_major_minor',
            return_value=['4.17', '4.18'],
        ):
            await pipeline.sweep_golang_bugs()

        # Both versions attempted
        self.assertEqual(mock_cmd_assert.call_count, 2)
        # No Slack notification in dry_run
        pipeline.slack_client.say.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
