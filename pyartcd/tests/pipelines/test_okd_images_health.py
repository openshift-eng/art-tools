import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.cli.images_health import ConcernCode
from pyartcd.pipelines.okd_images_health import ImagesHealthPipeline

DATA_PATH = "https://github.com/openshift-eng/ocp-build-data"


def _make_concern(image_name, group, code, **kwargs):
    concern = {
        "image_name": image_name,
        "group": group,
        "code": code,
        "latest_failed_build_time": "2025-12-17T10:00:00+00:00",
        "latest_failed_nvr": f"{image_name}-1.0-1",
        "latest_failed_build_record_id": "12345",
    }
    concern.update(kwargs)
    return concern


class TestGetReport(IsolatedAsyncioTestCase):
    def _make_pipeline(self, versions="4.21", image_list=""):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        return ImagesHealthPipeline(
            runtime=runtime,
            versions=versions,
            send_to_release_channel=False,
            send_to_okd_channel=False,
            ping_chai_bot=False,
            data_path=DATA_PATH,
            data_gitref="",
            image_list=image_list,
            assembly="stream",
        )

    @patch("pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.okd_images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_redis_pre_filters_doozer_call(self, mock_get_failures, mock_cmd):
        """Redis failures scope the --images flag on the doozer subprocess."""
        mock_get_failures.return_value = {
            'ironic': {'failure_count': 3, 'url': '', 'nvr': ''},
            'ovn-kubernetes': {'failure_count': 2, 'url': '', 'nvr': ''},
        }
        doozer_report = [
            _make_concern("ironic", "okd-4.21", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
            _make_concern("ovn-kubernetes", "okd-4.21", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=2),
        ]
        mock_cmd.return_value = (0, json.dumps(doozer_report), '')

        pipeline = self._make_pipeline()
        # Mock _get_valid_images to return all images as valid
        pipeline._get_valid_images = AsyncMock(return_value={'ironic', 'ovn-kubernetes'})

        await pipeline.get_report('4.21')

        mock_get_failures.assert_called_once_with('build-failure', group='okd-4.21', logger=pipeline.runtime.logger)
        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        images_arg = next(a for a in cmd if a.startswith('--images='))
        self.assertIn('ironic', images_arg)
        self.assertIn('ovn-kubernetes', images_arg)
        self.assertEqual(len(pipeline.report), 2)

    @patch("pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.okd_images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_image_list_intersects_with_redis(self, mock_get_failures, mock_cmd):
        """When --image-list is provided, only images in BOTH lists are queried."""
        mock_get_failures.return_value = {
            'ironic': {'failure_count': 3, 'url': '', 'nvr': ''},
            'ovn-kubernetes': {'failure_count': 2, 'url': '', 'nvr': ''},
        }
        doozer_report = [
            _make_concern("ironic", "okd-4.21", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        mock_cmd.return_value = (0, json.dumps(doozer_report), '')

        pipeline = self._make_pipeline(image_list="ironic")
        # Mock _get_valid_images to return ironic as valid
        pipeline._get_valid_images = AsyncMock(return_value={'ironic', 'ovn-kubernetes'})

        await pipeline.get_report('4.21')

        cmd = mock_cmd.call_args[0][0]
        images_arg = next(a for a in cmd if a.startswith('--images='))
        self.assertEqual(images_arg, '--images=ironic')

    @patch("pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.okd_images_health.util.get_counter_failures", new_callable=AsyncMock, return_value={})
    async def test_skips_bigquery_when_no_redis_failures(self, _mock_get_failures, mock_cmd):
        """When Redis reports no failures, doozer images:health is not called at all."""
        pipeline = self._make_pipeline()

        await pipeline.get_report('4.21')

        mock_cmd.assert_not_called()
        self.assertEqual(len(pipeline.report), 0)
        self.assertIn('4.21', pipeline.scanned_versions)

    @patch("pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock)
    async def test_filters_disabled_versions(self, mock_is_enabled):
        """Requested versions without okd.enabled in build-data are skipped."""
        mock_is_enabled.side_effect = [True, False]

        pipeline = self._make_pipeline(versions="4.21,4.23")
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()

        await pipeline.run()

        self.assertEqual(pipeline.versions, ['4.21'])
        self.assertEqual(mock_is_enabled.await_count, 2)
        pipeline.get_report.assert_awaited_once_with('4.21')

    @patch("pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock)
    async def test_discovers_enabled_versions_when_not_provided(self, mock_is_enabled):
        """When --versions is omitted, probe ACTIVE_OCP_VERSIONS and keep okd.enabled versions."""

        async def enabled_for_cmd(cmd):
            return any(arg == '--group=openshift-4.21' for arg in cmd)

        mock_is_enabled.side_effect = enabled_for_cmd

        pipeline = self._make_pipeline(versions="")
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()

        with patch(
            'pyartcd.pipelines.okd_images_health.ACTIVE_OCP_VERSIONS',
            ['4.21', '4.23'],
        ):
            await pipeline.run()

        self.assertEqual(pipeline.versions, ['4.21'])
        self.assertEqual(mock_is_enabled.await_count, 2)
        pipeline.get_report.assert_awaited_once_with('4.21')

    @patch(
        "pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock, return_value=False
    )
    async def test_skips_when_no_enabled_versions(self, _mock_is_enabled):
        pipeline = self._make_pipeline(versions="4.23")
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.notify_release_channel = AsyncMock()
        pipeline.notify_okd_channel = AsyncMock()

        await pipeline.run()

        self.assertEqual(pipeline.versions, [])
        pipeline.get_report.assert_not_called()
        pipeline.notify_okd_channel.assert_not_called()


class TestGetMultiRebaseFailures(IsolatedAsyncioTestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        return ImagesHealthPipeline(
            runtime=runtime,
            versions="4.21",
            send_to_release_channel=False,
            send_to_okd_channel=False,
            ping_chai_bot=True,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )

    def test_filters_single_failures(self):
        """Images with only 1 rebase failure are excluded."""
        pipeline = self._make_pipeline()
        pipeline.rebase_failures = {
            '4.21': {
                'ironic': {'failure_count': 1, 'jenkins_url': ''},
                'ovn-kubernetes': {'failure_count': 3, 'jenkins_url': 'http://jenkins/job/1'},
            }
        }
        result = pipeline._get_multi_rebase_failures()
        self.assertIn('4.21', result)
        self.assertNotIn('ironic', result['4.21'])
        self.assertIn('ovn-kubernetes', result['4.21'])

    def test_excludes_versions_with_no_multi_failures(self):
        """Versions where all images have <=1 failure are excluded from result."""
        pipeline = self._make_pipeline()
        pipeline.rebase_failures = {
            '4.21': {'ironic': {'failure_count': 1, 'jenkins_url': ''}},
        }
        result = pipeline._get_multi_rebase_failures()
        self.assertNotIn('4.21', result)

    def test_empty_rebase_failures(self):
        pipeline = self._make_pipeline()
        pipeline.rebase_failures = {}
        self.assertEqual(pipeline._get_multi_rebase_failures(), {})


class TestNotifyChaiBot(IsolatedAsyncioTestCase):
    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        runtime.new_slack_client.return_value = MagicMock()
        return ImagesHealthPipeline(
            runtime=runtime,
            versions="4.21",
            send_to_release_channel=False,
            send_to_okd_channel=False,
            ping_chai_bot=True,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )

    async def test_skips_when_no_failures_of_either_kind(self):
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock()
        await pipeline.notify_chai_bot('4.21', [], {})
        pipeline.slack_client.say.assert_not_called()

    async def test_notifies_for_rebase_only(self):
        """Rebase failures alone trigger chai-bot notification."""
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock(return_value={'ts': '123'})
        pipeline.slack_client.bind_channel = MagicMock()
        rebase_failures = {
            'ironic': {'failure_count': 3, 'jenkins_url': 'http://jenkins/job/1'},
        }
        await pipeline.notify_chai_bot('4.21', [], rebase_failures)
        pipeline.slack_client.say.assert_called_once()
        msg = pipeline.slack_client.say.call_args[0][0]
        self.assertIn('ironic', msg)
        self.assertIn('rebase failures', msg)

    async def test_prompt_includes_both_build_and_rebase_sections(self):
        """When both failure types present, prompt contains both sections."""
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock(return_value={'ts': '123'})
        pipeline.slack_client.bind_channel = MagicMock()
        concern = _make_concern('console', 'openshift-4.21', 'LATEST_ATTEMPT_FAILED', latest_success_idx=3)
        rebase_failures = {'ironic': {'failure_count': 3, 'jenkins_url': ''}}
        await pipeline.notify_chai_bot('4.21', [concern], rebase_failures)
        msg = pipeline.slack_client.say.call_args[0][0]
        self.assertIn('build failures', msg)
        self.assertIn('rebase failures', msg)
        self.assertIn('console', msg)
        self.assertIn('ironic', msg)

    async def test_prompt_contains_skip_label_and_jenkins_url(self):
        """Prompt includes skip-label suggestion and full Jenkins URL pattern."""
        pipeline = self._make_pipeline()
        concern = _make_concern('console', 'openshift-4.21', 'LATEST_ATTEMPT_FAILED', latest_success_idx=2)
        prompt = pipeline._build_chai_bot_prompt('4.21', [concern], {})
        self.assertIn('Suggest that ART add the `art:bot-skip-auto-fix` label', prompt)
        self.assertIn('art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com', prompt)

    @patch("pyartcd.pipelines.okd_images_health.util.is_okd_version_enabled", new_callable=AsyncMock, return_value=True)
    async def test_run_calls_chai_bot_for_rebase_only_version(self, _mock_enabled):
        """run() calls notify_chai_bot when only rebase failures exist (no build failures)."""
        pipeline = self._make_pipeline()
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.notify_chai_bot = AsyncMock()
        pipeline.rebase_failures = {
            '4.21': {'ironic': {'failure_count': 3, 'jenkins_url': ''}},
        }
        pipeline.report = []

        await pipeline.run()

        pipeline.notify_chai_bot.assert_awaited_once()
        call_args = pipeline.notify_chai_bot.call_args
        self.assertEqual(call_args[0][0], '4.21')
        self.assertEqual(call_args[0][1], [])
        self.assertIn('ironic', call_args[0][2])
