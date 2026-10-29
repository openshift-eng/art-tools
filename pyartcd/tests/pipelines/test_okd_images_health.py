import json
from contextlib import redirect_stdout
from io import StringIO
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib import Runtime
from doozerlib.cli.images import images_print
from doozerlib.cli.images_health import ConcernCode
from doozerlib.cli.images_health import ImagesHealthPipeline as DoozerImagesHealthPipeline
from doozerlib.image import ImageMetadata
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


class TestNotifyOkdChannel(IsolatedAsyncioTestCase):
    async def test_no_concerns_after_filtering(self):
        runtime = MagicMock()
        runtime.working_dir = MagicMock()
        runtime.logger = MagicMock()
        slack_client = MagicMock()
        slack_client.say = AsyncMock()
        runtime.new_slack_client.return_value = slack_client

        pipeline = ImagesHealthPipeline(
            runtime=runtime,
            versions="4.22",
            send_to_release_channel=False,
            send_to_okd_channel=True,
            ping_chai_bot=False,
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
        )
        pipeline.report = [
            {
                "code": ConcernCode.NEVER_BUILT.value,
                "image_name": "never-built-image",
                "group": "openshift-4.22",
            },
            {
                "code": ConcernCode.LATEST_BUILD_SUCCEEDED.value,
                "image_name": "succeeded-image",
                "group": "openshift-4.22",
            },
        ]
        pipeline.rebase_failures = {"4.22": {}}

        await pipeline.notify_okd_channel()

        slack_client.say.assert_awaited_once_with(
            ":white_check_mark: All OKD images are healthy for all monitored releases"
        )


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

    @patch('pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.okd_images_health.util.get_counter_failures', new_callable=AsyncMock)
    async def test_non_release_base_failure_reaches_summary(self, mock_get_failures, mock_cmd):
        """An OKD-only base failure survives real enumeration, classification and summary reporting."""
        image_name = 'openshift-enterprise-base-rhel10'
        doozer_runtime = MagicMock(spec=Runtime)
        doozer_runtime.group = 'openshift-5.1'
        doozer_runtime.variant = BuildVariant.OKD
        doozer_runtime.branch = 'openshift-5.1'
        doozer_runtime.assembly = 'stream'
        doozer_runtime.assembly_basis_event = None
        doozer_runtime.group_config = Model({})
        doozer_runtime.get_releases_config.return_value = Model({})
        doozer_runtime.load_disabled = False
        image_meta = ImageMetadata(
            doozer_runtime,
            Model(
                {
                    'key': image_name,
                    'filename': f'{image_name}.yml',
                    'data': {
                        'name': f'openshift/{image_name}',
                        'mode': 'disabled',
                        'okd': {'mode': 'enabled'},
                        'base_only': True,
                        'for_release': False,
                        'for_payload': False,
                    },
                }
            ),
        )
        self.assertFalse(image_meta.enabled)
        doozer_runtime.image_metas.return_value = [image_meta]
        doozer_runtime.konflux_db = MagicMock()
        doozer_runtime.konflux_db._ensure_group_cached = AsyncMock()
        failed_build = MagicMock(outcome=KonfluxBuildOutcome.FAILURE)
        doozer_runtime.konflux_db.cache.get_builds_by_name.return_value = [failed_build]
        mock_get_failures.return_value = {image_name: {'failure_count': 1}}

        async def run_doozer(cmd, **kwargs):
            if 'images:print' in cmd:
                result = CliRunner().invoke(images_print, cmd[cmd.index('images:print') + 1 :], obj=doozer_runtime)
                self.assertEqual(result.exit_code, 0, result.output)
                self.assertEqual(result.output.strip(), image_name)
                return 0, result.output, ''
            self.assertIn(f'--images={image_name}', cmd)
            health = DoozerImagesHealthPipeline(
                runtime=doozer_runtime, limit=100, group='okd-5.1', assembly='stream', variant=BuildVariant.OKD
            )
            output = StringIO()
            with redirect_stdout(output):
                await health.run()
            return 0, output.getvalue(), ''

        mock_cmd.side_effect = run_doozer
        pipeline = self._make_pipeline(versions='5.1')
        pipeline._resolve_versions = AsyncMock(return_value=['5.1'])
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.send_to_okd_channel = True
        pipeline.slack_client.say = AsyncMock()

        await pipeline.run()

        enumeration_cmd = mock_cmd.await_args_list[0].args[0]
        self.assertIn('--show-base', enumeration_cmd)
        self.assertIn('--show-non-release', enumeration_cmd)
        doozer_runtime.konflux_db.cache.get_builds_by_name.assert_called_once_with(
            name=image_name, group='okd-5.1', assembly='stream', limit=100
        )
        self.assertEqual(len(pipeline.report), 1)
        self.assertEqual(pipeline.report[0]['image_name'], image_name)
        self.assertEqual(pipeline.report[0]['code'], ConcernCode.LATEST_ATTEMPT_FAILED.value)
        self.assertFalse(pipeline.report[0]['for_release'])
        message = pipeline.slack_client.say.await_args.args[0]
        self.assertIn('1 image with build failures', message)
        self.assertNotIn('All OKD images are healthy', message)

    @patch('pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.okd_images_health.util.get_counter_failures', new_callable=AsyncMock)
    async def test_metadata_enumeration_failure_stops_notifications(self, mock_get_failures, mock_cmd):
        mock_get_failures.return_value = {'base-image': {'failure_count': 1}}
        mock_cmd.side_effect = ChildProcessError('metadata enumeration failed after partial output')
        pipeline = self._make_pipeline()
        pipeline._resolve_versions = AsyncMock(return_value=['4.21'])
        pipeline.send_to_okd_channel = True
        pipeline.send_to_release_channel = True
        pipeline.slack_client.say = AsyncMock()

        with self.assertRaisesRegex(ChildProcessError, 'metadata enumeration failed'):
            await pipeline.run()

        self.assertEqual(pipeline.scanned_versions, [])
        pipeline.slack_client.say.assert_not_awaited()

    @patch('pyartcd.pipelines.okd_images_health.exectools.cmd_gather_async', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.okd_images_health.util.get_rebase_failures', new_callable=AsyncMock)
    async def test_rebase_metadata_failure_stops_notifications(self, mock_rebase_failures, mock_cmd):
        mock_rebase_failures.return_value = {'base-image': {'failure_count': 1}}
        mock_cmd.side_effect = ChildProcessError('metadata enumeration failed')
        pipeline = self._make_pipeline()
        pipeline._resolve_versions = AsyncMock(return_value=['4.21'])
        pipeline.get_report = AsyncMock()
        pipeline.send_to_okd_channel = True
        pipeline.slack_client.say = AsyncMock()

        with self.assertRaisesRegex(ChildProcessError, 'metadata enumeration failed'):
            await pipeline.run()

        pipeline.slack_client.say.assert_not_awaited()

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
        self.assertIn('art:bot-skip-auto-fix', prompt)
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
