import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.cli.images_health import ConcernCode
from pyartcd.pipelines.images_health import ImagesHealthPipeline

DATA_PATH = "https://github.com/openshift-eng/ocp-build-data"


def _make_ticket(key, labels):
    ticket = MagicMock()
    ticket.key = key
    ticket.fields.labels = labels
    return ticket


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


def _make_pipeline(runtime, versions="4.22", public_channel="", image_list="", assembly="stream", sync_jira=False):
    return ImagesHealthPipeline(
        runtime=runtime,
        versions=versions,
        send_to_release_channel=False,
        public_channel=public_channel,
        data_path=DATA_PATH,
        data_gitref="",
        image_list=image_list,
        assembly=assembly,
        sync_jira=sync_jira,
    )


def _make_runtime():
    runtime = MagicMock()
    runtime.working_dir = MagicMock()
    runtime.logger = MagicMock()
    mock_slack_client = MagicMock()
    mock_slack_client.say = AsyncMock()
    runtime.new_slack_client = MagicMock(return_value=mock_slack_client)
    return runtime


def _collect_logged(logger) -> str:
    """Concatenate every positional arg of every warning/info/error log call into one string."""
    parts = []
    for logmethod in (logger.warning, logger.info, logger.error):
        for call in logmethod.call_args_list:
            parts.append(" ".join(str(a) for a in call.args))
    return "\n".join(parts)


class TestNotifyPublicChannel(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = _make_runtime()

    async def test_failure_summary_links_to_ocp_dashboard(self):
        for base_url in ('https://dashboard.example.test', 'https://dashboard.example.test/'):
            with self.subTest(base_url=base_url):
                mock_slack_client = self.mock_runtime.new_slack_client.return_value
                mock_slack_client.reset_mock()
                pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
                pipeline.report = [
                    _make_concern("failing-image", "openshift-4.22", ConcernCode.LATEST_ATTEMPT_FAILED.value)
                ]
                mock_slack_client.say.return_value = {"ts": "thread-123"}

                with patch('pyartcd.pipelines.images_health.ART_BUILD_FAILURES_URL', base_url):
                    await pipeline.notify_public_channel()

                summary = mock_slack_client.say.call_args_list[0].args[0]
                self.assertIn('<https://dashboard.example.test/?variant=ocp|ART Build Failures Dashboard>', summary)

    async def test_no_concerns_after_filtering(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
        pipeline.report = [
            {"code": ConcernCode.NEVER_BUILT.value, "image_name": "never-built-image", "group": "openshift-4.22"},
            {"code": ConcernCode.LATEST_BUILD_SUCCEEDED.value, "image_name": "ok-image", "group": "openshift-4.22"},
        ]

        await pipeline.notify_public_channel()

        mock_slack_client.say.assert_called_once_with(
            ":white_check_mark: All images are healthy for all monitored releases"
        )

    async def test_with_build_concerns(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, versions="4.21", public_channel="#forum-ocp-art")
        pipeline.report = [
            _make_concern("failing-image", "openshift-4.21", ConcernCode.FAILING_AT_LEAST_FOR.value),
            _make_concern(
                "failing-image", "openshift-4.20", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3
            ),
        ]
        mock_slack_client.say.return_value = {"ts": "thread-123"}

        await pipeline.notify_public_channel()

        # Should post a summary message + one thread response per group (2 groups)
        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 3)  # 1 summary + 2 groups

        # Check main summary message only has alert and dashboard link
        summary_args = calls[0][0][0]
        self.assertIn(":alert:", summary_args)
        self.assertIn("ART Build Failures Dashboard", summary_args)
        # Group names should NOT be in the main message
        self.assertNotIn("openshift-4.20:", summary_args)
        self.assertNotIn("openshift-4.21:", summary_args)

        # Check thread responses contain group summaries
        thread_1 = calls[1][0][0]
        thread_2 = calls[2][0][0]

        # One should be 4.20, the other 4.21
        self.assertTrue("openshift-4.20" in thread_1 or "openshift-4.20" in thread_2)
        self.assertTrue("openshift-4.21" in thread_1 or "openshift-4.21" in thread_2)
        self.assertTrue("build failures" in thread_1 or "build failures" in thread_2)

        self.assertEqual(calls[1][1]['thread_ts'], "thread-123")
        self.assertEqual(calls[2][1]['thread_ts'], "thread-123")

    async def test_with_mixed_failure_types(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#test-channel")
        pipeline.report = [
            _make_concern("build-fail", "openshift-4.22", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]
        pipeline.ec_failures = {
            '4.22': {'ec-fail-image': {'failure_count': 3, 'jenkins_url': 'http://j/1', 'pipeline_url': 'http://ec/1'}},
        }
        pipeline.release_failures = {
            '4.22': {'release-fail-image': {'failure_count': 2, 'jenkins_url': 'http://j/2'}},
        }
        pipeline.rebase_failures = {
            '4.22': {'rebase-fail-image': {'failure_count': 1, 'jenkins_url': 'http://j/3', 'build_system': 'konflux'}},
        }
        mock_slack_client.say.return_value = {"ts": "thread-456"}

        await pipeline.notify_public_channel()

        # Should post a summary message + one thread response for openshift-4.22
        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 2)  # 1 summary + 1 group

        # Check main summary message only has alert and dashboard link
        summary = calls[0][0][0]
        self.assertIn(":alert:", summary)
        self.assertIn("ART Build Failures Dashboard", summary)
        # Group details should NOT be in the main message
        self.assertNotIn("openshift-4.22:", summary)

        # Check thread response contains group summary
        thread_message = calls[1][0][0]
        self.assertIn("openshift-4.22", thread_message)
        self.assertIn("build failures", thread_message)
        self.assertIn("ITS verification failures", thread_message)
        self.assertIn("release to authz failures", thread_message)
        self.assertIn("rebase failures", thread_message)
        self.assertEqual(calls[1][1]['thread_ts'], "thread-456")

    async def test_binds_to_configured_channel(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#my-custom-channel")
        pipeline.report = []

        await pipeline.notify_public_channel()

        mock_slack_client.bind_channel.assert_called_once_with("#my-custom-channel")

    async def test_groups_sorted_by_version_descending(self):
        """
        Verify that releases are ordered from newest to oldest (e.g., 5.0, 4.23, 4.22).
        """
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
        pipeline.report = [
            _make_concern("img-1", "openshift-4.22", ConcernCode.FAILING_AT_LEAST_FOR.value),
            _make_concern("img-2", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
            _make_concern("img-3", "openshift-4.23", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]
        mock_slack_client.say.return_value = {"ts": "thread-999"}

        await pipeline.notify_public_channel()

        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 4)  # 1 summary + 3 groups

        # Extract group names from thread messages (skipping the summary)
        thread_messages = [calls[i][0][0] for i in range(1, 4)]

        # Verify order: should be 5.0, 4.23, 4.22
        self.assertIn("openshift-5.0", thread_messages[0])
        self.assertIn("openshift-4.23", thread_messages[1])
        self.assertIn("openshift-4.22", thread_messages[2])

    async def test_empty_report(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
        pipeline.report = []

        await pipeline.notify_public_channel()

        mock_slack_client.say.assert_called_once_with(
            ":white_check_mark: All images are healthy for all monitored releases"
        )

    async def test_only_its_failures(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
        pipeline.report = []
        pipeline.ec_failures = {
            '4.22': {'image-a': {'failure_count': 5, 'jenkins_url': '', 'pipeline_url': 'http://ec/1'}},
        }
        mock_slack_client.say.return_value = {"ts": "thread-789"}

        await pipeline.notify_public_channel()

        # Should post a summary message + one thread response for openshift-4.22
        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 2)  # 1 summary + 1 group

        # Check main summary message only has alert and dashboard link
        summary = calls[0][0][0]
        self.assertIn(":alert:", summary)
        self.assertIn("ART Build Failures Dashboard", summary)
        # Group details should NOT be in the main message
        self.assertNotIn("openshift-4.22:", summary)

        # Check thread response
        thread_message = calls[1][0][0]
        self.assertIn("openshift-4.22", thread_message)
        self.assertIn("ITS verification failures", thread_message)
        self.assertNotIn("build failures", thread_message)
        self.assertEqual(calls[1][1]['thread_ts'], "thread-789")


class TestNotifyReleaseChannel(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = _make_runtime()

    async def test_all_healthy(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, versions="4.18")
        pipeline.report = []
        pipeline.scanned_versions = ["4.18"]

        await pipeline.notify_release_channel("4.18")

        mock_slack_client.say.assert_called_once()
        self.assertIn("healthy", mock_slack_client.say.call_args[0][0])

    async def test_grouped_failure_summary(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, versions="4.18")
        pipeline.report = [
            _make_concern(
                "build-fail-1", "openshift-4.18", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=2
            ),
            _make_concern("build-fail-2", "openshift-4.18", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]
        pipeline.ec_failures = {
            '4.18': {'ec-img': {'failure_count': 3, 'jenkins_url': 'http://j/1', 'pipeline_url': 'http://ec/1'}},
        }
        pipeline.release_failures = {
            '4.18': {'rel-img': {'failure_count': 1, 'jenkins_url': 'http://j/2'}},
        }
        mock_slack_client.say.return_value = {"ts": ""}

        await pipeline.notify_release_channel("4.18")

        calls = mock_slack_client.say.call_args_list
        summary = calls[0][0][0]
        self.assertIn("2 images failed to build", summary)
        self.assertIn("1 image failed ITS verification", summary)
        self.assertIn("1 image failed to be released to authz", summary)

        thread_detail = calls[1][0][0]
        self.assertIn("Build Failures (2)", thread_detail)
        self.assertIn("ITS Verification Failures (1)", thread_detail)
        self.assertIn("Release to Authz Failures (1)", thread_detail)


class TestGetReport(IsolatedAsyncioTestCase):
    def _make_pipeline(self, versions="4.18", image_list=""):
        runtime = _make_runtime()
        return _make_pipeline(runtime, versions=versions, image_list=image_list)

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.is_build_permitted", new_callable=AsyncMock, return_value=True)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_redis_pre_filters_doozer_call(self, mock_get_failures, _mock_permitted, mock_cmd):
        mock_get_failures.return_value = {
            'ironic': {'failure_count': 3, 'url': '', 'nvr': ''},
            'hypershift': {'failure_count': 5, 'url': '', 'nvr': ''},
        }
        doozer_report = [
            _make_concern("ironic", "openshift-4.18", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
            _make_concern(
                "hypershift", "openshift-4.18", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=5
            ),
        ]
        mock_cmd.return_value = (0, json.dumps(doozer_report), '')

        pipeline = self._make_pipeline()
        pipeline._get_valid_images = AsyncMock(return_value={'ironic', 'hypershift'})

        await pipeline.get_report('4.18')

        mock_get_failures.assert_called_once_with(
            'build-failure', group='openshift-4.18', logger=pipeline.runtime.logger
        )
        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        images_arg = next(a for a in cmd if a.startswith('--images='))
        self.assertIn('ironic', images_arg)
        self.assertIn('hypershift', images_arg)
        self.assertEqual(len(pipeline.report), 2)

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.is_build_permitted", new_callable=AsyncMock, return_value=True)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_image_list_intersects_with_redis(self, mock_get_failures, _mock_permitted, mock_cmd):
        mock_get_failures.return_value = {
            'ironic': {'failure_count': 3, 'url': '', 'nvr': ''},
            'hypershift': {'failure_count': 5, 'url': '', 'nvr': ''},
        }
        doozer_report = [
            _make_concern("ironic", "openshift-4.18", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        mock_cmd.return_value = (0, json.dumps(doozer_report), '')

        pipeline = self._make_pipeline(image_list="ironic")
        pipeline._get_valid_images = AsyncMock(return_value={'ironic', 'hypershift'})

        await pipeline.get_report('4.18')

        cmd = mock_cmd.call_args[0][0]
        images_arg = next(a for a in cmd if a.startswith('--images='))
        self.assertEqual(images_arg, '--images=ironic')

    @patch("pyartcd.pipelines.images_health.util.is_build_permitted", new_callable=AsyncMock, return_value=False)
    async def test_skips_frozen_group(self, _mock_permitted):
        pipeline = self._make_pipeline()

        await pipeline.get_report('4.18')

        self.assertEqual(len(pipeline.report), 0)
        self.assertNotIn('4.18', pipeline.scanned_versions)

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.is_build_permitted", new_callable=AsyncMock, return_value=True)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock, return_value={})
    async def test_skips_bigquery_when_no_redis_failures(self, _mock_get_failures, _mock_permitted, mock_cmd):
        pipeline = self._make_pipeline()

        await pipeline.get_report('4.18')

        mock_cmd.assert_not_called()
        self.assertEqual(len(pipeline.report), 0)
        self.assertIn('4.18', pipeline.scanned_versions)


class TestGetTypedFailures(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_ec_failures_filtered_by_valid_images(self, mock_get_failures):
        mock_get_failures.return_value = {
            'valid-image': {'failure_count': 3, 'jenkins_url': 'http://j/1', 'pipeline_url': 'http://ec/1'},
            'invalid-image': {'failure_count': 1, 'jenkins_url': 'http://j/2'},
        }
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime, versions="4.18")
        pipeline._get_valid_images = AsyncMock(return_value={'valid-image', 'other-image'})

        await pipeline.get_ec_failures('4.18')

        self.assertIn('valid-image', pipeline.ec_failures['4.18'])
        self.assertNotIn('invalid-image', pipeline.ec_failures['4.18'])
        mock_get_failures.assert_called_once_with('ec-failure', group='openshift-4.18', logger=runtime.logger)

    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_release_failures_filtered_by_valid_images(self, mock_get_failures):
        mock_get_failures.return_value = {
            'ose-base': {'failure_count': 2, 'jenkins_url': 'http://j/1'},
        }
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime, versions="4.18")
        pipeline._get_valid_images = AsyncMock(return_value={'ose-base'})

        await pipeline.get_release_failures('4.18')

        self.assertEqual(pipeline.release_failures['4.18']['ose-base']['failure_count'], 2)

    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_empty_redis_returns_empty(self, mock_get_failures):
        mock_get_failures.return_value = {}
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime, versions="4.18")

        await pipeline.get_ec_failures('4.18')

        self.assertEqual(pipeline.ec_failures['4.18'], {})

    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_valid_images_cache(self, mock_get_failures):
        mock_get_failures.return_value = {
            'img': {'failure_count': 1, 'jenkins_url': ''},
        }
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime, versions="4.18")
        mock_get_valid = AsyncMock(return_value={'img'})
        pipeline._get_valid_images = mock_get_valid

        await pipeline.get_ec_failures('4.18')
        await pipeline.get_release_failures('4.18')

        # _get_valid_images is called twice because _get_typed_failures calls it,
        # but _get_valid_images itself has caching so doozer would only be called once
        self.assertEqual(mock_get_valid.call_count, 2)


class TestSyncJira(TestCase):
    def _make_pipeline(self, image_list=""):
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime, versions="5.0", image_list=image_list, sync_jira=True)
        pipeline.scanned_versions = ["5.0"]
        return pipeline

    def test_creates_ticket_for_new_build_failure(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        mock_jira.create_issue.return_value = MagicMock(key="ART-100")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        create_calls = [c for c in mock_jira.create_issue.call_args_list]
        build_call = next(c for c in create_calls if "build failure" in c[1]["summary"].lower())
        self.assertEqual(build_call[1]["project"], "ART")
        self.assertIn("art:image-build-failure", build_call[1]["labels"])
        self.assertIn("art:package:ironic", build_call[1]["labels"])
        self.assertIn("art:group:openshift-5.0", build_call[1]["labels"])
        self.assertIn("art:fail-count:100", build_call[1]["labels"])

    def test_creates_ticket_with_fail_count_from_latest_success_idx(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=7),
        ]

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        mock_jira.create_issue.return_value = MagicMock(key="ART-101")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        build_call = next(
            c for c in mock_jira.create_issue.call_args_list if "build failure" in c[1]["summary"].lower()
        )
        self.assertIn("art:fail-count:7", build_call[1]["labels"])

    def test_updates_existing_build_ticket(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]

        existing_ticket = _make_ticket(
            "ART-99",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0", "art:fail-count:5"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [existing_ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.create_issue.assert_not_called()
        existing_ticket.update.assert_called_once()
        updated_fields = existing_ticket.update.call_args[1]["fields"]
        updated_labels = updated_fields["labels"]
        self.assertIn("art:fail-count:100", updated_labels)
        self.assertNotIn("art:fail-count:5", updated_labels)
        self.assertIn("ironic", updated_fields["description"])
        self.assertIn("Error logs", updated_fields["description"])

    def test_updates_existing_ticket_without_prior_fail_count(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]

        existing_ticket = _make_ticket(
            "ART-99",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [existing_ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.create_issue.assert_not_called()
        existing_ticket.update.assert_called_once()
        updated_fields = existing_ticket.update.call_args[1]["fields"]
        updated_labels = updated_fields["labels"]
        self.assertIn("art:fail-count:3", updated_labels)
        self.assertIn("art:image-build-failure", updated_labels)
        self.assertIn("ironic-1.0-1", updated_fields["description"])

    def test_closes_resolved_build_ticket(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.LATEST_BUILD_SUCCEEDED.value),
        ]

        stale_ticket = _make_ticket(
            "ART-50",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [stale_ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.close_task.assert_called_with("ART-50")

    def test_skips_close_for_unscanned_version(self):
        pipeline = self._make_pipeline()
        pipeline.scanned_versions = ["5.0"]
        pipeline.report = []

        ticket_4_18 = _make_ticket(
            "ART-60",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-4.18"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [ticket_4_18]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.close_task.assert_not_called()

    def test_skips_close_for_unscanned_image(self):
        pipeline = self._make_pipeline(image_list="other-image")
        pipeline.report = []

        ticket = _make_ticket(
            "ART-70",
            ["art:image-build-failure", "art:package:ironic", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.close_task.assert_not_called()

    def test_creates_ticket_for_its_failure(self):
        pipeline = self._make_pipeline()
        pipeline.report = []
        pipeline.ec_failures = {
            '5.0': {
                'ec-image': {
                    'failure_count': 4,
                    'jenkins_url': 'http://j/1',
                    'nvr': 'ec-image-1.0-1',
                    'pipeline_url': 'http://ec/1',
                },
            },
        }

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        mock_jira.create_issue.return_value = MagicMock(key="ART-200")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        its_call = next(
            c for c in mock_jira.create_issue.call_args_list if "ITS verification failure" in c[1]["summary"]
        )
        self.assertIn("art:image-ec-failure", its_call[1]["labels"])
        self.assertIn("art:package:ec-image", its_call[1]["labels"])
        self.assertIn("art:fail-count:4", its_call[1]["labels"])
        self.assertIn("Pipeline:", its_call[1]["description"])

    def test_creates_ticket_for_release_failure(self):
        pipeline = self._make_pipeline()
        pipeline.report = []
        pipeline.release_failures = {
            '5.0': {
                'ose-base': {
                    'failure_count': 2,
                    'jenkins_url': 'http://j/1',
                    'nvr': 'ose-base-1.0-1',
                },
            },
        }

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        mock_jira.create_issue.return_value = MagicMock(key="ART-300")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        release_call = next(
            c for c in mock_jira.create_issue.call_args_list if "release to authz failure" in c[1]["summary"].lower()
        )
        self.assertIn("art:image-release-failure", release_call[1]["labels"])
        self.assertIn("art:package:ose-base", release_call[1]["labels"])

    def test_closes_resolved_ec_ticket(self):
        pipeline = self._make_pipeline()
        pipeline.report = []
        pipeline.ec_failures = {'5.0': {}}

        stale_ticket = _make_ticket(
            "ART-201",
            ["art:image-ec-failure", "art:package:ec-image", "art:group:openshift-5.0"],
        )
        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = [stale_ticket]
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        mock_jira.close_task.assert_called_with("ART-201")

    def test_fetches_all_pages(self):
        pipeline = self._make_pipeline()
        pipeline.report = []

        mock_jira = MagicMock()
        mock_jira.search_issues.return_value = []
        pipeline.runtime.new_jira_client.return_value = mock_jira

        pipeline.sync_jira()

        # sync_jira now calls search_issues 3 times (build, ec, release)
        self.assertEqual(mock_jira.search_issues.call_count, 3)
        for call in mock_jira.search_issues.call_args_list:
            _, kwargs = call
            self.assertFalse(kwargs.get("maxResults", True))


class TestSyncJiraIntegration(IsolatedAsyncioTestCase):
    def _make_pipeline(self, assembly="stream"):
        runtime = _make_runtime()
        pipeline = ImagesHealthPipeline(
            runtime=runtime,
            versions="5.0",
            send_to_release_channel=False,
            public_channel="",
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly=assembly,
            sync_jira=True,
        )
        pipeline.scanned_versions = ["5.0"]
        return pipeline

    @patch.object(ImagesHealthPipeline, "get_report", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_rebase_failures", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_ec_failures", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_release_failures", new_callable=AsyncMock)
    async def test_skips_jira_sync_for_non_stream_assembly(self, _mock_release, _mock_ec, _mock_rebase, _mock_report):
        pipeline = self._make_pipeline(assembly="4.18.5")
        pipeline.report = [
            _make_concern("ironic", "openshift-5.0", ConcernCode.FAILING_AT_LEAST_FOR.value),
        ]
        mock_jira = MagicMock()
        pipeline.runtime.new_jira_client.return_value = mock_jira

        await pipeline.run()

        mock_jira.search_issues.assert_not_called()
        mock_jira.create_issue.assert_not_called()

    @patch.object(ImagesHealthPipeline, "get_report", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_rebase_failures", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_ec_failures", new_callable=AsyncMock)
    @patch.object(ImagesHealthPipeline, "get_release_failures", new_callable=AsyncMock)
    async def test_jira_failure_does_not_block_notifications(self, _mock_release, _mock_ec, _mock_rebase, _mock_report):
        pipeline = self._make_pipeline()
        pipeline.report = []
        pipeline.send_to_release_channel = True
        pipeline.scanned_versions = ["5.0"]

        mock_jira = MagicMock()
        mock_jira.search_issues.side_effect = RuntimeError("Jira is down")
        pipeline.runtime.new_jira_client.return_value = mock_jira

        mock_slack = MagicMock()
        mock_slack.say = AsyncMock()
        pipeline.slack_client = mock_slack

        await pipeline.run()

        mock_slack.say.assert_called()


class TestGroupFailuresByImage(TestCase):
    def test_groups_across_versions(self):
        failures_by_version = {
            '4.18': {
                'img-a': {'failure_count': 3, 'jenkins_url': 'http://j/1', 'build_system': 'konflux'},
            },
            '4.19': {
                'img-a': {'failure_count': 1, 'jenkins_url': 'http://j/2', 'build_system': 'konflux'},
                'img-b': {'failure_count': 2, 'jenkins_url': 'http://j/3', 'build_system': 'brew'},
            },
        }

        result = ImagesHealthPipeline._group_failures_by_image(failures_by_version)

        self.assertEqual(len(result['img-a']), 2)
        self.assertEqual(len(result['img-b']), 1)
        self.assertEqual(result['img-a'][0]['version'], '4.18')
        self.assertEqual(result['img-a'][1]['version'], '4.19')

    def test_empty_input(self):
        result = ImagesHealthPipeline._group_failures_by_image({})
        self.assertEqual(result, {})


class TestRedisFailuresToJiraDict(TestCase):
    def test_converts_version_keyed_to_tuple_keyed(self):
        failures = {
            '4.18': {
                'img-a': {'failure_count': 3, 'jenkins_url': 'http://j/1'},
            },
            '4.19': {
                'img-b': {'failure_count': 1, 'jenkins_url': 'http://j/2'},
            },
        }

        result = ImagesHealthPipeline._redis_failures_to_jira_dict(failures)

        self.assertIn(('img-a', 'openshift-4.18'), result)
        self.assertIn(('img-b', 'openshift-4.19'), result)
        self.assertEqual(result[('img-a', 'openshift-4.18')]['image_name'], 'img-a')
        self.assertEqual(result[('img-a', 'openshift-4.18')]['group'], 'openshift-4.18')


class TestGetMultiRebaseFailures(TestCase):
    def _make_pipeline(self):
        runtime = _make_runtime()
        return _make_pipeline(runtime, versions="4.18")

    def test_filters_single_failures(self):
        """Images with only 1 rebase failure are excluded."""
        pipeline = self._make_pipeline()
        pipeline.rebase_failures = {
            '4.18': {
                'ironic': {'failure_count': 1, 'jenkins_url': ''},
                'hypershift': {'failure_count': 3, 'jenkins_url': 'http://jenkins/job/1'},
            }
        }
        result = pipeline._get_multi_rebase_failures()
        self.assertIn('4.18', result)
        self.assertNotIn('ironic', result['4.18'])
        self.assertIn('hypershift', result['4.18'])

    def test_excludes_versions_with_no_multi_failures(self):
        pipeline = self._make_pipeline()
        pipeline.rebase_failures = {
            '4.18': {'ironic': {'failure_count': 1, 'jenkins_url': ''}},
        }
        result = pipeline._get_multi_rebase_failures()
        self.assertNotIn('4.18', result)

    def test_empty_rebase_failures(self):
        pipeline = self._make_pipeline()
        pipeline.rebase_failures = {}
        self.assertEqual(pipeline._get_multi_rebase_failures(), {})


class TestGetMultiFailureImages(TestCase):
    def _make_pipeline(self):
        runtime = _make_runtime()
        return _make_pipeline(runtime, versions="4.18")

    def test_filters_single_failure(self):
        """Images with latest_success_idx <= 1 are excluded."""
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern('img-a', 'openshift-4.18', ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=1),
            _make_concern('img-b', 'openshift-4.18', ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        result = pipeline._get_multi_failure_images()
        self.assertIn('4.18', result)
        names = [c['image_name'] for c in result['4.18']]
        self.assertNotIn('img-a', names)
        self.assertIn('img-b', names)

    def test_includes_failing_at_least_for(self):
        """FAILING_AT_LEAST_FOR concerns with >1 idx are included."""
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern('img-a', 'openshift-4.18', ConcernCode.FAILING_AT_LEAST_FOR.value, latest_success_idx=5),
        ]
        result = pipeline._get_multi_failure_images()
        self.assertIn('4.18', result)
        self.assertEqual(result['4.18'][0]['image_name'], 'img-a')

    def test_skips_never_built(self):
        pipeline = self._make_pipeline()
        pipeline.report = [
            _make_concern('img-a', 'openshift-4.18', ConcernCode.NEVER_BUILT.value),
        ]
        result = pipeline._get_multi_failure_images()
        self.assertEqual(result, {})


class TestNotifyChaiBot(IsolatedAsyncioTestCase):
    def _make_pipeline(self):
        runtime = _make_runtime()
        return ImagesHealthPipeline(
            runtime=runtime,
            versions="4.18",
            send_to_release_channel=False,
            public_channel="",
            data_path=DATA_PATH,
            data_gitref="",
            image_list="",
            assembly="stream",
            sync_jira=False,
            ping_chai_bot=True,
        )

    async def test_skips_when_no_failures(self):
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock()
        await pipeline.notify_chai_bot('4.18', [], {})
        pipeline.slack_client.say.assert_not_called()

    async def test_notifies_for_rebase_only(self):
        """Rebase failures alone trigger chai-bot notification."""
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock(return_value={'ts': '123'})
        pipeline.slack_client.bind_channel = MagicMock()
        rebase_failures = {
            'ironic': {'failure_count': 3, 'jenkins_url': 'http://jenkins/job/1'},
        }
        await pipeline.notify_chai_bot('4.18', [], rebase_failures)
        pipeline.slack_client.say.assert_called_once()
        msg = pipeline.slack_client.say.call_args[0][0]
        self.assertIn('ironic', msg)
        self.assertIn('rebase failures', msg)

    async def test_prompt_includes_both_build_and_rebase_sections(self):
        """When both failure types present, prompt contains both sections."""
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock(return_value={'ts': '123'})
        pipeline.slack_client.bind_channel = MagicMock()
        concern = _make_concern('console', 'openshift-4.18', 'LATEST_ATTEMPT_FAILED', latest_success_idx=3)
        rebase_failures = {'ironic': {'failure_count': 3, 'jenkins_url': ''}}
        await pipeline.notify_chai_bot('4.18', [concern], rebase_failures)
        msg = pipeline.slack_client.say.call_args[0][0]
        self.assertIn('build failures', msg)
        self.assertIn('rebase failures', msg)
        self.assertIn('console', msg)
        self.assertIn('ironic', msg)

    async def test_prompt_uses_openshift_group_not_okd(self):
        """Prompt references openshift-X.Y group, not okd-X.Y."""
        pipeline = self._make_pipeline()
        pipeline.slack_client.say = AsyncMock(return_value={'ts': '123'})
        pipeline.slack_client.bind_channel = MagicMock()
        concern = _make_concern('console', 'openshift-4.18', 'LATEST_ATTEMPT_FAILED', latest_success_idx=2)
        await pipeline.notify_chai_bot('4.18', [concern], {})
        msg = pipeline.slack_client.say.call_args[0][0]
        self.assertIn('openshift-4.18', msg)
        self.assertNotIn('okd-', msg)

    async def test_prompt_contains_skip_label_and_jenkins_url(self):
        """Prompt includes skip-label suggestion and full Jenkins URL pattern."""
        pipeline = self._make_pipeline()
        concern = _make_concern('console', 'openshift-4.18', 'LATEST_ATTEMPT_FAILED', latest_success_idx=2)
        prompt = pipeline._build_chai_bot_prompt('4.18', [concern], {})
        self.assertIn('art:bot-skip-auto-fix', prompt)
        self.assertIn('art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com', prompt)

    async def test_run_calls_chai_bot_for_multi_failures(self):
        """run() calls notify_chai_bot when images have >1 consecutive failure."""
        pipeline = self._make_pipeline()
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.get_ec_failures = AsyncMock()
        pipeline.get_release_failures = AsyncMock()
        pipeline.notify_chai_bot = AsyncMock()
        pipeline.report = [
            _make_concern('ironic', 'openshift-4.18', ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        pipeline.rebase_failures = {}
        pipeline.scanned_versions = ['4.18']

        await pipeline.run()

        pipeline.notify_chai_bot.assert_awaited_once()
        call_args = pipeline.notify_chai_bot.call_args
        self.assertEqual(call_args[0][0], '4.18')
        self.assertEqual(call_args[0][1][0]['image_name'], 'ironic')

    async def test_run_skips_chai_bot_when_flag_false(self):
        """run() does not call notify_chai_bot when ping_chai_bot=False."""
        pipeline = self._make_pipeline()
        pipeline.ping_chai_bot = False
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.get_ec_failures = AsyncMock()
        pipeline.get_release_failures = AsyncMock()
        pipeline.notify_chai_bot = AsyncMock()
        pipeline.report = [
            _make_concern('ironic', 'openshift-4.18', ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        pipeline.rebase_failures = {}

        await pipeline.run()

        pipeline.notify_chai_bot.assert_not_called()

    async def test_run_contains_prune_exceptions(self):
        """A failing _prune_deleted_counters must not abort subsequent run() steps."""
        pipeline = self._make_pipeline()
        pipeline.get_report = AsyncMock()
        pipeline.get_rebase_failures = AsyncMock()
        pipeline.get_ec_failures = AsyncMock()
        pipeline.get_release_failures = AsyncMock()
        pipeline.notify_chai_bot = AsyncMock()
        pipeline._prune_deleted_counters = AsyncMock(side_effect=RuntimeError("redis boom"))
        pipeline.report = [
            _make_concern('ironic', 'openshift-4.18', ConcernCode.LATEST_ATTEMPT_FAILED.value, latest_success_idx=3),
        ]
        pipeline.rebase_failures = {}
        pipeline.scanned_versions = ['4.18']

        # Must not raise despite the prune failure.
        await pipeline.run()

        # Later steps still ran.
        pipeline._prune_deleted_counters.assert_awaited_once()
        pipeline.notify_chai_bot.assert_awaited_once()


class TestGetAllDefinedImages(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = _make_runtime()

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_loads_with_load_disabled_and_stream_assembly(self, mock_cmd):
        """The 'A' load uses --load-disabled and --assembly stream and returns loaded_ok=True."""
        mock_cmd.return_value = (0, "enabled-img\ndisabled-img\n", "")
        pipeline = _make_pipeline(self.mock_runtime, versions="4.22")

        images, loaded_ok = await pipeline._get_all_defined_images("4.22", "/tmp/doozer-4.22")

        self.assertTrue(loaded_ok)
        self.assertEqual(images, {"enabled-img", "disabled-img"})
        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--load-disabled", cmd)
        self.assertIn("--load-wip", cmd)
        self.assertIn("--group=openshift-4.22", cmd)
        self.assertIn("images:print", cmd)
        # --assembly stream passed as two consecutive tokens
        self.assertIn("--assembly", cmd)
        self.assertEqual(cmd[cmd.index("--assembly") + 1], "stream")
        # --assembly, --load-disabled and --load-wip are doozer GLOBAL options and MUST precede
        # the images:print subcommand, otherwise Click rejects the command.
        self.assertLess(cmd.index("--assembly"), cmd.index("images:print"))
        self.assertLess(cmd.index("--load-disabled"), cmd.index("images:print"))
        self.assertLess(cmd.index("--load-wip"), cmd.index("images:print"))
        # --show-* flags remain subcommand options, after images:print
        self.assertLess(cmd.index("images:print"), cmd.index("--show-base"))

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_load_failure_returns_loaded_ok_false(self, mock_cmd):
        """On a doozer exception, returns (set(), False) and never infers from emptiness."""
        mock_cmd.side_effect = Exception("doozer blew up")
        pipeline = _make_pipeline(self.mock_runtime, versions="4.22")

        images, loaded_ok = await pipeline._get_all_defined_images("4.22", "/tmp/doozer-4.22")

        self.assertEqual(images, set())
        self.assertFalse(loaded_ok)

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_load_failure_log_has_no_sensitive_values(self, mock_cmd):
        """The load-failure warning must not leak the exception text (which embeds data_path/gitref)."""
        sentinel_host = "art-stale-sentinel.invalid"
        sentinel_ref = "sentinel-ref"
        # Simulate a doozer failure whose message embeds the command (host + @gitref).
        mock_cmd.side_effect = Exception(
            f"Command failed: doozer --data-path=https://{sentinel_host}/repo.git "
            f"--group=openshift-4.22@{sentinel_ref} images:print"
        )
        pipeline = _make_pipeline(self.mock_runtime, versions="4.22")
        pipeline.data_path = f"https://{sentinel_host}/repo.git"
        pipeline.data_gitref = sentinel_ref

        images, loaded_ok = await pipeline._get_all_defined_images("4.22", "/tmp/doozer-4.22")

        self.assertEqual(images, set())
        self.assertFalse(loaded_ok)
        blob = _collect_logged(self.mock_runtime.logger)
        self.assertNotIn(sentinel_host, blob)
        self.assertNotIn(sentinel_ref, blob)
        # The exception class name is still logged for diagnosis.
        self.assertIn("Exception", blob)

    @patch("pyartcd.pipelines.images_health.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_cached_after_success(self, mock_cmd):
        """A successful load is cached; doozer is not re-invoked."""
        mock_cmd.return_value = (0, "enabled-img\n", "")
        pipeline = _make_pipeline(self.mock_runtime, versions="4.22")

        await pipeline._get_all_defined_images("4.22", "/tmp/doozer-4.22")
        await pipeline._get_all_defined_images("4.22", "/tmp/doozer-4.22")

        mock_cmd.assert_awaited_once()


class TestPruneDeletedCounters(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = _make_runtime()

    def _pipeline(self, image_list="", delete_stale_counters=False):
        pipeline = _make_pipeline(self.mock_runtime, versions="4.22", image_list=image_list)
        pipeline.scanned_versions = ["4.22"]
        pipeline.delete_stale_counters = delete_stale_counters
        return pipeline

    @staticmethod
    def _only(counter_type_match, build_system_match, failures):
        """Build a get_counter_failures side_effect returning failures only for one family."""

        def side_effect(counter_type, group=None, build_system="konflux", logger=None):
            if counter_type == counter_type_match and build_system == build_system_match:
                return dict(failures)
            return {}

        return side_effect

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_deleted_image_pruned(self, mock_get_failures, mock_reset):
        """An image present in Redis but absent from A has its counter branch deleted."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"deleted-img": {"failure_count": 3}})

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_awaited_once_with("count:build-failure:konflux:openshift-4.22:deleted-img")

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_disabled_image_not_pruned(self, mock_get_failures, mock_reset):
        """A disabled image (file present, so in A) keeps its counter."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=({"disabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"disabled-img": {"failure_count": 2}})

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_enabled_failing_image_not_pruned(self, mock_get_failures, mock_reset):
        """An enabled failing image (in A) is reported, not pruned."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"enabled-img": {"failure_count": 5}})

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_a_load_failure_deletes_nothing(self, mock_get_failures, mock_reset):
        """When the A load fails (loaded_ok False), nothing is deleted and no counters are fetched."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=(set(), False))

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        mock_get_failures.assert_not_awaited()

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_image_list_restricted_deletes_nothing(self, mock_get_failures, mock_reset):
        """With --image-list set, pruning is skipped entirely (A is never even loaded)."""
        pipeline = self._pipeline(image_list="ironic", delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        mock_get_failures.assert_not_awaited()
        pipeline._get_all_defined_images.assert_not_awaited()

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_dry_run_logs_but_does_not_delete(self, mock_get_failures, mock_reset):
        """Default (dry-run) logs a WOULD-delete line and does NOT call reset_fail_counter."""
        pipeline = self._pipeline(delete_stale_counters=False)
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"deleted-img": {"failure_count": 3}})

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        warning_messages = [str(c.args[0]) for c in self.mock_runtime.logger.warning.call_args_list]
        self.assertTrue(any("WOULD delete" in m for m in warning_messages))

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_rebase_covers_brew_and_konflux(self, mock_get_failures, mock_reset):
        """Rebase pruning deletes both brew and konflux counter branches for a deleted image."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))

        def side_effect(counter_type, group=None, build_system="konflux", logger=None):
            if counter_type == "rebase-failure":
                return {"deleted-img": {"failure_count": 4}}
            return {}

        mock_get_failures.side_effect = side_effect

        await pipeline._prune_deleted_counters("4.22")

        branches = {c.args[0] for c in mock_reset.await_args_list}
        self.assertIn("count:rebase-failure:brew:openshift-4.22:deleted-img", branches)
        self.assertIn("count:rebase-failure:konflux:openshift-4.22:deleted-img", branches)

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_unscanned_version_deletes_nothing(self, mock_get_failures, mock_reset):
        """A version not in scanned_versions (frozen/not build-permitted) is skipped."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline.scanned_versions = []
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        mock_get_failures.assert_not_awaited()

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_empty_a_load_deletes_nothing(self, mock_get_failures, mock_reset):
        """A successful but EMPTY A load (set(), True) is treated as a failed load: delete nothing."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline._get_all_defined_images = AsyncMock(return_value=(set(), True))

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        mock_get_failures.assert_not_awaited()

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_non_default_data_path_skips_deletion(self, mock_get_failures, mock_reset):
        """With a non-default data_path, deletion is forced to dry-run even when enabled."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline.data_path = "https://github.com/someone/ocp-build-data-fork"
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"deleted-img": {"failure_count": 3}})

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        warning_messages = [str(c.args[0]) for c in self.mock_runtime.logger.warning.call_args_list]
        self.assertTrue(any("WOULD delete" in m for m in warning_messages))

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_data_gitref_skips_deletion(self, mock_get_failures, mock_reset):
        """With a pinned data_gitref, deletion is forced to dry-run even when enabled."""
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline.data_gitref = "some-branch"
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"deleted-img": {"failure_count": 3}})

        await pipeline._prune_deleted_counters("4.22")

        mock_reset.assert_not_awaited()
        warning_messages = [str(c.args[0]) for c in self.mock_runtime.logger.warning.call_args_list]
        self.assertTrue(any("WOULD delete" in m for m in warning_messages))

    @patch("pyartcd.pipelines.images_health.util.reset_fail_counter", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.images_health.util.get_counter_failures", new_callable=AsyncMock)
    async def test_non_default_data_source_log_has_no_sensitive_values(self, mock_get_failures, mock_reset):
        """The non-default-data-source skip log must not leak raw data_path / data_gitref values."""
        # De-credentialed sentinels (no embedded credentials) so the scanner is not tripped.
        sentinel_host = "art-stale-sentinel.invalid"
        sentinel_url = f"https://{sentinel_host}/repo.git"
        sentinel_ref = "sentinel-ref"
        pipeline = self._pipeline(delete_stale_counters=True)
        pipeline.data_path = sentinel_url
        pipeline.data_gitref = sentinel_ref
        pipeline._get_all_defined_images = AsyncMock(return_value=({"enabled-img"}, True))
        mock_get_failures.side_effect = self._only("build-failure", "konflux", {"deleted-img": {"failure_count": 3}})

        await pipeline._prune_deleted_counters("4.22")

        # Deletion is suppressed for a non-default source.
        mock_reset.assert_not_awaited()
        # Neither the sentinel host nor the sentinel ref appears in ANY captured log call.
        blob = _collect_logged(self.mock_runtime.logger)
        self.assertNotIn(sentinel_host, blob)
        self.assertNotIn(sentinel_ref, blob)
