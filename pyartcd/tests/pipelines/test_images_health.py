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


class TestNotifyPublicChannel(IsolatedAsyncioTestCase):
    def setUp(self):
        self.mock_runtime = _make_runtime()

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
        mock_slack_client.say.return_value = {"ts": ""}

        await pipeline.notify_public_channel()

        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 2)
        first_call_args = calls[0][0][0]
        self.assertIn(":alert:", first_call_args)
        self.assertIn("build failures", first_call_args)

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
        mock_slack_client.say.return_value = {"ts": ""}

        await pipeline.notify_public_channel()

        calls = mock_slack_client.say.call_args_list
        # Summary + 4 thread messages (one per failure type)
        self.assertEqual(len(calls), 5)
        summary = calls[0][0][0]
        self.assertIn("build failures", summary)
        self.assertIn("EC verification failures", summary)
        self.assertIn("release to authz failures", summary)
        self.assertIn("rebase failures", summary)
        # Thread messages: section header with image name, then group bullets below
        build_thread = calls[1][0][0]
        self.assertIn("Build Failures (1 image)", build_thread)
        self.assertIn("`build-fail`:", build_thread)
        ec_thread = calls[2][0][0]
        self.assertIn("EC Verification Failures (1 image)", ec_thread)
        self.assertIn("`ec-fail-image`:", ec_thread)

    async def test_binds_to_configured_channel(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#my-custom-channel")
        pipeline.report = []

        await pipeline.notify_public_channel()

        mock_slack_client.bind_channel.assert_called_once_with("#my-custom-channel")

    async def test_empty_report(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
        pipeline.report = []

        await pipeline.notify_public_channel()

        mock_slack_client.say.assert_called_once_with(
            ":white_check_mark: All images are healthy for all monitored releases"
        )

    async def test_only_ec_failures(self):
        mock_slack_client = self.mock_runtime.new_slack_client.return_value
        pipeline = _make_pipeline(self.mock_runtime, public_channel="#forum-ocp-art")
        pipeline.report = []
        pipeline.ec_failures = {
            '4.22': {'image-a': {'failure_count': 5, 'jenkins_url': '', 'pipeline_url': 'http://ec/1'}},
        }
        mock_slack_client.say.return_value = {"ts": ""}

        await pipeline.notify_public_channel()

        calls = mock_slack_client.say.call_args_list
        self.assertEqual(len(calls), 2)
        summary = calls[0][0][0]
        self.assertIn("EC verification failures", summary)
        self.assertNotIn("build failures", summary)


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
        self.assertIn("1 image failed EC verification", summary)
        self.assertIn("1 image failed to be released to authz", summary)

        thread_detail = calls[1][0][0]
        self.assertIn("Build Failures (2)", thread_detail)
        self.assertIn("EC Verification Failures (1)", thread_detail)
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

    def test_creates_ticket_for_ec_failure(self):
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

        ec_call = next(c for c in mock_jira.create_issue.call_args_list if "EC verification failure" in c[1]["summary"])
        self.assertIn("art:image-ec-failure", ec_call[1]["labels"])
        self.assertIn("art:package:ec-image", ec_call[1]["labels"])
        self.assertIn("art:fail-count:4", ec_call[1]["labels"])
        self.assertIn("Pipeline:", ec_call[1]["description"])

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
