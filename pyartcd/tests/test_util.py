import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd import util


class TestUtil(IsolatedAsyncioTestCase):
    def test_isolate_el_version_in_release(self):
        # Existing .elN patterns
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.4.9.99.el7'), 7)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.4.9.el7'), 7)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.art12398.el199'), 199)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.art12398'), None)
        self.assertEqual(util.isolate_el_version_in_release('1.2.3-y.p.p1.assembly.4.7.e.8'), None)
        # Floating image tags with -rhelN suffix (delegated to artcommonlib)
        self.assertEqual(util.isolate_el_version_in_release('golang-builder-v1.22-rhel9'), 9)
        self.assertEqual(util.isolate_el_version_in_release('golang-builder-v1.22-rhel8'), 8)

    def test_isolate_el_version_in_branch(self):
        self.assertEqual(util.isolate_el_version_in_branch('rhaos-4.9-rhel-7-candidate'), 7)
        self.assertEqual(util.isolate_el_version_in_branch('rhaos-4.9-rhel-7-hotfix'), 7)
        self.assertEqual(util.isolate_el_version_in_branch('rhaos-4.9-rhel-7'), 7)
        self.assertEqual(util.isolate_el_version_in_branch('rhaos-4.9-rhel-777'), 777)
        self.assertEqual(util.isolate_el_version_in_branch('rhaos-4.9'), None)

    def test_nightlies_with_pullspecs(self):
        nightly_tags = [
            '4.14.0-0.nightly-arm64-2023-09-15-082316',
            '4.14.0-0.nightly-ppc64le-2023-09-15-125921',
            '4.14.0-0.nightly-s390x-2023-09-15-114441',
            '4.14.0-0.nightly-2023-09-15-055234',
        ]

        expected = {
            'aarch64': 'registry.ci.openshift.org/ocp-arm64/release-arm64:4.14.0-0.nightly-arm64-2023-09-15-082316',
            'ppc64le': 'registry.ci.openshift.org/ocp-ppc64le/release-ppc64le:4.14.0-0.nightly-ppc64le-2023-09-15-125921',
            's390x': 'registry.ci.openshift.org/ocp-s390x/release-s390x:4.14.0-0.nightly-s390x-2023-09-15-114441',
            'x86_64': 'registry.ci.openshift.org/ocp/release:4.14.0-0.nightly-2023-09-15-055234',
        }
        self.assertEqual(util.nightlies_with_pullspecs(nightly_tags), expected)

    @patch("tempfile.mkdtemp")
    @patch("shutil.rmtree")
    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_load_group_config(self, cmd_gather_async: AsyncMock, *_):
        group_config_content = """
        key: "value"
        """
        cmd_gather_async.return_value = (0, group_config_content, "")
        actual = await util.load_group_config("openshift-4.9", "art0001")
        self.assertEqual(actual["key"], "value")

    def test_dockerfile_url_for(self):
        # HTTPS url
        url = util.dockerfile_url_for(
            url='https://github.com/openshift/ironic-image',
            branch='release-4.13',
            sub_path='scripts',
        )
        self.assertEqual(url, 'https///github.com/openshift/ironic-image/blob/release-4.13/scripts')

        # Empty subpath
        url = util.dockerfile_url_for(
            url='https://github.com/openshift/ironic-image',
            branch='release-4.13',
            sub_path='',
        )
        self.assertEqual(url, 'https///github.com/openshift/ironic-image/blob/release-4.13/')

        # Empty url
        url = util.dockerfile_url_for(
            url='',
            branch='release-4.13',
            sub_path='',
        )
        self.assertEqual(url, '')

        # Empty branch
        url = util.dockerfile_url_for(
            url='https://github.com/openshift/ironic-image',
            branch='',
            sub_path='scripts',
        )
        self.assertEqual(url, '')

        # SSH remote
        url = util.dockerfile_url_for(
            url='git@github.com:openshift/ironic-image.git',
            branch='release-4.13',
            sub_path='scripts',
        )
        self.assertEqual(url, 'https///github.com/openshift/ironic-image/blob/release-4.13/scripts')

        # SSH remote, empty subpath
        url = util.dockerfile_url_for(
            url='git@github.com:openshift/ironic-image.git',
            branch='release-4.13',
            sub_path='',
        )
        self.assertEqual(url, 'https///github.com/openshift/ironic-image/blob/release-4.13/')

    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_get_freeze_automation(self, cmd_gather_async: AsyncMock):
        cmd_gather_async.return_value = (0, '', '')

        await util.get_freeze_automation(
            group='openshift-4.15',
        )
        cmd_gather_async.assert_awaited_once_with(
            [
                'doozer',
                '',
                '--assembly=stream',
                '--data-path=https://github.com/openshift-eng/ocp-build-data',
                '--group=openshift-4.15',
                'config:read-group',
                '--default=no',
                'freeze_automation',
            ]
        )

        cmd_gather_async.reset_mock()
        await util.get_freeze_automation(
            group='openshift-4.15',
            doozer_data_path='https://github.com/random-fork/ocp-build-data',
            doozer_working='doozer_working',
            doozer_data_gitref='random-branch',
        )
        cmd_gather_async.assert_awaited_once_with(
            [
                'doozer',
                '--working-dir=doozer_working',
                '--assembly=stream',
                '--data-path=https://github.com/random-fork/ocp-build-data',
                '--group=openshift-4.15@random-branch',
                'config:read-group',
                '--default=no',
                'freeze_automation',
            ]
        )

    @patch("pyartcd.util.get_weekday")
    @patch("pyartcd.util.is_manual_build")
    @patch("pyartcd.util.get_freeze_automation")
    async def test_is_build_permitted(self, get_freeze_automation_mock: AsyncMock, is_manual_build_mock, weekday_mock):
        # Automation is frozen
        get_freeze_automation_mock.return_value = 'yes'
        res = await util.is_build_permitted(version='4.15')
        self.assertFalse(res)

        get_freeze_automation_mock.return_value = 'True'
        res = await util.is_build_permitted(version='4.15')
        self.assertFalse(res)

        # Scheduled automation is frozen, scheduled build
        get_freeze_automation_mock.return_value = 'scheduled'
        is_manual_build_mock.return_value = False
        res = await util.is_build_permitted(version='4.15')
        self.assertFalse(res)

        # Scheduled automation is frozen, manual build
        is_manual_build_mock.return_value = True
        res = await util.is_build_permitted(version='4.15')
        self.assertTrue(res)

        # Automation frozen during weekdays; scheduled builds
        get_freeze_automation_mock.return_value = 'weekdays'
        is_manual_build_mock.return_value = False
        weekday_mock.return_value = 'Sunday'
        res = await util.is_build_permitted(version='4.15')
        self.assertTrue(res)
        weekday_mock.return_value = 'Monday'
        res = await util.is_build_permitted(version='4.15')
        self.assertFalse(res)

        # Unknown value for 'freeze_automation'
        get_freeze_automation_mock.return_value = 'unknown'
        res = await util.is_build_permitted(version='4.15')
        self.assertTrue(res)

    @patch("pyartcd.util.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_is_okd_version_enabled(self, cmd_gather_async: AsyncMock):
        cmd_gather_async.return_value = (0, 'True\n', '')

        base_cmd = ['doozer', '--variant=okd', '--group=openshift-4.21']
        self.assertTrue(await util.is_okd_version_enabled(base_cmd))
        cmd_gather_async.assert_awaited_once()
        cmd = cmd_gather_async.await_args.args[0]
        self.assertIn('--variant=okd', cmd)
        self.assertEqual(cmd[-3:], ['config:read-group', 'enabled', '--default=False'])

        cmd_gather_async.return_value = (0, 'False\n', '')
        self.assertFalse(await util.is_okd_version_enabled(base_cmd))

    @patch("pyartcd.util.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_get_okd_enabled_versions(self, cmd_gather_async: AsyncMock):
        cmd_gather_async.side_effect = [(0, 'True\n', ''), (0, 'False\n', '')]

        enabled = await util.get_okd_enabled_versions(
            working_dir='/tmp/wd',
            candidates=['4.21', '4.23'],
        )
        self.assertEqual(enabled, ['4.21'])
        self.assertEqual(cmd_gather_async.await_count, 2)

    @patch("pyartcd.util.load_group_config")
    async def test_get_signing_mode(self, load_group_config_mock: AsyncMock):
        group_config = {'software_lifecycle': {'phase': 'release'}}
        signing_mode = await util.get_signing_mode(group_config=group_config)
        self.assertEqual(signing_mode, 'signed')

        group_config = {'software_lifecycle': {'phase': 'eol'}}
        signing_mode = await util.get_signing_mode(group_config=group_config)
        self.assertEqual(signing_mode, 'signed')

        group_config = {'software_lifecycle': {'phase': 'pre-release'}}
        signing_mode = await util.get_signing_mode(group_config=group_config)
        self.assertEqual(signing_mode, 'unsigned')

        load_group_config_mock.return_value = {'software_lifecycle': {'phase': 'release'}}
        group = 'bogus'
        assembly = 'bogus'

        with self.assertRaises(AssertionError) as _:
            await util.get_signing_mode(group_config=None)
        with self.assertRaises(AssertionError) as _:
            await util.get_signing_mode(group=group, group_config=None)
        with self.assertRaises(AssertionError) as _:
            await util.get_signing_mode(assembly=assembly, group_config=None)

        signing_mode = await util.get_signing_mode(group, assembly, None)
        self.assertEqual(signing_mode, 'signed')

    def test_get_rpm_if_pinned_directly(self):
        rpms = {'el8': 'foo-1.0.0-1.el8', 'el9': 'foo-1.0.0-1.el9'}
        releases_config = {
            'releases': {
                '4.11.1': {
                    'assembly': {
                        'basis': {'assembly': '4.11.0'},
                    },
                },
                '4.11.0': {
                    'assembly': {
                        'members': {
                            'rpms': [{'distgit_key': 'foo', 'metadata': {'is': rpms}}],
                        },
                    },
                },
            },
        }
        self.assertEqual(util.get_rpm_if_pinned_directly(releases_config, '4.11.0', 'foo'), rpms)
        self.assertEqual(util.get_rpm_if_pinned_directly(releases_config, '4.11.1', 'foo'), dict())
        self.assertEqual(util.get_rpm_if_pinned_directly(releases_config, '4.11.0', 'bar'), dict())

    @patch("artcommonlib.redis.set_value", new_callable=AsyncMock)
    @patch("artcommonlib.redis.call", new_callable=AsyncMock)
    async def test_increment_fail_counter_new(self, mock_call, mock_set):
        mock_call.return_value = 1
        await util.increment_fail_counter('count:test:branch', url='http://j/1', nvr='test-1.0-1')
        mock_call.assert_called_once_with('incr', 'count:test:branch:failure')
        mock_set.assert_any_call(key='count:test:branch:url', value='http://j/1')
        mock_set.assert_any_call(key='count:test:branch:nvr', value='test-1.0-1')

    @patch("artcommonlib.redis.call", new_callable=AsyncMock)
    async def test_increment_fail_counter_existing(self, mock_call):
        mock_call.return_value = 6
        await util.increment_fail_counter('count:test:branch')
        mock_call.assert_called_once_with('incr', 'count:test:branch:failure')

    @patch("artcommonlib.redis.delete_keys_by_pattern", new_callable=AsyncMock)
    async def test_reset_fail_counter(self, mock_delete):
        await util.reset_fail_counter('count:test:branch')
        mock_delete.assert_called_once_with('count:test:branch:*')

    def test_get_failed_images_for_counter_updates_preserves_mixed_failure_behavior(self):
        """Mixed infrastructure failures preserve the existing counter behavior."""
        failed_images = ["real-failure", "infrastructure-failure", "parent-failure"]
        failed_entries = {
            "real-failure": {"task_id": "plr-1", "message": "Build failed"},
            "infrastructure-failure": {"task_id": "n/a", "message": "Infrastructure failure"},
            "parent-failure": {
                "task_id": "n/a",
                "message": "The following parent images failed to build: parent-image",
            },
        }

        counter_images = util.get_failed_images_for_counter_updates(failed_images, failed_entries)

        self.assertEqual(counter_images, ["real-failure", "infrastructure-failure"])

    def test_get_failed_images_for_counter_updates_skips_all_unattempted_failures(self):
        """All infrastructure and parent-dependent failures produce no counter updates."""
        failed_images = ["infrastructure-failure", "parent-failure"]
        failed_entries = {
            "infrastructure-failure": {"task_id": "n/a", "message": "Infrastructure failure"},
            "parent-failure": {"task_id": "n/a", "message": "parent images failed to build"},
        }

        counter_images = util.get_failed_images_for_counter_updates(failed_images, failed_entries)

        self.assertEqual(counter_images, [])

    def test_get_no_attempted_builds_warning(self):
        """The no-attempt warning includes the group, failure count, and Jenkins URL."""
        warning = util.get_no_attempted_builds_warning("openshift-4.18", 2, "https://jenkins/job/1")

        self.assertEqual(
            warning,
            "No builds were actually attempted for openshift-4.18: all 2 failures "
            "have task_id=n/a (infrastructure failure) or are parent-dependency failures. "
            "Skipping individual image counter updates. Jenkins job: https://jenkins/job/1",
        )

    def test_categorize_failed_images_by_outcome(self):
        """Failure outcomes are mapped to build, ITS, and release categories."""
        failed_images = ["build-failure", "its-failure", "release-failure", "unknown-failure"]
        failed_entries = {
            "build-failure": {"outcome": "build_error"},
            "its-failure": {"outcome": "its_error"},
            "release-failure": {"outcome": "release_error"},
            "unknown-failure": {"outcome": "unexpected_error"},
        }

        categories = util.categorize_failed_images(failed_images, failed_entries)

        self.assertEqual(categories.build, ["build-failure", "unknown-failure"])
        self.assertEqual(categories.its, ["its-failure"])
        self.assertEqual(categories.release, ["release-failure"])

    @patch("pyartcd.util.increment_fail_counter", new_callable=AsyncMock)
    async def test_increment_failed_image_counters(self, mock_increment):
        """Failure categories map to their counter and pipeline metadata."""
        categories = util.FailedImageCategories(
            build=["build-failure"],
            its=["its-failure"],
            release=["release-failure"],
        )
        failed_entries = {
            "build-failure": {
                "nvrs": "build-1.0-1",
                "build_pipeline_url": "https://build/1",
            },
            "its-failure": {
                "nvrs": "its-1.0-1",
                "ec_pipeline_url": "https://its/1",
            },
            "release-failure": {
                "nvrs": "release-1.0-1",
                "release_pipeline": "https://release/1",
            },
        }

        await util.increment_failed_image_counters(
            group="openshift-4.18",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
            failure_categories=categories,
            failed_entries=failed_entries,
            increment_counter=mock_increment,
        )

        self.assertEqual(
            [call.args[0] for call in mock_increment.await_args_list],
            [
                "count:build-failure:konflux:openshift-4.18:build-failure",
                "count:ec-failure:konflux:openshift-4.18:its-failure",
                "count:release-failure:konflux:openshift-4.18:release-failure",
            ],
        )
        mock_increment.assert_any_await(
            "count:ec-failure:konflux:openshift-4.18:its-failure",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
            nvr="its-1.0-1",
            pipeline_url="https://its/1",
        )

    async def test_update_build_fail_counters(self):
        """Build counters reset successes and update only meaningful failures."""
        mock_reset = AsyncMock()
        mock_increment = AsyncMock()
        mock_logger = MagicMock()
        failed_images = ["build-failure", "its-failure", "release-failure", "parent-failure"]
        failed_entries = {
            "build-failure": {
                "task_id": "build-task",
                "outcome": "build_error",
                "nvrs": "build-1.0-1",
                "build_pipeline_url": "https://build/1",
            },
            "its-failure": {
                "task_id": "its-task",
                "outcome": "its_error",
                "nvrs": "its-1.0-1",
                "ec_pipeline_url": "https://its/1",
            },
            "release-failure": {
                "task_id": "release-task",
                "outcome": "release_error",
                "nvrs": "release-1.0-1",
                "release_pipeline": "https://release/1",
            },
            "parent-failure": {
                "task_id": "n/a",
                "message": "parent images failed to build",
            },
        }

        await util.update_build_fail_counters(
            group="openshift-4.18",
            assembly="stream",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
            built_images=["successful-image"],
            failed_images=failed_images,
            failed_entries=failed_entries,
            reset_counter=mock_reset,
            increment_counter=mock_increment,
            logger=mock_logger,
        )

        self.assertEqual(
            {call.args[0] for call in mock_reset.await_args_list},
            {
                "count:build-failure:konflux:openshift-4.18:successful-image",
                "count:ec-failure:konflux:openshift-4.18:successful-image",
                "count:release-failure:konflux:openshift-4.18:successful-image",
            },
        )
        self.assertEqual(
            [call.args[0] for call in mock_increment.await_args_list],
            [
                "count:build-failure:konflux:openshift-4.18:build-failure",
                "count:ec-failure:konflux:openshift-4.18:its-failure",
                "count:release-failure:konflux:openshift-4.18:release-failure",
            ],
        )
        mock_logger.info.assert_called_once()

    async def test_update_rebase_fail_counters(self):
        """Rebase counters reset successful images and increment direct failures only."""
        mock_reset = AsyncMock()
        mock_increment = AsyncMock()

        await util.update_rebase_fail_counters(
            group="openshift-4.18",
            assembly="stream",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
            image_build_strategy="all",
            group_images=["successful-image", "failed-image", "skipped-image"],
            requested_images=[],
            images_excluded=[],
            state_path=None,
            failed_images=["failed-image"],
            skipped_due_to_parent=["skipped-image"],
            reset_counter=mock_reset,
            increment_counter=mock_increment,
        )

        mock_reset.assert_awaited_once_with("count:rebase-failure:konflux:openshift-4.18:successful-image")
        mock_increment.assert_awaited_once_with(
            "count:rebase-failure:konflux:openshift-4.18:failed-image",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
        )

    async def test_update_rebase_fail_counters_skips_non_stream_assemblies(self):
        """Rebase counters are not updated outside the stream assembly."""
        mock_reset = AsyncMock()
        mock_increment = AsyncMock()

        await util.update_rebase_fail_counters(
            group="openshift-4.18",
            assembly="4.18.1",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
            image_build_strategy="all",
            group_images=["successful-image"],
            requested_images=[],
            images_excluded=[],
            state_path=None,
            failed_images=["failed-image"],
            skipped_due_to_parent=None,
            reset_counter=mock_reset,
            increment_counter=mock_increment,
        )

        mock_reset.assert_not_awaited()
        mock_increment.assert_not_awaited()

    async def test_update_rebase_fail_counters_only_strategy_uses_state(self):
        """The ONLY strategy resets successful images recorded in rebase state."""
        mock_reset = AsyncMock()
        mock_increment = AsyncMock()

        with tempfile.TemporaryDirectory() as tempdir:
            state_path = Path(tempdir, "state.yaml")
            state_path.write_text("images:konflux:rebase:\n  images:\n    parent-image:\n      status: success\n")
            await util.update_rebase_fail_counters(
                group="openshift-4.18",
                assembly="stream",
                build_variant="ocp",
                jenkins_url="https://jenkins/1",
                image_build_strategy="only",
                group_images=["requested-image"],
                requested_images=["requested-image"],
                images_excluded=[],
                state_path=state_path,
                failed_images=["failed-image"],
                skipped_due_to_parent=None,
                reset_counter=mock_reset,
                increment_counter=mock_increment,
            )

        mock_reset.assert_awaited_once_with("count:rebase-failure:konflux:openshift-4.18:parent-image")

    async def test_update_rebase_fail_counters_except_strategy_uses_exclusions(self):
        """The EXCEPT strategy does not reset explicitly excluded images."""
        mock_reset = AsyncMock()
        mock_increment = AsyncMock()

        await util.update_rebase_fail_counters(
            group="openshift-4.18",
            assembly="stream",
            build_variant="ocp",
            jenkins_url="https://jenkins/1",
            image_build_strategy="except",
            group_images=["healthy-image", "excluded-image"],
            requested_images=[],
            images_excluded=["excluded-image"],
            state_path=None,
            failed_images=[],
            skipped_due_to_parent=None,
            reset_counter=mock_reset,
            increment_counter=mock_increment,
        )

        mock_reset.assert_awaited_once_with("count:rebase-failure:konflux:openshift-4.18:healthy-image")

    @patch("artcommonlib.redis.get_value", new_callable=AsyncMock)
    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_build(self, mock_get_keys, mock_get_value):
        # Mock get_keys to handle both the initial failure key search and metadata discovery
        def mock_get_keys_side_effect(pattern):
            if pattern.endswith(':*:failure'):
                # Initial search for failure keys
                return [
                    'count:build-failure:konflux:openshift-4.21:ironic:failure',
                    'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:failure',
                ]
            elif 'ironic' in pattern:
                # Metadata discovery for ironic
                return [
                    'count:build-failure:konflux:openshift-4.21:ironic:failure',
                    'count:build-failure:konflux:openshift-4.21:ironic:jenkins_url',
                    'count:build-failure:konflux:openshift-4.21:ironic:nvr',
                ]
            elif 'ovn-kubernetes' in pattern:
                # Metadata discovery for ovn-kubernetes
                return [
                    'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:failure',
                    'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:jenkins_url',
                    'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:nvr',
                ]
            return []

        mock_get_keys.side_effect = mock_get_keys_side_effect
        mock_get_value.side_effect = lambda key: {
            'count:build-failure:konflux:openshift-4.21:ironic:failure': '5',
            'count:build-failure:konflux:openshift-4.21:ironic:jenkins_url': 'http://j/1',
            'count:build-failure:konflux:openshift-4.21:ironic:nvr': 'ironic-1.0-1',
            'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:failure': '2',
            'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:jenkins_url': '',
            'count:build-failure:konflux:openshift-4.21:ovn-kubernetes:nvr': None,
        }.get(key)
        result = await util.get_counter_failures('build-failure', 'openshift-4.21')
        self.assertEqual(result['ironic']['failure_count'], 5)
        self.assertEqual(result['ironic']['jenkins_url'], 'http://j/1')
        self.assertEqual(result['ironic']['nvr'], 'ironic-1.0-1')
        self.assertEqual(result['ovn-kubernetes']['failure_count'], 2)

    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_empty(self, mock_get_keys):
        mock_get_keys.return_value = []
        result = await util.get_counter_failures('build-failure', 'openshift-4.21')
        self.assertEqual(result, {})

    @patch("artcommonlib.redis.get_value", new_callable=AsyncMock)
    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_filters_by_build_variant(self, mock_get_keys, mock_get_value):
        """Only failure records matching the requested build variant are returned."""

        def mock_get_keys_side_effect(pattern):
            if pattern.endswith(":*:failure"):
                return [
                    "count:build-failure:konflux:oadp-1.4:shared-image:failure",
                    "count:build-failure:konflux:logging-6.6:shared-image:failure",
                ]
            if "oadp-1.4:shared-image" in pattern:
                return [
                    "count:build-failure:konflux:oadp-1.4:shared-image:failure",
                    "count:build-failure:konflux:oadp-1.4:shared-image:build_variant",
                ]
            if "logging-6.6:shared-image" in pattern:
                return [
                    "count:build-failure:konflux:logging-6.6:shared-image:failure",
                    "count:build-failure:konflux:logging-6.6:shared-image:build_variant",
                ]
            return []

        mock_get_keys.side_effect = mock_get_keys_side_effect
        mock_get_value.side_effect = lambda key: {
            "count:build-failure:konflux:oadp-1.4:shared-image:failure": "3",
            "count:build-failure:konflux:oadp-1.4:shared-image:build_variant": "oadp",
            "count:build-failure:konflux:logging-6.6:shared-image:failure": "5",
            "count:build-failure:konflux:logging-6.6:shared-image:build_variant": "openshift-logging",
        }.get(key)

        result = await util.get_counter_failures("build-failure", "*", build_variant="oadp")

        self.assertEqual(set(result), {"shared-image"})
        self.assertEqual(result["shared-image"]["build_variant"], "oadp")

    @patch("artcommonlib.redis.get_value", new_callable=AsyncMock)
    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_rebase_failures_filters_by_build_variant(self, mock_get_keys, mock_get_value):
        """Rebase failure queries support the same build variant filter as other counters."""

        def mock_get_keys_side_effect(pattern):
            if pattern.endswith(":*:failure"):
                return [
                    "count:rebase-failure:konflux:openshift-4.21:shared-image:failure",
                    "count:rebase-failure:konflux:okd-4.21:shared-image:failure",
                ]
            if "openshift-4.21:shared-image" in pattern:
                return [
                    "count:rebase-failure:konflux:openshift-4.21:shared-image:failure",
                    "count:rebase-failure:konflux:openshift-4.21:shared-image:build_variant",
                ]
            if "okd-4.21:shared-image" in pattern:
                return [
                    "count:rebase-failure:konflux:okd-4.21:shared-image:failure",
                    "count:rebase-failure:konflux:okd-4.21:shared-image:build_variant",
                ]
            return []

        mock_get_keys.side_effect = mock_get_keys_side_effect
        mock_get_value.side_effect = lambda key: {
            "count:rebase-failure:konflux:openshift-4.21:shared-image:failure": "3",
            "count:rebase-failure:konflux:openshift-4.21:shared-image:build_variant": "ocp",
            "count:rebase-failure:konflux:okd-4.21:shared-image:failure": "2",
            "count:rebase-failure:konflux:okd-4.21:shared-image:build_variant": "okd",
        }.get(key)

        result = await util.get_rebase_failures("*", ["rebase-failure"], ["konflux"], build_variant="okd")

        self.assertEqual(set(result), {"shared-image"})
        self.assertEqual(result["shared-image"]["failure_count"], 2)
        self.assertEqual(result["shared-image"]["build_variant"], "okd")

    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_redis_error(self, mock_get_keys):
        mock_get_keys.side_effect = Exception("Redis connection refused")
        result = await util.get_counter_failures('build-failure', 'openshift-4.21', logger=MagicMock())
        self.assertEqual(result, {})

    @patch("artcommonlib.redis.get_value", new_callable=AsyncMock)
    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_ec(self, mock_get_keys, mock_get_value):
        def mock_get_keys_side_effect(pattern):
            if pattern.endswith(':*:failure'):
                return ['count:ec-failure:konflux:openshift-4.21:ironic:failure']
            elif 'ironic' in pattern:
                return [
                    'count:ec-failure:konflux:openshift-4.21:ironic:failure',
                    'count:ec-failure:konflux:openshift-4.21:ironic:jenkins_url',
                    'count:ec-failure:konflux:openshift-4.21:ironic:pipeline_url',
                ]
            return []

        mock_get_keys.side_effect = mock_get_keys_side_effect
        mock_get_value.side_effect = lambda key: {
            'count:ec-failure:konflux:openshift-4.21:ironic:failure': '3',
            'count:ec-failure:konflux:openshift-4.21:ironic:jenkins_url': 'http://j/1',
            'count:ec-failure:konflux:openshift-4.21:ironic:pipeline_url': 'http://its/plr/1',
        }.get(key)
        result = await util.get_counter_failures('ec-failure', 'openshift-4.21')
        self.assertEqual(result['ironic']['failure_count'], 3)
        self.assertEqual(result['ironic']['jenkins_url'], 'http://j/1')
        self.assertEqual(result['ironic']['pipeline_url'], 'http://its/plr/1')

    @patch("artcommonlib.redis.get_value", new_callable=AsyncMock)
    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_release(self, mock_get_keys, mock_get_value):
        def mock_get_keys_side_effect(pattern):
            if pattern.endswith(':*:failure'):
                return ['count:release-failure:konflux:openshift-4.21:ose-base:failure']
            elif 'ose-base' in pattern:
                return [
                    'count:release-failure:konflux:openshift-4.21:ose-base:failure',
                    'count:release-failure:konflux:openshift-4.21:ose-base:jenkins_url',
                    'count:release-failure:konflux:openshift-4.21:ose-base:nvr',
                ]
            return []

        mock_get_keys.side_effect = mock_get_keys_side_effect
        mock_get_value.side_effect = lambda key: {
            'count:release-failure:konflux:openshift-4.21:ose-base:failure': '2',
            'count:release-failure:konflux:openshift-4.21:ose-base:jenkins_url': 'http://j/2',
            'count:release-failure:konflux:openshift-4.21:ose-base:nvr': 'ose-base-1.0-1',
        }.get(key)
        result = await util.get_counter_failures('release-failure', 'openshift-4.21')
        self.assertEqual(result['ose-base']['failure_count'], 2)
        self.assertEqual(result['ose-base']['jenkins_url'], 'http://j/2')
        self.assertEqual(result['ose-base']['nvr'], 'ose-base-1.0-1')

    @patch("artcommonlib.redis.get_keys", new_callable=AsyncMock)
    async def test_get_counter_failures_ec_empty(self, mock_get_keys):
        mock_get_keys.return_value = []
        result = await util.get_counter_failures('ec-failure', 'openshift-4.21')
        self.assertEqual(result, {})
