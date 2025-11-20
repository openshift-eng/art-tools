from typing import Dict, Iterable, List, Optional, Tuple
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import koji
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
from doozerlib.cli.config_tag_rpms import TagRPMsCli


class TestRpmDelivery(IsolatedAsyncioTestCase):
    async def test_get_tagged_builds(self):
        koji_api = MagicMock(autospec=koji.ClientSession)
        tag_component_tuples = [
            ("tag1", "foo"),
            ("tag2", "bar"),
        ]
        mc = koji_api.multicall.return_value.__enter__.return_value
        mc.listTagged.side_effect = lambda tag, package, **kwargs: MagicMock(
            result={
                ("tag1", "foo"): [
                    {"nvr": "foo-1.0.0-1"},
                ],
                ("tag2", "bar"): [
                    {"nvr": "bar-1.0.0-1"},
                    {"nvr": "bar-1.0.1-1"},
                ],
            }[(tag, package)]
        )
        expected = [
            [{"nvr": "foo-1.0.0-1"}],
            [{"nvr": "bar-1.0.0-1"}, {"nvr": "bar-1.0.1-1"}],
        ]
        actual = await TagRPMsCli.get_tagged_builds(
            koji_api, tag_component_tuples, build_type="rpm", event=None, latest=100, inherit=False
        )
        self.assertEqual(actual, expected)

    async def test_untag_builds(self):
        koji_api = MagicMock(autospec=koji.ClientSession)
        tag_build_tuples = [
            ("tag1", "foo-1.0.0-1"),
            ("tag2", "bar-1.0.0-1"),
        ]
        mc = koji_api.multicall.return_value.__enter__.return_value
        mc.untagBuild.return_value = MagicMock(result=None)
        await TagRPMsCli.untag_builds(koji_api, tag_build_tuples)
        mc.untagBuild.assert_any_call("tag1", "foo-1.0.0-1", strict=False)
        mc.untagBuild.assert_any_call("tag2", "bar-1.0.0-1", strict=False)

    @patch("doozerlib.brew.watch_tasks_async")
    async def test_tag_builds(self, watch_tasks_async: AsyncMock):
        koji_api = MagicMock(autospec=koji.ClientSession)
        tag_build_tuples = [
            ("tag1", "foo-1.0.0-1"),
            ("tag2", "bar-1.0.0-1"),
        ]
        mc = koji_api.multicall.return_value.__enter__.return_value
        mc.tagBuild.side_effect = lambda tag, package, **kwargs: MagicMock(
            result={
                ("tag1", "foo-1.0.0-1"): 10001,
                ("tag2", "bar-1.0.0-1"): 10002,
            }[(tag, package)]
        )
        watch_tasks_async.return_value = {
            10001: None,
            10002: None,
        }
        runtime = MagicMock()
        cli = TagRPMsCli(runtime=runtime, dry_run=False, as_json=False)
        await cli.tag_builds(koji_api, tag_build_tuples)
        mc.tagBuild.assert_any_call("tag1", "foo-1.0.0-1")
        mc.tagBuild.assert_any_call("tag2", "bar-1.0.0-1")
        watch_tasks_async.assert_awaited_once_with(koji_api, ANY, [10001, 10002])

    @patch("doozerlib.brew.get_builds_tags")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.tag_builds")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.untag_builds")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.get_tagged_builds")
    async def test_run_non_kernel_packages(
        self, get_tagged_builds: AsyncMock, untag_builds: AsyncMock, tag_builds: AsyncMock, get_builds_tags: Mock
    ):
        group_config = Model(
            {
                "vars": {
                    "RHCOS_EL_MAJOR": "9",
                    "RHCOS_EL_MINOR": "4",
                },
                "rpm_deliveries": [
                    {
                        "packages": ["foo", "bar"],
                        "rhel_tag": "test-rhel-tag",
                        "integration_tag": "test-integration-tag",
                        "stop_ship_tag": "test-stop-ship-tag",
                        "target_tag": "test-target-tag",
                    },
                ],
            }
        )
        runtime = MagicMock(assembly_type=AssemblyTypes.STREAM, group_config=group_config)
        koji_api = runtime.build_retrying_koji_client.return_value

        def _get_tagged_builds(
            session: koji.ClientSession,
            tag_component_tuples: Iterable[Tuple[str, Optional[str]]],
            build_type: Optional[str],
            event: Optional[int] = None,
            latest: int = 0,
            inherit: bool = False,
        ) -> List[List[Dict]]:
            results = {
                ("test-stop-ship-tag", "foo"): [{"nvr": "foo-1.0.0-1", "version": "1.0.0", "name": "foo"}],
                ("test-stop-ship-tag", "bar"): [{"nvr": "bar-1.0.0-1", "version": "1.0.0", "name": "bar"}],
                ("test-integration-tag", "foo"): [
                    {"nvr": "foo-1.0.0-1", "version": "1.0.0", "name": "foo"},
                    {"nvr": "foo-1.0.1-1", "version": "1.0.1", "name": "foo"},
                ],
                ("test-integration-tag", "bar"): [
                    {"nvr": "bar-1.0.0-1", "version": "1.0.0", "name": "bar"},
                    {"nvr": "bar-1.0.1-1", "version": "1.0.1", "name": "bar"},
                    {"nvr": "bar-1.0.2-1", "version": "1.0.2", "name": "bar"},
                ],
                ("test-rhel-tag", "foo"): [{"nvr": "foo-1.0.0-1", "version": "1.0.0", "name": "foo"}],
                ("test-rhel-tag", "bar"): [{"nvr": "bar-1.0.0-1", "version": "1.0.0", "name": "bar"}],
            }
            return [results[tc] for tc in tag_component_tuples]

        get_tagged_builds.side_effect = _get_tagged_builds
        get_builds_tags.side_effect = lambda nvr_list, _: [
            {
                "foo-1.0.0-1": [{"name": "test-stop-ship-tag"}, {"name": "test-integration-tag"}],
                "bar-1.0.0-1": [
                    {"name": "test-stop-ship-tag"},
                    {"name": "test-integration-tag"},
                    {"name": "test-target-tag"},
                ],
            }[nvr]
            for nvr in nvr_list
        ]
        koji_api.queryHistory.side_effect = lambda tables, build, tag: {
            "tag_listing": {
                "foo-1.0.1-1": [],
                "bar-1.0.1-1": [{"active": False}],
                "bar-1.0.2-1": [],
            }[build],
        }
        cli = TagRPMsCli(runtime=runtime, dry_run=False, as_json=False)
        await cli.run()
        untag_builds.assert_awaited_once_with(ANY, [('test-target-tag', 'bar-1.0.0-1')])

    @patch("doozerlib.brew.get_builds_tags")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.tag_builds")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.untag_builds")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.get_tagged_builds")
    async def test_run_kernel_version_match(
        self, get_tagged_builds: AsyncMock, untag_builds: AsyncMock, tag_builds: AsyncMock, get_builds_tags: Mock
    ):
        group_config = Model(
            {
                "vars": {
                    "RHCOS_EL_MAJOR": "9",
                    "RHCOS_EL_MINOR": "4",
                },
                "rpm_deliveries": [
                    {
                        "packages": ["kernel", "kernel-rt"],
                        "rhel_tag": "test-rhel-tag",
                        "integration_tag": "test-integration-tag",
                        "stop_ship_tag": "test-stop-ship-tag",
                        "target_tag": "test-target-tag",
                    },
                ],
            }
        )
        runtime = MagicMock(assembly_type=AssemblyTypes.STREAM, group_config=group_config)
        koji_api = runtime.build_retrying_koji_client.return_value

        kernel_build = {
            'nvr': 'kernel-5.14.0-284.28.1.el9_2',
            'version': '5.14.0',
            'name': 'kernel',
            'release': '284.28.1.el9_2',
        }
        kernel_rt_build = {
            'nvr': 'kernel-rt-5.14.0-284.28.1.rt14.313.el9_2',
            'version': '5.14.0',
            'name': 'kernel-rt',
            'release': '284.28.1.rt14.313.el9_2',
        }

        def _get_tagged_builds(
            session: koji.ClientSession,
            tag_component_tuples: Iterable[Tuple[str, Optional[str]]],
            build_type: Optional[str],
            event: Optional[int] = None,
            latest: int = 0,
            inherit: bool = False,
        ) -> List[List[Dict]]:
            results = {
                ("test-stop-ship-tag", "kernel"): [],
                ("test-stop-ship-tag", "kernel-rt"): [],
                ("test-rhel-tag", "kernel"): [kernel_build],
                ("test-rhel-tag", "kernel-rt"): [kernel_rt_build],
                ("test-integration-tag", "kernel"): [kernel_build],
                ("test-integration-tag", "kernel-rt"): [kernel_rt_build],
            }
            return [results[tc] for tc in tag_component_tuples]

        get_tagged_builds.side_effect = _get_tagged_builds
        get_builds_tags.side_effect = lambda nvr_list, _: [
            {
                kernel_build['nvr']: [{"name": "test-integration-tag"}],
                kernel_rt_build['nvr']: [{"name": "test-integration-tag"}],
            }[nvr]
            for nvr in nvr_list
        ]
        koji_api.queryHistory.side_effect = lambda tables, build, tag: {
            "tag_listing": {kernel_build['nvr']: [], kernel_rt_build['nvr']: []}[build],
        }
        cli = TagRPMsCli(runtime=runtime, dry_run=False, as_json=False)
        await cli.run()
        tag_builds.assert_awaited_once_with(
            ANY, [('test-target-tag', kernel_build['nvr']), ('test-target-tag', kernel_rt_build['nvr'])]
        )

    @patch("doozerlib.brew.get_builds_tags")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.tag_builds")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.untag_builds")
    @patch("doozerlib.cli.config_tag_rpms.TagRPMsCli.get_tagged_builds")
    async def test_run_kernel_version_mismatch(
        self, get_tagged_builds: AsyncMock, untag_builds: AsyncMock, tag_builds: AsyncMock, get_builds_tags: Mock
    ):
        group_config = Model(
            {
                "vars": {
                    "RHCOS_EL_MAJOR": "9",
                    "RHCOS_EL_MINOR": "2",
                },
                "rpm_deliveries": [
                    {
                        "packages": ["kernel", "kernel-rt"],
                        "rhel_tag": "test-rhel-tag",
                        "integration_tag": "test-integration-tag",
                        "stop_ship_tag": "test-stop-ship-tag",
                        "target_tag": "test-target-tag",
                    },
                ],
            }
        )
        runtime = MagicMock(assembly_type=AssemblyTypes.STREAM, group_config=group_config)
        koji_api = runtime.build_retrying_koji_client.return_value

        kernel_build = {
            'nvr': 'kernel-5.14.0-284.28.1.el9_2',
            'version': '5.14.0',
            'name': 'kernel',
            'release': '284.28.1.el9_2',
        }
        kernel_rt_build = {
            'nvr': 'kernel-rt-5.14.0-284.31.1.rt14.313.el9_2',
            'version': '5.14.0',
            'name': 'kernel-rt',
            'release': '284.31.1.rt14.313.el9_2',
        }

        def _get_tagged_builds(
            session: koji.ClientSession,
            tag_component_tuples: Iterable[Tuple[str, Optional[str]]],
            build_type: Optional[str],
            event: Optional[int] = None,
            latest: int = 0,
            inherit: bool = False,
        ) -> List[List[Dict]]:
            results = {
                ("test-stop-ship-tag", "kernel"): [],
                ("test-stop-ship-tag", "kernel-rt"): [],
                ("test-rhel-tag", "kernel"): [kernel_build],
                ("test-rhel-tag", "kernel-rt"): [kernel_rt_build],
                ("test-integration-tag", "kernel"): [kernel_build],
                ("test-integration-tag", "kernel-rt"): [kernel_rt_build],
            }
            return [results[tc] for tc in tag_component_tuples]

        get_tagged_builds.side_effect = _get_tagged_builds
        get_builds_tags.side_effect = lambda nvr_list, _: [
            {
                kernel_build['nvr']: [{"name": "test-integration-tag"}],
                kernel_rt_build['nvr']: [{"name": "test-integration-tag"}],
            }[nvr]
            for nvr in nvr_list
        ]
        koji_api.queryHistory.side_effect = lambda tables, build, tag: {
            "tag_listing": {kernel_build['nvr']: [], kernel_rt_build['nvr']: []}[build],
        }
        cli = TagRPMsCli(runtime=runtime, dry_run=False, as_json=False)
        with self.assertRaisesRegex(ValueError, "Version mismatch"):
            await cli.run()
