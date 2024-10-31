import asyncio
from collections import OrderedDict
import os
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch, ANY
from pyartcd.pipelines.gen_assembly import GenAssemblyPipeline
from doozerlib.cli.release_gen_assembly import GenAssemblyCli


class TestGenAssemblyPipeline(IsolatedAsyncioTestCase):
    @patch("artcommonlib.exectools.cmd_gather_async", autospec=True, return_value=(0, "a b c", ""))
    def test_get_nightlies(self, cmd_gather_async: AsyncMock):
        runtime = MagicMock()
        pipeline = GenAssemblyPipeline(runtime, "openshift-4.12", "4.12.99", "https://example.com/ocp-build-data.git",
                                       nightlies=(), allow_pending=False, allow_rejected=False,
                                       allow_inconsistency=False, custom=False, arches=(), in_flight="4.11.88",
                                       previous_list=(), auto_previous=True, auto_trigger_build_sync=False,
                                       pre_ga_mode="none", skip_get_nightlies=False)
        actual = asyncio.run(pipeline._get_nightlies())
        self.assertEqual(actual, ["a", "b", "c"])
        cmd_gather_async.assert_awaited_once_with(
            ['doozer', '--group', 'openshift-4.12', '--assembly', 'stream', 'get-nightlies'], stderr=None, env=ANY)

        cmd_gather_async.reset_mock()
        pipeline.allow_pending = True
        pipeline.allow_inconsistency = True
        pipeline.allow_rejected = True
        actual = asyncio.run(pipeline._get_nightlies())
        self.assertEqual(actual, ["a", "b", "c"])
        cmd_gather_async.assert_awaited_once_with(
            ['doozer', '--group', 'openshift-4.12', '--assembly', 'stream', 'get-nightlies', '--allow-pending',
             '--allow-rejected', '--allow-inconsistency'], stderr=None, env=ANY)

        cmd_gather_async.reset_mock()
        pipeline.arches = ("x86_64", "aarch64")
        pipeline.custom = True
        pipeline.nightlies = ("n1", "n2")
        actual = asyncio.run(pipeline._get_nightlies())
        self.assertEqual(actual, ["a", "b", "c"])
        cmd_gather_async.assert_awaited_once_with(
            ['doozer', '--group', 'openshift-4.12', '--assembly', 'stream', '--arches', 'x86_64,aarch64',
             'get-nightlies', '--allow-pending', '--allow-rejected', '--allow-inconsistency', '--matching=n1',
             '--matching=n2'], stderr=None, env=ANY)

    @patch("os.environ", return_value={"GITHUB_TOKEN": "deadbeef"})
    @patch("pathlib.Path.exists", autospec=True, return_value=True)
    @patch("pyartcd.pipelines.gen_assembly.GhApi")
    @patch("pyartcd.pipelines.gen_assembly.yaml")
    @patch("pyartcd.pipelines.gen_assembly.GitRepository", autospec=True)
    def test_create_or_update_pull_request(self, git_repo: MagicMock, yaml: MagicMock, gh_api: MagicMock, *_):
        runtime = MagicMock(dry_run=False, config={"build_config": {
            "ocp_build_data_repo_push_url": "git@github.com:someone/ocp-build-data.git",
        }})
        pipeline = GenAssemblyPipeline(runtime, "openshift-4.12", "4.12.99", "https://example.com/ocp-build-data.git",
                                       nightlies=(), allow_pending=False, allow_rejected=False,
                                       allow_inconsistency=False, custom=False, arches=(), in_flight="4.11.88",
                                       previous_list=(), auto_previous=True, auto_trigger_build_sync=False,
                                       pre_ga_mode="none", skip_get_nightlies=False)
        pipeline._working_dir = Path("/path/to/working")
        yaml.load.return_value = OrderedDict([
            ("releases", OrderedDict([
                ("4.12.98", OrderedDict()),
                ("4.12.97", OrderedDict()),
            ]))
        ])
        fn = MagicMock(return_value=OrderedDict([
            ("releases", OrderedDict([
                ("4.12.99", OrderedDict()),
                ("4.12.98", OrderedDict()),
                ("4.12.97", OrderedDict()),
            ]))
        ]))
        git_repo.return_value.commit_push.return_value = True
        api = gh_api.return_value
        api.pulls.list.return_value = MagicMock(items=[])
        api.pulls.create.return_value = MagicMock(html_url="https://github.example.com/foo/bar/pull/1234", number=1234)
        actual = asyncio.run(pipeline._create_or_update_pull_request(fn))
        self.assertEqual(actual.number, 1234)
        git_repo.return_value.setup.assert_awaited_once_with("git@github.com:someone/ocp-build-data.git")
        git_repo.return_value.fetch_switch_branch.assert_awaited_once_with(
            'auto-gen-assembly-openshift-4.12-4.12.99', 'openshift-4.12')
        yaml.load.assert_called_once_with(pipeline._working_dir / 'ocp-build-data-push/releases.yml')
        git_repo.return_value.commit_push.assert_awaited_once_with(ANY)
        api.pulls.create.assert_called_once_with(head='someone:auto-gen-assembly-openshift-4.12-4.12.99',
                                                 base='openshift-4.12', title='Add assembly 4.12.99', body=ANY,
                                                 maintainer_can_modify=True)

    @patch("pyartcd.pipelines.gen_assembly.GenAssemblyPipeline._create_or_update_pull_request", autospec=True,
           return_value=MagicMock(html_url="https://github.example.com/foo/bar/pull/1234", number=1234))
    @patch("pyartcd.pipelines.gen_assembly.GenAssemblyPipeline._get_nightlies", autospec=True)
    @patch("pyartcd.pipelines.gen_assembly.GenAssemblyCli", autospec=True)
    async def test_run(self, gacli: AsyncMock, get_nightlies: AsyncMock, _create_or_update_pull_request: AsyncMock):

        os.environ["GITHUB_TOKEN"] = "irrelevant"

        runtime = MagicMock(dry_run=False, config={"build_config": {
            "ocp_build_data_repo_push_url": "git@github.com:someone/ocp-build-data.git",
        }})
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()

        pipeline = GenAssemblyPipeline(runtime, "openshift-4.12", "4.12.99", "https://example.com/ocp-build-data.git",
                                       nightlies=(), allow_pending=False, allow_rejected=False,
                                       allow_inconsistency=False, custom=False, arches=(), in_flight="4.11.88",
                                       previous_list=(), auto_previous=True, auto_trigger_build_sync=False,
                                       pre_ga_mode="none", skip_get_nightlies=False)
        pipeline._working_dir = Path("/path/to/working")
        get_nightlies.return_value = ["nightly1", "nightly2", "nightly3", "nightly4"]
        gacli.run.return_value = (OrderedDict([("releases", OrderedDict([("4.12.99", OrderedDict())]))]))
        await pipeline.run()
        get_nightlies.assert_awaited_once_with(pipeline)
        _create_or_update_pull_request.assert_awaited_once_with(pipeline, ANY)
        del os.environ["GITHUB_TOKEN"]
