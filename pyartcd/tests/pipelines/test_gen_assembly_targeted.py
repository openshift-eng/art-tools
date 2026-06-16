import os
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

from pyartcd.pipelines.gen_assembly_targeted import GenAssemblyTargetedPipeline


def _make_runtime(dry_run=False):
    runtime = MagicMock(
        dry_run=dry_run,
        config={
            "build_config": {
                "ocp_build_data_repo_push_url": "git@github.com:someone/ocp-build-data.git",
                "ocp_build_data_url": "https://github.com/openshift-eng/ocp-build-data",
            }
        },
    )
    runtime.new_slack_client.return_value = AsyncMock()
    runtime.new_slack_client.return_value.say.return_value = {"message": {"ts": ""}}
    runtime.new_slack_client.return_value.bind_channel = MagicMock()
    runtime.working_dir = Path("/tmp/working")
    return runtime


def _make_pipeline(runtime=None, **overrides):
    runtime = runtime or _make_runtime()
    defaults = {
        "runtime": runtime,
        "group": "openshift-4.17",
        "assembly": "4.17.5",
        "basis_assembly": "4.17.4",
        "kernel_nvrs": ("kernel-5.14.0-284.28.1.el9_2",),
        "bug_ids": ("CVE-2025-1234",),
        "build_system": "konflux",
        "data_path": None,
        "release_date": None,
        "auto_trigger_build_sync": False,
    }
    defaults.update(overrides)
    return GenAssemblyTargetedPipeline(**defaults)


class TestGenAssemblyFromKernel(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.gen_assembly_targeted.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_constructs_correct_doozer_command(self, mock_cmd):
        """
        _gen_assembly_from_targeted constructs correct doozer command with all args.
        """
        mock_cmd.return_value = (0, "releases:\n  4.17.5:\n    assembly: {}\n", "")
        pipeline = _make_pipeline(
            release_date="2026-Jun-15",
            data_path="https://github.com/custom/ocp-build-data",
        )
        pipeline._registry_config = "/tmp/auth.json"

        await pipeline._gen_assembly_from_targeted()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("doozer", cmd)
        self.assertIn("--group", cmd)
        self.assertIn("openshift-4.17", cmd)
        self.assertIn("--build-system", cmd)
        self.assertIn("konflux", cmd)
        self.assertIn("--data-path=https://github.com/custom/ocp-build-data", cmd)
        self.assertIn("--registry-config=/tmp/auth.json", cmd)
        self.assertIn("release:gen-assembly", cmd)
        self.assertIn("--name=4.17.5", cmd)
        self.assertIn("from-targeted", cmd)
        self.assertIn("--basis-assembly=4.17.4", cmd)
        self.assertIn("--kernel-nvr=kernel-5.14.0-284.28.1.el9_2", cmd)
        self.assertIn("--bug-id=CVE-2025-1234", cmd)
        self.assertIn("--date=2026-Jun-15", cmd)

    @patch("pyartcd.pipelines.gen_assembly_targeted.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_omits_optional_args_when_not_set(self, mock_cmd):
        """
        Optional args (data-path, date, registry-config) omitted when not set.
        """
        mock_cmd.return_value = (0, "releases:\n  4.17.5:\n    assembly: {}\n", "")
        pipeline = _make_pipeline()
        pipeline._registry_config = None

        await pipeline._gen_assembly_from_targeted()

        cmd = mock_cmd.call_args[0][0]
        cmd_str = " ".join(cmd)
        self.assertNotIn("--data-path", cmd_str)
        self.assertNotIn("--registry-config", cmd_str)
        self.assertNotIn("--date", cmd_str)


class TestCreateOrUpdatePullRequest(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.gen_assembly_targeted.create_or_update_assembly_pr", new_callable=AsyncMock)
    async def test_calls_shared_pr_function(self, mock_create_pr):
        """
        PR creation delegates to the shared create_or_update_assembly_pr function.
        """
        mock_create_pr.return_value = MagicMock(html_url="https://github.com/test/pr/1", number=1)
        pipeline = _make_pipeline()
        pipeline._working_dir = Path("/tmp/working")

        assembly_def = {"releases": {}}
        result = await pipeline._create_or_update_pull_request(assembly_def)

        mock_create_pr.assert_awaited_once_with(
            runtime=pipeline.runtime,
            group="openshift-4.17",
            assembly="4.17.5",
            build_system="konflux",
            assembly_definition=assembly_def,
            title="Add targeted assembly 4.17.5",
            body=ANY,
            working_dir=pipeline._working_dir,
            logger=ANY,
            auto_trigger_build_sync=False,
        )
        self.assertEqual(result.number, 1)


class TestRunPipeline(IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.gen_assembly_targeted.RegistryConfig")
    @patch.dict(os.environ, {"QUAY_AUTH_FILE": "/tmp/fake-auth.json"})
    @patch("pyartcd.pipelines.gen_assembly_targeted.exectools.cmd_gather_async", new_callable=AsyncMock)
    @patch.object(GenAssemblyTargetedPipeline, "_create_or_update_pull_request", new_callable=AsyncMock)
    async def test_end_to_end(self, mock_create_pr, mock_cmd, mock_registry_config):
        """
        End-to-end pipeline run calls doozer and creates PR.
        """
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime=runtime)

        mock_cmd.return_value = (0, "releases:\n  4.17.5:\n    assembly:\n      type: standard\n", "")
        mock_create_pr.return_value = MagicMock(html_url="https://github.com/test/pr/1")

        await pipeline.run()

        mock_cmd.assert_awaited_once()
        mock_create_pr.assert_awaited_once()

    @patch("pyartcd.pipelines.gen_assembly_targeted.RegistryConfig")
    @patch.dict(os.environ, {"QUAY_AUTH_FILE": "/tmp/fake-auth.json"})
    @patch("pyartcd.pipelines.gen_assembly_targeted.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_error_posted_to_slack(self, mock_cmd, mock_registry_config):
        """
        Errors are posted to Slack channel.
        """
        runtime = _make_runtime()
        pipeline = _make_pipeline(runtime=runtime)
        mock_cmd.side_effect = ChildProcessError("doozer failed")

        with self.assertRaises(ChildProcessError):
            await pipeline.run()

        slack_client = runtime.new_slack_client.return_value
        self.assertTrue(any("Error" in str(call) for call in slack_client.say.call_args_list))
