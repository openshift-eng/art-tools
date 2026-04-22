"""
Unit tests for build_microshift_bootc pipeline
"""

import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
from pyartcd.pipelines.build_microshift_bootc import BuildMicroShiftBootcPipeline
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient


class TestBuildMicroShiftBootcPipeline(IsolatedAsyncioTestCase):
    """
    Test cases for the BuildMicroShiftBootcPipeline class
    """

    def setUp(self):
        """
        Set up test fixtures before each test method
        """
        self.runtime = Mock(spec=Runtime)
        self.runtime.working_dir = Path(tempfile.mkdtemp())
        self.runtime.dry_run = False
        self.runtime.config = {
            "gitlab_url": "https://gitlab.example.com",
            "shipment_config": {
                "shipment_data_url": "https://gitlab.example.com/shipment-data.git",
                "shipment_data_push_url": "https://gitlab.example.com/shipment-data.git",
            },
        }
        self.runtime.logger = Mock()
        self.mock_slack_client = Mock(spec=SlackClient)
        self.group = "openshift-4.21"
        self.assembly = "4.21.0"
        os.environ["GITHUB_TOKEN"] = "fake-token"
        os.environ["GITLAB_TOKEN"] = "fake-gitlab-token"

    def tearDown(self):
        """
        Clean up test fixtures after each test method
        """
        os.environ.pop("GITHUB_TOKEN", None)
        os.environ.pop("GITLAB_TOKEN", None)

    @patch('pyartcd.pipelines.build_microshift_bootc.get_github_client_for_org')
    async def test_update_shipment_data_extracts_timestamp_from_branch(self, mock_get_client):
        """
        Test that timestamp is correctly extracted from branch name for filename generation
        """
        # given
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=True,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

        pipeline.shipment_data_repo = Mock()
        pipeline.shipment_data_repo._directory = self.runtime.working_dir
        pipeline.shipment_data_repo.write_file = AsyncMock()
        pipeline.shipment_data_repo.add_all = AsyncMock()
        pipeline.shipment_data_repo.log_diff = AsyncMock()
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)

        mock_shipment_config = Mock()
        mock_shipment_config.shipment.metadata.product = "ocp"
        mock_shipment_config.shipment.metadata.group = self.group
        mock_shipment_config.shipment.metadata.application = "ocp-art-tenant"
        mock_shipment_config.model_dump = Mock(return_value={"shipment": {}})

        existing_timestamp = "20260129004538"
        source_branch = f"prepare-microshift-bootc-shipment-{self.assembly}-{existing_timestamp}"

        # when
        await pipeline._update_shipment_data(mock_shipment_config, "Test commit", source_branch)

        # then
        pipeline.shipment_data_repo.write_file.assert_called_once()
        written_filepath = pipeline.shipment_data_repo.write_file.call_args[0][0]
        expected_filename = f"{self.assembly}.microshift-bootc.{existing_timestamp}.yaml"
        self.assertTrue(str(written_filepath).endswith(expected_filename))

    @patch('pyartcd.pipelines.build_microshift_bootc.GitLabClient')
    @patch('pyartcd.pipelines.build_microshift_bootc.get_github_client_for_org')
    async def test_create_shipment_mr_reuses_existing_timestamp(self, mock_get_client, mock_gitlab_class):
        """
        Test that when updating an existing MR, the timestamp from the existing branch is reused
        """
        # given
        existing_timestamp = "20260129004538"
        existing_branch = f"prepare-microshift-bootc-shipment-{self.assembly}-{existing_timestamp}"
        existing_mr_url = "https://gitlab.example.com/shipment-data/-/merge_requests/123"

        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=True,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

        # Set gitlab_token like in test_prepare_release_konflux
        pipeline.gitlab_token = "fake-gitlab-token"

        pipeline.shipment_data_repo = Mock()
        pipeline.shipment_data_repo._directory = self.runtime.working_dir
        pipeline.shipment_data_repo.fetch_switch_branch = AsyncMock()
        pipeline.shipment_data_repo.write_file = AsyncMock()
        pipeline.shipment_data_repo.add_all = AsyncMock()
        pipeline.shipment_data_repo.log_diff = AsyncMock()
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)

        # Mock releases config with existing shipment URL - use Model wrapper like in test_prepare_release_konflux
        pipeline.releases_config = Model(
            {"releases": {self.assembly: {"assembly": {"group": {"shipment": {"url": existing_mr_url}}}}}}
        )

        # Mock GitLab MR to return existing branch
        mock_mr = Mock()
        mock_mr.source_branch = existing_branch
        mock_mr.web_url = existing_mr_url

        mock_project = Mock()
        mock_project.mergerequests.get.return_value = mock_mr
        mock_project.mergerequests.list.return_value = []  # No existing MRs for this branch (will create new one)
        mock_project.mergerequests.create.return_value = mock_mr

        mock_gitlab_instance = Mock()
        mock_gitlab_instance.get_project.return_value = mock_project
        mock_gitlab_class.return_value = mock_gitlab_instance

        mock_shipment_config = Mock()
        mock_shipment_config.shipment.metadata.product = "ocp"
        mock_shipment_config.shipment.metadata.group = self.group
        mock_shipment_config.shipment.metadata.application = "ocp-art-tenant"
        mock_shipment_config.model_dump = Mock(return_value={"shipment": {}})

        pipeline._gitlab = mock_gitlab_instance

        # when
        with patch('pyartcd.pipelines.build_microshift_bootc.get_release_name_for_assembly', return_value="4.21.0"):
            _ = await pipeline._create_shipment_mr(mock_shipment_config)

        # then
        pipeline.shipment_data_repo.write_file.assert_called_once()
        written_filepath = pipeline.shipment_data_repo.write_file.call_args[0][0]
        expected_filename = f"{self.assembly}.microshift-bootc.{existing_timestamp}.yaml"
        self.assertTrue(str(written_filepath).endswith(expected_filename))

    @patch('pyartcd.pipelines.build_microshift_bootc.get_github_client_for_org')
    async def test_create_shipment_mr_generates_new_timestamp_for_new_shipment(self, mock_get_client):
        """
        Test that when creating a new MR (no existing shipment), a new branch with timestamp is created
        """
        # given
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=True,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

        # Set gitlab_token like in test_prepare_release_konflux
        pipeline.gitlab_token = "fake-gitlab-token"

        pipeline.shipment_data_repo = Mock()
        pipeline.shipment_data_repo._directory = self.runtime.working_dir
        pipeline.shipment_data_repo.create_branch = AsyncMock()
        pipeline.shipment_data_repo.write_file = AsyncMock()
        pipeline.shipment_data_repo.add_all = AsyncMock()
        pipeline.shipment_data_repo.log_diff = AsyncMock()
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)

        # No existing shipment URL in releases config - use Model wrapper with proper structure
        pipeline.releases_config = Model(
            {"releases": {self.assembly: {"assembly": {"group": {}}}}}  # No shipment URL here
        )

        mock_gitlab_client = Mock()
        mock_shipment_config = Mock()
        mock_shipment_config.shipment.metadata.product = "ocp"
        mock_shipment_config.shipment.metadata.group = self.group
        mock_shipment_config.shipment.metadata.application = "ocp-art-tenant"
        mock_shipment_config.model_dump = Mock(return_value={"shipment": {}})

        mock_project = Mock()
        mock_project.mergerequests.list.return_value = []
        mock_mr = Mock()
        mock_mr.web_url = "https://gitlab.example.com/shipment-data/-/merge_requests/123"
        mock_project.mergerequests.create.return_value = mock_mr
        mock_gitlab_client.get_project.return_value = mock_project
        pipeline._gitlab = mock_gitlab_client

        # when
        with patch('pyartcd.pipelines.build_microshift_bootc.get_release_name_for_assembly', return_value="4.18.1"):
            _ = await pipeline._create_shipment_mr(mock_shipment_config)

        # then
        # Verify a new branch with timestamp was created
        pipeline.shipment_data_repo.create_branch.assert_called_once()
        created_branch = pipeline.shipment_data_repo.create_branch.call_args[0][0]
        self.assertTrue(created_branch.startswith(f"prepare-microshift-bootc-shipment-{self.assembly}-"))

        # Verify the filename uses the timestamp from the branch
        pipeline.shipment_data_repo.write_file.assert_called_once()
        written_filepath = pipeline.shipment_data_repo.write_file.call_args[0][0]
        filename = str(written_filepath.name)

        self.assertTrue(filename.startswith(f"{self.assembly}.microshift-bootc."))
        self.assertTrue(filename.endswith(".yaml"))

        # Extract timestamp from branch and verify it matches the filename
        timestamp_from_branch = created_branch.split("-")[-1]
        timestamp_part = filename.replace(f"{self.assembly}.microshift-bootc.", "").replace(".yaml", "")
        self.assertEqual(timestamp_from_branch, timestamp_part)
        self.assertEqual(len(timestamp_part), 14)
        self.assertTrue(timestamp_part.isdigit())

    @patch("pyartcd.pipelines.build_microshift_bootc.get_microshift_builds")
    async def test_get_microshift_rpm_commit_extracts_commit(self, mock_get_builds):
        """
        Test that _get_microshift_rpm_commit correctly extracts the git commit
        from the microshift RPM NVR.
        """
        # given
        mock_get_builds.return_value = [
            "microshift-4.21.0-202601290005.p2.g0d0943b.assembly.4.21.0.el8",
            "microshift-4.21.0-202601290005.p2.g0d0943b.assembly.4.21.0.el9",
        ]
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

        # when
        commit = await pipeline._get_microshift_rpm_commit()

        # then
        self.assertEqual(commit, "0d0943b")
        mock_get_builds.assert_called_once_with(self.group, self.assembly, env=pipeline._elliott_env_vars)

    @patch("pyartcd.pipelines.build_microshift_bootc.get_microshift_builds")
    async def test_get_microshift_rpm_commit_raises_when_no_nvrs(self, mock_get_builds):
        """
        Test that _get_microshift_rpm_commit raises ValueError when no NVRs are found.
        """
        # given
        mock_get_builds.return_value = []
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

        # when / then
        with self.assertRaises(ValueError) as ctx:
            await pipeline._get_microshift_rpm_commit()
        self.assertIn("Could not find microshift RPM NVRs", str(ctx.exception))

    @patch("pyartcd.pipelines.build_microshift_bootc.get_microshift_builds")
    async def test_get_microshift_rpm_commit_raises_when_no_commit_in_nvr(self, mock_get_builds):
        """
        Test that _get_microshift_rpm_commit raises ValueError when the NVR
        does not contain a recognizable commit hash.
        """
        # given
        mock_get_builds.return_value = [
            "microshift-4.21.0-202601290005.assembly.4.21.0.el9",
        ]
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

        # when / then
        with self.assertRaises(ValueError) as ctx:
            await pipeline._get_microshift_rpm_commit()
        self.assertIn("commit", str(ctx.exception).lower())

    def _make_pipeline(self, group="openshift-4.21", assembly="4.21.0"):
        """Helper to create a pipeline instance with the given group and assembly."""
        return BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=group,
            assembly=assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

    def test_get_assembly_label_value_standard(self):
        pipeline = self._make_pipeline(assembly="4.21.0")
        pipeline.assembly_type = AssemblyTypes.STANDARD
        self.assertEqual(pipeline._get_assembly_label_value(), "v4.21.0")

    def test_get_assembly_label_value_standard_z_stream(self):
        pipeline = self._make_pipeline(assembly="4.18.3")
        pipeline.assembly_type = AssemblyTypes.STANDARD
        self.assertEqual(pipeline._get_assembly_label_value(), "v4.18.3")

    def test_get_assembly_label_value_candidate_rc(self):
        pipeline = self._make_pipeline(group="openshift-4.22", assembly="rc.1")
        pipeline.assembly_type = AssemblyTypes.CANDIDATE
        self.assertEqual(pipeline._get_assembly_label_value(), "v4.22.0-rc.1")

    def test_get_assembly_label_value_candidate_ec(self):
        pipeline = self._make_pipeline(group="openshift-4.22", assembly="ec.3")
        pipeline.assembly_type = AssemblyTypes.CANDIDATE
        self.assertEqual(pipeline._get_assembly_label_value(), "v4.22.0-ec.3")

    def test_get_assembly_label_value_preview(self):
        pipeline = self._make_pipeline(group="openshift-4.22", assembly="ec.2")
        pipeline.assembly_type = AssemblyTypes.PREVIEW
        self.assertEqual(pipeline._get_assembly_label_value(), "v4.22.0-ec.2")

    def test_get_assembly_label_value_custom_raises(self):
        pipeline = self._make_pipeline(assembly="custom-hotfix")
        pipeline.assembly_type = AssemblyTypes.CUSTOM
        with self.assertRaises(ValueError) as ctx:
            pipeline._get_assembly_label_value()
        self.assertIn("CUSTOM", str(ctx.exception))

    def test_get_assembly_label_value_stream_returns_none(self):
        pipeline = self._make_pipeline(assembly="stream")
        pipeline.assembly_type = AssemblyTypes.STREAM
        self.assertIsNone(pipeline._get_assembly_label_value())

    @patch("pyartcd.pipelines.build_microshift_bootc.exectools.cmd_assert_async", new_callable=AsyncMock)
    @patch.object(BuildMicroShiftBootcPipeline, "get_latest_bootc_build", new_callable=AsyncMock)
    @patch.object(BuildMicroShiftBootcPipeline, "_get_microshift_rpm_commit", new_callable=AsyncMock)
    @patch.object(BuildMicroShiftBootcPipeline, "_build_plashet_for_bootc", new_callable=AsyncMock)
    async def test_rebase_uses_two_segment_version(self, mock_plashet, mock_get_commit, mock_get_build, mock_cmd):
        """
        Test that _rebase_and_build_bootc passes --version v4.21 (2 segments)
        instead of v4.21.0 to avoid confusing tags on the container catalog
        (OCPBUGS-78040).
        """
        mock_get_commit.return_value = "abc1234"
        mock_get_build.return_value = Mock(nvr="microshift-bootc-4.21.0-1.el9")
        pipeline = self._make_pipeline(group="openshift-4.21", assembly="4.21.7")
        pipeline.assembly_type = AssemblyTypes.STANDARD
        pipeline.force = True
        os.environ["KONFLUX_SA_KUBECONFIG"] = "/fake/kubeconfig"

        try:
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await pipeline._rebase_and_build_bootc()

            rebase_cmd = mock_cmd.call_args_list[0][0][0]

            # --version uses 2-segment format (no trailing .0)
            ver_idx = rebase_cmd.index("--version")
            self.assertEqual(rebase_cmd[ver_idx + 1], "v4.21")

            # assembly label is also set
            extra_label_indices = [i for i, v in enumerate(rebase_cmd) if v == "--extra-label"]
            extra_labels = [rebase_cmd[i + 1] for i in extra_label_indices]
            self.assertIn("assembly=v4.21.7", extra_labels)
        finally:
            os.environ.pop("KONFLUX_SA_KUBECONFIG", None)

    @patch("pyartcd.pipelines.build_microshift_bootc.exectools.cmd_assert_async", new_callable=AsyncMock)
    @patch.object(BuildMicroShiftBootcPipeline, "get_latest_bootc_build", new_callable=AsyncMock)
    @patch.object(BuildMicroShiftBootcPipeline, "_get_microshift_rpm_commit", new_callable=AsyncMock)
    @patch.object(BuildMicroShiftBootcPipeline, "_build_plashet_for_bootc", new_callable=AsyncMock)
    async def test_rebase_and_build_bootc_uses_rpm_commit(
        self, mock_plashet, mock_get_commit, mock_get_build, mock_cmd
    ):
        """
        Test that _rebase_and_build_bootc passes the RPM commit to --lock-upstream
        instead of HEAD.
        """
        # given
        mock_get_commit.return_value = "0d0943b"
        mock_get_build.return_value = Mock(nvr="microshift-bootc-4.21.0-1.el9")
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            force=False,
            force_plashet_sync=False,
            prepare_shipment=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.assembly_type = AssemblyTypes.STANDARD
        pipeline.force = True
        os.environ["KONFLUX_SA_KUBECONFIG"] = "/fake/kubeconfig"

        try:
            # when
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await pipeline._rebase_and_build_bootc()

            # then
            # Verify rebase command uses the RPM commit, not HEAD
            rebase_call = mock_cmd.call_args_list[0]
            rebase_cmd = rebase_call[0][0]
            lock_idx = rebase_cmd.index("--lock-upstream")
            self.assertEqual(rebase_cmd[lock_idx + 1], "microshift-bootc")
            self.assertEqual(rebase_cmd[lock_idx + 2], "0d0943b")

            # Verify build command uses the RPM commit, not HEAD
            build_call = mock_cmd.call_args_list[1]
            build_cmd = build_call[0][0]
            lock_idx = build_cmd.index("--lock-upstream")
            self.assertEqual(build_cmd[lock_idx + 1], "microshift-bootc")
            self.assertEqual(build_cmd[lock_idx + 2], "0d0943b")
        finally:
            os.environ.pop("KONFLUX_SA_KUBECONFIG", None)
