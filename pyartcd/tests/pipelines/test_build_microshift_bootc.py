"""
Unit tests for build_microshift_bootc pipeline
"""

import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

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

    @patch('pyartcd.pipelines.build_microshift_bootc.ShipmentConfig')
    @patch('pyartcd.pipelines.build_microshift_bootc.KonfluxImageBuilder.get_application_name')
    @patch('pyartcd.pipelines.build_microshift_bootc.Github')
    async def test_load_existing_shipment_config_extracts_timestamp(
        self, mock_github, mock_get_app_name, mock_shipment_config_class
    ):
        """
        Test that when loading an existing shipment config, the timestamp is correctly extracted
        from the filename and stored in existing_shipment_timestamp
        """
        # given
        mock_get_app_name.return_value = "openshift-4-21"
        mock_shipment_instance = Mock()
        mock_shipment_config_class.return_value = mock_shipment_instance

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

        application = "openshift-4-21"
        env = "prod"
        shipment_dir = self.runtime.working_dir / "shipment" / "ocp" / self.group / application / env
        shipment_dir.mkdir(parents=True, exist_ok=True)

        existing_timestamp = "20260129004538"
        existing_filename = f"{self.assembly}.microshift-bootc.{existing_timestamp}.yaml"
        existing_file = shipment_dir / existing_filename

        shipment_yaml_content = """
shipment:
  metadata:
    product: ocp
    application: openshift-4-21
    group: openshift-4.21
    assembly: 4.21.0
    fbc: false
  snapshot:
    spec:
      application: openshift-4-21
      components:
        - name: ose-4-21-microshift-bootc
    nvrs:
      - microshift-bootc-container-v4.21.0-202601290005.p2.g509fdb7.assembly.4.21.0.el9
"""
        existing_file.write_text(shipment_yaml_content)

        # when
        shipment_config = await pipeline._load_existing_shipment_config()
        # then
        self.assertIsNotNone(shipment_config)
        self.assertEqual(pipeline.existing_shipment_timestamp, existing_timestamp)

    @patch('pyartcd.pipelines.build_microshift_bootc.Github')
    async def test_create_shipment_mr_reuses_existing_timestamp(self, mock_github):
        """
        Test that when updating an existing MR, the existing timestamp is reused
        instead of generating a new one
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

        existing_timestamp = "20260129004538"
        pipeline.existing_shipment_timestamp = existing_timestamp

        pipeline.shipment_data_repo = Mock()
        pipeline.shipment_data_repo._directory = self.runtime.working_dir
        pipeline.shipment_data_repo.does_branch_exist_on_remote = AsyncMock(return_value=True)
        pipeline.shipment_data_repo.fetch_switch_branch = AsyncMock()
        pipeline.shipment_data_repo.create_branch = AsyncMock()
        pipeline.shipment_data_repo.write_file = AsyncMock()
        pipeline.shipment_data_repo.add_all = AsyncMock()
        pipeline.shipment_data_repo.log_diff = AsyncMock()
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)

        pipeline.releases_config = {"releases": {self.assembly: {}}}

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
        with patch('pyartcd.pipelines.build_microshift_bootc.get_release_name_for_assembly', return_value="4.21.0"):
            _ = await pipeline._create_shipment_mr(mock_shipment_config)

        # then
        pipeline.shipment_data_repo.write_file.assert_called_once()
        written_filepath = pipeline.shipment_data_repo.write_file.call_args[0][0]
        expected_filename = f"{self.assembly}.microshift-bootc.{existing_timestamp}.yaml"

        self.assertTrue(str(written_filepath).endswith(expected_filename))

    @patch('pyartcd.pipelines.build_microshift_bootc.Github')
    async def test_create_shipment_mr_generates_new_timestamp_for_new_shipment(self, mock_github):
        """
        Test that when creating a new MR (no existing shipment), a new timestamp is generated
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

        self.assertIsNone(pipeline.existing_shipment_timestamp)

        pipeline.shipment_data_repo = Mock()
        pipeline.shipment_data_repo._directory = self.runtime.working_dir
        pipeline.shipment_data_repo.does_branch_exist_on_remote = AsyncMock(return_value=False)
        pipeline.shipment_data_repo.fetch_switch_branch = AsyncMock()
        pipeline.shipment_data_repo.create_branch = AsyncMock()
        pipeline.shipment_data_repo.write_file = AsyncMock()
        pipeline.shipment_data_repo.add_all = AsyncMock()
        pipeline.shipment_data_repo.log_diff = AsyncMock()
        pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)

        pipeline.releases_config = {"releases": {self.assembly: {}}}

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
        pipeline.shipment_data_repo.write_file.assert_called_once()
        written_filepath = pipeline.shipment_data_repo.write_file.call_args[0][0]
        filename = str(written_filepath.name)

        self.assertTrue(filename.startswith(f"{self.assembly}.microshift-bootc."))
        self.assertTrue(filename.endswith(".yaml"))

        timestamp_part = filename.replace(f"{self.assembly}.microshift-bootc.", "").replace(".yaml", "")
        self.assertEqual(len(timestamp_part), 14)
        self.assertTrue(timestamp_part.isdigit())
