import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, call, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.model import Model
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import (
    Environments,
    Metadata,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    Spec,
    Data,
    ReleaseNotes
)

from pyartcd import constants
from pyartcd.git import GitRepository
from pyartcd.pipelines.prepare_release_konflux import PrepareReleaseKonfluxPipeline
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient


class TestPrepareReleaseKonfluxPipeline(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = Mock(spec=Runtime)
        self.runtime.working_dir = Path("/tmp/working-dir")
        self.runtime.dry_run = False
        self.runtime.config = {
            "gitlab_url": "https://gitlab.example.com", 
            "build_config": {
                "ocp_build_data_url": "https://build.url/repo",
                "ocp_build_data_push_url": "https://build.push.url/repo",
            }, 
            "shipment_config": {
                "shipment_data_url": "https://shipment.url/repo",
                "shipment_data_push_url": "https://shipment.push.url/repo",
            },
        }
        self.mock_slack_client = Mock(spec=SlackClient)
        self.group = "openshift-4.18"
        self.assembly = "test-assembly"
        self.github_token = "gh_token"
        self.gitlab_token = "gl_token"
        self.job_url = "http://jenkins/job/test-job/1"

    def test_init(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        self.assertEqual(pipeline.build_repo_pull_url, self.runtime.config["build_config"]["ocp_build_data_url"])
        self.assertEqual(pipeline.build_data_gitref, None)
        self.assertEqual(pipeline.build_data_push_url, self.runtime.config["build_config"]["ocp_build_data_push_url"])
        self.assertEqual(pipeline.shipment_repo_pull_url, self.runtime.config["shipment_config"]["shipment_data_url"])
        self.assertEqual(pipeline.shipment_repo_push_url, self.runtime.config["shipment_config"]["shipment_data_push_url"])
    
    def test_init_empty_config(self):
        self.runtime.config = {}
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        self.assertEqual(pipeline.build_repo_pull_url, constants.OCP_BUILD_DATA_URL)
        self.assertEqual(pipeline.build_data_gitref, None)
        self.assertEqual(pipeline.build_data_push_url, constants.OCP_BUILD_DATA_URL)
        self.assertEqual(pipeline.shipment_repo_pull_url, SHIPMENT_DATA_URL_TEMPLATE.format("ocp"))
        self.assertEqual(pipeline.shipment_repo_push_url, SHIPMENT_DATA_URL_TEMPLATE.format("ocp"))
    
    def test_init_with_custom_urls(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
            build_repo_url="https://github.com/foo/build-repo@branch",
            shipment_repo_url="https://gitlab.com/bar/shipment-repo",
        )
        self.assertEqual(pipeline.build_repo_pull_url, "https://github.com/foo/build-repo")
        self.assertEqual(pipeline.build_data_gitref, "branch")
        self.assertEqual(pipeline.build_data_push_url, self.runtime.config["build_config"]["ocp_build_data_push_url"])
        self.assertEqual(pipeline.shipment_repo_pull_url, "https://gitlab.com/bar/shipment-repo")
        self.assertEqual(pipeline.shipment_repo_push_url, self.runtime.config["shipment_config"]["shipment_data_push_url"])

    @patch('pyartcd.pipelines.prepare_release_konflux.GitRepository', return_value=AsyncMock())
    async def test_setup_repos(self, MockGitRepository):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )

        mock_build_repo_instance = Mock()
        mock_shipment_repo_instance = Mock()
        mock_build_repo_instance.setup = AsyncMock()
        mock_build_repo_instance.fetch_switch_branch = AsyncMock()
        mock_build_repo_instance.read_file = AsyncMock()
        mock_shipment_repo_instance.setup = AsyncMock()
        mock_shipment_repo_instance.fetch_switch_branch = AsyncMock()

        MockGitRepository.side_effect = [mock_build_repo_instance, mock_shipment_repo_instance]

        mock_build_repo_instance.read_file.side_effect = lambda filename: "releases_content" if filename == "releases.yml" else "group_content"

        await pipeline.setup_repos()

        self.assertEqual(MockGitRepository.call_count, 2)
        mock_build_repo_instance.setup.assert_awaited_once()
        mock_shipment_repo_instance.setup.assert_awaited_once()
        mock_build_repo_instance.fetch_switch_branch.assert_awaited_once()
        mock_shipment_repo_instance.fetch_switch_branch.assert_awaited_once()
        mock_build_repo_instance.read_file.assert_any_call("releases.yml")
        mock_build_repo_instance.read_file.assert_any_call("group.yml")

    async def test_validate_assembly_valid(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        pipeline.releases_config = Model(
            {
                "releases": {
                    "test-assembly": {"assembly": {"type": AssemblyTypes.STANDARD.value, "group": {"product": "ocp"}}}
                }
            }
        )
        pipeline.group_config = Model({"product": "ocp"})
        await pipeline.validate_assembly()  # Should not raise

    async def test_validate_assembly_missing_assembly(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        pipeline.releases_config = Model({"releases": {}})
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Assembly not found: test-assembly", str(context.exception))

    async def test_validate_assembly_stream_type(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        pipeline.releases_config = Model(
            {"releases": {self.assembly: {"assembly": {"type": AssemblyTypes.STREAM.value}}}}
        )
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Preparing a release from a stream assembly is no longer supported.", str(context.exception))

    async def test_validate_assembly_product_mismatch(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        pipeline.releases_config = Model(
            {
                "releases": {
                    "test-assembly": {
                        "assembly": {
                            "type": AssemblyTypes.STANDARD.value,
                            "group": {"product": "other-product"},
                        },
                    },
                },
            },
        )
        pipeline.group_config = Model({})  # No product override from group.yml
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Product mismatch: other-product != ocp.", str(context.exception))

        # Second part: group_config product takes precedence
        pipeline.releases_config = Model(
            {
                "releases": {
                    "test-assembly": {
                        "assembly": {
                            "type": AssemblyTypes.STANDARD.value,
                            "group": {},  # No product in assembly definition
                        }
                    }
                }
            }
        )
        pipeline.group_config = Model({"product": "another-product"})  # Product defined in group.yml
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Product mismatch: another-product != ocp.", str(context.exception))
