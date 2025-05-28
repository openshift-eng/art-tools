import copy
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, call, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.model import Model
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import (
    Data,
    Environments,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    Spec,
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
        self.assertEqual(
            pipeline.shipment_repo_push_url, self.runtime.config["shipment_config"]["shipment_data_push_url"]
        )

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
        self.assertEqual(
            pipeline.shipment_repo_push_url, self.runtime.config["shipment_config"]["shipment_data_push_url"]
        )

    @patch('pyartcd.pipelines.prepare_release_konflux.GitRepository')
    async def test_setup_repos(self, MockGitRepositoryClass):
        # Mock file contents for YAML parsing
        file_contents = {"releases.yml": 'releases_key: releases_value', "group.yml": 'group_key: group_value'}

        # Create mock repositories
        mock_build_repo = AsyncMock(spec=GitRepository)
        mock_build_repo.read_file.side_effect = lambda f: file_contents[f]

        mock_shipment_repo = AsyncMock(spec=GitRepository)

        MockGitRepositoryClass.side_effect = [mock_build_repo, mock_shipment_repo]

        # Create pipeline and call setup_repos
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            github_token=self.github_token,
            gitlab_token=self.gitlab_token,
        )
        await pipeline.setup_repos()

        # Verify GitRepository instantiation
        self.assertEqual(MockGitRepositoryClass.call_count, 2)

        # Verify build repo setup
        mock_build_repo.setup.assert_awaited_once_with(
            remote_url=pipeline.build_data_push_url,
            upstream_remote_url=pipeline.build_repo_pull_url,
        )
        mock_build_repo.fetch_switch_branch.assert_awaited_once_with(self.group)
        mock_build_repo.read_file.assert_has_awaits([call("releases.yml"), call("group.yml")])

        # Verify shipment repo setup
        mock_shipment_repo.setup.assert_awaited_once_with(
            remote_url=pipeline.basic_auth_url(pipeline.shipment_repo_push_url, pipeline.gitlab_token),
            upstream_remote_url=pipeline.shipment_repo_pull_url,
        )
        mock_shipment_repo.fetch_switch_branch.assert_awaited_once_with("main")
        mock_shipment_repo.read_file.assert_not_awaited()

        # Verify config parsing
        self.assertEqual(pipeline.releases_config, {'releases_key': 'releases_value'})
        self.assertEqual(pipeline.group_config, {'group_key': 'group_value'})

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

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    async def test_prepare_shipment_new_mr_prod_env(
        self, mock_init_shipment, mock_find_builds, mock_create_mr, mock_create_pr, mock_errata_api
    ):
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
                    self.assembly: {
                        "assembly": {
                            "group": {
                                "shipment": {
                                    "env": "prod",
                                    "advisories": [{"kind": "image"}, {"kind": "extras"}],
                                    # No 'url'
                                },
                                "advisories": {"rpm": 123},
                            }
                        }
                    }
                }
            }
        )

        mock_live_id = "LIVE_ID_FROM_ERRATA"
        mock_errata_api_instance = mock_errata_api.return_value
        mock_errata_api_instance.reserve_live_id = AsyncMock(return_value=mock_live_id)
        mock_errata_api_instance.close = AsyncMock()

        mock_data = Data(
            releaseNotes=ReleaseNotes(
                type="RHBA",
                synopsis="synopsis",
                topic="topic",
                description="description",
                solution="solution",
            )
        )

        mock_shipment_image_data = Shipment(
            metadata=Metadata(product="ocp", group=self.group, assembly=self.assembly, application="app-image"),
            snapshot=Snapshot(spec=Spec(nvrs=[]), name="snapshot1"),
            environments=Environments(
                stage=ShipmentEnv(releasePlan="rp-img-stage"), prod=ShipmentEnv(releasePlan="rp-img-prod")
            ),
            data=mock_data,
        )
        mock_shipment_image = ShipmentConfig(shipment=mock_shipment_image_data)

        mock_shipment_extras_data = Shipment(
            metadata=Metadata(product="ocp", group=self.group, assembly=self.assembly, application="app-extras"),
            snapshot=Snapshot(spec=Spec(nvrs=[]), name="snapshot2"),
            environments=Environments(
                stage=ShipmentEnv(releasePlan="rp-ext-stage"), prod=ShipmentEnv(releasePlan="rp-ext-prod")
            ),
            data=mock_data,
        )
        mock_shipment_extras = ShipmentConfig(shipment=mock_shipment_extras_data)

        # copy since the original objects will be modified
        mock_init_shipment.side_effect = [copy.deepcopy(mock_shipment_image), copy.deepcopy(mock_shipment_extras)]
        mock_find_builds.side_effect = [Spec(nvrs=["nvr1"]), Spec(nvrs=["nvr2"])]
        mock_create_mr.return_value = "https://gitlab.example.com/mr/1"

        await pipeline.prepare_shipment()

        mock_errata_api.assert_called_once()
        self.assertEqual(mock_errata_api_instance.reserve_live_id.call_count, 2)

        mock_init_shipment.assert_any_call("image")
        mock_init_shipment.assert_any_call("extras")
        self.assertEqual(mock_init_shipment.call_count, 2)

        mock_find_builds.assert_any_call("image")
        mock_find_builds.assert_any_call("extras")
        self.assertEqual(mock_find_builds.call_count, 2)

        mock_create_mr.assert_awaited_once()
        generated_shipments_arg = mock_create_mr.call_args[0][0]

        mock_shipment_image.shipment.snapshot.spec.nvrs = ["nvr1"]
        mock_shipment_image.shipment.data.releaseNotes.live_id = mock_live_id

        mock_shipment_extras.shipment.snapshot.spec.nvrs = ["nvr2"]
        mock_shipment_extras.shipment.data.releaseNotes.live_id = mock_live_id

        self.assertEqual(generated_shipments_arg["image"], mock_shipment_image)
        self.assertEqual(generated_shipments_arg["extras"], mock_shipment_extras)
        self.assertEqual(mock_create_mr.call_args[0][1], "prod")

        mock_create_pr.assert_awaited_once()
        final_shipment_config_arg = mock_create_pr.call_args[0][0]
        self.assertEqual(
            final_shipment_config_arg,
            {
                "env": "prod",
                "advisories": [
                    {"kind": "image", "live_id": mock_live_id},
                    {"kind": "extras", "live_id": mock_live_id},
                ],
                "url": "https://gitlab.example.com/mr/1",
            },
        )
        mock_errata_api_instance.close.assert_called_once()
