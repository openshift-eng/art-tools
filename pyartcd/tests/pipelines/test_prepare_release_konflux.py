import unittest
from unittest.mock import Mock, AsyncMock, patch, call
from pathlib import Path
from pyartcd import constants
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
from elliottlib.shipment_model import (
    ShipmentConfig, ShipmentMetadata, Shipment, Snapshot, Spec,
    ShipmentEnvironments, ShipmentEnvDetails
)
from elliottlib.errata_async import AsyncErrataAPI
from pyartcd.pipelines.prepare_release_konflux import PrepareReleaseKonfluxPipeline
from pyartcd.runtime import Runtime
from artcommonlib.slack_client import SlackClient
from pyartcd.git import GitRepository

class TestPrepareReleaseKonfluxPipeline(unittest.TestCase):
    def setUp(self):
        self.runtime = Mock(spec=Runtime)
        self.runtime.working_dir = Path("/tmp/working-dir")
        self.runtime.dry_run = False
        self.runtime.config = {
            "gitlab_url": "https://gitlab.example.com",
            "build_config": {},
            "shipment_config": {}
        }
        self.mock_slack_client = Mock(spec=SlackClient)

    def test_init_minimal_params(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group="test-group",
            assembly="test-assembly",
            build_repo_url=None,
            shipment_repo_url=None,
            github_token="gh_token",
            gitlab_token="gl_token",
            job_url="http://example.com/job"
        )
        self.assertEqual(pipeline.group, "test-group")
        self.assertEqual(pipeline.assembly, "test-assembly")
        self.assertEqual(pipeline.github_token, "gh_token")
        self.assertEqual(pipeline.gitlab_token, "gl_token")
        self.assertEqual(pipeline.job_url, "http://example.com/job")
        self.assertEqual(pipeline.working_dir, Path("/tmp/working-dir"))
        self.assertEqual(pipeline.elliott_working_dir, Path("/tmp/working-dir/elliott-working"))
        self.assertEqual(pipeline._build_repo_dir, Path("/tmp/working-dir/ocp-build-data-push"))
        self.assertEqual(pipeline._shipment_repo_dir, Path("/tmp/working-dir/shipment-data-push"))
        self.assertEqual(pipeline.build_repo_pull_url, constants.OCP_BUILD_DATA_URL)
        self.assertIsNone(pipeline.build_data_gitref)
        self.assertEqual(pipeline.build_data_push_url, constants.OCP_BUILD_DATA_URL)
        self.assertEqual(pipeline.shipment_repo_pull_url, SHIPMENT_DATA_URL_TEMPLATE.format(product="ocp"))
        self.assertEqual(pipeline.shipment_repo_push_url, SHIPMENT_DATA_URL_TEMPLATE.format(product="ocp"))

    def test_init_with_custom_urls(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group="another-group",
            assembly="another-assembly",
            build_repo_url="https://custom.build/repo@branch",
            shipment_repo_url="https://custom.shipment/repo",
            github_token="gh_token2",
            gitlab_token="gl_token2",
            job_url=None
        )
        self.assertEqual(pipeline.build_repo_pull_url, "https://custom.build/repo")
        self.assertEqual(pipeline.build_data_gitref, "branch")
        self.assertEqual(pipeline.build_data_push_url, constants.OCP_BUILD_DATA_URL)
        self.assertEqual(pipeline.shipment_repo_pull_url, "https://custom.shipment/repo")
        self.assertEqual(pipeline.shipment_repo_push_url, SHIPMENT_DATA_URL_TEMPLATE.format(product="ocp"))

    def test_init_with_config_override_urls(self):
        self.runtime.config["build_config"]["ocp_build_data_url"] = "https://config.build/repo"
        self.runtime.config["build_config"]["ocp_build_data_push_url"] = "https://config.build.push/repo"
        self.runtime.config["shipment_config"]["shipment_data_url"] = "https://config.shipment/repo"
        self.runtime.config["shipment_config"]["shipment_data_push_url"] = "https://config.shipment.push/repo"

        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group="cg-group",
            assembly="cg-assembly",
            build_repo_url=None,
            shipment_repo_url=None,
            github_token="gh_token3",
            gitlab_token="gl_token3",
            job_url=None
        )
        self.assertEqual(pipeline.build_repo_pull_url, "https://config.build/repo")
        self.assertEqual(pipeline.build_data_push_url, "https://config.build.push/repo")
        self.assertEqual(pipeline.shipment_repo_pull_url, "https://config.shipment/repo")
        self.assertEqual(pipeline.shipment_repo_push_url, "https://config.shipment.push/repo")

    def test_example(self):
        self.assertTrue(True)

    def test_build_repo_vars_defaults(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, gitref, push_url = pipeline._build_repo_vars(None)
        self.assertEqual(pull_url, constants.OCP_BUILD_DATA_URL)
        self.assertIsNone(gitref)
        self.assertEqual(push_url, constants.OCP_BUILD_DATA_URL)

    def test_build_repo_vars_custom_url(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, gitref, push_url = pipeline._build_repo_vars("https://custom.build/repo")
        self.assertEqual(pull_url, "https://custom.build/repo")
        self.assertIsNone(gitref)
        self.assertEqual(push_url, constants.OCP_BUILD_DATA_URL)

    def test_build_repo_vars_custom_url_with_gitref(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, gitref, push_url = pipeline._build_repo_vars("https://custom.build/repo@mybranch")
        self.assertEqual(pull_url, "https://custom.build/repo")
        self.assertEqual(gitref, "mybranch")
        self.assertEqual(push_url, constants.OCP_BUILD_DATA_URL)

    def test_build_repo_vars_with_config_override(self):
        self.runtime.config["build_config"]["ocp_build_data_url"] = "https://config.build/url"
        self.runtime.config["build_config"]["ocp_build_data_push_url"] = "https://config.push.build/url"
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, gitref, push_url = pipeline._build_repo_vars(None)
        self.assertEqual(pull_url, "https://config.build/url")
        self.assertIsNone(gitref)
        self.assertEqual(push_url, "https://config.push.build/url")
        # Reset config for other tests
        self.runtime.config["build_config"] = {}

    def test_shipment_repo_vars_defaults(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, push_url = pipeline._shipment_repo_vars(None)
        self.assertEqual(pull_url, SHIPMENT_DATA_URL_TEMPLATE.format(product="ocp"))
        self.assertEqual(push_url, SHIPMENT_DATA_URL_TEMPLATE.format(product="ocp"))

    def test_shipment_repo_vars_custom_url(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, push_url = pipeline._shipment_repo_vars("https://custom.shipment/repo")
        self.assertEqual(pull_url, "https://custom.shipment/repo")
        self.assertEqual(push_url, SHIPMENT_DATA_URL_TEMPLATE.format(product="ocp"))

    def test_shipment_repo_vars_with_config_override(self):
        self.runtime.config["shipment_config"]["shipment_data_url"] = "https://config.shipment/url"
        self.runtime.config["shipment_config"]["shipment_data_push_url"] = "https://config.push.shipment/url"
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "g", "a", None, None, "gh", "gl", None)
        pull_url, push_url = pipeline._shipment_repo_vars(None)
        self.assertEqual(pull_url, "https://config.shipment/url")
        self.assertEqual(push_url, "https://config.push.shipment/url")
        # Reset config for other tests
        self.runtime.config["shipment_config"] = {}

    @patch('shutil.rmtree')
    @patch('pathlib.Path.mkdir')
    def test_setup_working_dir(self, mock_mkdir, mock_rmtree):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group="test-group",
            assembly="test-assembly",
            build_repo_url=None,
            shipment_repo_url=None,
            github_token="gh_token",
            gitlab_token="gl_token",
            job_url="http://example.com/job"
        )
        pipeline.setup_working_dir()

        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

        # Check that rmtree was called for each directory
        expected_rmtree_calls = [
            call(pipeline._build_repo_dir, ignore_errors=True),
            call(pipeline._shipment_repo_dir, ignore_errors=True),
            call(pipeline.elliott_working_dir, ignore_errors=True)
        ]
        for expected_call in expected_rmtree_calls:
            self.assertIn(expected_call, mock_rmtree.call_args_list)

    @patch('pyartcd.pipelines.prepare_release_konflux.GitRepository', spec=GitRepository)
    async def test_setup_repos(self, MockGitRepository):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group="test-group",
            assembly="test-assembly",
            build_repo_url="https://build.url/repo@main", # build_data_gitref = main
            shipment_repo_url="https://ship.url/repo",
            github_token="gh_token",
            gitlab_token="gl_token",
            job_url=None
        )

        mock_build_repo_instance = Mock(spec=GitRepository)
        mock_shipment_repo_instance = Mock(spec=GitRepository)
        mock_build_repo_instance.setup = AsyncMock()
        mock_build_repo_instance.fetch_switch_branch = AsyncMock()
        mock_build_repo_instance.read_file = AsyncMock()
        mock_shipment_repo_instance.setup = AsyncMock()
        mock_shipment_repo_instance.fetch_switch_branch = AsyncMock()

        MockGitRepository.side_effect = [mock_build_repo_instance, mock_shipment_repo_instance]

        mock_releases_content = """
releases:
  test-assembly:
    assembly_type: standard
"""
        mock_group_content = "group_config_key: group_config_value"

        mock_build_repo_instance.read_file.side_effect = [mock_releases_content, mock_group_content]

        await pipeline.setup_repos()

        self.assertEqual(MockGitRepository.call_count, 2)

        mock_build_repo_instance.setup.assert_awaited_once_with(
            remote_url=pipeline.build_data_push_url,
            upstream_remote_url=pipeline.build_repo_pull_url
        )
        mock_shipment_repo_instance.setup.assert_awaited_once_with(
            remote_url=pipeline.basic_auth_url(pipeline.shipment_repo_push_url, pipeline.gitlab_token),
            upstream_remote_url=pipeline.shipment_repo_pull_url
        )

        mock_build_repo_instance.fetch_switch_branch.assert_awaited_once_with("main")
        mock_shipment_repo_instance.fetch_switch_branch.assert_awaited_once_with("main")

        mock_build_repo_instance.read_file.assert_any_call("releases.yml")
        mock_build_repo_instance.read_file.assert_any_call("group.yml")

        self.assertEqual(pipeline.releases_config, {"releases": {"test-assembly": {"assembly_type": "standard"}}})
        self.assertEqual(pipeline.group_config, {"group_config_key": "group_config_value"})

    async def test_validate_assembly_valid(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "test-group", "test-assembly", None, None, "gh", "gl", None)
        pipeline.releases_config = Model({
            "releases": {
                "test-assembly": {
                    "assembly": {
                        "type": AssemblyTypes.STANDARD.value,
                        "group": {"product": "ocp"}
                    }
                }
            }
        })
        pipeline.group_config = Model({"product": "ocp"})
        await pipeline.validate_assembly()  # Should not raise

    async def test_validate_assembly_missing_assembly(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "test-group", "nonexistent-assembly", None, None, "gh", "gl", None)
        pipeline.releases_config = Model({"releases": {"test-assembly": {}}})
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Assembly not found: nonexistent-assembly", str(context.exception))

    async def test_validate_assembly_stream_type(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "test-group", "stream-assembly", None, None, "gh", "gl", None)
        pipeline.releases_config = Model({
            "releases": {
                "stream-assembly": {"assembly": {"type": AssemblyTypes.STREAM.value}}
            }
        })
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Preparing a release from a stream assembly is no longer supported.", str(context.exception))

    async def test_validate_assembly_product_mismatch(self):
        pipeline = PrepareReleaseKonfluxPipeline(self.mock_slack_client, self.runtime, "test-group", "test-assembly", None, None, "gh", "gl", None)
        pipeline.releases_config = Model({
            "releases": {
                "test-assembly": {
                    "assembly": {
                        "type": AssemblyTypes.STANDARD.value,
                        "group": {"product": "other-product"}
                    }
                }
            }
        })
        pipeline.group_config = Model({})  # No product override from group.yml
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Product mismatch: other-product != ocp.", str(context.exception))

        # Second part: group_config product takes precedence
        pipeline.releases_config = Model({
            "releases": {
                "test-assembly": {
                    "assembly": {
                        "type": AssemblyTypes.STANDARD.value,
                        "group": {}  # No product in assembly definition
                    }
                }
            }
        })
        pipeline.group_config = Model({"product": "another-product"})  # Product defined in group.yml
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_assembly()
        self.assertIn("Product mismatch: another-product != ocp.", str(context.exception))

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    async def test_prepare_shipment_new_mr_stage_env(
        self, mock_init_shipment, mock_find_builds, mock_create_mr, mock_create_pr, MockErrataAPI
    ):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.16", "4.16.1", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({
            "releases": {
                "4.16.1": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.16": {
                                "env": "stage",
                                "advisories": [
                                    {"kind": "image"},
                                    {"kind": "extras"}
                                ]
                                # No 'url'
                            },
                            "advisories": {
                                "rpm": {}
                            }
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.16.1-test"  # Mock property

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.reserve_live_id = AsyncMock(return_value="LIVE_ID_FROM_ERRATA")
        mock_errata_api_instance.close = Mock()

        # Basic ShipmentConfig structure; details don't matter as much as the flow
        mock_shipment_image_data = Shipment(metadata=ShipmentMetadata(product="ocp", group="openshift-4.16", assembly="4.16.1", application="app-image"), snapshot=Snapshot(spec=Spec(nvrs=[])), environments=ShipmentEnvironments(stage=ShipmentEnvDetails(releasePlan="rp-img")))
        mock_shipment_image = ShipmentConfig(shipment=mock_shipment_image_data, kind="image")

        mock_shipment_extras_data = Shipment(metadata=ShipmentMetadata(product="ocp", group="openshift-4.16", assembly="4.16.1", application="app-extras"), snapshot=Snapshot(spec=Spec(nvrs=[])), environments=ShipmentEnvironments(stage=ShipmentEnvDetails(releasePlan="rp-ext")))
        mock_shipment_extras = ShipmentConfig(shipment=mock_shipment_extras_data, kind="extras")

        mock_init_shipment.side_effect = [mock_shipment_image, mock_shipment_extras]
        mock_find_builds.return_value = Spec(nvrs=["build1.nvr", "build2.nvr"])
        mock_create_mr.return_value = "https://gitlab.example.com/mr/1"

        await pipeline.prepare_shipment()

        MockErrataAPI.assert_called_once()
        mock_errata_api_instance.reserve_live_id.assert_not_called()

        mock_init_shipment.assert_any_call("image")
        mock_init_shipment.assert_any_call("extras")
        self.assertEqual(mock_init_shipment.call_count, 2)

        mock_find_builds.assert_any_call("image")
        mock_find_builds.assert_any_call("extras")
        self.assertEqual(mock_find_builds.call_count, 2)

        self.assertEqual(mock_shipment_image.shipment.snapshot.spec.nvrs, ["build1.nvr", "build2.nvr"])
        self.assertEqual(mock_shipment_extras.shipment.snapshot.spec.nvrs, ["build1.nvr", "build2.nvr"])
        self.assertIsNone(mock_shipment_image.shipment.environments.stage.liveId) # Should not be set in stage
        self.assertIsNone(mock_shipment_extras.shipment.environments.stage.liveId) # Should not be set in stage

        mock_create_mr.assert_awaited_once()
        generated_shipments_arg = mock_create_mr.call_args[0][0]
        self.assertEqual(generated_shipments_arg["image"], mock_shipment_image)
        self.assertEqual(generated_shipments_arg["extras"], mock_shipment_extras)
        self.assertEqual(mock_create_mr.call_args[0][1], "stage")

        self.mock_slack_client.say_in_thread.assert_any_call("Shipment MR created: https://gitlab.example.com/mr/1")

        mock_create_pr.assert_awaited_once()
        final_shipment_config_arg = mock_create_pr.call_args[0][0]
        self.assertEqual(final_shipment_config_arg["url"], "https://gitlab.example.com/mr/1")
        mock_errata_api_instance.close.assert_called_once()

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    async def test_prepare_shipment_new_mr_prod_env_needs_liveid(
        self, mock_init_shipment, mock_find_builds, mock_create_mr, mock_create_pr, MockErrataAPI
    ):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.16", "4.16.1", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({  # Using Model to wrap the dict
            "releases": {
                "4.16.1": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.16": {
                                "env": "prod",  # Explicitly prod
                                "advisories": [
                                    {"kind": "image"},  # No live_id, prod env
                                    {"kind": "extras"}  # No live_id, prod env
                                ]
                                # No 'url'
                            },
                            "advisories": {
                                "rpm": {}
                            }
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.16.1-test"  # Mock property

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.reserve_live_id = AsyncMock(return_value="LIVE_ID_FROM_ERRATA")
        mock_errata_api_instance.close = Mock()

        # Mock ShipmentConfig objects that init_shipment would return
        mock_shipment_image_data = Shipment(
            metadata=ShipmentMetadata(product="ocp", group="openshift-4.16", assembly="4.16.1", application="app-image-prod"),
            snapshot=Snapshot(spec=Spec(nvrs=[])),
            environments=ShipmentEnvironments(prod=ShipmentEnvDetails(releasePlan="rp-img-prod")) # Ensure prod env details
        )
        mock_shipment_image = ShipmentConfig(shipment=mock_shipment_image_data, kind="image")

        mock_shipment_extras_data = Shipment(
            metadata=ShipmentMetadata(product="ocp", group="openshift-4.16", assembly="4.16.1", application="app-extras-prod"),
            snapshot=Snapshot(spec=Spec(nvrs=[])),
            environments=ShipmentEnvironments(prod=ShipmentEnvDetails(releasePlan="rp-ext-prod")) # Ensure prod env details
        )
        mock_shipment_extras = ShipmentConfig(shipment=mock_shipment_extras_data, kind="extras")

        mock_init_shipment.side_effect = [mock_shipment_image, mock_shipment_extras]
        mock_find_builds.return_value = Spec(nvrs=["build1.nvr", "build2.nvr"])
        mock_create_mr.return_value = "https://gitlab.example.com/mr/prod/1"

        await pipeline.prepare_shipment()

        MockErrataAPI.assert_called_once()
        # reserve_live_id should be called for both image and extras as env is prod and no live_id is provided
        self.assertEqual(mock_errata_api_instance.reserve_live_id.call_count, 2)
        mock_errata_api_instance.reserve_live_id.assert_any_call(product_id=pipeline.errata_product_id, release_name=pipeline.release_name)


        mock_init_shipment.assert_any_call("image")
        mock_init_shipment.assert_any_call("extras")
        self.assertEqual(mock_init_shipment.call_count, 2)

        mock_find_builds.assert_any_call("image")
        mock_find_builds.assert_any_call("extras")
        self.assertEqual(mock_find_builds.call_count, 2)

        self.assertEqual(mock_shipment_image.shipment.snapshot.spec.nvrs, ["build1.nvr", "build2.nvr"])
        self.assertEqual(mock_shipment_extras.shipment.snapshot.spec.nvrs, ["build1.nvr", "build2.nvr"])

        # Check that liveId was set by reserve_live_id
        self.assertEqual(mock_shipment_image.shipment.environments.prod.liveId, "LIVE_ID_FROM_ERRATA")
        self.assertEqual(mock_shipment_extras.shipment.environments.prod.liveId, "LIVE_ID_FROM_ERRATA")


        mock_create_mr.assert_awaited_once()
        generated_shipments_arg = mock_create_mr.call_args[0][0]
        self.assertEqual(generated_shipments_arg["image"], mock_shipment_image)
        self.assertEqual(generated_shipments_arg["extras"], mock_shipment_extras)
        self.assertEqual(mock_create_mr.call_args[0][1], "prod")

        self.mock_slack_client.say_in_thread.assert_any_call("Shipment MR created: https://gitlab.example.com/mr/prod/1")

        mock_create_pr.assert_awaited_once()
        final_shipment_config_arg = mock_create_pr.call_args[0][0]
        self.assertEqual(final_shipment_config_arg["url"], "https://gitlab.example.com/mr/prod/1")
        mock_errata_api_instance.close.assert_called_once()

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'update_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'validate_shipment_mr')
    async def test_prepare_shipment_existing_mr_needs_update(
        self, mock_validate_mr, mock_update_mr, mock_init_shipment, mock_find_builds,
        mock_create_build_pr, MockErrataAPI
    ):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.17", "4.17.0", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({
            "releases": {
                "4.17.0": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.17": {
                                "env": "stage",
                                "advisories": [
                                    {"kind": "image"}
                                ],
                                "url": "https://gitlab.example.com/existing/mr/123"
                            },
                            "advisories": {"rpm": {}}
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.17.0-test"

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.reserve_live_id = AsyncMock()
        mock_errata_api_instance.close = Mock()

        mock_shipment_image = ShipmentConfig(
            shipment=Shipment(
                metadata=ShipmentMetadata(product="ocp", group="openshift-4.17", assembly="4.17.0", application="app"),
                snapshot=Snapshot(spec=Spec(nvrs=[])),
                environments=ShipmentEnvironments(stage=ShipmentEnvDetails(releasePlan="rp"))
            ),
            kind="image"
        )
        mock_init_shipment.return_value = mock_shipment_image
        mock_find_builds.return_value = Spec(nvrs=["build1.nvr"])
        mock_validate_mr.return_value = None  # Does not raise
        mock_update_mr.return_value = True  # Indicates MR was updated

        await pipeline.prepare_shipment()

        mock_validate_mr.assert_called_once_with("https://gitlab.example.com/existing/mr/123")
        mock_errata_api_instance.reserve_live_id.assert_not_called()
        mock_init_shipment.assert_awaited_once_with("image")
        mock_find_builds.assert_awaited_once_with("image")
        self.assertEqual(mock_shipment_image.shipment.snapshot.spec.nvrs, ["build1.nvr"])

        mock_update_mr.assert_awaited_once()
        generated_shipments_arg = mock_update_mr.call_args[0][0]
        self.assertEqual(generated_shipments_arg["image"], mock_shipment_image)
        self.assertEqual(mock_update_mr.call_args[0][1], "stage")
        self.assertEqual(mock_update_mr.call_args[0][2], "https://gitlab.example.com/existing/mr/123")

        self.mock_slack_client.say_in_thread.assert_any_call("Shipment MR updated: https://gitlab.example.com/existing/mr/123")

        mock_create_build_pr.assert_awaited_once()
        final_shipment_config_arg = mock_create_build_pr.call_args[0][0]
        self.assertEqual(final_shipment_config_arg["url"], "https://gitlab.example.com/existing/mr/123")
        mock_errata_api_instance.close.assert_called_once()

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'update_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'validate_shipment_mr')
    async def test_prepare_shipment_existing_mr_no_update_needed(
        self, mock_validate_mr, mock_update_mr, mock_init_shipment, mock_find_builds,
        mock_create_build_pr, MockErrataAPI
    ):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.17", "4.17.1", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({
            "releases": {
                "4.17.1": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.17": {
                                "env": "stage",
                                "advisories": [
                                    {"kind": "image"}
                                ],
                                "url": "https://gitlab.example.com/existing/mr/456"
                            },
                            "advisories": {"rpm": {}}
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.17.1-test"

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.close = Mock()

        mock_shipment_image = ShipmentConfig(
            shipment=Shipment(
                metadata=ShipmentMetadata(product="ocp", group="openshift-4.17", assembly="4.17.1", application="app"),
                snapshot=Snapshot(spec=Spec(nvrs=[])),
                environments=ShipmentEnvironments(stage=ShipmentEnvDetails(releasePlan="rp"))
            ),
            kind="image"
        )
        mock_init_shipment.return_value = mock_shipment_image
        mock_find_builds.return_value = Spec(nvrs=["build1.nvr"])
        mock_validate_mr.return_value = None  # Does not raise
        mock_update_mr.return_value = False  # Indicates MR was NOT updated

        await pipeline.prepare_shipment()

        mock_validate_mr.assert_called_once_with("https://gitlab.example.com/existing/mr/456")
        mock_init_shipment.assert_awaited_once_with("image")
        mock_find_builds.assert_awaited_once_with("image")
        mock_update_mr.assert_awaited_once() # Still called to check if update is needed

        # Verify that the slack message for "Shipment MR updated" was NOT sent.
        say_in_thread_calls = [call_args[0][0] for call_args in self.mock_slack_client.say_in_thread.call_args_list]
        self.assertNotIn("Shipment MR updated: https://gitlab.example.com/existing/mr/456", say_in_thread_calls)
        # Check if any other message related to MR update was sent (it shouldn't)
        for call_text in say_in_thread_calls:
            self.assertNotIn("updated", call_text.lower())


        mock_create_build_pr.assert_awaited_once() # This should still be called to update build data PR
        final_shipment_config_arg = mock_create_build_pr.call_args[0][0]
        self.assertEqual(final_shipment_config_arg["url"], "https://gitlab.example.com/existing/mr/456")
        mock_errata_api_instance.close.assert_called_once()

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'validate_shipment_mr')
    async def test_prepare_shipment_invalid_mr_url(self, mock_validate_mr, MockErrataAPI):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.17", "4.17.2", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({
            "releases": {
                "4.17.2": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.17": {
                                "env": "stage",
                                "advisories": [
                                    {"kind": "image"}
                                ],
                                "url": "https://gitlab.example.com/invalid/mr/789"
                            },
                            "advisories": {"rpm": {}}
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.17.2-test"

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.close = Mock()
        mock_validate_mr.side_effect = ValueError("Invalid MR state")

        with self.assertRaises(ValueError) as context:
            await pipeline.prepare_shipment()

        self.assertEqual(str(context.exception), "Invalid MR state")
        mock_validate_mr.assert_called_once_with("https://gitlab.example.com/invalid/mr/789")
        mock_errata_api_instance.close.assert_called_once()  # close() is called in a finally block

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    async def test_prepare_shipment_missing_advisories_in_config(self, MockErrataAPI):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.17", "4.17.3", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({
            "releases": {
                "4.17.3": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.17": {
                                "env": "stage"
                                # 'advisories' field is deliberately missing
                            },
                            "advisories": {"rpm": {}}
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.17.3-test"

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.close = Mock()

        with self.assertRaises(ValueError) as context:
            await pipeline.prepare_shipment()

        self.assertEqual(
            str(context.exception),
            "Operation not supported: shipment config should specify which advisories to create and prepare"
        )
        mock_errata_api_instance.close.assert_called_once()

    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    async def test_prepare_shipment_common_advisories_error(self, MockErrataAPI):
        pipeline = PrepareReleaseKonfluxPipeline(
            self.mock_slack_client, self.runtime, "openshift-4.17", "4.17.4", None, None,
            "gh_token", "gl_token", "http://job.url"
        )
        pipeline.product = "ocp"
        pipeline.releases_config = Model({
            "releases": {
                "4.17.4": {
                    "assembly": {
                        "group": {
                            "shipment-ocp-4.17": {
                                "env": "stage",
                                "advisories": [
                                    {"kind": "image"},
                                    {"kind": "extras"}
                                ]
                            },
                            "advisories": {
                                "image": {},
                                "rpm": {}
                            }
                        }
                    }
                }
            }
        })
        pipeline.release_name = "4.17.4-test"

        mock_errata_api_instance = MockErrataAPI.return_value
        mock_errata_api_instance.close = Mock()

        with self.assertRaises(ValueError) as context:
            await pipeline.prepare_shipment()

        self.assertEqual(
            str(context.exception),
            "shipment config should not specify advisories that are already defined in assembly.group.advisories: {'image'}"
        )
        mock_errata_api_instance.close.assert_called_once()
