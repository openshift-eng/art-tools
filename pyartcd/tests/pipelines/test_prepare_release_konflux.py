import copy
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, call, patch

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.model import Model
from artcommonlib.util import convert_remote_git_to_ssh
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import (
    ComponentSource,
    Data,
    Environments,
    GitSource,
    Issue,
    Issues,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotComponent,
    SnapshotSpec,
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
                "ocp_build_data_url": "https://github.com/user1/repo1",
                "ocp_build_data_push_url": "https://github.com/user2/repo2",
            },
            "shipment_config": {
                "shipment_data_url": "https://gitlab.example.com/user3/repo3",
                "shipment_data_push_url": "https://gitlab.example.com/user4/repo4",
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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token

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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token

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
            build_repo_url="https://github.com/foo/build-repo@branch",
            shipment_repo_url="https://gitlab.com/bar/shipment-repo",
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token

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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token

        await pipeline.setup_repos()

        # Verify GitRepository instantiation
        self.assertEqual(MockGitRepositoryClass.call_count, 2)

        # Verify build repo setup
        mock_build_repo.setup.assert_awaited_once_with(
            remote_url=convert_remote_git_to_ssh(pipeline.build_data_push_url),
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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
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
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
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

    async def test_validate_shipment_config_no_advisories(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
        pipeline.releases_config = Model(
            {
                "releases": {
                    self.assembly: {
                        "assembly": {
                            "group": {
                                "shipment": {
                                    "env": "prod",
                                    # No advisories
                                },
                                "advisories": {"rpm": 123},
                            }
                        }
                    }
                }
            }
        )
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_shipment_config(pipeline.shipment_config)
        self.assertIn("Shipment config should specify which advisories to create and prepare", str(context.exception))

    async def test_validate_shipment_config_no_kind(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
        pipeline.releases_config = Model(
            {
                "releases": {
                    self.assembly: {
                        "assembly": {
                            "group": {
                                "shipment": {
                                    "env": "prod",
                                    "advisories": [{"live_id": 123}],  # No 'kind' specified
                                },
                                "advisories": {"rpm": 123},
                            }
                        }
                    }
                }
            }
        )
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_shipment_config(pipeline.shipment_config)
        self.assertIn("Shipment config should specify `kind` for each advisory", str(context.exception))

    async def test_validate_shipment_config_overlap(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
        pipeline.releases_config = Model(
            {
                "releases": {
                    self.assembly: {
                        "assembly": {
                            "group": {
                                "shipment": {
                                    "env": "prod",
                                    "advisories": [{"kind": "image"}],  # overlap with group advisories
                                },
                                "advisories": {"image": 123},
                            }
                        }
                    }
                }
            }
        )
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_shipment_config(pipeline.shipment_config)
        self.assertIn(
            "Shipment config should not specify advisories that are already defined in assembly.group.advisories",
            str(context.exception),
        )

    async def test_validate_shipment_config_invalid_env(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
        pipeline.releases_config = Model(
            {
                "releases": {
                    self.assembly: {
                        "assembly": {
                            "group": {
                                "shipment": {
                                    "env": "production",  # Invalid env
                                    "advisories": [{"kind": "image"}],
                                },
                                "advisories": {"rpm": 123},
                            }
                        }
                    }
                }
            }
        )
        with self.assertRaises(ValueError) as context:
            await pipeline.validate_shipment_config(pipeline.shipment_config)
        self.assertIn("Shipment config `env` should be either `prod` or `stage`", str(context.exception))

    @patch.object(PrepareReleaseKonfluxPipeline, 'attach_cve_flaws', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'update_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_bugs', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds_all', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_or_build_bundle_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'get_snapshot', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    async def test_prepare_shipment_new_mr_prod_env(
        self,
        mock_init_shipment,
        mock_get_snapshot,
        mock_find_or_build_bundle_builds,
        mock_find_builds_all,
        mock_find_bugs,
        mock_create_shipment_mr,
        mock_create_build_data_pr,
        mock_update_shipment_mr,
        mock_errata_api,
        *_,
    ):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token
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

        mock_shipment_image = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    group=self.group,
                    assembly=self.assembly,
                    application="app-image",
                ),
                snapshot=Snapshot(
                    nvrs=[],
                    spec=SnapshotSpec(
                        application="app-image",
                        components=[],
                    ),
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="rp-img-stage"), prod=ShipmentEnv(releasePlan="rp-img-prod")
                ),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis="synopsis",
                        topic="topic",
                        description="description",
                        solution="solution",
                    )
                ),
            )
        )
        mock_shipment_extras = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    group=self.group,
                    assembly=self.assembly,
                    application="app-extras",
                ),
                snapshot=Snapshot(
                    nvrs=[],
                    spec=SnapshotSpec(
                        application="app-extras",
                        components=[],
                    ),
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="rp-ext-stage"), prod=ShipmentEnv(releasePlan="rp-ext-prod")
                ),
                data=Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis="synopsis",
                        topic="topic",
                        description="description",
                        solution="solution",
                    )
                ),
            )
        )

        def init_shipment(kind):
            return {
                "image": mock_shipment_image,
                "extras": mock_shipment_extras,
            }.get(kind)

        mock_init_shipment.side_effect = init_shipment

        def find_builds_all():
            return {
                "image": ["image-nvr"],
                "extras": ["extras-nvr"],
                "metadata": ["olm-nvr"],
                "olm_builds_not_found": ["new-operatorm-builds"],
            }

        def find_or_build_bundle_builds(nvr):
            return ["new-olm-builds"]

        mock_find_builds_all.side_effect = find_builds_all
        mock_find_or_build_bundle_builds.side_effect = find_or_build_bundle_builds

        def find_bugs(kind, **_):
            return {
                "image": Issues(fixed=[Issue(id="IMAGEBUG", source="issues.redhat.com")]),
                "extras": Issues(fixed=[Issue(id="EXTRASBUG", source="issues.redhat.com")]),
            }.get(kind)

        mock_find_bugs.side_effect = find_bugs
        mock_create_shipment_mr.return_value = "https://gitlab.example.com/mr/1"
        mock_update_shipment_mr.return_value = "https://gitlab.example.com/mr/1"

        def get_snapshot(builds):
            if "image-nvr" in builds:
                return Snapshot(
                    nvrs=["image-nvr"],
                    spec=SnapshotSpec(
                        application="app-image",
                        components=[
                            SnapshotComponent(
                                name="test-image-component",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-image.git", revision="abc123")
                                ),
                                containerImage="test-image:latest",
                            ),
                        ],
                    ),
                )
            elif "extras-nvr" in builds:
                return Snapshot(
                    nvrs=["extras-nvr"],
                    spec=SnapshotSpec(
                        application="app-extras",
                        components=[
                            SnapshotComponent(
                                name="test-extras-component",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-extras.git", revision="def456")
                                ),
                                containerImage="test-extras:latest",
                            ),
                        ],
                    ),
                )

        mock_get_snapshot.side_effect = get_snapshot

        await pipeline.prepare_shipment()

        # assert liveID is reserved
        mock_errata_api.assert_called_once()
        self.assertEqual(mock_errata_api_instance.reserve_live_id.call_count, 2)

        # assert shipment init calls and get_snapshot calls
        mock_init_shipment.assert_any_call("extras")
        mock_init_shipment.assert_any_call("image")
        self.assertEqual(mock_init_shipment.call_count, 2)
        self.assertEqual(mock_find_builds_all.call_count, 1)
        self.assertEqual(mock_find_or_build_bundle_builds.call_count, 1)
        mock_get_snapshot.assert_any_call(['extras-nvr'])
        mock_get_snapshot.assert_any_call(['image-nvr'])
        self.assertEqual(mock_get_snapshot.call_count, 2)

        # copy and modify mocks to what is expected after init and build finding, i.e., at create shipment MR time
        mock_shipment_image_create = copy.deepcopy(mock_shipment_image)
        mock_shipment_extras_create = copy.deepcopy(mock_shipment_extras)
        mock_shipment_image_create.shipment.data.releaseNotes.live_id = mock_live_id
        mock_shipment_image_create.shipment.snapshot.nvrs = ["image-nvr"]
        mock_shipment_extras_create.shipment.data.releaseNotes.live_id = mock_live_id
        mock_shipment_extras_create.shipment.snapshot.nvrs = ["extras-nvr"]

        # assert MR is created with the right shipment configs
        mock_create_shipment_mr.assert_awaited_once()
        created_shipments_arg = mock_create_shipment_mr.call_args[0][0]
        self.assertEqual(created_shipments_arg["image"], mock_shipment_image_create)
        self.assertEqual(created_shipments_arg["extras"], mock_shipment_extras_create)
        self.assertEqual(mock_create_shipment_mr.call_args[0][1], "prod")

        # assert that build-data PR was created with the right config
        mock_create_build_data_pr.assert_awaited_once()
        self.assertEqual(
            mock_create_build_data_pr.call_args[0][0],
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

        # assert bug finding was done and MR updated with the right shipment configs
        mock_find_bugs.assert_any_call("extras", permissive=False)
        mock_find_bugs.assert_any_call("image", permissive=False)
        self.assertEqual(mock_find_bugs.call_count, 2)

        self.assertEqual(mock_update_shipment_mr.call_count, 2)
        updated_shipments_arg = mock_update_shipment_mr.call_args[0][0]

        mock_shipment_image_update = copy.deepcopy(mock_shipment_image_create)
        mock_shipment_extras_update = copy.deepcopy(mock_shipment_extras_create)
        mock_shipment_image_update.shipment.data.releaseNotes.issues = Issues(
            fixed=[Issue(id="IMAGEBUG", source="issues.redhat.com")]
        )
        mock_shipment_extras_update.shipment.data.releaseNotes.issues = Issues(
            fixed=[Issue(id="EXTRASBUG", source="issues.redhat.com")]
        )
        self.assertEqual(updated_shipments_arg["image"], mock_shipment_image_update)
        self.assertEqual(updated_shipments_arg["extras"], mock_shipment_extras_update)
        self.assertEqual(mock_update_shipment_mr.call_args[0][1], "prod")
