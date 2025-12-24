import copy
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, call, patch

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
from pyartcd.git import GitRepository
from pyartcd.pipelines.prepare_release_konflux import PrepareReleaseKonfluxPipeline
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient

from pyartcd import constants


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

        self.assertEqual(pipeline.build_data_repo_pull_url, self.runtime.config["build_config"]["ocp_build_data_url"])
        self.assertEqual(pipeline.build_data_gitref, None)
        self.assertEqual(pipeline.build_data_push_url, self.runtime.config["build_config"]["ocp_build_data_push_url"])
        self.assertEqual(
            pipeline.shipment_data_repo_pull_url, self.runtime.config["shipment_config"]["shipment_data_url"]
        )
        self.assertEqual(
            pipeline.shipment_data_repo_push_url, self.runtime.config["shipment_config"]["shipment_data_push_url"]
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

        self.assertEqual(pipeline.build_data_repo_pull_url, constants.OCP_BUILD_DATA_URL)
        self.assertEqual(pipeline.build_data_gitref, None)
        self.assertEqual(pipeline.build_data_push_url, constants.OCP_BUILD_DATA_URL)
        self.assertEqual(pipeline.shipment_data_repo_pull_url, SHIPMENT_DATA_URL_TEMPLATE)
        self.assertEqual(pipeline.shipment_data_repo_push_url, SHIPMENT_DATA_URL_TEMPLATE)

    def test_init_with_custom_urls(self):
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            build_data_repo_url="https://github.com/foo/build-repo@branch",
            shipment_data_repo_url="https://gitlab.com/bar/shipment-repo",
        )
        pipeline.github_token = self.github_token
        pipeline.gitlab_token = self.gitlab_token

        self.assertEqual(pipeline.build_data_repo_pull_url, "https://github.com/foo/build-repo")
        self.assertEqual(pipeline.build_data_gitref, "branch")
        self.assertEqual(pipeline.build_data_push_url, self.runtime.config["build_config"]["ocp_build_data_push_url"])
        self.assertEqual(pipeline.shipment_data_repo_pull_url, "https://gitlab.com/bar/shipment-repo")
        self.assertEqual(
            pipeline.shipment_data_repo_push_url, self.runtime.config["shipment_config"]["shipment_data_push_url"]
        )

    @patch('pyartcd.pipelines.prepare_release_konflux.GitRepository')
    async def test_setup_repos(self, MockGitRepositoryClass):
        # Mock file contents for YAML parsing
        file_contents = {"releases.yml": 'releases_key: releases_value', "group.yml": 'group_key: group_value'}

        # Create mock repositories
        mock_build_data_repo = AsyncMock(spec=GitRepository)
        mock_build_data_repo.read_file.side_effect = lambda f: file_contents[f]

        mock_shipment_data_repo = AsyncMock(spec=GitRepository)

        MockGitRepositoryClass.side_effect = [mock_build_data_repo, mock_shipment_data_repo]

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
        mock_build_data_repo.setup.assert_awaited_once_with(
            remote_url=convert_remote_git_to_ssh(pipeline.build_data_push_url),
            upstream_remote_url=pipeline.build_data_repo_pull_url,
        )
        mock_build_data_repo.fetch_switch_branch.assert_awaited_once_with(self.group)
        mock_build_data_repo.read_file.assert_has_awaits([call("releases.yml"), call("group.yml")])

        # Verify shipment repo setup
        mock_shipment_data_repo.setup.assert_awaited_once_with(
            remote_url=pipeline.basic_auth_url(pipeline.shipment_data_repo_push_url, pipeline.gitlab_token),
            upstream_remote_url=pipeline.shipment_data_repo_pull_url,
        )
        mock_shipment_data_repo.fetch_switch_branch.assert_awaited_once_with("main")
        mock_shipment_data_repo.read_file.assert_not_awaited()

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
                    "test-assembly": {
                        "assembly": {
                            "type": AssemblyTypes.STANDARD.value,
                            "group": {"product": "ocp", "release_date": "2025-Oct-22"},
                        }
                    }
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
                            "group": {"product": "other-product", "release_date": "2025-Oct-22"},
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

    @patch('pyartcd.pipelines.prepare_release_konflux.KonfluxDb')
    async def test_verify_attached_operators(self, MockKonfluxDb):
        """
        Tests that verify_attached_operators completes successfully when all
        operator and operand NVRs are present in the release builds.
        """
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
        )
        pipeline.logger = Mock()
        build = MagicMock(
            nvr="my-bundle-1.0", operator_nvr="my-operator-1.0", operand_nvrs=["my-operand-A-1.0", "my-operand-B-1.0"]
        )

        # Create async mock that returns the build
        async def return_build(*args, **kwargs):
            return build

        mock_kdb_instance = MockKonfluxDb.return_value
        mock_kdb_instance.bind = Mock()  # Mock the bind method
        mock_kdb_instance.get_latest_build = AsyncMock(side_effect=return_build)

        kind_to_builds = {
            "metadata": ["my-bundle-1.0"],
            "image": ["my-operator-1.0", "my-operand-A-1.0"],
            "extras": ["my-operand-B-1.0"],
        }

        # Should NOT raise an exception since all builds are present
        await pipeline.verify_attached_operators(kind_to_builds)

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

    @patch.object(
        PrepareReleaseKonfluxPipeline,
        "assembly_group_config",
        new_callable=PropertyMock,
    )
    async def test_prepare_rpm_advisory_creates_and_processes_advisory(self, mock_assembly_group_config):
        mock_assembly_group_config.return_value = {"advisories": {"rpm": -1}}
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly="4.18.0",  # Use the assembly that matches releases_config
        )
        pipeline.release_date = "2024-07-01"
        pipeline.assembly_type = AssemblyTypes.STANDARD
        pipeline.assembly = "4.18.0"
        pipeline.releases_config = Model(
            {
                "releases": {
                    "4.18.0": {
                        "assembly": {
                            "type": AssemblyTypes.STANDARD.value,
                            "group": {"product": "ocp", "release_date": "2025-Oct-22"},
                        }
                    }
                }
            }
        )
        pipeline.logger = Mock()
        pipeline._slack_client = AsyncMock()
        pipeline.create_advisory = AsyncMock(return_value=12345)
        pipeline.run_cmd_with_retry = AsyncMock()
        pipeline.execute_command_with_logging = AsyncMock(return_value='{"rpm": ["BUG-1", "BUG-2"]}')
        pipeline._elliott_base_command = [
            "elliott",
            "--group=openshift-4.18",
            "--assembly=4.18.0",
            "--build-system=konflux",
        ]

        pipeline.updated_assembly_group_config = Model({"advisories": {"rpm": -1}})

        # Mock git repository operations
        pipeline.build_data_repo = AsyncMock()
        pipeline.build_data_repo.does_branch_exist_on_remote = AsyncMock(return_value=False)
        pipeline.build_data_repo.create_branch = AsyncMock()
        pipeline.build_data_repo.commit_all = AsyncMock()
        pipeline.build_data_repo.push = AsyncMock()

        # Run the function
        with (
            patch("pyartcd.pipelines.prepare_release_konflux.push_cdn_stage") as mock_push_cdn_stage,
            patch("pyartcd.pipelines.prepare_release_konflux.GhApi") as mock_gh_api,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            # Mock GitHub API
            mock_api = Mock()
            mock_api.pulls.list.return_value = Mock(items=[])
            mock_gh_api.return_value = mock_api

            await pipeline.prepare_et_advisories()

        # Assertions
        self.assertEqual(
            pipeline.updated_assembly_group_config,
            Model({"advisories": {"rpm": 12345}}),
        )

        pipeline.create_advisory.assert_awaited_once()
        pipeline._slack_client.say_in_thread.assert_any_await(
            "ET rpm advisory 12345 created with release date 2024-07-01"
        )
        pipeline.run_cmd_with_retry.assert_any_await(
            [item for item in pipeline._elliott_base_command if item != '--build-system=konflux'],
            ["find-builds", "--kind=rpm", "--attach=12345", "--clean"],
        )
        pipeline.execute_command_with_logging.assert_awaited()
        pipeline.run_cmd_with_retry.assert_any_await(
            pipeline._elliott_base_command, ["attach-bugs", "BUG-1", "BUG-2", "--advisory=12345"]
        )
        mock_push_cdn_stage.assert_called_with(12345)

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

    @patch.object(PrepareReleaseKonfluxPipeline, 'verify_attached_operators', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'attach_cve_flaws', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_update_build_data_pr', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_konflux.AsyncErrataAPI', spec=AsyncErrataAPI)
    @patch.object(PrepareReleaseKonfluxPipeline, 'update_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'create_shipment_mr', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_bugs', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_or_build_fbc_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_or_build_bundle_builds', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'find_builds_all', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'get_snapshot', new_callable=AsyncMock)
    @patch.object(PrepareReleaseKonfluxPipeline, 'init_shipment', new_callable=AsyncMock)
    async def test_prepare_shipment_new_mr_prod_env(
        self,
        mock_init_shipment,
        mock_get_snapshot,
        mock_find_builds_all,
        mock_find_or_build_bundle_builds,
        mock_find_or_build_fbc_builds,
        mock_find_bugs,
        mock_create_shipment_mr,
        mock_update_shipment_mr,
        mock_errata_api,
        mock_create_update_build_data_pr,
        *_,
    ):
        group_config = {
            "shipment": {
                "env": "prod",
                "advisories": [
                    {"kind": "image"},
                    {"kind": "extras"},
                    {"kind": "metadata"},
                    {"kind": "fbc"},
                ],
                # No 'url'
            },
            "advisories": {"rpm": 123},
        }

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
                            "group": group_config,
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
                    stage=ShipmentEnv(releasePlan="rp-img-stage"),
                    prod=ShipmentEnv(releasePlan="rp-img-prod"),
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
                    stage=ShipmentEnv(releasePlan="rp-ext-stage"),
                    prod=ShipmentEnv(releasePlan="rp-ext-prod"),
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
        mock_shipment_metadata = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    group=self.group,
                    assembly=self.assembly,
                    application="app-metadata",
                ),
                snapshot=Snapshot(
                    nvrs=[],
                    spec=SnapshotSpec(
                        application="app-metadata",
                        components=[],
                    ),
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="rp-meta-stage"),
                    prod=ShipmentEnv(releasePlan="rp-meta-prod"),
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
        mock_shipment_fbc = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product="ocp",
                    group=self.group,
                    assembly=self.assembly,
                    application="app-fbc",
                    fbc=True,
                ),
                snapshot=Snapshot(
                    nvrs=[],
                    spec=SnapshotSpec(
                        application="app-fbc",
                        components=[],
                    ),
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan="rp-fbc-stage"),
                    prod=ShipmentEnv(releasePlan="rp-fbc-prod"),
                ),
            )
        )

        def init_shipment(kind):
            return {
                "image": mock_shipment_image,
                "extras": mock_shipment_extras,
                "metadata": mock_shipment_metadata,
                "fbc": mock_shipment_fbc,
            }.get(kind)

        mock_init_shipment.side_effect = init_shipment

        def find_builds_all():
            return {
                "image": ["image-nvr"],
                "extras": ["extras-nvr"],
                "metadata": [],
                "olm_builds_not_found": ["extras-nvr"],
            }

        mock_find_builds_all.side_effect = find_builds_all

        def find_or_build_bundle_builds(nvrs):
            return ["extras-bundle-nvr"]

        mock_find_or_build_bundle_builds.side_effect = find_or_build_bundle_builds

        def find_or_build_fbc_builds(nvrs):
            return ["fbc-nvr"], []  # Return tuple of (successful_nvrs, errors)

        mock_find_or_build_fbc_builds.side_effect = find_or_build_fbc_builds

        mock_find_bugs.return_value = {
            "image": ["IMAGEBUG"],
            "extras": ["EXTRASBUG"],
            "metadata": [],
        }

        mock_create_shipment_mr.return_value = "https://gitlab.example.com/mr/1"
        mock_update_shipment_mr.return_value = "https://gitlab.example.com/mr/1"
        mock_create_update_build_data_pr.return_value = True

        def get_snapshot(builds):
            if "image-nvr" in builds:
                return Snapshot(
                    nvrs=builds,
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
                    nvrs=builds,
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
            elif "extras-bundle-nvr" in builds:
                return Snapshot(
                    nvrs=builds,
                    spec=SnapshotSpec(
                        application="app-metadata",
                        components=[
                            SnapshotComponent(
                                name="test-metadata-component",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-metadata.git", revision="ghi789")
                                ),
                                containerImage="test-metadata:latest",
                            ),
                        ],
                    ),
                )
            elif "fbc-nvr" in builds:
                return Snapshot(
                    nvrs=builds,
                    spec=SnapshotSpec(
                        application="app-fbc",
                        components=[
                            SnapshotComponent(
                                name="test-fbc-component",
                                source=ComponentSource(
                                    git=GitSource(url="https://github.com/test-fbc.git", revision="jkl012")
                                ),
                                containerImage="test-fbc:latest",
                            ),
                        ],
                    ),
                )

        mock_get_snapshot.side_effect = get_snapshot

        pipeline.updated_assembly_group_config = Model(group_config)

        await pipeline.prepare_shipment()

        # assert liveID is reserved
        mock_errata_api.assert_called_once()
        self.assertEqual(mock_errata_api_instance.reserve_live_id.call_count, 3)

        # assert shipment init calls and get_snapshot calls
        mock_init_shipment.assert_any_call("extras")
        mock_init_shipment.assert_any_call("image")
        mock_init_shipment.assert_any_call("metadata")
        mock_init_shipment.assert_any_call("fbc")
        self.assertEqual(mock_init_shipment.call_count, 4)
        self.assertEqual(mock_find_builds_all.call_count, 1)
        self.assertEqual(mock_find_or_build_bundle_builds.call_count, 1)
        self.assertEqual(mock_find_or_build_fbc_builds.call_count, 1)
        mock_get_snapshot.assert_any_call(['extras-nvr'])
        mock_get_snapshot.assert_any_call(['image-nvr'])
        mock_get_snapshot.assert_any_call(['extras-bundle-nvr'])
        mock_get_snapshot.assert_any_call(['fbc-nvr'])
        self.assertEqual(mock_get_snapshot.call_count, 4)

        # copy and modify mocks to what is expected after init and build finding, i.e., at create shipment MR time
        mock_shipment_image_create = copy.deepcopy(mock_shipment_image)
        mock_shipment_extras_create = copy.deepcopy(mock_shipment_extras)
        mock_shipment_metadata_create = copy.deepcopy(mock_shipment_metadata)
        mock_shipment_fbc_create = copy.deepcopy(mock_shipment_fbc)
        mock_shipment_image_create.shipment.data.releaseNotes.live_id = mock_live_id
        mock_shipment_image_create.shipment.snapshot.nvrs = ["image-nvr"]
        mock_shipment_extras_create.shipment.data.releaseNotes.live_id = mock_live_id
        mock_shipment_extras_create.shipment.snapshot.nvrs = ["extras-nvr"]
        mock_shipment_metadata_create.shipment.data.releaseNotes.live_id = mock_live_id
        mock_shipment_metadata_create.shipment.snapshot.nvrs = ["extras-bundle-nvr"]
        # fbc does not get live_id
        mock_shipment_fbc_create.shipment.snapshot.nvrs = ["fbc-nvr"]

        # assert MR is created with the right shipment configs
        mock_create_shipment_mr.assert_awaited_once()
        created_shipments_arg = mock_create_shipment_mr.call_args[0][0]
        self.assertEqual(created_shipments_arg["image"], mock_shipment_image_create)
        self.assertEqual(created_shipments_arg["extras"], mock_shipment_extras_create)
        self.assertEqual(created_shipments_arg["metadata"], mock_shipment_metadata_create)
        self.assertEqual(created_shipments_arg["fbc"], mock_shipment_fbc_create)
        self.assertEqual(mock_create_shipment_mr.call_args[0][1], "prod")

        mock_errata_api_instance.close.assert_called_once()

        # assert bug finding was done and MR updated with the right shipment configs
        mock_find_bugs.assert_any_call()
        self.assertEqual(mock_find_bugs.call_count, 3)

        self.assertEqual(mock_update_shipment_mr.call_count, 3)
        updated_shipments_arg = mock_update_shipment_mr.call_args[0][0]

        mock_shipment_image_update = copy.deepcopy(mock_shipment_image_create)
        mock_shipment_extras_update = copy.deepcopy(mock_shipment_extras_create)
        mock_shipment_metadata_update = copy.deepcopy(mock_shipment_metadata_create)
        mock_shipment_image_update.shipment.data.releaseNotes.issues = Issues(
            fixed=[Issue(id="IMAGEBUG", source="issues.redhat.com")]
        )
        mock_shipment_extras_update.shipment.data.releaseNotes.issues = Issues(
            fixed=[Issue(id="EXTRASBUG", source="issues.redhat.com")]
        )
        self.assertEqual(updated_shipments_arg["image"], mock_shipment_image_update)
        self.assertEqual(updated_shipments_arg["extras"], mock_shipment_extras_update)
        self.assertEqual(updated_shipments_arg["metadata"], mock_shipment_metadata_update)
        self.assertEqual(mock_update_shipment_mr.call_args[0][1], "prod")

        self.assertEqual(
            pipeline.updated_assembly_group_config,
            Model(
                {
                    'shipment': {
                        'env': 'prod',
                        'advisories': [
                            {'kind': 'image', 'live_id': 'LIVE_ID_FROM_ERRATA'},
                            {'kind': 'extras', 'live_id': 'LIVE_ID_FROM_ERRATA'},
                            {'kind': 'metadata', 'live_id': 'LIVE_ID_FROM_ERRATA'},
                            {'kind': 'fbc'},
                        ],
                        'url': 'https://gitlab.example.com/mr/1',
                    },
                    'advisories': {'rpm': 123},
                }
            ),
        )
