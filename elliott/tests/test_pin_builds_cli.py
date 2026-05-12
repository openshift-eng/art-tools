from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome
from artcommonlib.model import ListModel, Model
from elliottlib.cli.pin_builds_cli import AssemblyPinBuildsCli


class TestAssemblyPinBuildsCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.assembly_config = Model(
            {
                "members": {
                    "images": [],
                    "rpms": [],
                },
                "group": {},
            }
        )

        self.runtime = MagicMock(
            group="openshift-4.18",
            assembly="4.18.2",
            build_system=Engine.BREW.value,
        )
        self.runtime.get_major_minor.return_value = (4, 18)
        self.runtime.get_releases_config.return_value = Model(
            {
                "releases": {
                    self.runtime.assembly: {
                        "assembly": self.assembly_config,
                    },
                },
            }
        )
        self.runtime.konflux_db = MagicMock()

        self.image_nvr = "image1-4.18.0-1"
        self.rpm_nvr = "rpm1-4.18.0-1.el8"
        self.rhcos_nvr = "rhcos-418.92.202309222337-0"
        self.non_art_rpm_nvr = "non-art-rpm-1.0-1.el8"

        image_mock = MagicMock()
        image_mock.get_component_name.return_value = "image1"
        image_mock.config.content.source.git.web = "https://github.com/org/repo"
        image_mock.distgit_key = "image1"

        rpm_mock = MagicMock()
        rpm_mock.get_component_name.return_value = "rpm1"
        rpm_mock.config.content.source.git.web = "https://github.com/org/repo"
        rpm_mock.distgit_key = "rpm1"
        rpm_mock.determine_rhel_targets.return_value = [8]

        self.runtime.image_map = {"image1": image_mock}
        self.runtime.rpm_map = {"rpm1": rpm_mock}

        self.pr = "https://github.com/org/repo/pull/123"
        self.why = "Testing"
        self.github_client = MagicMock()

        self.build_record_image = AsyncMock()
        self.build_record_image.name = "image1"
        self.build_record_image.nvr = self.image_nvr

        self.build_record_rpm = AsyncMock()
        self.build_record_rpm.name = "rpm1"
        self.build_record_rpm.nvr = self.rpm_nvr

    def setup_search_builds_mock(self):
        """Set up the search_builds_by_fields mock to return our build records"""

        async def mock_search_builds_by_fields(*args, **kwargs):
            yield self.build_record_image
            yield self.build_record_rpm

        search_builds_mock = MagicMock(side_effect=mock_search_builds_by_fields)
        self.runtime.konflux_db.search_builds_by_fields = search_builds_mock
        return search_builds_mock

    @patch("elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.validate_nvrs_in_brew")
    async def test_run_with_nvrs(self, mock_validate_nvrs_brew):
        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[self.image_nvr, self.rpm_nvr],
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        out, changed = await cli.run()

        mock_validate_nvrs_brew.assert_called_once_with([self.image_nvr, self.rpm_nvr])
        expected_assembly_config = {
            "members": {
                "images": [
                    {
                        "distgit_key": "image1",
                        "metadata": {
                            "is": {"nvr": self.image_nvr},
                        },
                        "why": self.why,
                    }
                ],
                "rpms": [
                    {
                        "distgit_key": "rpm1",
                        "metadata": {
                            "is": {"el8": self.rpm_nvr},
                        },
                        "why": self.why,
                    }
                ],
            },
            "group": {},
        }
        self.assertEqual(changed, True)
        self.assertEqual(out["releases"][self.runtime.assembly]["assembly"], expected_assembly_config)

    @patch("elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.validate_nvrs_in_brew")
    async def test_run_with_nvrs_no_change(self, mock_validate_nvrs_brew):
        assembly_config = {
            "members": {
                "images": [
                    {
                        "distgit_key": "image1",
                        "metadata": {
                            "is": {"nvr": self.image_nvr},
                        },
                        "why": self.why,
                    }
                ],
                "rpms": [
                    {
                        "distgit_key": "rpm1",
                        "metadata": {
                            "is": {"el8": self.rpm_nvr},
                        },
                        "why": self.why,
                    }
                ],
            },
            "group": {},
        }
        self.runtime.get_releases_config.return_value = Model(
            {
                "releases": {
                    self.runtime.assembly: {
                        "assembly": assembly_config,
                    },
                },
            }
        )
        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[self.image_nvr, self.rpm_nvr],
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        _, changed = await cli.run()
        mock_validate_nvrs_brew.assert_called_once_with([self.image_nvr, self.rpm_nvr])
        self.assertEqual(changed, False)

    @patch("elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.get_nvrs_for_pr")
    @patch("elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.validate_nvrs_in_brew")
    async def test_run_with_pr(self, mock_validate_nvrs_brew, mock_get_nvrs_for_pr):
        mock_get_nvrs_for_pr.return_value = [self.image_nvr, self.rpm_nvr]

        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[],
            pr=self.pr,
            why=self.why,
            github_client=self.github_client,
        )
        out, changed = await cli.run()

        mock_validate_nvrs_brew.assert_called_once_with([self.image_nvr, self.rpm_nvr])
        expected_assembly_config = {
            "members": {
                "images": [
                    {
                        "distgit_key": "image1",
                        "metadata": {
                            "is": {"nvr": self.image_nvr},
                        },
                        "why": self.why,
                    }
                ],
                "rpms": [
                    {
                        "distgit_key": "rpm1",
                        "metadata": {
                            "is": {"el8": self.rpm_nvr},
                        },
                        "why": self.why,
                    }
                ],
            },
            "group": {},
        }
        self.assertEqual(changed, True)
        self.assertEqual(out["releases"][self.runtime.assembly]["assembly"], expected_assembly_config)

    @patch("elliottlib.cli.pin_builds_cli.get_container_configs")
    @patch("elliottlib.cli.pin_builds_cli.get_container_pullspec")
    @patch("elliottlib.cli.pin_builds_cli.RHCOSBuildFinder")
    async def test_run_with_rhcos_nvr(
        self,
        mock_rhcos_build_finder,
        mock_get_container_pullspec,
        mock_get_container_configs,
    ):
        self.runtime.group_config = MagicMock()
        self.runtime.group_config.arches = ["x86_64", "aarch64"]
        self.runtime.group_config.rhcos.get.return_value = False

        # Setup container configs
        mock_container_conf = MagicMock()
        mock_container_conf.name = "machine-os-content"
        mock_get_container_configs.return_value = [mock_container_conf]

        # Mock RHCOSBuildFinder
        mock_finder_instance = MagicMock()
        mock_finder_instance.rhcos_build_meta.return_value = MagicMock()
        mock_finder_instance.get_primary_container_conf.return_value = mock_container_conf
        mock_rhcos_build_finder.return_value = mock_finder_instance

        mock_get_container_pullspec.return_value = "registry.example.com/rhel-coreos/machine-os-content@sha256:abc123"

        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[self.rhcos_nvr],
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        out, changed = await cli.run()

        mock_rhcos_build_finder.assert_any_call(
            self.runtime,
            "4.18",
            "x86_64",
            False,
        )
        mock_rhcos_build_finder.assert_any_call(
            self.runtime,
            "4.18",
            "aarch64",
            False,
        )
        parsed_build_id = self.rhcos_nvr.split('-', 1)[1]
        mock_finder_instance.rhcos_build_meta.assert_called_with(parsed_build_id)
        self.assertEqual(mock_get_container_pullspec.call_count, 2)
        expected_assembly_config = {
            "members": {
                "images": [],
                "rpms": [],
            },
            "group": {},
            "rhcos": {
                "machine-os-content": {
                    "images": {
                        "x86_64": "registry.example.com/rhel-coreos/machine-os-content@sha256:abc123",
                        "aarch64": "registry.example.com/rhel-coreos/machine-os-content@sha256:abc123",
                    },
                },
            },
        }
        self.assertEqual(changed, True)
        self.assertEqual(out["releases"][self.runtime.assembly]["assembly"], expected_assembly_config)

    @patch("elliottlib.cli.pin_builds_cli.get_container_configs")
    @patch("elliottlib.cli.pin_builds_cli.oc_image_info_for_arch")
    @patch("elliottlib.cli.pin_builds_cli.get_art_prod_image_repo_for_version")
    @patch("elliottlib.cli.pin_builds_cli.go_arch_for_brew_arch")
    async def test_run_with_layered_rhcos_nvr(
        self,
        mock_go_arch_for_brew_arch,
        mock_get_art_repo,
        mock_oc_image_info,
        mock_get_container_configs,
    ):
        self.runtime.get_major_minor.return_value = (4, 21)
        self.runtime.group_config = MagicMock()
        self.runtime.group_config.arches = ["x86_64", "aarch64"]
        self.runtime.group_config.rhcos.get.return_value = True
        self.runtime.group_config.vars.RHCOS_EL_MAJOR = 9
        self.runtime.group_config.vars.RHCOS_EL_MINOR = 6

        mock_go_arch_for_brew_arch.side_effect = lambda a: {"x86_64": "amd64", "aarch64": "arm64"}[a]
        art_repo = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
        mock_get_art_repo.return_value = art_repo

        # Layered container configs have rhcos_index_tag instead of build_metadata_key
        primary_conf = MagicMock()
        primary_conf.name = "rhel-coreos"
        primary_conf.rhcos_index_tag = f"{art_repo}:4.21-9.6-node-image"
        primary_conf.rhel_version = "9.6"

        ext_conf = MagicMock()
        ext_conf.name = "rhel-coreos-extensions"
        ext_conf.rhcos_index_tag = f"{art_repo}:4.21-9.6-node-image-extensions"
        ext_conf.rhel_version = "9.6"

        mock_get_container_configs.return_value = [primary_conf, ext_conf]

        # Return different digests per tag+arch to verify correct routing
        def fake_oc_image_info(pullspec, go_arch, registry_config=None):
            if "node-image-extensions" in pullspec:
                return {"digest": f"sha256:ext-{go_arch}"}
            return {"digest": f"sha256:primary-{go_arch}"}

        mock_oc_image_info.side_effect = fake_oc_image_info

        layered_rhcos_build = "rhcos-4.21-9.6-202605121024"
        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[layered_rhcos_build],
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        out, changed = await cli.run()

        # Verify build-specific tags were constructed correctly
        mock_oc_image_info.assert_any_call(
            f"{art_repo}:4.21-9.6-202605121024-node-image",
            "amd64",
            registry_config=self.runtime.registry_config,
        )
        mock_oc_image_info.assert_any_call(
            f"{art_repo}:4.21-9.6-202605121024-node-image-extensions",
            "arm64",
            registry_config=self.runtime.registry_config,
        )
        self.assertEqual(mock_oc_image_info.call_count, 4)  # 2 containers x 2 arches

        expected_assembly_config = {
            "members": {
                "images": [],
                "rpms": [],
            },
            "group": {},
            "rhcos": {
                "rhel-coreos": {
                    "images": {
                        "x86_64": f"{art_repo}@sha256:primary-amd64",
                        "aarch64": f"{art_repo}@sha256:primary-arm64",
                    },
                },
                "rhel-coreos-extensions": {
                    "images": {
                        "x86_64": f"{art_repo}@sha256:ext-amd64",
                        "aarch64": f"{art_repo}@sha256:ext-arm64",
                    },
                },
            },
        }
        self.assertEqual(changed, True)
        self.assertEqual(out["releases"][self.runtime.assembly]["assembly"], expected_assembly_config)

    @patch("elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.validate_nvrs_in_brew")
    async def test_run_with_non_art_rpm_nvr(self, mock_validate_nvrs_brew):
        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[self.non_art_rpm_nvr],
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        out, changed = await cli.run()

        mock_validate_nvrs_brew.assert_called_once_with([self.non_art_rpm_nvr])
        expected_assembly_config = {
            "members": {
                "images": [],
                "rpms": [],
            },
            "group": {
                "dependencies": {
                    "rpms": [
                        {
                            "el8": self.non_art_rpm_nvr,
                            "why": self.why,
                            "non_gc_tag": "insert tag here if needed",
                        }
                    ],
                },
            },
        }
        self.assertEqual(changed, True)
        self.assertEqual(out["releases"][self.runtime.assembly]["assembly"], expected_assembly_config)

    @patch(
        "elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.get_pr_merge_commit", return_value=("commit_hash", "main")
    )
    async def test_get_nvrs_for_pr(self, mock_get_pr_merge_commit):
        search_builds_mock = self.setup_search_builds_mock()

        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[],
            pr=self.pr,
            why=self.why,
            github_client=self.github_client,
        )
        nvrs = await cli.get_nvrs_for_pr()

        mock_get_pr_merge_commit.assert_called_once_with(self.pr, self.github_client)
        search_builds_mock.assert_called_once_with(
            where={
                "group": self.runtime.group,
                "commitish": "commit_hash",
                "outcome": KonfluxBuildOutcome.SUCCESS.value,
                "engine": self.runtime.build_system,
            }
        )
        self.assertEqual(nvrs, [self.image_nvr, self.rpm_nvr])

    def test_get_pr_merge_commit(self):
        mock_pr = MagicMock()
        mock_pr.merge_commit_sha = "abc123def456"
        mock_pr.base.ref = "release-4.18"
        mock_repo = MagicMock()
        mock_repo.get_pull.return_value = mock_pr
        mock_client = MagicMock()
        mock_client.get_repo.return_value = mock_repo

        sha, branch = AssemblyPinBuildsCli.get_pr_merge_commit(self.pr, mock_client)
        mock_client.get_repo.assert_called_once_with("org/repo")
        mock_repo.get_pull.assert_called_once_with(123)
        self.assertEqual(sha, "abc123def456")
        self.assertEqual(branch, "release-4.18")

    async def test_pin_rpms_missing_rhel_target(self):
        # Set up RPM mock with two RHEL targets: 8 and 9
        rpm_mock = MagicMock()
        rpm_mock.get_component_name.return_value = "rpm1"
        rpm_mock.config.content.source.git.web = "https://github.com/org/repo"
        rpm_mock.distgit_key = "rpm1"
        rpm_mock.determine_rhel_targets.return_value = [8, 9]  # RPM supports both el8 and el9
        self.runtime.rpm_map = {"rpm1": rpm_mock}

        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[self.rpm_nvr],  # Only passing the el8 NVR
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        cli.assembly_config = self.assembly_config

        with self.assertRaises(ValueError) as context:
            cli.pin_rpms({"rpm1": rpm_mock}, [self.rpm_nvr])
        self.assertIn("RPM rpm1 is missing a pin for rhel9", str(context.exception))

    @patch("elliottlib.cli.pin_builds_cli.AssemblyPinBuildsCli.validate_nvrs_in_konflux_db")
    async def test_run_with_nvrs_konflux_engine(self, mock_validate_nvrs_konflux):
        self.runtime.build_system = Engine.KONFLUX.value

        cli = AssemblyPinBuildsCli(
            runtime=self.runtime,
            nvrs=[self.image_nvr, self.rpm_nvr],
            pr=None,
            why=self.why,
            github_client=self.github_client,
        )
        await cli.run()

        mock_validate_nvrs_konflux.assert_called_once_with([self.image_nvr, self.rpm_nvr])
