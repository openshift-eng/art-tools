from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.assembly import AssemblyIssueCode, AssemblyTypes
from artcommonlib.model import Model
from doozerlib.assembly_inspector import AssemblyInspector
from doozerlib.build_info import BrewBuildRecordInspector, KonfluxBuildRecordInspector
from doozerlib.image import ImageMetadata
from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repo


class TestAssemblyInspector(IsolatedAsyncioTestCase):
    async def test_check_installed_packages_for_rpm_delivery(self):
        # Test a standard release with a package tagged into my-ship-ok-tag
        rt = MagicMock(
            mode="both",
            group_config=Model(
                {
                    "rpm_deliveries": [
                        {
                            "packages": ["kernel", "kernel-rt"],
                            "rhel_tag": "my-rhel-tag",
                            "integration_tag": "my-integration-tag",
                            "stop_ship_tag": "my-stop-ship-tag",
                            "target_tag": "my-target-tag",
                        },
                    ],
                }
            ),
        )
        brew_session = MagicMock()
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
        ]
        ai = AssemblyInspector(rt, brew_session)
        await ai.initialize()
        ai.assembly_type = AssemblyTypes.STANDARD
        rpm_packages = {
            "kernel": {"nvr": "kernel-1.2.3-1", "id": 1},
        }
        issues = ai._check_installed_packages_for_rpm_delivery("foo", "foo-1.2.3-1", rpm_packages)
        self.assertEqual(issues, [])

        # Test a stream "release" with a package not tagged into my-stop-ship-tag
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
            {"name": "my-stop-ship-tag"},
        ]
        issues = ai._check_installed_packages_for_rpm_delivery("foo", "foo-1.2.3-1", rpm_packages)
        self.assertEqual(len(issues), 1)

    def test_check_installed_rpms_in_image(self):
        rt = MagicMock(
            mode="both",
            group_config=Model(
                {
                    "rpm_deliveries": [
                        {
                            "packages": ["kernel", "kernel-rt"],
                            "rhel_tag": "my-rhel-tag",
                            "integration_tag": "my-integration-tag",
                            "stop_ship_tag": "my-stop-ship-tag",
                            "target_tag": "my-target-tag",
                        },
                    ],
                }
            ),
        )
        brew_session = MagicMock()
        brew_session.listTags.return_value = [
            {"name": "tag-a"},
            {"name": "my-integration-tag"},
        ]
        ai = AssemblyInspector(rt, brew_session)
        ai.assembly_type = AssemblyTypes.STANDARD
        build_inspector = MagicMock()
        build_inspector.get_all_installed_package_build_dicts.return_value = {
            "kernel": {"nvr": "kernel-1.2.3-1", "id": 1},
        }
        issues = ai.check_installed_rpms_in_image("foo", build_inspector, None)
        self.assertEqual(issues, [])

    def test_is_installed_rpm_in_tag(self):
        rt = MagicMock(mode="both", group_config=Model({}))
        brew_session = MagicMock()
        ai = AssemblyInspector(rt, brew_session)

        # None installed_rpm returns False
        self.assertFalse(ai._is_installed_rpm_in_tag(None, "my-candidate-tag"))

        # Installed RPM is tagged in the candidate tag
        brew_session.listTags.return_value = [
            {"name": "my-candidate-tag"},
            {"name": "other-tag"},
        ]
        installed_rpm = {"id": 100, "nvr": "cri-o-1.28.0-1.el9"}
        self.assertTrue(ai._is_installed_rpm_in_tag(installed_rpm, "my-candidate-tag"))

        # Installed RPM is NOT tagged in the candidate tag
        brew_session.listTags.return_value = [
            {"name": "rhel-9.2.0-z"},
            {"name": "RHSA-2026:13971-released"},
        ]
        self.assertFalse(ai._is_installed_rpm_in_tag(installed_rpm, "my-candidate-tag"))

    pass


class TestRepoAwareImageConsistency(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock(group_config=Model({"check_external_packages": []}), assembly_basis_event=123)
        self.runtime.repos = {}
        self.inspector = AssemblyInspector(self.runtime, MagicMock())
        self.installed = {"id": 1, "nvr": "kernel-1-1", "name": "kernel", "version": "1", "release": "1"}
        self.expected = {"id": 2, "nvr": "kernel-2-1", "name": "kernel", "version": "2", "release": "1"}
        self.inspector.get_external_rpm_build_dicts = MagicMock(return_value={"candidate": {"kernel": self.expected}})
        self.inspector.get_group_rpm_build_dicts = MagicMock(return_value={})
        self.meta = MagicMock(spec=ImageMetadata)
        self.meta.logger = MagicMock()
        self.meta.runtime = self.runtime
        self.meta.config = Model(
            {
                "enabled_repos": [],
                "check_external_packages": [{"tag": "candidate", "name": "kernel", "condition": "match"}],
            }
        )
        self.meta.distgit_key = "image"
        self.meta.get_enabled_repos.side_effect = lambda: ImageMetadata.get_enabled_repos(self.meta)
        self.meta.get_assembly_rpm_package_dependencies.return_value = ({}, {})
        self.meta.candidate_brew_tag.return_value = "candidate"
        self.build = MagicMock(spec=BrewBuildRecordInspector)
        self.build.get_image_meta.return_value = self.meta
        self.build.get_all_installed_package_build_dicts.return_value = {"kernel": self.installed}
        self.build.get_rhel_base_version.return_value = 9
        self.archive = MagicMock()
        self.archive.image_arch.return_value = "x86_64"
        self.archive.get_installed_rpm_dicts.return_value = [{"name": "kernel-core", "arch": "x86_64", "build_id": 1}]
        self.build.get_image_inspectors.return_value = [self.archive]
        self.repodatas = {}
        self.loader = self.enterContext(patch("doozerlib.repos.RepodataLoader.load", new_callable=AsyncMock))
        self.loader.side_effect = lambda name, url: self.repodatas[name]

    def add_repo(self, name, rpms=(), enabled=True, options=None):
        repo = Repo(
            name,
            {
                "conf": {
                    "baseurl": "https://example.invalid/repo/",
                    "enabled": enabled,
                    "extra_options": options or {},
                },
                "content_set": {"optional": True},
            },
            ["x86_64", "aarch64"],
        )
        self.runtime.repos[name] = repo
        self.meta.config.enabled_repos.append(name)
        for arch in repo.arches:
            self.repodatas[f"{name}-{arch}"] = Repodata(f"{name}-{arch}", list(rpms))
        return repo

    def rpm(self, name="kernel-core", source="kernel-2-1.src.rpm", arch="x86_64"):
        return Rpm(name, 0, "2", "checksum", 1, "package.rpm", source, "1", arch)

    async def issues(self):
        return await self.inspector.check_group_image_consistency(self.build, None)

    async def assert_conflict(self):
        issues = await self.issues()
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY])
        return issues

    async def test_no_enabled_repos(self):
        self.assertEqual(await self.issues(), [])
        self.loader.assert_not_awaited()

    async def test_relevant_expected_binary_in_any_enabled_repo(self):
        for repo_name in ("ose", "dedicated", "arbitrary-name"):
            with self.subTest(repo=repo_name):
                self.runtime.repos.clear()
                self.meta.config.enabled_repos = []
                self.add_repo(repo_name, [self.rpm()])
                await self.assert_conflict()
        self.inspector.brew_session.listTags.assert_not_called()

    async def test_rhel_only_and_dedicated_without_expected_kernel(self):
        self.add_repo("rhel", [self.rpm(source="kernel-1-1.src.rpm")])
        self.add_repo("dedicated", [self.rpm(name="ironic")])
        self.assertEqual(await self.issues(), [])

    async def test_unrelated_subpackage_or_wrong_source_or_arch_is_not_eligible(self):
        self.add_repo(
            "packages",
            [self.rpm(name="kernel-devel"), self.rpm(source="kernel-3-1.src.rpm"), self.rpm(arch="aarch64")],
        )
        self.assertEqual(await self.issues(), [])

    async def test_noarch_binary_is_eligible(self):
        self.add_repo("packages", [self.rpm(arch="noarch")])
        await self.assert_conflict()

    async def test_brew_archive_arches_are_not_deduplicated(self):
        self.add_repo("packages", [self.rpm(arch="aarch64")])
        other_archive = MagicMock()
        other_archive.image_arch.return_value = "aarch64"
        other_archive.get_installed_rpm_dicts.return_value = [{"name": "kernel-core", "arch": "aarch64", "build_id": 1}]
        self.build.get_image_inspectors.return_value.append(other_archive)
        await self.assert_conflict()

    async def test_global_and_image_enablement(self):
        self.add_repo("globally-disabled", [self.rpm()], enabled=False)
        self.add_repo("not-selected", [self.rpm()])
        self.meta.config.enabled_repos.remove("not-selected")
        self.add_repo("selected", [])
        self.assertEqual(await self.issues(), [])
        self.assertEqual([call.args[0] for call in self.loader.await_args_list], ["selected-x86_64"])

    async def test_dedicated_repo_include_and_exclude_filters(self):
        for options in ({"exclude": "kernel*"}, {"includepkgs": "ironic*"}, {"exclude": "kernel*", "includepkgs": "*"}):
            with self.subTest(options=options):
                self.add_repo("dedicated", [self.rpm()], options=options)
                self.assertEqual(await self.issues(), [])

    async def test_consistent_or_irrelevant_cases_do_not_fetch_metadata(self):
        self.add_repo("packages", [self.rpm()])
        self.build.get_all_installed_package_build_dicts.return_value = {"kernel": self.expected}
        self.assertEqual(await self.issues(), [])
        self.build.get_all_installed_package_build_dicts.return_value = {}
        self.assertEqual(await self.issues(), [])
        self.build.get_all_installed_package_build_dicts.return_value = {"kernel": self.installed}
        self.runtime.assembly_basis_event = None
        self.assertEqual(await self.issues(), [])
        self.runtime.assembly_basis_event = 123
        self.meta.config.check_external_packages = []
        self.assertEqual(await self.issues(), [])
        self.loader.assert_not_awaited()

    async def test_greater_equal_satisfied_does_not_fetch(self):
        self.add_repo("packages", [self.rpm()])
        self.meta.config.check_external_packages[0]["condition"] = "greater_equal"
        self.build.get_all_installed_package_build_dicts.return_value = {
            "kernel": {"nvr": "kernel-3-1", "name": "kernel", "version": "3", "release": "1"}
        }
        self.assertEqual(await self.issues(), [])
        self.loader.assert_not_awaited()
        self.build.get_all_installed_package_build_dicts.return_value = {"kernel": self.installed}
        await self.assert_conflict()

    async def test_unknown_inventory_retains_conflict_without_fetch(self):
        self.add_repo("packages", [])
        for rpms in (
            [],
            [{"name": "kernel-core", "arch": "x86_64"}],
            [{"name": "kernel-core", "arch": "src", "build_id": 1}],
        ):
            with self.subTest(rpms=rpms):
                self.archive.get_installed_rpm_dicts.return_value = rpms
                await self.assert_conflict()
        self.loader.assert_not_awaited()

    async def test_missing_source_linkage_retains_conflict(self):
        for source in (None, "", "malformed", "malformed.src.rpm", "path/kernel-2-1.src.rpm", "kernel-1:2-1.src.rpm"):
            with self.subTest(source=source):
                self.add_repo("packages", [self.rpm(source=source)])
                await self.assert_conflict()

    async def test_nosrc_source_build_is_eligible(self):
        self.add_repo("packages", [self.rpm(source="kernel-2-1.nosrc.rpm")])
        await self.assert_conflict()

    async def test_metadata_failure_propagates(self):
        self.add_repo("packages", [])
        self.loader.side_effect = RuntimeError("metadata unavailable")
        with self.assertRaisesRegex(RuntimeError, "metadata unavailable"):
            await self.issues()

    async def test_repo_metadata_cache_is_reused(self):
        self.add_repo("packages", [])
        self.assertEqual(await self.issues(), [])
        self.assertEqual(await self.issues(), [])
        self.loader.assert_awaited_once()

    async def test_explicit_pins_are_not_repo_gated_or_overwritten(self):
        for member in ({}, {"kernel": "kernel-3-1"}):
            with self.subTest(member=member):
                self.meta.get_assembly_rpm_package_dependencies.return_value = (member, {"kernel": "kernel-3-1"})
                issues = await self.assert_conflict()
                self.assertIn("kernel-3-1", str(issues[0]))
        self.loader.assert_not_awaited()

        self.build.get_all_installed_package_build_dicts.return_value = {}
        issues = await self.issues()
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.MISSING_INHERITED_DEPENDENCY])

    async def test_managed_rpm_mismatch_is_not_repo_gated(self):
        self.inspector.get_group_rpm_build_dicts.return_value = {
            "kernel": {"package_name": "kernel", "nvr": "kernel-3-1"}
        }
        issues = await self.issues()
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED])

    async def test_rhcos_tag_logic_and_mandatory_checks_are_unchanged(self):
        tag = "rhaos-4.18-rhel-9-candidate"
        self.runtime.group_config = Model(
            {
                "vars": {"MAJOR": 4, "MINOR": 18, "RHCOS_EL_MAJOR": 9},
                "check_external_packages": [{"tag": tag, "name": "kernel", "condition": "match"}],
            }
        )
        self.runtime.get_group_config.return_value = Model({"dependencies": []})
        self.runtime.assembly_type = AssemblyTypes.STANDARD
        self.inspector.assembly_rhcos_config = Model({})
        self.inspector.get_external_rpm_build_dicts.return_value = {tag: {"kernel": self.expected}}
        self.inspector._external_rpm_is_unavailable = AsyncMock(side_effect=AssertionError("image-only policy"))
        rhcos = MagicMock()
        rhcos.get_rhel_base_version.return_value = 9
        rhcos.get_package_build_objects.return_value = {"kernel": self.installed}
        self.inspector.brew_session.listTags.return_value = [{"name": tag}]
        issues = await self.inspector.check_rhcos_issues(rhcos)
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY])
        self.inspector.brew_session.listTags.return_value = []
        self.assertEqual(await self.inspector.check_rhcos_issues(rhcos), [])
        self.runtime.get_group_config.return_value = Model({"dependencies": [{"el9": "kernel-3-1"}]})
        issues = await self.inspector.check_rhcos_issues(rhcos)
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY])
        self.runtime.get_group_config.return_value = Model({"dependencies": []})
        self.inspector.get_group_rpm_build_dicts.return_value = {
            "kernel": {"package_name": "kernel", "nvr": "kernel-3-1"}
        }
        issues = await self.inspector.check_rhcos_issues(rhcos)
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED])
        self.inspector._external_rpm_is_unavailable.assert_not_awaited()
        self.loader.assert_not_awaited()

    async def test_konflux_uses_installed_binaries_not_all_source_outputs(self):
        self.add_repo("packages", [self.rpm(name="kernel-devel")])
        build = MagicMock(spec=KonfluxBuildRecordInspector)
        build.get_image_meta.return_value = self.meta
        build.get_all_installed_package_build_dicts.return_value = {"kernel": self.installed}
        build.get_rhel_base_version.return_value = 9
        record = build.get_build_obj.return_value
        record.arches = ["amd64", "arm64"]
        record.installed_rpms = ["kernel-core-1-1"]
        finder = MagicMock()
        finder.get_rpms_in_pkg_build.return_value = [
            {"name": name, "version": "1", "release": "1", "arch": "x86_64"} for name in ("kernel-core", "kernel-devel")
        ]
        self.assertEqual(await self.inspector.check_group_image_consistency(build, finder), [])
        self.runtime.repos["packages"]._repodatas.clear()
        self.repodatas["packages-aarch64"] = Repodata("packages-aarch64", [self.rpm(arch="aarch64")])
        issues = await self.inspector.check_group_image_consistency(build, finder)
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY])
        record.installed_rpms = []
        self.runtime.repos["packages"]._repodatas.clear()
        self.loader.reset_mock()
        issues = await self.inspector.check_group_image_consistency(build, finder)
        self.assertEqual([issue.code for issue in issues], [AssemblyIssueCode.CONFLICTING_INHERITED_DEPENDENCY])
        self.loader.assert_not_awaited()
        record.installed_rpms = ["malformed"]
        with self.assertRaises(ValueError):
            await self.inspector.check_group_image_consistency(build, finder)
