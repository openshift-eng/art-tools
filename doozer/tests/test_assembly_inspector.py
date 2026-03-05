from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from artcommonlib.assembly import AssemblyIssueCode, AssemblyTypes
from artcommonlib.model import Model
from doozerlib.assembly_inspector import AssemblyInspector


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

    def _make_image_consistency_runtime(self):
        """Helper to create a runtime mock for image consistency tests"""
        rt = MagicMock(
            mode="both",
            build_system="brew",
            group_config=Model({
                "rpm_deliveries": [],
                "public_upstreams": [],
                "check_external_packages": [],
                "dependencies": [],
            }),
        )
        rt.get_minor_version.return_value = "4.22"
        rt.assembly_basis_event = None

        image_meta = MagicMock()
        image_meta.distgit_key = "openshift-console"
        image_meta.get_component_name.return_value = "openshift-enterprise-console-container"
        image_meta.config = Model({
            "content": {"source": {"git": {"url": None, "branch": {"target": None}}}},
            "check_external_packages": [],
        })
        image_meta.get_assembly_rpm_package_dependencies.return_value = ({}, {})
        rt.image_map = {"openshift-console": image_meta}

        return rt, image_meta

    def test_check_group_image_version_mismatch(self):
        """Pinning a build from a different Y-stream should raise VERSION_MISMATCH"""
        rt, image_meta = self._make_image_consistency_runtime()

        ai = AssemblyInspector(rt, MagicMock())
        ai.assembly_type = AssemblyTypes.STANDARD

        build_inspector = MagicMock()
        build_inspector.get_version.return_value = "v4.12.0"
        build_inspector.get_nvr.return_value = "openshift-enterprise-console-container-v4.12.0-202301010000.p0.el8"
        build_inspector.get_all_installed_package_build_dicts.return_value = {}
        build_inspector.get_rhel_base_version.return_value = 8

        issues = ai.check_group_image_consistency("openshift-console", build_inspector, None)
        version_issues = [i for i in issues if i.code == AssemblyIssueCode.VERSION_MISMATCH]
        self.assertEqual(len(version_issues), 1)
        self.assertIn("4.12", version_issues[0].msg)
        self.assertIn("4.22", version_issues[0].msg)

    def test_check_group_image_version_match(self):
        """A build from the correct Y-stream should not raise VERSION_MISMATCH"""
        rt, image_meta = self._make_image_consistency_runtime()

        ai = AssemblyInspector(rt, MagicMock())
        ai.assembly_type = AssemblyTypes.STANDARD

        build_inspector = MagicMock()
        build_inspector.get_version.return_value = "v4.22.0"
        build_inspector.get_nvr.return_value = "openshift-enterprise-console-container-v4.22.0-202301010000.p0.el8"
        build_inspector.get_all_installed_package_build_dicts.return_value = {}
        build_inspector.get_rhel_base_version.return_value = 8

        issues = ai.check_group_image_consistency("openshift-console", build_inspector, None)
        version_issues = [i for i in issues if i.code == AssemblyIssueCode.VERSION_MISMATCH]
        self.assertEqual(len(version_issues), 0)

    def test_check_group_image_wrong_component(self):
        """Pinning a build from a different component should raise IMPERMISSIBLE"""
        rt, image_meta = self._make_image_consistency_runtime()

        ai = AssemblyInspector(rt, MagicMock())
        ai.assembly_type = AssemblyTypes.STANDARD

        build_inspector = MagicMock()
        build_inspector.get_version.return_value = "v4.22.0"
        build_inspector.get_nvr.return_value = "openshift-enterprise-pod-container-v4.22.0-202301010000.p0.el8"
        build_inspector.get_all_installed_package_build_dicts.return_value = {}
        build_inspector.get_rhel_base_version.return_value = 8

        issues = ai.check_group_image_consistency("openshift-console", build_inspector, None)
        component_issues = [i for i in issues if i.code == AssemblyIssueCode.IMPERMISSIBLE]
        self.assertEqual(len(component_issues), 1)
        self.assertIn("openshift-enterprise-pod-container", component_issues[0].msg)
        self.assertIn("openshift-enterprise-console-container", component_issues[0].msg)

    def test_check_group_image_correct_component(self):
        """A build from the correct component should not raise component issues"""
        rt, image_meta = self._make_image_consistency_runtime()

        ai = AssemblyInspector(rt, MagicMock())
        ai.assembly_type = AssemblyTypes.STANDARD

        build_inspector = MagicMock()
        build_inspector.get_version.return_value = "v4.22.0"
        build_inspector.get_nvr.return_value = "openshift-enterprise-console-container-v4.22.0-202301010000.p0.el8"
        build_inspector.get_all_installed_package_build_dicts.return_value = {}
        build_inspector.get_rhel_base_version.return_value = 8

        issues = ai.check_group_image_consistency("openshift-console", build_inspector, None)
        component_issues = [i for i in issues if i.code == AssemblyIssueCode.IMPERMISSIBLE]
        self.assertEqual(len(component_issues), 0)

    async def test_check_rhcos_version_mismatch_non_layered(self):
        """Non-layered RHCOS from a different Y-stream should raise VERSION_MISMATCH"""
        rt = MagicMock(
            mode="both",
            group_config=Model({
                "rpm_deliveries": [],
                "check_external_packages": [],
                "dependencies": [],
                "rhcos": {"payload_tags": [], "dependencies": []},
                "vars": {"MAJOR": "4", "MINOR": "22", "RHCOS_EL_MAJOR": "9"},
            }),
        )
        rt.get_minor_version.return_value = "4.22"
        rt.assembly_type = AssemblyTypes.STANDARD
        rt.assembly_basis_event = None
        rt.rpm_metas.return_value = []

        brew_session = MagicMock()
        ai = AssemblyInspector(rt, brew_session)
        ai.assembly_type = AssemblyTypes.STANDARD

        rhcos_build = MagicMock()
        rhcos_build.build_id = "412.92.20250101.0"
        rhcos_build.brew_arch = "x86_64"
        rhcos_build.stream_version = "4.12"
        rhcos_build.get_rhel_base_version.return_value = 9
        rhcos_build.get_package_build_objects.return_value = {}
        rhcos_build.get_rpm_nvrs.return_value = []

        issues = await ai.check_rhcos_issues(rhcos_build)
        version_issues = [i for i in issues if i.code == AssemblyIssueCode.VERSION_MISMATCH]
        self.assertEqual(len(version_issues), 1)
        self.assertIn("4.12", version_issues[0].msg)
        self.assertIn("4.22", version_issues[0].msg)

    async def test_check_rhcos_version_mismatch_layered(self):
        """Layered RHCOS from a different Y-stream should raise VERSION_MISMATCH"""
        rt = MagicMock(
            mode="both",
            group_config=Model({
                "rpm_deliveries": [],
                "check_external_packages": [],
                "dependencies": [],
                "rhcos": {"payload_tags": [], "dependencies": []},
                "vars": {"MAJOR": "4", "MINOR": "22", "RHCOS_EL_MAJOR": "9"},
            }),
        )
        rt.get_minor_version.return_value = "4.22"
        rt.assembly_type = AssemblyTypes.STANDARD
        rt.assembly_basis_event = None
        rt.rpm_metas.return_value = []

        brew_session = MagicMock()
        ai = AssemblyInspector(rt, brew_session)
        ai.assembly_type = AssemblyTypes.STANDARD

        rhcos_build = MagicMock()
        rhcos_build.build_id = "4.21.9.6.202602041851-0"
        rhcos_build.brew_arch = "x86_64"
        rhcos_build.stream_version = None  # layered RHCOS does not set stream_version
        rhcos_build.get_rhel_base_version.return_value = 9
        rhcos_build.get_package_build_objects.return_value = {}
        rhcos_build.get_rpm_nvrs.return_value = []

        issues = await ai.check_rhcos_issues(rhcos_build)
        version_issues = [i for i in issues if i.code == AssemblyIssueCode.VERSION_MISMATCH]
        self.assertEqual(len(version_issues), 1)
        self.assertIn("4.21", version_issues[0].msg)
        self.assertIn("4.22", version_issues[0].msg)

    async def test_check_rhcos_version_match(self):
        """RHCOS from the correct Y-stream should not raise VERSION_MISMATCH"""
        rt = MagicMock(
            mode="both",
            group_config=Model({
                "rpm_deliveries": [],
                "check_external_packages": [],
                "dependencies": [],
                "rhcos": {"payload_tags": [], "dependencies": []},
                "vars": {"MAJOR": "4", "MINOR": "22", "RHCOS_EL_MAJOR": "9"},
            }),
        )
        rt.get_minor_version.return_value = "4.22"
        rt.assembly_type = AssemblyTypes.STANDARD
        rt.assembly_basis_event = None
        rt.rpm_metas.return_value = []

        brew_session = MagicMock()
        ai = AssemblyInspector(rt, brew_session)
        ai.assembly_type = AssemblyTypes.STANDARD

        rhcos_build = MagicMock()
        rhcos_build.build_id = "422.92.20250101.0"
        rhcos_build.brew_arch = "x86_64"
        rhcos_build.stream_version = "4.22"
        rhcos_build.get_rhel_base_version.return_value = 9
        rhcos_build.get_package_build_objects.return_value = {}
        rhcos_build.get_rpm_nvrs.return_value = []

        issues = await ai.check_rhcos_issues(rhcos_build)
        version_issues = [i for i in issues if i.code == AssemblyIssueCode.VERSION_MISMATCH]
        self.assertEqual(len(version_issues), 0)

    pass
