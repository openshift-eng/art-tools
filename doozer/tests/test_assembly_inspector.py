
from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.model import Model
from doozerlib.assembly_inspector import AssemblyInspector


class TestAssemblyInspector(IsolatedAsyncioTestCase):
    async def test_check_installed_packages_for_rpm_delivery(self):
        # Test a standard release with a package tagged into my-ship-ok-tag
        rt = MagicMock(mode="both", group_config=Model({
            "rpm_deliveries": [
                {
                    "packages": ["kernel", "kernel-rt"],
                    "rhel_tag": "my-rhel-tag",
                    "integration_tag": "my-integration-tag",
                    "stop_ship_tag": "my-stop-ship-tag",
                    "target_tag": "my-target-tag",
                },
            ],
        }))
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
        rt = MagicMock(mode="both", group_config=Model({
            "rpm_deliveries": [
                {
                    "packages": ["kernel", "kernel-rt"],
                    "rhel_tag": "my-rhel-tag",
                    "integration_tag": "my-integration-tag",
                    "stop_ship_tag": "my-stop-ship-tag",
                    "target_tag": "my-target-tag",
                },
            ],
        }))
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
    pass
