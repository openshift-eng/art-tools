from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock, patch

import koji
from artcommonlib.model import Model
from doozerlib.targeted_rpms import _source_builds, find_rhcos_rpm_pins


def _rpm(name: str, version: str, release: str, arch: str, stream: str = "rhel-coreos") -> list[str]:
    """
    Create an installed RPM entry from RHCOS metadata.
    """
    return [name, "0", version, release, arch, stream]


class TestSourceBuildResolution(TestCase):
    def test_subpackages_resolve_to_one_cached_source_build(self):
        """
        Binary subpackages must pin the source build, with cached results across arches.
        """
        session = MagicMock()
        calls = session.multicall.return_value.__enter__.return_value
        calls.getRPM.return_value = SimpleNamespace(result={"build_id": 42})
        source = {"name": "cri-o", "nvr": "cri-o-1.0-2.el9"}
        calls.getBuild.return_value = SimpleNamespace(result=source)
        cache = {}
        entries = [_rpm("cri-o", "1.0", "2.el9", "x86_64"), _rpm("cri-o-extra", "1.0", "2.el9", "x86_64")]
        result = _source_builds(entries, session, cache, {"cri-o"}, MagicMock())
        self.assertEqual(result, [source])
        _source_builds(entries, session, cache, {"cri-o"}, MagicMock())
        calls.getBuild.assert_called_once_with(42)
        calls.getRPM.assert_any_call("cri-o-extra-1.0-2.el9.x86_64")

    def test_missing_art_rpm_resolution_fails(self):
        """
        An unresolved installed ART RPM must not silently lose its member override.
        """
        session = MagicMock()
        session.multicall.return_value.__enter__.return_value.getRPM.return_value = SimpleNamespace(result=None)
        with self.assertRaisesRegex(ValueError, "Cannot resolve installed ART RPM cri-o"):
            _source_builds([_rpm("cri-o", "1.0", "2.el9", "x86_64")], session, {}, {"cri-o"}, MagicMock())

    def test_brew_service_errors_are_not_treated_as_missing_packages(self):
        """
        Authentication and service errors must stop comparison rather than skip evidence.
        """
        session = MagicMock()
        task = MagicMock()
        type(task).result = PropertyMock(side_effect=koji.GenericError("Permission denied"))
        session.multicall.return_value.__enter__.return_value.getRPM.return_value = task
        with self.assertRaisesRegex(koji.GenericError, "Permission denied"):
            _source_builds([_rpm("glibc", "1.0", "2.el9", "x86_64")], session, {}, {"cri-o"}, MagicMock())

    def test_unresolved_external_rpms_are_reported(self):
        """
        Missing external RPM records produce a warning without creating member pins.
        """
        session = MagicMock()
        session.multicall.return_value.__enter__.return_value.getRPM.return_value = SimpleNamespace(result=None)
        logger = MagicMock()
        result = _source_builds([_rpm("kernel-64k-core", "1.0", "2.el9", "aarch64")], session, {}, {"cri-o"}, logger)
        self.assertEqual(result, [])
        logger.warning.assert_called_once()


class TestRhcosRpmPins(TestCase):
    def setUp(self):
        """
        Model two RHCOS architectures with ART CRI-O and an external RHEL package.
        """
        self.runtime = MagicMock()
        self.runtime.group_config.arches = ["x86_64", "aarch64"]
        self.runtime.registry_config = "/tmp/auth.json"
        self.meta = MagicMock(rpm_name="cri-o", distgit_key="cri-o-distgit")
        self.meta.determine_rhel_targets.return_value = [9, 10]
        self.runtime.rpm_metas.return_value = [self.meta]
        self.before = {
            "rhel-coreos": {"images": {arch: f"repo/before@sha256:{arch}" for arch in self.runtime.group_config.arches}}
        }
        self.after = {
            "rhel-coreos": {"images": {arch: f"repo/after@sha256:{arch}" for arch in self.runtime.group_config.arches}}
        }
        self.runtime.get_releases_config.return_value = Model(
            {"releases": {"5.0.0": {"assembly": {"rhcos": self.before}}}}
        )
        self.session = self.runtime.pooled_koji_client_session.return_value.__enter__.return_value
        calls = self.session.multicall.return_value.__enter__.return_value
        self.rpms = {}
        self.builds = {}
        calls.getRPM.side_effect = lambda nvra: SimpleNamespace(result=self.rpms.get(nvra))
        calls.getBuild.side_effect = lambda build_id: SimpleNamespace(result=self.builds.get(build_id))
        self.inventories = {}
        for arch in self.runtime.group_config.arches:
            self.inventories[("before", arch)] = self._entries(
                "cri-o", "202610221550.p2.g4f5848d.assembly.stream.el9", 1, arch
            )
            self.inventories[("after", arch)] = self._entries(
                "cri-o", "202610292257.p2.g9186725.assembly.stream.el9", 2, arch
            )
            self.inventories[("before", arch)] += self._entries("glibc", "1.el9", 3, arch)
            self.inventories[("after", arch)] += self._entries("glibc", "2.el9", 4, arch)

    def _entries(
        self, name: str, release: str, build_id: int, arch: str, binary_name: str | None = None
    ) -> list[list[str]]:
        """
        Add installed binary RPM and source build identities to the mocked Brew service.
        """
        binary = binary_name or name
        nvr = f"{name}-5.0.0-{release}"
        self.rpms[f"{binary}-5.0.0-{release}.{arch}"] = {"build_id": build_id}
        self.builds[build_id] = {"name": name, "nvr": nvr, "state": 1}
        return [_rpm(binary, "5.0.0", release, arch)]

    def _inspect(self, runtime, images: dict, arch: str, registry_config: str) -> MagicMock:
        """
        Serve different inventories for the actual selected and basis image references.
        """
        self.assertEqual(registry_config, "/tmp/auth.json")
        side = "before" if "/before@" in images["rhel-coreos"] else "after"
        return MagicMock(get_os_metadata_rpm_list=MagicMock(return_value=self.inventories[(side, arch)]))

    def _pins(self) -> list[dict]:
        """
        Execute inventory comparison using the mocked image metadata and Brew service.
        """
        with patch("doozerlib.targeted_rpms.RHCOSBuildInspector", side_effect=self._inspect):
            return find_rhcos_rpm_pins(self.runtime, self.after, "5.0.0")

    def test_crio_change_produces_source_build_member_pin(self):
        """
        Fix the reported CRI-O mismatch while excluding unrelated RHEL RPM upgrades.
        """
        pins = self._pins()
        self.assertEqual(len(pins), 1)
        self.assertEqual(pins[0]["distgit_key"], "cri-o-distgit")
        self.assertEqual(pins[0]["metadata"]["is"], {"el9": "cri-o-5.0.0-202610292257.p2.g9186725.assembly.stream.el9"})

    def test_review_receives_both_kernel_changes_and_art_pin_changes(self):
        """
        Inspect external kernel source builds for claims as well as ART RPM changes.
        """
        for arch in self.runtime.group_config.arches:
            self.inventories[("before", arch)] += self._entries("kernel", "1.el9_8", 100, arch)
            self.inventories[("after", arch)] += self._entries("kernel", "2.el9_8", 200, arch)
        review = MagicMock()
        review.record.return_value = "Triggered by OCPBUGS-135021; new references: CVE-2026-2000"
        with patch("doozerlib.targeted_rpms.RHCOSBuildInspector", side_effect=self._inspect):
            pins = find_rhcos_rpm_pins(self.runtime, self.after, "5.0.0", review=review)
        review.record.assert_any_call("kernel", 9, "kernel-5.0.0-1.el9_8", "kernel-5.0.0-2.el9_8")
        review.record.assert_any_call(
            "cri-o",
            9,
            "cri-o-5.0.0-202610221550.p2.g4f5848d.assembly.stream.el9",
            "cri-o-5.0.0-202610292257.p2.g9186725.assembly.stream.el9",
        )
        self.assertIn("OCPBUGS-135021", pins[0]["why"])
        self.assertIn("CVE-2026-2000", pins[0]["why"])
        self.assertEqual(len(pins), 1)

    def test_unchanged_art_build_needs_no_override(self):
        """
        Preserve inherited pins when the installed source build did not change.
        """
        for arch in self.runtime.group_config.arches:
            self.inventories[("after", arch)] = self.inventories[("before", arch)]
        self.assertEqual(self._pins(), [])

    def test_rhel9_and_rhel10_builds_share_one_component_pin(self):
        """
        Both RHEL targets get their own source NVR under the same ART member.
        """
        for arch in self.runtime.group_config.arches:
            self.inventories[("before", arch)] += self._entries("cri-o", "1.el10", 10, arch)
            self.inventories[("after", arch)] += self._entries("cri-o", "2.el10", 20, arch)
        pins = self._pins()
        self.assertEqual(pins[0]["metadata"]["is"]["el10"], "cri-o-5.0.0-2.el10")
        self.assertIn("el9", pins[0]["metadata"]["is"])

    def test_source_package_is_detected_through_binary_subpackage(self):
        """
        Resolve the source identity rather than mistaking a binary RPM name for the member.
        """
        for arch in self.runtime.group_config.arches:
            self.inventories[("after", arch)] = self._entries("cri-o", "2.el9", 20, arch, binary_name="cri-o-extra")
        self.assertEqual(self._pins()[0]["metadata"]["is"]["el9"], "cri-o-5.0.0-2.el9")

    def test_divergent_architectures_fail_before_emitting_pins(self):
        """
        A single assembly pin cannot represent two builds for the same RHEL target.
        """
        self.inventories[("after", "aarch64")] = self._entries("cri-o", "3.el9", 30, "aarch64")
        with self.assertRaisesRegex(ValueError, "Conflicting ART RPM builds.*cri-o-distgit/el9"):
            self._pins()

    def test_incomplete_art_source_build_is_not_pinned(self):
        """
        Only complete source builds can be emitted as assembly member overrides.
        """
        self.builds[2]["state"] = koji.BUILD_STATES["BUILDING"]
        with self.assertRaisesRegex(ValueError, "not complete in Brew"):
            self._pins()

    def test_missing_basis_images_are_not_treated_as_empty_inventory(self):
        """
        Missing baseline evidence must not silently mark all packages as newly installed.
        """
        self.runtime.get_releases_config.return_value = Model({"releases": {"5.0.0": {"assembly": {}}}})
        with self.assertRaisesRegex(ValueError, "No explicit RHCOS image pins"):
            self._pins()
