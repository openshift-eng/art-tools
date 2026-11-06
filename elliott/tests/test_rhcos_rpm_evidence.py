"""Unit tests for RHCOS RPM evidence parsing, analysis, and adapters."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from elliottlib.rhcos_rpm_evidence import build_evidence, inventory, report, repository
from elliottlib.rhcos_rpm_evidence.discovery import compare_rpm_inventories, match_changed_rpm_cves, required_build_nvrs
from elliottlib.rhcos_rpm_evidence.models import (
    AnalysisReport,
    InstalledRpm,
    RHCOSImageInventory,
    RHCOSRpm,
    RpmAdvisoryEvidence,
    RpmBuildEvidence,
    RpmChange,
    RpmNevra,
    TrackerDisposition,
    TrackerFinding,
    VerificationComment,
    VerificationResult,
)
from elliottlib.rhcos_rpm_evidence.ports import RHCOSTracker
from elliottlib.rhcos_rpm_evidence.requirements import (
    check_required_rpms,
    comment_text,
    parse_latest_verification,
    parse_required_rpm,
)
from elliottlib.rhcos_rpm_evidence.service import RHCOSRpmEvidenceService


def _rpm(
    name: str,
    version: str = "5.14.0",
    release: str = "100.el9_8",
    arch: str = "x86_64",
    epoch: str = "0",
    stream: str = "rhel-coreos",
) -> InstalledRpm:
    """Create an installed RPM fixture."""
    return InstalledRpm(name, epoch, version, release, arch, stream)


def _image(arch: str, rpms: tuple[InstalledRpm, ...], build_id: str = "4.22.9.8") -> RHCOSImageInventory:
    """Create an image inventory fixture."""
    return RHCOSImageInventory(arch, build_id, (("rhel-coreos", f"image@{build_id}"),), rpms)


def _required(value: str) -> RpmNevra:
    """Parse an RPM requirement fixture and fail early if it is invalid."""
    parsed = parse_required_rpm(value)
    if parsed is None:
        raise AssertionError(f"Invalid test RPM requirement: {value}")
    return parsed


def _tracker(
    issue_key: str = "OCPBUGS-1",
    cve_id: str | None = "CVE-2026-12345",
    status: str = "NEW",
    component: str | None = "openshift/ose-rhel-coreos-9",
) -> RHCOSTracker:
    """Create a tracker fixture implementing the RHCOS tracker port."""
    return SimpleNamespace(id=issue_key, cve_id=cve_id, status=status, whiteboard_component=component)


def _verification_comment(
    result: str = "PASS",
    requirement: str = "kernel-core-0:5.14.0-100.el9_8.x86_64",
    cve_id: str = "CVE-2026-12345",
    created: str = "2026-11-01T12:00:00+0000",
) -> VerificationComment:
    """Create a verifier comment with a required RPM record."""
    body = (
        f"{cve_id} verified in RHCOS build nightly. Required RPMs: {requirement}. "
        f"Result: {result}. Automated verification by the RHCOS CVE verifier."
    )
    return VerificationComment(created, body)


class TestRpmModels(unittest.TestCase):
    """Check value model behavior and serialization."""

    def test_rhel_branch_and_nevra_formatting(self):
        rpm = RpmNevra("kernel-core", "0", "5.14.0", "100.el9_8", "x86_64")
        self.assertEqual(rpm.rhel_branch, ".el9_8")
        self.assertEqual(str(rpm), "kernel-core-0:5.14.0-100.el9_8.x86_64")
        self.assertEqual(rpm.to_dict()["arch"], "x86_64")
        self.assertIsNone(RpmNevra("pkg", "0", "1", "2", None).rhel_branch)

    def test_kernel_selector_excludes_extension_streams(self):
        base_kernel = _rpm("kernel-core", stream="rhel-coreos")
        extension_kernel = _rpm("kernel-core", stream="rhcos-coreos-extensions")
        unrelated = _rpm("kernel-modules", stream="rhel-coreos")

        self.assertTrue(RHCOSRpm.KERNEL.matches_inventory_rpm(base_kernel))
        self.assertFalse(RHCOSRpm.KERNEL.matches_inventory_rpm(extension_kernel))
        self.assertFalse(RHCOSRpm.KERNEL.matches_inventory_rpm(unrelated))
        self.assertEqual(RHCOSRpm.KERNEL.build_nvr(base_kernel), "kernel-5.14.0-100.el9_8")

    def test_report_models_serialize_nested_evidence(self):
        advisory = RpmAdvisoryEvidence("123", "https://errata/123", "QE", ("CVE-2026-12345",))
        build = RpmBuildEvidence("kernel-5.14.0-100.el9_8", ("CVE-2026-12345",), True, (advisory,), True)
        finding = TrackerFinding(
            "OCPBUGS-1",
            "CVE-2026-12345",
            "openshift/ose-rhel-coreos-9",
            "NEW",
            TrackerDisposition.NO_EVIDENCE,
            "No match",
        )
        image = _image("x86_64", (_rpm("kernel-core"),))
        value = AnalysisReport("4.22.1", ("4.22.z",), True, (image,), (finding,), True, rpm_build_evidence=(build,))

        serialized = value.to_dict()

        self.assertEqual(serialized["selected_images"][0]["rpm_count"], 1)
        self.assertEqual(serialized["rpm_build_evidence"][0]["advisories"][0]["id"], "123")
        self.assertEqual(serialized["findings"][0]["disposition"], "no_evidence")


class TestVerificationRequirements(unittest.TestCase):
    """Exercise verifier parsing and installed RPM checks."""

    def test_parse_required_rpm_preserves_epoch_arch_and_branch_variant(self):
        rpm = parse_required_rpm("libreswan-0:4.15-11.el9fdp.x86_64.")

        self.assertIsNotNone(rpm)
        self.assertEqual(rpm, RpmNevra("libreswan", "0", "4.15", "11.el9fdp", "x86_64"))
        self.assertIsNone(parse_required_rpm("kernel-core-5.14.0-100.x86_64"))
        self.assertIsNone(parse_required_rpm("not-a-nevra"))

    def test_parse_actual_libreswan_comment_and_deduplicate_equivalent_records(self):
        body = (
            "CVE-2026-94453 verified in RHCOS build 4.22.0-0.nightly. "
            "Package: libreswan. Installed: libreswan-5.3.2-2.el9fdp.x86_64. "
            "Required: libreswan-0:4.15-11.el9_8. Source: ext:unknown. Result: PASS "
            "Package: libreswan. Installed: libreswan-5.3.2-2.el9fdp.x86_64. "
            "Required: libreswan-0:4.15-11.el9_8.x86_64. Source: ext:unknown. Result: PASS "
            "OCP: 4.22. Result: PASS. Automated verification by the RHCOS CVE verifier."
        )
        parsed = parse_latest_verification([VerificationComment("2026-11-01", body)], "CVE-2026-94453")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.result, VerificationResult.PASS)
        self.assertEqual(parsed.requirements, (_required("libreswan-0:4.15-11.el9_8.x86_64"),))

    def test_latest_matching_verification_wins_without_falling_back(self):
        comments = [
            _verification_comment(result="PASS", created="2026-10-01"),
            _verification_comment(result="FAIL", created="2026-11-01"),
            _verification_comment(result="PASS", cve_id="CVE-2026-99999", created="2026-12-01"),
        ]

        parsed = parse_latest_verification(comments, "CVE-2026-12345")

        self.assertEqual(parsed.result, VerificationResult.FAIL)
        self.assertEqual(parsed.comment_created, "2026-11-01")

    def test_atlassian_document_text_is_recursively_extracted(self):
        value = {"text": "first", "content": [{"text": "second"}, {"content": [{"text": "third"}]}]}
        self.assertEqual(comment_text(value), "first second third")

    def test_unsupported_records_and_pass_without_requirements_are_reviewable(self):
        unsupported = VerificationComment(
            "2026-11-01",
            "CVE-2026-12345 verified in RHCOS build. Package: libre. Installed: invalid. Required: invalid. Result: PASS",
        )
        no_requirements = VerificationComment("2026-11-02", "CVE-2026-12345 verified in RHCOS build. Result: PASS")

        self.assertEqual(
            parse_latest_verification([unsupported], "CVE-2026-12345").result,
            VerificationResult.UNSUPPORTED,
        )
        self.assertEqual(
            parse_latest_verification([no_requirements], "CVE-2026-12345").result,
            VerificationResult.UNSUPPORTED,
        )

    def test_check_required_rpms_accepts_variant_branch_and_checks_evr(self):
        required = _required("libreswan-0:4.15-11.el9_8.x86_64")
        selected = _image("x86_64", (_rpm("libreswan", "5.3.2", "2.el9fdp"),))

        checks = check_required_rpms((required,), (selected,), "openshift/ose-rhel-coreos-9")

        self.assertEqual(len(checks), 1)
        self.assertTrue(checks[0].passed)
        self.assertEqual(checks[0].installed[0].release, "2.el9fdp")

    def test_check_required_rpms_rejects_other_minor_or_rhel_major_and_old_evr(self):
        required = _required("libreswan-0:4.15-11.el9_8.x86_64")
        wrong_minor = _image("x86_64", (_rpm("libreswan", "5.3.2", "2.el9_9"),))
        wrong_major = _image("x86_64", (_rpm("libreswan", "5.3.2", "2.el10_2"),))
        old_evr = _image("x86_64", (_rpm("libreswan", "4.14", "1.el9_8"),))

        checks = check_required_rpms((required,), (wrong_minor,), "openshift/ose-rhel-coreos-9")
        self.assertIn("absent", checks[0].reason)
        checks = check_required_rpms((required,), (wrong_major,), "openshift/ose-rhel-coreos-9")
        self.assertIn("absent", checks[0].reason)
        checks = check_required_rpms((required,), (old_evr,), "openshift/ose-rhel-coreos-9")
        self.assertFalse(checks[0].passed)
        self.assertIn("below required EVR", checks[0].reason)

    def test_check_required_rpms_supports_kernel_source_package_alias(self):
        required = _required("kernel-0:5.14.0-100.el9_8.x86_64")
        selected = _image("x86_64", (_rpm("kernel-core", "5.14.0", "200.el9_8"),))

        checks = check_required_rpms((required,), (selected,), "openshift/ose-rhel-coreos-9")

        self.assertTrue(checks[0].passed)


class TestInventoryDiscovery(unittest.TestCase):
    """Check image inventory comparisons and CVE evidence joins."""

    def test_compare_inventory_reports_kernel_upgrade_and_ignores_extension_stream(self):
        basis = _image(
            "x86_64",
            (
                _rpm("kernel-core", "5.14.0", "100.el9_8"),
                _rpm("kernel-core", "5.14.0", "10.el9_8", stream="rhcos-coreos-extensions"),
            ),
        )
        selected = _image(
            "x86_64",
            (
                _rpm("kernel-core", "5.14.0", "200.el9_8"),
                _rpm("kernel-core", "5.14.0", "20.el9_8", stream="rhcos-coreos-extensions"),
            ),
        )

        changes = compare_rpm_inventories((basis,), (selected,), (RHCOSRpm.KERNEL,))

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0].change, "upgraded")
        self.assertEqual(changes[0].before.release, "100.el9_8")
        self.assertEqual(changes[0].after.release, "200.el9_8")

    def test_compare_inventory_rejects_arch_mismatch_and_duplicate_versions(self):
        with self.assertRaisesRegex(ValueError, "architectures differ"):
            compare_rpm_inventories(
                (_image("x86_64", (_rpm("kernel-core"),)),),
                (_image("aarch64", (_rpm("kernel-core", arch="aarch64"),)),),
                (RHCOSRpm.KERNEL,),
            )

        duplicate_basis = _image(
            "x86_64",
            (_rpm("kernel-core", release="100.el9_8"), _rpm("kernel-core", release="101.el9_8")),
        )
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            compare_rpm_inventories((duplicate_basis,), (duplicate_basis,), (RHCOSRpm.KERNEL,))

    def test_new_changelog_cve_is_matched_and_basis_nvr_is_requested(self):
        before = _rpm("kernel-core", release="100.el9_8")
        after = _rpm("kernel-core", release="200.el9_8")
        change = RpmChange("x86_64", RHCOSRpm.KERNEL, "rhel-coreos", "upgraded", before, after)
        selected_nvr = RHCOSRpm.KERNEL.build_nvr(after)
        basis_nvr = RHCOSRpm.KERNEL.build_nvr(before)
        evidence = (
            RpmBuildEvidence(selected_nvr, ("CVE-2026-12345",), True, (), True),
            RpmBuildEvidence(basis_nvr, (), True, (), True),
        )

        matched = match_changed_rpm_cves((change,), evidence, baseline_inventory_complete=True)

        self.assertEqual(matched[0].cve_id, "CVE-2026-12345")
        self.assertTrue(matched[0].new_changelog_match)
        self.assertFalse(matched[0].observed_in_basis)
        self.assertEqual(required_build_nvrs((change,)), {selected_nvr, basis_nvr})

    def test_cve_already_in_basis_is_not_a_new_fix_and_removed_builds_are_not_requested(self):
        before = _rpm("kernel-core", release="100.el9_8")
        after = _rpm("kernel-core", release="200.el9_8")
        change = RpmChange("x86_64", RHCOSRpm.KERNEL, "rhel-coreos", "upgraded", before, after)
        selected_nvr = RHCOSRpm.KERNEL.build_nvr(after)
        basis_nvr = RHCOSRpm.KERNEL.build_nvr(before)
        evidence = (
            RpmBuildEvidence(selected_nvr, ("CVE-2026-12345",), True, (), True),
            RpmBuildEvidence(basis_nvr, (), True, (RpmAdvisoryEvidence("1", "u", "QE", ("CVE-2026-12345",)),), True),
        )

        matched = match_changed_rpm_cves((change,), evidence, baseline_inventory_complete=True)
        removed = RpmChange("x86_64", RHCOSRpm.KERNEL, "rhel-coreos", "removed", before, None)

        self.assertTrue(matched[0].observed_in_basis)
        self.assertFalse(matched[0].new_changelog_match)
        self.assertEqual(required_build_nvrs((removed,)), set())


class TestRHCOSService(unittest.TestCase):
    """Exercise the use case with in-memory ports instead of Jira or Brew."""

    class Inventory:
        def __init__(self, selected, basis, selected_error=None, basis_error=None):
            self.selected = selected
            self.basis = basis
            self.selected_error = selected_error
            self.basis_error = basis_error

        def collect(self):
            if self.selected_error:
                raise self.selected_error
            return self.selected

        def collect_basis(self):
            if self.basis_error:
                raise self.basis_error
            return self.basis

    class BuildEvidence:
        def __init__(self, evidence=()):
            self.evidence = evidence
            self.requested = None

        def collect(self, nvrs):
            self.requested = set(nvrs)
            return tuple(self.evidence)

    class Repository:
        def __init__(self, trackers, comments=None, target_error=None, comments_error=None, updated=True):
            self.trackers = trackers
            self.comment_map = comments or {}
            self.target_error = target_error
            self.comments_error = comments_error
            self.updated = updated
            self.marked = []
            self.comment_calls = []

        def target_releases(self):
            if self.target_error:
                raise self.target_error
            return ("4.22.z",)

        def find_trackers(self):
            return self.trackers

        def comments(self, issue_key):
            self.comment_calls.append(issue_key)
            if self.comments_error:
                raise self.comments_error
            return self.comment_map.get(issue_key, [])

        def mark_verified(self, bug, comment, dry_run):
            self.marked.append((bug.id, comment, dry_run))
            return self.updated

    def setUp(self):
        self.selected = _image("x86_64", (_rpm("kernel-core", release="200.el9_8"),))
        self.basis = _image("x86_64", (_rpm("kernel-core", release="200.el9_8"),))
        self.bug = _tracker()
        self.comment = _verification_comment()

    def _service(self, repository=None, inventory_source=None, build_source=None, excluded=()):
        return RHCOSRpmEvidenceService(
            "4.22.22",
            inventory_source or self.Inventory((self.selected,), (self.basis,)),
            build_source or self.BuildEvidence(),
            repository or self.Repository([self.bug], {self.bug.id: [self.comment]}),
            excluded_issue_keys=excluded,
        )

    def test_passing_verification_yields_ready_to_verify_without_jira_mutation(self):
        repository = self.Repository([self.bug], {self.bug.id: [self.comment]})

        analysis = self._service(repository=repository).run()

        finding = analysis.findings[0]
        self.assertEqual(finding.disposition, TrackerDisposition.READY_TO_VERIFY)
        self.assertIn("every required RPM", finding.reason)
        self.assertTrue(finding.checks[0].passed)
        self.assertTrue(analysis.complete)
        self.assertEqual(repository.marked, [])

    def test_dry_run_and_real_transition_are_reported_separately(self):
        dry_repo = self.Repository([self.bug], {self.bug.id: [self.comment]})
        dry_result = self._service(repository=dry_repo).run(update_trackers=True, dry_run=True)
        self.assertEqual(dry_result.findings[0].disposition, TrackerDisposition.WOULD_VERIFY)
        self.assertTrue(dry_repo.marked[0][2])
        self.assertIn("RHCOS RPM evidence", dry_repo.marked[0][1])

        real_repo = self.Repository([self.bug], {self.bug.id: [self.comment]})
        real_result = self._service(repository=real_repo).run(update_trackers=True)
        self.assertEqual(real_result.findings[0].disposition, TrackerDisposition.VERIFIED_FROM_EVIDENCE)
        self.assertFalse(real_repo.marked[0][2])

    def test_excluded_tracker_is_not_queried_or_updated(self):
        repository = self.Repository([self.bug], {self.bug.id: [self.comment]})

        result = self._service(repository=repository, excluded=(self.bug.id,)).run(update_trackers=True)

        self.assertEqual(result.findings[0].disposition, TrackerDisposition.EXCLUDED)
        self.assertEqual(repository.marked, [])
        self.assertEqual(repository.comment_calls, [])

    def test_verified_and_ineligible_statuses_skip_verifier_comment_lookup(self):
        verified = _tracker(status="VERIFIED")
        repository = self.Repository([verified])
        result = self._service(repository=repository).run(update_trackers=True)
        self.assertEqual(result.findings[0].disposition, TrackerDisposition.ALREADY_VERIFIED)
        self.assertEqual(repository.comment_calls, [])

        closed = _tracker(status="CLOSED")
        repository = self.Repository([closed])
        result = self._service(repository=repository).run(update_trackers=True)
        self.assertEqual(result.findings[0].disposition, TrackerDisposition.STATUS_NOT_ELIGIBLE)
        self.assertEqual(repository.comment_calls, [])

    def test_failed_verifier_and_comment_lookup_error_require_review(self):
        repository = self.Repository(
            [self.bug],
            {self.bug.id: [_verification_comment(result="FAIL")]},
        )
        result = self._service(repository=repository).run(update_trackers=True)
        self.assertEqual(result.findings[0].disposition, TrackerDisposition.NEEDS_REVIEW)
        self.assertEqual(repository.marked, [])

        repository = self.Repository([self.bug], comments_error=RuntimeError("Jira comments unavailable"))
        result = self._service(repository=repository).run()
        self.assertEqual(result.findings[0].disposition, TrackerDisposition.EVIDENCE_INCOMPLETE)
        self.assertIn("Unable to read verification comments", result.findings[0].reason)

    def test_tracker_search_and_selected_inventory_failures_are_reported(self):
        repo = self.Repository([self.bug], target_error=RuntimeError("Jira unavailable"))
        result = self._service(repository=repo).run()
        self.assertFalse(result.tracker_search_complete)
        self.assertIn("Jira tracker search incomplete", result.warnings[0])

        inventory_source = self.Inventory((), (), selected_error=RuntimeError("image unavailable"))
        result = self._service(inventory_source=inventory_source).run()
        self.assertEqual(result.findings[0].disposition, TrackerDisposition.EVIDENCE_INCOMPLETE)
        self.assertIn("Selected RHCOS RPM inventory unavailable", result.findings[0].reason)

    def test_new_changelog_evidence_can_qualify_without_verifier_comment(self):
        basis = _image("x86_64", (_rpm("kernel-core", release="100.el9_8"),))
        selected = _image("x86_64", (_rpm("kernel-core", release="200.el9_8"),))
        selected_nvr = RHCOSRpm.KERNEL.build_nvr(selected.rpms[0])
        basis_nvr = RHCOSRpm.KERNEL.build_nvr(basis.rpms[0])
        source = self.BuildEvidence(
            (
                RpmBuildEvidence(selected_nvr, ("CVE-2026-12345",), True, (), True),
                RpmBuildEvidence(basis_nvr, (), True, (), True),
            )
        )
        repository = self.Repository([self.bug], {self.bug.id: []})
        service = self._service(
            repository=repository,
            inventory_source=self.Inventory((selected,), (basis,)),
            build_source=source,
        )

        result = service.run()

        self.assertEqual(result.findings[0].disposition, TrackerDisposition.READY_TO_VERIFY)
        self.assertTrue(result.findings[0].rpm_evidence[0].new_changelog_match)
        self.assertEqual(source.requested, {selected_nvr, basis_nvr})


class TestReportAndAdapters(unittest.TestCase):
    """Test report persistence and external-system adapters in isolation."""

    def test_report_render_and_write_include_findings_and_warnings(self):
        finding = TrackerFinding(
            "OCPBUGS-1",
            "CVE-2026-12345",
            "openshift/ose-rhel-coreos-9",
            "NEW",
            TrackerDisposition.NEEDS_REVIEW,
            "RPM is too old",
        )
        evidence_report = AnalysisReport("4.22.22", (), False, (), (finding,), True, warnings=("partial",))

        rendered = report.render_report(evidence_report)
        with tempfile.TemporaryDirectory() as temp_dir:
            json_path, text_path = report.write_report(Path(temp_dir), evidence_report)
            persisted = json.loads(json_path.read_text())
            self.assertTrue(text_path.exists())

        self.assertIn("OCPBUGS-1", rendered)
        self.assertIn("Warnings:", rendered)
        self.assertEqual(persisted["findings"][0]["reason"], "RPM is too old")

    def test_inventory_metadata_converters_and_architecture_checks(self):
        converted = inventory._installed_rpm(["kernel-core", 0, "5.14.0", "100.el9_8", "x86_64", "base"])
        self.assertEqual(converted.stream, "base")
        self.assertEqual(inventory._installed_rpm(["pkg", None, "1", "2.el9_8", "noarch"]).epoch, "0")
        with self.assertRaisesRegex(ValueError, "Invalid RHCOS RPM metadata row"):
            inventory._installed_rpm(["too", "short"])
        with self.assertRaisesRegex(ValueError, "another architecture"):
            inventory._validate_kernel_rpm_architecture((_rpm("kernel-core", arch="x86_64"),), "aarch64", "Basis")

    def test_pullspec_pinning_and_architecture_normalization(self):
        digest = "repo/image@sha256:abc"
        with patch.object(inventory, "oc_image_info_for_arch") as image_info:
            self.assertEqual(inventory._pin_pullspec(digest, "x86_64", None), digest)
            image_info.assert_not_called()
            image_info.return_value = {"digest": "sha256:def"}
            pinned = inventory._pin_pullspec("repo/image:tag", "x86_64", "/tmp/auth.json")

        self.assertEqual(pinned, "repo/image@sha256:def")
        image_info.assert_called_once_with("repo/image:tag", "amd64", registry_config="/tmp/auth.json")
        self.assertEqual(
            inventory._brew_arches({"amd64": "ref", "arm64": "ref", "bad-arch": "ref"}), {"x86_64", "aarch64"}
        )

    def test_provider_architecture_scope_uses_only_basis_supported_arches(self):
        runtime = SimpleNamespace(
            group_config=SimpleNamespace(arches=("amd64", "arm64")),
            logger=Mock(),
        )
        provider = inventory.RHCOSInventoryProvider(runtime)
        primary = SimpleNamespace(name="rhel-coreos")

        architectures = provider._architectures_with_basis({"amd64": "payload"}, None, primary)

        self.assertEqual(architectures, ("x86_64",))
        runtime.logger.info.assert_called_once()
        with self.assertRaisesRegex(ValueError, "no RHCOS references"):
            provider._architectures_with_basis({"s390x": "payload"}, None, primary)

    def test_basis_payload_resolution_skips_optional_images_but_requires_primary(self):
        provider = inventory.RHCOSInventoryProvider(SimpleNamespace(registry_config=None))
        primary = SimpleNamespace(name="rhel-coreos", primary=True)
        optional = SimpleNamespace(name="rhel-coreos-extensions", primary=False)

        with patch.object(
            inventory.exectools,
            "cmd_assert",
            side_effect=[("repo/coreos@sha256:abc", ""), RuntimeError("missing")],
        ) as command:
            pullspecs = provider._basis_pullspecs_from_payload("repo/payload:tag", [primary, optional], primary)

        self.assertEqual(pullspecs, {"rhel-coreos": "repo/coreos@sha256:abc"})
        self.assertEqual(command.call_count, 2)

        with patch.object(inventory.exectools, "cmd_assert", return_value=("", "")):
            with self.assertRaisesRegex(ValueError, "has no primary RHCOS image"):
                provider._basis_pullspecs_from_payload("repo/payload:tag", [primary], primary)

    def test_basis_assembly_pullspecs_require_primary_pin(self):
        provider = inventory.RHCOSInventoryProvider(SimpleNamespace())
        primary = SimpleNamespace(name="rhel-coreos", primary=True)
        optional = SimpleNamespace(name="rhel-coreos-extensions", primary=False)
        basis_config = {
            "rhel-coreos": {"images": {"x86_64": "repo/coreos:tag"}},
            "rhel-coreos-extensions": {"images": {"x86_64": "repo/extensions:tag"}},
        }

        pullspecs = provider._basis_assembly_pullspecs(basis_config, [primary, optional], primary, "x86_64")

        self.assertEqual(pullspecs, {"rhel-coreos": "repo/coreos:tag", "rhel-coreos-extensions": "repo/extensions:tag"})
        with self.assertRaisesRegex(ValueError, "no pinned primary RHCOS image"):
            provider._basis_assembly_pullspecs({}, [primary], primary, "x86_64")

    def test_selected_pullspecs_use_stream_fallback_only_for_stream_assemblies(self):
        primary = SimpleNamespace(name="rhel-coreos", primary=True)
        stream_runtime = SimpleNamespace(
            assembly="stream",
            assembly_type=inventory.AssemblyTypes.STREAM,
            registry_config=None,
            get_minor_version=Mock(return_value="4.22"),
        )
        stream_provider = inventory.RHCOSInventoryProvider(stream_runtime)
        with patch.object(inventory, "RHCOSBuildFinder") as finder_class:
            finder_class.return_value.latest_container.return_value = ("4.22.0", "repo/coreos:tag")
            pullspecs = stream_provider._selected_pullspecs({}, [primary], primary, "x86_64")

        self.assertEqual(pullspecs, {"rhel-coreos": "repo/coreos:tag"})
        finder_class.assert_called_once_with(stream_runtime, "4.22", "x86_64", registry_config=None)

        pinned_provider = inventory.RHCOSInventoryProvider(
            SimpleNamespace(assembly="4.22.22", assembly_type="standard", registry_config=None)
        )
        pinned = pinned_provider._selected_pullspecs(
            {"rhel-coreos": {"images": {"x86_64": "repo/coreos@sha256:abc"}}}, [primary], primary, "x86_64"
        )
        self.assertEqual(pinned, {"rhel-coreos": "repo/coreos@sha256:abc"})
        with self.assertRaisesRegex(ValueError, "has no selected RHCOS image"):
            pinned_provider._selected_pullspecs({}, [primary], primary, "x86_64")

    def test_collect_pins_selected_images_and_builds_domain_inventories(self):
        runtime = MagicMock()
        runtime.assembly = "4.22.22"
        runtime.registry_config = None
        primary = SimpleNamespace(name="rhel-coreos", primary=True)
        provider = inventory.RHCOSInventoryProvider(runtime)
        inspector = Mock()
        inspector.build_id = "4.22.9.8"
        inspector.get_os_metadata_rpm_list.return_value = [["kernel-core", "0", "5.14.0", "200.el9_8", "x86_64"]]
        assembly_config = {"rhel-coreos": {"images": {"x86_64": "repo/image:tag"}}}
        config_model = SimpleNamespace(primitive=lambda: assembly_config)
        basis_model = SimpleNamespace(primitive=lambda: {"reference_releases": {"amd64": "payload"}})

        with (
            patch.object(inventory, "assembly_rhcos_config", return_value=config_model),
            patch.object(inventory, "assembly_basis", return_value=basis_model),
            patch.object(inventory, "get_container_configs", return_value=[primary]),
            patch.object(provider, "_architectures_with_basis", return_value=("x86_64",)),
            patch.object(provider, "_selected_pullspecs", return_value={"rhel-coreos": "repo/image:tag"}),
            patch.object(inventory, "_pin_pullspec", return_value="repo/image@sha256:abc") as pin,
            patch.object(inventory, "RHCOSBuildInspector", return_value=inspector) as inspector_class,
        ):
            images = provider.collect()

        self.assertEqual(len(images), 1)
        self.assertEqual(images[0].arch, "x86_64")
        self.assertEqual(images[0].pullspecs, (("rhel-coreos", "repo/image@sha256:abc"),))
        self.assertEqual(images[0].rpms[0].name, "kernel-core")
        pin.assert_called_once_with("repo/image:tag", "x86_64", None)
        inspector_class.assert_called_once_with(
            runtime,
            {"rhel-coreos": "repo/image@sha256:abc"},
            "x86_64",
            registry_config=None,
        )

    def test_collect_basis_reuses_payload_resolution_across_architectures(self):
        runtime = MagicMock()
        runtime.assembly = "4.22.22"
        runtime.build_system = "konflux"
        runtime.registry_config = None
        primary = SimpleNamespace(name="rhel-coreos", primary=True)
        provider = inventory.RHCOSInventoryProvider(runtime)
        basis_data = {
            "reference_releases": {
                "amd64": "registry/payload:4.22.0",
                "arm64": "registry/payload:4.22.0",
            }
        }
        basis_model = SimpleNamespace(primitive=lambda: basis_data)

        def _make_inspector(_runtime, _pullspecs, brew_arch, registry_config=None):
            inspector = Mock()
            inspector.build_id = f"build-{brew_arch}"
            inspector.get_os_metadata_rpm_list.return_value = [["kernel-core", "0", "5.14.0", "200.el9_8", brew_arch]]
            return inspector

        with (
            patch.object(inventory, "assembly_basis", return_value=basis_model),
            patch.object(inventory, "get_container_configs", return_value=[primary]),
            patch.object(provider, "_architectures_with_basis", return_value=("x86_64", "aarch64")),
            patch.object(provider, "_basis_reference_for_arch", return_value="registry/payload:4.22.0"),
            patch.object(
                provider, "_basis_pullspecs_from_payload", return_value={"rhel-coreos": "repo/rhcos:tag"}
            ) as resolve_payload,
            patch.object(inventory, "_pin_pullspec", side_effect=lambda pullspec, *_: pullspec),
            patch.object(inventory, "RHCOSBuildInspector", side_effect=_make_inspector),
        ):
            images = provider.collect_basis()

        self.assertEqual([image.arch for image in images], ["x86_64", "aarch64"])
        self.assertEqual([image.build_id for image in images], ["build-x86_64", "build-aarch64"])
        resolve_payload.assert_called_once_with("registry/payload:4.22.0", [primary], primary)

    def test_build_evidence_source_normalizes_cves_and_keeps_partial_failure_details(self):
        runtime = MagicMock()
        koji = MagicMock()
        koji.getChangelogEntries.return_value = [
            {"text": "Fix CVE-2026-12345 and cve-2026-12345"},
            {"changelog": "Related CVE-2026-99999"},
        ]
        runtime.build_retrying_koji_client.return_value = koji
        with (
            patch.object(build_evidence, "Build") as build_class,
            patch.object(build_evidence, "Erratum") as erratum_class,
        ):
            build_class.return_value.all_errata_ids = [123]
            erratum = erratum_class.return_value
            erratum.cve_names = "CVE-2026-12345 CVE-2026-88888"
            erratum.url.return_value = "https://errata/123"
            erratum.errata_state = "QE"

            result = build_evidence.KojiErrataRpmBuildEvidenceSource(runtime).collect({"kernel-1-1.el9"})

        self.assertEqual(result[0].changelog_cves, ("CVE-2026-12345", "CVE-2026-99999"))
        self.assertTrue(result[0].changelog_complete)
        self.assertEqual(result[0].advisory_cves, {"CVE-2026-12345", "CVE-2026-88888"})
        self.assertTrue(result[0].advisory_lookup_complete)

    def test_build_evidence_source_keeps_advisory_results_when_brew_client_fails(self):
        runtime = MagicMock()
        runtime.build_retrying_koji_client.side_effect = RuntimeError("Brew unavailable")
        with (
            patch.object(build_evidence, "Build") as build_class,
            patch.object(build_evidence, "Erratum") as erratum_class,
        ):
            build_class.return_value.all_errata_ids = [123]
            erratum_class.return_value.cve_names = "CVE-2026-12345"
            erratum_class.return_value.url.return_value = "https://errata/123"
            erratum_class.return_value.errata_state = "QE"

            results = build_evidence.KojiErrataRpmBuildEvidenceSource(runtime).collect(
                {"kernel-1-1.el9", "kernel-2-1.el9"}
            )

        self.assertEqual(len(results), 2)
        self.assertTrue(all(not item.changelog_complete for item in results))
        self.assertTrue(all(item.advisory_lookup_complete for item in results))
        self.assertTrue(all("Unable to initialize Brew changelog client" in item.warnings[0] for item in results))

    def test_jira_repository_scopes_search_and_refuses_unconfigured_releases(self):
        tracker = MagicMock()
        tracker.target_release.return_value = ["4.22.z"]
        tracker.available_target_versions.return_value = ["4.22.z"]
        rhcos_bug = SimpleNamespace(cve_id="CVE-1", whiteboard_component="openshift/ose-rhel-coreos-9")
        non_rhcos_bug = SimpleNamespace(cve_id="CVE-2", whiteboard_component="openshift/ose-control-plane")
        no_cve_bug = SimpleNamespace(cve_id=None, whiteboard_component="openshift/ose-rhel-coreos-9")
        tracker.search_bugs.return_value = [rhcos_bug, non_rhcos_bug, no_cve_bug]
        adapter = repository.JiraRHCOSTrackerRepository(tracker)

        self.assertEqual(adapter.target_releases(), ("4.22.z",))
        self.assertEqual(adapter.find_trackers(), [rhcos_bug])
        tracker.search_bugs.assert_called_with(
            status=None,
            search_filter=None,
            include_labels=["SecurityTracking"],
            custom_query='and status != "Closed"',
        )
        tracker.target_release.return_value = []
        with self.assertRaisesRegex(ValueError, "refusing an unscoped"):
            adapter.target_releases()

        tracker.target_release.return_value = ["4.22.1"]
        tracker.available_target_versions.return_value = ["4.22.z"]
        with self.assertRaisesRegex(ValueError, "unavailable"):
            adapter.target_releases()

    def test_jira_repository_adapts_comment_records(self):
        tracker = MagicMock()
        tracker.client.return_value.comments.return_value = [
            SimpleNamespace(created="2026-11-01", body="verifier comment")
        ]
        adapter = repository.JiraRHCOSTrackerRepository(tracker)

        self.assertEqual(
            adapter.comments("OCPBUGS-1"),
            [VerificationComment("2026-11-01", "verifier comment")],
        )

    def test_jira_repository_marks_verified_safely(self):
        tracker = MagicMock()
        tracker.client.return_value.issue.return_value = SimpleNamespace(status="NEW")
        tracker.update_bug_status.return_value = True
        adapter = repository.JiraRHCOSTrackerRepository(tracker)
        bug = _tracker()

        self.assertTrue(adapter.mark_verified(bug, "evidence", dry_run=True))
        tracker.add_comment.assert_not_called()
        tracker.update_bug_status.assert_called_with(bug, "VERIFIED", comment="evidence", log_comment=True, noop=True)

        with patch.object(repository, "JIRABug", return_value=SimpleNamespace(status="NEW")) as jira_bug:
            self.assertTrue(adapter.mark_verified(bug, "evidence", dry_run=False))
        tracker.add_comment.assert_called_once_with(bug.id, "evidence", private=True)
        tracker.update_bug_status.assert_called_with(
            jira_bug.return_value,
            "VERIFIED",
            log_comment=False,
            noop=False,
        )


if __name__ == "__main__":
    unittest.main()
