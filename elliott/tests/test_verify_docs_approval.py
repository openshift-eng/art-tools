"""
Tests for elliottlib.cli.verify_docs_approval.
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.verify_docs_approval import (
    CheckResult,
    _build_advisory_text,
    _is_dropped,
    _normalize_errata_name,
    check_extras_references_image,
    check_image_does_not_reference_dropped_rpm,
    check_image_references_rpm,
    check_payload_shas,
    check_rhcos_does_not_reference_dropped_rpm,
    check_rhcos_payload_shas,
    check_rhcos_references_rpm,
    check_rpm_references_image,
    contains_advisory_reference,
    contains_exact_advisory_url,
    extract_advisory_name,
    extract_payload_shas,
    format_report,
    run_checks,
)
from elliottlib.shipment_model import ReleaseNotes


def _release_notes(advisory_type: str, live_id: int, description: str = "") -> ReleaseNotes:
    """
    Build a minimal ReleaseNotes for tests.

    Arg(s):
        advisory_type (str): RHSA/RHBA/RHEA.
        live_id (int): the advisory's live id.
        description (str): the description text to test against.
    Return Value(s):
        ReleaseNotes: a minimal, valid instance.
    """
    return ReleaseNotes(type=advisory_type, live_id=live_id, description=description)


def test_contains_exact_advisory_url_match():
    text = "See https://access.redhat.com/errata/RHBA-2026:48673 for details."
    assert contains_exact_advisory_url(text, "RHBA-2026:48673") is True


def test_contains_exact_advisory_url_no_match():
    text = "See https://access.redhat.com/errata/RHBA-2026:99999 for details."
    assert contains_exact_advisory_url(text, "RHBA-2026:48673") is False


def test_contains_exact_advisory_url_no_prefix_match():
    # "RHBA-2026:4867" must not match a URL containing "RHBA-2026:48673"
    text = "See https://access.redhat.com/errata/RHBA-2026:48673 for details."
    assert contains_exact_advisory_url(text, "RHBA-2026:4867") is False


def test_contains_advisory_reference_match_any_year():
    text = "See https://access.redhat.com/errata/RHSA-2026:48676 for details."
    assert contains_advisory_reference(text, "RHSA", 48676) is True


def test_contains_advisory_reference_wrong_live_id():
    text = "See https://access.redhat.com/errata/RHSA-2026:11111 for details."
    assert contains_advisory_reference(text, "RHSA", 48676) is False


def test_extract_advisory_name_found():
    text = "See https://access.redhat.com/errata/RHSA-2026:48676 for details."
    assert extract_advisory_name(text, "RHSA", 48676) == "RHSA-2026:48676"


def test_extract_advisory_name_not_found():
    text = "no advisory reference here"
    assert extract_advisory_name(text, "RHSA", 48676) is None


def test_check_extras_references_image_pass():
    image = _release_notes("RHSA", 48676)
    extras = _release_notes("RHBA", 48677, description="See https://access.redhat.com/errata/RHSA-2026:48676")
    result = check_extras_references_image(extras, image, "RHSA-2026:48676")
    assert result.status == "pass"


def test_check_extras_references_image_fail():
    image = _release_notes("RHSA", 48676)
    extras = _release_notes("RHBA", 48677, description="no reference here")
    result = check_extras_references_image(extras, image, "RHSA-2026:48676")
    assert result.status == "fail"


def test_check_extras_references_image_skip_when_missing():
    result = check_extras_references_image(None, _release_notes("RHSA", 48676), "RHSA-2026:48676")
    assert result.status == "skip"


def test_check_image_references_rpm_pass():
    image = _release_notes("RHSA", 48676, description="See https://access.redhat.com/errata/RHBA-2026:48673")
    result = check_image_references_rpm(image, "RHBA-2026:48673")
    assert result.status == "pass"


def test_check_image_references_rpm_fail():
    image = _release_notes("RHSA", 48676, description="no reference here")
    result = check_image_references_rpm(image, "RHBA-2026:48673")
    assert result.status == "fail"


def test_check_image_references_rpm_skip_when_no_rpm_advisory():
    image = _release_notes("RHSA", 48676, description="anything")
    result = check_image_references_rpm(image, None)
    assert result.status == "skip"


def test_check_rpm_references_image_pass():
    image = _release_notes("RHSA", 48676)
    rpm_description = "See https://access.redhat.com/errata/RHSA-2026:48676"
    result = check_rpm_references_image(rpm_description, image, "RHSA-2026:48676")
    assert result.status == "pass"


def test_check_rpm_references_image_fail():
    image = _release_notes("RHSA", 48676)
    result = check_rpm_references_image("no reference here", image, "RHSA-2026:48676")
    assert result.status == "fail"


def test_check_extras_references_image_skip_when_extras_description_is_none():
    image = _release_notes("RHSA", 48676)
    extras = ReleaseNotes(type="RHBA", live_id=48677)  # description defaults to None
    result = check_extras_references_image(extras, image, "RHSA-2026:48676")
    assert result.status == "skip"


def test_check_image_references_rpm_skip_when_image_description_is_none():
    image = ReleaseNotes(type="RHSA", live_id=48676)  # description defaults to None
    result = check_image_references_rpm(image, "RHBA-2026:48673")
    assert result.status == "skip"


def test_check_extras_references_image_skip_when_image_live_id_is_none():
    image = ReleaseNotes(type="RHSA", live_id=48676)
    image.live_id = None  # Force live_id to None
    extras = _release_notes("RHBA", 48677, description="See https://access.redhat.com/errata/RHSA-2026:48676")
    result = check_extras_references_image(extras, image, None)
    assert result.status == "skip"


def test_check_rpm_references_image_skip_when_image_live_id_is_none():
    image = ReleaseNotes(type="RHSA", live_id=48676)
    image.live_id = None  # Force live_id to None
    rpm_description = "See https://access.redhat.com/errata/RHSA-2026:48676"
    result = check_rpm_references_image(rpm_description, image, None)
    assert result.status == "skip"


SAMPLE_SOLUTION = """\
The sha values for the release are as follows:

      (For x86_64 architecture)
      The image digest is sha256:d03909e954e9a6d24900809750bd39f5f69d8a9e480eec97d481b9977c430d4d

      (For aarch64 architecture)
      The image digest is sha256:907f840289ef117890d19775bdde1ca03111a40e986824b12457a865929e91b9
"""


def test_extract_payload_shas():
    shas = extract_payload_shas(SAMPLE_SOLUTION)
    assert shas == {
        "x86_64": "sha256:d03909e954e9a6d24900809750bd39f5f69d8a9e480eec97d481b9977c430d4d",
        "aarch64": "sha256:907f840289ef117890d19775bdde1ca03111a40e986824b12457a865929e91b9",
    }


def test_extract_payload_shas_two_line_format():
    # RHCOS advisories split "The image digest is" and the SHA across two lines
    text = "(For x86_64 architecture)\nThe image digest is\nsha256:abc123"
    assert extract_payload_shas(text) == {"x86_64": "sha256:abc123"}


def test_extract_payload_shas_none_found():
    assert extract_payload_shas("no digests here") == {}


class TestCheckPayloadShas(unittest.IsolatedAsyncioTestCase):
    """
    Verify check_payload_shas cross-checks image shipment SHAs against `oc adm release info`.
    """

    async def test_skip_when_no_image_release_notes(self):
        result, shas = await check_payload_shas(None, "4.20.32")
        assert result.status == "skip"
        assert shas == {}

    async def test_fail_when_no_shas_found(self):
        release_notes = ReleaseNotes(type="RHSA", live_id=1, solution="no digests here")
        result, shas = await check_payload_shas(release_notes, "4.20.32")
        assert result.status == "fail"
        assert shas == {}

    async def test_skip_when_image_solution_is_none(self):
        release_notes = ReleaseNotes(type="RHSA", live_id=1)  # solution defaults to None
        result, shas = await check_payload_shas(release_notes, "4.20.32")
        assert result.status == "skip"
        assert shas == {}

    @patch("elliottlib.cli.verify_docs_approval.fetch_payload_digest", new_callable=AsyncMock)
    async def test_pass_when_digests_match(self, mock_fetch):
        mock_fetch.return_value = "sha256:d03909e954e9a6d24900809750bd39f5f69d8a9e480eec97d481b9977c430d4d"
        release_notes = ReleaseNotes(
            type="RHSA",
            live_id=1,
            solution="(For x86_64 architecture)\nThe image digest is sha256:d03909e954e9a6d24900809750bd39f5f69d8a9e480eec97d481b9977c430d4d",
        )
        result, shas = await check_payload_shas(release_notes, "4.20.32")
        assert result.status == "pass"
        assert shas == {"x86_64": "sha256:d03909e954e9a6d24900809750bd39f5f69d8a9e480eec97d481b9977c430d4d"}
        mock_fetch.assert_awaited_once_with("quay.io/openshift-release-dev/ocp-release:4.20.32-x86_64")

    @patch("elliottlib.cli.verify_docs_approval.fetch_payload_digest", new_callable=AsyncMock)
    async def test_fail_when_digest_mismatch(self, mock_fetch):
        mock_fetch.return_value = "sha256:doesnotmatch"
        release_notes = ReleaseNotes(
            type="RHSA",
            live_id=1,
            solution="(For x86_64 architecture)\nThe image digest is sha256:d03909e954e9a6d24900809750bd39f5f69d8a9e480eec97d481b9977c430d4d",
        )
        result, _ = await check_payload_shas(release_notes, "4.20.32")
        assert result.status == "fail"


_RHCOS_SHA_BLOCK = "(For x86_64 architecture)\nThe image digest is sha256:abc123"


def test_check_rhcos_references_rpm_pass():
    text = f"{_RHCOS_SHA_BLOCK}\nhttps://access.redhat.com/errata/RHBA-2026:48673"
    result = check_rhcos_references_rpm(text, "RHBA-2026:48673")
    assert result.status == "pass"


def test_check_rhcos_references_rpm_fail():
    result = check_rhcos_references_rpm(_RHCOS_SHA_BLOCK, "RHBA-2026:48673")
    assert result.status == "fail"
    assert "RHBA-2026:48673" in result.detail


def test_check_rhcos_references_rpm_skip_when_no_advisory():
    result = check_rhcos_references_rpm(None, "RHBA-2026:48673")
    assert result.status == "skip"


def test_check_rhcos_references_rpm_skip_when_no_rpm():
    result = check_rhcos_references_rpm(_RHCOS_SHA_BLOCK, None)
    assert result.status == "skip"


def test_check_rhcos_payload_shas_pass():
    rhcos_text = "(For x86_64 architecture)\nThe image digest is sha256:abc123"
    result = check_rhcos_payload_shas(rhcos_text, {"x86_64": "sha256:abc123"})
    assert result.status == "pass"


def test_check_rhcos_payload_shas_fail_mismatch():
    rhcos_text = "(For x86_64 architecture)\nThe image digest is sha256:different"
    result = check_rhcos_payload_shas(rhcos_text, {"x86_64": "sha256:abc123"})
    assert result.status == "fail"
    assert "x86_64" in result.detail


def test_check_rhcos_payload_shas_fail_arch_missing_in_rhcos():
    rhcos_text = "(For x86_64 architecture)\nThe image digest is sha256:abc123"
    result = check_rhcos_payload_shas(rhcos_text, {"x86_64": "sha256:abc123", "aarch64": "sha256:def456"})
    assert result.status == "fail"
    assert "aarch64" in result.detail


def test_check_rhcos_payload_shas_fail_no_shas_in_rhcos():
    result = check_rhcos_payload_shas("no digests here", {"x86_64": "sha256:abc123"})
    assert result.status == "fail"


def test_check_rhcos_payload_shas_skip_when_no_advisory():
    result = check_rhcos_payload_shas(None, {"x86_64": "sha256:abc123"})
    assert result.status == "skip"


def test_check_rhcos_payload_shas_skip_when_no_image_shas():
    result = check_rhcos_payload_shas(_RHCOS_SHA_BLOCK, {})
    assert result.status == "skip"


def test_is_dropped_true():
    erratum = MagicMock(errata_state="DROPPED_NO_SHIP")
    assert _is_dropped(erratum) is True


def test_is_dropped_false():
    erratum = MagicMock(errata_state="SHIPPED_LIVE")
    assert _is_dropped(erratum) is False


def test_check_image_does_not_reference_dropped_rpm_pass():
    image = _release_notes("RHSA", 48676, description="no rpm reference here")
    result = check_image_does_not_reference_dropped_rpm(image, "RHBA-2026:44227")
    assert result.status == "pass"


def test_check_image_does_not_reference_dropped_rpm_fail():
    image = _release_notes("RHSA", 48676, description="See https://access.redhat.com/errata/RHBA-2026:44227")
    result = check_image_does_not_reference_dropped_rpm(image, "RHBA-2026:44227")
    assert result.status == "fail"
    assert "RHBA-2026:44227" in result.detail


def test_check_image_does_not_reference_dropped_rpm_skip_when_no_image():
    result = check_image_does_not_reference_dropped_rpm(None, "RHBA-2026:44227")
    assert result.status == "skip"


def test_check_rhcos_does_not_reference_dropped_rpm_pass():
    result = check_rhcos_does_not_reference_dropped_rpm("no rpm reference here", "RHBA-2026:44227")
    assert result.status == "pass"


def test_check_rhcos_does_not_reference_dropped_rpm_fail():
    text = "See https://access.redhat.com/errata/RHBA-2026:44227"
    result = check_rhcos_does_not_reference_dropped_rpm(text, "RHBA-2026:44227")
    assert result.status == "fail"
    assert "RHBA-2026:44227" in result.detail


def test_check_rhcos_does_not_reference_dropped_rpm_skip_when_no_rhcos():
    result = check_rhcos_does_not_reference_dropped_rpm(None, "RHBA-2026:44227")
    assert result.status == "skip"


def test_build_advisory_text_handles_none_description():
    erratum = MagicMock(description=None, solution="some solution text")
    text = _build_advisory_text(erratum)
    assert "None" not in text
    assert text == "\nsome solution text"


def test_build_advisory_text_handles_none_solution():
    erratum = MagicMock(description="some description text", solution=None)
    text = _build_advisory_text(erratum)
    assert "None" not in text
    assert text == "some description text\n"


def test_build_advisory_text_both_present():
    erratum = MagicMock(description="desc", solution="sol")
    assert _build_advisory_text(erratum) == "desc\nsol"


def test_normalize_errata_name_strips_revision_suffix():
    assert _normalize_errata_name("RHBA-2026:48691-02") == "RHBA-2026:48691"


def test_normalize_errata_name_leaves_unrevised_name_unchanged():
    assert _normalize_errata_name("RHBA-2026:48691") == "RHBA-2026:48691"


def test_format_report_includes_status_and_detail():
    results = [CheckResult(name="some check", status="pass", detail="all good")]
    report = format_report(results)
    assert "PASS" in report
    assert "some check" in report
    assert "all good" in report


class TestRunChecks(unittest.IsolatedAsyncioTestCase):
    """
    Verify run_checks wires Runtime/assembly-config/errata-tool lookups into the five checks.
    """

    def _make_runtime(self, rpm_advisory_id=170123, rhcos_advisory_id=170124, assembly="4.20.32"):
        runtime = MagicMock()
        runtime.assembly = assembly
        group_config = MagicMock()
        group_config.get.return_value = {"rpm": rpm_advisory_id, "rhcos": rhcos_advisory_id}
        runtime.get_releases_config.return_value = MagicMock()
        return runtime

    @patch("elliottlib.cli.verify_docs_approval.assembly_config_struct")
    @patch("elliottlib.cli.verify_docs_approval.Erratum")
    @patch("elliottlib.cli.verify_docs_approval.fetch_payload_digest", new_callable=AsyncMock)
    async def test_run_checks_all_pass(self, mock_fetch, mock_erratum_cls, mock_assembly_struct):
        image_config_raw = {
            "shipment": {
                "metadata": {"product": "ocp", "application": "app", "group": "openshift-4.20", "assembly": "4.20.32"},
                "environments": {"stage": {"releasePlan": "p"}, "prod": {"releasePlan": "p"}},
                "data": {
                    "releaseNotes": {
                        "type": "RHSA",
                        "live_id": 48676,
                        "description": "See https://access.redhat.com/errata/RHBA-2026:48673",
                        "solution": "(For x86_64 architecture)\nThe image digest is sha256:abc",
                    }
                },
            }
        }
        extras_config_raw = {
            "shipment": {
                "metadata": {"product": "ocp", "application": "app", "group": "openshift-4.20", "assembly": "4.20.32"},
                "environments": {"stage": {"releasePlan": "p"}, "prod": {"releasePlan": "p"}},
                "data": {
                    "releaseNotes": {
                        "type": "RHBA",
                        "live_id": 48677,
                        "description": "See https://access.redhat.com/errata/RHSA-2026:48676",
                        "solution": "n/a",
                    }
                },
            }
        }
        runtime = self._make_runtime()
        runtime.shipment_gitdata.load_yaml_file.side_effect = lambda path: {
            "image.yaml": image_config_raw,
            "extras.yaml": extras_config_raw,
        }[path]

        mock_assembly_struct.return_value.get.return_value = {"rpm": 170123, "rhcos": 170124}

        rpm_erratum = MagicMock(
            # "-02" simulates a revised advisory's fulladvisory suffix (see _normalize_errata_name);
            # the image/rhcos text below reference the unsuffixed id, proving run_checks normalizes it.
            errata_name="RHBA-2026:48673-02",
            # rpm description references the image advisory — used to derive image_errata_name display string
            description="https://access.redhat.com/errata/RHSA-2026:48676",
        )
        rhcos_erratum = MagicMock(
            description=(
                "(For x86_64 architecture)\nThe image digest is sha256:abc\n"
                "https://access.redhat.com/errata/RHBA-2026:48673"
            ),
            solution="",
        )
        mock_erratum_cls.side_effect = [rpm_erratum, rhcos_erratum]
        mock_fetch.return_value = "sha256:abc"

        results = await run_checks(runtime, "image.yaml", "extras.yaml")

        assert len(results) == 6
        assert all(r.status == "pass" for r in results), results


if __name__ == "__main__":
    unittest.main()
