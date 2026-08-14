"""
elliott verify-docs-approval: verify shipment MR advisory cross-references
and per-architecture release payload SHA values, automating the manual
Docs team review that currently happens before prod release.
"""

import json
import re
from typing import Literal

import click
from artcommonlib import exectools
from artcommonlib.assembly import assembly_config_struct
from errata_tool import Erratum
from pydantic import BaseModel

from elliottlib import Runtime
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.shipment_model import ReleaseNotes, ShipmentConfig

PUBLIC_ERRATA_URL = "https://access.redhat.com/errata"
OCP_RELEASE_PULLSPEC_TEMPLATE = "quay.io/openshift-release-dev/ocp-release:{assembly}-{arch}"
_PAYLOAD_SHA_PATTERN = r"\(For (\S+) architecture\)\s*\n\s*The image digest is\s+(sha256:[0-9a-f]+)"


class CheckResult(BaseModel):
    """
    The outcome of a single docs-approval check.
    """

    name: str
    status: Literal["pass", "fail", "skip"]
    detail: str


def contains_exact_advisory_url(text: str, errata_name: str) -> bool:
    """
    Check whether text links to the exact given advisory.

    Arg(s):
        text (str): freeform advisory/release-notes text to search.
        errata_name (str): the advisory's full display name, e.g. "RHBA-2026:48673".
    Return Value(s):
        bool: True if text contains a link to that advisory.
    """
    pattern = rf"{re.escape(PUBLIC_ERRATA_URL)}/{re.escape(errata_name)}(?![-\w:])"
    return re.search(pattern, text) is not None


def _any_reference_to_advisory(text: str, errata_name: str) -> bool:
    """
    Check whether text references the advisory as a URL or a bare name.

    Used by the inverse (does-not-reference) checks to catch both:
      - URL form: https://access.redhat.com/errata/RHBA-2026:44227
      - Bare name: RHBA-2026:44227 (without the URL prefix)

    Arg(s):
        text (str): freeform advisory/release-notes text to search.
        errata_name (str): the advisory's full display name, e.g. "RHBA-2026:44227".
    Return Value(s):
        bool: True if text references that advisory in any form.
    """
    if contains_exact_advisory_url(text, errata_name):
        return True
    return bool(re.search(rf"(?<![-\w:]){re.escape(errata_name)}(?![-\w:])", text))


def contains_advisory_reference(text: str, advisory_type: str, live_id: int) -> bool:
    """
    Check whether text references an advisory by type and live id, for any year.

    The year isn't known for shipment-file-only advisories (image/extras),
    since it's not stored in the shipment YAML, so any 4-digit year matches.

    Arg(s):
        text (str): freeform advisory/release-notes text to search.
        advisory_type (str): RHSA/RHBA/RHEA.
        live_id (int): the advisory's live id.
    Return Value(s):
        bool: True if text references TYPE-<any year>:live_id.
    """
    pattern = rf"{re.escape(advisory_type)}-\d{{4}}:{live_id}\b"
    return re.search(pattern, text) is not None


def extract_advisory_name(text: str, advisory_type: str, live_id: int) -> str | None:
    """
    Extract the year-qualified advisory name from text.

    Searches for the first occurrence of TYPE-YYYY:live_id in text and returns
    it, giving callers the display name without needing a separate errata fetch.

    Arg(s):
        text (str): freeform advisory/release-notes text to search.
        advisory_type (str): RHSA/RHBA/RHEA.
        live_id (int): the advisory's live id.
    Return Value(s):
        str | None: the full display name (e.g. "RHSA-2026:51007"), or None if not found.
    """
    pattern = rf"({re.escape(advisory_type)}-\d{{4}}:{live_id})\b"
    match = re.search(pattern, text)
    return match.group(1) if match else None


def check_extras_references_image(
    extras_release_notes: ReleaseNotes | None,
    image_release_notes: ReleaseNotes | None,
    image_errata_name: str | None,
) -> CheckResult:
    """
    Check that the extras shipment's description references the image advisory.

    Arg(s):
        extras_release_notes (ReleaseNotes | None): the extras shipment's release notes, if provided.
        image_release_notes (ReleaseNotes | None): the image shipment's release notes, if provided.
        image_errata_name (str | None): the image advisory's full display name (e.g. "RHSA-2026:51007").
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "extras references image"
    if (
        extras_release_notes is None
        or image_release_notes is None
        or extras_release_notes.description is None
        or image_release_notes.live_id is None
    ):
        return CheckResult(name=name, status="skip", detail="extras or image shipment not provided")
    display = image_errata_name or f"{image_release_notes.type}:{image_release_notes.live_id}"
    if contains_advisory_reference(
        extras_release_notes.description, image_release_notes.type, image_release_notes.live_id
    ):
        return CheckResult(name=name, status="pass", detail=f"extras description references {display}")
    return CheckResult(
        name=name,
        status="fail",
        detail=f"extras description does not reference image advisory {display}",
    )


def check_image_references_rpm(image_release_notes: ReleaseNotes | None, rpm_errata_name: str | None) -> CheckResult:
    """
    Check that the image shipment's description references the rpm advisory.

    Arg(s):
        image_release_notes (ReleaseNotes | None): the image shipment's release notes, if provided.
        rpm_errata_name (str | None): the rpm advisory's full display name, if one is configured.
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "image references rpm"
    if image_release_notes is None or rpm_errata_name is None or image_release_notes.description is None:
        return CheckResult(name=name, status="skip", detail="image shipment or rpm advisory not available")
    if contains_exact_advisory_url(image_release_notes.description, rpm_errata_name):
        return CheckResult(name=name, status="pass", detail=f"image description references {rpm_errata_name}")
    return CheckResult(
        name=name, status="fail", detail=f"image description does not reference rpm advisory {rpm_errata_name}"
    )


def check_rpm_references_image(
    rpm_description: str | None,
    image_release_notes: ReleaseNotes | None,
    image_errata_name: str | None,
) -> CheckResult:
    """
    Check that the rpm advisory's own description references the image advisory.

    Arg(s):
        rpm_description (str | None): the rpm advisory's description text, if available.
        image_release_notes (ReleaseNotes | None): the image shipment's release notes, if provided.
        image_errata_name (str | None): the image advisory's full display name (e.g. "RHSA-2026:51007").
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "rpm references image"
    if rpm_description is None or image_release_notes is None or image_release_notes.live_id is None:
        return CheckResult(name=name, status="skip", detail="rpm advisory or image shipment not available")
    display = image_errata_name or f"{image_release_notes.type}:{image_release_notes.live_id}"
    if contains_advisory_reference(rpm_description, image_release_notes.type, image_release_notes.live_id):
        return CheckResult(name=name, status="pass", detail=f"rpm advisory references {display}")
    return CheckResult(
        name=name,
        status="fail",
        detail=f"rpm advisory does not reference image advisory {display}",
    )


def extract_payload_shas(solution_text: str) -> dict[str, str]:
    """
    Extract per-architecture release payload digests from an image
    shipment's `solution` text.

    Arg(s):
        solution_text (str): the image shipment's data.releaseNotes.solution text.
    Return Value(s):
        dict[str, str]: {arch: "sha256:<digest>"} for each arch found.
    """
    return dict(re.findall(_PAYLOAD_SHA_PATTERN, solution_text))


async def fetch_payload_digest(pullspec: str) -> str:
    """
    Fetch the manifest digest of a published release payload.

    Arg(s):
        pullspec (str): full release image pullspec, e.g.
            "quay.io/openshift-release-dev/ocp-release:4.20.32-x86_64".
    Return Value(s):
        str: the payload's manifest digest, e.g. "sha256:...".
    """
    rc, stdout, stderr = await exectools.cmd_gather_async(f"oc adm release info -o json {pullspec}")
    if rc != 0:
        raise RuntimeError(f"oc adm release info failed for {pullspec}: {stderr}")
    return json.loads(stdout)["digest"]


async def check_payload_shas(
    image_release_notes: ReleaseNotes | None, assembly: str
) -> tuple[CheckResult, dict[str, str]]:
    """
    Check that the per-arch SHAs in the image shipment's solution text match
    the published release payload's actual digests.

    Arg(s):
        image_release_notes (ReleaseNotes | None): the image shipment's release notes, if provided.
        assembly (str): the assembly name, used to build the release pullspec per arch.
    Return Value(s):
        tuple[CheckResult, dict[str, str]]: the check outcome, and the extracted
            {arch: digest} dict (for reuse by the rhcos check; empty if skipped/failed to extract).
    """
    name = "image payload SHAs"
    if image_release_notes is None or image_release_notes.solution is None:
        return CheckResult(name=name, status="skip", detail="image shipment solution not provided"), {}
    expected = extract_payload_shas(image_release_notes.solution)
    if not expected:
        return CheckResult(name=name, status="fail", detail="no per-arch SHA values found in image solution text"), {}
    mismatches = []
    for arch, expected_digest in expected.items():
        pullspec = OCP_RELEASE_PULLSPEC_TEMPLATE.format(assembly=assembly, arch=arch)
        actual_digest = await fetch_payload_digest(pullspec)
        if actual_digest != expected_digest:
            mismatches.append(f"{arch}: expected {expected_digest}, found {actual_digest}")
    if mismatches:
        return CheckResult(name=name, status="fail", detail="; ".join(mismatches)), expected
    return CheckResult(
        name=name, status="pass", detail=f"all {len(expected)} arch digests match published payload"
    ), expected


def check_rhcos_references_rpm(
    rhcos_text: str | None,
    rpm_errata_name: str | None,
) -> CheckResult:
    """
    Check that the RHCOS advisory references the rpm advisory.

    Arg(s):
        rhcos_text (str | None): the rhcos advisory's description + solution text, if configured.
        rpm_errata_name (str | None): the rpm advisory's full display name, if available.
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "rhcos references rpm"
    if rhcos_text is None:
        return CheckResult(name=name, status="skip", detail="no rhcos advisory configured for this assembly")
    if rpm_errata_name is None:
        return CheckResult(name=name, status="skip", detail="no rpm advisory configured")
    if contains_exact_advisory_url(rhcos_text, rpm_errata_name):
        return CheckResult(name=name, status="pass", detail=f"rhcos advisory references {rpm_errata_name}")
    return CheckResult(
        name=name, status="fail", detail=f"rhcos advisory does not reference rpm advisory {rpm_errata_name}"
    )


def check_image_does_not_reference_dropped_rpm(
    image_release_notes: ReleaseNotes | None,
    rpm_dropped_name: str,
) -> CheckResult:
    """
    When the rpm advisory is DROPPED_NO_SHIP, verify the image shipment
    description has been cleaned up and no longer references it.

    Arg(s):
        image_release_notes (ReleaseNotes | None): the image shipment's release notes, if provided.
        rpm_dropped_name (str): the dropped rpm advisory's full display name (e.g. "RHBA-2026:44227").
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "image does not reference dropped rpm"
    if image_release_notes is None or image_release_notes.description is None:
        return CheckResult(name=name, status="skip", detail="image shipment not available")
    if _any_reference_to_advisory(image_release_notes.description, rpm_dropped_name):
        return CheckResult(
            name=name, status="fail", detail=f"image description still references dropped advisory {rpm_dropped_name}"
        )
    return CheckResult(
        name=name, status="pass", detail=f"image description does not reference dropped advisory {rpm_dropped_name}"
    )


def check_rhcos_does_not_reference_dropped_rpm(
    rhcos_text: str | None,
    rpm_dropped_name: str,
) -> CheckResult:
    """
    When the rpm advisory is DROPPED_NO_SHIP, verify the RHCOS advisory
    description has been cleaned up and no longer references it.

    Arg(s):
        rhcos_text (str | None): the rhcos advisory's description + solution text, if configured.
        rpm_dropped_name (str): the dropped rpm advisory's full display name (e.g. "RHBA-2026:44227").
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "rhcos does not reference dropped rpm"
    if rhcos_text is None:
        return CheckResult(name=name, status="skip", detail="no rhcos advisory configured for this assembly")
    if _any_reference_to_advisory(rhcos_text, rpm_dropped_name):
        return CheckResult(
            name=name, status="fail", detail=f"rhcos advisory still references dropped advisory {rpm_dropped_name}"
        )
    return CheckResult(
        name=name, status="pass", detail=f"rhcos advisory does not reference dropped advisory {rpm_dropped_name}"
    )


def check_rhcos_payload_shas(
    rhcos_text: str | None,
    payload_shas: dict[str, str],
) -> CheckResult:
    """
    Check that the per-arch SHAs in the RHCOS advisory match those in the
    image shipment (which have already been verified against the live payload).

    Arg(s):
        rhcos_text (str | None): the rhcos advisory's description + solution text, if configured.
        payload_shas (dict[str, str]): the {arch: digest} dict from the image shipment.
    Return Value(s):
        CheckResult: pass/fail/skip outcome.
    """
    name = "rhcos payload SHAs"
    if rhcos_text is None:
        return CheckResult(name=name, status="skip", detail="no rhcos advisory configured for this assembly")
    if not payload_shas:
        return CheckResult(name=name, status="skip", detail="no image payload SHAs to compare against")
    rhcos_shas = extract_payload_shas(rhcos_text)
    if not rhcos_shas:
        return CheckResult(name=name, status="fail", detail="no per-arch SHA values found in rhcos advisory text")
    all_arches = sorted(set(payload_shas) | set(rhcos_shas))
    mismatches = []
    for arch in all_arches:
        image_digest = payload_shas.get(arch)
        rhcos_digest = rhcos_shas.get(arch)
        if image_digest != rhcos_digest:
            mismatches.append(f"{arch}: image={image_digest}, rhcos={rhcos_digest}")
    if mismatches:
        return CheckResult(name=name, status="fail", detail="; ".join(mismatches))
    return CheckResult(
        name=name,
        status="pass",
        detail=f"image and rhcos advisory SHAs match for all {len(payload_shas)} arches",
    )


def load_release_notes(runtime: Runtime, config_path: str) -> ReleaseNotes | None:
    """
    Load a shipment config's releaseNotes from a local file path.

    Arg(s):
        runtime (Runtime): the elliott Runtime, already initialized with shipment data access.
        config_path (str): path to the shipment yaml file.
    Return Value(s):
        ReleaseNotes | None: the parsed release notes, or None if this shipment has none (e.g. FBC).
    """
    config_raw = runtime.shipment_gitdata.load_yaml_file(config_path)
    config = ShipmentConfig.model_validate(config_raw)
    if config.shipment.data is None:
        return None
    return config.shipment.data.releaseNotes


def get_advisory_id(runtime: Runtime, kind: str) -> int | None:
    """
    Look up a classic Errata Tool advisory id for this assembly.

    Arg(s):
        runtime (Runtime): the elliott Runtime, already initialized.
        kind (str): "rpm" or "rhcos", matching ocp-build-data releases.yml's
            assembly.group.advisories keys.
    Return Value(s):
        int | None: the advisory id, or None if not configured for this assembly.
    """
    releases_config = runtime.get_releases_config()
    group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    return group_config.get("advisories", {}).get(kind)


def _normalize_errata_name(errata_name: str) -> str:
    """
    Strip the trailing revision suffix (e.g. "-02") errata_tool sometimes
    appends to an advisory's full display name.

    `errata_tool.Erratum.errata_name` (the ET API's `fulladvisory` field)
    can read like "RHBA-2026:48691-02" for a revised advisory, but the
    public access.redhat.com URL for that advisory never includes the
    revision suffix (just "RHBA-2026:48691") — see the errata_tool
    library's own comment on this in `Erratum._syncDetails`.

    Arg(s):
        errata_name (str): an advisory's full display name, possibly with a revision suffix.
    Return Value(s):
        str: the display name with any trailing "-<digits>" revision suffix removed.
    """
    return re.sub(r"-\d+$", "", errata_name)


def _is_dropped(erratum: Erratum) -> bool:
    """
    Return True if the advisory has been dropped and will not ship.

    Arg(s):
        erratum (Erratum): a fetched errata_tool.Erratum instance.
    Return Value(s):
        bool: True if the advisory state is DROPPED_NO_SHIP.
    """
    return erratum.errata_state == "DROPPED_NO_SHIP"


def _build_advisory_text(erratum: Erratum) -> str:
    """
    Safely combine an errata_tool Erratum's description and solution text.

    Both fields default to None on Erratum until populated by a fetch, so
    this treats a missing field as an empty string rather than embedding
    the literal string "None" into the text used for substring searches.

    Arg(s):
        erratum (Erratum): a fetched errata_tool.Erratum instance.
    Return Value(s):
        str: the erratum's description and solution, newline-separated.
    """
    description = erratum.description or ""
    solution = erratum.solution or ""
    return f"{description}\n{solution}"


async def run_checks(
    runtime: Runtime, image_config_path: str | None, extras_config_path: str | None
) -> list[CheckResult]:
    """
    Run all six docs-approval checks for one shipment MR.

    Arg(s):
        runtime (Runtime): the elliott Runtime, already initialized with shipment data access.
        image_config_path (str | None): path to the changed image shipment file, if any.
        extras_config_path (str | None): path to the changed extras shipment file, if any.
    Return Value(s):
        list[CheckResult]: one result per check, in a fixed order.
    """
    image_release_notes = load_release_notes(runtime, image_config_path) if image_config_path else None
    extras_release_notes = load_release_notes(runtime, extras_config_path) if extras_config_path else None

    rpm_advisory_id = get_advisory_id(runtime, "rpm")
    rpm_erratum = Erratum(errata_id=rpm_advisory_id) if rpm_advisory_id else None
    rpm_dropped_name = None
    if rpm_erratum and _is_dropped(rpm_erratum):
        rpm_dropped_name = _normalize_errata_name(rpm_erratum.errata_name)
        rpm_erratum = None
    rpm_errata_name = _normalize_errata_name(rpm_erratum.errata_name) if rpm_erratum else None
    rpm_description = rpm_erratum.description if rpm_erratum else None

    # Derive the year-qualified image advisory display name from any text that
    # references it (rpm description, extras description). The live_id in the
    # shipment YAML is not the errata_tool internal ID, so we cannot use
    # Erratum(errata_id=live_id) to look it up directly.
    image_errata_name = None
    if image_release_notes and image_release_notes.live_id:
        for candidate in filter(
            None,
            [
                rpm_description,
                extras_release_notes.description if extras_release_notes else None,
            ],
        ):
            image_errata_name = extract_advisory_name(candidate, image_release_notes.type, image_release_notes.live_id)
            if image_errata_name:
                break

    rhcos_advisory_id = get_advisory_id(runtime, "rhcos")
    rhcos_erratum = Erratum(errata_id=rhcos_advisory_id) if rhcos_advisory_id else None
    rhcos_dropped_name = None
    if rhcos_erratum and _is_dropped(rhcos_erratum):
        rhcos_dropped_name = _normalize_errata_name(rhcos_erratum.errata_name)
        rhcos_erratum = None
    rhcos_text = _build_advisory_text(rhcos_erratum) if rhcos_erratum else None

    sha_result, payload_shas = await check_payload_shas(image_release_notes, runtime.assembly)

    def _dropped(name: str, dropped_name: str) -> CheckResult:
        return CheckResult(name=name, status="skip", detail=f"advisory {dropped_name} is DROPPED_NO_SHIP")

    return [
        check_extras_references_image(extras_release_notes, image_release_notes, image_errata_name),
        check_image_does_not_reference_dropped_rpm(image_release_notes, rpm_dropped_name)
        if rpm_dropped_name
        else check_image_references_rpm(image_release_notes, rpm_errata_name),
        _dropped("rpm references image", rpm_dropped_name)
        if rpm_dropped_name
        else check_rpm_references_image(rpm_description, image_release_notes, image_errata_name),
        sha_result,
        _dropped("rhcos references rpm", rhcos_dropped_name)
        if rhcos_dropped_name
        else (
            check_rhcos_does_not_reference_dropped_rpm(rhcos_text, rpm_dropped_name)
            if rpm_dropped_name
            else check_rhcos_references_rpm(rhcos_text, rpm_errata_name)
        ),
        _dropped("rhcos payload SHAs", rhcos_dropped_name)
        if rhcos_dropped_name
        else check_rhcos_payload_shas(rhcos_text, payload_shas),
    ]


def format_report(results: list[CheckResult]) -> str:
    """
    Format check results into a human-readable report.

    Arg(s):
        results (list[CheckResult]): the checks to report.
    Return Value(s):
        str: a multi-line report, one line per check.
    """
    lines = ["Docs approval check report:"]
    for r in results:
        lines.append(f"  [{r.status.upper()}] {r.name}: {r.detail}")
    return "\n".join(lines)


@cli.command("verify-docs-approval", short_help="Verify shipment MR advisory references and payload SHAs")
@click.option(
    "--image-config",
    "image_config_path",
    metavar="PATH",
    default=None,
    help="Path to the image shipment config for this release, if changed",
)
@click.option(
    "--extras-config",
    "extras_config_path",
    metavar="PATH",
    default=None,
    help="Path to the extras shipment config for this release, if changed",
)
@click.pass_obj
@click_coroutine
async def verify_docs_approval(runtime, image_config_path, extras_config_path):
    """
    Verify advisory cross-references (extras->image, image->rpm, rpm->image,
    rhcos->rpm) and per-arch release payload SHA values are correct
    for a shipment MR, automating the manual Docs approval review.

    \b
    Checks performed:
      1. extras (shipment MR) references image advisory
      2. image (shipment MR) references rpm advisory
      3. rpm advisory references image advisory
      4. image shipment payload SHAs match the published release payload
      5. rhcos advisory references rpm advisory
      6. rhcos advisory payload SHAs match image shipment SHAs

    \b
        $ elliott -g openshift-4.20 --assembly=4.20.32 --shipment-path=. \\
            verify-docs-approval --image-config=shipment/.../4.20.32.image.yaml \\
            --extras-config=shipment/.../4.20.32.extras.yaml
    """
    runtime.initialize(build_system="konflux", with_shipment=True)
    results = await run_checks(runtime, image_config_path, extras_config_path)
    click.echo(format_report(results))
    if any(r.status == "fail" for r in results):
        raise click.ClickException("One or more docs-approval checks failed")
    if not any(r.status == "pass" for r in results):
        raise click.ClickException(
            "All docs-approval checks were skipped; ensure --image-config and --extras-config are provided"
        )
