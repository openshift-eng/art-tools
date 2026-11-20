"""Parse RHCOS verification comments and compare required RPMs with selected images."""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from artcommonlib.rpm_utils import compare_nvr, parse_nvr

from elliottlib.rhcos_rpm_evidence.models import (
    InstalledRpm,
    RHCOSImageInventory,
    RpmNevra,
    RpmRequirementCheck,
    VerificationComment,
    VerificationResult,
)

_ARCH_RE = re.compile(r"\.(x86_64|aarch64|s390x|ppc64le|noarch)$")
_RHEL_BRANCH_RE = re.compile(r"\.el(?P<major>\d+)(?:(?:_(?P<minor>\d+))|(?P<variant>[a-z]+))?(?=$|[._])")
_REQUIRED_RPM_RE = re.compile(r"\brequired(?:\s+rpms?)?\s*:?\s+([^\s,;]+(?:\s*,\s*[^\s,;]+)*)", re.IGNORECASE)
_PACKAGE_REQUIREMENT_RE = re.compile(
    r"\bPackage:\s*(?P<package>.+?)\.\s*Installed:\s*(?P<installed>\S+)\.\s*Required:\s*(?P<required>\S+)",
    re.IGNORECASE,
)
_RESULT_RE = re.compile(r"\bResult\s*:\s*([A-Za-z_]+)", re.IGNORECASE)


@dataclass(frozen=True)
class ParsedVerification:
    """
    The newest supported verification evidence found for a CVE.
    """

    result: VerificationResult
    comment_created: str
    requirements: tuple[RpmNevra, ...]
    reason: str


def parse_required_rpm(value: str) -> RpmNevra | None:
    """
    Parse one required RPM NEVRA and require a recognizable RHEL branch.
    """
    value = value.rstrip(".,;:)")
    arch_match = _ARCH_RE.search(value)
    arch = arch_match.group(1) if arch_match else None
    nvr = value[: arch_match.start()] if arch_match else value

    try:
        parsed = parse_nvr(nvr)
    except ValueError:
        return None

    branch_match = _RHEL_BRANCH_RE.search(parsed["release"])
    if not branch_match:
        return None
    if not arch and re.search(r"\.[a-z][a-z0-9_]*$", parsed["release"][branch_match.end() :], re.IGNORECASE):
        return None

    epoch = str(parsed.get("epoch") or "0")
    if not epoch.isdigit():
        return None

    return RpmNevra(
        name=parsed["name"],
        epoch=epoch,
        version=parsed["version"],
        release=parsed["release"],
        arch=arch,
    )


def comment_text(body: Any) -> str:
    """
    Extract plain text from Jira strings and Atlassian document-format bodies.
    """
    if isinstance(body, str):
        return body
    if isinstance(body, list):
        return " ".join(comment_text(item) for item in body)
    if isinstance(body, dict):
        own_text = body.get("text", "")
        children = body.get("content") or []
        return " ".join([str(own_text), *(comment_text(item) for item in children)]).strip()
    return ""


def parse_latest_verification(comments: Iterable[VerificationComment], cve_id: str) -> ParsedVerification | None:
    """
    Parse the latest RHCOS verification comment for a CVE without falling back to older results.
    """
    cve_pattern = re.compile(rf"(?<![A-Z0-9-]){re.escape(cve_id)}(?![A-Z0-9-])", re.IGNORECASE)
    relevant = []
    for comment in comments:
        text = comment_text(comment.body)
        if cve_pattern.search(text) and re.search(r"\bRHCOS builds?\b", text, re.IGNORECASE):
            relevant.append((comment, text))
    if not relevant:
        return None

    try:

        def _timestamp(item: tuple[VerificationComment, str]) -> datetime:
            """
            Compare Jira comment timestamps as UTC instants.
            """
            value = datetime.fromisoformat(item[0].created.replace("Z", "+00:00"))
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

        comment, text = max(relevant, key=_timestamp)
    except (ValueError, TypeError):
        return ParsedVerification(
            VerificationResult.UNSUPPORTED, "", (), "Unable to determine the latest verification comment timestamp"
        )
    result_matches = list(_RESULT_RE.finditer(text))
    if not result_matches:
        return ParsedVerification(
            result=VerificationResult.UNKNOWN,
            comment_created=comment.created,
            requirements=(),
            reason="Latest RHCOS verification comment has no supported Result field",
        )

    raw_results = [match.group(1).upper() for match in result_matches]
    if any(value not in {"PASS", "FAIL", "UNKNOWN", "NOT_FIXED"} for value in raw_results):
        return ParsedVerification(
            VerificationResult.UNSUPPORTED,
            comment.created,
            (),
            "Latest RHCOS verification contains an unsupported Result field",
        )
    # A passing summary cannot override a failing or unknown package result.
    raw_result = next((value for value in raw_results if value in {"FAIL", "NOT_FIXED"}), None)
    if raw_result is None:
        raw_result = "UNKNOWN" if "UNKNOWN" in raw_results else raw_results[-1]
    result = {
        "PASS": VerificationResult.PASS,
        "FAIL": VerificationResult.FAIL,
        "UNKNOWN": VerificationResult.UNKNOWN,
        "NOT_FIXED": VerificationResult.FAIL,
    }[raw_result]
    requirements = []
    package_records = list(_PACKAGE_REQUIREMENT_RE.finditer(text))
    package_markers = len(re.findall(r"\bPackage\s*:", text, re.IGNORECASE))
    if package_markers:
        if len(package_records) != package_markers:
            return ParsedVerification(
                result=VerificationResult.UNSUPPORTED,
                comment_created=comment.created,
                requirements=(),
                reason="Latest RHCOS verification contains an incomplete package requirement record",
            )

        for index, record in enumerate(package_records):
            end = package_records[index + 1].start() if index + 1 < len(package_records) else len(text)
            if not _RESULT_RE.search(text[record.end() : end]):
                return ParsedVerification(
                    VerificationResult.UNSUPPORTED,
                    comment.created,
                    (),
                    "Latest RHCOS verification contains a package without a Result field",
                )
            package_name = record.group("package").strip()
            installed = parse_required_rpm(record.group("installed"))
            required = parse_required_rpm(record.group("required"))
            if installed is None or required is None or installed.name != package_name:
                return ParsedVerification(
                    result=VerificationResult.UNSUPPORTED,
                    comment_created=comment.created,
                    requirements=(),
                    reason="Latest RHCOS verification contains an unsupported package record",
                )
            if required.arch and installed.arch and required.arch != installed.arch:
                return ParsedVerification(
                    VerificationResult.UNSUPPORTED,
                    comment.created,
                    (),
                    "Installed and required RPMs have different architectures",
                )
            if not rhel_branches_compatible(installed.rhel_branch, required.rhel_branch):
                return ParsedVerification(
                    result=VerificationResult.UNSUPPORTED,
                    comment_created=comment.created,
                    requirements=(),
                    reason="Installed and required RPMs have different RHEL major versions",
                )

            # The verifier's Required field can name a source RPM while Package and
            # Installed identify the binary RPM that must be present in the image.
            requirements.append(
                RpmNevra(
                    name=installed.name,
                    epoch=required.epoch,
                    version=required.version,
                    release=required.release,
                    arch=required.arch or installed.arch,
                )
            )
    else:
        for match in _REQUIRED_RPM_RE.finditer(text):
            for value in re.split(r"\s*,\s*", match.group(1)):
                requirement = parse_required_rpm(value)
                if requirement is None:
                    return ParsedVerification(
                        result=VerificationResult.UNSUPPORTED,
                        comment_created=comment.created,
                        requirements=(),
                        reason="Latest RHCOS verification contains an unsupported required RPM",
                    )
                requirements.append(requirement)

    if result is VerificationResult.PASS and not requirements:
        return ParsedVerification(
            result=VerificationResult.UNSUPPORTED,
            comment_created=comment.created,
            requirements=(),
            reason="Latest passing RHCOS verification contains no parseable required RPMs",
        )

    requirements = list(dict.fromkeys(requirements))

    return ParsedVerification(
        result=result,
        comment_created=comment.created,
        requirements=tuple(requirements),
        reason=(
            "Latest RHCOS verification passed"
            if result is VerificationResult.PASS
            else f"Latest RHCOS verification returned Result: {raw_result}"
        ),
    )


def check_required_rpms(
    requirements: Iterable[RpmNevra],
    inventories: Iterable[RHCOSImageInventory],
    component: str,
) -> tuple[RpmRequirementCheck, ...]:
    """
    Check every required RPM across the selected RHCOS images for its RHEL branch.
    """
    images = tuple(inventories)
    requirements = tuple(requirements)
    component_branch = _component_rhel_major(component)
    checks = []

    for required in requirements:
        branch = required.rhel_branch
        if branch is None:
            checks.append(RpmRequirementCheck(required, (), False, "Required RPM has no RHEL branch"))
            continue

        branch_major = int(re.search(r"\d+", branch).group())
        if component_branch is not None and branch_major != component_branch:
            checks.append(
                RpmRequirementCheck(
                    required,
                    (),
                    False,
                    f"Required RPM branch {branch} does not match tracker component RHEL {component_branch}",
                )
            )
            continue

        branch_images = [
            image for image in images if any(rhel_branches_compatible(rpm.rhel_branch, branch) for rpm in image.rpms)
        ]
        if required.arch and required.arch != "noarch":
            branch_images = [image for image in branch_images if image.arch == required.arch]
        installed: list[InstalledRpm] = []
        older_rpms = []
        missing_arches = []
        for image in branch_images:
            matches = [
                rpm
                for rpm in image.rpms
                if rhel_branches_compatible(rpm.rhel_branch, branch)
                and _package_matches(required.name, rpm.name)
                and (not required.arch or required.arch == rpm.arch)
            ]
            if not matches:
                missing_arches.append(image.arch)
                continue

            installed.extend(matches)
            for rpm in matches:
                if compare_nvr(_as_nvr(rpm), _as_nvr(required), ignore_name=True) < 0:
                    older_rpms.append(rpm)

        if not installed:
            checks.append(
                RpmRequirementCheck(required, (), False, f"Required RPM is absent from selected RHCOS branch {branch}")
            )
        elif missing_arches:
            checks.append(
                RpmRequirementCheck(
                    required,
                    tuple(installed),
                    False,
                    f"Required RPM is absent from selected RHCOS architectures: {', '.join(missing_arches)}",
                )
            )
        elif older_rpms:
            older = ", ".join(f"{rpm.name}-{rpm.version}-{rpm.release}.{rpm.arch}" for rpm in older_rpms)
            checks.append(
                RpmRequirementCheck(
                    required,
                    tuple(installed),
                    False,
                    f"Installed RPM is below required EVR: {older}",
                )
            )
        else:
            checks.append(RpmRequirementCheck(required, tuple(installed), True, "Required RPM EVR is installed"))

    for required in requirements:
        if not required.arch or required.arch == "noarch" or not required.rhel_branch:
            continue
        applicable_arches = {
            image.arch
            for image in images
            if any(rhel_branches_compatible(rpm.rhel_branch, required.rhel_branch) for rpm in image.rpms)
        }
        covered_arches = {
            other.arch
            for other in requirements
            if (_package_matches(other.name, required.name) or _package_matches(required.name, other.name))
            and rhel_branches_compatible(other.rhel_branch, required.rhel_branch)
            and other.arch not in {None, "noarch"}
        }
        if any(
            (_package_matches(other.name, required.name) or _package_matches(required.name, other.name))
            and other.arch in {None, "noarch"}
            and rhel_branches_compatible(other.rhel_branch, required.rhel_branch)
            for other in requirements
        ):
            continue
        missing = sorted(applicable_arches - covered_arches)
        if missing:
            checks.append(
                RpmRequirementCheck(
                    required, (), False, f"Verification has no RPM requirement for architectures: {', '.join(missing)}"
                )
            )

    return tuple(checks)


def _component_rhel_major(component: str) -> int | None:
    """
    Return a component's explicit RHEL major version, when it has one.
    """
    match = re.fullmatch(r"openshift/ose-rhel-coreos-(\d+)", component)
    return int(match.group(1)) if match else None


def rhel_branches_compatible(installed_branch: str | None, required_branch: str | None) -> bool:
    """
    Match exact branches or a same-major variant branch against a minor branch.
    """
    if installed_branch is None or required_branch is None:
        return False
    installed_major = _RHEL_BRANCH_RE.search(installed_branch)
    required_major = _RHEL_BRANCH_RE.search(required_branch)
    if installed_branch == required_branch:
        return True
    if not installed_major or not required_major or installed_major.group("major") != required_major.group("major"):
        return False
    return bool(
        (installed_major.group("variant") and required_major.group("minor"))
        or (required_major.group("variant") and installed_major.group("minor"))
    )


def _package_matches(required_name: str, installed_name: str) -> bool:
    """
    Allow the RHCOS kernel tracker package name to match the installed kernel-core RPM.
    """
    return required_name == installed_name or (required_name == "kernel" and installed_name == "kernel-core")


def _as_nvr(rpm: RpmNevra) -> dict[str, str]:
    """
    Create the NVR mapping expected by RPM EVR comparison helpers.
    """
    return {"name": rpm.name, "epoch": rpm.epoch, "version": rpm.version, "release": rpm.release}
