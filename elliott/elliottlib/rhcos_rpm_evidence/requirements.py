"""Parse RHCOS verification comments and compare required RPMs with selected images."""

import re
from collections.abc import Iterable
from dataclasses import dataclass
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
_RESULT_RE = re.compile(r"\bResult\s*:\s*(PASS|FAIL|UNKNOWN|NOT_FIXED)\b", re.IGNORECASE)


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

    comment, text = max(relevant, key=lambda item: item[0].created)
    result_matches = list(_RESULT_RE.finditer(text))
    if not result_matches:
        return ParsedVerification(
            result=VerificationResult.UNKNOWN,
            comment_created=comment.created,
            requirements=(),
            reason="Latest RHCOS verification comment has no supported Result field",
        )

    raw_result = result_matches[-1].group(1).upper()
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

        for record in package_records:
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
            if not _rhel_branches_compatible(installed.rhel_branch, required.rhel_branch):
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
                    arch=installed.arch,
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
            image for image in images if any(_rhel_branches_compatible(rpm.rhel_branch, branch) for rpm in image.rpms)
        ]
        if required.arch and required.arch != "noarch":
            branch_images = [image for image in branch_images if image.arch == required.arch]
        installed: list[InstalledRpm] = []
        older_rpms = []
        for image in branch_images:
            matches = [
                rpm
                for rpm in image.rpms
                if _rhel_branches_compatible(rpm.rhel_branch, branch)
                and _package_matches(required.name, rpm.name)
                and (not required.arch or required.arch == rpm.arch)
            ]
            if not matches:
                continue

            installed.extend(matches)
            for rpm in matches:
                if compare_nvr(_as_nvr(rpm), _as_nvr(required), ignore_name=True) < 0:
                    older_rpms.append(rpm)

        if not installed:
            checks.append(
                RpmRequirementCheck(required, (), False, f"Required RPM is absent from selected RHCOS branch {branch}")
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

    return tuple(checks)


def _component_rhel_major(component: str) -> int | None:
    """
    Return a component's explicit RHEL major version, when it has one.
    """
    match = re.fullmatch(r"openshift/ose-rhel-coreos-(\d+)", component)
    return int(match.group(1)) if match else None


def _rhel_branches_compatible(installed_branch: str | None, required_branch: str | None) -> bool:
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
