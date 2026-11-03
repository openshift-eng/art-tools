"""
Validate RHCOS CVE verifier comments against the selected images' installed RPMs.

Only explicit passing verification with parseable required RPM versions provides
automatic inclusion evidence. Unknown comment formats remain review candidates.
"""

import re
from typing import Any

from artcommonlib.rpm_utils import compare_nvr, parse_nvr


def _comment_text(body: Any) -> str:
    """
    Extract text from both Jira plain text and Atlassian document format comments.
    """
    if isinstance(body, str):
        return body
    if isinstance(body, dict):
        return " ".join([body.get("text", ""), *[_comment_text(item) for item in body.get("content", [])]])
    return ""


def verified_rpm_requirements(comments: list[Any], cve: str) -> list[dict[str, str]]:
    """
    Read required NEVRAs from the latest RHCOS verification comment for this CVE.

    Automated verification and explicit manual corrections both record a passing
    result and required RPM version. Do not fall back to an older passing comment
    when the latest verification fails or has an unsupported format.
    """
    for comment in sorted(comments, key=lambda item: item.created, reverse=True):
        text = _comment_text(comment.body)
        if cve not in text or "RHCOS build" not in text:
            continue
        if not re.search(r"Result:\s*PASS\b", text, re.IGNORECASE) or re.search(
            r"Result:\s*(?:FAIL|UNKNOWN|NOT_FIXED)\b", text, re.IGNORECASE
        ):
            return []
        requirements = []
        for value in re.findall(r"\brequired:?\s+([^\s,;)]+)", text, re.IGNORECASE):
            value = value.rstrip(".")
            arch_match = re.search(r"\.(x86_64|aarch64|s390x|ppc64le|noarch)$", value)
            arch = arch_match.group(1) if arch_match else ""
            nvr = value[: arch_match.start()] if arch_match else value
            try:
                rpm = parse_nvr(nvr)
            except ValueError:
                return []
            if not re.fullmatch(r"\d+", rpm.get("epoch") or "0"):
                return []
            if not re.search(r"\.el\d+(?:_\d+)?", rpm["release"]):
                return []
            requirements.append({**rpm, "arch": arch})
        return requirements
    return []


def requirements_installed(requirements: list[dict[str, str]], inventories: dict[str, list[list]], el_ver: int) -> bool:
    """
    Check every required package against installed RPM EVRs on the same RHEL branch.

    Architecture-specific requirements apply to the corresponding selected image.
    Packages absent from an architecture, such as Intel microcode on ARM, do not
    apply there; every requirement must still match at least one installed RPM.
    """
    if not requirements:
        return False
    for required in requirements:
        branch = re.search(r"\.el\d+(?:_\d+)?", required["release"]).group()
        if not re.match(rf"\.el{el_ver}(?:_|$)", branch):
            return False
        found = False
        for image_arch, entries in inventories.items():
            if required["arch"] and required["arch"] not in {image_arch, "noarch"}:
                continue
            for name, epoch, version, release, arch, _ in entries:
                if name != required["name"] and not (required["name"] == "kernel" and name == "kernel-core"):
                    continue
                installed_branch = re.search(r"\.el\d+(?:_\d+)?", release)
                if not installed_branch or installed_branch.group() != branch:
                    continue
                if required["arch"] and required["arch"] != arch:
                    continue
                found = True
                installed = {
                    "name": required["name"],
                    "epoch": str(epoch or "0"),
                    "version": version,
                    "release": release,
                }
                if compare_nvr(installed, required) < 0:
                    return False
        if not found:
            return False
    return True
