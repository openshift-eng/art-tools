"""Render and persist RHCOS RPM evidence reports."""

import json
from collections import Counter
from pathlib import Path

from elliottlib.rhcos_rpm_evidence.models import AnalysisReport


def write_report(directory: Path, report: AnalysisReport) -> tuple[Path, Path]:
    """
    Write structured evidence and a concise review summary to the Elliott working directory.
    """
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / "rhcos-rpm-evidence.json"
    text_path = directory / "rhcos-rpm-evidence.log"
    json_path.write_text(json.dumps(report.to_dict(), indent=2) + "\n")
    text_path.write_text(render_report(report))
    return json_path, text_path


def render_report(report: AnalysisReport) -> str:
    """
    Render tracker outcomes, package evidence, and evidence collection gaps.
    """
    lines: list[str] = [
        f"RHCOS RPM evidence for assembly {report.assembly}",
        f"Target releases: {', '.join(report.target_releases) or 'unavailable'}",
        f"Evidence collection: {'complete' if report.complete else 'incomplete'}",
        f"Architecture scope: {', '.join(image.arch for image in report.selected_images) or 'unavailable'}",
        f"Basis RPM inventory: {len(report.basis_images)} architecture(s)",
        f"RPM inventory changes: {len(report.rpm_changes)}",
        f"Tracker RPM CVE matches: {sum(len(finding.rpm_evidence) for finding in report.findings)}",
        "",
        "Selected images:",
    ]
    if report.selected_images:
        lines.extend(f"- {image.arch}: {image.build_id} ({len(image.rpms)} RPMs)" for image in report.selected_images)
    else:
        lines.append("- None")

    lines.extend(["", "Basis images:"])
    if report.basis_images:
        lines.extend(f"- {image.arch}: {image.build_id} ({len(image.rpms)} RPMs)" for image in report.basis_images)
    else:
        lines.append("- Unavailable")

    dispositions = Counter(finding.disposition.value for finding in report.findings)
    lines.extend(["", "Outcome counts:"])
    if dispositions:
        lines.extend(f"- {disposition}: {count}" for disposition, count in sorted(dispositions.items()))
    else:
        lines.append("- None")

    lines.extend(["", "Tracker findings:"])
    if report.findings:
        for finding in report.findings:
            lines.append(
                f"- {finding.issue_key} ({finding.cve_id}, {finding.status}): "
                f"{finding.disposition.value} — {finding.reason}"
            )
            for evidence in finding.rpm_evidence:
                basis_nvr = evidence.basis_nvr or "absent from basis"
                advisory_ids = ", ".join(evidence.advisory_ids) or "none"
                lines.append(
                    f"  - {evidence.rpm_type.name.lower()} / {evidence.rpm_name} "
                    f"({evidence.image_arch}/{evidence.stream}): "
                    f"{basis_nvr} -> {evidence.selected_nvr}; changelog match: {evidence.changelog_match}; "
                    f"new changelog match: {evidence.new_changelog_match}; advisories: {advisory_ids}"
                )
    else:
        lines.append("- None")

    if report.warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in report.warnings)

    lines.extend(["", "Full evidence: rhcos-rpm-evidence.json", ""])
    return "\n".join(lines)
