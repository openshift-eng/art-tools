"""Compare RHCOS RPM inventories and match package CVE evidence to changes."""

from artcommonlib.rpm_utils import compare_nvr

from elliottlib.rhcos_rpm_evidence.models import (
    InstalledRpm,
    RHCOSImageInventory,
    RHCOSRpm,
    RpmBuildEvidence,
    RpmChange,
    RpmCveEvidence,
)


def compare_rpm_inventories(
    basis_images: tuple[RHCOSImageInventory, ...],
    selected_images: tuple[RHCOSImageInventory, ...],
    rpm_types: tuple[RHCOSRpm, ...],
) -> tuple[RpmChange, ...]:
    """
    Compare exact basis and selected RPMs within matching image and RHEL branches.
    """
    basis_by_arch = {image.arch: image for image in basis_images}
    selected_by_arch = {image.arch: image for image in selected_images}
    if set(basis_by_arch) != set(selected_by_arch):
        raise ValueError(
            f"Basis and selected RHCOS architectures differ: "
            f"basis={sorted(basis_by_arch)}, selected={sorted(selected_by_arch)}"
        )

    changes: list[RpmChange] = []
    for rpm_type in rpm_types:
        for image_arch in sorted(selected_by_arch):
            basis_index = _inventory_index(basis_by_arch[image_arch].rpms, rpm_type)
            selected_index = _inventory_index(selected_by_arch[image_arch].rpms, rpm_type)
            if rpm_type.required_per_architecture and not basis_index:
                raise ValueError(f"Basis RHCOS image for {image_arch} contains no {rpm_type.name.lower()} RPMs")
            if rpm_type.required_per_architecture and not selected_index:
                raise ValueError(f"Selected RHCOS image for {image_arch} contains no {rpm_type.name.lower()} RPMs")
            for key in sorted(basis_index.keys() | selected_index.keys()):
                before = basis_index.get(key)
                after = selected_index.get(key)
                if before == after:
                    continue
                if before is None:
                    change = "added"
                elif after is None:
                    change = "removed"
                else:
                    change = "upgraded" if compare_nvr(_nvr(after), _nvr(before)) > 0 else "downgraded"
                changes.append(
                    RpmChange(
                        image_arch=image_arch,
                        rpm_type=rpm_type,
                        stream=after.stream if after else before.stream,
                        change=change,
                        before=before,
                        after=after,
                    )
                )
    return tuple(changes)


def match_changed_rpm_cves(
    changes: tuple[RpmChange, ...],
    build_evidence: tuple[RpmBuildEvidence, ...],
    baseline_inventory_complete: bool,
) -> tuple[RpmCveEvidence, ...]:
    """
    Link CVEs on changed selected RPMs to exact package and baseline evidence.
    """
    evidence_by_nvr = {evidence.nvr: evidence for evidence in build_evidence}
    matches: list[RpmCveEvidence] = []
    for change in changes:
        if change.change not in {"added", "upgraded"} or change.after is None:
            continue

        selected_nvr = change.rpm_type.build_nvr(change.after)
        selected_evidence = evidence_by_nvr.get(selected_nvr)
        if selected_evidence is None:
            continue
        basis_nvr = change.rpm_type.build_nvr(change.before) if change.before else None
        basis_evidence = evidence_by_nvr.get(basis_nvr) if basis_nvr else None
        baseline_complete = baseline_inventory_complete and (
            change.before is None or bool(basis_evidence and basis_evidence.changelog_complete)
        )
        basis_cves = basis_evidence.all_cves if basis_evidence else set()
        for cve_id in sorted(selected_evidence.all_cves):
            changelog_match = cve_id in selected_evidence.changelog_cves
            observed_in_basis = cve_id in basis_cves
            new_changelog_match = (
                changelog_match
                and change.after.rhel_branch is not None
                and selected_evidence.changelog_complete
                and baseline_complete
                and not observed_in_basis
            )
            advisories = tuple(advisory for advisory in selected_evidence.advisories if cve_id in advisory.cves)
            matches.append(
                RpmCveEvidence(
                    cve_id=cve_id,
                    rpm_type=change.rpm_type,
                    rpm_name=change.after.name,
                    stream=change.stream,
                    image_arch=change.image_arch,
                    rhel_branch=change.after.rhel_branch,
                    selected_nvr=selected_nvr,
                    basis_nvr=basis_nvr,
                    changelog_match=changelog_match,
                    advisory_ids=tuple(sorted(advisory.advisory_id for advisory in advisories)),
                    advisory_urls=tuple(sorted(advisory.url for advisory in advisories)),
                    observed_in_basis=observed_in_basis,
                    new_changelog_match=new_changelog_match,
                    baseline_complete=baseline_complete,
                )
            )
    return tuple(matches)


def required_build_nvrs(changes: tuple[RpmChange, ...]) -> set[str]:
    """
    Return selected and basis build NVRs needed to assess positive RPM changes.
    """
    nvrs = set()
    for change in changes:
        if change.change not in {"added", "upgraded"} or change.after is None:
            continue
        nvrs.add(change.rpm_type.build_nvr(change.after))
        if change.before:
            nvrs.add(change.rpm_type.build_nvr(change.before))
    return nvrs


def _inventory_index(
    rpms: tuple[InstalledRpm, ...], rpm_type: RHCOSRpm
) -> dict[tuple[str, str, str, str | None], InstalledRpm]:
    """
    Index one image inventory while rejecting ambiguous duplicate package entries.
    """
    result = {}
    for rpm in rpms:
        if not rpm_type.matches_inventory_rpm(rpm):
            continue
        key = (rpm.stream, rpm.name, rpm.arch or "", rpm.rhel_branch)
        existing = result.get(key)
        if existing is not None and existing != rpm:
            raise ValueError(f"Multiple installed RPM versions for {key}; inventory comparison is ambiguous")
        result[key] = rpm
    return result


def _nvr(rpm: InstalledRpm) -> dict[str, str]:
    """
    Format an RPM's name, version, and release for RPM EVR comparison.
    """
    return {"name": rpm.name, "epoch": rpm.epoch, "version": rpm.version, "release": rpm.release}
