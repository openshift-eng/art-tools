"""
Generate ART-managed RPM overrides for a targeted RHCOS update.

Compare source builds installed in the basis and selected RHCOS images. Preserve
external RHEL RPMs as RHCOS content and emit member pins only for changed ART RPMs.
"""

from typing import Any

import koji
from artcommonlib.assembly import assembly_rhcos_config
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rpm_utils import parse_nvr

from doozerlib.rhcos import RHCOSBuildInspector
from doozerlib.runtime import Runtime


def _source_builds(
    entries: list[list], session: Any, build_cache: dict[int, dict], managed_names: set[str], logger: Any
) -> list[dict]:
    """
    Resolve installed binary RPMs to Brew source builds, including subpackages.
    """
    nvras = {f"{name}-{version}-{release}.{arch}": name for name, _, version, release, arch, _ in entries}
    with session.multicall(strict=False) as calls:
        rpm_tasks = {nvra: calls.getRPM(nvra) for nvra in nvras}
    build_ids = set()
    unresolved = []
    for nvra, task in rpm_tasks.items():
        try:
            rpm = task.result
        except koji.GenericError as exc:
            if "No such rpm" not in str(exc):
                raise
            rpm = None
        if rpm is None:
            if any(nvras[nvra] == name or nvras[nvra].startswith(f"{name}-") for name in managed_names):
                raise ValueError(f"Cannot resolve installed ART RPM {nvra} to a source build")
            unresolved.append(nvra)
            continue
        build_ids.add(rpm["build_id"])
    if unresolved:
        logger.warning(
            "Unable to resolve RHCOS RPMs in Brew for ART source-build comparison: %s", ", ".join(sorted(unresolved))
        )
    with session.multicall(strict=True) as calls:
        build_tasks = {build_id: calls.getBuild(build_id) for build_id in build_ids if build_id not in build_cache}
    for build_id, task in build_tasks.items():
        build = task.result
        if not build:
            raise ValueError(f"Cannot resolve source build {build_id} for an installed RHCOS RPM")
        build_cache[build_id] = build
    return [build_cache[build_id] for build_id in sorted(build_ids)]


def _managed_builds(runtime: Runtime, config: dict, metas: dict[str, Any], label: str) -> dict[tuple[str, int], str]:
    """
    Collect one installed source build per ART component and RHEL target across images.
    """
    installed: dict[tuple[str, int], str] = {}
    provenance: dict[tuple[str, int], str] = {}
    build_cache: dict[int, dict] = {}
    with runtime.pooled_koji_client_session() as session:
        for arch in runtime.group_config.arches:
            images = {
                name: container["images"][arch]
                for name, container in config.items()
                if isinstance(container, dict) and container.get("images", {}).get(arch)
            }
            if not images:
                raise ValueError(f"No explicit RHCOS image pins for {label}/{arch}; cannot compare ART RPMs")
            inspector = RHCOSBuildInspector(runtime, images, arch, registry_config=runtime.registry_config)
            for build in _source_builds(
                inspector.get_os_metadata_rpm_list(), session, build_cache, set(metas), runtime.logger
            ):
                package_name = build.get("package_name") or build["name"]
                meta = metas.get(package_name)
                if meta is None:
                    continue
                nvr = build["nvr"]
                if build.get("state") != koji.BUILD_STATES["COMPLETE"]:
                    raise ValueError(f"Installed ART source build {nvr} is not complete in Brew")
                el_ver = isolate_el_version_in_release(parse_nvr(nvr)["release"])
                if el_ver not in meta.determine_rhel_targets():
                    raise ValueError(f"Installed ART build {nvr} has no matching RHEL target in {meta.distgit_key}")
                key = (meta.distgit_key, el_ver)
                if key in installed and installed[key] != nvr:
                    raise ValueError(
                        f"Conflicting ART RPM builds in {label} RHCOS for {meta.distgit_key}/el{el_ver}: "
                        f"{installed[key]} ({provenance[key]}) and {nvr} ({arch}). "
                        "Select consistent RHCOS images before generating the assembly."
                    )
                installed[key] = nvr
                provenance[key] = arch
    return installed


def find_rhcos_rpm_pins(runtime: Runtime, rhcos_config: dict, basis_assembly: str) -> list[dict]:
    """
    Propose member overrides for changed ART source builds installed in selected RHCOS.

    Generated pins use the distgit key, source build NVR and correct RHEL target.
    Images and external RHEL RPMs remain outside the member RPM overrides.
    """
    metas = {}
    for meta in runtime.rpm_metas():
        if meta.rpm_name in metas:
            raise ValueError(f"Multiple ART RPM definitions for source package {meta.rpm_name}")
        metas[meta.rpm_name] = meta
    if not metas:
        return []
    basis_config = assembly_rhcos_config(runtime.get_releases_config(), basis_assembly).primitive()
    before = _managed_builds(runtime, basis_config, metas, basis_assembly)
    selected = _managed_builds(runtime, rhcos_config, metas, "selected")
    pins: dict[str, dict] = {}
    for (distgit_key, el_ver), nvr in sorted(selected.items()):
        if before.get((distgit_key, el_ver)) == nvr:
            continue
        pin = pins.setdefault(
            distgit_key,
            {
                "distgit_key": distgit_key,
                "why": "Match ART-managed RPM builds installed in the selected RHCOS images",
                "metadata": {"is": {}},
            },
        )
        pin["metadata"]["is"][f"el{el_ver}"] = nvr
        runtime.logger.info(
            "Pinning ART RPM %s/el%s to %s (basis RHCOS: %s)",
            distgit_key,
            el_ver,
            nvr,
            before.get((distgit_key, el_ver), "not installed"),
        )
    return list(pins.values())
