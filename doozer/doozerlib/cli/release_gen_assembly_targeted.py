"""
Doozer subcommand for generating targeted assembly definitions.

Given kernel NVR(s) and/or image NVR(s), finds matching RHCOS and DTK builds
and produces an assembly YAML definition pinning them. Output goes to stdout
so pyartcd can capture it.

Example usage (kernel fix):
    doozer --group openshift-4.17 --assembly stream --build-system konflux \
        release:gen-assembly --name 4.17.5 from-targeted \
        --basis-assembly 4.17.4 \
        --kernel-nvr kernel-5.14.0-284.28.1.el9_2 \
        --bug-id OCPBUGS-85292 \
        --cve-id CVE-2025-XXXX \
        --date 2026-Jun-15

Example usage (image pin for RC transition):
    doozer --group openshift-4.22 --assembly stream --build-system konflux \
        release:gen-assembly --name rc.5 from-targeted \
        --basis-assembly rc.4 \
        --image-nvr ose-installer-container-v4.22.0-202505200000.p0.el9
"""

import functools
import json
import logging
import os
import sys
import tempfile

import click
import koji
import requests
import semver
from artcommonlib.constants import BREW_HUB
from artcommonlib.exectools import cmd_assert
from artcommonlib.model import Missing
from artcommonlib.ocp_version_ancestry import calc_upgrade_sources_async
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rhcos import (
    RhcosMissingContainerException,
    get_container_configs,
    get_container_pullspec,
    get_latest_layered_rhcos_build,
)
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import get_inflight, new_roundtrip_yaml_handler, normalize_release_date
from pydantic import BaseModel, field_validator
from tenacity import retry, stop_after_attempt, wait_exponential

from doozerlib.cli import click_coroutine, pass_runtime
from doozerlib.cli.release_gen_assembly import releases_gen_assembly
from doozerlib.rhcos import RHCOSBuildFinder
from doozerlib.runtime import Runtime
from doozerlib.util import infer_assembly_type

logger = logging.getLogger(__name__)


class KernelInfo(BaseModel, frozen=True):
    """
    Parsed and validated kernel NVR with derived package names.
    """

    nvr: str
    el_ver: int
    kernel_core_nvr: str
    kernel_devel_nvr: str


class AssemblyParams(BaseModel, frozen=True):
    """
    Validated CLI inputs for targeted assembly generation.
    """

    assembly_name: str
    basis_assembly: str
    kernel_nvrs: tuple[str, ...] = ()
    bug_ids: tuple[str, ...] = ()
    cve_ids: tuple[str, ...] = ()
    image_nvrs: tuple[str, ...] = ()
    release_date: str | None = None

    @field_validator("kernel_nvrs")
    @classmethod
    def _validate_kernel_nvr_names(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        for nvr in v:
            parsed = parse_nvr(nvr)
            if not parsed["name"].startswith("kernel"):
                raise ValueError(
                    f"NVR {nvr} does not look like a kernel package. "
                    f"Expected name starting with 'kernel', got '{parsed['name']}'"
                )
        return v

    @field_validator("release_date")
    @classmethod
    def _validate_release_date(cls, v: str | None) -> str | None:
        if v is None:
            return None
        return normalize_release_date(v)


@releases_gen_assembly.command(
    "from-targeted", short_help="Outputs targeted assembly metadata with kernel and/or image pins"
)
@click.option(
    "--basis-assembly",
    metavar="ASSEMBLY",
    required=True,
    help="The basis assembly to inherit from (e.g. 4.17.4 or rc.4)",
)
@click.option(
    "--kernel-nvr",
    "kernel_nvrs",
    metavar="NVR",
    multiple=True,
    required=False,
    help="[MULTIPLE] Kernel NVR(s) to pin. Provide one per RHEL target if needed.",
)
@click.option(
    "--bug-id",
    "bug_ids",
    metavar="BUG_ID",
    multiple=True,
    required=False,
    help="[MULTIPLE] Jira issue IDs to include (e.g. OCPBUGS-85292). When provided, sets targeted_fixes_only.",
)
@click.option(
    "--cve-id",
    "cve_ids",
    metavar="CVE_ID",
    multiple=True,
    required=False,
    help="[MULTIPLE] CVE identifiers for the 'why' text (e.g. CVE-2026-43284).",
)
@click.option(
    "--image-nvr",
    "image_nvrs",
    metavar="NVR",
    multiple=True,
    required=False,
    help="[MULTIPLE] Additional image NVR(s) to pin (e.g. ose-installer-container-v4.18.0-202502110000.p0.el9).",
)
@click.option(
    "--date",
    metavar="YYYY-Mon-DD",
    default=None,
    help="Expected release date (e.g. 2026-Jun-15)",
)
@click.option(
    "--output-file",
    "-o",
    required=False,
    help="Specify a file path to write the generated assembly definition to",
)
@pass_runtime
@click_coroutine
@click.pass_context
async def gen_assembly_from_targeted(
    ctx,
    runtime: Runtime,
    basis_assembly: str,
    kernel_nvrs: tuple[str, ...],
    bug_ids: tuple[str, ...],
    cve_ids: tuple[str, ...],
    image_nvrs: tuple[str, ...],
    date: str | None,
    output_file: str | None,
):
    runtime.initialize(config_only=True)

    if runtime.group_config.canonical_builders_from_upstream and runtime.build_system == "brew":
        runtime.initialize(mode="both", clone_distgits=True, clone_source=False, prevent_cloning=False)
    else:
        runtime.initialize(mode="both", clone_distgits=False, clone_source=False, prevent_cloning=True)

    params = AssemblyParams(
        assembly_name=ctx.obj["ASSEMBLY_NAME"],
        basis_assembly=basis_assembly,
        kernel_nvrs=kernel_nvrs,
        bug_ids=bug_ids,
        cve_ids=cve_ids,
        image_nvrs=image_nvrs,
        release_date=date,
    )

    assembly_def = await GenAssemblyTargetedCli(runtime=runtime, params=params).run()

    yaml = new_roundtrip_yaml_handler()
    yaml.dump(assembly_def, sys.stdout)
    if output_file:
        with open(output_file, "w") as f:
            yaml.dump(assembly_def, f)


class GenAssemblyTargetedCli:
    """
    Generate a targeted assembly definition pinning specific kernel NVR(s)
    and/or image NVR(s), along with matching RHCOS and DTK builds.
    """

    def __init__(self, runtime: Runtime | None = None, params: AssemblyParams | None = None):
        self.runtime = runtime
        self.params = params
        self.logger = runtime.logger if runtime else logger

        self.kernel_info: list[KernelInfo] = []
        self.rhcos_config: dict = {}
        self.dtk_nvr: str = ""
        self.upgrades: str = ""

        self._ocp_major: int = 0
        self._ocp_minor: int = 0
        if runtime:
            self._ocp_major = runtime.group_config.vars.MAJOR
            self._ocp_minor = runtime.group_config.vars.MINOR

    async def run(self) -> dict:
        """
        Execute the targeted assembly generation.

        Return Value(s):
            dict: Assembly definition YAML structure
        """
        if self.params.kernel_nvrs:
            self._validate_and_parse_kernel_nvrs()
            await self._find_rhcos_build()
            await self._find_dtk_build()
        await self._compute_upgrades()
        return self._build_assembly_definition()

    def _validate_and_parse_kernel_nvrs(self) -> None:
        """
        Validate each kernel NVR exists in Brew and populate self.kernel_info.
        """
        koji_session = koji.ClientSession(BREW_HUB)

        for nvr in self.params.kernel_nvrs:
            parsed = parse_nvr(nvr)

            build = koji_session.getBuild(nvr)
            if not build:
                raise ValueError(f"Kernel NVR {nvr} not found in Brew. Check the NVR is correct.")
            self.logger.info("Validated kernel NVR %s (build_id=%s)", nvr, build["build_id"])

            el_ver = isolate_el_version_in_release(parsed["release"])
            if not el_ver:
                raise ValueError(f"Could not determine RHEL version from kernel release: {parsed['release']}")

            ver_rel = f"{parsed['version']}-{parsed['release']}"
            self.kernel_info.append(
                KernelInfo(
                    nvr=nvr,
                    el_ver=el_ver,
                    kernel_core_nvr=f"kernel-core-{ver_rel}",
                    kernel_devel_nvr=f"kernel-devel-{ver_rel}",
                )
            )

    async def _find_rhcos_build(self) -> None:
        """
        Find RHCOS build containing the target kernel. Dispatches to layered
        or non-layered lookup based on group config. Sets self.rhcos_config.
        """
        layered = self.runtime.group_config.rhcos.get("layered_rhcos", False)
        if layered is Missing:
            layered = False

        if layered:
            self._find_rhcos_build_layered()
        else:
            self._find_rhcos_build_nonlayered()

    def _find_rhcos_build_nonlayered(self) -> None:
        """
        For non-layered RHCOS: find the build containing the target kernel-core,
        then resolve per-arch pullspecs using RHCOSBuildFinder. Sets self.rhcos_config.
        Skips builds whose meta.json is incomplete (e.g. in-progress builds).
        """
        target_kernel_nvrs = {ki.kernel_core_nvr for ki in self.kernel_info}
        arches = list(self.runtime.group_config.arches)
        container_configs = get_container_configs(self.runtime)
        version = f"{self._ocp_major}.{self._ocp_minor}"

        finder = RHCOSBuildFinder(self.runtime, version)

        for build_id in self._iter_rhcos_builds_by_kernel(finder, target_kernel_nvrs):
            self.logger.info("Found matching RHCOS build: %s — resolving pullspecs", build_id)
            rhcos_config = {}
            failed = False
            for container_conf in container_configs:
                container_name = str(container_conf.name)
                rhcos_config[container_name] = {"images": {}}

                for arch in arches:
                    arch_finder = RHCOSBuildFinder(self.runtime, version, brew_arch=arch)
                    try:
                        meta = arch_finder.rhcos_build_meta(build_id)
                        pullspec = get_container_pullspec(meta, container_conf)
                        rhcos_config[container_name]["images"][arch] = pullspec
                    except RhcosMissingContainerException:
                        self.logger.debug(
                            "Build %s meta.json missing container key for %s/%s — likely incomplete, skipping",
                            build_id,
                            container_name,
                            arch,
                        )
                        failed = True
                        break
                if failed:
                    break

            if not failed:
                self.rhcos_config = rhcos_config
                return

        raise RuntimeError(
            f"No complete RHCOS build found containing {target_kernel_nvrs}. "
            f"The RHCOS pipeline may not have run yet with this kernel. "
            f"Trigger an RHCOS build and retry."
        )

    def _iter_rhcos_builds_by_kernel(self, finder: RHCOSBuildFinder, target_kernel_nvrs: set[str]):
        """
        Yield RHCOS build IDs that contain a matching kernel-core RPM, newest first.
        Uses RHCOSBuildFinder for URL construction and metadata fetching.
        """
        builds_url = f"{finder.rhcos_release_url()}/builds.json"
        self.logger.info("Fetching RHCOS builds from %s", builds_url)

        @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
        def _fetch_builds():
            response = requests.get(builds_url, timeout=60)
            response.raise_for_status()
            return response.json()

        try:
            builds_data = _fetch_builds()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Failed to fetch RHCOS builds list from {builds_url}: {e}") from e

        if not builds_data.get("builds"):
            raise RuntimeError(f"No RHCOS builds found at {builds_url}")

        for build_entry in builds_data["builds"]:
            build_id = build_entry["id"] if isinstance(build_entry, dict) else build_entry

            try:
                commitmeta = finder.rhcos_build_meta(build_id, meta_type="commitmeta")
            except Exception:
                self.logger.debug("Could not fetch commitmeta for build %s, skipping", build_id)
                continue

            rpm_list = commitmeta.get("rpmostree.rpmdb.pkglist", [])
            build_kernel_nvrs = set()
            for entry in rpm_list:
                if entry[0] == "kernel-core":
                    build_kernel_nvrs.add(f"kernel-core-{entry[2]}-{entry[3]}")

            if target_kernel_nvrs & build_kernel_nvrs:
                yield build_id

    def _find_rhcos_build_layered(self) -> None:
        """
        For layered RHCOS (4.19+): get the latest layered image for each container config
        and verify it contains the target kernel. Sets self.rhcos_config.

        Note: layered RHCOS images are built by a separate pipeline on top of the base
        RHCOS. The layered image tag timestamps differ from the base RHCOS build IDs,
        so we must query the latest tag and verify kernel content.
        """

        target_kernel_nvrs = {ki.kernel_core_nvr for ki in self.kernel_info}
        arches = list(self.runtime.group_config.arches)
        container_configs = get_container_configs(self.runtime)

        rhcos_config = {}
        for container_conf in container_configs:
            container_name = str(container_conf.name)
            rhcos_config[container_name] = {"images": {}}

            for arch in arches:
                build_id, pullspec = get_latest_layered_rhcos_build(
                    container_conf, arch, registry_config=self.runtime.registry_config
                )
                rhcos_config[container_name]["images"][arch] = pullspec

        self._verify_layered_rhcos_kernel(rhcos_config, container_configs, target_kernel_nvrs)
        self.rhcos_config = rhcos_config

    def _verify_layered_rhcos_kernel(
        self,
        rhcos_config: dict,
        container_configs,
        target_kernel_nvrs: set[str],
    ):
        """
        Verify that the layered RHCOS build contains the target kernel
        by extracting RPM metadata from the container image.
        """
        for container_conf in container_configs:
            if not container_conf.get("primary", False):
                continue
            container_name = str(container_conf.name)
            pullspec = rhcos_config.get(container_name, {}).get("images", {}).get("x86_64")
            if not pullspec:
                continue

            with tempfile.TemporaryDirectory() as temp_dir:
                reg_conf_arg = (
                    f" --registry-config={self.runtime.registry_config}" if self.runtime.registry_config else ""
                )
                cmd_assert(
                    f"oc image extract {pullspec}[-1] --path /usr/share/openshift/base/meta.json:{temp_dir} --confirm{reg_conf_arg}",
                    retries=3,
                )
                with open(os.path.join(temp_dir, "meta.json"), "r") as f:
                    meta_data = json.load(f)

            rpm_list = meta_data.get("rpmdb.pkglist", [])
            build_kernel_nvrs = set()
            for entry in rpm_list:
                if entry[0] == "kernel-core":
                    build_kernel_nvrs.add(f"kernel-core-{entry[2]}-{entry[3]}")

            if not (target_kernel_nvrs & build_kernel_nvrs):
                raise RuntimeError(
                    f"Latest layered RHCOS build does not contain target kernel. "
                    f"Expected one of {target_kernel_nvrs}, "
                    f"found {build_kernel_nvrs}. "
                    f"The RHCOS pipeline may not have run yet with this kernel. "
                    f"Trigger an RHCOS build and retry."
                )
            self.logger.info("Verified layered RHCOS contains target kernel")
            return

        raise RuntimeError("No primary RHCOS container config found in group config")

    async def _find_dtk_build(self) -> None:
        """
        Find a driver-toolkit build containing kernel-devel matching the target kernel.
        Sets self.dtk_nvr.
        """
        if self.runtime.build_system == "brew":
            self._find_dtk_build_brew()
        else:
            await self._find_dtk_build_konflux()

    def _find_dtk_build_brew(self) -> None:
        """
        For Brew: find driver-toolkit-container build in candidate tag
        that contains kernel-devel with matching version-release. Sets self.dtk_nvr.
        """
        target_devel_nvrs = {ki.kernel_devel_nvr for ki in self.kernel_info}
        koji_session = koji.ClientSession(BREW_HUB)

        for ki in self.kernel_info:
            candidate_tag = f"rhaos-{self._ocp_major}.{self._ocp_minor}-rhel-{ki.el_ver}-candidate"
            self.logger.info("Searching for DTK builds in tag %s", candidate_tag)

            try:
                tagged_builds = koji_session.listTagged(
                    candidate_tag,
                    package="driver-toolkit-container",
                    latest=True,
                    inherit=True,
                )
            except koji.GenericError as e:
                self.logger.warning("Failed to list tagged builds in %s: %s", candidate_tag, e)
                continue

            for build in tagged_builds:
                if self._dtk_build_has_kernel_devel(koji_session, build, target_devel_nvrs):
                    self.logger.info("Found matching DTK build: %s", build["nvr"])
                    self.dtk_nvr = build["nvr"]
                    return

            try:
                all_tagged = koji_session.listTagged(
                    candidate_tag,
                    package="driver-toolkit-container",
                    latest=False,
                    inherit=True,
                )
            except koji.GenericError:
                continue

            for build in all_tagged[:20]:
                if build["nvr"] in {b["nvr"] for b in tagged_builds}:
                    continue
                if self._dtk_build_has_kernel_devel(koji_session, build, target_devel_nvrs):
                    self.logger.info("Found matching DTK build: %s", build["nvr"])
                    self.dtk_nvr = build["nvr"]
                    return

        raise RuntimeError(
            f"No driver-toolkit build found containing {target_devel_nvrs}. "
            f"DTK may need rebuilding with the target kernel headers. "
            f"Trigger a DTK build and retry."
        )

    def _dtk_build_has_kernel_devel(
        self,
        koji_session: koji.ClientSession,
        build: dict,
        target_devel_nvrs: set[str],
    ) -> bool:
        """
        Check if a DTK container build contains a matching kernel-devel RPM.
        """
        try:
            archives = koji_session.listArchives(buildID=build["build_id"], type="image")
        except koji.GenericError:
            return False

        for archive in archives:
            try:
                rpms = koji_session.listRPMs(imageID=archive["id"])
            except koji.GenericError:
                continue

            for rpm in rpms:
                if rpm["name"] == "kernel-devel":
                    rpm_nvr = f"{rpm['name']}-{rpm['version']}-{rpm['release']}"
                    if rpm_nvr in target_devel_nvrs:
                        return True
        return False

    async def _find_dtk_build_konflux(self) -> None:
        """
        For Konflux: query KonfluxDB for driver-toolkit builds
        containing the target kernel-devel RPM. Sets self.dtk_nvr.
        """
        from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord
        from artcommonlib.konflux.konflux_db import KonfluxDb

        konflux_db = KonfluxDb()
        konflux_db.bind(KonfluxBuildRecord)

        group = f"openshift-{self._ocp_major}.{self._ocp_minor}"
        for ki in self.kernel_info:
            self.logger.info("Searching Konflux DB for DTK with %s", ki.kernel_devel_nvr)

            async for record in konflux_db.search_builds_by_fields(
                where={
                    "name": "driver-toolkit",
                    "group": group,
                    "outcome": str(KonfluxBuildOutcome.SUCCESS),
                },
                array_contains={"installed_rpms": ki.kernel_devel_nvr},
                limit=1,
            ):
                self.logger.info("Found matching DTK build in Konflux: %s", record.nvr)
                self.dtk_nvr = record.nvr
                return

        target_devel_nvrs = {ki.kernel_devel_nvr for ki in self.kernel_info}
        raise RuntimeError(
            f"No driver-toolkit build found in Konflux DB containing {target_devel_nvrs}. "
            f"DTK may need rebuilding with the target kernel headers. "
            f"Trigger a DTK build and retry."
        )

    async def _compute_upgrades(self) -> None:
        """
        Compute the upgrades list by querying the Cincinnati graph and checking
        the release schedule for in-flight releases from the previous minor.
        Sets self.upgrades.
        """
        if not self.runtime:
            return

        version = self.params.assembly_name
        group = f"openshift-{self._ocp_major}.{self._ocp_minor}"
        arches = list(self.runtime.group_config.arches)
        self.logger.info("Calculating upgrade sources for %s across arches: %s", version, arches)

        all_sources: set[str] = set()

        in_flight = get_inflight(version, group, self.params.release_date)
        if in_flight:
            self.logger.info("Found in-flight release from previous minor: %s", in_flight)
            all_sources.add(in_flight)

        for arch in arches:
            try:
                sources = await calc_upgrade_sources_async(version, arch)
                all_sources.update(sources)
            except Exception as e:
                self.logger.warning("Failed to calculate upgrade sources for arch %s: %s", arch, e)

        if all_sources:
            self.upgrades = ",".join(sorted(all_sources, key=functools.cmp_to_key(semver.compare)))
            self.logger.info("Computed %d upgrade sources", len(all_sources))

    def _build_assembly_definition(self) -> dict:
        """
        Construct the assembly YAML definition.

        Return Value(s):
            dict: Complete assembly definition ready to merge into releases.yml
        """
        assembly_type = infer_assembly_type(custom=False, assembly_name=self.params.assembly_name)

        why = f"CVE fix for {', '.join(self.params.cve_ids)}" if self.params.cve_ids else "Targeted pin"

        rpm_pins = []
        image_pins = []

        if self.kernel_info:
            kernel_is = {f"el{ki.el_ver}": ki.nvr for ki in self.kernel_info}
            rpm_pins.append({"distgit_key": "kernel", "why": why, "metadata": {"is": kernel_is}})
            image_pins.append({"distgit_key": "driver-toolkit", "why": why, "metadata": {"is": {"nvr": self.dtk_nvr}}})

        for nvr in self.params.image_nvrs:
            parsed = parse_nvr(nvr)
            distgit_key = parsed["name"].removesuffix("-container")
            image_pins.append({"distgit_key": distgit_key, "why": why, "metadata": {"is": {"nvr": nvr}}})

        group_config = {
            "advisories!": {"rpm": -1, "rhcos": -1},
            "release_jira": "ART-0",
            "shipment!": {"advisories": [{"kind": "image"}]},
            "release_date": self.params.release_date or "",
            "upgrades": self.upgrades,
            "dependencies": {
                "rpms!": [{f"el{ki.el_ver}": ki.nvr, "why": why, "non_gc_tag": "hotfix"} for ki in self.kernel_info]
            },
        }

        issues_config = {}
        if self.params.bug_ids:
            issues_config["targeted_fixes_only"] = True
            issues_config["include"] = [{"id": bug_id} for bug_id in self.params.bug_ids]

        members = {}
        if rpm_pins:
            members["rpms"] = rpm_pins
        if image_pins:
            members["images"] = image_pins

        return {
            "releases": {
                self.params.assembly_name: {
                    "assembly": {
                        "type": assembly_type.value,
                        "basis": {"assembly": self.params.basis_assembly, "reference_releases!": {}},
                        "group": group_config,
                        "rhcos": self.rhcos_config,
                        "members": members,
                        "issues": issues_config,
                    },
                },
            },
        }
