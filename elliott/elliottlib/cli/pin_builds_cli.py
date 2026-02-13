import os
from typing import List, Optional

import click
import requests
import yaml
from artcommonlib import logutil
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.model import ListModel, Model
from artcommonlib.release_util import isolate_el_version_in_release
from artcommonlib.rhcos import get_container_configs, get_container_pullspec
from artcommonlib.rpm_utils import parse_nvr
from doozerlib.brew import get_build_objects
from doozerlib.rhcos import RHCOSBuildFinder

from elliottlib import Runtime
from elliottlib.cli.common import cli, click_coroutine

LOGGER = logutil.get_logger(__name__)


class AssemblyPinBuildsCli:
    def __init__(self, runtime: Runtime, nvrs: List[str], pr: str, why: str, github_token: str):
        self.runtime = runtime
        self.nvrs = nvrs
        self.pr = pr
        self.why = why
        self.github_token = github_token
        self.assembly_config = None

    async def run(self):
        # load disabled configs to enable processing of special components like microshift
        self.runtime.initialize(mode="both", disabled=True)

        self.runtime.konflux_db.bind(KonfluxBuildRecord)
        releases_config = self.runtime.get_releases_config()
        self.assembly_config = releases_config["releases"][self.runtime.assembly]["assembly"]

        if self.pr:
            # get all builds for the PR
            self.nvrs = await self.get_nvrs_for_pr()
            if not self.nvrs:
                raise ValueError(f"Could not find any builds for PR {self.pr}")

        # categorize the nvrs
        art_images_by_comp = {i.get_component_name(): i for i in self.runtime.image_map.values()}
        art_rpms_by_comp = {r.get_component_name(): r for r in self.runtime.rpm_map.values()}

        images, art_rpms, non_art_rpms, rhcos = [], [], [], []
        major, minor = self.runtime.get_major_minor()
        nvrs = set(self.nvrs)
        for nvr in nvrs:
            parsed = parse_nvr(nvr)
            if parsed["name"] in art_images_by_comp:
                if f"{major}.{minor}" not in parsed["version"]:
                    raise ValueError(f"Does nvr belong to the current group? {nvr}")
                images.append(nvr)
            elif parsed["name"].startswith("rhcos"):
                if f"{major}{minor}" not in parsed["version"]:
                    raise ValueError(f"Does rhcos nvr belong to the current group: {nvr}")
                rhcos.append(nvr)
            elif parsed["name"] in art_rpms_by_comp:
                if f"{major}.{minor}" not in parsed["version"]:
                    raise ValueError(f"Does nvr belong to the current group? {nvr}")
                art_rpms.append(nvr)
            else:
                non_art_rpms.append(nvr)

        if len(rhcos) > 1:
            raise ValueError(
                "Did not expect more than one rhcos nvr. Provide only 1 nvr which will be pinned for all arches."
            )

        nvrs_to_fetch = sorted(images + art_rpms + non_art_rpms)
        if nvrs_to_fetch:
            if self.runtime.build_system == Engine.BREW.value:
                self.validate_nvrs_in_brew(nvrs_to_fetch)
            else:
                # only validate if nvrs were passed in directly
                if not self.pr:
                    await self.validate_nvrs_in_konflux_db(nvrs_to_fetch)

        images_changed = self.pin_images(art_images_by_comp, images) if images else False
        art_rpms_changed = self.pin_rpms(art_rpms_by_comp, art_rpms) if art_rpms else False
        non_art_rpms_changed = self.pin_non_art_rpms(non_art_rpms) if non_art_rpms else False
        rhcos_changed = self.pin_rhcos(rhcos[0]) if rhcos else False

        assembly_changed = images_changed or art_rpms_changed or non_art_rpms_changed or rhcos_changed
        out = {
            "releases": {
                self.runtime.assembly: {
                    "assembly": self.assembly_config.primitive(),
                },
            },
        }
        return out, assembly_changed

    async def get_nvrs_for_pr(self):
        images, rpms, nvrs = [], [], []
        repo_url = self.pr.split("/pull")[0]
        for image in self.runtime.image_map.values():
            if not image.config.content.source.git.web:
                LOGGER.warning("public upstream url not found at config.content.source.git.web for image %s")
            if repo_url == image.config.content.source.git.web:
                images.append(image)
        for rpm in self.runtime.rpm_map.values():
            if not rpm.config.content.source.git.web:
                LOGGER.warning("public upstream url not found at config.content.source.git.web for rpm %s")
            if repo_url == rpm.config.content.source.git.web:
                rpms.append(rpm)
        LOGGER.info(f"ART components building from {repo_url}: {[c.get_component_name() for c in images + rpms]}")

        if not (images and rpms):
            raise ValueError(f"No ART components found building from {repo_url}")

        merge_commit, branch = self.get_pr_merge_commit(self.pr, self.github_token)
        # we don't need to validate version of branch since we will validate it in the db
        LOGGER.info(f"PR merged to {branch} with commit {merge_commit}")

        LOGGER.info("Fetching component builds for commit...")
        where = {
            "group": self.runtime.group,
            "commitish": merge_commit,
            # Avoid filtering by source_url
            # because it can be inconsistent bw public and private upstream url
            "outcome": KonfluxBuildOutcome.SUCCESS.value,
            "engine": self.runtime.build_system,
        }

        # TODO: A component can have multiple builds for the same commit. We need to handle this case.
        nvrs_by_dg_key = {}
        async for build_record in self.runtime.konflux_db.search_builds_by_fields(where=where):
            if build_record.name not in nvrs_by_dg_key:
                nvrs_by_dg_key[build_record.name] = []
            nvrs_by_dg_key[build_record.name].append(build_record.nvr)
            nvrs.append(build_record.nvr)

        LOGGER.info(f"Found {len(nvrs)} builds for commit {merge_commit}: {nvrs}")
        return nvrs

    @staticmethod
    def get_pr_merge_commit(pr_url, auth_token):
        repo_url = pr_url.split("/pull")[0]
        org_repo_suffix = repo_url.split("github.com/")[1]
        github_api_repo_url = "https://api.github.com/repos"
        pr_api_url = f"{github_api_repo_url}/{org_repo_suffix}/pulls/{pr_url.split('/')[-1]}"
        LOGGER.info("Fetching url %s", pr_api_url)
        response = requests.get(
            pr_api_url,
            headers={
                "Authorization": f"token {auth_token}",
                "Accept": "application/json",
            },
        )
        response.raise_for_status()
        json_data = response.json()
        sha, branch = None, None
        try:
            sha = json_data["merge_commit_sha"]
            branch = json_data["base"]["ref"]
        except KeyError:
            raise ValueError(f"Could not find merge commit SHA for PR {pr_url}. Are you sure it merged?")
        return sha, branch

    def validate_nvrs_in_brew(self, nvrs_to_fetch):
        LOGGER.info("Validating image and rpm NVRs exist in brew...")
        with self.runtime.shared_koji_client_session() as koji_api:
            build_objects = get_build_objects(nvrs_to_fetch, koji_api)
        found_nvrs = [b["nvr"] for b in build_objects if b is not None]
        if len(found_nvrs) != len(nvrs_to_fetch):
            missing = set(nvrs_to_fetch) - set(found_nvrs)
            raise ValueError(f"Could not find the following NVRs in brew: {missing}")

    async def validate_nvrs_in_konflux_db(self, nvrs_to_fetch):
        LOGGER.info("Fetching NVRs from DB...")
        where = {"group": self.runtime.group, "engine": self.runtime.build_system}
        await self.runtime.konflux_db.get_build_records_by_nvrs(
            nvrs_to_fetch, where=where, strict=True, exclude_large_columns=True
        )

    def pin_images(self, art_images_by_comp, images):
        changed = False
        self.assembly_config["members"].setdefault("images", ListModel([]))
        pinned_member_images = self.assembly_config["members"]["images"].primitive()
        pinned_member_images = {i["distgit_key"]: i for i in pinned_member_images}
        for i in images:
            comp = parse_nvr(i)["name"]
            dg_key = art_images_by_comp[comp].distgit_key
            image_pin = {
                "distgit_key": dg_key,
                "metadata": {
                    "is": {"nvr": i},
                },
                "why": self.why,
            }
            if dg_key in pinned_member_images:
                # we only care if the nvr is different
                if pinned_member_images[dg_key]["metadata"]["is"] != image_pin["metadata"]["is"]:
                    pinned_member_images[dg_key] = image_pin
                    changed = True
            else:
                pinned_member_images[dg_key] = image_pin
                changed = True

        if changed:
            self.assembly_config["members"]["images"] = list(pinned_member_images.values())
        return changed

    def pin_rpms(self, art_rpms_by_comp, rpms):
        changed = False
        self.assembly_config["members"].setdefault("rpms", ListModel([]))
        pinned_member_rpms = self.assembly_config["members"]["rpms"].primitive()
        pinned_member_rpms = {r["distgit_key"]: r for r in pinned_member_rpms}
        for r in rpms:
            parsed = parse_nvr(r)
            comp = parsed["name"]
            dg_key = art_rpms_by_comp[comp].distgit_key
            el_v = isolate_el_version_in_release(parsed["release"])
            rpm_pin = {
                "distgit_key": dg_key,
                "metadata": {
                    "is": {f"el{el_v}": r},
                },
                "why": self.why,
            }
            if dg_key in pinned_member_rpms:
                # we only care if the nvrs are different
                if pinned_member_rpms[dg_key]["metadata"]["is"] != rpm_pin["metadata"]["is"]:
                    # merge the is metadata since it can contain nvrs for multiple rhel targets
                    pinned_member_rpms[dg_key]["metadata"]["is"].update(rpm_pin["metadata"]["is"])
                    pinned_member_rpms[dg_key]["why"] = self.why
                    changed = True
            else:
                pinned_member_rpms[dg_key] = rpm_pin
                changed = True

        # art rpms usually have multiple rhel targets
        # complain if all rhel targets are not pinned
        for dg_key in pinned_member_rpms:
            rpm_meta = self.runtime.rpm_map[dg_key]
            el_targets = rpm_meta.determine_rhel_targets()
            for el_v in el_targets:
                if f"el{el_v}" not in pinned_member_rpms[dg_key]["metadata"]["is"]:
                    raise ValueError(f"RPM {dg_key} is missing a pin for rhel{el_v}. Please provide an NVR for it.")

        if changed:
            self.assembly_config["members"]["rpms"] = list(pinned_member_rpms.values())
        return changed

    def pin_non_art_rpms(self, rpms):
        changed = False
        self.assembly_config["group"].setdefault("dependencies", Model({})).setdefault("rpms", ListModel([]))
        pinned_non_art_rpms = self.assembly_config["group"]["dependencies"]["rpms"].primitive()
        # pinned_non_art_rpms: nvr -> pin
        pinned_non_art_rpms = {r[next(k for k in r.keys() if "el" in k)]: r for r in pinned_non_art_rpms}
        for r in rpms:
            if r in pinned_non_art_rpms:
                continue

            parsed = parse_nvr(r)
            el_v = isolate_el_version_in_release(parsed["release"])
            if not el_v:
                raise ValueError(f"Could not determine RHEL version for {r}")
            rpm_pin = {
                f"el{el_v}": r,
                "why": self.why,
                "non_gc_tag": "insert tag here if needed",
            }
            pinned_non_art_rpms[r] = rpm_pin
            changed = True

        if changed:
            self.assembly_config["group"]["dependencies"]["rpms"] = list(pinned_non_art_rpms.values())
        return changed

    def pin_rhcos(self, rhcos_nvr):
        """
        Pin RHCOS build to the assembly's group.rhcos section.
        The RHCOS build is identified by its build ID, which is extracted from the NVR.
        For each architecture in the group, we generate the appropriate container pullspecs.
        """
        parsed = parse_nvr(rhcos_nvr)
        build_id = f"{parsed['version']}-{parsed['release']}"
        LOGGER.info(f"Pinning RHCOS build: {build_id}")

        rhcos_info = {}
        changed = False

        for arch in self.runtime.group_config.arches:
            brew_arch = brew_arch_for_go_arch(arch)
            LOGGER.info(f"Getting RHCOS pullspecs for build {build_id}-{brew_arch}...")
            for container_conf in get_container_configs(self.runtime):
                major, minor = self.runtime.get_major_minor()
                version = f"{major}.{minor}"
                finder = RHCOSBuildFinder(self.runtime, version, brew_arch, False)
                if container_conf.name not in rhcos_info:
                    rhcos_info[container_conf.name] = {"images": {}}
                pullspec = get_container_pullspec(
                    finder.rhcos_build_meta(build_id),
                    container_conf or finder.get_primary_container_conf(),
                )
                rhcos_info[container_conf.name]["images"][arch] = pullspec

        self.assembly_config.setdefault("rhcos", Model({}))
        current_rhcos = self.assembly_config["rhcos"].primitive()
        if current_rhcos != rhcos_info:
            self.assembly_config["rhcos"] = Model(rhcos_info)
            changed = True

        return changed


@cli.command("pin-builds", short_help="Pin given builds to assembly")
@click.argument("nvrs", metavar="NVR", required=False, nargs=-1)
@click.option("--pr", metavar="PR", required=False, help="Pin all builds for the given PR")
@click.option("--why", metavar="REASON", required=True, help="Reason for pinning")
@click.pass_obj
@click_coroutine
async def assembly_pin_builds_cli(runtime: Runtime, nvrs: List[str], pr: Optional[str], why: str):
    """
    Pin given builds to assembly.
    For RHCOS build, ensure it has prefix `rhcos` e.g `rhcos-414.92.202309222337-0`
    Note: This command does not validate assembly consistency. It is only meant as a helper command.
    For reference, see: https://art-docs.engineering.redhat.com/assemblies/#pinning-builds
    The pins are global to the assembly:
    - Images are pinned to assembly's members.images
    - ART RPMs are pinned to assembly's members.rpms
    - Non-ART RPMs are pinned to assembly's group.dependencies.rpms
    - Rhcos build is pinned to assembly's group.rhcos

    Command does not support pinning rpms as dependencies to specific components
    Output is the modified assembly yaml to standard output. It will not commit or push any changes.
    """
    if sum([bool(nvrs), bool(pr)]) != 1:
        raise click.UsageError("Exactly one of NVRs or PR is required.")

    github_token = None
    if pr:
        github_token = os.environ.get("GITHUB_TOKEN")
        if not github_token:
            raise ValueError("GITHUB_TOKEN must be set in the environment to query PR information")

    pipeline = AssemblyPinBuildsCli(runtime, nvrs, pr, why, github_token)
    out, changed = await pipeline.run()
    if changed:
        LOGGER.info("Assembly config updated. Pins added.")
        click.echo(yaml.dump(out, default_flow_style=False))
    else:
        LOGGER.info("No change in assembly config. Pins already exist.")
