import asyncio
import io
import logging
import os
import re
from typing import List, cast

import click
import koji
from artcommonlib import exectools
from artcommonlib.brew import BuildStates
from artcommonlib.constants import (
    BREW_HUB,
    GOLANG_BUILDER_IMAGE_NAME,
    PRODUCT_NAMESPACE_MAP,
    REGISTRY_REDHAT_IO,
)
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.release_util import split_el_suffix_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.backend.base_image_handler import ART_IMAGES_BASE_APPLICATION
from elliottlib import util as elliottutil
from elliottlib.constants import GOLANG_BUILDER_CVE_COMPONENT

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.oc import get_image_info
from pyartcd.runtime import Runtime
from pyartcd.util import default_release_suffix

_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()
PUBLISHED_GOLANG_BUILDER_REPO = f"{REGISTRY_REDHAT_IO}/openshift/{ART_IMAGES_BASE_APPLICATION}"


def is_latest_build(ocp_version: str, el_v: int, nvr: str, koji_session) -> bool:
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    parsed_nvr = parse_nvr(nvr)
    latest_build = koji_session.getLatestBuilds(build_tag, package=parsed_nvr['name'])
    if not latest_build:  # if this happens, investigate
        raise ValueError(f'Cannot find latest {parsed_nvr["name"]} build in {build_tag}. Please investigate.')
    if nvr == latest_build[0]['nvr']:
        _LOGGER.info(f'{nvr} is the latest build in {build_tag}')
        return True
    override_tag = f'rhaos-{ocp_version}-rhel-{el_v}-override'
    _LOGGER.info(
        f'{nvr} is not the latest build in {build_tag}. Run `brew tag {override_tag} {nvr}` to tag the '
        f'build and then run `brew regen-repo {build_tag}` to make it available.'
    )
    return False


def get_latest_nvr_in_tag(tag: str, package: str, koji_session) -> str:
    latest_build = koji_session.listTagged(tag, latest=True, package=package, inherit=False)
    if not latest_build:
        return None
    return latest_build[0]['nvr']


async def is_latest_and_available(ocp_version: str, el_v: int, nvr: str, koji_session) -> bool:
    if not is_latest_build(ocp_version, el_v, nvr, koji_session):
        return False
    # If regen repo has been run this would take a few seconds
    # sadly --timeout cannot be less than 1 minute, so we wait for 1 minute
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    cmd = f'brew wait-repo {build_tag} --build {nvr} --request --timeout=1'
    rc, _, _ = await exectools.cmd_gather_async(cmd, check=False)
    if rc != 0:
        _LOGGER.info(
            f'Build {nvr} is tagged but not available in {build_tag}. Run `brew regen-repo {build_tag} to '
            'make the build available.'
        )
        return False
    _LOGGER.info(f'{nvr} is available in {build_tag}')
    return True


def extract_and_validate_golang_nvrs(ocp_version: str, go_nvrs: List[str]):
    match = re.fullmatch(r"(\d).(\d+)", ocp_version)
    if not match:
        raise ValueError(f'Invalid OCP version: {ocp_version}')
    major, minor = int(match[1]), int(match[2])
    if (major, minor) < (4, 12):
        raise ValueError(f'Only OCP 4.12+ is supported, found: {ocp_version}')

    supported_els = {8, 9}
    if (major, minor) >= (4, 21):
        supported_els.add(10)

    if len(go_nvrs) > len(supported_els):
        raise click.BadParameter(f'There should be max 1 nvr for each supported rhel version: {supported_els}')

    el_nvr_map = dict()  # {8: 'golang-1.16.7-1.el8', 9: 'golang-1.16.7-1.el9'}
    go_version = None
    for nvr in go_nvrs:
        parsed_nvr = parse_nvr(nvr)
        name = parsed_nvr['name']
        if name != 'golang':
            raise ValueError(f'Only `golang` nvrs are supported, found package name: {name}')
        if go_version and go_version != parsed_nvr['version']:
            raise ValueError(
                f'All nvrs should have the same golang version, found: {go_version} and {parsed_nvr["version"]}'
            )
        go_version = parsed_nvr['version']

        _, el_version = split_el_suffix_in_release(parsed_nvr['release'])
        # el_version is either None or something like "el8"
        if not el_version:
            raise ValueError(f'Cannot detect an el version in NVR {nvr}')
        el_version = int(el_version[2:])
        if el_version not in supported_els:
            raise ValueError(
                f'Unsupported RHEL version detected for nvr {nvr}, supported versions are: {supported_els}'
            )
        if el_version in el_nvr_map:
            raise ValueError(f'Cannot have two nvrs for the same rhel version: {nvr}, {el_nvr_map[el_version]}')
        el_nvr_map[el_version] = nvr
    return go_version, el_nvr_map


def extract_major_minor(version: str, label: str = "version") -> str:
    if not isinstance(version, str):
        output = io.StringIO()
        yaml.dump({"value": version}, output)
        version = output.getvalue().split(":", 1)[1].strip()
    match = re.fullmatch(r"(\d+)\.(\d+)(?:\.\d+)?", version)
    if not match:
        raise ValueError(f"Invalid {label}: {version}")
    return f"{match[1]}.{match[2]}"


async def move_golang_bugs(
    ocp_version: str,
    cves: list[str] | None = None,
    nvrs: list[str] | None = None,
    components: list[str] | None = None,
    force_update_tracker: bool = False,
    rpms_only: bool = False,
    dry_run: bool = False,
):
    cmd = [
        'elliott',
        '--group',
        f'openshift-{ocp_version}',
        '--assembly',
        'stream',
        'find-bugs:golang',
        '--analyze',
        '--update-tracker',
    ]
    if cves:
        for cve in cves:
            cmd.extend(['--cve-id', cve])
    if nvrs:
        for nvr in nvrs:
            cmd.extend(['--fixed-in-nvr', nvr])
    if components:
        for component in components:
            cmd.extend(['--component', component])
    if force_update_tracker:
        cmd.append('--force-update-tracker')
    if rpms_only:
        cmd.append('--rpms-only')
    if dry_run:
        cmd.append('--dry-run')
    await exectools.cmd_assert_async(cmd)


class UpdateGolangPipeline:
    def __init__(
        self,
        runtime: Runtime,
        ocp_version: str,
        cves: List[str] | None,
        force_update_tracker: bool,
        go_nvrs: List[str],
        art_jira: str,
        tag_builds: bool,
        scratch: bool = False,
        force_image_build: bool = False,
        build_system: str = 'brew',
        kubeconfig: str | None = None,
        data_path: str | None = None,
        data_gitref: str | None = None,
        skip_pr: bool = False,
        external_golang_rpms: bool = False,
        network_mode: str | None = None,
    ):
        self.runtime = runtime
        self.dry_run = runtime.dry_run
        self.scratch = scratch
        self.ocp_version = ocp_version
        self.cves = cves
        self.force_update_tracker = force_update_tracker
        self.force_image_build = force_image_build
        self.go_nvrs = go_nvrs
        self.art_jira = art_jira
        self.build_system = build_system
        self.koji_session = koji.ClientSession(BREW_HUB)  # Always needed for RPM builds
        self.tag_builds = tag_builds
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.skip_pr = skip_pr
        self.external_golang_rpms = external_golang_rpms
        self.network_mode = network_mode
        self._slack_client = self.runtime.new_slack_client()
        self._doozer_working_dir = self.runtime.working_dir / "doozer-working"
        self._doozer_env_vars = os.environ.copy()
        self._branch_content = None

        # Get kubeconfig from environment variable if not provided
        if not kubeconfig:
            kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
        self.kubeconfig = kubeconfig

        # GitHub auth is handled by get_github_client_for_org() with App auth / PAT fallback

        # Initialize KonfluxDb for Konflux build system
        if build_system in ('konflux', 'both'):
            self.konflux_db = KonfluxDb()
            self.konflux_db.bind(KonfluxBuildRecord)

    @staticmethod
    def _load_yaml_from_repo(repo, path: str, ref: str):
        return yaml.load(repo.get_contents(path, ref=ref).decoded_content)

    def _get_upstream_ocp_build_data_repo(self):
        return get_github_client_for_org("openshift-eng").get_repo("openshift-eng/ocp-build-data")

    def _get_branch_content(self):
        if self._branch_content is None:
            branch = f"openshift-{self.ocp_version}"
            upstream_repo = self._get_upstream_ocp_build_data_repo()
            self._branch_content = {
                "branch": branch,
                "repo": upstream_repo,
                "group": self._load_yaml_from_repo(upstream_repo, "group.yml", branch),
                "streams": self._load_yaml_from_repo(upstream_repo, "streams.yml", branch),
            }
        return self._branch_content

    def _get_allowed_go_major_minors(self) -> tuple[str, dict[str, str]]:
        branch_content = self._get_branch_content()
        branch = branch_content["branch"]
        group_content = branch_content["group"]

        vars_content = group_content.get("vars", {})
        go_latest_var = "GO_LATEST"
        go_latest = vars_content.get(go_latest_var)
        if not go_latest:
            raise ValueError(
                f"{go_latest_var} variable not found in group.yml, please make sure it is defined before running the pipeline"
            )

        allowed_major_minors = {
            var_name: extract_major_minor(var_value, f"group.yml {var_name}")
            for var_name in ("GO_LATEST", "GO_EXTRA", "GO_PREVIOUS")
            for var_value in [vars_content.get(var_name)]
            if var_value
        }
        return branch, allowed_major_minors

    def validate_go_version_matches_group_vars(self, go_version: str):
        branch, allowed_major_minors = self._get_allowed_go_major_minors()
        build_major_minor = extract_major_minor(go_version, "golang build version")
        if build_major_minor not in allowed_major_minors.values():
            allowed_versions = ", ".join(
                f"{var_name} ({major_minor})" for var_name, major_minor in allowed_major_minors.items()
            )
            raise ValueError(
                f"The provided golang build major.minor ({build_major_minor}) must match "
                f"one of {branch} group.yml vars: {allowed_versions}."
            )
        return branch, allowed_major_minors, build_major_minor

    def validate_tag_builds_go_latest(self, branch: str, allowed_major_minors: dict[str, str], build_major_minor: str):
        latest_major_minor = allowed_major_minors["GO_LATEST"]
        if build_major_minor != latest_major_minor:
            raise ValueError(
                f"When --tag-builds is set, the provided golang build major.minor ({build_major_minor}) must match "
                f"{branch} group.yml GO_LATEST major.minor ({latest_major_minor})."
            )

    async def run(self):
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(self.ocp_version, self.go_nvrs)
        _LOGGER.info(f'Golang version detected: {go_version}')
        _LOGGER.info(f'NVRs by rhel version: {el_nvr_map}')
        branch, allowed_major_minors, build_major_minor = self.validate_go_version_matches_group_vars(go_version)
        if self.tag_builds:
            self.validate_tag_builds_go_latest(branch, allowed_major_minors, build_major_minor)

        # el10 is only supported for build roots (RPM tagging), not for golang-builder images yet
        el_nvr_map_for_images = {el_v: nvr for el_v, nvr in el_nvr_map.items() if el_v != 10}
        if el_nvr_map.keys() - el_nvr_map_for_images.keys():
            _LOGGER.info("RHEL 10 NVRs will only be used for build root tagging, not for golang-builder images")
        self._slack_client.bind_channel(self.ocp_version)
        running_in_jenkins = os.environ.get('BUILD_ID', False)
        if running_in_jenkins:
            title_update = f" {self.ocp_version} - {go_version} - el{list(el_nvr_map.keys())} - {self.build_system}"
            if self.dry_run:
                title_update += ' [dry-run]'
            jenkins.init_jenkins()
            jenkins.update_title(title_update)
        external_repos_msg = " using golang RPMs from external repos" if self.external_golang_rpms else ""
        await self._slack_client.say_in_thread(
            f":construction: Updating golang for {self.ocp_version} (building images on {self.build_system}{external_repos_msg}) :construction:"
        )

        if self.external_golang_rpms:
            _LOGGER.warning(
                "Using golang RPMs from external repos. Skipping tagging and availability checks. "
                "Ensure external repos are enabled in golang-builder image metadata config."
            )
            await self._slack_client.say_in_thread(
                ":warning: Using golang RPMs from external repos. Skipping tagging and availability checks."
            )
        else:
            # Process golang RPM builds (always from Brew)
            cannot_proceed = not all(
                await asyncio.gather(*[self.process_build(el_v, nvr) for el_v, nvr in el_nvr_map.items()])
            )
            if cannot_proceed:
                raise ValueError(
                    'Cannot proceed until all builds are tagged and available, did you forget check TAG_BUILD?'
                )

            _LOGGER.info('All golang RPM builds are tagged and available!')
            await self._slack_client.say_in_thread("All golang RPM builds are tagged and available!")

        # Check if openshift-golang-builder image builds exist for the provided compiler builds
        # Only for RHEL versions that support golang-builder images (excludes el10 for now)
        brew_nvrs = {}
        konflux_nvrs = {}
        if not self.force_image_build:
            if self.build_system in ['both', 'brew']:
                brew_nvrs = self.get_existing_builders(el_nvr_map_for_images, go_version)
            if self.build_system in ['both', 'konflux']:
                konflux_nvrs = await self.get_existing_builders_konflux(el_nvr_map_for_images, go_version)

        # Determine which rhel versions need builds
        brew_missing = (
            el_nvr_map_for_images.keys() - brew_nvrs.keys() if self.build_system in ['both', 'brew'] else set()
        )
        konflux_missing = (
            el_nvr_map_for_images.keys() - konflux_nvrs.keys() if self.build_system in ['both', 'konflux'] else set()
        )

        if brew_missing or konflux_missing:
            if not self.external_golang_rpms:
                for el_v in el_nvr_map_for_images.keys():
                    self.verify_golang_builder_repo(el_v, go_version)

            # Rebase and build missing images
            build_tasks = []
            if self.build_system in ['both', 'brew'] and brew_missing:
                build_tasks.extend([self._rebase_and_build_brew(el_v, go_version) for el_v in brew_missing])
            if self.build_system in ['both', 'konflux'] and konflux_missing:
                build_tasks.extend([self._rebase_and_build_konflux(el_v, go_version) for el_v in konflux_missing])
            if build_tasks:
                results = await asyncio.gather(*build_tasks, return_exceptions=True)
            errors = [r for r in results if isinstance(r, Exception)]
            if errors:
                error_msgs = "\n".join([str(e) for e in errors])
                raise RuntimeError(f"{len(errors)} image build(s) failed:\n{error_msgs}")

            # Now all builders should be available, fetch again
            if self.build_system in ['both', 'brew']:
                brew_nvrs = self.get_existing_builders(el_nvr_map_for_images, go_version)
            if self.build_system in ['both', 'konflux']:
                konflux_nvrs = await self.get_existing_builders_konflux(el_nvr_map_for_images, go_version)

            brew_still_missing = (
                el_nvr_map_for_images.keys() - brew_nvrs.keys() if self.build_system in ['both', 'brew'] else set()
            )
            konflux_still_missing = (
                el_nvr_map_for_images.keys() - konflux_nvrs.keys()
                if self.build_system in ['both', 'konflux']
                else set()
            )
            if brew_still_missing or konflux_still_missing:
                error_parts = []
                if brew_still_missing:
                    error_parts.append(f'Brew: {brew_still_missing}')
                if konflux_still_missing:
                    error_parts.append(f'Konflux: {konflux_still_missing}')
                error_msg = f'Failed to find existing builder(s) for rhel version(s): {", ".join(error_parts)}'
                if self.external_golang_rpms:
                    error_msg += (
                        '. When using --external-golang-rpms, ensure the external repo is enabled '
                        'in the enabled_repos section of golang-builder image metadata config.'
                    )
                raise ValueError(error_msg)

        # streams.yml should only reference the published pullspecs for Konflux-built builders.
        builder_nvrs = konflux_nvrs
        builder_pullspecs = {el_v: self._get_builder_pullspec(nvr) for el_v, nvr in builder_nvrs.items()}
        if builder_pullspecs:
            await asyncio.gather(
                *[self._ensure_builder_pullspec_available(pullspec) for pullspec in builder_pullspecs.values()]
            )
            builder_details = "\n".join(
                [f"  - RHEL {el_v}: {nvr} -> {builder_pullspecs[el_v]}" for el_v, nvr in builder_nvrs.items()]
            )
            builder_message = "Konflux golang builders available for streams.yml:\n" + builder_details
            _LOGGER.info(builder_message)
            await self._slack_client.say_in_thread(builder_message)
        else:
            _LOGGER.info("No Konflux golang builder images found; streams.yml will not be updated.")

        await self.update_golang_streams(go_version, builder_pullspecs)

        await move_golang_bugs(
            ocp_version=self.ocp_version,
            cves=self.cves,
            nvrs=self.go_nvrs if self.cves else None,
            components=[GOLANG_BUILDER_CVE_COMPONENT],
            force_update_tracker=self.force_update_tracker,
            dry_run=self.dry_run,
        )
        await self._slack_client.say_in_thread(f":white_check_mark: Updating golang for {self.ocp_version} complete.")

    async def process_build(self, el_v, nvr):
        if await is_latest_and_available(self.ocp_version, el_v, nvr, self.koji_session):
            return True
        if not self.tag_builds:
            return False
        # Tag builds into override tag
        await self.tag_build(el_v, nvr)
        # Wait for repo to be available (5 hours max)

        if self.dry_run:
            _LOGGER.info(f"[DRY RUN] Would have waited for {nvr} to be available in build tags")
            return True

        for _ in range(30):
            await asyncio.sleep(600)  # 10 minutes
            if await is_latest_and_available(self.ocp_version, el_v, nvr, self.koji_session):
                return True
            _LOGGER.info("wait 10 mins...")
        _LOGGER.info("build not available after 5 hours")
        return False

    def brew_login(self):
        if not self.koji_session.logged_in:
            _LOGGER.info("user logged out from session, login again")
            self.koji_session.gssapi_login()

    async def tag_build(self, el_v, nvr):
        build_tag = f'rhaos-{self.ocp_version}-rhel-{el_v}-override'
        self.brew_login()
        builds_to_tag = [nvr]
        if el_v == 8:
            rhel8_module_tag = self.get_module_tag(nvr, el_v)
            if rhel8_module_tag:
                latest_rhel8_builds = self.koji_session.listTagged(rhel8_module_tag, latest=True, inherit=True)
                # need to tag delve go-toolset golang 3 module builds
                builds_to_tag = [b['nvr'] for b in latest_rhel8_builds]
        for build in builds_to_tag:
            if self.dry_run:
                _LOGGER.info(f"[DRY RUN] Would have tagged {build} into {build_tag}")
                continue
            self.koji_session.tagBuild(build_tag, build)
            _LOGGER.info(f"Tagged {build} with {build_tag} tag")
            await self._slack_client.say_in_thread(f"Tagged {build} with {build_tag} tag")

    def get_existing_builders(self, el_nvr_map, go_version):
        component = GOLANG_BUILDER_CVE_COMPONENT
        _LOGGER.info(f"Checking if {component} builds exist for given golang builds")
        package_info = self.koji_session.getPackage(component)
        if not package_info:
            raise IOError(f'Cannot find brew package info for {component}')
        package_id = package_info['id']
        builder_nvrs = {}
        for el_v, go_nvr in el_nvr_map.items():
            pattern = f"{component}-v{go_version}-*el{el_v}*"
            builds = self.koji_session.listBuilds(
                packageID=package_id,
                state=BuildStates.COMPLETE.value,
                pattern=pattern,
                queryOpts={'limit': 1, 'order': '-creation_event_id'},
            )
            if builds:
                build = builds[0]
                # `elliottutil.get_golang_container_nvrs` uses p-flag to determine the build system.
                # However, our existing golang-builders may not have p-flags.
                # Here we are safe to looking at only Brew builds.
                go_nvr_map = elliottutil.get_golang_container_nvrs_brew(
                    [(build['name'], build['version'], build['release'])],
                    _LOGGER,
                )  # {'1.20.12-2.el9_3': {('openshift-golang-builder-container', 'v1.20.12',
                # '202403212137.el9.g144a3f8.el9')}}
                builder_go_vr = list(go_nvr_map.keys())[0]
                if builder_go_vr in go_nvr:
                    _LOGGER.info(f"Found existing builder image: {build['nvr']} built with {go_nvr}")
                    builder_nvrs[el_v] = build['nvr']
        return builder_nvrs

    async def get_existing_builders_konflux(self, el_nvr_map: dict[int, str], go_version: str):
        """
        Check if openshift-golang-builder builds exist in Konflux for the provided compiler builds.
        Similar to get_existing_builders but queries KonfluxDb instead of Brew.
        """
        # group = f'openshift-{self.ocp_version}'
        _LOGGER.info(f"Checking if {GOLANG_BUILDER_IMAGE_NAME} builds exist in Konflux for given golang builds")

        builder_nvrs = {}
        extra_patterns = {'nvr': f"{GOLANG_BUILDER_CVE_COMPONENT}-v{go_version}"}
        build_records = await asyncio.gather(
            *(
                anext(
                    self.konflux_db.search_builds_by_fields(
                        where={
                            "name": GOLANG_BUILDER_IMAGE_NAME,
                            "el_target": f'el{el_v}',
                            "artifact_type": str(ArtifactType.IMAGE),
                            "outcome": str(KonfluxBuildOutcome.SUCCESS),
                            "engine": str(Engine.KONFLUX),
                        },
                        extra_patterns=extra_patterns,
                        limit=1,
                    ),
                    None,
                )
                for el_v in el_nvr_map
            )
        )
        found_records = {
            el_v: cast(KonfluxBuildRecord, build_record)
            for el_v, build_record in zip(el_nvr_map, build_records)
            if build_record
        }

        # The found builder records until this point, their NVRs contain the go version substring that we are looking for
        # Sometimes due to misconfiguration, installed golang rpm may be different/incorrect
        # So ensure that the installed rpm is exactly the one we want
        for el_v, build_record in found_records.items():
            go_nvr_map = elliottutil.get_golang_container_nvrs_for_konflux_record(
                [build_record],
                _LOGGER,
                exact=True,
            )
            actual_go_nvr, _ = next(iter(go_nvr_map.items()))
            expected_go_nvr = el_nvr_map[el_v]
            if actual_go_nvr != expected_go_nvr:
                _LOGGER.warning(
                    f"Installed golang rpm in {build_record.nvr} is different from the expected one: {actual_go_nvr} != {expected_go_nvr}"
                )
            else:
                _LOGGER.info(
                    f"Found existing builder image in Konflux: {build_record.nvr} built with {expected_go_nvr}"
                )
                builder_nvrs[el_v] = build_record.nvr
        return builder_nvrs

    def _get_builder_pullspec(self, builder_nvr: str):
        """Generate the published pullspec used in streams.yml for Konflux-built builders."""
        parsed_nvr = parse_nvr(builder_nvr)
        component_name = parsed_nvr["name"]
        if component_name == GOLANG_BUILDER_IMAGE_NAME:
            component_name = GOLANG_BUILDER_CVE_COMPONENT
        elif component_name != GOLANG_BUILDER_CVE_COMPONENT:
            raise ValueError(f"Expected a golang builder image NVR, got: {builder_nvr}")
        published_nvr = f'{component_name}-{parsed_nvr["version"]}-{parsed_nvr["release"]}'
        return f'{PUBLISHED_GOLANG_BUILDER_REPO}:{published_nvr}'

    async def _ensure_builder_pullspec_available(self, pullspec: str, retries: int = 3):
        """Verify the published golang builder pullspec is available before updating streams.yml."""
        if retries != 3:
            _LOGGER.warning("Ignoring custom retry count %s; pyartcd.oc.get_image_info retries 3 times", retries)
        try:
            await get_image_info(pullspec, raise_if_not_found=True)
        except Exception as e:
            raise RuntimeError(f"Published golang builder pullspec is not available: {pullspec}: {e}") from e

    async def update_golang_streams(self, go_version, builder_pullspecs):
        """
        Update golang builders for current release in ocp-build-data
        1. First check go verion from group.yml var to decide if it's a major version bump or minor version bump
        2. Get the golang image value, find and replace for each item in streams.yml
        3. If it'a major version bump, also need to update key in streams.yml and vars in group.yml
        4. Create pr to update changes
        """
        branch_content = self._get_branch_content()
        branch = branch_content["branch"]
        upstream_repo = branch_content["repo"]
        streams_content = branch_content["streams"]
        group_content = branch_content["group"]

        go_latest_var, go_previous_var = "GO_LATEST", "GO_PREVIOUS"
        go_latest = group_content['vars'].get(go_latest_var)
        if not go_latest:
            raise ValueError(
                f"{go_latest_var} variable not found in group.yml, please make sure it is defined before running the pipeline"
            )
        go_previous = group_content['vars'].get(go_previous_var)
        build_major_minor = extract_major_minor(go_version, "golang build version")
        latest_major_minor = extract_major_minor(go_latest, f"group.yml {go_latest_var}")
        previous_major_minor = extract_major_minor(go_previous, f"group.yml {go_previous_var}") if go_previous else None
        build_major_minor_tuple = tuple(map(int, build_major_minor.split(".")))
        latest_major_minor_tuple = tuple(map(int, latest_major_minor.split(".")))

        # these group var templates are used in streams.yml
        # but we do not need to replace/update them
        # we will just look for the literal value
        go_latest_var_template = "{" + go_latest_var + "}"
        go_previous_var_template = "{" + go_previous_var + "}"

        def latest_go_stream_name(el_v):
            return f'rhel-{el_v}-golang-{go_latest_var_template}'

        def previous_go_stream_name(el_v):
            return f'rhel-{el_v}-golang-{go_previous_var_template}'

        update_streams = update_group = False

        # register aliases
        stream_alias_map = {}
        for stream_name, info in streams_content.items():
            aliases = info.get('aliases', [])
            for alias in aliases:
                if alias in streams_content:
                    raise ValueError(f"Alias name {alias} already exists in streams.yml")
                if alias in stream_alias_map:
                    raise ValueError(
                        f"Duplicate alias detected: {alias} is already mapped to {stream_alias_map[alias]}"
                    )
                stream_alias_map[alias] = stream_name

        # first check if an exact stream is defined, if not check aliases
        def get_stream(stream_name):
            stream_key = stream_alias_map.get(stream_name, None) or stream_name
            return streams_content.get(stream_key, None)

        # This is to bump minor golang for GO_LATEST
        if build_major_minor == latest_major_minor:
            for el_v, pullspec in builder_pullspecs.items():
                _LOGGER.info("Looking for golang stream %s in streams.yml", latest_go_stream_name(el_v))
                latest_go = get_stream(latest_go_stream_name(el_v))['image']

                for _, info in streams_content.items():
                    if info['image'] == latest_go:
                        info['image'] = pullspec
                        update_streams = True
        # This is to bump minor golang for GO_PREVIOUS
        elif previous_major_minor and build_major_minor == previous_major_minor:
            for el_v, pullspec in builder_pullspecs.items():
                _LOGGER.info("Looking for golang stream %s in streams.yml", previous_go_stream_name(el_v))
                previous_go = get_stream(previous_go_stream_name(el_v))['image']

                for _, info in streams_content.items():
                    if info['image'] == previous_go:
                        info['image'] = pullspec
                        update_streams = True
        # This is to bump major golang for GO_LATEST and update GO_PREVIOUS to current GO_LATEST
        elif build_major_minor_tuple > latest_major_minor_tuple:
            for el_v, pullspec in builder_pullspecs.items():
                _LOGGER.info("Looking for golang stream %s in streams.yml", latest_go_stream_name(el_v))
                latest_go = get_stream(latest_go_stream_name(el_v))['image']

                _LOGGER.info("Looking for golang stream %s in streams.yml", previous_go_stream_name(el_v))
                previous_go = get_stream(previous_go_stream_name(el_v))['image'] if go_previous else None

                for _, info in streams_content.items():
                    if info['image'] == latest_go:
                        info['image'] = pullspec
                    if info['image'] == previous_go:
                        info['image'] = latest_go
                group_content['vars'][go_latest_var] = go_version
                group_content['vars']['GO_EXTRA'] = go_version
                if go_previous:
                    group_content['vars'][go_previous_var] = go_latest
                update_streams = update_group = True
        # save changes and create pr
        if update_streams:
            if self.dry_run:
                _LOGGER.info(f"[DRY RUN] Would have created PR to update {go_version} golang builders")
                return
            if self.skip_pr:
                # Log NVRs and pullspecs for golang builder images
                builder_info = []
                for el_v, pullspec in builder_pullspecs.items():
                    builder_info.append(f"  - RHEL {el_v}: {pullspec}")
                builder_details = "\n".join(builder_info)

                _LOGGER.info(
                    f"Skipping PR creation (--skip-pr flag set) for {go_version} golang builders update.\n"
                    f"Golang builder images:\n{builder_details}"
                )
                await self._slack_client.say_in_thread(
                    f"Skipping PR creation (--skip-pr flag set) for {go_version} golang builders update.\n"
                    f"Golang builder images:\n{builder_details}"
                )
                return
            fork_repo = get_github_client_for_org("openshift-bot").get_repo("openshift-bot/ocp-build-data")
            branch_name = f"update-golang-{self.ocp_version}-{go_version}"
            title = f"{self.art_jira} - Bump {self.ocp_version} golang builders to {go_version}"
            update_message = f"Bump {self.ocp_version} golang builders to {go_version}"
            for fork_branch in fork_repo.get_branches():
                if fork_branch.name == branch_name:
                    fork_repo.get_git_ref(f"heads/{branch_name}").delete()
            fork_branch = fork_repo.create_git_ref(
                f"refs/heads/{branch_name}", upstream_repo.get_branch(branch).commit.sha
            )
            _LOGGER.info(f"Created fork branch ref {fork_branch.ref}")
            output = io.BytesIO()
            yaml.dump(streams_content, output)
            output.seek(0)
            fork_file = fork_repo.get_contents("streams.yml", ref=branch_name)
            fork_repo.update_file("streams.yml", update_message, output.read(), fork_file.sha, branch=branch_name)
            if update_group:
                output = io.BytesIO()
                yaml.dump(group_content, output)
                output.seek(0)
                fork_file = fork_repo.get_contents("group.yml", ref=branch_name)
                fork_repo.update_file("group.yml", update_message, output.read(), fork_file.sha, branch=branch_name)
            # create pr
            build_url = jenkins.get_build_url()
            body = f"Created by job run {build_url}" if build_url else ""
            pr = upstream_repo.create_pull(title=title, body=body, base=branch, head=f"openshift-bot:{branch_name}")
            _LOGGER.info(
                f"PR created {pr.html_url} for {branch_name} to bump {self.ocp_version} golang builders to {go_version}"
            )
            await self._slack_client.say_in_thread(
                f"PR created {pr.html_url} for {branch_name} to bump {self.ocp_version} golang builders to {go_version}"
            )
        else:
            if self.tag_builds:
                await self._slack_client.say_in_thread(
                    "No pr created, please double check if it's expected because new build get tagged."
                )
            else:
                _LOGGER.info(f"No update needed in {branch}")

    async def _rebase_brew(self, el_v, go_version):
        _LOGGER.info("Rebasing for Brew...")
        branch = self.get_golang_branch(el_v, go_version)
        if self.data_gitref:
            branch += f'@{self.data_gitref}'
        version = f"v{go_version}"
        release = default_release_suffix()
        cmd = [
            "doozer",
            f"--working-dir={self._doozer_working_dir}-brew-{el_v}",
            "--build-system=brew",
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")
        cmd.extend(
            [
                "--group",
                branch,
                "-i",
                GOLANG_BUILDER_IMAGE_NAME,
                "images:rebase",
                "--version",
                version,
                "--release",
                release,
                "--message",
                f"bumping to {version}-{release}",
            ]
        )
        if not self.dry_run:
            cmd.append("--push")
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _build_brew(self, el_v, go_version):
        _LOGGER.info("Building on Brew...")
        branch = self.get_golang_branch(el_v, go_version)
        if self.data_gitref:
            branch += f'@{self.data_gitref}'
        cmd = [
            "doozer",
            f"--working-dir={self._doozer_working_dir}-brew-{el_v}",
            "--build-system=brew",
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")
        cmd.extend(
            [
                "--group",
                branch,
                "-i",
                GOLANG_BUILDER_IMAGE_NAME,
                "images:build",
                "--repo-type",
                "unsigned",
                "--push-to-defaults",
            ]
        )
        if self.dry_run:
            cmd.append("--dry-run")
        if self.scratch:
            cmd.append("--scratch")
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _rebase_and_build_brew(self, el_v, go_version):
        await self._rebase_brew(el_v, go_version)
        await self._build_brew(el_v, go_version)

    async def _rebase_konflux(self, el_v, go_version):
        """Rebase golang-builder image for Konflux"""
        _LOGGER.info("Rebasing for Konflux...")
        branch = self.get_golang_branch(el_v, go_version)
        if self.data_gitref:
            branch += f'@{self.data_gitref}'
        version = f"v{go_version}"
        release = default_release_suffix()
        cmd = [
            "doozer",
            f"--working-dir={self._doozer_working_dir}-konflux-{el_v}",
            "--build-system=konflux",
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")
        cmd.extend(
            [
                "--group",
                branch,
                "-i",
                GOLANG_BUILDER_IMAGE_NAME,
                "beta:images:konflux:rebase",
                "--version",
                version,
                "--release",
                release,
                "--message",
                f"bumping to {version}-{release}",
            ]
        )
        if self.network_mode:
            cmd.extend(["--network-mode", self.network_mode])
        if not self.dry_run:
            cmd.append("--push")
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _build_konflux(self, el_v, go_version):
        """Build golang-builder image on Konflux"""
        _LOGGER.info("Building on Konflux...")
        branch = self.get_golang_branch(el_v, go_version)
        if self.data_gitref:
            branch += f'@{self.data_gitref}'
        konflux_namespace = PRODUCT_NAMESPACE_MAP["ocp"]
        cmd = [
            "doozer",
            f"--working-dir={self._doozer_working_dir}-konflux-{el_v}",
            "--build-system=konflux",
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")
        cmd.extend(
            [
                "--group",
                branch,
                "-i",
                GOLANG_BUILDER_IMAGE_NAME,
                "beta:images:konflux:build",
                f"--konflux-namespace={konflux_namespace}",
                "--skip-ec-verify",
            ]
        )
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
        if self.network_mode:
            cmd.extend(["--network-mode", self.network_mode])
        if self.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _rebase_and_build_konflux(self, el_v, go_version):
        """Rebase and build golang-builder image on Konflux"""
        await self._rebase_konflux(el_v, go_version)
        await self._build_konflux(el_v, go_version)

    @staticmethod
    def get_golang_branch(el_v, go_version):
        major_go, minor_go, _ = go_version.split('.')
        go_v = f"{major_go}.{minor_go}"
        return f'rhel-{el_v}-golang-{go_v}'

    def verify_golang_builder_repo(self, el_v, go_version):
        branch = self.get_golang_branch(el_v, go_version)
        filename = 'group.yml'

        repo = get_github_client_for_org("openshift-eng").get_repo("openshift-eng/ocp-build-data")
        content = repo.get_contents(filename, ref=branch)
        group_config = yaml.load(content.decoded_content)
        content_repo_url_suffix = self.get_content_repo_url_suffix(el_v)

        golang_repo = f'rhel-{el_v}-golang-rpms'
        if golang_repo not in group_config['repos']:
            raise ValueError(
                f"Did not find {golang_repo} defined at "
                f"https://github.com/openshift-eng/ocp-build-data/blob/{branch}/{filename}. If it's with a different "
                "name please correct it."
            )

        major, minor = group_config['vars']['MAJOR'], group_config['vars']['MINOR']
        err = False
        for arch, template_url in group_config['repos'][golang_repo]['conf']['baseurl'].items():
            expected_suffix = f'{content_repo_url_suffix}/{arch}/'
            actual_url = template_url.format(MAJOR=major, MINOR=minor)
            if not actual_url.endswith(expected_suffix):
                err = True
                _LOGGER.error(f"{expected_suffix} not found in URL {actual_url}")

        if err:
            raise ValueError(
                f"Content repo URLs for {golang_repo} in {branch} do not look correct. Wrong MAJOR/MINOR vars?"
            )

        _LOGGER.info(f"Builder branch {branch} has the expected content set urls")

    def get_content_repo_url_suffix(self, el_v):
        url = '/brewroot/repos/{repo}/latest'
        return url.format(repo=f'rhaos-{self.ocp_version}-rhel-{el_v}-build')

    def get_module_tag(self, nvr, el_v) -> str:
        tags = [t['name'] for t in self.koji_session.listTags(build=nvr)]
        prefix = f'module-go-toolset-rhel{el_v}-'
        return next((t for t in tags if t.startswith(prefix) and not t.endswith('-build')), None)


@cli.command('update-golang')
@click.option('--ocp-version', required=True, help='OCP version to update golang for, e.g. 4.16')
@click.option('--scratch', is_flag=True, default=False, help='Build images in scratch mode')
@click.option('--art-jira', required=True, help='Related ART Jira ticket e.g. ART-1234')
@click.option(
    '--cves',
    help='CVEs that are confirmed to be fixed in all given golang nvrs (comma separated). '
    'This will be used to fetch relevant Tracker bugs and move them to ON_QA state if '
    'determined to be fixed (nightly is found containing fixed builds). e.g. CVE-2024-1234',
)
@click.option(
    '--force-update-tracker',
    is_flag=True,
    default=False,
    help='Force update found tracker bugs for the given CVEs, even if the latest nightly is not found containing fixed builds',
)
@click.option('--confirm', is_flag=True, default=False, help='Confirm to proceed with rebase and build')
@click.option(
    '--tag-builds', is_flag=True, default=False, help='Confirm to tag builds with override tag if they are not tagged'
)
@click.argument('go_nvrs', metavar='GO_NVRS...', nargs=-1, required=True)
@click.option(
    '--force-image-build', is_flag=True, default=False, help='Rebuild golang builder image regardless of if one exists'
)
@click.option(
    '--build-system',
    type=click.Choice(['brew', 'konflux', 'both'], case_sensitive=False),
    default='brew',
    help='Build system to use for golang-builder images (brew, konflux, or both). Defaults to brew for backward compatibility.',
)
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--skip-pr',
    is_flag=True,
    default=False,
    help='Skip PR generation for ocp-build-data updates. Defaults to False (PRs will be created).',
)
@click.option(
    '--external-golang-rpms',
    is_flag=True,
    default=False,
    help='Use golang RPMs from external repos (not tagged in rhaos build tags). Skips tagging and availability checks. Defaults to False.',
)
@click.option(
    '--network-mode',
    type=click.Choice(['hermetic', 'internal-only', 'open']),
    default=None,
    help='Override network mode for Konflux builds. Takes precedence over image and group config settings.',
)
@pass_runtime
@click_coroutine
async def update_golang(
    runtime: Runtime,
    ocp_version: str,
    scratch: bool,
    art_jira: str,
    cves: str,
    force_update_tracker: bool,
    confirm: bool,
    tag_builds: bool,
    go_nvrs: List[str],
    force_image_build: bool,
    build_system: str,
    kubeconfig: str,
    data_path: str,
    data_gitref: str,
    skip_pr: bool,
    external_golang_rpms: bool,
    network_mode: str | None,
):
    if not runtime.dry_run and not confirm:
        _LOGGER.info('--confirm is not set, running in dry-run mode')
        runtime.dry_run = True
    cves_list = cves.split(',') if cves else None
    if force_update_tracker and not cves_list:
        raise ValueError('CVEs must be provided with --force-update-tracker')
    if network_mode and build_system == 'brew':
        raise click.BadParameter('--network-mode only applies when --build-system is "konflux" or "both".')

    await UpdateGolangPipeline(
        runtime,
        ocp_version,
        cves_list,
        force_update_tracker,
        go_nvrs,
        art_jira,
        tag_builds,
        scratch,
        force_image_build,
        build_system,
        kubeconfig,
        data_path,
        data_gitref,
        skip_pr,
        external_golang_rpms,
        network_mode,
    ).run()
