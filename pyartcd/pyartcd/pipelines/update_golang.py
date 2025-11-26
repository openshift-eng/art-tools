import asyncio
import base64
import io
import logging
import os
import re
from datetime import datetime, timezone
from typing import List, cast

import click
import koji
from artcommonlib import exectools
from artcommonlib.brew import BuildStates
from artcommonlib.constants import BREW_HUB, GOLANG_BUILDER_IMAGE_NAME, PRODUCT_NAMESPACE_MAP
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.release_util import split_el_suffix_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from doozerlib.brew import watch_task_async
from elliottlib import util as elliottutil
from elliottlib.constants import GOLANG_BUILDER_CVE_COMPONENT
from ghapi.all import GhApi
from github import Github, GithubException
from ruamel.yaml import YAML

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import GITHUB_OWNER
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()


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
    if major != 4:
        raise ValueError(f'Only OCP major version 4 is supported, found: {major}')
    if minor < 12:
        raise ValueError(f'Only OCP 4.12+ is supported, found: {ocp_version}')

    # only rhel 8 and 9 are supported 4.12 onwards
    supported_els = {8, 9}

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


async def move_golang_bugs(
    ocp_version: str,
    cves: list[str] | None = None,
    nvrs: list[str] | None = None,
    components: list[str] | None = None,
    force_update_tracker: bool = False,
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
        self._slack_client = self.runtime.new_slack_client()
        self._doozer_working_dir = self.runtime.working_dir / "doozer-working"
        self._doozer_env_vars = os.environ.copy()

        # Get kubeconfig from environment variable if not provided
        if not kubeconfig:
            kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
        self.kubeconfig = kubeconfig

        self.github_token = os.environ.get('GITHUB_TOKEN')
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to fetch build data repo contents")

        # Initialize KonfluxDb for Konflux build system
        if build_system in ('konflux', 'both'):
            self.konflux_db = KonfluxDb()
            self.konflux_db.bind(KonfluxBuildRecord)

    async def run(self):
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(self.ocp_version, self.go_nvrs)
        _LOGGER.info(f'Golang version detected: {go_version}')
        _LOGGER.info(f'NVRs by rhel version: {el_nvr_map}')
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
        brew_nvrs = {}
        konflux_nvrs = {}
        if not self.force_image_build:
            if self.build_system in ['both', 'brew']:
                brew_nvrs = self.get_existing_builders(el_nvr_map, go_version)
            if self.build_system in ['both', 'konflux']:
                konflux_nvrs = await self.get_existing_builders_konflux(el_nvr_map, go_version)

        # Determine which rhel versions need builds
        brew_missing = el_nvr_map.keys() - brew_nvrs.keys() if self.build_system in ['both', 'brew'] else set()
        konflux_missing = el_nvr_map.keys() - konflux_nvrs.keys() if self.build_system in ['both', 'konflux'] else set()

        if brew_missing or konflux_missing:
            # FIXME: yuxzhu: already broken
            # for el_v in missing_in:
            #     self.verify_golang_builder_repo(el_v, go_version)

            # Rebase and build missing images
            build_tasks = []
            if self.build_system in ['both', 'brew'] and brew_missing:
                build_tasks.extend([self._rebase_and_build_brew(el_v, go_version) for el_v in brew_missing])
            if self.build_system in ['both', 'konflux'] and konflux_missing:
                build_tasks.extend([self._rebase_and_build_konflux(el_v, go_version) for el_v in konflux_missing])
            if build_tasks:
                await asyncio.gather(*build_tasks)

            # Now all builders should be available, try to fetch again
            if self.build_system in ['both', 'brew']:
                brew_nvrs = self.get_existing_builders(el_nvr_map, go_version)
            if self.build_system in ['both', 'konflux']:
                konflux_nvrs = await self.get_existing_builders_konflux(el_nvr_map, go_version)

            # Check if we still have missing builds
            brew_still_missing = (
                el_nvr_map.keys() - brew_nvrs.keys() if self.build_system in ['both', 'brew'] else set()
            )
            konflux_still_missing = (
                el_nvr_map.keys() - konflux_nvrs.keys() if self.build_system in ['both', 'konflux'] else set()
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

        _LOGGER.info("Updating streams.yml with found builder images")
        # Prepare message with all newly built builders
        all_builder_messages = []
        if brew_nvrs:
            for el_v, nvr in brew_nvrs.items():
                parsed_nvr = parse_nvr(nvr)
                pullspec = self._get_builder_pullspec(parsed_nvr, 'brew')
                all_builder_messages.append(f"RHEL {el_v}: {nvr} -> {pullspec} (brew)")
        if konflux_nvrs:
            for el_v, nvr in konflux_nvrs.items():
                parsed_nvr = parse_nvr(nvr)
                pullspec = self._get_builder_pullspec(parsed_nvr, 'konflux')
                all_builder_messages.append(f"RHEL {el_v}: {nvr} -> {pullspec} (konflux)")

        builder_message = "New golang builders available:\n" + "\n".join([f"  - {msg}" for msg in all_builder_messages])
        _LOGGER.info(builder_message)
        await self._slack_client.say_in_thread(builder_message)

        # For 'both' mode, only update streams.yml with konflux builds
        # For 'brew' mode, use brew_nvrs; for 'konflux' mode, use konflux_nvrs
        if self.build_system == 'both':
            builder_nvrs = konflux_nvrs
        elif self.build_system == 'brew':
            builder_nvrs = brew_nvrs
        else:  # konflux
            builder_nvrs = konflux_nvrs

        await self.update_golang_streams(go_version, builder_nvrs)

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
        go_nvr_map = elliottutil.get_golang_container_nvrs_for_konflux_record(found_records.values(), _LOGGER)
        for builder_go_vr, nvrs in go_nvr_map.items():
            for el_v, go_nvr in el_nvr_map.items():
                if builder_go_vr in go_nvr:
                    for nvr in nvrs:
                        nvr_str = f"{nvr[0]}-{nvr[1]}-{nvr[2]}"
                        _LOGGER.info(f"Found existing builder image in Konflux: {nvr_str} built with {go_nvr}")
                        builder_nvrs[el_v] = nvr_str
        return builder_nvrs

    def _get_builder_pullspec(self, parsed_nvr, build_system: str):
        """Generate the complete pullspec based on build system"""
        if build_system == 'brew':
            return f'openshift/golang-builder:{parsed_nvr["version"]}-{parsed_nvr["release"]}'
        else:  # konflux or both (both uses konflux pullspec)
            # TODO: This is temporary. In the future we need a location to share with multiple teams.
            return f'quay.io/redhat-user-workloads/ocp-art-tenant/art-images:golang-builder-{parsed_nvr["version"]}-{parsed_nvr["release"]}'

    async def update_golang_streams(self, go_version, builder_nvrs):
        """
        Update golang builders for current release in ocp-build-data
        1. First check go verion from group.yml var to decide if it's a major version bump or minor version bump
        2. Get the golang image value, find and replace for each item in streams.yml
        3. If it'a major version bump, also need to update key in streams.yml and vars in group.yml
        4. Create pr to update changes
        """
        github_client = Github(os.environ.get("GITHUB_TOKEN"))
        branch = f"openshift-{self.ocp_version}"
        upstream_repo = github_client.get_repo("openshift-eng/ocp-build-data")
        streams_content = yaml.load(upstream_repo.get_contents("streams.yml", ref=branch).decoded_content)
        group_content = yaml.load(upstream_repo.get_contents("group.yml", ref=branch).decoded_content)

        go_latest_var, go_previous_var = "GO_LATEST", "GO_PREVIOUS"
        go_latest = group_content['vars'][go_latest_var]
        go_previous = group_content['vars'].get(go_previous_var, None)

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
        if go_latest in go_version:
            for el_v, builder_nvr in builder_nvrs.items():
                parsed_nvr = parse_nvr(builder_nvr)

                _LOGGER.info("Looking for golang stream %s in streams.yml", latest_go_stream_name(el_v))
                latest_go = get_stream(latest_go_stream_name(el_v))['image']

                new_latest_go = self._get_builder_pullspec(parsed_nvr, self.build_system)
                for _, info in streams_content.items():
                    if info['image'] == latest_go:
                        info['image'] = new_latest_go
                        update_streams = True
        # This is to bump minor golang for GO_PREVIOUS
        elif go_previous in go_version:
            for el_v, builder_nvr in builder_nvrs.items():
                parsed_nvr = parse_nvr(builder_nvr)

                _LOGGER.info("Looking for golang stream %s in streams.yml", previous_go_stream_name(el_v))
                previous_go = get_stream(previous_go_stream_name(el_v))['image']

                new_previous_go = self._get_builder_pullspec(parsed_nvr, self.build_system)
                for _, info in streams_content.items():
                    if info['image'] == previous_go:
                        info['image'] = new_previous_go
                        update_streams = True
        # This is to bump major golang for GO_LATEST and update GO_PREVIOUS to current GO_LATEST
        elif go_version.split('.')[0] >= go_latest.split('.')[0] and go_version.split('.')[1] > go_latest.split('.')[1]:
            for el_v, builder_nvr in builder_nvrs.items():
                parsed_nvr = parse_nvr(builder_nvr)

                _LOGGER.info("Looking for golang stream %s in streams.yml", latest_go_stream_name(el_v))
                latest_go = get_stream(latest_go_stream_name(el_v))['image']

                _LOGGER.info("Looking for golang stream %s in streams.yml", previous_go_stream_name(el_v))
                previous_go = get_stream(previous_go_stream_name(el_v))['image'] if go_previous else None

                new_latest_go = self._get_builder_pullspec(parsed_nvr, self.build_system)
                for _, info in streams_content.items():
                    if info['image'] == latest_go:
                        info['image'] = new_latest_go
                    if info['image'] == previous_go:
                        info['image'] = latest_go
                group_content['vars']['GO_LATEST'] = go_version
                group_content['vars']['GO_EXTRA'] = go_version
                if go_previous:
                    group_content['vars']['GO_PREVIOUS'] = go_latest
                update_streams = update_group = True
        # save changes and create pr
        if update_streams:
            if self.dry_run:
                _LOGGER.info(f"[DRY RUN] Would have created PR to update {go_version} golang builders")
                return
            if self.skip_pr:
                # Log NVRs and pullspecs for golang builder images
                builder_info = []
                for el_v, nvr in builder_nvrs.items():
                    parsed_nvr = parse_nvr(nvr)
                    pullspec = self._get_builder_pullspec(parsed_nvr, self.build_system)
                    builder_info.append(f"  - RHEL {el_v}: {nvr} -> {pullspec}")
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
            fork_repo = github_client.get_repo("openshift-bot/ocp-build-data")
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
        release = datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M')
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
        release = datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M')
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
                "beta:images:konflux:rebase",
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
                "beta:images:konflux:build",
                f"--konflux-namespace={konflux_namespace}",
            ]
        )
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
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
        # FIXME: yuxzhu: this is already broken
        # read group.yml from the golang branch using ghapi
        owner, repo = 'openshift-eng', 'ocp-build-data'
        branch = self.get_golang_branch(el_v, go_version)
        filename = 'group.yml'

        api = GhApi(owner=owner, repo=repo, token=self.github_token)
        blob = api.repos.get_content(filename, ref=branch)
        group_config = yaml.load(base64.b64decode(blob['content']))
        content_repo_url = self.get_content_repo_url(el_v)

        golang_repo = f'rhel-{el_v}-golang-rpms'
        if golang_repo not in group_config['repos']:
            raise ValueError(
                f"Did not find {golang_repo} defined at "
                f"https://github.com/{owner}/{repo}/blob/{branch}/{filename}. If it's with a different "
                "name please correct it."
            )

        expected = {
            arch: f'{content_repo_url}/{arch}/' for arch in group_config['repos'][golang_repo]['conf']['baseurl'].keys()
        }

        major, minor = group_config['vars']['MAJOR'], group_config['vars']['MINOR']
        actual = {
            arch: val.format(MAJOR=major, MINOR=minor)
            for arch, val in group_config['repos'][golang_repo]['conf']['baseurl'].items()
        }

        if expected != actual:
            raise ValueError(
                f"Did not find repo {golang_repo} to have the expected urls. \nexpected={expected}\nactual={actual}"
            )

        _LOGGER.info(f"Builder branch {branch} has the expected content set urls")

    def get_content_repo_url(self, el_v):
        url = 'https://download-node-02.eng.bos.redhat.com/brewroot/repos/{repo}/latest'
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
):
    if not runtime.dry_run and not confirm:
        _LOGGER.info('--confirm is not set, running in dry-run mode')
        runtime.dry_run = True
    cves_list = cves.split(',') if cves else None
    if force_update_tracker and not cves_list:
        raise ValueError('CVEs must be provided with --force-update-tracker')

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
    ).run()
