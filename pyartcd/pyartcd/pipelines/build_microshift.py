import asyncio
import copy
import io
import logging
import os
import re
import traceback
from collections import namedtuple
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import click
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.assembly import AssemblyTypes, assembly_config_struct
from artcommonlib.model import Model
from artcommonlib.util import get_ocp_version_from_group, new_roundtrip_yaml_handler
from doozerlib.util import isolate_nightly_name_components
from elliottlib.errata import push_cdn_stage
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.util import get_advisory_boilerplate
from errata_tool import Erratum
from ghapi.all import GhApi
from github import Github, GithubException
from semver import VersionInfo
from tenacity import retry, stop_after_attempt, wait_fixed

from pyartcd import constants, jenkins, oc, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime
from pyartcd.util import (
    default_release_suffix,
    get_assembly_type,
    get_microshift_builds,
    get_release_name_for_assembly,
    isolate_el_version_in_release,
    load_group_config,
    load_releases_config,
)

yaml = new_roundtrip_yaml_handler()


class BuildMicroShiftPipeline:
    """Rebase and build MicroShift for an assembly"""

    SUPPORTED_ASSEMBLY_TYPES = {
        AssemblyTypes.STANDARD,
        AssemblyTypes.CANDIDATE,
        AssemblyTypes.PREVIEW,
        AssemblyTypes.STREAM,
        AssemblyTypes.CUSTOM,
    }

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        payloads: Tuple[str, ...],
        no_rebase: bool,
        force: bool,
        skip_prepare_advisory: bool,
        data_path: str,
        slack_client,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.assembly_type = AssemblyTypes.STREAM
        self.payloads = payloads
        self.no_rebase = no_rebase
        self.force = force
        self.skip_prepare_advisory = skip_prepare_advisory
        self._logger = logger or runtime.logger
        self._working_dir = self.runtime.working_dir.absolute()
        self.releases_config = None
        self.advisory_num = None
        self.slack_client = slack_client
        self.github_client = Github(os.environ.get("GITHUB_TOKEN"))
        # determines OCP version
        self._ocp_version = get_ocp_version_from_group(group)

        # sets environment variables for Elliott and Doozer
        self._elliott_env_vars = os.environ.copy()
        self._elliott_env_vars["ELLIOTT_WORKING_DIR"] = str(self._working_dir / "elliott-working")
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._working_dir / "doozer-working")

        if not data_path:
            data_path = self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
        if data_path:
            self._doozer_env_vars["DOOZER_DATA_PATH"] = data_path
            self._elliott_env_vars["ELLIOTT_DATA_PATH"] = data_path

    async def run(self):
        # Make sure our api.ci token is fresh
        await oc.registry_login(self.runtime)

        group_config = await load_group_config(self.group, self.assembly, env=self._doozer_env_vars)
        advisories = group_config.get("advisories", {})
        self.releases_config = await load_releases_config(
            group=self.group,
            data_path=self._doozer_env_vars.get("DOOZER_DATA_PATH", None) or constants.OCP_BUILD_DATA_URL,
        )
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
        if self.assembly_type not in self.SUPPORTED_ASSEMBLY_TYPES:
            raise ValueError(
                f"Building MicroShift for assembly type {self.assembly_type.value} is not currently supported."
            )

        if self.assembly_type is AssemblyTypes.STREAM:
            await self._rebase_and_build_for_stream()
        else:
            # Check if microshift advisory is defined in assembly
            if ('microshift' not in advisories or advisories.get("microshift") <= 0) and not self.skip_prepare_advisory:
                self.advisory_num = await self.create_microshift_advisory()
            await self._rebase_and_build_for_named_assembly()
            await self._trigger_microshift_sync()
            await self._trigger_build_microshift_bootc()
            if not self.skip_prepare_advisory:
                await self._prepare_advisory(self.advisory_num if self.advisory_num else advisories['microshift'])

    def load_errata_config(self, group: str, data_path: str = constants.OCP_BUILD_DATA_URL):
        return yaml.load(self.get_file_from_branch(group, "erratatool.yml", data_path))

    def get_file_from_branch(self, branch, filename, data_path=None):
        if data_path is None:
            data_path = self._doozer_env_vars["DOOZER_DATA_PATH"]
        try:
            user, repo = self.extract_git_repo(data_path)
            upstream_repo = self.github_client.get_repo(f"{user}/{repo}")
            et_content = upstream_repo.get_contents(filename, ref=branch)
        except GithubException as e:
            raise ValueError(f"Can't load file contents from {data_path}: {e}")
        return et_content.decoded_content

    def extract_git_repo(self, data_path: str):
        """
        extract git repo name from data path
        https://github.com/openshift-eng/ocp-build-data --> openshift-eng, ocp-build-data
        """
        data_path = data_path.rstrip(".git")
        parts = data_path.rstrip("/").split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid git repo URL: {data_path}")
        return parts[-2], parts[-1]

    async def create_microshift_advisory(self) -> int:
        release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        release_version = VersionInfo.parse(release_name)
        advisory_type = "RHEA" if release_version.patch == 0 else "RHBA"
        release_date = assembly_config_struct(Model(self.releases_config), self.assembly, "group", {}).get(
            "release_date"
        )
        et_data = self.load_errata_config(self.group, self._doozer_env_vars["DOOZER_DATA_PATH"])
        boilerplate = get_advisory_boilerplate(
            runtime=self, et_data=et_data, art_advisory_key="microshift", errata_type=advisory_type
        )
        errata_api = AsyncErrataAPI()

        # Format the advisory boilerplate
        synopsis = boilerplate['synopsis'].format(MINOR=release_version.minor, PATCH=release_version.patch)
        advisory_topic = boilerplate['topic'].format(MINOR=release_version.minor, PATCH=release_version.patch)
        advisory_description = boilerplate['description'].format(
            MINOR=release_version.minor, PATCH=release_version.patch
        )
        advisory_solution = boilerplate['solution'].format(MINOR=release_version.minor, PATCH=release_version.patch)

        self._logger.info("Creating advisory with type %s art_advisory_key microshift ...", advisory_type)
        if self.runtime.dry_run:
            self._logger.info("[DRY-RUN] Would have created microshift %s advisory 0", advisory_type)
            self._logger.info("[DRY-RUN] Synopsis: %s", synopsis)
            self._logger.info("[DRY-RUN] Topic: %s", advisory_topic)
            self._logger.info("[DRY-RUN] Description: %s", advisory_description)
            self._logger.info("[DRY-RUN] Solution: %s", advisory_solution)
            return 0
        try:
            created_advisory = await errata_api.create_advisory(
                product=et_data['product'],
                release=boilerplate.get('release', et_data['release']),
                errata_type=advisory_type,
                advisory_synopsis=synopsis,
                advisory_topic=advisory_topic,
                advisory_description=advisory_description,
                advisory_solution=advisory_solution,
                advisory_quality_responsibility_name=et_data['quality_responsibility_name'],
                advisory_package_owner_email=self.runtime.config['advisory']['package_owner'],
                advisory_manager_email=self.runtime.config['advisory']['manager'],
                advisory_assigned_to_email=self.runtime.config['advisory']['assigned_to'],
                advisory_publish_date_override=release_date,
                batch_id=0,
            )
            advisory_info = next(iter(created_advisory["errata"].values()))
            advisory_id = advisory_info["id"]
            await errata_api.request_liveid(advisory_id)
            self._logger.info(f"Created microshift advisory {advisory_id}")
        finally:
            await errata_api.close()
        return advisory_id

    async def _rebase_and_build_for_stream(self):
        # Do a sanity check
        if self.assembly_type != AssemblyTypes.STREAM:
            raise ValueError(f"Cannot process assembly type {self.assembly_type.value}")

        major, minor = self._ocp_version
        # rebase against nightlies
        # rpm version-release will be like `4.12.0~test-202201010000.p?`
        if self.no_rebase:
            # Without knowing the nightly name, it is hard to determine rpm version-release.
            raise ValueError("--no-rebase is not supported to build against assembly stream.")

        if not self.payloads:
            raise ValueError("Release payloads must be specified to rebase against assembly stream.")

        payload_infos = await self.parse_release_payloads(self.payloads)
        if "x86_64" not in payload_infos or "aarch64" not in payload_infos:
            raise ValueError("x86_64 payload and aarch64 payload are required for rebasing microshift.")

        for info in payload_infos.values():
            payload_version = VersionInfo.parse(info["version"])
            if (payload_version.major, payload_version.minor) != (major, minor):
                raise ValueError(
                    f"Specified payload {info['pullspec']} does not match group major.minor {major}.{minor}: {payload_version}"
                )

        # use the version of the x86_64 payload to generate the rpm version-release.
        release_name = payload_infos["x86_64"]["version"]
        custom_payloads = payload_infos

        # Rebase and build microshift
        version, release = self.generate_microshift_version_release(release_name)

        try:
            await self._rebase_and_build_rpm(version, release, custom_payloads)
        except Exception as build_err:
            self._logger.error(build_err)
            # Send a message to #microshift-alerts for STREAM failures
            await self._notify_microshift_alerts(f"{version}-{release}")
            raise

    async def _rebase_and_build_for_named_assembly(self):
        # Do a sanity check
        if self.assembly_type == AssemblyTypes.STREAM:
            raise ValueError(f"Cannot process assembly type {self.assembly_type.value}")

        # For named assemblies, check if builds are pinned or already exist
        nvrs = []
        pinned_nvrs = dict()

        if self.payloads:
            raise ValueError(f"Specifying payloads for assembly type {self.assembly_type.value} is not allowed.")

        release_name = util.get_release_name_for_assembly(self.group, self.releases_config, self.assembly)

        await self.slack_client.say_in_thread(
            f":construction: Microshift prep for assembly {self.assembly} :construction:"
        )

        if not self.force:
            pinned_nvrs = util.get_rpm_if_pinned_directly(self.releases_config, self.assembly, 'microshift')
            if pinned_nvrs:
                message = (
                    f"For assembly {self.assembly} builds are already pinned: {pinned_nvrs}. Use FORCE to rebuild."
                )
                self._logger.info(message)
                await self.slack_client.say_in_thread(message)
                nvrs = list(pinned_nvrs.values())
            else:
                nvrs = await get_microshift_builds(self.group, self.assembly, env=self._elliott_env_vars)

        if nvrs:
            self._logger.info("Builds already exist: %s", nvrs)
        else:
            # Rebase and build microshift
            version, release = self.generate_microshift_version_release(release_name)
            nvrs = await self._rebase_and_build_rpm(version, release, custom_payloads=None)
            message = f"microshift for assembly {self.assembly} has been successfully built."
            await self.slack_client.say_in_thread(message)

        # Check if we need create a PR to pin eligible builds
        diff = set(nvrs) - set(pinned_nvrs.values())
        if diff:
            self._logger.info("Creating PR to pin microshift build: %s", diff)
            pr = await self._create_or_update_pull_request(nvrs)
            message = f"PR to pin microshift build to the {self.assembly} assembly has been merged: {pr}"
            await self.slack_client.say_in_thread(message)

    async def _trigger_microshift_sync(self):
        if self.assembly_type not in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            return

        major, minor = self._ocp_version
        version = f'{major}.{minor}'
        try:
            jenkins.start_microshift_sync(version=version, assembly=self.assembly, dry_run=self.runtime.dry_run)
            message = (
                f"microshift_sync for version {version} and assembly {self.assembly} has been triggered\n"
                f"This will publish the microshift build to mirror"
            )
            await self.slack_client.say_in_thread(message)
        except Exception as err:
            self._logger.error("Failed to trigger microshift_sync job: %s", err)
            message = (
                f"@release-artists Please start <{constants.JENKINS_UI_URL}"
                "/job/aos-cd-builds/job/build%252Fmicroshift_sync|microshift sync> manually."
            )
            await self.slack_client.say_in_thread(message)

    async def _trigger_build_microshift_bootc(self):
        if self.assembly_type not in [AssemblyTypes.STANDARD, AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            return

        if self._ocp_version < (4, 18):
            self._logger.info("Skipping build-microshift-bootc job for OCP version < 4.18")
            return

        major, minor = self._ocp_version
        version = f'{major}.{minor}'
        try:
            jenkins.start_build_microshift_bootc(version=version, assembly=self.assembly, dry_run=self.runtime.dry_run)
            message = (
                f"build_microshift_bootc for version {version} and assembly {self.assembly} has been triggered\n"
                f"This will build microshift-bootc and publish it's pullspec to mirror"
            )
            await self.slack_client.say_in_thread(message)
        except Exception as err:
            self._logger.error("Failed to trigger build-microshift-bootc job: %s", err)
            message = (
                f"@release-artists Please start <{constants.JENKINS_UI_URL}"
                "/job/aos-cd-builds/job/build%252Fbuild-microshift-bootc|build-microshift-bootc> manually."
            )
            await self.slack_client.say_in_thread(message)

    async def _prepare_advisory(self, microshift_advisory_id):
        await self.slack_client.say_in_thread(f"Start preparing microshift advisory for assembly {self.assembly}..")
        await self._attach_builds()
        await self._sweep_bugs()
        await self._attach_cve_flaws()
        await self._change_advisory_status(microshift_advisory_id)
        await self._verify_microshift_bugs(microshift_advisory_id)
        await self.slack_client.say_in_thread("Completed preparing microshift advisory.")

    async def _attach_builds(self):
        """attach the microshift builds to advisory"""
        cmd = [
            "elliott",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "--rpms",
            "microshift",
            "find-builds",
            "-k",
            "rpm",
            "--member-only",
            "--use-default-advisory",
            "microshift",
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    async def _sweep_bugs(self):
        """sweep the microshift bugs to advisory"""
        cmd = [
            "elliott",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "find-bugs:sweep",
            "--permissive",  # this is so we don't error out on non-microshift bugs
            "--use-default-advisory",
            "microshift",
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    async def _attach_cve_flaws(self):
        """attach CVE flaws to advisory"""
        cmd = [
            "elliott",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "attach-cve-flaws",
            "--use-default-advisory",
            "microshift",
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)

    async def _verify_microshift_bugs(self, advisory_id):
        """verify attached bugs on microshift advisory"""
        cmd = [
            "elliott",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "verify-attached-bugs",
            "--verify-flaws",
            str(advisory_id),
        ]
        try:
            await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars)
        except ChildProcessError as err:
            self._logger.warning("Error verifying attached bugs: %s", err)
            if self.assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
                await self.slack_client.say_in_thread(
                    f"Attached bugs have some issues. Permitting since assembly is of type {self.assembly_type}"
                )
                await self.slack_client.say_in_thread(str(err))
            else:
                raise err

    # Advisory can have several pending checks, so retry it a few times
    @retry(reraise=True, stop=stop_after_attempt(5), wait=wait_fixed(1200))
    async def _change_advisory_status(self, advisory_id):
        if self.runtime.dry_run:
            self._logger.info("[DRY-RUN] Would have changed advisory to QE")
            return
        # move advisory status to QE
        e = Erratum(errata_id=advisory_id)
        if e.errata_state == "QE":
            self._logger.info(f"Errata {advisory_id} status already in QE")
        else:
            e.setState("QE")
            e.commit()
            self._logger.info(f"Changed {advisory_id} to QE")
            push_cdn_stage(advisory_id)

    @staticmethod
    async def parse_release_payloads(payloads: Iterable[str]):
        result = {}
        pullspecs = []
        for payload in payloads:
            if "/" not in payload:
                # Convert nightly name to pullspec
                # 4.12.0-0.nightly-2022-10-25-210451 ==> registry.ci.openshift.org/ocp/release:4.12.0-0.nightly-2022-10-25-210451
                _, brew_cpu_arch, _ = isolate_nightly_name_components(payload)
                pullspecs.append(constants.NIGHTLY_PAYLOAD_REPOS[brew_cpu_arch] + ":" + payload)
            else:
                # payload is a pullspec
                pullspecs.append(payload)
        payload_infos = await asyncio.gather(*(oc.get_release_image_info(pullspec) for pullspec in pullspecs))
        for info in payload_infos:
            arch = info["config"]["architecture"]
            brew_arch = brew_arch_for_go_arch(arch)
            version = info["metadata"]["version"]
            result[brew_arch] = {
                "version": version,
                "arch": arch,
                "pullspec": info["image"],
                "digest": info["digest"],
            }
        return result

    @staticmethod
    def generate_microshift_version_release(ocp_version: str):
        """Generate version and release strings for microshift builds
        Example version-releases:
        - 4.12.42-202210011234
        - 4.13.0~rc.4-202210011234
        """
        release_version = VersionInfo.parse(ocp_version)
        version = f"{release_version.major}.{release_version.minor}.{release_version.patch}"
        if release_version.prerelease is not None:
            version += f"~{release_version.prerelease.replace('-', '_')}"
        release = default_release_suffix()
        return version, release

    async def _rebase_and_build_rpm(
        self, version, release: str, custom_payloads: Optional[Dict[str, str]]
    ) -> List[str]:
        """Rebase and build RPM
        :param release: release field for rebase
        :return: NVRs
        """
        cmd = [
            "doozer",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "-r",
            "microshift",
            "rpms:rebase-and-build",
            "--version",
            version,
            "--release",
            release,
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        set_env = self._doozer_env_vars.copy()
        if custom_payloads:
            set_env["MICROSHIFT_PAYLOAD_X86_64"] = custom_payloads["x86_64"]["pullspec"]
            set_env["MICROSHIFT_PAYLOAD_AARCH64"] = custom_payloads["aarch64"]["pullspec"]
        if self.no_rebase:
            set_env["MICROSHIFT_NO_REBASE"] = "1"
        await exectools.cmd_assert_async(cmd, env=set_env)

        if self.runtime.dry_run:
            return [f"microshift-{version}-{release.replace('.p?', '.p0')}.el8"]

        # parse record.log
        with open(Path(self._doozer_env_vars["DOOZER_WORKING_DIR"]) / "record.log", "r") as file:
            record_log = parse_record_log(file)
            return record_log["build_rpm"][-1]["nvrs"].split(",")

    def _pin_nvrs(self, nvrs: List[str], releases_config) -> Dict:
        """Update releases.yml to pin the specified NVRs.
        Example:
            releases:
                4.11.7:
                    assembly:
                    members:
                        rpms:
                        - distgit_key: microshift
                        metadata:
                            is:
                                el8: microshift-4.11.7-202209300751.p0.g7ebffc3.assembly.4.11.7.el8
        """
        is_entry = {}
        dg_key = "microshift"
        for nvr in nvrs:
            el_version = isolate_el_version_in_release(nvr)
            assert el_version is not None
            is_entry[f"el{el_version}"] = nvr

        if self.advisory_num:
            releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {}).setdefault(
                "advisories", {}
            ).setdefault("microshift", self.advisory_num)
        rpms_entry = (
            releases_config["releases"][self.assembly]
            .setdefault("assembly", {})
            .setdefault("members", {})
            .setdefault("rpms", [])
        )
        microshift_entry = next(filter(lambda rpm: rpm.get("distgit_key") == dg_key, rpms_entry), None)
        if microshift_entry is None:
            microshift_entry = {"distgit_key": dg_key, "why": "Pin microshift to assembly"}
            rpms_entry.append(microshift_entry)
        microshift_entry.setdefault("metadata", {})["is"] = is_entry
        return microshift_entry

    async def _create_or_update_pull_request(self, nvrs: List[str]):
        branch = f"auto-pin-microshift-{self.group}-{self.assembly}"
        title = f"Pin microshift build for {self.group} {self.assembly}"
        body = f"Created by job run {jenkins.get_build_url()}"
        if self.runtime.dry_run:
            self._logger.warning(
                "[DRY RUN] Would have created pull-request with head '%s', title '%s', body '%s'",
                branch,
                title,
                body,
            )
            return "https://github.example.com/foo/bar/pull/1234"
        upstream_repo = self.github_client.get_repo("openshift-eng/ocp-build-data")
        release_file_content = yaml.load(upstream_repo.get_contents("releases.yml", ref=self.group).decoded_content)
        source_file_content = copy.deepcopy(release_file_content)
        self._pin_nvrs(nvrs, release_file_content)
        if source_file_content == release_file_content:
            self._logger.warning("PR is not created: upstream already updated, nothing to change.")
            return "Nothing to change"
        for b in upstream_repo.get_branches():
            if b.name == branch:
                upstream_repo.get_git_ref(f"heads/{branch}").delete()
        upstream_repo.create_git_ref(f"refs/heads/{branch}", upstream_repo.get_branch(self.group).commit.sha)
        output = io.BytesIO()
        yaml.dump(release_file_content, output)
        output.seek(0)
        fork_file = upstream_repo.get_contents("releases.yml", ref=branch)
        upstream_repo.update_file("releases.yml", body, output.read(), fork_file.sha, branch=branch)
        # create pr
        try:
            pr = upstream_repo.create_pull(title=title, body=body, base=self.group, head=branch)
            pr.merge()
        except GithubException as e:
            self._logger.warning(f"Failed to create pr: {e}")
        return pr.html_url

    async def _notify_microshift_alerts(self, version_release: str):
        doozer_log_file = Path(self._doozer_env_vars["DOOZER_WORKING_DIR"]) / "debug.log"
        slack_client = self.runtime.new_slack_client()
        slack_client.channel = "C0310LGMQQY"  # microshift-alerts
        message = f":alert: @here ART build failure: microshift-{version_release}."
        message += "\nPing @ release-artists if you need help."
        slack_response = await slack_client.say(message)
        slack_thread = slack_response["message"]["ts"]
        if doozer_log_file.exists():
            await slack_client.upload_file(
                file=str(doozer_log_file),
                filename="microshift-build.log",
                initial_comment="Build logs",
                thread_ts=slack_thread,
            )
        else:
            await slack_client.say("Logs are not available.", thread_ts=slack_thread)


@cli.command("build-microshift")
@click.option(
    "--data-path",
    metavar='BUILD_DATA',
    default=None,
    help=f"Git repo or directory containing groups metadata e.g. {constants.OCP_BUILD_DATA_URL}",
)
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group of components on which to operate. e.g. openshift-4.9",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The name of an assembly to rebase & build for. e.g. 4.9.1",
)
@click.option(
    "--payload",
    "payloads",
    metavar="PULLSPEC",
    multiple=True,
    help="[Multiple] Release payload to rebase against; Can be a nightly name or full pullspec",
)
@click.option(
    "--no-rebase",
    is_flag=True,
    help="Don't rebase microshift code; build the current source we have in the upstream repo for testing purpose",
)
@click.option("--force", is_flag=True, help="(For named assemblies) Rebuild even if a build already exists")
@click.option(
    "--skip-prepare-advisory",
    is_flag=True,
    help="(For named assemblies) Skip create advisory and prepare advisory logic",
)
@pass_runtime
@click_coroutine
async def build_microshift(
    runtime: Runtime,
    data_path: str,
    group: str,
    assembly: str,
    payloads: Tuple[str, ...],
    no_rebase: bool,
    force: bool,
    skip_prepare_advisory: bool,
):
    # slack client is dry-run aware and will not send messages if dry-run is enabled
    slack_client = runtime.new_slack_client()
    slack_client.bind_channel(group)
    try:
        pipeline = BuildMicroShiftPipeline(
            runtime=runtime,
            group=group,
            assembly=assembly,
            payloads=payloads,
            no_rebase=no_rebase,
            force=force,
            skip_prepare_advisory=skip_prepare_advisory,
            data_path=data_path,
            slack_client=slack_client,
        )
        await pipeline.run()
    except Exception as err:
        slack_message = f"build-microshift pipeline encountered error: {err}"
        reaction = None
        error_message = slack_message + f"\n {traceback.format_exc()}"
        runtime.logger.error(error_message)
        await slack_client.say_in_thread(slack_message, reaction)
        raise
