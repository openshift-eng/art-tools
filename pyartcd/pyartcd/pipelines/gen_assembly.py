import asyncio
import logging
import os
import re
import traceback
from collections import namedtuple
from io import StringIO
from typing import Iterable, Optional, OrderedDict, Tuple

import aiohttp
import click
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
from artcommonlib.util import (
    get_inflight,
    isolate_major_minor_in_group,
    merge_objects,
    new_roundtrip_yaml_handler,
    split_git_url,
)
from doozerlib.cli.get_nightlies import get_nightly_tag_base, rc_api_url
from ghapi.all import GhApi

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.jenkins import start_build_sync
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()


class GenAssemblyPipeline:
    """Rebase and build MicroShift for an assembly"""

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        build_system: str,
        data_path: str,
        nightlies: Tuple[str, ...],
        allow_pending: bool,
        allow_rejected: bool,
        allow_inconsistency: bool,
        custom: bool,
        arches: Tuple[str, ...],
        in_flight: Optional[str],
        previous_list: Tuple[str, ...],
        auto_previous: bool,
        auto_trigger_build_sync: bool,
        pre_ga_mode: str,
        skip_get_nightlies: bool,
        ignore_non_x86_nightlies: Optional[bool] = False,
        logger: Optional[logging.Logger] = None,
        gen_microshift: bool = False,
        date: Optional[str] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.build_system = build_system
        self.data_path = data_path
        self.nightlies = nightlies
        self.ignore_non_x86_nightlies = ignore_non_x86_nightlies
        self.allow_pending = allow_pending
        self.allow_rejected = allow_rejected
        self.allow_inconsistency = allow_inconsistency
        self.auto_trigger_build_sync = auto_trigger_build_sync
        self.custom = custom
        self.arches = arches
        self.date = date
        self.skip_get_nightlies = skip_get_nightlies
        self.gen_microshift = gen_microshift
        if in_flight:
            self.in_flight = in_flight
        elif not custom and not pre_ga_mode and build_system == 'brew':
            self.in_flight = get_inflight(assembly, group)
        else:
            self.in_flight = None

        self.previous_list = previous_list
        self.auto_previous = auto_previous
        self.pre_ga_mode = pre_ga_mode
        self._logger = logger or runtime.logger
        self._slack_client = self.runtime.new_slack_client()
        self._working_dir = self.runtime.working_dir.absolute()

        self._github_token = os.environ.get('GITHUB_TOKEN')
        if not self._github_token and not self.runtime.dry_run:
            raise ValueError("GITHUB_TOKEN environment variable is required to create a pull request.")

        # determines OCP version
        match = re.fullmatch(r"openshift-(\d+).(\d+)", group)
        if not match:
            raise ValueError(f"Invalid group name: {group}")
        self._ocp_version = (int(match[1]), int(match[2]))

        # sets environment variables for Doozer
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._working_dir / "doozer-working")
        self._doozer_env_vars["DOOZER_DATA_PATH"] = (
            data_path if data_path else self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
        )

        if self.skip_get_nightlies and len(self.nightlies) != len(self.arches):
            raise ValueError(
                f"When using --skip-get-nightlies, nightlies for all given {len(self.arches)} arches must be specified"
            )

        self.private_nightlies = any("priv" in nightly for nightly in self.nightlies)
        if self.private_nightlies:
            if not all("priv" in nightly for nightly in self.nightlies):
                raise ValueError("All nightlies must be private or none")

    async def run(self):
        self._slack_client.bind_channel(self.group)
        slack_response = await self._slack_client.say(
            f":construction: Generating assembly definition {self.assembly} :construction:"
        )
        slack_thread = slack_response["message"]["ts"]
        try:
            if self.arches and not self.custom:
                self._logger.warning("Customizing arches for non-custom assembly, proceed with caution")

            if self.custom and (self.auto_previous or self.previous_list or self.in_flight):
                raise ValueError("Specifying previous list for a custom release is not allowed.")

            if self.skip_get_nightlies:
                candidate_nightlies = self.nightlies
            elif self.ignore_non_x86_nightlies:
                candidate_nightlies = [await self._get_latest_accepted_nightly()]
            else:
                candidate_nightlies, latest_nightly = await asyncio.gather(
                    *[self._get_nightlies(), self._get_latest_accepted_nightly()]
                )

            self._logger.info("Generating assembly definition...")
            assembly_definition = await self._gen_assembly_from_releases(candidate_nightlies)
            out = StringIO()
            yaml.dump(assembly_definition, out)
            self._logger.info("Generated assembly definition:\n%s", out.getvalue())

            # For Konflux, stop here at the moment
            if (
                self.build_system == 'konflux'
                and self.group.removeprefix('openshift-') not in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
            ):
                return

            # Create a PR
            pr = await self._create_or_update_pull_request(assembly_definition)

            # Sends a slack message
            message = (
                f"Hi @release-artists, please review assembly definition for {self.assembly}: {pr.html_url}\n\n"
                f"The inflight release is {self.in_flight}"
            )
            if not self.skip_get_nightlies:
                if not self.ignore_non_x86_nightlies and latest_nightly not in candidate_nightlies:
                    message += '\n\n:warning: note that `gen-assembly` did not select the latest accepted amd64 nightly'
            else:
                message += '\n\n:warning: note that `gen-assembly` was run with `--skip-get-nightlies`'

            await self._slack_client.say(message, slack_thread)

        except Exception as err:
            error_message = f"Error generating assembly definition: {err}\n {traceback.format_exc()}"
            self._logger.error(error_message)
            await self._slack_client.say(f"Error generating assembly definition for {self.assembly}", slack_thread)
            raise

    async def _get_latest_accepted_nightly(self):
        self._logger.info('Retrieving most recent accepted amd64 nightly...')
        major, minor = isolate_major_minor_in_group(self.group)
        tag_base = get_nightly_tag_base(major, minor, self.build_system)
        rc_endpoint = f"{rc_api_url(tag_base, 'amd64', self.private_nightlies)}/latest"
        async with aiohttp.ClientSession() as session:
            async with session.get(rc_endpoint) as response:
                if response.status != 200:
                    self._logger.warning('Failed retrieving latest accepted nighly from %s', rc_endpoint)
                    return None
                return (await response.json()).get('name')

    async def _get_nightlies(self):
        """
        Get nightlies from Release Controllers
        :return: NVRs
        """

        self._logger.info("Getting nightlies from Release Controllers...")

        cmd = [
            "doozer",
            "--group",
            self.group,
            "--assembly",
            "stream",
            "--build-system",
            self.build_system,
        ]
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))
        cmd.append("get-nightlies")
        if self.allow_pending:
            cmd.append("--allow-pending")
        if self.allow_rejected:
            cmd.append("--allow-rejected")
        if self.allow_inconsistency:
            cmd.append("--allow-inconsistency")
        for nightly in self.nightlies:
            cmd.append(f"--matching={nightly}")

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None, env=self._doozer_env_vars)
        return out.strip().split()

    async def _gen_assembly_from_releases(self, candidate_nightlies: Iterable[str]) -> OrderedDict:
        """Run doozer release:gen-assembly from-releases
        :return: Assembly definition
        """

        cmd = [
            "doozer",
            "--group",
            self.group,
            "--assembly",
            "stream",
            "--build-system",
            self.build_system,
        ]
        if self.arches:
            cmd.append("--arches")
            cmd.append(",".join(self.arches))
        cmd.append("release:gen-assembly")
        cmd.append(f"--name={self.assembly}")
        cmd.append("from-releases")
        for nightly in candidate_nightlies:
            cmd.append(f"--nightly={nightly}")

        if self.pre_ga_mode:
            cmd.append(f"--pre-ga-mode={self.pre_ga_mode}")
        if self.gen_microshift:
            cmd.append("--gen-microshift")
        if self.date:
            cmd.append(f"--date={self.date}")
        if self.custom:
            cmd.append("--custom")
        else:
            if self.in_flight:
                cmd.append(f"--in-flight={self.in_flight}")
            for previous in self.previous_list:
                cmd.append(f"--previous={previous}")
            if self.auto_previous:
                cmd.append("--auto-previous")
        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None, env=self._doozer_env_vars)
        return yaml.load(out)

    async def _create_or_update_pull_request(self, assembly_definition: OrderedDict):
        """
        Create or update pull request for ocp-build-data
        :param assembly_definition: the assembly definition to be added to releases.yml
        """

        self._logger.info("Creating ocp-build-data PR...")

        # Gather PR data
        ocp_build_data_repo_push_url = self.runtime.config["build_config"]["ocp_build_data_repo_push_url"]
        match = re.search(r"github\.com[:/](.+)/(.+)(?:.git)?", ocp_build_data_repo_push_url)
        if not match:
            raise ValueError(
                f"Couldn't create a pull request: {ocp_build_data_repo_push_url} is not a valid github repo"
            )

        title = f"Add assembly {self.assembly}"
        body = f"Created by job run {jenkins.get_build_url()}"
        branch = f"auto-gen-assembly-{self.group}-{self.assembly}"
        head = f"{match[1]}:{branch}"
        base = self.group

        # Dry run
        if self.runtime.dry_run:
            self._logger.warning(
                "[DRY RUN] Would have created pull-request with head '%s', base '%s' title '%s', body '%s'",
                head,
                base,
                title,
                body,
            )

            if self.auto_trigger_build_sync:
                self._logger.info("[DRY RUN] Would have triggered build-sync with the PR assembly definition")
            d = {"html_url": "https://github.example.com/foo/bar/pull/1234", "number": 1234}
            result = namedtuple('pull_request', d.keys())(*d.values())
            return result

        # Clone ocp-build-data
        build_data_path = self._working_dir / "ocp-build-data-push"
        build_data = GitRepository(build_data_path, dry_run=self.runtime.dry_run)
        await build_data.setup(ocp_build_data_repo_push_url)
        await build_data.fetch_switch_branch(branch, self.group)

        # Make changes to releases.yml and push changes to auto-gen-assembly branch
        releases_yaml_path = build_data_path / "releases.yml"
        releases_yaml = yaml.load(releases_yaml_path) if releases_yaml_path.exists() else {}
        releases_yaml = merge_objects(assembly_definition, releases_yaml)
        yaml.dump(releases_yaml, releases_yaml_path)
        pushed = await build_data.commit_push(f"{title}\n{body}")
        if not pushed:
            self._logger.warning("PR is not created: Nothing to commit.")
            return None

        # Create a pull request
        _, owner, repo = split_git_url(ocp_build_data_repo_push_url)
        api = GhApi(owner=owner, repo=repo, token=self._github_token)
        existing_prs = api.pulls.list(state="open", base=base, head=head)
        if not existing_prs.items:
            result = api.pulls.create(head=head, base=base, title=title, body=body, maintainer_can_modify=True)
        else:
            pull_number = existing_prs.items[0].number
            result = api.pulls.update(pull_number=pull_number, title=title, body=body)

        # Trigger build-sync
        if self.auto_trigger_build_sync:
            self._logger.info("Triggering build-sync")
            build_version = self.group.split("-")[1]  # eg: 4.14 from openshift-4.14
            start_build_sync(
                build_version=build_version,
                assembly=self.assembly,
                doozer_data_path=constants.OCP_BUILD_DATA_URL,  # we're not passing doozer_data_path
                # to build-sync because we always create branch on the base repo
                doozer_data_gitref=branch,
            )

        return result


@cli.command("gen-assembly")
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
    "--assembly", metavar="ASSEMBLY_NAME", required=True, help="The name of an assembly to generate for. e.g. 4.9.1"
)
@click.option(
    "--build-system",
    metavar="BUILD_SYSTEM",
    required=False,
    default='brew',
    help="What build system we're operating on ('brew'|'konflux')",
)
@click.option(
    "--nightly",
    "nightlies",
    metavar="TAG",
    multiple=True,
    help="(Optional) [MULTIPLE] List of nightlies to match with `doozer get-nightlies` (if empty, find latest)",
)
@click.option("--allow-pending", is_flag=True, help="Match nightlies that have not completed tests")
@click.option("--allow-rejected", is_flag=True, help="Match nightlies that have failed their tests")
@click.option(
    "--allow-inconsistency",
    is_flag=True,
    help="Allow matching nightlies built from matching commits but with inconsistent RPMs",
)
@click.option(
    "--custom",
    is_flag=True,
    help="Custom assemblies are not for official release. They can, for example, not have all required arches for the group.",
)
@click.option(
    "--pre-ga-mode",
    type=click.Choice(["prerelease"], case_sensitive=False),
    help="Prepare the advisory for 'prerelease' operator release",
)
@click.option('--auto-trigger-build-sync', is_flag=True, help='Will trigger build-sync automatically after PR creation')
@click.option(
    "--arch",
    "arches",
    metavar="TAG",
    multiple=True,
    help="(Optional) [MULTIPLE] (for custom assemblies only) Limit included arches to this list",
)
@click.option('--in-flight', 'in_flight', metavar='EDGE', help='An in-flight release that can upgrade to this release')
@click.option(
    '--previous',
    'previous_list',
    metavar='EDGES',
    default=[],
    multiple=True,
    help='A list of releases that can upgrade to this release',
)
@click.option(
    '--auto-previous',
    'auto_previous',
    is_flag=True,
    help='If specified, previous list is calculated from Cincinnati graph',
)
@click.option(
    '--skip-get-nightlies',
    'skip_get_nightlies',
    is_flag=True,
    default=False,
    help='Skip get_nightlies_function (Use only for special cases)',
)
@click.option(
    '--ignore-non-x86-nightlies',
    'ignore_non_x86_nightlies',
    is_flag=True,
    default=False,
    help='Ignore non-x86 nightlies, only use x86 nightly',
)
@click.option(
    "--gen-microshift",
    'gen_microshift',
    default=False,
    is_flag=True,
    help="Create microshift entry for assembly release.",
)
@click.option("--date", metavar="YYYY-MMM-DD", help="Expected release date (e.g. 2020-Nov-25)")
@pass_runtime
@click_coroutine
async def gen_assembly(
    runtime: Runtime,
    data_path: str,
    group: str,
    assembly: str,
    build_system: str,
    nightlies: Tuple[str, ...],
    allow_pending: bool,
    allow_rejected: bool,
    allow_inconsistency: bool,
    custom: bool,
    pre_ga_mode: str,
    auto_trigger_build_sync: bool,
    arches: Tuple[str, ...],
    in_flight: Optional[str],
    previous_list: Tuple[str, ...],
    auto_previous: bool,
    skip_get_nightlies: bool,
    ignore_non_x86_nightlies: bool,
    gen_microshift: bool,
    date: Optional[str],
):
    pipeline = GenAssemblyPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        build_system=build_system,
        data_path=data_path,
        nightlies=nightlies,
        allow_pending=allow_pending,
        allow_rejected=allow_rejected,
        allow_inconsistency=allow_inconsistency,
        arches=arches,
        custom=custom,
        auto_trigger_build_sync=auto_trigger_build_sync,
        in_flight=in_flight,
        previous_list=previous_list,
        auto_previous=auto_previous,
        pre_ga_mode=pre_ga_mode,
        skip_get_nightlies=skip_get_nightlies,
        ignore_non_x86_nightlies=ignore_non_x86_nightlies,
        gen_microshift=gen_microshift,
        date=date,
    )
    await pipeline.run()
