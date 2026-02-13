import json
import traceback

import click
from artcommonlib.util import isolate_major_minor_in_group

from pyartcd import jenkins, locks, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.locks import Lock
from pyartcd.plashets import build_plashets
from pyartcd.runtime import Runtime
from pyartcd.util import get_freeze_automation, is_manual_build


class RpmMirror:
    def __init__(self):
        self.url = ""
        self.local_plashet_path = ""
        self.plashet_dir_name = ""


class BuildPlashetsPipeline:
    def __init__(
        self, runtime: Runtime, group, release, assembly, repos=None, data_path="", data_gitref="", copy_links=False
    ):
        self.runtime = runtime
        self.group = group
        self.release = release
        self.assembly = assembly
        self.repos = repos.split(",") if repos else []
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.copy_links = copy_links

        self.slack_client = runtime.new_slack_client()

    async def run(self):
        jenkins.init_jenkins()

        # Check if compose build is permitted
        if not await self.is_compose_build_permitted():
            self.runtime.logger.info("Skipping compose build as it's not permitted")
            return

        # Build plashets
        await self.build()

        if self.assembly != "stream":
            return

        # If group looks like openshift-<MAJOR.MINOR>, trigger CI sync for plashets
        if self.group.startswith("openshift-"):
            major, minor = isolate_major_minor_in_group(self.group)
            if major is not None:
                version = f"{major}.{minor}"
                # Since plashets may have been rebuilt, fire off sync for CI. This will transfer RPMs out to
                # mirror.openshift.com/enterprise so that they may be consumed through CI rpm mirrors.
                # Set block_until_building=False since it can take a very long time for the job to start
                # it is enough for it to be queued
                jenkins.start_sync_for_ci(version=version, block_until_building=False)

    async def build(self):
        try:
            plashets_built = await build_plashets(
                group=self.group,
                release=self.release,
                assembly=self.assembly,
                repos=self.repos,
                doozer_working=self.runtime.doozer_working,
                data_path=self.data_path,
                data_gitref=self.data_gitref,
                copy_links=self.copy_links,
                dry_run=self.runtime.dry_run,
            )
            self.runtime.logger.info("Built plashets: %s", json.dumps(plashets_built, indent=4))

        except ChildProcessError as e:
            error_msg = f"Failed building compose: {e}"
            self.runtime.logger.error(error_msg)
            self.runtime.logger.error(traceback.format_exc())
            channel = self.group if self.group.startswith("openshift-") else None
            self.slack_client.bind_channel(channel)
            await self.slack_client.say(f"Failed building compose for {self.group}")
            raise

    async def is_compose_build_permitted(self) -> bool:
        """
        If automation is not frozen, go ahead
        If scheduled automation is frozen and job was triggered by hand, return True
        Otherwise, return False
        """

        # pass group instead of version to get_freeze_automation
        automation_freeze_state: str = await get_freeze_automation(
            group=self.group,
            doozer_data_path=self.data_path,
            doozer_working=self.runtime.doozer_working,
            doozer_data_gitref=self.data_gitref,
        )
        self.runtime.logger.info("Automation freeze state for %s: %s", self.group, automation_freeze_state)

        # If automation is not frozen, allow compose
        if automation_freeze_state not in ["scheduled", "yes", "True"]:
            return True

        # If scheduled automation is frozen and this is a manual build, allow compose
        if automation_freeze_state == "scheduled" and is_manual_build():
            # Send a Slack notification since we're running compose build during automation freeze
            channel = self.group if self.group.startswith("openshift-") else None
            self.slack_client.bind_channel(channel)
            await self.slack_client.say(f":alert: ocp4 build compose running during automation freeze for {self.group}")
            return True

        # In all other case, forbid compose
        return False


@cli.command(
    "build-plashets",
    help="Create multiple yum repos (one for each arch) based on -candidate tags "
    "Those repos can be signed (release state) or unsigned (pre-release state)",
)
@click.option("--group", required=True, help="OCP group to scan, e.g. openshift-4.14, openshift-5.0")
@click.option(
    "--release",
    required=False,
    default="",
    help="e.g. 201901011200.p? (auto-generated from current timestamp if not provided)",
)
@click.option("--assembly", required=True, help="The name of an assembly to rebase & build for")
@click.option(
    "--repos",
    required=False,
    default="",
    help='(optional) limit the repos to build to this list. If empty, build all repos. e.g. ["rhel-8-server-ose-rpms"]',
)
@click.option(
    "--data-path",
    required=False,
    default=OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use (e.g. assembly definition in your own fork)",
)
@click.option("--data-gitref", required=False, default="", help="Doozer data path git [branch / tag / sha] to use")
@click.option("--copy-links", is_flag=True, default=False, help="Call rsync with --copy-links instead of --links")
@pass_runtime
@click_coroutine
async def build_plashets_cli(
    runtime: Runtime,
    group: str,
    release: str,
    assembly: str,
    repos: str,
    data_path: str,
    data_gitref: str,
    copy_links: bool,
):
    # Auto-generate release timestamp if not provided
    if not release:
        release = util.default_release_suffix()
        runtime.logger.info(f"Auto-generated release timestamp: {release}")

    pipeline = BuildPlashetsPipeline(
        runtime=runtime,
        group=group,
        release=release,
        assembly=assembly,
        repos=repos,
        data_path=data_path,
        data_gitref=data_gitref,
        copy_links=copy_links,
    )

    lock = Lock.PLASHET
    lock_name = lock.value.format(assembly=assembly, group=group)
    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning("Env var BUILD_URL has not been defined: a random identifier will be used for the locks")

    await locks.run_with_lock(
        coro=pipeline.run(),
        lock=lock,
        lock_name=lock_name,
        lock_id=lock_identifier,
    )
