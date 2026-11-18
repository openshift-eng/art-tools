import json
import traceback

import click
from artcommonlib.constants import FREEZE_AUTOMATION_SCHEDULED
from artcommonlib.util import isolate_major_minor_in_group

from pyartcd import jenkins, locks, tekton, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.locks import Lock
from pyartcd.plashets import build_plashets
from pyartcd.runtime import Runtime
from pyartcd.util import get_freeze_automation, is_freeze_state_build_permitted


class RpmMirror:
    def __init__(self):
        self.url = ''
        self.local_plashet_path = ''
        self.plashet_dir_name = ''


class BuildPlashetsPipeline:
    def __init__(
        self,
        runtime: Runtime,
        group,
        release,
        assembly,
        repos=None,
        data_path='',
        data_gitref='',
        copy_links=False,
        version=None,
        ignore_automation_freeze=False,
    ):
        self.runtime = runtime
        self.group = group
        self.release = release
        self.assembly = assembly
        self.repos = repos.replace(',', ' ').split() if repos else []
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.copy_links = copy_links
        self.version = version
        self.ignore_automation_freeze = ignore_automation_freeze

        self.slack_client = runtime.new_slack_client()

    async def run(self):
        if tekton.is_tekton_context():
            title = f'{self.group} - {self.release} - {self.assembly}'
            if self.runtime.dry_run:
                title += ' [DRY_RUN]'
            util.update_run_title(title, annotation_key='art.openshift.io/build-plashets-status')
        else:
            jenkins.init_jenkins()

        # Honor the same explicit override as the parent OCP build.
        if self.ignore_automation_freeze:
            self.runtime.logger.warning(
                'Ignoring global and group freeze_automation for %s because '
                '--ignore-automation-freeze was explicitly set',
                self.group,
            )
        elif not await self.is_compose_build_permitted():
            self.runtime.logger.info("Skipping compose build as it's not permitted")
            return

        # Build plashets
        await self.build()

        if self.runtime.dry_run:
            self.runtime.logger.info('Not triggering CI sync in dry run mode')
            return

        if self.assembly != 'stream':
            return

        # If group looks like openshift-<MAJOR.MINOR>, trigger CI sync for plashets
        if self.group.startswith('openshift-'):
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
                version=self.version,
            )
            self.runtime.logger.info('Built plashets: %s', json.dumps(plashets_built, indent=4))

        except ChildProcessError as e:
            error_msg = f'Failed building compose: {e}'
            self.runtime.logger.error(error_msg)
            self.runtime.logger.error(traceback.format_exc())
            channel = self.group if self.group.startswith('openshift-') else None
            self.slack_client.bind_channel(channel)
            await self.slack_client.say(f'Failed building compose for {self.group}')
            raise

    async def is_compose_build_permitted(self) -> bool:
        """
        Decide whether a compose may be built right now based on the group's
        effective freeze_automation state. The decision is delegated to the
        shared util.is_freeze_state_build_permitted helper so it stays in sync
        with is_build_permitted: 'yes'/'True' block everything, 'scheduled'
        permits only manual runs, and 'weekdays' permits weekend (or manual)
        runs while blocking weekday non-manual runs.
        """

        if self.group.startswith('golang'):
            self.runtime.logger.info('Skipping freeze automation check for golang group %s', self.group)
            return True

        automation_freeze_state: str = await get_freeze_automation(
            group=self.group,
            doozer_data_path=self.data_path,
            doozer_working=self.runtime.doozer_working,
            doozer_data_gitref=self.data_gitref,
        )
        self.runtime.logger.info('Automation freeze state for %s: %s', self.group, automation_freeze_state)

        permitted = is_freeze_state_build_permitted(automation_freeze_state)

        # If a manual compose is permitted while scheduled automation is frozen, alert on Slack since
        # we're running a compose build during an active automation freeze.
        if permitted and automation_freeze_state == FREEZE_AUTOMATION_SCHEDULED:
            channel = self.group if self.group.startswith('openshift-') else None
            self.slack_client.bind_channel(channel)
            await self.slack_client.say(f":alert: ocp4 build compose running during automation freeze for {self.group}")

        return permitted


@cli.command(
    "build-plashets",
    help="Create multiple yum repos (one for each arch) based on -candidate tags "
    "Those repos can be signed (release state) or unsigned (pre-release state)",
)
@click.option('--group', required=True, help='OCP group to scan, e.g. openshift-4.14, openshift-5.0')
@click.option(
    '--release',
    required=False,
    default='',
    help='e.g. 201901011200.p? (auto-generated from current timestamp if not provided)',
)
@click.option('--assembly', required=True, help='The name of an assembly to rebase & build for')
@click.option(
    '--repos',
    required=False,
    default='',
    help='(optional) limit the repos to build to this list. If empty, build all repos. e.g. ["rhel-8-server-ose-rpms"]',
)
@click.option(
    '--data-path',
    required=False,
    default=OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option('--copy-links', is_flag=True, default=False, help='Call rsync with --copy-links instead of --links')
@click.option(
    '--ignore-automation-freeze',
    is_flag=True,
    default=False,
    help='Ignore global and group freeze_automation policy for this run; compose locks still apply',
)
@click.option(
    '--version',
    required=False,
    default='',
    help='OCP version for resolving MAJOR/MINOR vars in golang groups (e.g. 4.18). Required for golang groups, ignored for other groups.',
)
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
    ignore_automation_freeze: bool,
    version: str,
):
    # Auto-generate release timestamp if not provided
    if not release:
        release = util.default_release_suffix()
        runtime.logger.info(f'Auto-generated release timestamp: {release}')

    # Validate version parameter for golang groups
    is_golang_group = group.startswith('golang')
    if is_golang_group and not version:
        raise click.BadParameter(
            f"Version parameter is required for golang groups (group: {group}). "
            "Please provide --version (e.g., --version 4.18)"
        )

    if not is_golang_group and version:
        runtime.logger.warning(
            f"Version parameter '{version}' will be ignored for non-golang group '{group}'. "
            "Version is only used for golang groups."
        )

    pipeline = BuildPlashetsPipeline(
        runtime=runtime,
        group=group,
        release=release,
        assembly=assembly,
        repos=repos,
        data_path=data_path,
        data_gitref=data_gitref,
        copy_links=copy_links,
        version=version or None,
        ignore_automation_freeze=ignore_automation_freeze,
    )

    lock = Lock.PLASHET
    lock_name = lock.value.format(assembly=assembly, group=group)
    lock_identifier = locks.get_run_lock_owner()

    await locks.run_with_lock(
        coro=pipeline.run(),
        lock=lock,
        lock_name=lock_name,
        lock_id=lock_identifier,
    )
