import json
import traceback

import click

from pyartcd import jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.locks import Lock
from pyartcd.plashets import build_plashets
from pyartcd.runtime import Runtime
from pyartcd.util import get_freeze_automation, is_manual_build


class RpmMirror:
    def __init__(self):
        self.url = ''
        self.local_plashet_path = ''
        self.plashet_dir_name = ''


class BuildPlashetsPipeline:
    def __init__(
        self, runtime: Runtime, version, release, assembly, repos=None, data_path='', data_gitref='', copy_links=False
    ):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.assembly = assembly
        self.repos = repos.split(',') if repos else []
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

        if self.assembly != 'stream':
            return

        # Since plashets may have been rebuilt, fire off sync for CI. This will transfer RPMs out to
        # mirror.openshift.com/enterprise so that they may be consumed through CI rpm mirrors.
        # Set block_until_building=False since it can take a very long time for the job to start
        # it is enough for it to be queued
        jenkins.start_sync_for_ci(version=self.version, block_until_building=False)

    async def build(self):
        try:
            plashets_built = await build_plashets(
                stream=self.version,
                release=self.release,
                assembly=self.assembly,
                repos=self.repos,
                doozer_working=self.runtime.doozer_working,
                data_path=self.data_path,
                data_gitref=self.data_gitref,
                copy_links=self.copy_links,
                dry_run=self.runtime.dry_run,
            )
            self.runtime.logger.info('Built plashets: %s', json.dumps(plashets_built, indent=4))

        except ChildProcessError as e:
            error_msg = f'Failed building compose: {e}'
            self.runtime.logger.error(error_msg)
            self.runtime.logger.error(traceback.format_exc())
            self.slack_client.bind_channel(f'openshift-{self.version}')
            await self.slack_client.say(f'Failed building compose for {self.version}')
            raise

    async def is_compose_build_permitted(self) -> bool:
        """
        If automation is not frozen, go ahead
        If scheduled automation is frozen and job was triggered by hand, return True
        Otherwise, return False
        """

        automation_freeze_state: str = await get_freeze_automation(
            version=self.version,
            doozer_data_path=self.data_path,
            doozer_working=self.runtime.doozer_working,
            doozer_data_gitref=self.data_gitref,
        )
        self.runtime.logger.info('Automation freeze state for %s: %s', self.version, automation_freeze_state)

        # If automation is not frozen, allow compose
        if automation_freeze_state not in ['scheduled', 'yes', 'True']:
            return True

        # If scheduled automation is frozen and this is a manual build, allow compose
        if automation_freeze_state == 'scheduled' and is_manual_build():
            # Send a Slack notification since we're running compose build during automation freeze
            self.slack_client.bind_channel(f'openshift-{self.version}')
            await self.slack_client.say(
                f":alert: ocp4 build compose running during automation freeze for {self.version}"
            )
            return True

        # In all other case, forbid compose
        return False


@cli.command(
    "build-plashets",
    help="Create multiple yum repos (one for each arch) based on -candidate tags "
    "Those repos can be signed (release state) or unsigned (pre-release state)",
)
@click.option('--version', required=True, help='OCP version to scan, e.g. 4.14')
@click.option('--release', required=True, help='e.g. 201901011200.?')
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
@pass_runtime
@click_coroutine
async def build_plashets_cli(
    runtime: Runtime,
    version: str,
    release: str,
    assembly: str,
    repos: str,
    data_path: str,
    data_gitref: str,
    copy_links: bool,
):
    pipeline = BuildPlashetsPipeline(
        runtime=runtime,
        version=version,
        release=release,
        assembly=assembly,
        repos=repos,
        data_path=data_path,
        data_gitref=data_gitref,
        copy_links=copy_links,
    )

    lock = Lock.PLASHET
    lock_name = lock.value.format(assembly=assembly, version=version)
    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    await locks.run_with_lock(
        coro=pipeline.run(),
        lock=lock,
        lock_name=lock_name,
        lock_id=lock_identifier,
    )
