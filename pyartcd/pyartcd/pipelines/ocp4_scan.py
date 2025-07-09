import os
from typing import Optional

import click
import yaml
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import get_changes, has_layered_rhcos


class Ocp4ScanPipeline:
    def __init__(self, runtime: Runtime, version: str, data_path: Optional[str] = None):
        self.runtime = runtime
        self.version = version
        self.data_path = data_path or constants.OCP_BUILD_DATA_URL  # in case we will make it a parameter
        self.logger = runtime.logger
        self.rhcos_updated = False
        self.rhcos_outdated = False
        self.rhcos_inconsistent = False
        self.inconsistent_rhcos_rpms = None
        self.changes = {}
        self.issues = []
        self._doozer_working = self.runtime.working_dir / "doozer_working"
        self.skipped = True  # True by default; if not locked, run() will set it to False

        self._doozer_base_command = [
            'doozer',
            '--assembly=stream',
            f'--working-dir={self._doozer_working}',
            f'--data-path={self.data_path}',
            f'--group=openshift-{self.version}',
        ]

    async def run(self):
        # If we get here, lock could be acquired
        self.skipped = False
        self.logger.info('Building: %s', self.version)

        # KUBECONFIG env var must be defined in order to scan sources
        if not os.getenv('KUBECONFIG'):
            raise RuntimeError('Environment variable KUBECONFIG must be defined')

        # Check for RHCOS changes and inconsistencies
        # Running these two commands sequentially (instead of using asyncio.gather) to avoid file system conflicts
        await self._get_changes()
        await self._handle_scan_issues()
        await self._rhcos_inconsistent()

        # Handle source changes, if any
        changes = False
        if self.changes.get('rpms', None) or self.changes.get('images', None):
            self.logger.info('Detected at least one updated RPM or image')
            changes = True
            rpm_list = self.changes.get('rpms', [])
            image_list = self.changes.get('images', [])

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s ocp4 build', self.version)
                return

            # Update build description
            jenkins.update_description(f'Changed {len(image_list)} images<br/>')
            if rpm_list:
                jenkins.update_description(f'Changed {len(rpm_list)} rpms<br/>')

            # Trigger ocp4
            self.logger.info('Triggering a %s ocp4 build', self.version)
            jenkins.start_ocp4(
                build_version=self.version,
                assembly='stream',
                rpm_list=rpm_list,
                image_list=image_list,
                comment_on_pr=True,
            )

        if self.rhcos_inconsistent or self.rhcos_outdated:
            changes = True
            if self.rhcos_inconsistent:
                self.logger.info('Detected inconsistent RHCOS RPMs:\n%s', self.inconsistent_rhcos_rpms)
            if self.rhcos_outdated:
                self.logger.info('Detected outdated RHCOS RPMs:\n%s', self.changes.get('rhcos', None))

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s RHCOS build', self.version)
                return
            # Inconsistency probably means partial failure and we would like to retry.
            # but don't kick off more if already in progress.
            self.logger.info('Triggering a %s RHCOS build for consistency', self.version)
            layered_rhcos = await has_layered_rhcos(self._doozer_base_command)
            job_name = 'build-node-image' if layered_rhcos else 'build'
            jenkins.start_rhcos(build_version=self.version, new_build=False, job_name=job_name)

        elif self.rhcos_updated:
            changes = True
            self.logger.info('Detected at least one updated RHCOS')

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s build-sync build', self.version)
                return

            self.logger.info('Triggering a %s build-sync to pick up latest RHCOS', self.version)

            if self.version in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS:
                self.runtime.logger.info(
                    f'Skipping Brew build-sync for streams updated by konflux builds {KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS}'
                )
            else:
                jenkins.start_build_sync(
                    build_version=self.version,
                    assembly="stream",
                    build_system="brew",
                )

            jenkins.start_build_sync(
                build_version=self.version,
                assembly="stream",
                build_system="konflux",
            )

        if changes is False:
            self.logger.info('*** No changes detected')
            jenkins.update_title(' [NO CHANGES]')

    async def _get_changes(self):
        """
        Check for changes by calling doozer config:scan-sources
        Changed rpms, images or rhcos are recorded in self.changes
        self.rhcos_changed is also updated accordingly
        """

        # Run doozer scan-sources
        cmd = self._doozer_base_command + [
            'config:scan-sources',
            '--yaml',
            f'--ci-kubeconfig={os.environ["KUBECONFIG"]}',
            '--rebase-priv',
        ]
        if self.runtime.dry_run:
            cmd += ' --dry-run'
        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)

        self.logger.info('scan-sources output for openshift-%s:\n%s', self.version, out)

        yaml_data = yaml.safe_load(out)
        changes = get_changes(yaml_data)
        if changes:
            self.logger.info('Detected source changes:\n%s', yaml.safe_dump(changes))
        else:
            self.logger.info('No changes detected in RPMs, images or RHCOS')

        # Check for RHCOS changes
        if changes.get('rhcos', None):
            for rhcos_change in changes['rhcos']:
                if rhcos_change['reason'].get('updated', None):
                    self.rhcos_updated = True
                if rhcos_change['reason'].get('outdated', None):
                    self.rhcos_outdated = True
        self.changes = changes
        self.issues = yaml_data.get('issues', [])

    async def _handle_scan_issues(self):
        """
        doozer config:scan-sources might have encountered issues during rebase into openshift-priv.
        Issues may include:
        - a repo could not be rebased because it needs manual reconciliation
        - git push to openshift-priv failed even after retries

        In both cases, an alert is generated on Slack
        """

        if not self.issues:
            return  # all good

        slack_client = self.runtime.new_slack_client()
        slack_client.bind_channel(self.version)
        message = (
            f':warning: @release-artists, some issues have arisen during scan-sources for *{self.version}* :warning:'
        )
        slack_response = await slack_client.say(message)

        slack_thread = slack_response["message"]["ts"]
        for issue in self.issues:
            await slack_client.say(f'\n- `{issue["name"]}`: {issue["issue"]}', slack_thread)

    async def _rhcos_inconsistent(self):
        """
        Check for RHCOS inconsistencies by calling doozer inspect:stream INCONSISTENT_RHCOS_RPMS
        """

        cmd = self._doozer_base_command + [
            'inspect:stream',
            'INCONSISTENT_RHCOS_RPMS',
            '--strict',
        ]
        try:
            _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
            self.logger.info(out)
            self.rhcos_inconsistent = False
        except ChildProcessError as e:
            self.rhcos_inconsistent = True
            self.inconsistent_rhcos_rpms = e


@cli.command('ocp4-scan')
@click.option('--version', required=True, help='OCP version to scan')
@pass_runtime
@click_coroutine
async def ocp4_scan(runtime: Runtime, version: str):
    pipeline = Ocp4ScanPipeline(runtime, version)
    jenkins.init_jenkins()

    if runtime.dry_run:
        await pipeline.run()

    else:
        lock = Lock.SCAN
        lock_name = lock.value.format(version=version)
        lock_identifier = jenkins.get_build_path()
        if not lock_identifier:
            runtime.logger.warning(
                'Env var BUILD_URL has not been defined: a random identifier will be used for the locks'
            )

        # Scheduled builds are already being skipped if the lock is already acquired.
        # For manual builds, we need to check if the build and scan locks are already acquired,
        # and skip the current build if that's the case.
        # Should that happen, signal it by appending a [SKIPPED][LOCKED] to the build title
        async def run_with_build_lock():
            build_lock = Lock.BUILD
            build_lock_name = build_lock.value.format(version=version)
            await locks.run_with_lock(
                coro=pipeline.run(),
                lock=build_lock,
                lock_name=build_lock_name,
                lock_id=lock_identifier,
                skip_if_locked=True,
            )

        await locks.run_with_lock(
            coro=run_with_build_lock(),
            lock=lock,
            lock_name=lock_name,
            lock_id=lock_identifier,
            skip_if_locked=True,
        )

    if pipeline.skipped:
        jenkins.update_title(' [SKIPPED][LOCKED]')
