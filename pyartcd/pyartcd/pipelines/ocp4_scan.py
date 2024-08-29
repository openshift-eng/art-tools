import os
from typing import Optional
import yaml

import click

from artcommonlib import exectools
from pyartcd import constants, util, locks
from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime


class Ocp4ScanPipeline:

    def __init__(self, runtime: Runtime, version: str, data_path: Optional[str] = None):
        self.runtime = runtime
        self.version = version
        self.data_path = data_path or constants.OCP_BUILD_DATA_URL  # in case we will make it a parameter
        self.logger = runtime.logger
        self.rhcos_changed = False
        self.rhcos_inconsistent = False
        self.inconsistent_rhcos_rpms = None
        self.changes = {}
        self.issues = []
        self._doozer_working = self.runtime.working_dir / "doozer_working"
        self.locked = True  # True by default; if not locked, run() will set it to False

    async def run(self):
        # If we get here, lock could be acquired
        self.locked = False
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
        if self.changes.get('rpms', None) or self.changes.get('images', None):
            self.logger.info('Detected at least one updated RPM or image')

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s ocp4 build', self.version)
                return

            # Trigger ocp4
            self.logger.info('Triggering a %s ocp4 build', self.version)
            jenkins.start_ocp4(
                build_version=self.version,
                assembly='stream',
                rpm_list=self.changes.get('rpms', []),
                image_list=self.changes.get('images', []),
                comment_on_pr=True
            )

        elif self.rhcos_inconsistent:
            self.logger.info('Detected inconsistent RHCOS RPMs:\n%s', self.inconsistent_rhcos_rpms)

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s RHCOS build', self.version)
                return

            # Inconsistency probably means partial failure and we would like to retry.
            # but don't kick off more if already in progress.
            self.logger.info('Triggering a %s RHCOS build for consistency', self.version)
            jenkins.start_rhcos(build_version=self.version, new_build=True)

        elif self.rhcos_changed:
            self.logger.info('Detected at least one updated RHCOS')

            if self.runtime.dry_run:
                self.logger.info('Would have triggered a %s build-sync build', self.version)
                return

            self.logger.info('Triggering a %s build-sync', self.version)
            jenkins.start_build_sync(
                build_version=self.version,
                assembly="stream",
            )

        else:
            jenkins.update_title(' [NO CHANGES]')

    async def _get_changes(self):
        """
        Check for changes by calling doozer config:scan-sources
        Changed rpms, images or rhcos are recorded in self.changes
        self.rhcos_changed is also updated accordingly
        """

        # Run doozer scan-sources
        cmd = f'doozer --data-path={self.data_path} --assembly stream --working-dir={self._doozer_working} ' \
              f'--group=openshift-{self.version} ' \
              f'config:scan-sources --yaml --ci-kubeconfig {os.environ["KUBECONFIG"]} --rebase-priv'
        if self.runtime.dry_run:
            cmd += ' --dry-run'
        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)

        self.logger.info('scan-sources output for openshift-%s:\n%s', self.version, out)

        yaml_data = yaml.safe_load(out)
        changes = util.get_changes(yaml_data)
        if changes:
            self.logger.info('Detected source changes:\n%s', yaml.safe_dump(changes))
        else:
            self.logger.info('No changes detected in RPMs, images or RHCOS')

        # Check for RHCOS changes
        if changes.get('rhcos', None):
            self.rhcos_changed = True
        else:
            self.rhcos_changed = False

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
        message = \
            f':warning: @release-artists, some issues have arisen during scan-sources for *{self.version}* :warning:'
        slack_response = await slack_client.say(message)

        slack_thread = slack_response["message"]["ts"]
        for issue in self.issues:
            await slack_client.say(f'\n- `{issue["name"]}`: {issue["issue"]}', slack_thread)

    async def _rhcos_inconsistent(self):
        """
        Check for RHCOS inconsistencies by calling doozer inspect:stream INCONSISTENT_RHCOS_RPMS
        """

        cmd = f'doozer --data-path={self.data_path} --assembly stream --working-dir {self._doozer_working} ' \
              f'--group openshift-{self.version} ' \
              f'inspect:stream INCONSISTENT_RHCOS_RPMS --strict'
        try:
            _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
            self.logger.info(out)
            self.rhcos_inconsistent = False
        except ChildProcessError as e:
            self.rhcos_inconsistent = True
            self.inconsistent_rhcos_rpms = e


@cli.command('ocp4-scan')
@click.option('--version', required=True, help='OCP version to scan')
@click.option('--ignore-locks', is_flag=True, default=False,
              help='Do not wait for other builds in this version to complete (only allowed in dry-run mode)')
@pass_runtime
@click_coroutine
async def ocp4_scan(runtime: Runtime, version: str, ignore_locks: bool):
    lock = Lock.SCAN
    lock_name = lock.value.format(version=version)
    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    pipeline = Ocp4ScanPipeline(runtime, version)
    jenkins.init_jenkins()

    if ignore_locks:
        # Already checked by aos-cd-jobs, but you never know...
        if not runtime.dry_run:
            raise RuntimeError('--ignore-locks can only by used with --dry-run')
        await pipeline.run()

    else:
        async def run_with_build_lock():
            build_lock = Lock.BUILD
            build_lock_name = build_lock.value.format(version=version)
            await locks.run_with_lock(
                coro=pipeline.run(),
                lock=build_lock,
                lock_name=build_lock_name,
                lock_id=lock_identifier,
                skip_if_locked=True
            )

        await locks.run_with_lock(
            coro=run_with_build_lock,
            lock=lock,
            lock_name=lock_name,
            lock_id=lock_identifier,
            skip_if_locked=True
        )

    # This should not happen, as the schedule build already checked for existing locks
    # It doesn't hurt to check once more though
    if pipeline.locked:
        jenkins.update_title(' [SKIPPED][LOCKED]')
