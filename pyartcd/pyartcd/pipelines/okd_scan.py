#!/usr/bin/env python3

"""
OKD Konflux Scan Pipeline

This pipeline scans for changes in OKD images and triggers OKD builds when relevant changes are detected.
It uses doozer scan-sources-konflux with --variant=okd to filter for OKD-specific rebuild reasons.

The --variant=okd flag causes doozer to skip checks that aren't relevant for OKD

Valid OKD rebuild reasons (checked by doozer with --variant=okd):
- NO_LATEST_BUILD: No build exists yet
- LAST_BUILD_FAILED: Previous build failed
- NEW_UPSTREAM_COMMIT: New commit in upstream source
- UPSTREAM_COMMIT_MISMATCH: Commit doesn't match expected
- ANCESTOR_CHANGING: Parent image is changing
- CONFIG_CHANGE: Configuration changed
- BUILDER_CHANGING: Builder image is changing
- DEPENDENCY_NEWER: Dependencies have newer versions

Example usage:
    artcd okd-scan --version 4.21 --assembly stream
"""

import logging
import os

import click
import yaml
from artcommonlib import exectools

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime


class OkdScanPipeline:
    """
    Pipeline for scanning OKD image sources and triggering builds when changes are detected.
    """

    def __init__(
        self, runtime: Runtime, version: str, data_path: str, assembly: str, data_gitref: str, image_list: str
    ):
        """
        Initialize the OKD scan pipeline.

        Arg(s):
            runtime (Runtime): Pipeline runtime context
            version (str): OCP/OKD version to scan (e.g., "4.21")
            data_path (str): Path to ocp-build-data repository
            assembly (str): Assembly name (typically "stream")
            data_gitref (str): Git reference for ocp-build-data
            image_list (str): Comma-separated list of images to scan (optional)
        """
        self.runtime = runtime
        self.version = version
        self.data_path = data_path
        self.assembly = assembly
        self.data_gitref = data_gitref
        self.image_list = image_list

        self.logger = logging.getLogger(__name__)
        self._doozer_working = self.runtime.working_dir / "doozer_working"
        self.report = {}

        self.skipped = True  # True by default; if not locked, run() will set it to False

        group_param = f'openshift-{self.version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        self.doozer_base_command = [
            'doozer',
            f'--working-dir={self._doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={group_param}',
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            '--load-okd-only',
            '--variant=okd',
        ]

    async def run(self):
        """
        Main pipeline execution.
        """
        # If we get here, lock could be acquired
        self.skipped = False

        # Early exit if version not enabled for OKD
        if self.version not in constants.OKD_ENABLED_VERSIONS:
            self.logger.info(
                'Version %s is not enabled for OKD (enabled versions: %s). Skipping scan.',
                self.version,
                constants.OKD_ENABLED_VERSIONS,
            )
            return

        scan_info = f'Scanning OKD for version {self.version}, assembly {self.assembly}, data path {self.data_path}'

        if self.data_gitref:
            scan_info += f'@{self.data_gitref}'
        self.logger.info(scan_info)

        self._check_params()

        # Scan for changes
        await self._scan_sources()

        # Handle image source changes
        self._handle_source_changes()

    def _check_params(self):
        """
        Validate pipeline parameters.
        Non-stream assemblies and custom data paths are only allowed in dry-run mode.
        """
        if not self.runtime.dry_run:
            if self.assembly != 'stream':
                raise ValueError('non-stream assemblies are only allowed in dry-run mode')
            if self.data_path != constants.OCP_BUILD_DATA_URL or self.data_gitref:
                raise ValueError('Custom data paths can only be used in dry-run mode')

    async def _scan_sources(self):
        """
        Scan for changes by calling doozer beta:config:konflux:scan-sources.
        Results are stored in self.report.
        """
        cmd = self.doozer_base_command.copy()
        if self.image_list:
            cmd.append(f'--images={self.image_list}')

        cmd.extend(
            [
                'beta:config:konflux:scan-sources',
                '--yaml',
            ]
        )

        if self.runtime.dry_run:
            cmd.append('--dry-run')

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        self.logger.info('scan-sources output for OKD openshift-%s:\n%s', self.version, out)

        self.report = yaml.safe_load(out)

    def _handle_source_changes(self):
        """
        Check scan results and trigger OKD build if changes are detected.
        Since we use --variant=okd, only valid OKD rebuild reasons are reported.
        """
        # Collect all changed images - doozer already filtered to valid OKD rebuild reasons
        changed_okd_images = [image['name'] for image in self.report.get('images', []) if image.get('changed')]

        if not changed_okd_images:
            self.logger.info('No OKD image changes detected')
            return

        # Update Jenkins title and description
        jenkins.update_title(' [OKD SOURCE CHANGES]')
        jenkins.update_description(f'Changed {len(changed_okd_images)} OKD images<br/>')

        if self.runtime.dry_run:
            self.logger.info(
                'Would have triggered a %s okd build with images %s', self.version, ','.join(changed_okd_images)
            )
            return

        # Trigger okd build
        self.logger.info('Triggering a %s okd build with %d images', self.version, len(changed_okd_images))
        jenkins.start_okd(
            build_version=self.version,
            assembly='stream',
            image_list=changed_okd_images,
        )


@cli.command('okd-scan')
@click.option('--version', required=True, help='OCP/OKD version to scan (e.g., 4.21)')
@click.option('--assembly', required=False, default='stream', help='Assembly to scan for')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--image-list', required=False, default='', help='Comma/space-separated list of images to scan, empty to scan all'
)
@pass_runtime
@click_coroutine
async def okd_scan(runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref: str, image_list: str):
    """
    Scan OKD image sources for changes and trigger builds when needed.
    """
    # KUBECONFIG env var must be defined in order to scan sources
    if not os.getenv('KUBECONFIG'):
        raise RuntimeError('Environment variable KUBECONFIG must be defined')

    jenkins.init_jenkins()

    pipeline = OkdScanPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        image_list=image_list,
    )

    if runtime.dry_run:
        await pipeline.run()

    else:
        lock = Lock.SCAN_OKD
        lock_name = lock.value.format(version=version)
        lock_identifier = jenkins.get_build_path_or_random()

        # Scheduled builds are already being skipped if the lock is already acquired.
        # For manual builds, we need to check if the build and scan locks are already acquired,
        # and skip the current build if that's the case.
        # Should that happen, signal it by appending a [SKIPPED][LOCKED] to the build title
        async def run_with_build_lock():
            build_lock = Lock.BUILD_OKD
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
