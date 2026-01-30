import asyncio
import logging
import os
from pathlib import Path
from typing import List, Optional, Set, Tuple

import click
from artcommonlib import exectools
from artcommonlib.util import uses_konflux_imagestream_override
from artcommonlib.konflux.konflux_build_record import (
    ArtifactType,
    Engine,
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import LARGE_COLUMNS, KonfluxDb

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime


class ScanOperatorPipeline:
    """Pipeline to scan for missing operator bundle/FBC builds."""

    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        data_path: str,
        data_gitref: str,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.group = f'openshift-{version}'

        self.logger = logging.getLogger(__name__)
        self._doozer_working = self.runtime.working_dir / "doozer_working"

        # Initialize Konflux DB clients
        self.operator_db = KonfluxDb()
        self.operator_db.bind(KonfluxBuildRecord)

        self.bundle_db = KonfluxDb()
        self.bundle_db.bind(KonfluxBundleBuildRecord)

        self.fbc_db = KonfluxDb()
        self.fbc_db.bind(KonfluxFbcBuildRecord)

        self.skipped = True  # Set to False if lock acquired

        # Track operators needing builds
        self.operators_without_bundles = []
        self.operators_without_fbcs = []

        # Build doozer base command for metadata loading
        group_param = f'{self.group}'
        if data_gitref:
            group_param += f'@{data_gitref}'
        self.doozer_base_command = [
            'doozer',
            f'--working-dir={self._doozer_working}',
            f'--data-path={data_path}',
            f'--group={group_param}',
            f'--assembly={assembly}',
            '--build-system=konflux',
        ]

    async def run(self):
        """Main pipeline execution."""
        self.skipped = False
        self.logger.info(f'Scanning version {self.version}, assembly {self.assembly}')

        # Check if it's a valid version
        if not uses_konflux_imagestream_override(self.version):
            self.logger.info(
                f'Version {self.version} is not a valid version'
            )
            return

        self.check_params()

        operator_names = await self.load_operator_names()
        self.logger.info(f'Found {len(operator_names)} operators in group {self.group}')

        operator_builds = await self.get_latest_operator_builds(operator_names)
        self.logger.info(f'Found {len(operator_builds)} operator builds with latest successful builds')

        if not operator_builds:
            self.logger.info('No operator builds found')
            return

        # 3. Check each operator in parallel
        results = await asyncio.gather(*[self.check_operator(op) for op in operator_builds], return_exceptions=True)

        # 4. Log any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f'Failed to process {operator_builds[i].nvr}: {result}')

        # 5. Trigger batched builds
        if self.operators_without_bundles:
            await self.trigger_bundle_builds(self.operators_without_bundles)

        if self.operators_without_fbcs:
            await self.trigger_fbc_builds(self.operators_without_fbcs)

    def check_params(self):
        """Validate pipeline parameters."""
        if not self.runtime.dry_run:
            if self.assembly != 'stream':
                raise ValueError('non-stream assemblies are only allowed in dry-run mode')
            if self.data_path != constants.OCP_BUILD_DATA_URL or self.data_gitref:
                raise ValueError('Custom data paths can only be used in dry-run mode')

    async def load_operator_names(self) -> Set[str]:
        """Load operator image names from ocp-build-data using doozer.

        Returns distgit keys (metadata names) like 'dpu-operator', not component names.
        """
        # Use doozer to list operator distgit keys)
        cmd = self.doozer_base_command + ['olm-bundle:list-olm-operators', '--output-format', 'distgit-key']

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        operator_names = set(out.strip().split('\n')) if out.strip() else set()

        return operator_names

    async def get_latest_operator_builds(self, operator_names: Set[str]) -> List[KonfluxBuildRecord]:
        """Get the latest successful build for each operator."""
        tasks = [
            self.operator_db.get_latest_build(
                name=operator_name,
                group=self.group,
                assembly=self.assembly,
                outcome=KonfluxBuildOutcome.SUCCESS,
            )
            for operator_name in operator_names
        ]

        builds = await asyncio.gather(*tasks)

        # Filter out None results
        operators = [build for build in builds if build]

        return operators

    async def check_operator(self, operator: KonfluxBuildRecord):
        """Check one operator for missing bundle/FBC builds."""
        # Check bundle
        bundle_exists, bundle = await self.check_bundle_exists(operator)
        if not bundle_exists:
            self.operators_without_bundles.append(operator)

        # Check FBC (only if bundle completed successfully)
        # Pending bundles will auto-trigger build-fbc job when their job completes
        if bundle_exists and bundle:
            fbc_exists, fbc_record = await self.check_fbc_exists(operator, bundle)
            if not fbc_exists:
                self.operators_without_fbcs.append(operator)

    async def check_bundle_exists(
        self, operator: KonfluxBuildRecord
    ) -> Tuple[bool, Optional[KonfluxBundleBuildRecord]]:
        """Check if bundle build exists for operator."""
        bundle_name = self.get_bundle_name(operator.name)

        # Check for successful bundle
        bundle = await self.bundle_db.get_latest_build(
            name=bundle_name,
            group=self.group,
            outcome=KonfluxBuildOutcome.SUCCESS,
            assembly=self.assembly,
            extra_patterns={'operator_nvr': operator.nvr},
        )

        if bundle:
            self.logger.info(f'  Bundle exists: {bundle.nvr}')
            return True, bundle

        # Check for pending bundle (avoid duplicate triggers)
        pending = await self.bundle_db.get_latest_build(
            name=bundle_name,
            group=self.group,
            outcome=KonfluxBuildOutcome.PENDING,
            assembly=self.assembly,
            extra_patterns={'operator_nvr': operator.nvr},
        )

        if pending:
            self.logger.info(f'  Bundle in progress: {pending.nvr}')
            return True, None  # Treat pending as if a bundle already exists

        self.logger.info(f'  Bundle MISSING for {operator.nvr}')
        return False, None

    async def check_fbc_exists(
        self, operator: KonfluxBuildRecord, bundle: Optional[KonfluxBundleBuildRecord]
    ) -> Tuple[bool, Optional[KonfluxFbcBuildRecord]]:
        """Check if FBC build exists containing this operator's bundle."""
        if not bundle:
            return False, None  # No bundle yet

        fbc_name = self.get_fbc_name(operator.name)

        # Query FBC records containing this bundle NVR
        async for fbc in self.fbc_db.search_builds_by_fields(
            where={
                'name': fbc_name,
                'group': self.group,
                'outcome': KonfluxBuildOutcome.SUCCESS,
                'assembly': self.assembly,
            },
            array_contains={'bundle_nvrs': bundle.nvr},
            limit=1,
            order_by='start_time',
            sorting='DESC',
        ):
            self.logger.info(f'  FBC exists: {fbc.nvr}')
            return True, fbc

        # Check for pending FBC
        async for fbc in self.fbc_db.search_builds_by_fields(
            where={
                'name': fbc_name,
                'group': self.group,
                'outcome': KonfluxBuildOutcome.PENDING,
                'assembly': self.assembly,
            },
            limit=1,
            order_by='start_time',
            sorting='DESC',
        ):
            self.logger.info(f'  FBC in progress: {fbc.nvr}')
            return True, None  # Treat pending as exists

        self.logger.info(f'  FBC MISSING for bundle {bundle.nvr}')
        return False, None

    async def trigger_bundle_builds(self, operators: List[KonfluxBuildRecord]):
        """Trigger bundle builds for multiple operators in one job."""
        nvrs = [op.nvr for op in operators]

        if self.runtime.dry_run:
            self.logger.info(f'[DRY-RUN] Would trigger bundle builds for {len(nvrs)} operators: {", ".join(nvrs)}')
            return

        try:
            self.logger.info(f'Triggering bundle builds for {len(nvrs)} operators')
            jenkins.start_olm_bundle_konflux(
                build_version=self.version,
                assembly=self.assembly,
                operator_nvrs=nvrs,
                group=self.group,
            )
        except Exception as e:
            self.logger.error(f'Failed to trigger bundle builds: {e}')

    async def trigger_fbc_builds(self, operators: List[KonfluxBuildRecord]):
        """Trigger FBC builds for multiple operators in one job."""
        nvrs = [op.nvr for op in operators]

        if self.runtime.dry_run:
            self.logger.info(f'[DRY-RUN] Would trigger FBC builds for {len(nvrs)} operators: {", ".join(nvrs)}')
            return

        try:
            self.logger.info(f'Triggering FBC builds for {len(nvrs)} operators')
            jenkins.start_build_fbc(
                version=self.version,
                assembly=self.assembly,
                operator_nvrs=nvrs,
                dry_run=False,
                group=self.group,
            )
        except Exception as e:
            self.logger.error(f'Failed to trigger FBC builds: {e}')

    def get_bundle_name(self, operator_name: str) -> str:
        """Get bundle name from operator name.

        Operator names are distgit keys (e.g., 'dpu-operator').
        Bundle names append -bundle suffix (e.g., 'dpu-operator-bundle').
        """
        return f'{operator_name}-bundle'

    def get_fbc_name(self, operator_name: str) -> str:
        """Get FBC name from operator name.

        Operator names are distgit keys (e.g., 'dpu-operator').
        FBC names append -fbc suffix (e.g., 'dpu-operator-fbc').
        """
        return f'{operator_name}-fbc'



@cli.command('scan-operator')
@click.option('--version', required=True, help='OCP version to scan')
@click.option('--assembly', required=False, default='stream', help='Assembly to scan')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use',
)
@click.option('--data-gitref', required=False, default='', help='Data path git ref')
@pass_runtime
@click_coroutine
async def scan_operator(runtime: Runtime, version: str, assembly: str, data_path: str, data_gitref: str):
    """Scan for missing operator bundle/FBC builds and trigger them."""

    jenkins.init_jenkins()

    pipeline = ScanOperatorPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
    )

    if runtime.dry_run:
        await pipeline.run()
    else:
        lock = Lock.SCAN_OPERATOR
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
            build_lock = Lock.BUILD_KONFLUX
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
