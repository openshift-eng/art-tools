import asyncio
import logging
from typing import Dict, List, Optional, Set

import click
from artcommonlib import exectools, redis
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import (
    product_version_from_group_name,
    resolve_konflux_fbc_stage_release_plan,
    uses_konflux_imagestream_override,
)

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config

# The stage/delivery registry where bundle images are published after stage release
STAGE_REGISTRY = "registry.redhat.io"


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
        self.operators_needing_stage_release = []  # Bundles built but not stage-released

        # Populated by load_operator_names(): distgit_key -> bundle short name
        # (honors any per-image `bundle_name_override` config)
        self.bundle_names_by_operator: Dict[str, str] = {}

        # Populated by load_operator_names(): distgit_key -> delivery repo name
        # (e.g., 'openshift4/ose-ptp-operator-bundle')
        self.delivery_repo_by_operator: Dict[str, str] = {}

        # Whether to check stage registry for bundle delivery.
        # Resolved in _init_stage_release_check(): True when assembly is 'stream'
        # and a stage release plan exists for the product/version.
        self._check_stage_release = False

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

        # TODO: Deprecate art-images-share check on Oct 31st 2026
        # Check if it's a valid version
        if not uses_konflux_imagestream_override(self.version):
            self.logger.info(f'Version {self.version} is not a valid version')
            return

        self.check_params()

        # Check if stage release checking is applicable
        await self._init_stage_release_check()

        operator_names = await self.load_operator_names()
        self.logger.info(f'Found {len(operator_names)} operators in group {self.group}')

        operator_builds = await self.get_latest_operator_builds(operator_names)
        self.logger.info(f'Found {len(operator_builds)} operator builds with latest successful builds')

        if not operator_builds:
            self.logger.info('No operator builds found')
            return

        # Check if each operator has an associated bundle and fbc
        results = await asyncio.gather(*[self.check_operator(op) for op in operator_builds], return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f'Failed to process {operator_builds[i].nvr}: {result}')

        # Track failures to trigger bundle/fbc jobs
        trigger_errors = []

        if self.operators_without_bundles:
            try:
                self.trigger_bundle_builds(self.operators_without_bundles)
            except Exception as e:
                trigger_errors.append(e)
                self.logger.error(f'Failed to trigger bundle builds: {e}')

        if self.operators_needing_stage_release:
            try:
                self.trigger_bundle_builds(self.operators_needing_stage_release, force_release=True)
            except Exception as e:
                trigger_errors.append(e)
                self.logger.error(f'Failed to trigger force-release bundle builds: {e}')

        if self.operators_without_fbcs:
            try:
                self.trigger_fbc_builds(self.operators_without_fbcs)
            except Exception as e:
                trigger_errors.append(e)
                self.logger.error(f'Failed to trigger FBC builds: {e}')

        # Raise at the end to mark job as failed
        if trigger_errors:
            raise RuntimeError(f"Failed to trigger bundle and/or FBC builds: {', '.join(map(str, trigger_errors))}")

    async def _init_stage_release_check(self):
        """Determine whether stage release checking is applicable for this assembly/product.

        Only ``stream`` assembly bundles are stage-released. When applicable,
        ``check_operator`` will verify that the bundle image exists in the
        stage registry (registry.redhat.io); operators whose bundle is missing
        are re-triggered with ``force_release=True``.
        """
        self._check_stage_release = False

        if self.assembly != 'stream':
            self.logger.info("Assembly is '%s' (not 'stream'); skipping stage release checks", self.assembly)
            return

        # Load group config to get product information
        group_config = await load_group_config(
            group=self.group,
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        product = group_config.get('product') or 'ocp'

        # Resolve product version from group name (preferred) or group config
        group_version = product_version_from_group_name(self.group)
        if group_version:
            product_major, product_minor = group_version
        else:
            version_str = group_config.get('version')
            if version_str:
                parts = str(version_str).split('.')
                product_major, product_minor = int(parts[0]), int(parts[1])
            else:
                vars_section = group_config.get('vars', {})
                product_major = int(vars_section.get('MAJOR', 0))
                product_minor = int(vars_section.get('MINOR', 0))

        plan = resolve_konflux_fbc_stage_release_plan(product, product_major, product_minor)

        if not plan:
            self.logger.info(
                "No stage release plan configured for product '%s' (%d.%d); skipping stage release checks",
                product,
                product_major,
                product_minor,
            )
            return

        self._check_stage_release = True
        self.logger.info(
            "Stage release plan '%s' found for product '%s' (%d.%d); will check stage registry for bundle delivery",
            plan,
            product,
            product_major,
            product_minor,
        )

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

        As a side effect, populates `self.bundle_names_by_operator` with each
        operator's bundle short name (honoring any `bundle_name_override` config),
        and `self.delivery_repo_by_operator` with each operator's bundle delivery
        repo name (e.g., 'openshift4/ose-ptp-operator-bundle'), for use by
        `get_bundle_name()` and `check_stage_registry()`.
        """
        # Use doozer to list operator distgit keys along with their bundle short names
        # and delivery repo names (tab-separated
        # "{distgit_key}\t{bundle_short_name}\t{delivery_repo_name}" triplets).
        cmd = self.doozer_base_command + ['olm-bundle:list-olm-operators', '--output-format', 'delivery-info']

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        self.bundle_names_by_operator = {}
        self.delivery_repo_by_operator = {}
        for line in out.strip().split('\n') if out.strip() else []:
            parts = line.split('\t')
            if len(parts) >= 2:
                distgit_key, bundle_name = parts[0], parts[1]
                if distgit_key and bundle_name:
                    self.bundle_names_by_operator[distgit_key] = bundle_name
            if len(parts) >= 3:
                distgit_key, delivery_repo = parts[0], parts[2]
                if distgit_key and delivery_repo:
                    self.delivery_repo_by_operator[distgit_key] = delivery_repo

        return set(self.bundle_names_by_operator.keys())

    async def get_latest_operator_builds(self, operator_names: Set[str]) -> List[KonfluxBuildRecord]:
        """Get the latest successful build for each operator."""
        tasks = [
            self.operator_db.get_latest_build(
                name=operator_name,
                group=self.group,
                assembly=self.assembly,
                outcome=KonfluxBuildOutcome.SUCCESS,
                exclude_large_columns=True,
            )
            for operator_name in operator_names
        ]

        builds = await asyncio.gather(*tasks)

        # Filter out None results
        operators = [build for build in builds if build]

        return operators

    async def check_operator(self, operator: KonfluxBuildRecord):
        """Check one operator for missing bundle/FBC builds."""
        bundle = await self.check_bundle_exists(operator)

        if bundle is None or bundle.outcome.is_failure():
            self.operators_without_bundles.append(operator)
        elif bundle.outcome == KonfluxBuildOutcome.SUCCESS:
            # Check stage release status if applicable
            if self._check_stage_release:
                is_released = await self.check_stage_registry(operator, bundle)
                if not is_released:
                    self.operators_needing_stage_release.append(operator)
                    # Stage release is a prerequisite for FBC; skip FBC check
                    return

            fbc = await self.check_fbc_exists(operator, bundle)
            if fbc is None or fbc.outcome.is_failure():
                self.operators_without_fbcs.append(operator)
        # If bundle.outcome == PENDING, do nothing (auto-triggers when complete)

    async def check_bundle_exists(self, operator: KonfluxBuildRecord) -> Optional[KonfluxBundleBuildRecord]:
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
            self.logger.info(f'  Bundle for {operator.nvr} exists: {bundle.nvr}')
            return bundle

        # Check for the most recent PENDING build
        pending = await self.bundle_db.get_latest_build(
            name=bundle_name,
            group=self.group,
            outcome=KonfluxBuildOutcome.PENDING,
            assembly=self.assembly,
            extra_patterns={'operator_nvr': operator.nvr},
        )

        if pending:
            # Check if a failure record with the same NVR exists
            async for failed_build in self.bundle_db.search_builds_by_fields(
                where={'nvr': pending.nvr, 'outcome': [o for o in KonfluxBuildOutcome if o.is_failure()]},
                limit=1,
            ):
                self.logger.info(
                    f'  Bundle build for {operator.nvr} failed ({failed_build.outcome.value}): {failed_build.nvr}'
                )
                return failed_build

            self.logger.info(f'  Bundle build for {operator.nvr} in progress: {pending.nvr}')
            return pending

        self.logger.info(f'  Bundle MISSING for {operator.nvr}')
        return None

    async def check_fbc_exists(
        self, operator: KonfluxBuildRecord, bundle: KonfluxBundleBuildRecord
    ) -> Optional[KonfluxFbcBuildRecord]:
        """Check if FBC build exists containing this operator's bundle."""
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
            self.logger.info(f'  FBC build for {operator.nvr} exists: {fbc.nvr}')
            return fbc

        # Check for the most recent PENDING build
        async for fbc in self.fbc_db.search_builds_by_fields(
            where={
                'name': fbc_name,
                'group': self.group,
                'outcome': KonfluxBuildOutcome.PENDING,
                'assembly': self.assembly,
            },
            array_contains={'bundle_nvrs': bundle.nvr},
            limit=1,
            order_by='start_time',
            sorting='DESC',
        ):
            # Check if a failure record with the same NVR exists
            async for failed_build in self.fbc_db.search_builds_by_fields(
                where={'nvr': fbc.nvr, 'outcome': [o for o in KonfluxBuildOutcome if o.is_failure()]},
                limit=1,
            ):
                self.logger.info(
                    f'  FBC build for {operator.nvr} failed ({failed_build.outcome.value}): {failed_build.nvr}'
                )
                return failed_build

            self.logger.info(f'  FBC build for {operator.nvr} in progress: {fbc.nvr}')
            return fbc

        self.logger.info(f'  FBC MISSING for operator {operator.nvr}')
        return None

    async def check_stage_registry(self, operator: KonfluxBuildRecord, bundle: KonfluxBundleBuildRecord) -> bool:
        """Check if a bundle image exists in the stage registry.

        Extracts the digest from the bundle's ``image_pullspec`` and checks
        whether ``registry.redhat.io/{delivery_repo}@{digest}`` exists using
        ``skopeo inspect --raw``.

        Returns ``True`` if the image exists (or if the check cannot be
        performed); ``False`` if the image is confirmed missing.
        """
        delivery_repo = self.delivery_repo_by_operator.get(operator.name)
        if not delivery_repo:
            self.logger.warning('  No delivery repo configured for %s; skipping stage release check', operator.name)
            return True  # Don't re-trigger if we can't check

        if not bundle.image_pullspec or '@' not in bundle.image_pullspec:
            self.logger.warning(
                '  No image digest in bundle pullspec for %s; skipping stage release check', operator.nvr
            )
            return True

        digest = bundle.image_pullspec.split('@', 1)[-1]
        stage_pullspec = f"docker://{STAGE_REGISTRY}/{delivery_repo}@{digest}"

        try:
            rc, _, err = await exectools.cmd_gather_async(
                ['skopeo', 'inspect', '--raw', stage_pullspec],
                check=False,
            )
            if rc == 0:
                self.logger.info(
                    '  Stage release for %s exists: %s/%s@%s', operator.nvr, STAGE_REGISTRY, delivery_repo, digest
                )
                return True
            if 'manifest unknown' in (err or '').lower():
                self.logger.info(
                    '  Stage release MISSING for %s: %s/%s@%s', operator.nvr, STAGE_REGISTRY, delivery_repo, digest
                )
                return False
            self.logger.warning('  Stage registry check inconclusive for %s (rc=%s): %s', operator.nvr, rc, err)
            return True

        except Exception as e:
            self.logger.warning('  Failed to check stage registry for %s: %s', operator.nvr, e)
            # Err on the side of caution: don't re-trigger if we can't check
            return True

    def trigger_bundle_builds(self, operators: List[KonfluxBuildRecord], force_release: bool = False):
        """Trigger bundle builds for multiple operators in one job."""
        nvrs = [op.nvr for op in operators]

        if self.runtime.dry_run:
            label = ' (force-release)' if force_release else ''
            self.logger.info(
                f'[DRY-RUN] Would trigger bundle builds{label} for {len(nvrs)} operators: {", ".join(nvrs)}'
            )
            return

        label = ' with force_release' if force_release else ''
        self.logger.info(f'Triggering bundle builds{label} for {len(nvrs)} operators')
        jenkins.start_olm_bundle_konflux(
            build_version=self.version,
            assembly=self.assembly,
            operator_nvrs=nvrs,
            group=self.group,
            force_release=force_release,
        )

    def trigger_fbc_builds(self, operators: List[KonfluxBuildRecord]):
        """Trigger FBC builds for multiple operators in one job."""
        nvrs = [op.nvr for op in operators]

        if self.runtime.dry_run:
            self.logger.info(f'[DRY-RUN] Would trigger FBC builds for {len(nvrs)} operators: {", ".join(nvrs)}')
            return

        self.logger.info(f'Triggering FBC builds for {len(nvrs)} operators')
        jenkins.start_build_fbc(
            version=self.version,
            assembly=self.assembly,
            operator_nvrs=nvrs,
            dry_run=False,
            group=self.group,
        )

    def get_bundle_name(self, operator_name: str) -> str:
        """Get bundle name from operator name.

        Uses the bundle short name resolved by `load_operator_names()`, which honors
        any `bundle_name_override` config. Falls back to the default '{operator_name}-bundle'
        pattern if the operator wasn't found in that mapping (e.g., in tests).
        """
        return self.bundle_names_by_operator.get(operator_name, f'{operator_name}-bundle')

    def get_fbc_name(self, operator_name: str) -> str:
        """Get FBC name from operator name.
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
        build_lock_name = Lock.BUILD_KONFLUX.value.format(version=version, assembly=assembly)
        lock_manager = locks.LockManager([redis.redis_url()])

        try:
            if await lock_manager.is_locked(build_lock_name):
                pipeline.logger.info(f'Locked on {build_lock_name}, skipping')
                pipeline.skipped = True
            else:
                lock = Lock.SCAN_OPERATOR
                lock_name = lock.value.format(version=version)
                lock_identifier = jenkins.get_build_path_or_random()

                await locks.run_with_lock(
                    coro=pipeline.run(),
                    lock=lock,
                    lock_name=lock_name,
                    lock_id=lock_identifier,
                    skip_if_locked=True,
                )
        finally:
            await lock_manager.destroy()

    if pipeline.skipped:
        jenkins.update_title(' [SKIPPED][LOCKED]')
