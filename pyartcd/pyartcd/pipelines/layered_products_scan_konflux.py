import asyncio
import logging
import os
import re
from typing import Dict, List, Optional, Set

import click
import yaml
from artcommonlib import exectools
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import product_version_from_group_name

from pyartcd import constants, jenkins, locks, tekton, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.stage_registry import check_bundle_in_stage_registry, resolve_stage_release_plan
from pyartcd.util import load_group_config


class LayeredProductsScanPipeline:
    def __init__(self, runtime, group, data_path, assembly, data_gitref, image_list):
        self.runtime = runtime
        self.group = group
        self.data_path = data_path
        self.assembly = assembly
        self.data_gitref = data_gitref
        self.image_list = image_list

        self.logger = logging.getLogger(__name__)
        self._doozer_working = self.runtime.working_dir / "doozer_working"
        self.report = {}
        self.changes = {}

        self.skipped = True  # True by default; if not locked, run() will set it to False

        # Track which images are being rebuilt due to source changes (for dedup)
        self.rebuilding_images: List[str] = []

        # Track operators needing bundle/FBC builds
        self.operators_without_bundles: List[KonfluxBuildRecord] = []
        self.operators_without_fbcs: List[KonfluxBuildRecord] = []
        self.operators_needing_stage_release: List[KonfluxBuildRecord] = []  # Bundles built but not stage-released

        # Populated by load_operator_names(): distgit_key -> bundle short name
        self.bundle_names_by_operator: Dict[str, str] = {}

        # Populated by load_operator_names(): distgit_key -> bundle delivery repo name
        self.delivery_repo_by_operator: Dict[str, str] = {}

        # Resolved in _init_stage_release_check(): True when assembly is 'stream'
        # and the product has an FBC advisory-stage ReleasePlan for this version.
        self._check_stage_release = False

        # OCP target versions for FBC builds (loaded from group config)
        self.ocp_target_versions: List[str] = []

        # Provisional MAJOR.MINOR from group name; overridden by group.yml
        # ``version`` in load_ocp_target_versions() (same source BLP uses for NVRs).
        version_tuple = product_version_from_group_name(self.group)
        self.version: Optional[str] = f'{version_tuple[0]}.{version_tuple[1]}' if version_tuple else None

        group_param = self.group
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'
        self.doozer_base_command = [
            'doozer',
            f'--working-dir={self._doozer_working}',
            f'--data-path={self.data_path}',
            f'--group={group_param}',
            f'--assembly={self.assembly}',
            '--build-system=konflux',
        ]

    async def run(self):
        # If we get here, lock could be acquired
        self.skipped = False
        scan_info = (
            f'Scanning layered products group {self.group}, assembly {self.assembly}, data path {self.data_path}'
        )

        if self.data_gitref:
            scan_info += f'@{self.data_gitref}'
        self.logger.info(scan_info)

        self.check_params()

        # Scan for changes (layered products only have images/RPMs, no RHCOS)
        await self.get_changes()

        # Handle image source changes
        self.handle_source_changes()

        # Check for operators with missing bundle/FBC builds
        await self.scan_for_missing_bundles_fbcs()

    def check_params(self):
        """
        Make sure non-stream assemblies, custom forks and branches are only used with dry run mode
        """

        if not self.runtime.dry_run:
            if self.assembly != 'stream':
                raise ValueError('non-stream assemblies are only allowed in dry-run mode')
            if self.data_path != constants.OCP_BUILD_DATA_URL or self.data_gitref:
                raise ValueError('Custom data paths can only be used in dry-run mode')

    async def get_changes(self):
        """
        Check for changes by calling doozer config:scan-sources
        Changed rpms and images are recorded in self.changes
        """

        cmd = self.doozer_base_command.copy()
        if self.image_list:
            cmd.append(f'--images={self.image_list}')
        cmd.extend(
            [
                'beta:config:konflux:scan-sources',
                '--yaml',
                f'--ci-kubeconfig={os.environ["KUBECONFIG"]}',
                '--rebase-priv',
            ]
        )
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        self.logger.info('scan-sources output for %s:\n%s', self.group, out)

        self.report = yaml.safe_load(out)
        self.changes = util.get_changes(self.report)
        if self.changes:
            self.logger.info('Detected source changes:\n%s', yaml.safe_dump(self.changes))
        else:
            self.logger.info('No changes detected in layered products RPMs or images')

    def handle_source_changes(self):
        if not self.changes:
            return

        self.logger.info('Detected at least one updated layered product image')

        # Filter out images that reference disabled dependencies
        # (e.g. an operator whose CSV references an operand with mode: disabled)
        image_list = [
            image['name']
            for image in self.report.get('images', [])
            if image.get('changed') and not image.get('has_disabled_dependency')
        ]

        skipped_images = [
            image['name']
            for image in self.report.get('images', [])
            if image.get('changed') and image.get('has_disabled_dependency')
        ]
        if skipped_images:
            self.logger.warning('Skipping images with disabled dependencies: %s', skipped_images)

        # Track which images are being rebuilt (for deduplication with bundle/FBC scan)
        self.rebuilding_images = image_list.copy()

        if not image_list:
            self.logger.info('No buildable images after filtering disabled dependencies')
            return

        if self.runtime.dry_run:
            self.logger.info('Would have triggered a layered products %s build', self.group)
            return

        self.logger.info('Triggering a layered products %s build', self.group)

        if tekton.is_tekton_context():
            created_name = tekton.start_pipeline_run(
                pipeline_name="build-layered-products",
                params={
                    "group": self.group,
                    "assembly": self.assembly,
                    "image-list": ",".join(image_list) if image_list else "",
                },
            )
            if created_name:
                tekton.annotate_current_pipelinerun(
                    {
                        "art.openshift.io/triggered-build-layered-products": created_name,
                    }
                )
        else:
            jenkins.update_title(' [SOURCE CHANGES]')
            jenkins.update_description(f'Changed {len(image_list)} layered product images<br/>')
            jenkins.start_layered_products(
                group=self.group,
                assembly=self.assembly,
                image_list=image_list,
            )

    # ---- Scan-operator logic for LP groups ----

    async def scan_for_missing_bundles_fbcs(self):
        """Check for operators with missing bundle/FBC builds and trigger them.

        This replicates the scan-operator logic for layered product groups.
        Operators that are already being rebuilt due to source changes are
        skipped (deduplication).
        """

        # Load group.yml (OCP_TARGET_VERSIONS + authoritative product version) first
        await self.load_ocp_target_versions()

        if not self.version:
            self.logger.info('Could not determine version for group %s; skipping bundle/FBC scan', self.group)
            return

        # Initialize Konflux DB clients
        operator_db = KonfluxDb()
        operator_db.bind(KonfluxBuildRecord)

        bundle_db = KonfluxDb()
        bundle_db.bind(KonfluxBundleBuildRecord)

        fbc_db = KonfluxDb()
        fbc_db.bind(KonfluxFbcBuildRecord)

        operator_names = await self.load_operator_names()
        self.logger.info('Found %d operators in group %s', len(operator_names), self.group)

        if not operator_names:
            self.logger.info('No operators found in group %s; skipping bundle/FBC scan', self.group)
            return

        # FBCs must only reference bundles that already reached the stage registry
        await self._init_stage_release_check()

        operator_builds = await self.get_latest_operator_builds(operator_db, operator_names)
        self.logger.info('Found %d operators with latest successful builds', len(operator_builds))

        if not operator_builds:
            self.logger.info('No successful operator builds found')
            return

        # Check each operator for missing bundles/FBCs
        results = await asyncio.gather(
            *[self.check_operator(bundle_db, fbc_db, op) for op in operator_builds],
            return_exceptions=True,
        )

        check_errors = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error('Failed to process %s: %s', operator_builds[i].nvr, result)
                check_errors.append(result)

        # Track failures to trigger bundle/fbc jobs
        trigger_errors = []

        if self.operators_without_bundles:
            try:
                self.trigger_bundle_builds(self.operators_without_bundles)
            except Exception as e:
                trigger_errors.append(e)
                self.logger.error('Failed to trigger bundle builds: %s', e)

        if self.operators_needing_stage_release:
            try:
                self.trigger_bundle_builds(self.operators_needing_stage_release, force_release=True)
            except Exception as e:
                trigger_errors.append(e)
                self.logger.error('Failed to trigger force-release bundle builds: %s', e)

        if self.operators_without_fbcs:
            try:
                self.trigger_fbc_builds(self.operators_without_fbcs)
            except Exception as e:
                trigger_errors.append(e)
                self.logger.error('Failed to trigger FBC builds: %s', e)

        all_errors = check_errors + trigger_errors
        if all_errors:
            raise RuntimeError(f"Errors during bundle/FBC scan: {', '.join(map(str, all_errors))}")

    async def load_operator_names(self) -> Set[str]:
        """Load operator image names from ocp-build-data using doozer.

        Returns distgit keys (metadata names) like 'oadp-operator'.
        Populates ``self.bundle_names_by_operator`` with each operator's
        bundle short name (honoring any ``bundle_name_override`` config),
        and ``self.delivery_repo_by_operator`` with each operator's bundle
        delivery repo name (e.g. 'rhacm2/acm-operator-bundle').
        """

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

    async def _init_stage_release_check(self):
        """Enable the stage delivery gate when this group's bundles are stage-released.

        Only ``stream`` bundles are stage-released, and only for products with an
        FBC advisory-stage ReleasePlan. When enabled, ``check_operator`` holds back
        the FBC build of any bundle missing from the stage registry and re-runs its
        stage release instead.
        """
        self._check_stage_release = False

        if self.assembly != 'stream':
            self.logger.info("Assembly is '%s' (not 'stream'); skipping stage release checks", self.assembly)
            return

        group_config = await load_group_config(
            group=self.group,
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        self._check_stage_release = bool(resolve_stage_release_plan(self.group, group_config, self.logger))

    async def load_ocp_target_versions(self):
        """Load group.yml fields needed for bundle/FBC triggers.

        Prefer ``version`` from group config (same source BLP uses) so rescheduled
        FBC builds get the full semver (e.g. kueue ``1.4.4``) rather than the
        group-name MAJOR.MINOR padded to ``.0``. Also loads ``OCP_TARGET_VERSIONS``
        for multi-version FBC builds.
        """

        try:
            group_config = await load_group_config(
                group=self.group,
                assembly=self.assembly,
                doozer_data_path=self.data_path,
                doozer_data_gitref=self.data_gitref,
            )

            cfg_version = group_config.get('version')
            if cfg_version:
                self.version = str(cfg_version)
                self.logger.info('Using version %s from group config', self.version)
            elif self.version:
                self.logger.info('No version in group config; keeping %s from group name', self.version)
            else:
                self.logger.warning('No version in group config or group name for %s', self.group)

            ocp_versions = group_config.get('OCP_TARGET_VERSIONS')
            if ocp_versions:
                # Handle string values: wrap in a list to avoid iterating characters
                if isinstance(ocp_versions, str):
                    self.logger.warning('OCP_TARGET_VERSIONS is a string (%r); wrapping in a list', ocp_versions)
                    ocp_versions = [ocp_versions]

                ocp_version_re = re.compile(r'^[0-9]+\.[0-9]+$')
                valid_versions = []
                for v in ocp_versions:
                    if isinstance(v, str) and ocp_version_re.fullmatch(v):
                        if v not in valid_versions:
                            valid_versions.append(v)
                    else:
                        self.logger.warning('Ignoring invalid OCP_TARGET_VERSION entry: %r', v)

                self.ocp_target_versions = valid_versions
                self.logger.info('OCP_TARGET_VERSIONS from group config: %s', self.ocp_target_versions)
            else:
                self.logger.info('No OCP_TARGET_VERSIONS defined for group %s', self.group)
        except Exception:
            self.logger.error(
                'Failed to load group config for OCP_TARGET_VERSIONS; cannot determine FBC target versions',
                exc_info=True,
            )
            raise

    async def get_latest_operator_builds(
        self, operator_db: KonfluxDb, operator_names: Set[str]
    ) -> List[KonfluxBuildRecord]:
        """Get the latest successful build for each operator."""

        tasks = [
            operator_db.get_latest_build(
                name=operator_name,
                group=self.group,
                assembly=self.assembly,
                outcome=KonfluxBuildOutcome.SUCCESS,
                exclude_large_columns=True,
            )
            for operator_name in operator_names
        ]

        builds = await asyncio.gather(*tasks)
        return [build for build in builds if build]

    async def check_operator(self, bundle_db: KonfluxDb, fbc_db: KonfluxDb, operator: KonfluxBuildRecord):
        """Check one operator for missing bundle/FBC builds.

        Operators that are already being rebuilt due to source changes
        are skipped (deduplication).
        """

        # Deduplication: skip operators already being rebuilt from source changes
        if operator.name in self.rebuilding_images:
            self.logger.info(
                'Skipping bundle/FBC check for %s: already being rebuilt due to source changes', operator.name
            )
            return

        bundle = await self.check_bundle_exists(bundle_db, operator)

        if bundle is None or bundle.outcome.is_failure():
            self.operators_without_bundles.append(operator)
        elif bundle.outcome == KonfluxBuildOutcome.SUCCESS:
            if self._check_stage_release:
                is_released = await self.check_stage_registry(operator, bundle)
                if is_released is False:
                    self.operators_needing_stage_release.append(operator)
                    # Stage release is a prerequisite for FBC; skip FBC check
                    return
                if is_released is None:
                    # An unavailable registry is not proof that release happened.
                    return

            fbc = await self.check_fbc_exists(fbc_db, operator, bundle)
            if fbc is None or fbc.outcome.is_failure():
                self.operators_without_fbcs.append(operator)
        # If bundle.outcome == PENDING, do nothing (auto-triggers when complete)

    async def check_stage_registry(
        self, operator: KonfluxBuildRecord, bundle: KonfluxBundleBuildRecord
    ) -> Optional[bool]:
        """Check if the operator's bundle image exists in the stage registry.

        Returns ``True`` if the image exists, ``False`` if confirmed missing,
        or ``None`` if the check is inconclusive.
        """
        return await check_bundle_in_stage_registry(
            self.delivery_repo_by_operator.get(operator.name),
            bundle.image_pullspec,
            operator.nvr,
            self.logger,
        )

    async def check_bundle_exists(
        self, bundle_db: KonfluxDb, operator: KonfluxBuildRecord
    ) -> Optional[KonfluxBundleBuildRecord]:
        """Check if bundle build exists for operator."""

        bundle_name = self.get_bundle_name(operator.name)

        # Check for successful bundle
        bundle = await bundle_db.get_latest_build(
            name=bundle_name,
            group=self.group,
            outcome=KonfluxBuildOutcome.SUCCESS,
            assembly=self.assembly,
            extra_patterns={'operator_nvr': operator.nvr},
        )

        if bundle:
            self.logger.info('  Bundle for %s exists: %s', operator.nvr, bundle.nvr)
            return bundle

        # Check for the most recent PENDING build
        pending = await bundle_db.get_latest_build(
            name=bundle_name,
            group=self.group,
            outcome=KonfluxBuildOutcome.PENDING,
            assembly=self.assembly,
            extra_patterns={'operator_nvr': operator.nvr},
        )

        if pending:
            # Check if a failure record with the same NVR exists
            async for failed_build in bundle_db.search_builds_by_fields(
                where={'nvr': pending.nvr, 'outcome': [o for o in KonfluxBuildOutcome if o.is_failure()]},
                limit=1,
            ):
                self.logger.info(
                    '  Bundle build for %s failed (%s): %s', operator.nvr, failed_build.outcome.value, failed_build.nvr
                )
                return failed_build

            self.logger.info('  Bundle build for %s in progress: %s', operator.nvr, pending.nvr)
            return pending

        self.logger.info('  Bundle MISSING for %s', operator.nvr)
        return None

    async def check_fbc_exists(
        self, fbc_db: KonfluxDb, operator: KonfluxBuildRecord, bundle: KonfluxBundleBuildRecord
    ) -> Optional[KonfluxFbcBuildRecord]:
        """Check if FBC build exists containing this operator's bundle.

        When ``self.ocp_target_versions`` is configured, verifies that a
        successful FBC exists for EACH target version (matched via the
        ``.ocp{major}.{minor}`` marker in the NVR).  Returns ``None`` if
        any version is missing, which causes ``trigger_fbc_builds`` to
        rebuild all versions (idempotent for those already built).
        """

        fbc_name = self.get_fbc_name(operator.name)

        if self.ocp_target_versions:
            return await self._check_fbc_per_version(fbc_db, fbc_name, operator, bundle)

        # No target versions configured – original single-check behaviour
        # Query FBC records containing this bundle NVR
        async for fbc in fbc_db.search_builds_by_fields(
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
            self.logger.info('  FBC build for %s exists: %s', operator.nvr, fbc.nvr)
            return fbc

        # Check for the most recent PENDING build
        async for fbc in fbc_db.search_builds_by_fields(
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
            async for failed_build in fbc_db.search_builds_by_fields(
                where={'nvr': fbc.nvr, 'outcome': [o for o in KonfluxBuildOutcome if o.is_failure()]},
                limit=1,
            ):
                self.logger.info(
                    '  FBC build for %s failed (%s): %s', operator.nvr, failed_build.outcome.value, failed_build.nvr
                )
                return failed_build

            self.logger.info('  FBC build for %s in progress: %s', operator.nvr, fbc.nvr)
            return fbc

        self.logger.info('  FBC MISSING for operator %s', operator.nvr)
        return None

    async def _check_fbc_per_version(
        self,
        fbc_db: KonfluxDb,
        fbc_name: str,
        operator: KonfluxBuildRecord,
        bundle: KonfluxBundleBuildRecord,
    ) -> Optional[KonfluxFbcBuildRecord]:
        """Check FBC existence for each OCP target version.

        Checks ALL configured versions before returning.  Prioritises
        "missing" over "pending": if ANY version has no FBC at all,
        returns ``None`` so the operator is flagged for rebuild
        (idempotent for versions already built).  If all versions have
        an FBC but some are still pending, returns the pending record.
        A failed record is returned immediately when found.
        """

        missing_any = False
        pending_record: Optional[KonfluxFbcBuildRecord] = None
        success_record: Optional[KonfluxFbcBuildRecord] = None

        for target_version in self.ocp_target_versions:
            version_pattern = r'\.ocp' + re.escape(target_version) + r'$'

            # Check for successful FBC for this version
            success_found = False
            async for fbc in fbc_db.search_builds_by_fields(
                where={
                    'name': fbc_name,
                    'group': self.group,
                    'outcome': KonfluxBuildOutcome.SUCCESS,
                    'assembly': self.assembly,
                },
                array_contains={'bundle_nvrs': bundle.nvr},
                extra_patterns={'nvr': version_pattern},
                limit=1,
                order_by='start_time',
                sorting='DESC',
            ):
                self.logger.info(
                    '  FBC for %s (ocp-target-version=%s) exists: %s', operator.nvr, target_version, fbc.nvr
                )
                success_record = fbc
                success_found = True

            if success_found:
                continue

            # No success for this version – check for pending build
            pending_found = False
            async for pending_fbc in fbc_db.search_builds_by_fields(
                where={
                    'name': fbc_name,
                    'group': self.group,
                    'outcome': KonfluxBuildOutcome.PENDING,
                    'assembly': self.assembly,
                },
                array_contains={'bundle_nvrs': bundle.nvr},
                extra_patterns={'nvr': version_pattern},
                limit=1,
                order_by='start_time',
                sorting='DESC',
            ):
                # Check if this pending build has a failure record
                async for failed_build in fbc_db.search_builds_by_fields(
                    where={'nvr': pending_fbc.nvr, 'outcome': [o for o in KonfluxBuildOutcome if o.is_failure()]},
                    limit=1,
                ):
                    self.logger.info(
                        '  FBC build for %s (ocp-target-version=%s) failed (%s): %s',
                        operator.nvr,
                        target_version,
                        failed_build.outcome.value,
                        failed_build.nvr,
                    )
                    return failed_build

                self.logger.info(
                    '  FBC build for %s (ocp-target-version=%s) in progress: %s',
                    operator.nvr,
                    target_version,
                    pending_fbc.nvr,
                )
                pending_record = pending_fbc
                pending_found = True

            if not pending_found:
                self.logger.info('  FBC MISSING for operator %s (ocp-target-version=%s)', operator.nvr, target_version)
                missing_any = True

        if missing_any:
            return None
        if pending_record:
            return pending_record

        self.logger.info('  FBC builds for %s exist for all target versions', operator.nvr)
        return success_record

    def trigger_bundle_builds(self, operators: List[KonfluxBuildRecord], force_release: bool = False):
        """Trigger bundle builds for multiple operators in one job.

        :param operators: Operator builds whose bundles to build
        :param force_release: Re-run the related-images stage release even when the bundles already exist
        """

        nvrs = [op.nvr for op in operators]
        label = ' (force-release)' if force_release else ''

        if self.runtime.dry_run:
            self.logger.info(
                '[DRY-RUN] Would trigger bundle builds%s for %d operators: %s', label, len(nvrs), ', '.join(nvrs)
            )
            return

        self.logger.info('Triggering bundle builds%s for %d operators', label, len(nvrs))
        if tekton.is_tekton_context():
            params = {
                'version': self.version,
                'assembly': self.assembly,
                'group': self.group,
                'operator-nvrs': ','.join(nvrs),
                'data-path': self.data_path or '',
                'data-gitref': self.data_gitref or '',
                'dry-run': 'false',
            }
            if force_release:
                params['force-release'] = 'true'
            created_name = tekton.start_pipeline_run(pipeline_name='olm-bundle-konflux', params=params)
            if created_name:
                annotation = 'art.openshift.io/triggered-olm-bundle-konflux'
                if force_release:
                    annotation += '-force-release'
                tekton.annotate_current_pipelinerun({annotation: created_name})
        else:
            jenkins.start_olm_bundle_konflux(
                build_version=self.version,
                assembly=self.assembly,
                operator_nvrs=nvrs,
                group=self.group,
                doozer_data_path=self.data_path or '',
                doozer_data_gitref=self.data_gitref or '',
                force_release=force_release,
            )

    def trigger_fbc_builds(self, operators: List[KonfluxBuildRecord]):
        """Trigger FBC builds for operators, one per OCP target version.

        When ``self.ocp_target_versions`` is populated (from the group
        config's ``OCP_TARGET_VERSIONS``), a separate ``build-fbc``
        PipelineRun is started for each target version – mirroring the
        olm-bundle-konflux pattern.  Without target versions the method
        raises ``RuntimeError``.
        """

        nvrs = [op.nvr for op in operators]

        if self.runtime.dry_run:
            self.logger.info('[DRY-RUN] Would trigger FBC builds for %d operators: %s', len(nvrs), ', '.join(nvrs))
            return

        self.logger.info('Triggering FBC builds for %d operators', len(nvrs))

        if self.ocp_target_versions:
            self.logger.info('Starting FBC builds for OCP target versions: %s', self.ocp_target_versions)
            for target_version in self.ocp_target_versions:
                self.logger.info('Triggering build-fbc for ocp-target-version=%s', target_version)
                if tekton.is_tekton_context():
                    created_name = tekton.start_pipeline_run(
                        pipeline_name='build-fbc',
                        params={
                            'version': self.version,
                            'assembly': self.assembly,
                            'group': self.group,
                            'operator-nvrs': ','.join(nvrs),
                            'data-path': self.data_path or '',
                            'data-gitref': self.data_gitref or '',
                            'dry-run': 'false',
                            'ocp-target-version': target_version,
                            'force': 'true',
                        },
                    )
                    if created_name:
                        tekton.annotate_current_pipelinerun(
                            {f'art.openshift.io/triggered-build-fbc-{target_version}': created_name}
                        )
                else:
                    jenkins.start_build_fbc(
                        version=self.version,
                        assembly=self.assembly,
                        operator_nvrs=nvrs,
                        dry_run=False,
                        group=self.group,
                        ocp_target_version=target_version,
                        force_build=True,
                    )
        else:
            self.logger.error(
                'No OCP_TARGET_VERSIONS configured for group %s; '
                'FBC builds cannot be triggered without target versions for non-OpenShift groups',
                self.group,
            )
            raise RuntimeError(
                f'No OCP_TARGET_VERSIONS configured for group {self.group}; '
                'cannot trigger FBC builds without target versions for non-OpenShift groups'
            )

    def get_bundle_name(self, operator_name: str) -> str:
        """Get bundle name from operator name.

        Uses the bundle short name resolved by ``load_operator_names()``,
        which honors any ``bundle_name_override`` config. Falls back to the
        default ``'{operator_name}-bundle'`` pattern.
        """
        return self.bundle_names_by_operator.get(operator_name, f'{operator_name}-bundle')

    def get_fbc_name(self, operator_name: str) -> str:
        """Get FBC name from operator name."""
        return f'{operator_name}-fbc'


@cli.command('layered-products-scan')
@click.option('--group', required=True, help='Layered products group to scan (e.g., acm-5.0)')
@click.option('--assembly', required=False, default='stream', help='Assembly to scan for')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option('--image-list', required=False, help='Comma/space-separated list to of images to scan, empty to scan all')
@pass_runtime
@click_coroutine
async def layered_products_scan(
    runtime: Runtime, group: str, assembly: str, data_path: str, data_gitref, image_list: str
):
    if not os.getenv('KUBECONFIG'):
        if tekton.is_tekton_context():
            # Layered product groups don't start with 'openshift-', so doozer's
            # RHCOS detection (the only consumer of --ci-kubeconfig) is skipped.
            os.environ['KUBECONFIG'] = ''
        else:
            raise RuntimeError('Environment variable KUBECONFIG must be defined')

    if not tekton.is_tekton_context():
        jenkins.init_jenkins()

    pipeline = LayeredProductsScanPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        image_list=image_list,
    )

    if runtime.dry_run:
        await pipeline.run()

    else:
        lock = Lock.LAYERED_PRODUCTS_SCAN
        lock_name = lock.value.format(group=group)
        lock_identifier = jenkins.get_build_path_or_random()

        # Scheduled builds are already being skipped if the lock is already acquired.
        # For manual builds, we need to check if the build and scan locks are already acquired,
        # and skip the current build if that's the case.
        # Should that happen, signal it by appending a [SKIPPED][LOCKED] to the build title
        async def run_with_build_lock():
            build_lock = Lock.LAYERED_PRODUCTS_BUILD
            build_lock_name = build_lock.value.format(group=group)
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

    if pipeline.skipped and not tekton.is_tekton_context():
        jenkins.update_title(' [SKIPPED][LOCKED]')
