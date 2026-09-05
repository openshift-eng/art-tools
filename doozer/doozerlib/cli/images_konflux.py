import asyncio
import json
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import click
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.telemetry import start_as_current_span_async
from artcommonlib.util import (
    KubeCondition,
    normalize_k8s_dns_label,
    resolve_konflux_fbc_stage_release_plan,
    validate_build_priority,
)
from artcommonlib.variants import BuildVariant
from kubernetes.dynamic import exceptions as k8s_exceptions
from opentelemetry import trace

from doozerlib import constants, util
from doozerlib.backend.konflux_client import (
    API_VERSION,
    KIND_RELEASE,
    KIND_RELEASE_PLAN,
    KIND_SNAPSHOT,
    KonfluxClient,
)
from doozerlib.backend.konflux_fbc import get_referenced_images
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder, KonfluxImageBuilderConfig
from doozerlib.backend.konflux_olm_bundler import KonfluxOlmBundleBuilder, KonfluxOlmBundleRebaser
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli import (
    cli,
    click_coroutine,
    option_commit_message,
    option_push,
    pass_runtime,
    validate_semver_major_minor,
    validate_semver_major_minor_patch,
)
from doozerlib.exceptions import DoozerFatalError, ParentRebaseFailedError
from doozerlib.image import ImageMetadata
from doozerlib.lockfile_prototype.constants import LockfileBackend
from doozerlib.runtime import Runtime

TRACER = trace.get_tracer(__name__)
LOGGER = logging.getLogger(__name__)


def _validate_version(ctx, param, version):
    """
    Accept both X.Y (microshift-bootc) and X.Y.Z versions, preserving segment count.
    """
    if version is None or version == "auto":
        return version

    if len(version.split(".")) == 2:  # used by microshift-bootc
        return validate_semver_major_minor(ctx, param, version)

    return validate_semver_major_minor_patch(ctx, param, version)


class KonfluxRebaseCli:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        release: str,
        embargoed: bool,
        force_yum_updates: bool,
        repo_type: str,
        image_repo: str,
        message: str,
        push: bool,
        lockfile_seed_nvrs: Optional[List[str]] = None,
        extra_labels: Optional[dict[str, str]] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.embargoed = embargoed
        self.force_yum_updates = force_yum_updates
        if repo_type not in ['signed', 'unsigned']:
            raise click.BadParameter(f"repo_type must be one of 'signed' or 'unsigned'. Got: {repo_type}")
        self.repo_type = repo_type
        self.image_repo = image_repo
        self.message = message
        self.push = push
        self.extra_labels = extra_labels or {}
        self.upcycle = runtime.upcycle
        self.lockfile_seed_nvrs = lockfile_seed_nvrs

    @start_as_current_span_async(TRACER, "beta:images:konflux:rebase")
    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False, build_system='konflux')
        assert runtime.source_resolver is not None, "source_resolver is required for this command"
        metas = runtime.ordered_image_metas()

        # Update span name to include metas count
        span = trace.get_current_span()
        span.update_name(f"beta:images:konflux:rebase ({len(metas)}) images")
        span.set_attribute("doozer.images.count", len(metas))
        base_dir = Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_BUILD_SOURCES)
        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=base_dir,
            source_resolver=runtime.source_resolver,
            repo_type=self.repo_type,
            upcycle=self.upcycle,
            force_private_bit=self.embargoed,
            image_repo=self.image_repo,
            lockfile_seed_nvrs=self.lockfile_seed_nvrs,
            extra_labels=self.extra_labels,
        )

        art_internal_metas = [
            m for m in metas if m.get_lockfile_backend() != LockfileBackend.RPM_LOCKFILE_PROTOTYPE.value
        ]
        await rebaser.rpm_lockfile_generator.ensure_repositories_loaded(art_internal_metas, base_dir)

        tasks = []
        for image_meta in metas:
            tasks.append(
                asyncio.create_task(
                    rebaser.rebase_to(
                        image_meta,
                        self.version,
                        self.release,
                        force_yum_updates=self.force_yum_updates,
                        commit_message=self.message,
                        push=self.push,
                    )
                )
            )
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_images = []
        skipped_due_to_parent = []
        images_state = {}
        for index, result in enumerate(results):
            image_name = metas[index].distgit_key
            if isinstance(result, Exception):
                if isinstance(result, ParentRebaseFailedError):
                    skipped_due_to_parent.append(image_name)
                    images_state[image_name] = {'status': 'skipped'}
                    LOGGER.warning(
                        "Skipping rebase for %s: parent rebase(s) failed: %s", image_name, result.failed_parents
                    )
                else:
                    failed_images.append(image_name)
                    images_state[image_name] = {'status': 'failure'}
                    LOGGER.error(f"Failed to rebase {image_name}: {result}")
                    LOGGER.error(f"Stack trace for {image_name}:")
                    LOGGER.error(''.join(traceback.format_exception(type(result), result, result.__traceback__)))
            else:
                images_state[image_name] = {'status': 'success'}

        # Always write per-image state (for counter reset and diagnostics)
        state_payload = {'images': images_state}
        if failed_images:
            state_payload['failed-images'] = failed_images
        if skipped_due_to_parent:
            state_payload['skipped-due-to-parent-rebase-failure'] = skipped_due_to_parent
        runtime.state['images:konflux:rebase'] = state_payload

        if failed_images or skipped_due_to_parent:
            summary = failed_images + [f"{k} (parent failure)" for k in skipped_due_to_parent]
            raise DoozerFatalError(f"Failed to rebase images: {summary}")
        LOGGER.info("Rebase complete")


@cli.command("beta:images:konflux:rebase", short_help="Refresh a group's konflux source content from source content.")
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=_validate_version,
    help="Version string to populate in Dockerfiles.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@click.option(
    "--embargoed",
    is_flag=True,
    help="Add .p3 to the release string for all images, which indicates those images have embargoed fixes",
)
@click.option(
    "--force-yum-updates",
    is_flag=True,
    default=False,
    help="Inject \"yum update -y\" in the final stage of an image build. This ensures the component image will be able to override RPMs it is inheriting from its parent image using RPMs in the rebuild plashet.",
)
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default="unsigned",
    help="Repo group type to use (e.g. signed, unsigned).",
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Image repo for base images')
@click.option(
    '--network-mode',
    type=click.Choice(['hermetic', 'internal-only', 'open']),
    help='Override network mode for Konflux builds. Takes precedence over image and group config settings.',
)
@click.option(
    '--lockfile-seed-nvrs',
    default=None,
    metavar='NVRS',
    help='NVRs of builds whose installed RPMs should seed lockfile generation. '
    'The distgit key is resolved internally from the build DB. '
    'Format: NVR[,NVR,...]. '
    'Example: ironic-container-v4.22.0-assembly.test',
)
@click.option(
    '--extra-label',
    multiple=True,
    metavar='KEY=VALUE',
    help='Extra labels to add to the Dockerfile. Can be specified multiple times. e.g. --extra-label assembly=4.18.1',
)
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def images_konflux_rebase(
    runtime: Runtime,
    version: str,
    release: str,
    embargoed: bool,
    force_yum_updates: bool,
    repo_type: str,
    image_repo: str,
    network_mode: Optional[str],
    lockfile_seed_nvrs: Optional[str],
    extra_label: tuple,
    message: str,
    push: bool,
):
    """
    Refresh a group's konflux content from source content.
    """
    if network_mode:
        runtime.network_mode_override = network_mode

    parsed_seed_nvrs = None
    if lockfile_seed_nvrs:
        parsed_seed_nvrs = [nvr.strip() for nvr in lockfile_seed_nvrs.split(',') if nvr.strip()]

    # Parse extra labels from KEY=VALUE format
    extra_labels = {}
    for label in extra_label:
        if '=' not in label:
            raise click.BadParameter(f"Extra label must be in KEY=VALUE format, got: {label}")
        key, value = label.split('=', 1)
        extra_labels[key] = value

    cli = KonfluxRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        embargoed=embargoed,
        force_yum_updates=force_yum_updates,
        repo_type=repo_type,
        image_repo=image_repo,
        message=message,
        push=push,
        lockfile_seed_nvrs=parsed_seed_nvrs,
        extra_labels=extra_labels,
    )
    await cli.run()


class KonfluxBuildCli:
    def __init__(
        self,
        runtime: Runtime,
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: str,
        image_repo: str,
        registry_auth_file: str,
        skip_checks: bool,
        dry_run: bool,
        plr_template: str,
        build_priority: Optional[str],
        skip_ec_verify: bool = False,
        skip_tasks: tuple[str, ...] = (),
    ):
        self.runtime = runtime
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.image_repo = image_repo
        self.registry_auth_file = registry_auth_file
        self.skip_checks = skip_checks
        self.skip_tasks = skip_tasks
        self.dry_run = dry_run
        self.plr_template = plr_template
        self.build_priority = build_priority
        self.skip_ec_verify = skip_ec_verify

        validate_build_priority(self.build_priority)

    @start_as_current_span_async(TRACER, "images:konflux:build")
    async def run(self):
        runtime = self.runtime

        # OKD configuration is automatically merged in get_group_config() when variant=okd
        runtime.initialize(mode='images', clone_distgits=False)

        runtime.konflux_db.bind(KonfluxBuildRecord)
        assert runtime.source_resolver is not None, "source_resolver is not initialized. Doozer bug?"
        metas = runtime.ordered_image_metas()

        # Update span name to include metas count
        span = trace.get_current_span()
        span.update_name(f"images:konflux:build.{len(metas)}metas")
        span.set_attribute("doozer.images.count", len(metas))

        if self.runtime.variant is BuildVariant.OKD:
            major, minor = runtime.get_major_minor_fields()
            group = f'okd-{major}.{minor}'
        else:
            group = runtime.group

        if runtime.assembly == "test":
            ec_policy = constants.KONFLUX_TEST_EC_POLICY_CONFIGURATION
            prega_ec_policy = constants.KONFLUX_TEST_PREGA_EC_POLICY_CONFIGURATION
        else:
            ec_policy = constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
            prega_ec_policy = constants.KONFLUX_PREGA_EC_POLICY_CONFIGURATION

        config = KonfluxImageBuilderConfig(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_BUILD_SOURCES),
            group_name=group,
            kubeconfig=self.konflux_kubeconfig,
            context=self.konflux_context,
            namespace=self.konflux_namespace,
            image_repo=self.image_repo,
            registry_auth_file=self.registry_auth_file,
            skip_checks=self.skip_checks,
            skip_tasks=self.skip_tasks,
            dry_run=self.dry_run,
            plr_template=self.plr_template,
            build_priority=self.build_priority,
            ec_policy_configuration=ec_policy,
            prega_ec_policy_configuration=prega_ec_policy,
            skip_ec_verify=self.skip_ec_verify,
        )
        builder = KonfluxImageBuilder(config=config, record_logger=runtime.record_logger)

        # Mint a per-invocation GitHub App token and create a transient Secret.
        # All PipelineRuns in this batch share the same secret — no contention.
        git_auth_secret = await builder._konflux_client.ensure_git_auth_secret(
            namespace=self.konflux_namespace,
        )
        refresh_task = asyncio.create_task(builder._konflux_client.token_refresh_loop(namespace=self.konflux_namespace))

        tasks = []
        for image_meta in metas:
            tasks.append(asyncio.create_task(builder.build(image_meta, git_auth_secret=git_auth_secret)))
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            failed_images = []
            for index, result in enumerate(results):
                if isinstance(result, Exception):
                    image_name = metas[index].distgit_key
                    failed_images.append(image_name)
                    stack_trace = ''.join(traceback.TracebackException.from_exception(result).format())
                    LOGGER.error(f"Failed to build {image_name}: {result}; {stack_trace}")
        finally:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
            try:
                await builder._konflux_client.delete_git_auth_secret(
                    namespace=self.konflux_namespace,
                )
                await builder._konflux_client.cleanup_stale_git_auth_secrets(
                    namespace=self.konflux_namespace,
                )
            except Exception as e:
                LOGGER.warning("Failed to cleanup git-auth secrets: %s", e)

        if failed_images:
            raise DoozerFatalError(f"Failed to build images: {failed_images}")
        LOGGER.info("Build complete")


@cli.command("beta:images:konflux:build", short_help="Build images for the group.")
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Push images to the specified repo.')
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option(
    '--skip-task',
    'skip_tasks',
    multiple=True,
    help='Remove a named Tekton task from the PipelineRun. Repeatable (e.g. --skip-task clair-scan --skip-task sast-snyk-check).',
)
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option(
    '--plr-template',
    required=False,
    help='Use a custom PipelineRun template to build the image. Overrides the default template from openshift-priv/art-konflux-template or the value from group.yaml if it is set',
)
@click.option(
    '--build-priority',
    type=str,
    metavar='PRIORITY',
    default='auto',
    required=True,
    help='Kueue build priority. Use "auto" for automatic resolution from image/group config, or specify a number 1-10 (where 1 is highest priority). Takes precedence over group and image config settings.',
)
@click.option(
    '--network-mode',
    type=click.Choice(['hermetic', 'internal-only', 'open']),
    help='Override network mode for Konflux builds. Takes precedence over image and group config settings.',
)
@click.option(
    '--skip-ec-verify',
    default=False,
    is_flag=True,
    help='Skip enterprise-contract verification after builds.',
)
@pass_runtime
@click_coroutine
async def images_konflux_build(
    runtime: Runtime,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    image_repo: str,
    skip_checks: bool,
    skip_tasks: tuple,
    dry_run: bool,
    plr_template: Optional[str],
    build_priority: Optional[str],
    network_mode: Optional[str],
    skip_ec_verify: bool,
):
    if network_mode:
        runtime.network_mode_override = network_mode

    cli = KonfluxBuildCli(
        runtime=runtime,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        image_repo=image_repo,
        registry_auth_file=runtime.registry_config,
        skip_checks=skip_checks,
        skip_tasks=skip_tasks,
        dry_run=dry_run,
        plr_template=plr_template,
        build_priority=build_priority,
        skip_ec_verify=skip_ec_verify,
    )
    await cli.run()


class KonfluxBundleCli:
    def __init__(
        self,
        runtime: Runtime,
        operator_nvrs: Sequence[str],
        force: bool,
        dry_run: bool,
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: str,
        image_repo: str,
        skip_checks: bool,
        release: Optional[str],
        plr_template: str,
        output: str,
        skip_tasks: tuple[str, ...] = (),
    ):
        self.runtime = runtime
        self.operator_nvrs = list(operator_nvrs)
        self.force = force
        self.dry_run = dry_run
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.skip_tasks = skip_tasks
        self.release = release
        self.output = output
        self.plr_template = plr_template
        self._db_for_bundles = KonfluxDb()
        self._db_for_bundles.bind(KonfluxBundleBuildRecord)

    async def get_operator_builds(self):
        """Get build records for the given operator nvrs or latest build records for all operators.

        :return: A dictionary of operator name to build records.
        """
        runtime = self.runtime
        assert runtime.konflux_db is not None, "konflux_db is not initialized. Doozer bug?"
        assert runtime.assembly is not None, "assembly is not initialized. Doozer bug?"
        dgk_records: Dict[str, KonfluxBuildRecord] = {}  # operator name to build records
        if self.operator_nvrs:
            # Get build records for the given operator nvrs
            LOGGER.info("Fetching given nvrs from Konflux DB...")
            records = await runtime.konflux_db.get_build_records_by_nvrs(self.operator_nvrs, exclude_large_columns=True)
            for record in records:
                assert record is not None and isinstance(record, KonfluxBuildRecord), "Invalid record. Doozer bug?"
                dgk_records[record.name] = record
            # Load image metas for the given operators
            runtime.images = list(dgk_records.keys())
            runtime.initialize(mode='images', clone_distgits=False)
            for dgk in dgk_records.keys():
                metadata = runtime.image_map[dgk]
                if not metadata.is_olm_operator:
                    raise DoozerFatalError(f"Operator {dgk} does not have 'update-csv' config")
        else:
            # Get latest build records for all specified operators
            runtime.initialize(mode='images', clone_distgits=False)
            LOGGER.info("Fetching latest operator builds from Konflux DB...")
            operator_metas: List[ImageMetadata] = [
                operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator
            ]
            records = await asyncio.gather(
                *(metadata.get_latest_build(exclude_large_columns=True) for metadata in operator_metas)
            )
            not_found = [metadata.distgit_key for metadata, record in zip(operator_metas, records) if record is None]
            if not_found:
                raise IOError(f"Couldn't find build records for {not_found}")
            for metadata, record in zip(operator_metas, records):
                assert record is not None and isinstance(record, KonfluxBuildRecord)
                dgk_records[metadata.distgit_key] = record
        return dgk_records

    async def _get_bundle_build_for(
        self, operator_build: KonfluxBuildRecord, strict: bool = True
    ) -> Optional[KonfluxBundleBuildRecord]:
        """Get bundle build record for the given operator build.

        :param operator_build: Operator build record.
        :return: Bundle build record.
        """
        operator_meta = self.runtime.image_map.get(operator_build.name)
        bundle_name = operator_meta.get_olm_bundle_short_name() if operator_meta else f"{operator_build.name}-bundle"
        LOGGER.info("Fetching bundle build for %s from Konflux DB...", operator_build.nvr)
        where = {
            "name": bundle_name,
            "group": self.runtime.group,
            "assembly": self.runtime.assembly,
            "operator_nvr": operator_build.nvr,
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }
        bundle_build = await anext(self._db_for_bundles.search_builds_by_fields(where=where, limit=1), None)
        if not bundle_build:
            if strict:
                raise IOError(f"Bundle build not found for {operator_build.name}. Please build the bundle first.")
            return None
        assert isinstance(bundle_build, KonfluxBundleBuildRecord)
        return bundle_build

    async def _get_bundle_build_by_nvr(self, nvr: str) -> Optional[KonfluxBundleBuildRecord]:
        """Find a successful bundle build by its exact NVR, regardless of assembly."""
        where = {
            "nvr": nvr,
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }
        bundle_build = await anext(self._db_for_bundles.search_builds_by_fields(where=where, limit=1), None)
        if bundle_build is None:
            return None
        assert isinstance(bundle_build, KonfluxBundleBuildRecord)
        return bundle_build

    async def _rebase_and_build(
        self,
        rebaser: KonfluxOlmBundleRebaser,
        builder: KonfluxOlmBundleBuilder,
        image_meta: ImageMetadata,
        operator_build: KonfluxBuildRecord,
        git_auth_secret: Optional[str] = None,
    ) -> str:
        logger = LOGGER.getChild(f"[{image_meta.distgit_key}]")
        input_release = self.release
        if not self.force or not input_release:
            logger.info("Checking if a previous bundle build exists...")
            bundle_build = await self._get_bundle_build_for(operator_build, strict=False)
            if bundle_build is not None:
                logger.info(f"A previous bundle build already exists: {bundle_build.nvr}")
                if not self.force:
                    logger.info("Skipping because --force is not set")
                    return bundle_build.nvr
                input_release = str(int(bundle_build.release) + 1)
                logger.info("Force rebuild requested because --force is set; release string will be %s", input_release)
            else:
                input_release = "1"
                logger.info(
                    "No previous bundle build found; a new bundle build will be created with release string %s",
                    input_release,
                )

        logger.info("Rebasing OLM bundle...")
        nvr = await rebaser.rebase(image_meta, operator_build, input_release)

        # The assembly-scoped lookup above determines whether this assembly already has a
        # bundle for the operator. The resulting NVR, however, is derived from the operator's
        # labels and can collide with a bundle recorded under another assembly. Check the exact
        # NVR globally before starting a PipelineRun so a cross-assembly invocation cannot
        # create two successful builds with the same NVR.
        existing_build = await self._get_bundle_build_by_nvr(nvr)
        if existing_build is not None:
            raise ValueError(
                f"Successful bundle NVR {nvr} already exists in DB! "
                f"Existing build assembly: {existing_build.assembly}; "
                f"pullspec: {existing_build.image_pullspec}. "
                "To rebuild, use a different bundle release."
            )

        logger.info("Building OLM bundle...")
        await builder.build(image_meta, git_auth_secret=git_auth_secret)
        logger.info("Bundle build complete")
        return nvr

    @start_as_current_span_async(TRACER, "images:konflux:bundle")
    async def run(self):
        runtime = self.runtime
        if runtime.images and self.operator_nvrs:
            raise click.BadParameter("Do not specify operator NVRs when --images is specified")

        runtime.initialize(config_only=True)
        assembly = runtime.assembly
        if assembly is None:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        assert runtime.konflux_db is not None, "konflux_db is not initialized. Doozer bug?"
        konflux_db = runtime.konflux_db
        konflux_db.bind(KonfluxBuildRecord)

        dgk_records = await self.get_operator_builds()

        assert runtime.source_resolver is not None, "source_resolver is not initialized. Doozer bug?"
        assert runtime.group_config is not None, "group_config is not initialized. Doozer bug?"

        rebaser = KonfluxOlmBundleRebaser(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_BUILD_SOURCES),
            group=runtime.group,
            assembly=assembly,
            group_config=runtime.group_config,
            konflux_db=runtime.konflux_db,
            source_resolver=runtime.source_resolver,
            upcycle=runtime.upcycle,
            image_repo=self.image_repo,
            dry_run=self.dry_run,
            record_logger=runtime.record_logger,
        )

        builder = KonfluxOlmBundleBuilder(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_BUILD_SOURCES),
            group=runtime.group,
            assembly=assembly,
            source_resolver=runtime.source_resolver,
            db=self._db_for_bundles,
            konflux_namespace=self.konflux_namespace,
            konflux_kubeconfig=self.konflux_kubeconfig,
            konflux_context=self.konflux_context,
            image_repo=self.image_repo,
            skip_checks=self.skip_checks,
            skip_tasks=self.skip_tasks,
            pipelinerun_template_url=self.plr_template,
            dry_run=self.dry_run,
            assembly_type=runtime.assembly_type,
            record_logger=runtime.record_logger,
        )

        # Mint a per-invocation GitHub App token for git-clone auth
        git_auth_secret = await builder._konflux_client.ensure_git_auth_secret(
            namespace=self.konflux_namespace,
        )
        refresh_task = asyncio.create_task(builder._konflux_client.token_refresh_loop(namespace=self.konflux_namespace))

        tasks = []
        for dgk, record in dgk_records.items():
            image_meta = runtime.image_map[dgk]
            tasks.append(
                asyncio.create_task(
                    self._rebase_and_build(rebaser, builder, image_meta, record, git_auth_secret=git_auth_secret)
                )
            )

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_nvrs = []
            failed_tasks = []
            errors = []
            for dgk, result in zip(dgk_records, results):
                if isinstance(result, Exception):
                    failed_tasks.append(dgk)
                    stack_trace = ''.join(traceback.TracebackException.from_exception(result).format())
                    errors.append(
                        {
                            "operator": dgk,
                            "operator_nvr": dgk_records[dgk].nvr,
                            "bundle_nvr": None,
                            "error": str(result),
                            "traceback": stack_trace,
                        }
                    )
                    LOGGER.error(f"Failed to rebase/build OLM bundle for {dgk}: {result}; {stack_trace}")
                else:
                    successful_nvrs.append(result)
        finally:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
            try:
                await builder._konflux_client.delete_git_auth_secret(
                    namespace=self.konflux_namespace,
                )
                await builder._konflux_client.cleanup_stale_git_auth_secrets(
                    namespace=self.konflux_namespace,
                )
            except Exception as e:
                LOGGER.warning("Failed to cleanup git-auth secrets: %s", e)

        if self.output == 'json':
            output_data = {
                "nvrs": successful_nvrs,
                "errors": errors,
                "failed_count": len(failed_tasks),
                "success_count": len(successful_nvrs),
            }
            click.echo(json.dumps(output_data, indent=4))
            if failed_tasks:
                LOGGER.error(f"Failed to rebase/build bundles: {failed_tasks}")
                sys.exit(1)
        elif failed_tasks:
            raise DoozerFatalError(f"Failed to rebase/build bundles: {failed_tasks}")
        LOGGER.info("Build complete")


@cli.command("beta:images:konflux:bundle", short_help="Rebase and build an OLM bundle for an operator with Konflux.")
@click.argument('operator_nvrs', nargs=-1, required=False)
@click.option(
    "-f",
    "--force",
    required=False,
    is_flag=True,
    help="Perform a build even if previous bundles for given NVRs already exist",
)
@click.option(
    '--dry-run',
    default=False,
    is_flag=True,
    help='Do not push to build repo or build anything, but print what would be done.',
)
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Push images to the specified repo.')
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option(
    '--skip-task',
    'skip_tasks',
    multiple=True,
    help='Remove a named Tekton task from the PipelineRun. Repeatable (e.g. --skip-task clair-scan --skip-task sast-snyk-check).',
)
@click.option("--release", metavar='RELEASE', help="Release string to populate in bundle's Dockerfiles.")
@click.option(
    '--plr-template',
    required=False,
    default=constants.KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL,
    help='Use a custom PipelineRun template to build the bundle. Overrides the default template from openshift-priv/art-konflux-template',
)
@click.option(
    '--output',
    '-o',
    type=click.Choice(['json'], case_sensitive=False),
    default='json',
    help='Output format for the build records.',
)
@pass_runtime
@click_coroutine
async def images_konflux_bundle(
    runtime: Runtime,
    operator_nvrs: Tuple[str, ...],
    force: bool,
    dry_run: bool,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    image_repo: str,
    skip_checks: bool,
    skip_tasks: tuple,
    release: Optional[str],
    plr_template: str,
    output: str,
):
    cli = KonfluxBundleCli(
        runtime=runtime,
        operator_nvrs=operator_nvrs,
        force=force,
        dry_run=dry_run,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        image_repo=image_repo,
        skip_checks=skip_checks,
        skip_tasks=skip_tasks,
        release=release,
        plr_template=plr_template,
        output=output,
    )
    await cli.run()


class BundleStageReleaseRelatedImagesCli:
    """Stage-release operator + operand images (bundle related images) via a Konflux advisory-stage ReleasePlan.

    Runs at the end of the bundle build job, before FBC builds are triggered. Each operator gets its own
    Snapshot/Release, created one after another, so a failure is contained to the operator that caused it.
    The release plan is resolved from the product version in group config (MAJOR.MINOR),
    not from the OCP version.
    """

    def __init__(
        self,
        runtime: Runtime,
        operator_nvrs: Tuple[str, ...],
        stage_release_plan: Optional[str],
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: str,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.operator_nvrs = operator_nvrs
        self.stage_release_plan = stage_release_plan
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.dry_run = dry_run
        self._logger = LOGGER.getChild("BundleStageReleaseRelatedImagesCli")
        self._db_for_bundles = KonfluxDb()
        self._db_for_bundles.bind(KonfluxBundleBuildRecord)

    async def run(self):
        runtime = self.runtime
        runtime.initialize(config_only=True)
        assembly = runtime.assembly

        if assembly != "stream":
            self._logger.info("Assembly is '%s' (not 'stream'); skipping stage release of related images", assembly)
            return

        assert runtime.konflux_db is not None, "konflux_db is not initialized. Doozer bug?"
        runtime.konflux_db.bind(KonfluxBuildRecord)

        # Use product version (not OCP version) to resolve the release plan.
        # Layered products (e.g. ACM) set `version: 2.16.0` in group.yml; MAJOR/MINOR there are the OCP
        # version used for the brew branch — do NOT use them for product version resolution.
        # OCP groups (e.g. openshift-5.0) have no `version:` field; MAJOR/MINOR ARE the product version.
        # MissingModel.__bool__ is False, so `if version_str:` is the correct guard for the missing case.
        assert runtime.group_config is not None, "group_config is not initialized. Doozer bug?"
        version_str = runtime.group_config.version
        if version_str:
            parts = str(version_str).split(".")
            product_major, product_minor = int(parts[0]), int(parts[1])
        else:
            product_major = int(runtime.group_config.vars.MAJOR)
            product_minor = int(runtime.group_config.vars.MINOR)

        # Resolve release plan once (same for all operators in this group)
        release_plan = self.stage_release_plan
        if not release_plan:
            release_plan = resolve_konflux_fbc_stage_release_plan(runtime.product, product_major, product_minor)
        if not release_plan:
            self._logger.info(
                "No stage release plan configured for product '%s' (%d.%d); skipping stage release",
                runtime.product,
                product_major,
                product_minor,
            )
            return

        # Look up operator build records. strict=False so a single missing NVR is reported against that
        # operator only, instead of raising and taking down every other operator's stage release.
        records = await runtime.konflux_db.get_build_records_by_nvrs(
            list(self.operator_nvrs), strict=False, exclude_large_columns=True, group=runtime.group
        )
        resolved: List[Tuple[str, KonfluxBuildRecord]] = []
        failed_nvrs: List[str] = []
        for nvr, record in zip(self.operator_nvrs, records):
            if record is None:
                self._logger.error("No build record found for operator NVR %s", nvr)
                self._add_record(nvr, operator=None, error=f"No build record found for operator NVR {nvr}")
                failed_nvrs.append(nvr)
            else:
                resolved.append((nvr, record))

        if not resolved:
            raise DoozerFatalError(f"No build records found for operator NVRs: {self.operator_nvrs}")

        # NOTE: this second initialize() is not skipped only because the config_only call above
        # returns before Runtime sets self.initialized. If that ever changes, the
        # `if self.initialized: return` guard would swallow this call and leave image_map empty.
        runtime.images = [r.name for _, r in resolved]
        runtime.initialize(mode='images', clone_distgits=False)

        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_namespace,
            config_file=self.konflux_kubeconfig,
            context=self.konflux_context,
            dry_run=self.dry_run,
        )

        # One Snapshot/Release per operator: an operator whose stage release fails must not prevent the
        # other operators in this job from being released and moving on to their FBC builds.
        # Operators of the same product routinely share operands, so a shared operand ends up in more than
        # one Snapshot. That duplication is deliberate — it is what keeps operators independent — and it is
        # also why these run serially: two Releases pushing the same image concurrently is asking for trouble.
        for nvr, record in resolved:
            try:
                release_url = await self._stage_release_operator(
                    konflux_client=konflux_client,
                    record=record,
                    release_plan_name=release_plan,
                    assembly=assembly,
                )
            except Exception as e:  # noqa: BLE001 - failures are contained to this operator
                self._logger.error(
                    "Stage release failed for operator %s (%s): %s\n%s",
                    record.name,
                    nvr,
                    e,
                    ''.join(traceback.TracebackException.from_exception(e).format()),
                )
                self._add_record(nvr, operator=record.name, error=str(e))
                failed_nvrs.append(nvr)
            else:
                self._add_record(nvr, operator=record.name, release_url=release_url)

        if failed_nvrs:
            raise DoozerFatalError(f"Stage release of related images failed for: {', '.join(failed_nvrs)}")
        self._logger.info("Stage release of related images completed for %d operator(s)", len(resolved))

    def _add_record(
        self,
        operator_nvr: str,
        operator: Optional[str],
        release_url: str = "",
        error: str = "",
    ):
        """Record this operator's stage release outcome in record.log for the calling pipeline.

        pyartcd reads these to drop failed operators from the FBC trigger list while letting the rest through.
        """
        if not self.runtime.record_logger:
            return
        # '|' is the record field separator; keep an error message from inventing fields
        message = error.replace("|", "/")[:500]
        self.runtime.record_logger.add_record(
            "stage_release_related_images",
            status=1 if error else 0,
            operator_nvr=operator_nvr,
            operator=operator or "",
            release_url=release_url,
            message=message,
        )

    async def _stage_release_operator(
        self,
        konflux_client: KonfluxClient,
        record: KonfluxBuildRecord,
        release_plan_name: str,
        assembly: str,
    ) -> str:
        """Stage-release one operator's related images (itself + its operands). Returns the release URL."""
        runtime = self.runtime
        operator_name = record.name
        operator_meta = runtime.image_map.get(operator_name)
        if not operator_meta:
            raise DoozerFatalError(f"No image metadata for operator '{operator_name}'; cannot resolve bundle name")

        where = {
            "name": operator_meta.get_olm_bundle_short_name(),
            "group": runtime.group,
            "operator_nvr": record.nvr,
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }
        bundle_build = await anext(self._db_for_bundles.search_builds_by_fields(where=where, limit=1), None)
        if not bundle_build:
            raise DoozerFatalError(f"Bundle build not found for operator {operator_name} ({record.nvr})")

        # Dedup by component name within this operator: an operator can list itself among its operands.
        components: Dict[str, dict] = {}
        for build in await get_referenced_images(runtime.konflux_db, bundle_build):
            name = build.get_konflux_component_name()
            if name in components:
                continue
            components[name] = {
                "name": name,
                "source": {"git": {"url": build.rebase_repo_url, "revision": build.rebase_commitish}},
                "containerImage": build.image_pullspec,
            }

        if not components:
            self._logger.info(
                "No related image builds found for operator %s (%s); nothing to stage-release",
                operator_name,
                record.nvr,
            )
            return ""

        self._logger.info(
            "Stage-releasing %d related image(s) for operator %s (%s) via ReleasePlan '%s'...",
            len(components),
            operator_name,
            record.nvr,
            release_plan_name,
        )
        release_url = await self._stage_release(
            konflux_client=konflux_client,
            components=sorted(components.values(), key=lambda c: c["name"]),
            release_plan_name=release_plan_name,
            group=runtime.group,
            assembly=assembly,
            operator_name=operator_name,
            operator_nvr=record.nvr,
        )
        self._logger.info("Stage release succeeded for operator %s (%s): %s", operator_name, record.nvr, release_url)
        return release_url

    async def _stage_release(
        self,
        konflux_client: KonfluxClient,
        components: List[dict],
        release_plan_name: str,
        group: str,
        assembly: str,
        operator_name: str,
        operator_nvr: str,
    ) -> str:
        """Create one Snapshot + Release for one operator's related images and wait for it.

        Returns the release URL.
        """
        application_name = util.konflux_application_name(group)
        # Snapshot names are generated server-side: two operators truncated to the same 63-char DNS label
        # would otherwise collide.
        # 63-char DNS label budget: "fbc-ri-stage-" (13) + group + "-" + operator + "-" + 5 chars the
        # apiserver appends for generateName.
        group_safe = normalize_k8s_dns_label(group, max_length=20)
        operator_safe = normalize_k8s_dns_label(operator_name, max_length=max(1, 43 - len(group_safe)))
        generate_name = f"fbc-ri-stage-{group_safe}-{operator_safe}-"

        if self.dry_run:
            self._logger.info(
                "[DRY-RUN] Would create Snapshot '%s*' with %d component(s) and Release via '%s'",
                generate_name,
                len(components),
                release_plan_name,
            )
            return "https://dry-run.invalid"

        # Create Snapshot
        snapshot_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_SNAPSHOT,
            "metadata": {
                "generateName": generate_name,
                "namespace": self.konflux_namespace,
                "labels": {
                    "test.appstudio.openshift.io/type": "override",
                    "appstudio.openshift.io/application": application_name,
                    # This workflow creates and waits for its own Release below.
                    "release.appstudio.openshift.io/auto-release": "false",
                },
            },
            "spec": {"application": application_name, "components": components},
        }
        result_snapshot = await konflux_client._create(snapshot_obj)
        snapshot_name = result_snapshot.metadata.name
        snapshot_url = konflux_client.resource_url(result_snapshot)
        self._logger.info("Created Snapshot %s (%s)", snapshot_name, snapshot_url)

        # Wait for Snapshot to be readable
        timeout_s, poll_s, elapsed = 60, 10, 0
        while elapsed < timeout_s:
            try:
                await konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot_name)
                break
            except k8s_exceptions.NotFoundError:
                await asyncio.sleep(poll_s)
                elapsed += poll_s
        else:
            raise RuntimeError(f"Snapshot {snapshot_name} not available after 1 minute")

        # Verify ReleasePlan exists before creating Release
        try:
            await konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, release_plan_name)
        except k8s_exceptions.NotFoundError:
            raise RuntimeError(
                f"ReleasePlan '{release_plan_name}' not found in namespace '{self.konflux_namespace}'. "
                "Ensure ART-17452 has landed and the ReleasePlan name is correct."
            ) from None

        # Create Release
        release_annotations = {
            "art.redhat.com/kind": "bundle-ri-stage-release",
            "art.redhat.com/group": group,
            "art.redhat.com/assembly": assembly,
            "art.redhat.com/distgit-key": operator_name,
            "art.redhat.com/operator-nvr": operator_nvr,
        }
        if job_url := os.getenv("BUILD_URL"):
            release_annotations["art.redhat.com/job-url"] = job_url

        release_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_RELEASE,
            "metadata": {
                "generateName": generate_name,
                "namespace": self.konflux_namespace,
                "labels": {"appstudio.openshift.io/application": application_name},
                "annotations": release_annotations,
            },
            "spec": {"releasePlan": release_plan_name, "snapshot": snapshot_name},
        }
        created_release = await konflux_client._create(release_obj)
        release_name = created_release.metadata.name
        release_url = konflux_client.resource_url(created_release)
        self._logger.info("Created Release %s for Snapshot %s (%s)", release_name, snapshot_name, release_url)

        released = KubeCondition.find_condition(await konflux_client.wait_for_release(release_name), 'Released')
        if not released or released.status != "True" or released.reason != "Succeeded":
            raise RuntimeError(
                f"Stage release {release_name} for {operator_nvr} did not succeed "
                f"({released.reason if released else 'no Released condition'}): "
                f"{released.message if released else 'timed out'}. See {release_url}"
            )
        self._logger.info("Stage release %s succeeded", release_name)
        return release_url


@cli.command(
    "beta:bundle:stage-release-related-images",
    short_help="Stage-release bundle related images (operator + operands) before FBC builds are triggered.",
)
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option(
    "--stage-release-plan",
    metavar="NAME",
    default=None,
    help="Override the auto-resolved Konflux ReleasePlan name.",
)
@click.option(
    '--dry-run', default=False, is_flag=True, help='Do not create Snapshots/Releases, only print what would be done.'
)
@click.argument('operator_nvrs', nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def bundle_stage_release_related_images(
    runtime: Runtime,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    stage_release_plan: Optional[str],
    dry_run: bool,
    operator_nvrs: Tuple[str, ...],
):
    """Stage-release operator and operand images to the advisory before FBC builds.

    Takes operator NVRs and, for each one in turn, looks up its bundle build, extracts the referenced
    images (operator + operands), and releases them through a Konflux advisory-stage ReleasePlan as its
    own Snapshot/Release. One operator's failure does not stop the others: every outcome is written to
    record.log as a `stage_release_related_images` entry, and the command exits non-zero if any operator
    failed so the caller can drop just those operators.

    Only runs for the 'stream' assembly. If no release plan is configured for the product, the command
    exits successfully without doing anything.
    """
    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    stage_cli = BundleStageReleaseRelatedImagesCli(
        runtime=runtime,
        operator_nvrs=operator_nvrs,
        stage_release_plan=stage_release_plan,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        dry_run=dry_run,
    )
    await stage_cli.run()
