import asyncio
import logging
import os
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, cast

import click
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb

from doozerlib import constants, opm
from doozerlib.backend.konflux_fbc import KonfluxFbcBuilder, KonfluxFbcImporter, KonfluxFbcRebaser
from doozerlib.cli import (
    cli,
    click_coroutine,
    option_commit_message,
    option_push,
    pass_runtime,
    validate_semver_major_minor_patch,
)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml


class FbcImportCli:
    def __init__(
        self,
        runtime: Runtime,
        index_image: str | None,
        keep_templates: bool,
        push: bool,
        registry_auth: Optional[str],
        fbc_repo: str,
        message: str,
        dest_dir: str | None,
    ):
        self.runtime = runtime
        self.index_image = index_image
        self.keep_templates = keep_templates
        self.push = push
        self.registry_auth = registry_auth
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.message = message
        self.dest_dir = (
            Path(dest_dir) if dest_dir else Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES)
        )

    async def run(self):
        """Run the FBC import process
        This function implements the main logic of the FBC import process
        following https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/docs/konflux-onboarding.md#create-the-fbc-in-the-git-repository.
        """
        # Ensure opm is installed
        try:
            await opm.verify_opm()
        except (IOError, FileNotFoundError):
            LOGGER.error(
                "Please install the latest opm cli binary from https://github.com/operator-framework/operator-registry/releases"
            )
            raise

        # Initialize runtime
        runtime = self.runtime
        runtime.initialize(mode="images", clone_distgits=False)
        assert runtime.group_config is not None, "group_config is not loaded; Doozer bug?"
        if not runtime.assembly:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        if (
            not runtime.group_config.vars
            or "MAJOR" not in runtime.group_config.vars
            or "MINOR" not in runtime.group_config.vars
        ):
            raise ValueError("MAJOR and MINOR must be set in group vars.")
        major, minor = int(runtime.group_config.vars.MAJOR), int(runtime.group_config.vars.MINOR)

        operator_metadatas = [
            operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator
        ]
        if not operator_metadatas:
            raise ValueError("No operator images loaded in group")
        auth = None
        if self.registry_auth:
            auth = opm.OpmRegistryAuth(path=self.registry_auth)
        importer = KonfluxFbcImporter(
            base_dir=self.dest_dir,
            group=runtime.group,
            assembly=str(runtime.assembly),
            ocp_version=(major, minor),
            keep_templates=self.keep_templates,
            upcycle=runtime.upcycle,
            push=self.push,
            commit_message=self.message,
            fbc_repo=self.fbc_repo,
            auth=auth,
        )

        LOGGER.info("Importing FBC from index image...")
        tasks = []
        for metadata in operator_metadatas:
            tasks.append(importer.import_from_index_image(metadata, self.index_image))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_tasks = []
        for metadata, result in zip(operator_metadatas, results):
            if isinstance(result, Exception):
                failed_tasks.append(metadata.distgit_key)
                LOGGER.error(f"Failed to import FBC for {metadata.distgit_key}: {result}")
        if failed_tasks:
            raise DoozerFatalError(f"Failed to import FBC for bundles: {', '.join(failed_tasks)}")
        LOGGER.info("FBC import complete")


@cli.command("beta:fbc:import", short_help="Create FBC by importing from the provided index image")
@click.option(
    "--from-index",
    metavar='INDEX_IMAGE',
    help="The index image to import from. If not set, the production index image will be used.",
)
@click.option(
    "--keep-templates",
    is_flag=True,
    help="Keep the generated templates. If not set, the templates will be deleted after rendering the final catalogs.",
)
@click.option("--push", is_flag=True, help="Push the generated FBC to the git repository.")
@click.option(
    "--fbc-repo", metavar='FBC_REPO', help="The git repository to push the FBC to.", default=constants.ART_FBC_GIT_REPO
)
@click.option("--registry-auth", metavar='AUTH', help="The registry authentication file to use for the index image.")
@option_commit_message
@click.argument("dest_dir", metavar='DEST_DIR', required=False, default=None)
@pass_runtime
@click_coroutine
async def fbc_import(
    runtime: Runtime,
    from_index: Optional[str],
    keep_templates: bool,
    push: bool,
    fbc_repo: str,
    registry_auth: Optional[str],
    message: str,
    dest_dir: Optional[str],
):
    """
    Create an FBC repository by importing from the provided index image

    Example usage:

    doozer --group=openshift-4.17 beta:fbc:import registry.redhat.io/redhat/redhat-operator-index:v4.17 ./fbc-4.17
    """
    cli = FbcImportCli(
        runtime=runtime,
        index_image=from_index,
        keep_templates=keep_templates,
        push=push,
        fbc_repo=fbc_repo,
        registry_auth=registry_auth,
        message=message,
        dest_dir=dest_dir,
    )
    await cli.run()


class FbcRebaseCli:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        release: str,
        commit_message: str,
        push: bool,
        fbc_repo: str,
        operator_nvrs: Tuple[str, ...],
    ):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.commit_message = commit_message
        self.push = push
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.operator_nvrs = operator_nvrs
        self._logger = LOGGER.getChild("FbcRebaseCli")
        self._db_for_bundles = KonfluxDb()
        self._db_for_bundles.bind(KonfluxBundleBuildRecord)

    async def run(self):
        runtime = self.runtime
        logger = self._logger.getChild("run")
        if runtime.images and self.operator_nvrs:
            raise click.BadParameter("Do not specify operator NVRs when --images is specified")
        runtime.initialize(config_only=True)
        assembly = runtime.assembly
        if assembly is None:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        assert runtime.konflux_db is not None, "konflux_db is not initialized. Doozer bug?"
        konflux_db = runtime.konflux_db
        konflux_db.bind(KonfluxBuildRecord)

        dgk_operator_builds = await self._get_operator_builds()
        bundle_builds = await self._get_bundle_builds(list(dgk_operator_builds.values()), strict=False)
        not_found = [
            dgk for dgk, bundle_build in zip(dgk_operator_builds.keys(), bundle_builds) if bundle_build is None
        ]
        if not_found:
            raise IOError(f"Bundle builds not found for {not_found}. Please build the bundles first.")
        bundle_builds = cast(List[KonfluxBundleBuildRecord], bundle_builds)

        assert runtime.source_resolver is not None, "source_resolver is not initialized. Doozer bug?"
        assert runtime.group_config is not None, "group_config is not initialized. Doozer bug?"

        rebaser = KonfluxFbcRebaser(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES),
            group=runtime.group,
            assembly=assembly,
            version=self.version,
            release=self.release,
            commit_message=self.commit_message,
            push=self.push,
            fbc_repo=self.fbc_repo,
            upcycle=runtime.upcycle,
            record_logger=runtime.record_logger,
        )
        tasks = []
        for dgk, bundle_build in zip(dgk_operator_builds.keys(), bundle_builds):
            operator_meta = runtime.image_map[dgk]
            tasks.append(rebaser.rebase(operator_meta, bundle_build, self.version, self.release))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_bundles = []
        for dgk, bundle_build, result in zip(dgk_operator_builds.keys(), bundle_builds, results):
            if isinstance(result, Exception):
                failed_bundles.append(bundle_build.nvr)
                LOGGER.error(f"Failed to rebase FBC for {bundle_build.nvr} ({dgk}): {result}")
        if failed_bundles:
            raise DoozerFatalError(f"Failed to rebase FBC for bundles: {', '.join(failed_bundles)}")
        logger.info("Rebase complete")

    async def _get_operator_builds(self):
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
            records = await runtime.konflux_db.get_build_records_by_nvrs(self.operator_nvrs)
            for record in records:
                assert record is not None and isinstance(record, KonfluxBuildRecord), "Invalid record. Doozer bug?"
                dgk_records[record.name] = record
            # Load image metas for the given operators
            runtime.images = list(dgk_records.keys())
            runtime.initialize(mode='images', clone_distgits=False)
            for dgk in dgk_records.keys():
                metadata = runtime.image_map[dgk]
                if not metadata.is_olm_operator:
                    raise IOError(f"Operator {dgk} does not have 'update-csv' config")
        else:
            # Get latest build records for all specified operators
            runtime.initialize(mode='images', clone_distgits=False)
            LOGGER.info("Fetching latest operator builds from Konflux DB...")
            operator_metas: List[ImageMetadata] = [
                operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator
            ]
            records = await asyncio.gather(*(metadata.get_latest_build() for metadata in operator_metas))
            not_found = [metadata.distgit_key for metadata, record in zip(operator_metas, records) if record is None]
            if not_found:
                raise IOError(f"Couldn't find build records for {not_found}")
            for metadata, record in zip(operator_metas, records):
                assert record is not None and isinstance(record, KonfluxBuildRecord)
                dgk_records[metadata.distgit_key] = record
        return dgk_records

    async def _get_bundle_builds(
        self, operator_builds: Sequence[KonfluxBuildRecord], strict: bool = True
    ) -> List[Optional[KonfluxBundleBuildRecord]]:
        """Get bundle build records for the given operator builds.

        :param operator_builds: operator builds.
        :return: A list of bundle build records.
        """
        logger = self._logger.getChild("get_bundle_builds")
        logger.info("Fetching bundle builds from Konflux DB...")
        builds = await asyncio.gather(
            *(self._get_bundle_build_for(operator_build, strict=strict) for operator_build in operator_builds)
        )
        return builds

    async def _get_bundle_build_for(
        self, operator_build: KonfluxBuildRecord, strict: bool = True
    ) -> Optional[KonfluxBundleBuildRecord]:
        """Get bundle build record for the given operator build.

        :param operator_build: Operator build record.
        :return: Bundle build record.
        """
        bundle_name = f"{operator_build.name}-bundle"
        LOGGER.info("Fetching bundle build for %s from Konflux DB...", operator_build.nvr)
        where = {
            "name": bundle_name,
            "group": self.runtime.group,
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


@cli.command("beta:fbc:rebase", short_help="Refresh a group's FBC konflux source content")
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=validate_semver_major_minor_patch,
    help="Version string to populate in Dockerfiles.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@option_commit_message
@option_push
@click.option(
    "--fbc-repo", metavar='FBC_REPO', help="The git repository to push the FBC to.", default=constants.ART_FBC_GIT_REPO
)
@click.argument('operator_nvrs', nargs=-1, required=False)
@pass_runtime
@click_coroutine
async def fbc_rebase(
    runtime: Runtime,
    version: str,
    release: str,
    message: str,
    push: bool,
    fbc_repo: str,
    operator_nvrs: Tuple[str, ...],
):
    """
    Refresh a group's FBC source content

    The group's FBC konflux source content will be updated to include OLM bundles of the runtime assembly. The group's Dockerfiles will be
    updated to use the specified version and release.
    """
    cli = FbcRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        commit_message=message,
        push=push,
        fbc_repo=fbc_repo,
        operator_nvrs=operator_nvrs,
    )
    await cli.run()


class FbcBuildCli:
    def __init__(
        self,
        runtime: Runtime,
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: str,
        image_repo: str,
        skip_checks: bool,
        fbc_repo: str,
        plr_template: str,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.plr_template = plr_template
        self._fbc_db = KonfluxDb()
        self._fbc_db.bind(KonfluxFbcBuildRecord)
        self.dry_run = dry_run

    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False)
        operator_metas: List[ImageMetadata] = [
            operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator
        ]
        builder = KonfluxFbcBuilder(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES),
            group=runtime.group,
            assembly=runtime.assembly,
            db=self._fbc_db,
            fbc_repo=self.fbc_repo,
            konflux_kubeconfig=self.konflux_kubeconfig,
            konflux_context=self.konflux_context,
            konflux_namespace=self.konflux_namespace,
            image_repo=self.image_repo,
            skip_checks=self.skip_checks,
            pipelinerun_template_url=self.plr_template,
            dry_run=self.dry_run,
            record_logger=runtime.record_logger,
        )
        tasks = []
        for operator_meta in operator_metas:
            tasks.append(builder.build(operator_meta))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_tasks = []
        for operator_meta, result in zip(operator_metas, results):
            if isinstance(result, Exception):
                failed_tasks.append(operator_meta.distgit_key)
                stack_trace = ''.join(traceback.TracebackException.from_exception(result).format())
                LOGGER.error(f"Failed to build FBC for {operator_meta.distgit_key}: {result}; {stack_trace}")
        if failed_tasks:
            raise DoozerFatalError(f"Failed to rebase FBC for bundles: {', '.join(failed_tasks)}")
        LOGGER.info("FBC build complete")


class FbcRebaseAndBuildCli:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        release: str,
        commit_message: str,
        fbc_repo: str,
        operator_nvrs: Tuple[str, ...],
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: str,
        image_repo: str,
        skip_checks: bool,
        plr_template: str,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.commit_message = commit_message
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.operator_nvrs = operator_nvrs
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.plr_template = plr_template
        self.dry_run = dry_run

    async def run(self):
        """Run both rebase and build operations sequentially"""
        LOGGER.info("Starting FBC rebase and build process...")

        # First, run the rebase operation
        LOGGER.info("Step 1: Running FBC rebase...")
        rebase_cli = FbcRebaseCli(
            runtime=self.runtime,
            version=self.version,
            release=self.release,
            commit_message=self.commit_message,
            push=not self.dry_run,
            fbc_repo=self.fbc_repo,
            operator_nvrs=self.operator_nvrs,
        )
        await rebase_cli.run()
        LOGGER.info("FBC rebase completed successfully")

        # Then, run the build operation
        LOGGER.info("Step 2: Running FBC build...")
        build_cli = FbcBuildCli(
            runtime=self.runtime,
            konflux_kubeconfig=self.konflux_kubeconfig,
            konflux_context=self.konflux_context,
            konflux_namespace=self.konflux_namespace,
            image_repo=self.image_repo,
            skip_checks=self.skip_checks,
            fbc_repo=self.fbc_repo,
            plr_template=self.plr_template,
            dry_run=self.dry_run,
        )
        await build_cli.run()
        LOGGER.info("FBC build completed successfully")

        LOGGER.info("FBC rebase and build process completed successfully")


@cli.command("beta:fbc:build", short_help="Build the FBC source content")
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
    default=constants.KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_FBC_REPO, help='Push images to the specified repo.')
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option(
    '--fbc-repo', metavar='FBC_REPO', help='The git repository to push the FBC to.', default=constants.ART_FBC_GIT_REPO
)
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option(
    '--plr-template',
    required=False,
    default=constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL,
    help='Use a custom PipelineRun template to build the FBC fragement. Overrides the default template from openshift-priv/art-konflux-template',
)
@pass_runtime
@click_coroutine
async def fbc_build(
    runtime: Runtime,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    image_repo: str,
    skip_checks: bool,
    fbc_repo: str,
    dry_run: bool,
    plr_template: str,
):
    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not konflux_kubeconfig:
        raise ValueError("Must pass kubeconfig using --konflux-kubeconfig or KONFLUX_SA_KUBECONFIG env var")

    cli = FbcBuildCli(
        runtime=runtime,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        image_repo=image_repo,
        skip_checks=skip_checks,
        fbc_repo=fbc_repo,
        plr_template=plr_template,
        dry_run=dry_run,
    )
    await cli.run()


@cli.command("beta:fbc:rebase-and-build", short_help="Rebase FBC source content and then build it")
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=validate_semver_major_minor_patch,
    help="Version string to populate in Dockerfiles.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@option_commit_message
@click.option(
    "--fbc-repo", metavar='FBC_REPO', help="The git repository to push the FBC to.", default=constants.ART_FBC_GIT_REPO
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
    default=constants.KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_FBC_REPO, help='Push images to the specified repo.')
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option(
    '--plr-template',
    required=False,
    default=constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL,
    help='Use a custom PipelineRun template to build the FBC fragement. Overrides the default template from openshift-priv/art-konflux-template',
)
@click.argument('operator_nvrs', nargs=-1, required=False)
@pass_runtime
@click_coroutine
async def fbc_rebase_and_build(
    runtime: Runtime,
    version: str,
    release: str,
    message: str,
    fbc_repo: str,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    image_repo: str,
    skip_checks: bool,
    dry_run: bool,
    plr_template: str,
    operator_nvrs: Tuple[str, ...],
):
    """
    Rebase FBC source content and then build it

    This command combines both the rebase and build operations into a single workflow.
    It will first refresh the group's FBC source content with OLM bundles of the runtime
    assembly, then proceed to build the FBC source content.
    """
    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not konflux_kubeconfig:
        raise ValueError("Must pass kubeconfig using --konflux-kubeconfig or KONFLUX_SA_KUBECONFIG env var")

    cli = FbcRebaseAndBuildCli(
        runtime=runtime,
        version=version,
        release=release,
        commit_message=message,
        fbc_repo=fbc_repo,
        operator_nvrs=operator_nvrs,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        image_repo=image_repo,
        skip_checks=skip_checks,
        plr_template=plr_template,
        dry_run=dry_run,
    )
    await cli.run()
