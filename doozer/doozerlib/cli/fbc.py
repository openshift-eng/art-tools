import asyncio
import json
import logging
import os
import sys
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import click
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.model import Missing
from artcommonlib.util import (
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
from tenacity import retry, stop_after_attempt, wait_exponential

from doozerlib import constants, opm
from doozerlib.backend.konflux_fbc import (
    KonfluxFbcBuilder,
    KonfluxFbcFragmentMerger,
    KonfluxFbcImporter,
    KonfluxFbcRebaser,
)
from doozerlib.cli import (
    cli,
    click_coroutine,
    option_commit_message,
    pass_runtime,
    validate_semver_major_minor_patch,
)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)


class FbcImportCli:
    def __init__(
        self,
        runtime: Runtime,
        index_image: str | None,
        push: bool,
        registry_auth: Optional[str],
        fbc_repo: str,
        major_minor: Optional[str],
        message: str,
        dest_dir: str | None,
    ):
        self.runtime = runtime
        self.index_image = index_image
        self.push = push
        self.registry_auth = registry_auth
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.major_minor = major_minor
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
        if self.major_minor:
            try:
                major_str, minor_str = self.major_minor.split(".")
                major, minor = int(major_str), int(minor_str)
            except (ValueError, AttributeError):
                raise ValueError(
                    f"Invalid major-minor format: {self.major_minor}. Expected format: MAJOR.MINOR (e.g. 4.17)"
                )
        else:
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
@click.option("--push", is_flag=True, help="Push the generated FBC to the git repository.")
@click.option(
    "--fbc-repo", metavar='FBC_REPO', help="The git repository to push the FBC to.", default=constants.ART_FBC_GIT_REPO
)
@click.option("--registry-auth", metavar='AUTH', help="The registry authentication file to use for the index image.")
@click.option(
    "--major-minor",
    metavar='MAJOR.MINOR',
    help="Override the MAJOR.MINOR version from group config (e.g. 4.17).",
)
@option_commit_message
@click.argument("dest_dir", metavar='DEST_DIR', required=False, default=None)
@pass_runtime
@click_coroutine
async def fbc_import(
    runtime: Runtime,
    from_index: Optional[str],
    push: bool,
    fbc_repo: str,
    registry_auth: Optional[str],
    major_minor: Optional[str],
    message: str,
    dest_dir: Optional[str],
):
    """
    Create an FBC repository by importing from the provided index image

    Example usage:

    doozer --group=openshift-4.17 beta:fbc:import --from-index=registry.redhat.io/redhat/redhat-operator-index:v4.17 ./fbc-4.17
    """
    cli = FbcImportCli(
        runtime=runtime,
        index_image=from_index,
        push=push,
        fbc_repo=fbc_repo,
        registry_auth=registry_auth,
        major_minor=major_minor,
        message=message,
        dest_dir=dest_dir,
    )
    await cli.run()


class FbcMergeCli:
    DEFAULT_STAGE_FBC_REPO = "quay.io/openshift-art/stage-fbc-fragments"

    def __init__(
        self,
        runtime: Runtime,
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: Optional[str],
        dry_run: bool,
        fragments: Tuple[str, ...],
        dest_dir: Optional[str],
        fbc_repo: str,
        fbc_branch: Optional[str],
        registry_auth: Optional[str],
        message: Optional[str],
        skip_checks: bool,
        plr_template: Optional[str],
        target_index: Optional[str],
        major_minor: Optional[str],
    ):
        """
        Initialize the FBCFragmentMergerCli.
        """
        self.runtime = runtime
        self.dry_run = dry_run
        self.dest_dir = dest_dir
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.fragments = fragments
        self.fbc_repo = fbc_repo
        self.fbc_branch = fbc_branch
        self.registry_auth = registry_auth
        self.message = message
        self.skip_checks = skip_checks
        self.plr_template = plr_template
        self.target_index = target_index
        self.major_minor = major_minor

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _list_tags(self):
        cmd = ["skopeo", "list-tags", f"docker://{self.DEFAULT_STAGE_FBC_REPO}"]
        _, out, _ = await exectools.cmd_gather_async(cmd)
        return json.loads(out)["Tags"]

    async def run(self):
        # Initialize runtime if not already initialized
        runtime = self.runtime
        if not getattr(runtime, 'initialized', False) or getattr(runtime, 'mode', None) != 'images':
            runtime.initialize(mode="images", clone_distgits=False, build_system="konflux", config_only=True)
        assert runtime.group_config is not None, "group_config is not loaded; Doozer bug?"
        if self.major_minor:
            try:
                major_str, minor_str = self.major_minor.split(".")
                major, minor = int(major_str), int(minor_str)
            except (ValueError, AttributeError):
                raise ValueError(
                    f"Invalid major-minor format: {self.major_minor}. Expected format: MAJOR.MINOR (e.g. 4.17)"
                )
        else:
            if (
                not runtime.group_config.vars
                or "MAJOR" not in runtime.group_config.vars
                or "MINOR" not in runtime.group_config.vars
            ):
                raise ValueError("MAJOR and MINOR must be set in group vars.")
            major, minor = int(runtime.group_config.vars.MAJOR), int(runtime.group_config.vars.MINOR)
        target_index = self.target_index or f"{self.DEFAULT_STAGE_FBC_REPO}:ocp-{major}.{minor}"
        working_dir = (
            Path(self.dest_dir)
            if self.dest_dir
            else Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES, "__merged")
        )

        fbc_git_username = os.environ.get("FBC_GIT_USERNAME")
        fbc_git_password = os.environ.get("FBC_GIT_PASSWORD")

        fragments = self.fragments
        if not fragments:
            # No explicit fragments provided, list all tags in the stage repo and use matching ones
            tags = await self._list_tags()
            prefix = f"ocp__{major}.{minor}__"
            matching_tags = [tag for tag in tags if tag.startswith(prefix)]
            if not matching_tags:
                raise IOError("No matching tags found")
            LOGGER.info("Found %s matching tags: %s", len(matching_tags), matching_tags)
            # Get all operators with bundles (enabled images with update-csv config)
            # This matches the logic in olm_bundle.py:list_olm_operators
            operators_with_bundles = [
                image.get_component_name()
                for image in runtime.ordered_image_metas()
                if image.enabled and image.config.get('update-csv') is not Missing
            ]

            if operators_with_bundles:
                # Extract operator names from matching_tags (format: ocp__{major}.{minor}__{operator_name})
                prefix_len = len(prefix)
                operators_in_tags = {tag[prefix_len:] for tag in matching_tags}
                # Find missing operators
                missing_operators = set(operators_with_bundles) - operators_in_tags

                if missing_operators:
                    error_msg = (
                        f"The following operators with bundles are missing from matching tags: {sorted(missing_operators)}\n"
                        f"Operators with bundles in this release: {sorted(operators_with_bundles)}\n"
                        f"Matching tags: {sorted(matching_tags)}"
                    )
                    LOGGER.error(error_msg)
                    raise IOError(error_msg)
                else:
                    LOGGER.info(
                        "All operators with bundles in this release are present in matching tags (%s operators)",
                        len(operators_with_bundles),
                    )
            fragments = [f"{self.DEFAULT_STAGE_FBC_REPO}:{tag}" for tag in matching_tags]

        merger = KonfluxFbcFragmentMerger(
            working_dir=working_dir,
            group=runtime.group,
            group_config=runtime.group_config,
            upcycle=runtime.upcycle,
            dry_run=self.dry_run,
            commit_message=self.message,
            fbc_git_repo=self.fbc_repo,
            fbc_git_branch=self.fbc_branch or f"art-{runtime.group}-fbc-stage",
            fbc_git_username=fbc_git_username,
            fbc_git_password=fbc_git_password,
            registry_auth=self.registry_auth,
            konflux_context=self.konflux_context,
            konflux_kubeconfig=self.konflux_kubeconfig,
            konflux_namespace=self.konflux_namespace,
            skip_checks=self.skip_checks,
            plr_template=self.plr_template,
            major_minor_override=(major, minor) if self.major_minor else None,
        )
        await merger.run(fragments, target_index)


@cli.command("beta:fbc:merge", short_help="Merge FBC fragments from multiple index images into a single FBC repository")
@pass_runtime
@click.option(
    "--message", "-m", metavar='MSG', help="Commit message. If not provided, a default generated message will be used."
)
@click.option("--dry-run", is_flag=True, default=False, help="Perform a dry run without pushing changes.")
@click.option(
    "--dest-dir", metavar='DEST_DIR', default=None, help="The destination directory for the merged FBC fragments."
)
@click.option("--registry-auth", metavar='REGISTRY_AUTH', default=None, help="Path to the registry auth file.")
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
    help='The namespace to use for Konflux cluster connections. If not provided, will be auto-detected based on group (e.g., ocp-art-tenant for openshift- groups, art-oadp-tenant for oadp- groups).',
)
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option(
    '--fbc-repo',
    metavar='FBC_REPO',
    help='The git repository to push the FBC to. Use FBC_GIT_USERNAME and FBC_GIT_PASSWORD environment variables to set credentials.',
    default=constants.ART_FBC_GIT_REPO,
)
@click.option(
    '--fbc-branch',
    metavar='FBC_BRANCH',
    default=None,
    help='The branch to push the FBC to. Defaults to art-<group>-fbc-stage',
)
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option(
    '--plr-template',
    required=False,
    default=constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL,
    help='Use a custom PipelineRun template to build the FBC fragement. Overrides the default template from openshift-priv/art-konflux-template',
)
@click.option('--skip-checks', is_flag=True, default=False, help='Skip all post build checks')
@click.option(
    '--target-index',
    metavar='TARGET_INDEX',
    default=None,
    help='The target index image to merge fragments into. If not specified, a default index will be used.',
)
@click.option(
    "--major-minor",
    metavar='MAJOR.MINOR',
    help="Override the MAJOR.MINOR version from group config (e.g. 4.17).",
)
@click.argument("fragments", nargs=-1, required=False)
@click_coroutine
async def fbc_merge(
    runtime: Runtime,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: Optional[str],
    fragments: Tuple[str, ...],
    message: str,
    dry_run: bool,
    fbc_repo: str,
    fbc_branch: Optional[str],
    dest_dir: Optional[str],
    registry_auth: Optional[str],
    skip_checks: bool,
    plr_template: Optional[str],
    target_index: Optional[str],
    major_minor: Optional[str],
):
    """
    Merge FBC fragments from multiple index images into a single FBC repository.

    If no input is provided, use quay.io/openshift-art/stage-fbc-fragments:ocp__{MAJOR}.{MINOR}__*.
    """
    # Initialize runtime to populate runtime.product before using resolver functions
    runtime.initialize(build_system='konflux', config_only=True)

    # Resolve kubeconfig and namespace using product-based utility functions
    resolved_kubeconfig = resolve_konflux_kubeconfig_by_product(runtime.product, konflux_kubeconfig)
    resolved_namespace = resolve_konflux_namespace_by_product(runtime.product, konflux_namespace)

    cli = FbcMergeCli(
        runtime=runtime,
        fragments=fragments,
        message=message,
        dry_run=dry_run,
        fbc_repo=fbc_repo,
        fbc_branch=fbc_branch,
        dest_dir=dest_dir,
        registry_auth=registry_auth,
        konflux_kubeconfig=resolved_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=resolved_namespace,
        skip_checks=skip_checks,
        plr_template=plr_template,
        target_index=target_index,
        major_minor=major_minor,
    )
    await cli.run()


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
        force: bool,
        output: str,
        reset_to_prod: bool,
        prod_registry_auth: Optional[str] = None,
        major_minor: Optional[str] = None,
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
        self.force = force
        self.output = output
        self.reset_to_prod = reset_to_prod
        self.prod_registry_auth = prod_registry_auth
        self.major_minor = major_minor
        self._logger = LOGGER.getChild("FbcRebaseAndBuildCli")
        self._db_for_bundles = KonfluxDb()
        self._db_for_bundles.bind(KonfluxBundleBuildRecord)
        self._fbc_db = KonfluxDb()
        self._fbc_db.bind(KonfluxFbcBuildRecord)

    async def get_operator_builds(self, operator_nvrs: Tuple[str, ...]) -> Dict[str, KonfluxBuildRecord]:
        """Get build records for the given operator nvrs or latest build records for all operators.

        :param runtime: The runtime instance
        :param operator_nvrs: Tuple of operator NVRs to fetch, or empty tuple for all operators
        :return: A dictionary of operator name to build records.
        """
        assert self.runtime.konflux_db is not None, "konflux_db is not initialized. Doozer bug?"
        assert self.runtime.assembly is not None, "assembly is not initialized. Doozer bug?"
        dgk_records: Dict[str, KonfluxBuildRecord] = {}  # operator name to build records
        if operator_nvrs:
            # Get build records for the given operator nvrs
            LOGGER.info("Fetching given nvrs from Konflux DB...")
            records = await self.runtime.konflux_db.get_build_records_by_nvrs(operator_nvrs)
            for record in records:
                assert record is not None and isinstance(record, KonfluxBuildRecord), "Invalid record. Doozer bug?"
                dgk_records[record.name] = record
            # Load image metas for the given operators
            self.runtime.images = list(dgk_records.keys())
            self.runtime.initialize(mode='images', clone_distgits=False)
            for dgk in dgk_records.keys():
                metadata = self.runtime.image_map[dgk]
                if not metadata.is_olm_operator:
                    raise IOError(f"Operator {dgk} does not have 'update-csv' config")
        else:
            # Get latest build records for all specified operators
            self.runtime.initialize(mode='images', clone_distgits=False)
            LOGGER.info("Fetching latest operator builds from Konflux DB...")
            operator_metas: List[ImageMetadata] = [
                operator_meta for operator_meta in self.runtime.ordered_image_metas() if operator_meta.is_olm_operator
            ]
            records = await asyncio.gather(*(metadata.get_latest_build() for metadata in operator_metas))
            not_found = [metadata.distgit_key for metadata, record in zip(operator_metas, records) if record is None]
            if not_found:
                raise DoozerFatalError(f"Couldn't find build records for {not_found}")
            for metadata, record in zip(operator_metas, records):
                assert record is not None and isinstance(record, KonfluxBuildRecord)
                dgk_records[metadata.distgit_key] = record
        return dgk_records

    async def get_bundle_build_for(
        self, operator_build: KonfluxBuildRecord, strict: bool = True
    ) -> Optional[KonfluxBundleBuildRecord]:
        """Get bundle build record for the given operator build.

        :param operator_build: Operator build record.
        :param strict: Whether to fail if bundle build is not found
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

    async def get_bundle_builds(
        self, operator_builds: Sequence[KonfluxBuildRecord], strict: bool = True
    ) -> Optional[Dict[str, KonfluxBundleBuildRecord]]:
        """Get bundle build records for the given operator builds.

        :param operator_builds: operator builds.
        :param strict: Whether to fail if bundle build is not found
        :return: A list of bundle build records.
        """
        dgk_operator_builds = [operator_build.name for operator_build in operator_builds]

        LOGGER.info("Fetching bundle builds from Konflux DB...")
        builds = await asyncio.gather(
            *(self.get_bundle_build_for(operator_build, strict=strict) for operator_build in operator_builds)
        )
        dgk_bundle_builds = {}
        for dgk, bundle_build in zip(dgk_operator_builds, builds):
            dgk_bundle_builds[dgk] = bundle_build
        return dgk_bundle_builds

    async def _check_existing_fbc_build(
        self, operator_meta: ImageMetadata, bundle_build: KonfluxBundleBuildRecord
    ) -> Optional[KonfluxFbcBuildRecord]:
        """Check if an FBC build already exists for the given bundle build.

        :param operator_meta: Operator metadata
        :param bundle_build: Bundle build record
        :return: Existing FBC build record if found, None otherwise
        """
        fbc_name = KonfluxFbcRebaser.get_fbc_name(operator_meta.distgit_key)
        logger = self._logger.getChild(f"[{operator_meta.distgit_key}]")

        logger.info("Checking for existing FBC build...")
        where = {
            "name": fbc_name,
            "group": self.runtime.group,
            "outcome": str(KonfluxBuildOutcome.SUCCESS),
        }

        # Search for FBC builds that contain this bundle build NVR
        existing_fbc_build = await anext(
            self._fbc_db.search_builds_by_fields(
                where=where, array_contains={"bundle_nvrs": bundle_build.nvr}, limit=1
            ),
            None,
        )
        if existing_fbc_build:
            logger.info(f"Found existing FBC build: {existing_fbc_build.nvr}")
            return existing_fbc_build
        return None

    async def _rebase_and_build(
        self,
        importer: KonfluxFbcImporter,
        rebaser: KonfluxFbcRebaser,
        builder: KonfluxFbcBuilder,
        operator_meta: ImageMetadata,
        bundle_build: KonfluxBundleBuildRecord,
    ) -> str:
        """Rebase and build FBC for the given operator and bundle build.

        :param rebaser: FBC rebaser instance
        :param builder: FBC builder instance
        :param operator_meta: Operator metadata
        :param bundle_build: Bundle build record
        :return: NVR of the FBC build
        """
        existing_fbc_build = await self._check_existing_fbc_build(operator_meta, bundle_build)
        if existing_fbc_build:
            self._logger.info(f"Found existing FBC build: {existing_fbc_build.nvr}")
            if not self.force:
                return existing_fbc_build.nvr
            self._logger.info("Force flag is set, rebuilding FBC")

        if self.reset_to_prod:
            self._logger.info(f"Resetting FBC source content to production index image for {operator_meta.name}...")
            await importer.import_from_index_image(operator_meta, index_image=None, strict=False)

        self._logger.info(f"Rebasing fbc for {operator_meta.name}...")
        nvr = await rebaser.rebase(operator_meta, bundle_build, self.version, self.release)
        self._logger.info(f"Building fbc for {operator_meta.name}...")
        await builder.build(operator_meta, operator_nvr=bundle_build.operator_nvr)
        return nvr

    async def run(self):
        """Rebase and build fbc fragments for given operator NVRs or all latest operator NVRs for the group and assembly"""
        self._logger.info("Starting FBC rebase and build process...")

        runtime = self.runtime
        runtime.initialize(config_only=True)
        assembly = runtime.assembly
        if assembly is None:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        assert runtime.konflux_db is not None, "konflux_db is not initialized. Doozer bug?"
        konflux_db = runtime.konflux_db
        konflux_db.bind(KonfluxBuildRecord)

        # Get operator builds and corresponding bundle builds
        dgk_operator_builds = await self.get_operator_builds(self.operator_nvrs)
        dgk_bundle_builds = await self.get_bundle_builds(list(dgk_operator_builds.values()), strict=False)
        dgk_not_found = [dgk for dgk, bundle_build in dgk_bundle_builds.items() if bundle_build is None]
        if dgk_not_found:
            for dgk in dgk_not_found:
                self._logger.warning(f"Bundle build not found for {dgk_operator_builds[dgk].nvr}. Will skip it.")
        dgk_bundle_builds = {
            dgk: bundle_build for dgk, bundle_build in dgk_bundle_builds.items() if bundle_build is not None
        }
        if not dgk_bundle_builds:
            raise DoozerFatalError("No bundle builds found. Exiting.")

        assert runtime.source_resolver is not None, "source_resolver is not initialized. Doozer bug?"
        assert runtime.group_config is not None, "group_config is not initialized. Doozer bug?"

        self._logger.info(f"Processing {len(dgk_operator_builds)} operator(s): {list(dgk_operator_builds.keys())}")

        # Set up rebase and build components once
        if self.major_minor:
            try:
                major_str, minor_str = self.major_minor.split(".")
                ocp_version = (int(major_str), int(minor_str))
            except (ValueError, AttributeError):
                raise ValueError(
                    f"Invalid major-minor format: {self.major_minor}. Expected format: MAJOR.MINOR (e.g. 4.17)"
                )
        else:
            ocp_version = (runtime.group_config.vars.MAJOR, runtime.group_config.vars.MINOR)

        importer = KonfluxFbcImporter(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES),
            group=runtime.group,
            assembly=assembly,
            ocp_version=ocp_version,
            upcycle=runtime.upcycle,
            push=False,  # We will push after rebase
            commit_message=self.commit_message,
            fbc_repo=self.fbc_repo,
            auth=opm.OpmRegistryAuth(path=self.prod_registry_auth) if self.prod_registry_auth else None,
        )

        rebaser = KonfluxFbcRebaser(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_FBC_SOURCES),
            group=runtime.group,
            assembly=assembly,
            version=self.version,
            release=self.release,
            commit_message=self.commit_message,
            push=not self.dry_run,
            fbc_repo=self.fbc_repo,
            upcycle=runtime.upcycle,
            ocp_version_override=ocp_version if self.major_minor else None,
            record_logger=runtime.record_logger,
        )

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
            major_minor_override=ocp_version if self.major_minor else None,
            record_logger=runtime.record_logger,
        )

        tasks = [
            self._rebase_and_build(importer, rebaser, builder, self.runtime.image_map[dgk], bundle_build)
            for dgk, bundle_build in dgk_bundle_builds.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_nvrs = []
        failed_tasks = []
        errors = []

        for dgk, result in zip(dgk_bundle_builds, results):
            if isinstance(result, Exception):
                failed_tasks.append(dgk)
                stack_trace = ''.join(traceback.TracebackException.from_exception(result).format())
                error_msg = f"Failed to rebase/build FBC for {dgk}: {result}"
                error_details = {
                    "operator": dgk,
                    "operator_nvr": dgk_operator_builds[dgk].nvr,
                    "bundle_nvr": dgk_bundle_builds[dgk].nvr,
                    "error": str(result),
                    "traceback": stack_trace,
                }
                errors.append(error_details)
                LOGGER.error(f"{error_msg}; {stack_trace}")
            else:
                successful_nvrs.append(result)

        if self.output == 'json':
            output_data = {
                "nvrs": successful_nvrs,
                "errors": errors,
                "failed_count": len(failed_tasks),
                "success_count": len(successful_nvrs),
            }
            click.echo(json.dumps(output_data, indent=4))
            if failed_tasks:
                LOGGER.error(f"Failed to rebase/build FBC for bundles: {', '.join(failed_tasks)}")
                sys.exit(1)
        else:
            if failed_tasks:
                raise DoozerFatalError(f"Failed to rebase/build FBC for bundles: {', '.join(failed_tasks)}")
            LOGGER.info("FBC rebase and build process completed successfully for all operators")


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
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_FBC_REPO, help='Push images to the specified repo.')
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option(
    '--force',
    default=False,
    is_flag=True,
    help='Force rebuild even if an FBC build already exists for the given bundle builds.',
)
@click.option(
    '--plr-template',
    required=False,
    default=constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL,
    help='Use a custom PipelineRun template to build the FBC fragement. Overrides the default template from openshift-priv/art-konflux-template',
)
@click.option(
    '--output',
    '-o',
    type=click.Choice(['json'], case_sensitive=False),
    default='json',
    help='Output format for the build records.',
)
@click.option(
    '--reset-to-prod/--no-reset-to-prod',
    default=True,
    is_flag=True,
    help='Reset the FBC source content to the production index image before rebasing.',
)
@click.option(
    "--prod-registry-auth",
    metavar='PATH',
    help="The registry authentication file to use for the index image."
    " This might be needed if --reset-to-prod is used and the production index image requires authentication."
    " If not set, the KONFLUX_OPERATOR_INDEX_AUTH_FILE environment variable will be used if set.",
)
@click.option(
    "--major-minor",
    metavar='MAJOR.MINOR',
    help="Override the MAJOR.MINOR version from group config (e.g. 4.17).",
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
    force: bool,
    plr_template: str,
    output: str,
    reset_to_prod: bool,
    prod_registry_auth: Optional[str],
    major_minor: Optional[str],
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
        LOGGER.info(
            "--konflux-kubeconfig and KONFLUX_SA_KUBECONFIG env var are not set. Will rely on oc being logged in"
        )

    if reset_to_prod and not prod_registry_auth:
        prod_registry_auth = os.environ.get('KONFLUX_OPERATOR_INDEX_AUTH_FILE')

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
        force=force,
        output=output,
        reset_to_prod=reset_to_prod,
        prod_registry_auth=prod_registry_auth,
        major_minor=major_minor,
    )
    await cli.run()
