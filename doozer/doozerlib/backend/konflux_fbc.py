import asyncio
import itertools
import logging
import os
import shutil
import ssl
from collections import defaultdict
from datetime import datetime, timezone
from io import StringIO
from os import PathLike
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import httpx
import truststore
from artcommonlib import exectools
from artcommonlib import util as artlib_util
from artcommonlib.constants import KONFLUX_ART_IMAGES_SHARE
from artcommonlib.konflux.konflux_build_record import (
    Engine,
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from async_lru import alru_cache
from dockerfile_parse import DockerfileParser
from doozerlib import constants, opm, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo
from doozerlib.constants import KONFLUX_DEFAULT_IMAGE_REPO
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
from kubernetes.dynamic import resource
from tenacity import retry, stop_after_attempt, wait_fixed

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml

PRODUCTION_INDEX_PULLSPEC_FORMAT = "registry.redhat.io/redhat/redhat-operator-index:v{major}.{minor}"
BASE_IMAGE_RHEL9_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v{major}.{minor}"
BASE_IMAGE_RHEL8_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry:v{major}.{minor}"
FBC_BUILD_PRIORITY = "2"


def _generate_fbc_branch_name(
    group: str, assembly: str, distgit_key: str, ocp_version: Optional[Tuple[int, int]] = None
) -> str:
    """Generate FBC branch name with optional OCP version for non-OpenShift products.

    Args:
        group: The doozer group name (e.g., "openshift-4.17", "oadp-1.4")
        assembly: Assembly name (e.g., "stream", "4.17.1")
        distgit_key: Operator distgit key (e.g., "cluster-nfd-operator")
        ocp_version: Target OCP version tuple (major, minor) for non-OpenShift products

    Returns:
        Branch name following the pattern:
        - For OpenShift: "art-{group}-assembly-{assembly}-fbc-{distgit_key}"
        - For non-OpenShift: "art-{group}-ocp-{ocp_major}.{ocp_minor}-assembly-{assembly}-fbc-{distgit_key}"
    """
    if group.startswith('openshift-'):
        # For OpenShift products, use the traditional naming
        return f"art-{group}-assembly-{assembly}-fbc-{distgit_key}"
    else:
        # For non-OpenShift products, include the target OCP version
        if ocp_version is None:
            raise ValueError(f"ocp_version is required for non-OpenShift group '{group}'")
        ocp_version_str = f"{ocp_version[0]}.{ocp_version[1]}"
        return f"art-{group}-ocp-{ocp_version_str}-assembly-{assembly}-fbc-{distgit_key}"


class KonfluxFbcImporter:
    def __init__(
        self,
        base_dir: Path,
        group: str,
        assembly: str,
        ocp_version: Tuple[int, int],
        upcycle: bool,
        push: bool,
        commit_message: Optional[str] = None,
        fbc_repo: str = constants.ART_FBC_GIT_REPO,
        auth: Optional[opm.OpmRegistryAuth] = None,
        logger: logging.Logger | None = None,
    ):
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self.ocp_version = ocp_version
        self.upcycle = upcycle
        self.push = push
        self.commit_message = commit_message
        self.fbc_git_repo = fbc_repo
        self.auth = auth
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)

    async def import_from_index_image(
        self, metadata: ImageMetadata, index_image: str | None = None, strict: bool = True
    ):
        """Create a file based catalog (FBC) by importing from an existing index image.

        :param metadata: The metadata of the operator image.
        :param index_image: The index image to import from. If not provided, a default index image is used.
        :param strict: If True, raises an error if the index image is not found.
        """
        # bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        repo_dir = self.base_dir.joinpath(metadata.distgit_key)
        if not index_image:
            index_image = PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=self.ocp_version[0], minor=self.ocp_version[1])
            logger.info(
                "Using default index image %s for OCP %s.%s", index_image, self.ocp_version[0], self.ocp_version[1]
            )

        # Get package name of the operator
        package_name = await self._get_package_name(metadata)

        # Render the catalog from the index image
        migrate_level = "none"
        if self.ocp_version >= (4, 17):
            migrate_level = "bundle-object-to-csv-metadata"
        catalog_blobs = await self._get_catalog_blobs_from_index_image(
            index_image, package_name, migrate_level=migrate_level
        )
        if not catalog_blobs:
            if strict:
                raise IOError(f"Package {package_name} not found in index image {index_image}; Is it published?")
            logger.info(
                "No catalog blobs found in index image %s for package %s; will clean up the catalog directory",
                index_image,
                package_name,
            )

        # Clone the FBC repo
        build_branch = _generate_fbc_branch_name(
            group=self.group, assembly=self.assembly, distgit_key=metadata.distgit_key, ocp_version=self.ocp_version
        )
        logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_git_repo, build_branch, repo_dir)
        build_repo = BuildRepo(url=self.fbc_git_repo, branch=build_branch, local_dir=repo_dir, logger=logger)
        await build_repo.ensure_source(upcycle=self.upcycle, strict=False)

        # Update the FBC directory
        await self._update_dir(build_repo, package_name, catalog_blobs, logger)

        # Validate the catalog
        logger.info("Validating the catalog")
        await opm.validate(repo_dir.joinpath("catalog"))

        # Commit and push the changes
        message = self.commit_message or f"Imported catalog from index image {index_image} for {metadata.distgit_key}"
        await build_repo.commit(message, allow_empty=True)
        if self.push:
            await build_repo.push()
            logger.info("Changes pushed to remote repository %s branch %s", self.fbc_git_repo, build_branch)
        else:
            logger.info("Not pushing changes to remote repository")

    async def _update_dir(
        self,
        build_repo: BuildRepo,
        package_name: str,
        catalog_blobs: List[Dict] | None,
        logger: logging.Logger,
    ):
        """Update the FBC directory with the given package name and catalog blobs."""
        repo_dir = build_repo.local_dir

        # Write catalog_blobs to catalog/<package>/catalog.yaml
        catalog_base_dir = repo_dir.joinpath("catalog")
        catalog_base_dir.mkdir(parents=True, exist_ok=True)
        catalog_dir = catalog_base_dir.joinpath(package_name)
        if catalog_blobs:
            catalog_dir.mkdir(parents=True, exist_ok=True)
            catalog_file_path = catalog_dir.joinpath("catalog.yaml")
            logger.info("Writing catalog blobs to %s", catalog_file_path)
            with catalog_file_path.open('w') as f:
                yaml.dump_all(catalog_blobs, f)
        elif catalog_dir.exists():
            logger.info("Catalog blobs are empty. Removing existing catalog directory %s", catalog_dir)
            shutil.rmtree(catalog_dir)

        # Generate Dockerfile
        df_path = repo_dir.joinpath("catalog.Dockerfile")
        logger.info("Generating Dockerfile %s", df_path)
        if df_path.exists():
            logger.info("Removing existing Dockerfile %s", df_path)
            df_path.unlink()
        base_image_format = (
            BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if self.ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        )
        base_image = base_image_format.format(major=self.ocp_version[0], minor=self.ocp_version[1])
        await opm.generate_dockerfile(repo_dir, "catalog", base_image=base_image, builder_image=base_image)

        logger.info("FBC directory updated")

    @alru_cache
    async def _render_index_image(self, index_image: str, migrate_level: str = "none") -> List[Dict]:
        blobs = await retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))(opm.render)(
            index_image,
            auth=self.auth,
            migrate_level=migrate_level,
        )
        return blobs

    def _filter_catalog_blobs(self, blobs: List[Dict], allowed_package_names: Set[str]):
        """Filter catalog blobs by package names.

        :param blobs: List of catalog blobs.
        :param allowed_package_names: Set of allowed package names.
        :return: Dict of filtered catalog blobs.
        """
        filtered: Dict[str, List[Dict[str, Any]]] = {}  # key is package name, value is blobs
        for blob in blobs:
            schema = blob["schema"]
            package_name = None
            match schema:
                case "olm.package":
                    package_name = blob["name"]
                case "olm.channel" | "olm.bundle" | "olm.deprecations":
                    package_name = blob["package"]
            if not package_name:
                raise IOError(f"Couldn't determine package name for unknown schema: {schema}")
            if package_name not in allowed_package_names:
                continue  # filtered out; skipping
            if package_name not in filtered:
                filtered[package_name] = []
            filtered[package_name].append(blob)
        return filtered

    async def _get_catalog_blobs_from_index_image(
        self, index_image: str, package_name: str, migrate_level: str = "none"
    ):
        blobs = await self._render_index_image(index_image, migrate_level=migrate_level)
        filtered_blobs = self._filter_catalog_blobs(blobs, {package_name})
        if package_name not in filtered_blobs:
            return
        return filtered_blobs[package_name]

    async def _get_package_name(self, metadata: ImageMetadata) -> str:
        """Get OLM package name of the given OLM operator
        This function loads OLM package name for the operator image
        by resolving sources and loading package.yaml files.

        :param metadata: operator image metadata.
        :return: OLM package name.
        """
        source_resolver = metadata.runtime.source_resolver
        assert source_resolver, "Source resolver is not initialized; Doozer bug?"
        source = await asyncio.to_thread(source_resolver.resolve_source, metadata)
        source_dir = source_resolver.get_source_dir(source, metadata)
        csv_config = metadata.config.get('update-csv')
        if not csv_config:
            raise ValueError(f"update-csv config not found for {metadata.distgit_key}")
        source_path = source_dir.joinpath(csv_config['manifests-dir'])
        package_yaml_file = next(source_path.glob('**/*package.yaml'))
        with package_yaml_file.open() as f:
            package_yaml = yaml.load(f)
            package_name = package_yaml.get('packageName')
            if not package_name:
                raise IOError(f"Package name not found in {package_yaml_file}")
            return str(package_name)


class KonfluxFbcFragmentMerger:
    def __init__(
        self,
        dry_run: bool,
        working_dir: Path,
        group: str,
        group_config: Dict[str, Any],
        konflux_namespace: str,
        konflux_kubeconfig: str | None,
        konflux_context: str | None = None,
        upcycle: bool = False,
        commit_message: str | None = None,
        fbc_git_repo: str = constants.ART_FBC_GIT_REPO,
        fbc_git_branch: str | None = None,
        fbc_git_username: str | None = None,
        fbc_git_password: str | None = None,
        registry_auth: Optional[str] = None,
        skip_checks: bool = False,
        plr_template: Optional[str] = None,
        major_minor_override: Optional[Tuple[int, int]] = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the KonfluxFbcFragmentMerger.
        """
        if bool(fbc_git_username) != bool(fbc_git_password):
            raise ValueError("Both fbc_git_username and fbc_git_password must be provided or neither.")
        self.dry_run = dry_run
        self.working_dir = Path(working_dir)
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_namespace = konflux_namespace
        self.konflux_context = konflux_context
        self.group = group
        self.group_config = group_config
        self.upcycle = upcycle
        self.commit_message = commit_message
        self.fbc_git_repo = fbc_git_repo
        self.fbc_git_branch = fbc_git_branch or f"art-{self.group}-fbc-stage"
        self.fbc_git_username = fbc_git_username
        self.fbc_git_password = fbc_git_password
        self.registry_auth = registry_auth
        self.skip_checks = skip_checks
        self.plr_template = plr_template or constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL
        self.major_minor_override = major_minor_override
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)
        self._konflux_client = KonfluxClient.from_kubeconfig(
            config_file=self.konflux_kubeconfig,
            default_namespace=self.konflux_namespace,
            context=self.konflux_context,
            dry_run=self.dry_run,
            logger=logger,
        )

    async def run(self, fragments: Collection[str], target_index: str):
        if not fragments:
            raise ValueError("At least one fragment must be provided.")
        if not target_index:
            raise ValueError("Target index must be provided.")
        if self.major_minor_override:
            major, minor = self.major_minor_override
        else:
            major = int(self.group_config.get("vars", {}).get("MAJOR"))
            minor = int(self.group_config.get("vars", {}).get("MINOR"))
        logger = self._logger
        konflux_client = self._konflux_client

        repo_dir = self.working_dir
        logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_git_repo, self.fbc_git_branch, repo_dir)
        build_repo = BuildRepo(
            url=self.fbc_git_repo,
            branch=self.fbc_git_branch,
            local_dir=repo_dir,
            username=self.fbc_git_username,
            password=self.fbc_git_password,
            logger=logger,
        )
        await build_repo.ensure_source(upcycle=self.upcycle, strict=False)

        # Merge ImageDigestMirrorSets
        idms_path = repo_dir.joinpath(".tekton/images-mirror-set.yaml")
        idms_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Generating ImageDigestMirrorSet %s", idms_path)
        target_idms = await self._merge_idms(fragments)
        with idms_path.open('w') as ims_file:
            yaml.dump(target_idms, ims_file)
        logger.info("ImageDigestMirrorSet written to %s", idms_path)

        # Render the FBC fragments
        logger.info("Rendering FBC fragments...")
        semaphore = asyncio.BoundedSemaphore(10)  # Limit concurrency to avoid overwhelming the registry

        async def _render_fragment(fragment: str):
            async with semaphore:
                return await opm.render(
                    fragment,
                    output_format="yaml",
                    auth=opm.OpmRegistryAuth(self.registry_auth) if self.registry_auth else None,
                )

        all_blobs = await asyncio.gather(*(map(_render_fragment, fragments)))
        catalog_blobs = list(itertools.chain.from_iterable(all_blobs))

        # Write the rendered blobs to a file
        # Write catalog_blobs to catalog/catalog.yaml
        catalog_dir = repo_dir.joinpath("catalog")
        catalog_dir.mkdir(parents=True, exist_ok=True)
        catalog_file_path = catalog_dir.joinpath("catalog.yaml")
        LOGGER.info("Writing catalog blobs to %s", catalog_file_path)
        with catalog_file_path.open('w') as f:
            yaml.dump_all(catalog_blobs, f)

        # Generate Dockerfile
        df_path = repo_dir.joinpath("catalog.Dockerfile")
        LOGGER.info("Generating Dockerfile %s", df_path)
        if df_path.exists():
            LOGGER.info("Removing existing Dockerfile %s", df_path)
            df_path.unlink()
        base_image_format = (
            BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if (major, minor) >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        )
        base_image = base_image_format.format(major=major, minor=minor)
        await opm.generate_dockerfile(repo_dir, "catalog", base_image=base_image, builder_image=base_image)

        # Validate the catalog
        logger.info("Validating the catalog")
        await opm.validate(catalog_dir)

        # Commit the changes to the git repository
        if not self.dry_run:
            logger.info("Committing changes to the git repository...")
            message = self.commit_message or f"Merge FBC fragments\n\n{', '.join(fragments)}"
            await build_repo.commit(message, allow_empty=True)
            logger.info("Pushing changes to the remote repository...")
            await build_repo.push()
            logger.info(f"Changes pushed to remote repository {self.fbc_git_repo} branch {self.fbc_git_branch}")
        else:
            logger.info("Dry run enabled, not pushing changes.")

        # Start the build
        retries = 3
        for attempt in range(retries):
            logger.info(f"Attempt {attempt + 1} of {retries} to start the build...")
            error = None
            created_plr = await self._start_build(
                build_repo=build_repo,
                output_image=target_index,
            )
            plr_name = created_plr.name
            plr_url = konflux_client.resource_url(created_plr.to_dict())
            logger.info(f"Created Pipelinerun {plr_name}: {plr_url}")

            # Wait for the Pipelinerun to complete
            logger.info(f"Waiting for Pipelinerun {plr_name} to complete...")
            plr_info = await konflux_client.wait_for_pipelinerun(plr_name, self.konflux_namespace)
            plr = plr_info.to_dict()
            succeeded_condition = plr_info.find_condition('Succeeded')
            outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)
            logger.info(
                "Pipelinerun %s completed with outcome: %s",
                plr_name,
                outcome,
            )
            if outcome is not KonfluxBuildOutcome.SUCCESS:
                error = KonfluxFbcBuildError(
                    f"Pipelinerun {plr_name} failed with outcome: {outcome.name} - {succeeded_condition.message or 'No message provided'}",
                    plr_name,
                    plr,
                )
            else:
                error = None
                break
        if error:
            logger.error("Build failed after %d attempts: %s", retries, error)
            raise error

    @staticmethod
    def get_application_name(group: str):
        """
        Get the application name for the given group.
        The application name is in the format `fbc-{group}-stage`.
        """
        return f"fbc-openshift-{group}-stage".replace(".", "-").replace("_", "-")

    @staticmethod
    def get_component_name(group: str):
        """
        Get the component name for the given group.
        The component name is currently the same as the application name.
        """
        app_name = KonfluxFbcFragmentMerger.get_application_name(group)
        return app_name

    async def _merge_idms(self, fragments: Collection[str]) -> dict:
        """
        Merge ImageDigestMirrorSets from the given fragments.
        This function fetches the ImageDigestMirrorSet for each fragment,
        combines them, and returns a single ImageDigestMirrorSet.

        :param fragments: List of fragment image pull specs.
        :return: Merged ImageDigestMirrorSet as a dictionary.
        """
        if not fragments:
            raise ValueError("At least one fragment must be provided.")

        async with httpx.AsyncClient(verify=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)) as http_client:

            async def _get_fragment_idms(fragment: str) -> list[dict]:
                """
                Fetch the ImageDigestMirrorSet for a given fragment.
                """
                fragment_info = await util.oc_image_info_for_arch_async__caching(
                    fragment,
                    registry_config=self.registry_auth,
                )
                commit = fragment_info["config"]["config"]["Labels"]["vcs-ref"]
                url = (
                    f"https://raw.githubusercontent.com/openshift-priv/art-fbc/{commit}/.tekton/images-mirror-set.yaml"
                )
                response = await http_client.get(url)
                response.raise_for_status()
                idms_data = yaml.load(StringIO(response.text))
                return idms_data

            idms_list = await asyncio.gather(*(_get_fragment_idms(fragment) for fragment in fragments))

        mirrors_by_source = defaultdict(set)
        for idms in idms_list:
            for mirror in idms.get("spec", {}).get("imageDigestMirrors", []):
                source = mirror.get("source")
                mirrors = mirror.get("mirrors", [])
                if source and mirrors:
                    mirrors_by_source[source].update(mirrors)
        target_idms = {
            "apiVersion": "config.openshift.io/v1",
            "kind": "ImageDigestMirrorSet",
            "metadata": {
                "name": "art-stage-mirror-set",
                "namespace": "openshift-marketplace",
            },
            "spec": {
                "imageDigestMirrors": [
                    {
                        "source": source,
                        "mirrors": sorted(mirrors),
                    }
                    for source, mirrors in sorted(mirrors_by_source.items())
                ],
            },
        }
        return target_idms

    async def _start_build(self, build_repo: BuildRepo, output_image: str):
        logger = self._logger
        konflux_client = self._konflux_client
        commit_sha = build_repo.commit_hash
        assert commit_sha, "Commit SHA should not be empty; Doozer bug?"

        # Create application
        app_name = KonfluxFbcFragmentMerger.get_application_name(self.group)

        await konflux_client.ensure_application(app_name, app_name)
        logger.info(f"Created application {app_name}")

        # Create component
        comp_name = KonfluxFbcFragmentMerger.get_component_name(self.group)
        target_index_split = output_image.rsplit(":", maxsplit=1)
        if len(target_index_split) < 2:
            raise ValueError(f"Invalid output_image format: {output_image}. Expected format is 'repo:tag'.")
        output_image_repo = target_index_split[0]
        output_image_tag = target_index_split[1]
        await konflux_client.ensure_component(
            application=app_name,
            name=comp_name,
            component_name=comp_name,
            source_url=build_repo.https_url,
            revision=build_repo.branch,
            image_repo=output_image_repo,
        )
        logger.info(f"Created component {comp_name} in application {app_name}")

        arches = self.group_config.get("arches", list(KonfluxClient.SUPPORTED_ARCHES.keys()))

        created_plr = await konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{comp_name}-",
            namespace=self.konflux_namespace,
            application_name=app_name,
            component_name=comp_name,
            git_url=build_repo.https_url,
            commit_sha=commit_sha,
            target_branch=self.fbc_git_branch or commit_sha,
            output_image=f"{output_image_repo}:{output_image_tag}",
            additional_tags=[],
            vm_override={},
            building_arches=arches,  # FBC should be built for all supported arches
            hermetic=True,  # FBC should be built in hermetic mode
            dockerfile="catalog.Dockerfile",
            skip_checks=self.skip_checks,
            pipelinerun_template_url=self.plr_template,
            build_priority=FBC_BUILD_PRIORITY,
        )
        return created_plr


class KonfluxFbcRebaser:
    def __init__(
        self,
        base_dir: str | PathLike,
        group: str,
        assembly: str,
        version: str,
        release: str,
        commit_message: str,
        push: bool,
        fbc_repo: str,
        upcycle: bool,
        ocp_version_override: Optional[Tuple[int, int]] = None,
        record_logger: Optional[RecordLogger] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.group = group
        self.assembly = assembly
        self.version = version
        self.release = release
        self.commit_message = commit_message
        self.push = push
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.upcycle = upcycle
        self.ocp_version_override = ocp_version_override
        self._record_logger = record_logger
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)

    @staticmethod
    def get_fbc_name(image_name: str):
        return f"{image_name}-fbc"

    async def rebase(
        self, metadata: ImageMetadata, bundle_build: KonfluxBundleBuildRecord, version: str, release: str
    ) -> str:
        bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{bundle_short_name}]")
        repo_dir = self.base_dir.joinpath(metadata.distgit_key)

        name = self.get_fbc_name(metadata.distgit_key)
        nvr = f"{name}-{version}-{release}"
        record = {
            # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            'status': -1,
            "name": name,
            "message": "Unknown failure",
            "fbc_nvr": nvr,
            "bundle_nvrs": ','.join(
                [str(bundle_build.nvr)]
            ),  # Currently we only support rebasing for one bundle at a time
        }

        try:
            # Get OCP version for branch naming
            group_config = metadata.runtime.group_config
            if self.ocp_version_override:
                ocp_version = self.ocp_version_override
            else:
                ocp_version = int(group_config.vars.MAJOR), int(group_config.vars.MINOR)

            # Clone the FBC repo
            fbc_build_branch = _generate_fbc_branch_name(
                group=self.group, assembly=self.assembly, distgit_key=metadata.distgit_key, ocp_version=ocp_version
            )
            logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_repo, fbc_build_branch, repo_dir)
            build_repo = BuildRepo(url=self.fbc_repo, branch=fbc_build_branch, local_dir=repo_dir, logger=logger)
            await build_repo.ensure_source(
                upcycle=self.upcycle,
                strict=False,
            )

            # Update the FBC repo
            rebase_nvr = await self._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)
            assert rebase_nvr == nvr, f"rebase_nvr != nvr; doozer bug? {rebase_nvr} != {nvr}"

            # Validate the updated catalog
            logger.info("Validating the updated catalog")
            await opm.validate(build_repo.local_dir.joinpath("catalog"))

            # Commit and push the changes
            addtional_message = (
                f"\n\n---\noperator: {bundle_build.operator_nvr}\noperands: {','.join(bundle_build.operand_nvrs)}"
            )
            await build_repo.commit(self.commit_message + addtional_message, allow_empty=True)
            if self.push:
                await build_repo.push()
            else:
                logger.info("Not pushing changes to remote repository")

            record["message"] = "Success"
            record["status"] = 0
            logger.info("rebase complete")
        except Exception as error:
            record['message'] = str(error)
            raise
        finally:
            if self._record_logger:
                self._record_logger.add_record("rebase_fbc_konflux", **record)
        return nvr

    async def _get_referenced_images(self, konflux_db: KonfluxDb, bundle_build: KonfluxBundleBuildRecord):
        assert bundle_build.operator_nvr, "operator_nvr is empty; doozer bug?"
        nvrs = {bundle_build.operator_nvr}
        if bundle_build.operand_nvrs:
            nvrs |= set(bundle_build.operand_nvrs)
        assert konflux_db.record_cls is KonfluxBuildRecord, "konflux_db is not bound to KonfluxBuildRecord. Doozer bug?"
        ref_builds = await konflux_db.get_build_records_by_nvrs(list(nvrs), exclude_large_columns=True)
        return ref_builds

    async def _rebase_dir(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        bundle_build: KonfluxBundleBuildRecord,
        version: str,
        release: str,
        logger: logging.Logger,
    ) -> str:
        logger.info("Rebasing dir %s", build_repo.local_dir)

        group_config = metadata.runtime.group_config
        if self.ocp_version_override:
            ocp_version = self.ocp_version_override
        else:
            ocp_version = int(group_config.vars.MAJOR), int(group_config.vars.MINOR)
        # OCP 4.17+ requires bundle object to CSV metadata migration.
        migrate_level = "none"
        if ocp_version >= (4, 17):
            migrate_level = "bundle-object-to-csv-metadata"

        # This will raise an ValueError if the bundle delivery repo name is not set in the metadata config.
        delivery_repo_name = metadata.get_olm_bundle_delivery_repo_name()

        # Fetch bundle image info and blob
        logger.info("Fetching OLM bundle image %s from %s", bundle_build.nvr, bundle_build.image_pullspec)
        olm_bundle_image_info = await self._fetch_olm_bundle_image_info(bundle_build)
        labels = olm_bundle_image_info["config"]["config"]["Labels"]
        image_name = labels.get("name")
        if not image_name:
            raise IOError("Image name not found in bundle image")
        channel_names = labels.get("operators.operatorframework.io.bundle.channels.v1")
        if not channel_names:
            raise IOError("Channel name not found in bundle image")
        channel_names = channel_names.split(",")
        default_channel_name = labels.get("operators.operatorframework.io.bundle.channel.default.v1")

        olm_bundle_name, olm_package, olm_bundle_blob = await self._fetch_olm_bundle_blob(
            bundle_build, migrate_level=migrate_level
        )
        if olm_package != labels.get("operators.operatorframework.io.bundle.package.v1"):
            raise IOError(
                f"Package name mismatch: {olm_package} != {labels.get('operators.operatorframework.io.bundle.package.v1')}"
            )

        manifests_dir = labels.get("operators.operatorframework.io.bundle.manifests.v1")
        if not manifests_dir:
            raise IOError("Manifests directory not found in bundle image labels")
        csv = await self._load_csv_from_bundle(bundle_build, manifests_dir)
        olm_skip_range = csv.get("metadata", {}).get("annotations", {}).get("olm.skipRange", None)

        # Load referenced images
        konflux_db: KonfluxDb = metadata.runtime.konflux_db
        konflux_db.bind(KonfluxBuildRecord)
        ref_builds = await self._get_referenced_images(konflux_db, bundle_build)
        ref_builds.append(bundle_build)  # Include the bundle build itself
        ref_pullspecs = {
            b.image_pullspec.replace(constants.REGISTRY_PROXY_BASE_URL, constants.BREW_REGISTRY_BASE_URL)
            for b in ref_builds
        }

        # Load current catalog
        catalog_dir = build_repo.local_dir.joinpath("catalog", olm_package)
        catalog_file_path = catalog_dir.joinpath("catalog.yaml")
        if catalog_file_path.is_file():
            logger.info("Catalog file %s already exists, loading it", catalog_file_path)
            with catalog_file_path.open() as f:
                catalog_blobs = list(yaml.load_all(f))
        else:
            logger.info("Catalog file %s does not exist, bootstrap a new one", catalog_file_path)
            icon = next(iter(csv.get("spec", {}).get("icon", [])), None)
            catalog_blobs = self._bootstrap_catalog(olm_package, default_channel_name, icon)

        categorized_catalog_blobs = self._catagorize_catalog_blobs(catalog_blobs)
        if olm_package not in categorized_catalog_blobs:
            raise IOError(f"Package {olm_package} not found in catalog. The FBC repo is not properly initialized.")
        if len(categorized_catalog_blobs) > 1:
            logger.warning(
                f"The catalog file {catalog_file_path} has multiple packages: {','.join(categorized_catalog_blobs.keys())}"
            )

        # Update the catalog
        def _update_channel(channel: Dict):
            # Update "skips" in the channel
            # FIXME: We try to mimic how `skips` field is updated by the old ET centric process.
            # We should verify if this is correct.
            skips = None
            bundle_with_skips = next(
                (it for it in channel['entries'] if it.get('skips')), None
            )  # Find which bundle has the skips field
            if bundle_with_skips is not None:
                # Then we move the skips field to the new bundle
                # and add the bundle name of bundle_with_skips to the skips field
                skips = set(bundle_with_skips.pop('skips'))
                skips = (skips | {bundle_with_skips['name']}) - {olm_bundle_name}
            elif len(channel['entries']) == 1:
                # In case the channel only contain one single entry, that bundle should
                # become the only member of new-entry's `skip`.
                skips = {channel['entries'][0]['name']}

            # For an operator bundle that uses replaces -- such as OADP
            # Update "replaces" in the channel
            replaces = None
            if not self.group.startswith('openshift-'):
                # Find the current head - the entry that is not replaced by any other entry
                bundle_with_replaces = [it for it in channel['entries']]
                replaced_names = {it.get('replaces') for it in bundle_with_replaces if it.get('replaces')}
                current_head = next((it for it in bundle_with_replaces if it['name'] not in replaced_names), None)
                if current_head:
                    # The new bundle should replace the current head
                    replaces = current_head['name']

            # Add the current bundle to the specified channel in the catalog
            entry = next((entry for entry in channel['entries'] if entry['name'] == olm_bundle_name), None)
            if not entry:
                logger.info("Adding bundle %s to channel %s", olm_bundle_name, channel['name'])
                entry = {"name": olm_bundle_name}
                channel['entries'].append(entry)
            else:
                logger.warning("Bundle %s already exists in channel %s. Replacing...", olm_bundle_name, channel['name'])
                entry.clear()
                entry["name"] = olm_bundle_name
            if olm_skip_range:
                entry["skipRange"] = olm_skip_range
            if skips:
                entry["skips"] = sorted(skips)
            if replaces:
                entry["replaces"] = replaces

        for channel_name in channel_names:
            logger.info("Updating channel %s", channel_name)
            channel = categorized_catalog_blobs[olm_package]["olm.channel"].get(channel_name, None)
            if not channel:
                raise IOError(
                    f"Channel {channel_name} not found in package {olm_package}. The FBC repo is not properly initialized."
                )
            _update_channel(channel)

        # Set default channel
        if default_channel_name:
            package_blob = categorized_catalog_blobs[olm_package]["olm.package"][olm_package]
            if package_blob.get("defaultChannel") != default_channel_name:
                logger.info("Setting default channel to %s", default_channel_name)
                package_blob["defaultChannel"] = default_channel_name

        # Replace pullspecs to use the prod registry
        digest = bundle_build.image_pullspec.split('@', 1)[-1]
        bundle_prod_pullspec = f"{constants.DELIVERY_IMAGE_REGISTRY}/{delivery_repo_name}@{digest}"
        olm_bundle_blob["image"] = bundle_prod_pullspec
        related_images = olm_bundle_blob.get("relatedImages", [])
        if related_images:
            target_entry = next((it for it in related_images if it['name'] == ""), None)
            if target_entry:
                logger.info("Replacing image reference %s with %s", target_entry['image'], bundle_prod_pullspec)
                target_entry['image'] = bundle_prod_pullspec

        # Add the new bundle blob to the catalog
        if olm_bundle_name not in categorized_catalog_blobs[olm_package].setdefault("olm.bundle", {}):
            logger.info("Adding bundle %s to package %s", olm_bundle_name, olm_package)
            categorized_catalog_blobs[olm_package]["olm.bundle"][olm_bundle_name] = olm_bundle_blob
            catalog_blobs.append(olm_bundle_blob)
        else:  # Update the existing bundle blob
            logger.warning("Bundle %s already exists in package %s. Replacing...", olm_bundle_name, olm_package)
            target_entry = categorized_catalog_blobs[olm_package]["olm.bundle"][olm_bundle_name]
            target_entry.clear()
            target_entry.update(olm_bundle_blob)

        # Write the updated catalog back to the file
        logger.info("Writing updated catalog to %s", catalog_file_path)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        with catalog_file_path.open("w") as f:
            yaml.dump_all(catalog_blobs, f)

        # Add ImageDigestMirrorSet .tekton/images-mirror-set.yaml to the build repo to make Enterprise Contract happy
        image_digest_mirror_set = self._generate_image_digest_mirror_set(
            categorized_catalog_blobs[olm_package]["olm.bundle"].values(),
            ref_pullspecs,
        )
        dot_tekton_dir = build_repo.local_dir.joinpath(".tekton")
        images_mirror_set_file_path = dot_tekton_dir.joinpath("images-mirror-set.yaml")
        if not image_digest_mirror_set:
            logger.info("No related images found, deleting existing images-mirror-set.yaml")
            images_mirror_set_file_path.unlink(missing_ok=True)
        else:
            logger.info("Adding ImageDigestMirrorSet to build repo")
            dot_tekton_dir.mkdir(exist_ok=True)
            with images_mirror_set_file_path.open('w') as f:
                yaml.dump(image_digest_mirror_set, f)

        # Update Dockerfile
        dockerfile_path = build_repo.local_dir.joinpath("catalog.Dockerfile")
        if dockerfile_path.is_file():
            logger.info("Dockerfile %s already exists, removing it", dockerfile_path)
            await asyncio.to_thread(dockerfile_path.unlink)
        base_image_format = (
            BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        )
        base_image = base_image_format.format(major=ocp_version[0], minor=ocp_version[1])
        await opm.generate_dockerfile(build_repo.local_dir, "catalog", base_image=base_image, builder_image=base_image)

        logger.info("Updating Dockerfile %s", dockerfile_path)
        dfp = DockerfileParser(str(dockerfile_path))
        metadata_envs: Dict[str, str] = {
            '__doozer_group': self.group,
            '__doozer_key': metadata.distgit_key,
            '__doozer_version': version,
            '__doozer_release': release,
            '__doozer_bundle_nvrs': ','.join([str(bundle_build.nvr)]),
        }
        for key, value in metadata_envs.items():
            if dfp.envs.get(key) != value:
                logger.info("Setting %s=%s", key, value)
                dfp.envs[key] = value

        dfp.labels['io.openshift.build.source-location'] = bundle_build.source_repo
        dfp.labels['io.openshift.build.commit.id'] = bundle_build.commitish

        # The following label is used internally by ART's shipment pipeline
        name = self.get_fbc_name(metadata.distgit_key)
        dfp.labels['com.redhat.art.name'] = name
        nvr = f'{name}-{version}-{release}'
        dfp.labels['com.redhat.art.nvr'] = nvr
        return nvr

    def _bootstrap_catalog(
        self, package_name: str, default_channel: str, icon: Dict[str, str] | None
    ) -> List[Dict[str, Any]]:
        """Bootstrap a new catalog for the given package name.
        :param package_name: The name of the package to bootstrap.
        :param default_channel: The default channel for the package.
        :param icon: Optional icon data to include in the package. e.g. {"base64data": "...", "mediatype": "..."}
        :return: A dictionary representing the catalog.
        """
        # Following https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/v4.13/catalog-template.json
        if not icon:
            # Default icon if not provided
            icon = {
                "base64data": "PHN2ZyBpZD0iZjc0ZTM5ZDEtODA2Yy00M2E0LTgyZGQtZjM3ZjM1NWQ4YWYzIiBkYXRhLW5hbWU9Ikljb24iIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDM2IDM2Ij4KICA8ZGVmcz4KICAgIDxzdHlsZT4KICAgICAgLmE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCB7CiAgICAgICAgZmlsbDogI2UwMDsKICAgICAgfQogICAgPC9zdHlsZT4KICA8L2RlZnM+CiAgPGc+CiAgICA8cGF0aCBjbGFzcz0iYTQxYzUyMzQtYTE0YS00ZjNkLTkxNjAtNDQ3MmI3NmQwMDQwIiBkPSJNMjUsMTcuMzhIMjMuMjNhNS4yNyw1LjI3LDAsMCwwLTEuMDktMi42NGwxLjI1LTEuMjVhLjYyLjYyLDAsMSwwLS44OC0uODhsLTEuMjUsMS4yNWE1LjI3LDUuMjcsMCwwLDAtMi42NC0xLjA5VjExYS42Mi42MiwwLDEsMC0xLjI0LDB2MS43N2E1LjI3LDUuMjcsMCwwLDAtMi42NCwxLjA5bC0xLjI1LTEuMjVhLjYyLjYyLDAsMCwwLS44OC44OGwxLjI1LDEuMjVhNS4yNyw1LjI3LDAsMCwwLTEuMDksMi42NEgxMWEuNjIuNjIsMCwwLDAsMCwxLjI0aDEuNzdhNS4yNyw1LjI3LDAsMCwwLDEuMDksMi42NGwtMS4yNSwxLjI1YS42MS42MSwwLDAsMCwwLC44OC42My42MywwLDAsMCwuODgsMGwxLjI1LTEuMjVhNS4yNyw1LjI3LDAsMCwwLDIuNjQsMS4wOVYyNWEuNjIuNjIsMCwwLDAsMS4yNCwwVjIzLjIzYTUuMjcsNS4yNywwLDAsMCwyLjY0LTEuMDlsMS4yNSwxLjI1YS42My42MywwLDAsMCwuODgsMCwuNjEuNjEsMCwwLDAsMC0uODhsLTEuMjUtMS4yNWE1LjI3LDUuMjcsMCwwLDAsMS4wOS0yLjY0SDI1YS42Mi42MiwwLDAsMCwwLTEuMjRabS03LDQuNjhBNC4wNiw0LjA2LDAsMSwxLDIyLjA2LDE4LDQuMDYsNC4wNiwwLDAsMSwxOCwyMi4wNloiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik0yNy45LDI4LjUyYS42Mi42MiwwLDAsMS0uNDQtLjE4LjYxLjYxLDAsMCwxLDAtLjg4LDEzLjQyLDEzLjQyLDAsMCwwLDIuNjMtMTUuMTkuNjEuNjEsMCwwLDEsLjMtLjgzLjYyLjYyLDAsMCwxLC44My4yOSwxNC42NywxNC42NywwLDAsMS0yLjg4LDE2LjYxQS42Mi42MiwwLDAsMSwyNy45LDI4LjUyWiIvPgogICAgPHBhdGggY2xhc3M9ImE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCIgZD0iTTI3LjksOC43M2EuNjMuNjMsMCwwLDEtLjQ0LS4xOUExMy40LDEzLjQsMCwwLDAsMTIuMjcsNS45MWEuNjEuNjEsMCwwLDEtLjgzLS4zLjYyLjYyLDAsMCwxLC4yOS0uODNBMTQuNjcsMTQuNjcsMCwwLDEsMjguMzQsNy42NmEuNjMuNjMsMCwwLDEtLjQ0LDEuMDdaIi8+CiAgICA8cGF0aCBjbGFzcz0iYTQxYzUyMzQtYTE0YS00ZjNkLTkxNjAtNDQ3MmI3NmQwMDQwIiBkPSJNNS4zNSwyNC42MmEuNjMuNjMsMCwwLDEtLjU3LS4zNUExNC42NywxNC42NywwLDAsMSw3LjY2LDcuNjZhLjYyLjYyLDAsMCwxLC44OC44OEExMy40MiwxMy40MiwwLDAsMCw1LjkxLDIzLjczYS42MS42MSwwLDAsMS0uMy44M0EuNDguNDgsMCwwLDEsNS4zNSwyNC42MloiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik0xOCwzMi42MkExNC42NCwxNC42NCwwLDAsMSw3LjY2LDI4LjM0YS42My42MywwLDAsMSwwLS44OC42MS42MSwwLDAsMSwuODgsMCwxMy40MiwxMy40MiwwLDAsMCwxNS4xOSwyLjYzLjYxLjYxLDAsMCwxLC44My4zLjYyLjYyLDAsMCwxLS4yOS44M0ExNC42NywxNC42NywwLDAsMSwxOCwzMi42MloiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik0zMCwyOS42MkgyN2EuNjIuNjIsMCwwLDEtLjYyLS42MlYyNmEuNjIuNjIsMCwwLDEsMS4yNCwwdjIuMzhIMzBhLjYyLjYyLDAsMCwxLDAsMS4yNFoiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik03LDMwLjYyQS42Mi42MiwwLDAsMSw2LjM4LDMwVjI3QS42Mi42MiwwLDAsMSw3LDI2LjM4aDNhLjYyLjYyLDAsMCwxLDAsMS4yNEg3LjYyVjMwQS42Mi42MiwwLDAsMSw3LDMwLjYyWiIvPgogICAgPHBhdGggY2xhc3M9ImE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCIgZD0iTTI5LDkuNjJIMjZhLjYyLjYyLDAsMCwxLDAtMS4yNGgyLjM4VjZhLjYyLjYyLDAsMCwxLDEuMjQsMFY5QS42Mi42MiwwLDAsMSwyOSw5LjYyWiIvPgogICAgPHBhdGggY2xhc3M9ImE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCIgZD0iTTksMTAuNjJBLjYyLjYyLDAsMCwxLDguMzgsMTBWNy42Mkg2QS42Mi42MiwwLDAsMSw2LDYuMzhIOUEuNjIuNjIsMCwwLDEsOS42Miw3djNBLjYyLjYyLDAsMCwxLDksMTAuNjJaIi8+CiAgPC9nPgo8L3N2Zz4K",
                "mediatype": "image/svg+xml",
            }
        elif "base64data" not in icon or "mediatype" not in icon:
            raise ValueError("Icon must contain 'base64data' and 'mediatype' fields")

        default_channel = default_channel or "stable"
        package_blob = {
            "defaultChannel": default_channel,
            "icon": icon,
            "name": package_name,
            "schema": "olm.package",
        }
        channel_blob = {
            "entries": [],
            "name": default_channel,
            "package": package_name,
            "schema": "olm.channel",
        }
        return [
            package_blob,
            channel_blob,
        ]

    def _generate_image_digest_mirror_set(self, olm_bundle_blobs: Iterable[Dict], ref_pullspecs: Iterable[str]):
        dest_repos = {}
        for bundle_blob in olm_bundle_blobs:
            for related_image in bundle_blob.get("relatedImages", []):
                if '@' in related_image["image"]:
                    repo, digest = related_image["image"].split('@', 1)
                    dest_repos[digest] = repo
                else:
                    continue
        for pullspec in ref_pullspecs:
            p_split = pullspec.split('@', 1)
            if len(p_split) == 1:
                raise ValueError(
                    f"{p_split} invalid, it dosen't contains @, olm_bundle_blobs: {olm_bundle_blobs} \n ref_pullspecs: {ref_pullspecs}"
                )
        source_repos = {p_split[1]: p_split[0] for pullspec in ref_pullspecs if (p_split := pullspec.split('@', 1))}
        if not dest_repos:
            return None
        image_digest_mirror_set = {
            "apiVersion": "config.openshift.io/v1",
            "kind": "ImageDigestMirrorSet",
            "metadata": {
                "name": "art-images-mirror-set",
                "namespace": "openshift-marketplace",
            },
            "spec": {
                "imageDigestMirrors": sorted(
                    [
                        {
                            "source": dest_repos[sha],
                            "mirrors": [
                                source_repo,
                            ],
                        }
                        # If source is same as destination, we don't need to add an IDMS mapping
                        for sha, source_repo in source_repos.items()
                        if sha in dest_repos and source_repo != dest_repos[sha]
                    ],
                    key=lambda x: x["source"],
                ),
            },
        }
        return image_digest_mirror_set

    async def _fetch_olm_bundle_image_info(self, bundle_build: KonfluxBundleBuildRecord):
        return await util.oc_image_info_for_arch_async__caching(
            bundle_build.image_pullspec,
            registry_config=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
        )

    async def _load_csv_from_bundle(self, bundle_build: KonfluxBundleBuildRecord, manifests_dir: str):
        """Load the CSV from the bundle image manifests directory.

        :param bundle_build: The bundle build record.
        :param manifests_dir: The directory where the manifests are stored in the bundle image.
        :return: The loaded CSV as a dictionary.
        """
        with TemporaryDirectory(prefix="doozer-") as tmpdir:
            path_specs = [f"{manifests_dir}:{tmpdir}"]
            await util.oc_image_extract_async(
                bundle_build.image_pullspec,
                path_specs=path_specs,
                registry_config=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
            )
            # Find the CSV file in the extracted manifests directory
            csv_file = next(Path(tmpdir).glob("*.clusterserviceversion.yaml"), None)
            if not csv_file:
                raise IOError(f"CSV file not found in bundle image manifests directory {manifests_dir}")
            return yaml.load(csv_file.open())

    async def _fetch_olm_bundle_blob(self, bundle_build: KonfluxBundleBuildRecord, migrate_level: str):
        """Fetch the olm.bundle blob for the given bundle build.

        :param bundle_build: The bundle build record.
        :param migrate_level: The migration level to use.
        :return: A tuple of (bundle name, package name, bundle blob).
        """
        registry_auth = opm.OpmRegistryAuth(
            path=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
        )
        rendered_blobs = await opm.render(bundle_build.image_pullspec, migrate_level=migrate_level, auth=registry_auth)
        if not isinstance(rendered_blobs, list) or len(rendered_blobs) != 1:
            raise IOError(f"Expected exactly one rendered blob, but got {len(rendered_blobs)}")
        olm_bundle_blob = rendered_blobs[0]
        if olm_bundle_blob.get('schema') != 'olm.bundle':
            raise IOError(f"Bundle blob has invalid schema: {olm_bundle_blob.get('schema')}")
        olm_package = olm_bundle_blob["package"]
        if not olm_package:
            raise IOError("Package not found in bundle blob")
        assert isinstance(olm_package, str), f"Expected package name to be a string, but got {type(olm_package)}"
        olm_bundle_name = olm_bundle_blob["name"]
        if not olm_bundle_name:
            raise IOError("Name not found in bundle blob")
        assert isinstance(olm_bundle_name, str), f"Expected bundle name to be a string, but got {type(olm_bundle_name)}"
        return olm_bundle_name, olm_package, olm_bundle_blob

    def _catagorize_catalog_blobs(self, blobs: List[Dict]):
        """Given a list of catalog blobs, categorize them by schema and package name.

        :param blobs: A list of catalog blobs.
        :return: a dictionary of the form {package_name: {schema: {blob_name: blob}}}
        """
        categorized_blobs = {}
        for blob in blobs:
            schema = blob["schema"]
            package_name = None
            match schema:
                case "olm.package":
                    package_name = blob["name"]
                case "olm.channel" | "olm.bundle" | "olm.deprecations":
                    package_name = blob["package"]
                case _:  # Unknown schema
                    raise IOError(f"Found unsupported schema: {schema}")
            if not package_name:
                raise IOError(f"Couldn't determine package name for unknown schema: {schema}")
            categorized_blobs.setdefault(package_name, {}).setdefault(schema, {})[blob["name"]] = blob
        return categorized_blobs


class KonfluxFbcBuildError(Exception):
    def __init__(self, message: str, pipelinerun_name: str, pipelinerun_dict: Optional[Dict]) -> None:
        super().__init__(message)
        self.pipelinerun_name = pipelinerun_name
        self.pipelinerun_dict = pipelinerun_dict


class KonfluxFbcBuilder:
    def __init__(
        self,
        base_dir: Path,
        group: str,
        assembly: str,
        product: str,
        db: KonfluxDb,
        fbc_repo: str,
        konflux_namespace: str,
        konflux_kubeconfig: Optional[str] = None,
        konflux_context: Optional[str] = None,
        image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO,
        skip_checks: bool = False,
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL,
        dry_run: bool = False,
        major_minor_override: Optional[Tuple[int, int]] = None,
        record_logger: Optional[RecordLogger] = None,
        logger: logging.Logger = LOGGER,
    ):
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self.product = product
        self._db = db
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.konflux_namespace = konflux_namespace
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.pipelinerun_template_url = pipelinerun_template_url
        self.dry_run = dry_run
        self.major_minor_override = major_minor_override
        self.source_git_commits = []
        self._record_logger = record_logger
        self._logger = logger.getChild(self.__class__.__name__)
        self._konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=konflux_namespace,
            config_file=konflux_kubeconfig,
            context=konflux_context,
            dry_run=self.dry_run,
        )

    @staticmethod
    def get_application_name(group_name: str) -> str:
        # Note: for now, we use a different application for each image
        # In future, we might change it to one application per group for all images
        return f"fbc-{group_name}".replace(".", "-").replace("_", "-")

    @staticmethod
    def get_component_name(group_name: str, image_name: str) -> str:
        application_name = KonfluxFbcBuilder.get_application_name(group_name)
        # Openshift doesn't allow dots or underscores in any of its fields, so we replace them with dashes
        name = f"{application_name}-{image_name}".replace(".", "-").replace("_", "-")
        # A component resource name must start with a lower case letter and must be no more than 63 characters long.
        # 'fbc-openshift-4-17-openshift-kubernetes-nmstate-operator' -> 'fbc-ose-4-17-openshift-kubernetes-nmstate-operator'
        name = f"fbc-ose-{name[14:]}" if name.startswith("fbc-openshift-") else name
        return name

    def _extract_git_commits_from_nvrs(self, bundle_nvrs: str) -> list[str]:
        """Extract git commit hashes from bundle NVRs, including the 'g' prefix.

        Example: "cluster-nfd-operator-metadata-container-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1" -> ["gd5498aa"]
        """
        if not bundle_nvrs:
            return []

        import re

        commits = []
        # Split by comma in case there are multiple NVRs
        nvrs = bundle_nvrs.split(',')

        for nvr in nvrs:
            nvr = nvr.strip()
            if nvr:
                # Look for pattern .g<commit> in the NVR and extract including the 'g'
                match = re.search(r'\.(g[a-f0-9]+)\.', nvr)
                if match:
                    commit = match.group(1)  # This includes the 'g' prefix
                    if commit not in commits:  # Avoid duplicates
                        commits.append(commit)

        return commits

    async def _check_image_exists(self, pullspec: str, logger: Optional[logging.Logger] = None) -> bool:
        """Check if an image exists in the registry.

        :param pullspec: Image pullspec to check
        :param logger: Logger instance
        :return: True if image exists, False otherwise
        """
        logger = logger or self._logger
        try:
            logger.debug(f"Checking if image exists: {pullspec}")
            registry_config = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
            await util.oc_image_info_for_arch_async(pullspec, registry_config=registry_config)
            logger.debug(f"Image exists: {pullspec}")
            return True
        except Exception as e:
            logger.debug(f"Image does not exist or is not accessible: {pullspec} - {e}")
            return False

    async def _sync_fbc_related_images_to_share(
        self, fbc_pullspec: str, product: str, logger: Optional[logging.Logger] = None
    ):
        """Sync FBC related images to art-images-share repository if they don't exist.

        :param fbc_pullspec: FBC image pullspec
        :param product: Product name (e.g., 'openshift', 'oadp')
        :param logger: Logger instance
        """
        logger = logger or self._logger

        try:
            logger.info("Extracting related images from FBC...")
            related_images = await artlib_util.extract_related_images_from_fbc(fbc_pullspec, product)
            logger.info(f"Found {len(related_images)} related images in FBC")

            if not related_images:
                logger.info("No related images to sync")
                return

            # Check each image and sync if missing from art-images-share
            for image_pullspec in related_images:
                share_pullspec = image_pullspec.replace(KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_ART_IMAGES_SHARE)

                if await self._check_image_exists(share_pullspec, logger):
                    logger.info(f"Image already exists in art-images-share: {share_pullspec}")
                else:
                    logger.info(f"Syncing {image_pullspec} to art-images-share")
                    try:
                        destination_repo = share_pullspec.split('@')[0]  # Remove digest for destination
                        await artlib_util.sync_to_quay(image_pullspec, destination_repo)
                        logger.info(f"Successfully synced to art-images-share: {share_pullspec}")
                    except Exception as e:
                        logger.warning(f"Failed to sync {image_pullspec} to art-images-share: {e}")

            logger.info("FBC related images sync complete")

        except Exception:
            logger.exception("Error while syncing FBC related images to art-images-share")

    async def build(self, metadata: ImageMetadata, operator_nvr: Optional[str] = None):
        bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{bundle_short_name}]")
        logger.info("Building FBC for %s", metadata.distgit_key)

        record = {
            # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            'status': -1,
            "name": metadata.distgit_key,
            "message": "Unknown failure",
            "task_id": "n/a",
            "task_url": "n/a",
            "fbc_nvr": "n/a",
            "bundle_nvrs": "n/a",
        }

        try:
            # Clone or load the FBC repository
            repo_dir = self.base_dir.joinpath(metadata.distgit_key)
            build_repo = None
            if repo_dir.exists():
                logger.info("Loading existing FBC repository...")
                build_repo = await BuildRepo.from_local_dir(repo_dir, logger)
                logger.info("FBC repository loaded from %s", repo_dir)
            else:
                # Get OCP version for branch naming
                if self.major_minor_override:
                    ocp_version = self.major_minor_override
                else:
                    group_config = metadata.runtime.group_config
                    ocp_version = int(group_config.vars.MAJOR), int(group_config.vars.MINOR)

                build_branch = _generate_fbc_branch_name(
                    group=self.group, assembly=self.assembly, distgit_key=metadata.distgit_key, ocp_version=ocp_version
                )
                logger.info("Cloning bundle repository %s branch %s into %s", self.fbc_repo, build_branch, repo_dir)
                build_repo = BuildRepo(
                    url=self.fbc_repo,
                    branch=build_branch,
                    local_dir=repo_dir,
                    logger=logger,
                )
                await build_repo.ensure_source(strict=True)
                logger.info("FBC repository cloned to %s", repo_dir)

            # Parse catalog.Dockerfile
            dfp = DockerfileParser(str(repo_dir.joinpath("catalog.Dockerfile")))
            version = dfp.envs.get("__doozer_version")
            release = dfp.envs.get("__doozer_release")
            if not version or not release:
                raise ValueError("Version and release not found in the catalog.Dockerfile. Did you rebase?")
            logger.info("Version: %s, Release: %s", version, release)
            bundle_nvrs = dfp.envs.get("__doozer_bundle_nvrs")
            if bundle_nvrs:
                record["bundle_nvrs"] = bundle_nvrs
                # Extract git commits from bundle NVRs
                # Example: "cluster-nfd-operator-metadata-container-v4.12.0.202509242028.p2.gd5498aa.assembly.stream.el8-1" -> ["gd5498aa"]
                self.source_git_commits = self._extract_git_commits_from_nvrs(bundle_nvrs)
            else:
                self.source_git_commits = []

            # Start FBC build
            logger.info("Starting FBC build...")
            retries = 3
            name = dfp.labels.get('com.redhat.art.name')
            if not name:
                raise ValueError("FBC name not found in the catalog.Dockerfile. Did you rebase?")
            record["name"] = name
            nvr = dfp.labels.get('com.redhat.art.nvr')
            if not nvr:
                raise ValueError("FBC NVR not found in the catalog.Dockerfile. Did you rebase?")
            record["fbc_nvr"] = nvr
            output_image = f"{self.image_repo}:{nvr}"

            # If major_minor_override is present, get arches from the override group's konflux.arches config
            if self.major_minor_override:
                major, minor = self.major_minor_override
                override_group = f"openshift-{major}.{minor}"
                logger.info(
                    "Using major_minor_override %s.%s, getting arches from group %s", major, minor, override_group
                )

                # Construct doozer command to read group config for konflux.arches
                doozer_cmd = ['doozer', f'--group={override_group}', 'config:read-group', 'konflux.arches', '--yaml']

                try:
                    # Execute the command asynchronously
                    _, out, _ = await exectools.cmd_gather_async(doozer_cmd)
                    if out.strip():
                        # Parse the YAML output to get the arches list
                        arches = yaml.load(out.strip())
                        logger.info("Retrieved arches from group %s: %s", override_group, arches)
                    else:
                        logger.warning(
                            "No konflux.arches found for group %s, falling back to metadata arches", override_group
                        )
                        arches = metadata.get_arches()
                except Exception as e:
                    logger.warning(
                        "Failed to get arches from group %s config: %s, falling back to metadata arches",
                        override_group,
                        e,
                    )
                    arches = metadata.get_arches()
            else:
                arches = metadata.get_arches()

            for attempt in range(1, retries + 1):
                logger.info("Build attempt %d/%d", attempt, retries)
                pipelinerun_info, url = await self._start_build(
                    metadata=metadata,
                    build_repo=build_repo,
                    output_image=output_image,
                    arches=arches,
                    logger=logger,
                    operator_nvr=operator_nvr,
                )
                pipelinerun_name = pipelinerun_info.name
                record["task_id"] = pipelinerun_name
                record["task_url"] = url
                if not self.dry_run:
                    await self._update_konflux_db(
                        metadata, build_repo, pipelinerun_info, KonfluxBuildOutcome.PENDING, arches, logger=logger
                    )
                else:
                    logger.info("Dry run: Would have inserted build record in Konflux DB")

                logger.info("Waiting for PipelineRun %s to complete...", pipelinerun_name)
                pipelinerun_info = await self._konflux_client.wait_for_pipelinerun(
                    pipelinerun_name, namespace=self.konflux_namespace
                )
                logger.info("PipelineRun %s completed", pipelinerun_name)

                pipelinerun_dict = pipelinerun_info.to_dict()
                succeeded_condition = pipelinerun_info.find_condition('Succeeded')
                outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)

                if self.dry_run:
                    logger.info("Dry run: Would have inserted build record in Konflux DB")
                else:
                    await self._update_konflux_db(
                        metadata, build_repo, pipelinerun_info, outcome, arches, logger=logger
                    )

                if outcome is not KonfluxBuildOutcome.SUCCESS:
                    error = KonfluxFbcBuildError(
                        f"Konflux image build for {metadata.distgit_key} failed", pipelinerun_name, pipelinerun_dict
                    )
                else:
                    error = None
                    metadata.build_status = True
                    record["message"] = "Success"
                    record["status"] = 0

                    # Sync FBC related images to art-images-share
                    if self.assembly == "stream":
                        if not self.dry_run:
                            try:
                                results = pipelinerun_dict.get('status', {}).get('results', [])
                                image_pullspec = next((r['value'] for r in results if r['name'] == 'IMAGE_URL'), None)
                                image_digest = next((r['value'] for r in results if r['name'] == 'IMAGE_DIGEST'), None)

                                if image_pullspec and image_digest:
                                    fbc_pullspec = f"{image_pullspec.split(':')[0]}@{image_digest}"
                                    await self._sync_fbc_related_images_to_share(
                                        fbc_pullspec, self.product, logger=logger
                                    )
                                else:
                                    logger.warning(
                                        "Could not extract FBC pullspec from pipelinerun results, "
                                        "skipping related images sync"
                                    )
                            except Exception as e:
                                logger.warning(f"Failed to sync FBC related images: {e}")
                        else:
                            logger.info("Dry run: Would have synced FBC related images to art-images-share")
                    else:
                        logger.info(f"Skipping FBC related images sync for non-stream assembly '{self.assembly}'")

                    break
            if error:
                record['message'] = str(error)
                raise error
        finally:
            if self._record_logger:
                self._record_logger.add_record("build_fbc_konflux", **record)
        return pipelinerun_name, pipelinerun_dict

    async def _start_build(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        output_image: str,
        arches: Sequence[str],
        logger: logging.Logger,
        operator_nvr: Optional[str] = None,
    ) -> Tuple[PipelineRunInfo, str]:
        """Start a build with Konflux."""
        if not build_repo.commit_hash:
            raise IOError("Bundle repository must have a commit to build. Did you rebase?")
        # Ensure the Application resource exists
        app_name = self.get_application_name(self.group)
        logger.info(f"Using Konflux application: {app_name}")
        konflux_client = self._konflux_client
        await konflux_client.ensure_application(name=app_name, display_name=app_name)
        logger.info(f"Konflux application {app_name} created")
        # Ensure the Component resource exists
        component_name = self.get_component_name(self.group, metadata.distgit_key)
        logger.info(f"Creating Konflux component: {component_name}")
        dest_image_repo = output_image.split(":")[0]
        await konflux_client.ensure_component(
            name=component_name,
            application=app_name,
            component_name=component_name,
            image_repo=dest_image_repo,
            source_url=build_repo.https_url,
            revision=build_repo.branch,
        )
        logger.info(f"Konflux component {component_name} created")
        # Create a new pipeline run
        logger.info("Starting Konflux pipeline run...")

        additional_tags = []
        group_name = metadata.runtime.group
        if metadata.runtime.assembly == "stream":
            delivery_repo_names = metadata.config.delivery.delivery_repo_names
            for delivery_repo in delivery_repo_names:
                delivery_repo_name = delivery_repo.split('/')[-1]
                if group_name.startswith("openshift-"):
                    product_name = "ocp"
                    version = group_name.removeprefix("openshift-")
                    additional_tags.append(f"{product_name}__{version}__{delivery_repo_name}")
                    # Add operator NVR tag for openshift- groups
                    if operator_nvr:
                        additional_tags.append(f"operator_nvr__{operator_nvr}")
                else:
                    # eg: oadp-1.5 / mta-1.2
                    if self.major_minor_override:
                        tag_with_version = (
                            f"{self.group}__v{'.'.join(map(str, self.major_minor_override))}__{delivery_repo_name}"
                        )
                        additional_tags.append(f"{tag_with_version}")
                        for commit in self.source_git_commits:
                            additional_tags.append(f"{tag_with_version}__{commit}")
                        # Add operator NVR tag for non-openshift groups
                        if operator_nvr:
                            version_tag = f"v{'.'.join(map(str, self.major_minor_override))}"
                            additional_tags.append(f"{version_tag}__operator_nvr__{operator_nvr}")

        if additional_tags:
            logger.info(
                f"Additional tags being added: {additional_tags}",
            )
        else:
            logger.info("No additional tags to be added")

        pipelinerun_info = await konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{component_name}-",
            namespace=self.konflux_namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=build_repo.https_url,
            commit_sha=build_repo.commit_hash,
            target_branch=build_repo.branch or build_repo.commit_hash,
            output_image=output_image,
            vm_override={},
            building_arches=arches,  # FBC should be built for all supported arches
            additional_tags=list(additional_tags),
            skip_checks=self.skip_checks,
            hermetic=True,
            dockerfile="catalog.Dockerfile",
            pipelinerun_template_url=self.pipelinerun_template_url,
            build_priority=FBC_BUILD_PRIORITY,
        )
        url = konflux_client.resource_url(pipelinerun_info.to_dict())
        logger.info(f"PipelineRun {pipelinerun_info.name} created: {url}")
        return pipelinerun_info, url

    async def _update_konflux_db(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        pipelinerun_info: PipelineRunInfo,
        outcome: KonfluxBuildOutcome,
        arches: Sequence[str],
        logger: Optional[logging.Logger] = None,
    ):
        logger = logger or self._logger.getChild(f"[{metadata.distgit_key}]")
        db = self._db
        if not db or db.record_cls != KonfluxFbcBuildRecord:
            logger.warning('Konflux DB connection is not initialized, not writing build record to the Konflux DB.')
            return
        try:
            rebase_repo_url = build_repo.https_url
            rebase_commit = build_repo.commit_hash

            df_path = build_repo.local_dir.joinpath("catalog.Dockerfile")
            dfp = DockerfileParser(str(df_path))

            name = dfp.labels.get('com.redhat.art.name')
            version = dfp.envs.get("__doozer_version")
            release = dfp.envs.get("__doozer_release")
            nvr = dfp.labels.get('com.redhat.art.nvr')
            assert name and version and release and nvr, (
                "Name, version, release, or NVR not found in the catalog.Dockerfile. Did you rebase?"
            )

            bundle_nvrs = dfp.envs.get("__doozer_bundle_nvrs", "").split(",")
            source_repo = dfp.labels.get('io.openshift.build.source-location')
            commitish = dfp.labels.get('io.openshift.build.commit.id')

            pipelinerun_name = pipelinerun_info.name
            pipelinerun_dict = pipelinerun_info.to_dict()
            build_pipeline_url = KonfluxClient.resource_url(pipelinerun_dict)
            build_component = pipelinerun_dict['metadata']['labels'].get('appstudio.openshift.io/component')

            build_record_params = {
                'name': name,
                'version': version,
                'release': release,
                'start_time': datetime.now(tz=timezone.utc),
                'end_time': None,
                'nvr': nvr,
                'group': metadata.runtime.group,
                'assembly': metadata.runtime.assembly,
                'source_repo': source_repo or "n/a",
                'commitish': commitish or "n/a",
                'rebase_repo_url': rebase_repo_url,
                'rebase_commitish': rebase_commit,
                'engine': Engine.KONFLUX,
                'outcome': str(outcome),
                'art_job_url': os.getenv('BUILD_URL', 'n/a'),
                'build_id': pipelinerun_name,
                'build_pipeline_url': build_pipeline_url,
                'pipeline_commit': 'n/a',  # TODO: populate this
                'bundle_nvrs': bundle_nvrs,
                'arches': arches,
                'build_component': build_component,
            }

            match outcome:
                case KonfluxBuildOutcome.SUCCESS:
                    # results:
                    # - name: IMAGE_URL
                    #   value: quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:ose-network-metrics-daemon-rhel9-v4.18.0-20241001.151532
                    # - name: IMAGE_DIGEST
                    #   value: sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807

                    results = pipelinerun_dict.get('status', {}).get('results', [])
                    image_pullspec = next((r['value'] for r in results if r['name'] == 'IMAGE_URL'), None)
                    image_digest = next((r['value'] for r in results if r['name'] == 'IMAGE_DIGEST'), None)

                    if not (image_pullspec and image_digest):
                        raise ValueError(
                            f"[{metadata.distgit_key}] Could not find expected results in konflux "
                            f"pipelinerun {pipelinerun_name}"
                        )

                    status = pipelinerun_dict.get('status', {})
                    start_time = status.get('startTime')
                    end_time = status.get('completionTime')

                    build_record_params.update(
                        {
                            'image_pullspec': f"{image_pullspec.split(':')[0]}@{image_digest}",
                            'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                                tzinfo=timezone.utc
                            ),
                            'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
                            'image_tag': image_pullspec.split(':')[-1],
                        }
                    )
                case KonfluxBuildOutcome.FAILURE:
                    status = pipelinerun_dict.get('status', {})
                    start_time = status.get('startTime')
                    end_time = status.get('completionTime')
                    build_record_params.update(
                        {
                            'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                                tzinfo=timezone.utc
                            ),
                            'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
                        }
                    )

            build_record = KonfluxFbcBuildRecord(**build_record_params)
            db.add_build(build_record)
            logger.info('Konflux build %s info stored successfully with status %s', build_record.nvr, outcome)

        except Exception:
            logger.exception('Failed writing record to the konflux DB')
            raise
