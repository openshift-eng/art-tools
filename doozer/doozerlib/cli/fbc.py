import asyncio
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, cast

import click

from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord, KonfluxBundleBuildRecord, ArtifactType, Engine, KonfluxBuildOutcome
from doozerlib import constants, opm
from doozerlib.backend.konflux_fbc import KonfluxFbcRebaser
from doozerlib.cli import (cli, click_coroutine, option_commit_message,
                           option_push, pass_runtime,
                           validate_semver_major_minor_patch)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolver

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml

PRODUCTION_INDEX_PULLSPEC_FORMAT = "registry.redhat.io/redhat/redhat-operator-index:v{major}.{minor}"
BASE_IMAGE_RHEL9_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v{major}.{minor}"
BASE_IMAGE_RHEL8_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry:v{major}.{minor}"


class FbcImportCli:
    def __init__(self, runtime: Runtime, index_image: str | None, dest_dir: str):
        self.runtime = runtime
        self.index_image = index_image
        self.dest_dir = dest_dir

    async def run(self):
        """ Run the FBC import process
        This function implements the main logic of the FBC import process
        following https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/docs/konflux-onboarding.md#create-the-fbc-in-the-git-repository.
        """
        # Ensure opm is installed
        try:
            await opm.verify_opm()
        except (IOError, FileNotFoundError):
            LOGGER.error("Please install the latest opm cli binary from https://github.com/operator-framework/operator-registry/releases")
            raise

        # Initialize runtime
        runtime = self.runtime
        runtime.initialize(mode="images", clone_distgits=False)
        if "MAJOR" not in runtime.group_config.vars or "MINOR" not in runtime.group_config.vars:
            raise ValueError("MAJOR and MINOR vars must be set in group config")
        major = runtime.group_config.vars.MAJOR
        minor = runtime.group_config.vars.MINOR
        source_resolver = runtime.source_resolver
        assert source_resolver, "source_resolver is not set; Doozer bug?"
        if not self.index_image:
            self.index_image = PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=major, minor=minor)

        # Load package.yaml files and operator index image
        base_dir = Path(self.dest_dir)
        base_dir.mkdir(parents=True, exist_ok=True)
        operator_metas: List[ImageMetadata] = [operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator]

        LOGGER.info("Loading package.yaml files and remote operator index image...")
        package_names, org_catalog_blobs = await asyncio.gather(
            self.load_package_names(operator_metas, source_resolver),
            opm.render(self.index_image),
        )

        LOGGER.info("Filtering catalog blobs...")
        org_catalog_blobs = self.filter_catalog_blobs(org_catalog_blobs, set(package_names))

        # Write catalog_blobs to catalog-migrate/<package>/catalog.json
        LOGGER.info("Writing original catalog blobs to catalog-migrate")
        org_catalog_dir = base_dir.joinpath("catalog-migrate")
        org_catalog_dir.mkdir(parents=True, exist_ok=True)
        org_catalog_files: Dict[str, Path] = {}  # key is package name, value is catalog file path
        for package_name, blobs in org_catalog_blobs.items():
            org_catalog_subdir = org_catalog_dir.joinpath(package_name)
            org_catalog_subdir.mkdir(parents=True, exist_ok=True)
            org_catalog_file = org_catalog_subdir.joinpath("catalog.yaml")
            with org_catalog_file.open('w') as f:
                yaml.dump_all(blobs, f)
                # for blob in blobs:
                #     json.dump(blob, f, indent=2)
            org_catalog_files[package_name] = org_catalog_file

        # Generate basic fbc templates to catalog-templates/<package>.yaml
        LOGGER.info("Generating basic FBC templates")
        templates_dir = base_dir.joinpath("catalog-templates")
        templates_dir.mkdir(parents=True, exist_ok=True)
        template_files: Dict[str, Path] = {}  # key is package name, value is template file path
        tasks = []
        for package_name, org_catalog_file in org_catalog_files.items():
            template_file = templates_dir.joinpath(f"{package_name}.yaml")
            tasks.append(asyncio.create_task(opm.generate_basic_template(org_catalog_file, template_file)))
            template_files[package_name] = template_file
        await asyncio.gather(*tasks)

        LOGGER.info("Cleaning up catalog-migrate")
        shutil.rmtree(org_catalog_dir)

        # Render final FBC catalogs from templates at catalog/<package>/catalog.yaml
        LOGGER.info("Rendering final FBC catalogs")
        migrate_level = "none"
        if (major, minor) >= (4, 17):
            migrate_level = "bundle-object-to-csv-metadata"
        catalog_dir = base_dir.joinpath("catalog")
        catalog_dir.mkdir(parents=True, exist_ok=True)
        tasks = []
        for package_name, template_file in template_files.items():
            catalog_subdir = catalog_dir.joinpath(package_name)
            catalog_subdir.mkdir(parents=True, exist_ok=True)
            catalog_file = catalog_subdir.joinpath("catalog.yaml")
            LOGGER.info("Rendering catalog %s", catalog_file)
            tasks.append(asyncio.create_task(opm.render_catalog_from_template(template_file, catalog_file, migrate_level=migrate_level)))
        await asyncio.gather(*tasks)

        # Generate Dockerfile
        LOGGER.info("Generating Dockerfile")
        base_image_format = BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if (major, minor) > (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        base_image = base_image_format.format(major=major, minor=minor)
        await opm.generate_dockerfile(base_dir, "catalog", base_image=base_image, builder_image=base_image)

    async def load_package_names(self, operator_metas: List[ImageMetadata], source_resolver: SourceResolver) -> List[str]:
        """ Load OLM package names for operator images.
        This function loads OLM package names for operator images by resolving sources and loading package.yaml files.

        :param operator_metas: List of operator image metadata.
        :param source_resolver: SourceResolver instance.
        :return: List of OLM package names.
        """
        package_names: List[str] = []
        sources = await asyncio.gather(*(asyncio.to_thread(source_resolver.resolve_source, operator_meta) for operator_meta in operator_metas))
        for operator_meta, source in zip(operator_metas, sources):
            source_dir = source_resolver.get_source_dir(source, operator_meta)
            csv_config = operator_meta.config.get('update-csv')
            if not csv_config:
                raise ValueError(f"update-csv config not found for {operator_meta.distgit_key}")
            source_path = source_dir.joinpath(csv_config['manifests-dir'])
            package_yaml_file = next(source_path.glob('**/*package.yaml'))
            with package_yaml_file.open() as f:
                package_yaml = yaml.load(f)
            package_name = str(package_yaml['packageName'])
            package_names.append(package_name)
        return package_names

    def filter_catalog_blobs(self, blobs: List[Dict], allowed_package_names: Set[str]):
        """ Filter catalog blobs by package names.

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


@cli.command("beta:fbc:import", short_help="Create FBC by importing from the provided index image")
@click.option("--from-index", metavar='INDEX_IMAGE', help="The index image to import from")
@click.argument("dest_dir", metavar='DEST_DIR', default=".")
@pass_runtime
@click_coroutine
async def fbc_import(runtime: Runtime, from_index: Optional[str], dest_dir: str):
    """
    Create an FBC repository by importing from the provided index image

    Example usage:

    doozer --group=openshift-4.17 beta:fbc:import registry.redhat.io/redhat/redhat-operator-index:v4.17 ./fbc-4.17
    """
    cli = FbcImportCli(runtime=runtime, index_image=from_index, dest_dir=dest_dir)
    await cli.run()


class FbcRebaseCli:
    def __init__(
            self,
            runtime: Runtime,
            version: str,
            release: str,
            commit_message: str,
            push: bool,
            operator_nvrs: Tuple[str, ...]):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.commit_message = commit_message
        self.push = push
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
        not_found = [dkg for dkg, bundle_build in zip(dgk_operator_builds.keys(), bundle_builds) if bundle_build is None]
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
            upcycle=False,
        )
        tasks = []
        for dkg, bundle_build in zip(dgk_operator_builds.keys(), bundle_builds):
            operator_meta = runtime.image_map[dkg]
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
        """ Get build records for the given operator nvrs or latest build records for all operators.

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
            for dkg in dgk_records.keys():
                metadata = runtime.image_map[dkg]
                if not metadata.is_olm_operator:
                    raise IOError(f"Operator {dkg} does not have 'update-csv' config")
        else:
            # Get latest build records for all specified operators
            runtime.initialize(mode='images', clone_distgits=False)
            LOGGER.info("Fetching latest operator builds from Konflux DB...")
            operator_metas: List[ImageMetadata] = [operator_meta for operator_meta in runtime.ordered_image_metas() if operator_meta.is_olm_operator]
            records = await asyncio.gather(*(metadata.get_latest_build() for metadata in operator_metas))
            not_found = [metadata.distgit_key for metadata, record in zip(operator_metas, records) if record is None]
            if not_found:
                raise IOError(f"Couldn't find build records for {not_found}")
            for metadata, record in zip(operator_metas, records):
                assert record is not None and isinstance(record, KonfluxBuildRecord)
                dgk_records[metadata.distgit_key] = record
        return dgk_records

    async def _get_bundle_builds(self, operator_builds: Sequence[KonfluxBuildRecord], strict: bool = True) -> List[Optional[KonfluxBundleBuildRecord]]:
        """ Get bundle build records for the given operator builds.

        :param operator_builds: operator builds.
        :return: A list of bundle build records.
        """
        logger = self._logger.getChild("get_bundle_builds")
        logger.info("Fetching bundle builds from Konflux DB...")
        builds = await asyncio.gather(*(self._get_bundle_build_for(operator_build, strict=strict) for operator_build in operator_builds))
        return builds

    async def _get_bundle_build_for(self, operator_build: KonfluxBuildRecord, strict: bool = True) -> Optional[KonfluxBundleBuildRecord]:
        """ Get bundle build record for the given operator build.

        :param operator_build: Operator build record.
        :return: Bundle build record.
        """
        bundle_name = f"{operator_build.name}-bundle"
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


@cli.command("beta:fbc:rebase", short_help="Refresh a group's FBC konflux source content from source content.")
@click.option("--version", metavar='VERSION', required=True, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@option_commit_message
@option_push
@click.argument('operator_nvrs', nargs=-1, required=False)
@pass_runtime
@click_coroutine
async def fbc_rebase(
    runtime: Runtime,
    version: str,
    release: str,
    message: str,
    push: bool,
    operator_nvrs: Tuple[str, ...]
):
    """
    Refresh a group's FBC konflux source content from source content.

    The group's FBC konflux source content will be updated to match the source content. The group's Dockerfiles will be
    updated to use the specified version and release. The group's Dockerfiles will be updated to use the specified image
    repo.
    """
    cli = FbcRebaseCli(runtime=runtime, version=version, release=release,
                       commit_message=message, push=push, operator_nvrs=operator_nvrs)
    await cli.run()
