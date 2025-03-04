
import asyncio
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from async_lru import alru_cache
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import constants, opm
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.image import ImageMetadata

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml

PRODUCTION_INDEX_PULLSPEC_FORMAT = "registry.redhat.io/redhat/redhat-operator-index:v{major}.{minor}"
BASE_IMAGE_RHEL9_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v{major}.{minor}"
BASE_IMAGE_RHEL8_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry:v{major}.{minor}"


class KonfluxFbcImporter:
    def __init__(self,
                 base_dir: Path,
                 group: str,
                 assembly: str,
                 ocp_version: Tuple[int, int],
                 keep_templates: bool,
                 upcycle: bool,
                 push: bool,
                 commit_message: Optional[str] = None,
                 fbc_repo: str = constants.ART_FBC_GIT_REPO,
                 logger: logging.Logger | None = None
                 ):
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self.ocp_version = ocp_version
        self.keep_templates = keep_templates
        self.upcycle = upcycle
        self.push = push
        self.commit_message = commit_message
        self.fbc_git_repo = fbc_repo
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)

    async def import_from_index_image(self, metadata: ImageMetadata, index_image: str | None = None):
        """ Create a file based catalog (FBC) by importing from an existing index image.

        :param metadata: The metadata of the operator image.
        :param index_image: The index image to import from. If not provided, a default index image is used.
        """
        # bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        repo_dir = self.base_dir.joinpath(metadata.distgit_key)
        if not index_image:
            index_image = PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=self.ocp_version[0], minor=self.ocp_version[1])
            logger.info("Using default index image %s for OCP %s.%s", index_image, self.ocp_version[0], self.ocp_version[1])

        # Clone the FBC repo
        build_branch = "art-{group}-assembly-{assembly_name}-fbc-{distgit_key}".format_map({
            "group": self.group,
            "assembly_name": self.assembly,
            "distgit_key": metadata.distgit_key,
        })
        logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_git_repo, build_branch, repo_dir)
        build_repo = BuildRepo(url=self.fbc_git_repo, branch=build_branch, local_dir=repo_dir, logger=logger)
        await build_repo.ensure_source(upcycle=self.upcycle, strict=False)

        # Update the FBC directory
        await self._update_dir(metadata, build_repo, index_image, logger)

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

    async def _update_dir(self, metadata: ImageMetadata, build_repo: BuildRepo, index_image: str, logger: logging.Logger):
        """ Update the FBC directory with the given operator image metadata and index image."""
        repo_dir = build_repo.local_dir
        # Get package name of the operator
        package_name = await self._get_package_name(metadata)
        # Render the catalog from the index image
        org_catalog_blobs = await self._get_catalog_blobs_from_index_image(index_image, package_name)

        # Write catalog_blobs to catalog-migrate/<package>/catalog.json
        org_catalog_dir = repo_dir.joinpath("catalog-migrate", package_name)
        org_catalog_dir.mkdir(parents=True, exist_ok=True)
        org_catalog_file_path = org_catalog_dir.joinpath("catalog.yaml")
        logger.info("Writing original catalog blobs to %s", org_catalog_file_path)
        with org_catalog_file_path.open('w') as f:
            yaml.dump_all(org_catalog_blobs, f)

        # Generate basic fbc template to catalog-templates/<package>.yaml
        templates_dir = repo_dir.joinpath("catalog-templates")
        templates_dir.mkdir(parents=True, exist_ok=True)
        template_file = templates_dir.joinpath(f"{package_name}.yaml")
        await opm.generate_basic_template(org_catalog_file_path, template_file)

        logger.info("Cleaning up catalog-migrate")
        shutil.rmtree(repo_dir.joinpath("catalog-migrate"))

        # Render final FBC catalog at catalog/<package>/catalog.yaml from template
        catalog_dir = repo_dir.joinpath("catalog", package_name)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        catalog_file = catalog_dir.joinpath("catalog.yaml")
        logger.info("Rendering catalog from template %s to %s", template_file, catalog_file)
        migrate_level = "none"
        if self.ocp_version >= (4, 17):
            migrate_level = "bundle-object-to-csv-metadata"
        await opm.render_catalog_from_template(template_file, catalog_file, migrate_level=migrate_level)

        # Clean up catalog-templates
        if not self.keep_templates:
            logger.info("Cleaning up catalog-templates")
            shutil.rmtree(templates_dir)

        # Generate Dockerfile
        df_path = repo_dir.joinpath("catalog.Dockerfile")
        logger.info("Generating Dockerfile %s", df_path)
        if df_path.exists():
            logger.info("Removing existing Dockerfile %s", df_path)
            df_path.unlink()
        base_image_format = BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if self.ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        base_image = base_image_format.format(major=self.ocp_version[0], minor=self.ocp_version[1])
        await opm.generate_dockerfile(repo_dir, "catalog", base_image=base_image, builder_image=base_image)

        logger.info("FBC directory updated")

    @alru_cache
    async def _render_index_image(self, index_image: str):
        blobs = await retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))(opm.render)(index_image)
        return blobs

    def _filter_catalog_blobs(self, blobs: List[Dict], allowed_package_names: Set[str]):
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

    async def _get_catalog_blobs_from_index_image(self, index_image: str, package_name):
        blobs = await self._render_index_image(index_image)
        filtered_blobs = self._filter_catalog_blobs(blobs, {package_name})
        if package_name not in filtered_blobs:
            raise IOError(f"Package {package_name} not found in index image")
        return filtered_blobs[package_name]

    async def _get_package_name(self, metadata: ImageMetadata) -> str:
        """ Get OLM package name of the given OLM operator
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
