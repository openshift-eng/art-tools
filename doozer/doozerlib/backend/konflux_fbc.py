
import asyncio
import logging
import os
import shutil
from os import PathLike
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from artcommonlib.konflux.konflux_build_record import KonfluxBundleBuildRecord
from async_lru import alru_cache
from dockerfile_parse import DockerfileParser
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import constants, opm, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger

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
        self._record_logger = record_logger
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)

    async def rebase(self, metadata: ImageMetadata, bundle_build: KonfluxBundleBuildRecord, version: str, release: str):
        bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{bundle_short_name}]")
        repo_dir = self.base_dir.joinpath(metadata.distgit_key)

        name = metadata.distgit_key + "-fbc"
        nvr = f"{name}-{version}-{release}"
        record = {
            'status': -1,  # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            "name": metadata.distgit_key,
            "message": "Unknown failure",
            "fbc_nvr": nvr,
            "bundle_nvrs": ','.join([str(bundle_build.nvr)]),  # Currently we only support rebasing for one bundle at a time
        }

        try:
            # Clone the FBC repo
            fbc_build_branch = "art-{group}-assembly-{assembly_name}-fbc-{distgit_key}".format_map({
                "group": self.group,
                "assembly_name": self.assembly,
                "distgit_key": metadata.distgit_key,
            })
            logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_repo, fbc_build_branch, repo_dir)
            build_repo = BuildRepo(url=self.fbc_repo, branch=fbc_build_branch, local_dir=repo_dir, logger=logger)
            await build_repo.ensure_source(upcycle=self.upcycle, strict=True)  # FIXME: Currently rebasing against an empty FBC repo is not supported

            # Update the FBC repo
            await self._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)

            # Validate the updated catalog
            logger.info("Validating the updated catalog")
            await opm.validate(build_repo.local_dir.joinpath("catalog"))

            # Commit and push the changes
            addtional_message = f"\n\n---\noperator: {bundle_build.operator_nvr}\noperands: {','.join(bundle_build.operand_nvrs)}"
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

    async def _rebase_dir(self, metadata: ImageMetadata, build_repo: BuildRepo, bundle_build: KonfluxBundleBuildRecord,
                          version: str, release: str, logger: logging.Logger):
        logger.info("Rebasing dir %s", build_repo.local_dir)

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
        olm_bundle_name, olm_package, olm_bundle_blob = await self._fetch_olm_bundle_blob(bundle_build)
        if olm_package != labels.get("operators.operatorframework.io.bundle.package.v1"):
            raise IOError(f"Package name mismatch: {olm_package} != {labels.get('operators.operatorframework.io.bundle.package.v1')}")
        olm_csv_metadata = next((entry for entry in olm_bundle_blob["properties"] if entry["type"] == "olm.csv.metadata"), None)
        if not olm_csv_metadata:
            raise IOError(f"CSV metadata not found in bundle {olm_bundle_name}")
        olm_skip_range = olm_csv_metadata["value"]["annotations"].get("olm.skipRange", None)

        # Load current catalog
        catalog_file_path = build_repo.local_dir.joinpath("catalog", olm_package, "catalog.yaml")
        logger.info("Loading catalog from %s", catalog_file_path)
        if not catalog_file_path.is_file():
            raise FileNotFoundError(f"Catalog file {catalog_file_path} not found. The FBC repo is not properly initialized.")
        with catalog_file_path.open() as f:
            catalog_blobs = list(yaml.load_all(f))
        categorized_catalog_blobs = self._catagorize_catalog_blobs(catalog_blobs)
        if olm_package not in categorized_catalog_blobs:
            raise IOError(f"Package {olm_package} not found in catalog. The FBC repo is not properly initialized.")
        if len(categorized_catalog_blobs) > 1:
            logger.warning(f"The catalog file {catalog_file_path} has multiple packages: {','.join(categorized_catalog_blobs.keys())}")

        # Update the catalog
        def _update_channel(channel: Dict):
            # Update "skips" in the channel
            # FIXME: We try to mimic how `skips` field is updated by the old ET centric process.
            # We should verify if this is correct.
            skips = None
            bundle_with_skips = next((it for it in channel['entries'] if it.get('skips')), None)  # Find which bundle has the skips field
            if bundle_with_skips:
                # Then we move the skips field to the new bundle
                # and add the bundle name of bundle_with_skips to the skips field
                skips = set(bundle_with_skips.pop('skips'))
                skips = (skips | {bundle_with_skips['name']}) - {olm_bundle_name}

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

        for channel_name in channel_names:
            logger.info("Updating channel %s", channel_name)
            channel = categorized_catalog_blobs[olm_package]["olm.channel"].get(channel_name, None)
            if not channel:
                raise IOError(f"Channel {channel_name} not found in package {olm_package}. The FBC repo is not properly initialized.")
            _update_channel(channel)

        # Set default channel
        if default_channel_name:
            package_blob = categorized_catalog_blobs[olm_package]["olm.package"][olm_package]
            if package_blob.get("defaultChannel") != default_channel_name:
                logger.info("Setting default channel to %s", default_channel_name)
                package_blob["defaultChannel"] = default_channel_name

        # Replace image references in relatedImages to use the prod registry
        related_images = olm_bundle_blob.get("relatedImages", [])
        if related_images:
            target_entry = next((it for it in related_images if it['name'] == ""), None)
            if target_entry:
                new_image_name = image_name.replace('openshift/', 'openshift4/')
                new_pullspec = f"{constants.DELIVERY_IMAGE_REGISTRY}/{new_image_name}@sha256:{bundle_build.image_tag}"
                logger.info("Replacing image reference %s with %s", target_entry['image'], new_pullspec)
                target_entry['image'] = new_pullspec

        # Add the new bundle blob to the catalog
        if olm_bundle_name not in categorized_catalog_blobs[olm_package]["olm.bundle"]:
            logger.info("Adding bundle %s to package %s", olm_bundle_name, olm_package)
            catalog_blobs.append(olm_bundle_blob)
        else:  # Update the existing bundle blob
            logger.warning("Bundle %s already exists in package %s. Replacing...", olm_bundle_name, olm_package)
            target_entry = categorized_catalog_blobs[olm_package]["olm.bundle"][olm_bundle_name]
            target_entry.clear()
            target_entry.update(olm_bundle_blob)

        # Write the updated catalog back to the file
        logger.info("Writing updated catalog to %s", catalog_file_path)
        with catalog_file_path.open("w") as f:
            yaml.dump_all(catalog_blobs, f)

        # Update Dockerfile
        dockerfile_path = build_repo.local_dir.joinpath("catalog.Dockerfile")
        logger.info("Updating Dockerfile %s", dockerfile_path)
        if not dockerfile_path.is_file():
            raise FileNotFoundError(f"Dockerfile {dockerfile_path} not found")
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

    async def _fetch_olm_bundle_image_info(self, bundle_build: KonfluxBundleBuildRecord):
        return await util.oc_image_info_for_arch_async__caching(
            bundle_build.image_pullspec,
            registry_config=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
            registry_username=os.environ.get("KONFLUX_ART_IMAGES_USERNAME"),
            registry_password=os.environ.get("KONFLUX_ART_IMAGES_PASSWORD"),
        )

    async def _fetch_olm_bundle_blob(self, bundle_build: KonfluxBundleBuildRecord):
        """ Fetch the olm.bundle blob for the given bundle build.

        :param bundle_build: The bundle build record.
        :return: A tuple of (bundle name, package name, bundle blob).
        """
        registry_auth = opm.OpmRegistryAuth(
            path=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
            username=os.environ.get("KONFLUX_ART_IMAGES_USERNAME"),
            password=os.environ.get("KONFLUX_ART_IMAGES_PASSWORD"))
        rendered_blobs = await opm.render(bundle_build.image_pullspec, migrate=True, auth=registry_auth)
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
        """ Given a list of catalog blobs, categorize them by schema and package name.

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
