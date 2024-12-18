
import logging

from os import PathLike
import os
from pathlib import Path
from typing import Dict, List, Sequence, cast

from dockerfile_parse import DockerfileParser

from doozerlib import constants, opm, util
from doozerlib.backend.build_repo import BuildRepo
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome, KonfluxBuildRecord, KonfluxBundleBuildRecord)

from ruamel.yaml import YAML

from doozerlib.image import ImageMetadata

LOGGER = logging.getLogger(__name__)
yaml = YAML(typ='rt')
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.explicit_start = True
yaml.width = 1024 * 1024
FBC_REPO_URL = "git@github.com:vfreex/art-fbc.git"


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
            upcycle: bool,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.group = group
        self.assembly = assembly
        self.version = version
        self.release = release
        self.commit_message = commit_message
        self.push = push
        self.upcycle = upcycle
        self._logger = LOGGER.getChild(self.__class__.__name__)

    async def rebase(self, metadata: ImageMetadata, bundle_build: KonfluxBundleBuildRecord, version: str, release: str):
        bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{bundle_short_name}]")
        repo_dir = self.base_dir.joinpath("konflux-fbc-sources", bundle_short_name)

        # Clone the FBC repo
        fbc_build_branch = "art-{group}-assembly-{assembly_name}-fbc-{distgit_key}".format_map({
            "group": self.group,
            "assembly_name": self.assembly,
            "distgit_key": metadata.distgit_key,
        })
        logger.info("Cloning FBC repo %s branch %s into %s", FBC_REPO_URL, fbc_build_branch, repo_dir)
        build_repo = BuildRepo(url=FBC_REPO_URL, branch=fbc_build_branch, local_dir=repo_dir, logger=logger)
        await build_repo.ensure_source(upcycle=self.upcycle, strict=True)  # FIXME: Currently rebasing against an empty FBC repo is not supported

        # Update the FBC repo
        await self._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)

        # Commit and push the changes
        addtional_message = f"\n---\noperator: {bundle_build.operator_nvr}\noperands: {','.join(bundle_build.operand_nvrs)}"
        await build_repo.commit(self.commit_message + addtional_message, allow_empty=True)
        # await build_repo.tag(f"{version}-{release}")
        if self.push:
            await build_repo.push()

        logger.info("rebase complete")

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
        channel_name = labels.get("operators.operatorframework.io.bundle.channels.v1")
        if not channel_name:
            raise IOError("Channel name not found in bundle image")
        default_channel_name = labels.get("operators.operatorframework.io.bundle.channel.default.v1")
        # package_name = labels.get("operators.operatorframework.io.bundle.package.v1")
        olm_bundle_name, olm_package, olm_bundle_blob = await self._fetch_olm_bundle_blob(bundle_build)
        olm_csv_metadata = next((entry for entry in olm_bundle_blob["properties"] if entry["type"] == "olm.csv.metadata"), None)
        if not olm_csv_metadata:
            raise IOError(f"CSV metadata not found in bundle {olm_bundle_name}")
        olm_skip_range = olm_csv_metadata["value"]["annotations"].get("olm.skipRange", None)

        # Load current catalog
        catalog_file_path = build_repo.local_dir.joinpath("catalog", olm_package, "catalog.yaml")
        logger.info("Loading catalog from %s", catalog_file_path)
        if not catalog_file_path.exists():
            raise FileNotFoundError(f"Catalog file {catalog_file_path} not found. The FBC repo is not properly initialized.")
        with catalog_file_path.open() as f:
            catalog_blobs = list(yaml.load_all(f))
        categorized_catalog_blobs = self.catagorize_catalog_blobs(catalog_blobs)
        if olm_package not in categorized_catalog_blobs:
            raise IOError(f"Package {olm_package} not found in catalog. The FBC repo is not properly initialized.")
        if len(categorized_catalog_blobs) > 1:
            logger.warning(f"The catalog file {catalog_file_path} has multiple packages: {','.join(categorized_catalog_blobs.keys())}")

        # Update the catalog
        # Add the new bundle to the channel in the catalog
        channel = categorized_catalog_blobs[olm_package]["olm.channel"].get(channel_name, None)
        if not channel:
            raise IOError(f"Channel {channel_name} not found in package {olm_package}. The FBC repo is not properly initialized.")

        # Update "skips" in the channel
        # FIXME: We try to mimic how `skips` field is updated by the old ET centric process.
        # We should verify if this is correct.
        skips = None
        bundle_with_skips = next((it for it in channel['entries'] if it.get('skips')), None)  # Find which bundle has the skips field
        if bundle_with_skips:
            # Then we move the skips field to the new bundle
            # and add the bundle name of bundle_with_skips to the skips field
            skips = set(bundle_with_skips['skips'])
            del bundle_with_skips['skips']
            skips = (skips | {bundle_with_skips['name']}) - {olm_bundle_name}

        idx, entry = next(((i, entry) for i, entry in enumerate(channel['entries']) if entry['name'] == olm_bundle_name), (-1, None))
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
            # idx = next((i for i, blob in enumerate(catalog_blobs) if blob["schema"] == "olm.bundle" and blob["name"] == olm_bundle_name), -1)
            # catalog_blobs[idx] = olm_bundle_blob
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
        }
        for key, value in metadata_envs.items():
            if dfp.envs.get(key) != value:
                logger.info("Setting %s=%s", key, value)
                dfp.envs[key] = value

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
        if len(rendered_blobs) != 1:
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

    def catagorize_catalog_blobs(self, blobs: List[Dict]):
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

    # async def update_fbc_template_for_package(self, repo_dir: Path, package_name: str, channel: str):
    #     template_file = repo_dir.joinpath("catalog-templates", f"{package_name}.yaml")
    #     if not template_file.exists():
    #         # TODO: Add a new template for the package if it doesn't exist
    #         raise FileNotFoundError(f"Template file {template_file} not found")
    #     # Update the template file
    #     with template_file.open() as template_file:
    #         template = yaml.load(template_file)
    #     if template['schema'] != 'olm.template.basic':
    #         raise IOError(f"Invalid schema: {template['schema']}")
    #     # Find entry with schema == "olm.package"
    #     package_entry = next((entry for entry in template if entry.get("schema") == "olm.package"))
    #     if package_name != package_entry['name']:
    #         raise ValueError(f"Package name mismatch: {package_name} != {package_entry['name']}")
    #     # Find entry with schema == "olm.channel" and channel = channel
    #     channel_entry = next((entry for entry in template if entry.get("schema") == "olm.channel" and entry["name"] == channel))
