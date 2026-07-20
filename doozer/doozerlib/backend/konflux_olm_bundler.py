import asyncio
import glob
import logging
import os
import re
import shutil
import string
from datetime import datetime, timezone
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple, cast

import aiofiles
import yaml
from artcommonlib import exectools
from artcommonlib import util as artlib_util
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_SHARE_REPO
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
)
from artcommonlib.konflux.konflux_db import Engine, KonfluxDb
from artcommonlib.model import Model
from artcommonlib.util import sync_to_quay
from dockerfile_parse import DockerfileParser
from doozerlib import constants, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import ImageBuildParams, KonfluxClient
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
from doozerlib.source_resolver import SourceResolution, SourceResolver

_LOGGER = logging.getLogger(__name__)


BUNDLE_BUILD_PRIORITY = "3"

# Characters that can appear inside a pullspec (registry/repo:tag or registry/repo@sha256:...)
_PULLSPEC_CHARS = frozenset(string.ascii_letters + string.digits + "/:@._-")


def _replace_pullspec(content: str, old_spec: str, new_spec: str) -> str:
    """
    Replace all occurrences of old_spec with new_spec in content, but only
    when old_spec appears as a complete pullspec token — i.e. it is not a
    substring of a longer pullspec.

    Arg(s):
        content (str): Text content (e.g. a CSV YAML file).
        old_spec (str): Upstream pullspec to find.
        new_spec (str): SHA-based pullspec to substitute.
    Return Value(s):
        str: Content with boundary-safe replacements applied.
    """
    if not isinstance(old_spec, str) or not old_spec:
        raise ValueError("old_spec must be a non-empty string")
    result = []
    start = 0
    while True:
        idx = content.find(old_spec, start)
        if idx == -1:
            result.append(content[start:])
            break
        end = idx + len(old_spec)
        before_ok = idx == 0 or content[idx - 1] not in _PULLSPEC_CHARS
        after_ok = end == len(content) or content[end] not in _PULLSPEC_CHARS
        if before_ok and after_ok:
            result.append(content[start:idx])
            result.append(new_spec)
            start = end
        else:
            result.append(content[start:end])
            start = end
    return "".join(result)


class KonfluxOlmBundleRebaseError(Exception):
    """Raised when an operator bundle cannot be safely rebased, e.g. an embargoed operand
    image has no public substitute available."""

    pass


class KonfluxOlmBundleRebaser:
    def __init__(
        self,
        base_dir: Path,
        group: str,
        assembly: str,
        group_config: Model,
        konflux_db: KonfluxDb,
        source_resolver: SourceResolver,
        upcycle: bool = False,
        image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO,
        dry_run: bool = False,
        logger: logging.Logger = _LOGGER,
        record_logger=None,
    ):
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self._group_config = group_config
        self._konflux_db = konflux_db
        self._konflux_db.bind(KonfluxBuildRecord)
        self._source_resolver = source_resolver
        self.upcycle = upcycle
        self.image_repo = image_repo
        self.dry_run = dry_run
        self._logger = logger
        self._record_logger = record_logger

    async def rebase(self, metadata: ImageMetadata, operator_build: KonfluxBuildRecord, input_release: str) -> str:
        """Rebase an operator with Konflux.

        :param metadata: The metadata of the operator to rebase.
        :param operator_build_record: The build record of the operator to rebase. If not provided, the latest build record will be used.
        :param input_release: The release string for the new bundle. None to let the build backend generate it.
        """
        assert input_release, "input_release must be provided"
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")

        # Initialize record with failure defaults
        record = {
            'status': -1,
            "message": "Rebase failed",
            "task_id": "n/a",
            "task_url": "n/a",
            "operator_nvr": operator_build.nvr,
            "operand_nvrs": "n/a",
            "bundle_nvr": "n/a",
        }

        try:
            source = None
            if metadata.has_source():
                logger.info("Resolving source...")
                source = cast(
                    SourceResolution,
                    await exectools.to_thread(self._source_resolver.resolve_source, metadata, no_clone=True),
                )
            else:
                raise IOError(
                    f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported."
                )

            logger.info("Cloning operator build source...")
            if operator_build.engine is Engine.KONFLUX:
                operator_build_repo_url = source.url
                operator_build_repo_refspec = operator_build.rebase_commitish
            elif operator_build.engine is Engine.BREW:
                operator_build_repo_url = metadata.distgit_remote_url()
                operator_build_repo_refspec = f'{operator_build.version}-{operator_build.release}'
            else:
                raise ValueError(f"Unsupported engine {operator_build.engine} for {metadata.distgit_key}")
            operator_dir = self.base_dir.joinpath(metadata.qualified_key)
            operator_build_repo = BuildRepo(
                url=operator_build_repo_url, branch=None, local_dir=operator_dir, logger=self._logger
            )
            await operator_build_repo.ensure_source(upcycle=self.upcycle)
            await operator_build_repo.fetch(operator_build_repo_refspec, strict=True)
            await operator_build_repo.switch('FETCH_HEAD', detach=True)
            logger.info(f"operator build source cloned to {operator_dir}")

            logger.info("Cloning bundle repository...")
            bundle_dir = self.base_dir.joinpath(metadata.get_olm_bundle_short_name())
            bundle_build_branch = "art-{group}-assembly-{assembly_name}-bundle-{distgit_key}".format_map(
                {
                    "group": self.group,
                    "assembly_name": self.assembly,
                    "distgit_key": metadata.distgit_key,
                }
            )
            bundle_build_repo = BuildRepo(
                url=source.url, branch=bundle_build_branch, local_dir=bundle_dir, logger=self._logger
            )
            await bundle_build_repo.ensure_source(upcycle=self.upcycle)
            logger.info("Bundle repository cloned to %s", bundle_dir)
            # clean the bundle build directory
            logger.info("Cleaning bundle build directory...")
            await bundle_build_repo.delete_all_files()
            logger.info("Rebasing bundle content...")
            nvr = await self._rebase_dir(metadata, operator_dir, bundle_dir, operator_build, input_release)

            record['bundle_nvr'] = nvr

            # commit and push the changes
            logger.info("Committing and pushing bundle content...")
            await bundle_build_repo.commit(f"Update bundle manifests for {operator_build.nvr}", allow_empty=True)
            if not self.dry_run:
                await bundle_build_repo.push()

            # Rebase succeeded - don't write record, builder will handle it
            return nvr
        except Exception as e:
            # Record rebase failure
            record['message'] = f"Rebase failed: {str(e)}"
            if self._record_logger:
                self._record_logger.add_record("build_olm_bundle_konflux", **record)
            raise

    async def _rebase_dir(
        self,
        metadata: ImageMetadata,
        operator_dir: Path,
        bundle_dir: Path,
        operator_build: KonfluxBuildRecord,
        input_release: str,
    ) -> str:
        """Rebase an operator directory with Konflux."""
        csv_config = metadata.config.get('update-csv')
        if not csv_config:
            raise ValueError(f"[{metadata.distgit_key}] No update-csv config found in the operator's metadata")
        if not csv_config.get('manifests-dir'):
            raise ValueError(f"[{metadata.distgit_key}] No manifests-dir defined in the operator's update-csv")
        if not csv_config.get('bundle-dir'):
            raise ValueError(f"[{metadata.distgit_key}] No bundle-dir defined in the operator's update-csv")
        if not csv_config.get('valid-subscription-label'):
            raise ValueError(
                f"[{metadata.distgit_key}] No valid-subscription-label defined in the operator's update-csv"
            )

        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        operator_manifests_dir = operator_dir.joinpath(csv_config['manifests-dir'])
        operator_bundle_dir = operator_manifests_dir.joinpath(csv_config['bundle-dir'])
        bundle_manifests_dir = bundle_dir.joinpath("manifests")

        if not next(operator_bundle_dir.iterdir(), None):
            raise FileNotFoundError(
                f"[{metadata.distgit_key}] No files found in bundle directory {operator_bundle_dir.relative_to(self.base_dir)}"
            )

        # Get operator package name and channel from its package YAML
        # This info will be used to generate bundle's Dockerfile labels and metadata/annotations.yaml
        file_path = glob.glob(f'{operator_manifests_dir}/*package.yaml')[0]
        async with aiofiles.open(file_path, 'r') as f:
            package_yaml = yaml.safe_load(await f.read())
        package_name = package_yaml['packageName']
        channel = package_yaml['channels'][0]
        channel_name = str(channel['name'])
        csv_name = str(channel['currentCSV'])

        # Read image references from the operator's image-references file.
        # Different operators place the file in different directories, so
        # search multiple candidates (same order as rebaser.py).
        refs_path = None
        for candidate in [operator_bundle_dir, operator_manifests_dir, operator_manifests_dir.parent]:
            if (candidate / "image-references").exists():
                refs_path = candidate / "image-references"
                break

        image_references: dict[str, dict] = {}
        if refs_path is not None:
            async with aiofiles.open(refs_path, "r") as f:
                image_refs = yaml.safe_load(await f.read())
            for entry in image_refs.get("spec", {}).get("tags", []):
                image_references[entry["name"]] = entry
        else:
            logger.warning(
                f"No image-references file found for {metadata.distgit_key}; "
                f"searched: {[str(c / 'image-references') for c in [operator_bundle_dir, operator_manifests_dir, operator_manifests_dir.parent]]}. "
                f"Falling back to legacy tag-based resolution. "
                f"This fallback will be removed in a future release."
            )

        # Validate that containerImage annotation in CSV matches an entry in image-references
        csv_files = list(operator_bundle_dir.glob("*.clusterserviceversion.yaml"))
        if csv_files:
            csv_file = csv_files[0]
            async with aiofiles.open(csv_file, "r") as f:
                csv_content = await f.read()
            csv_data = yaml.safe_load(csv_content)
            container_image = csv_data.get("metadata", {}).get("annotations", {}).get("containerImage")

            if container_image and image_references:
                specs_from_refs = {ref["from"]["name"] for ref in image_references.values()}
                if container_image not in specs_from_refs:
                    logger.warning(
                        f"CSV containerImage annotation '{container_image}' does not match any entry in image-references file. "
                        f"This may cause the containerImage to not be replaced correctly. "
                        f"Expected one of: {specs_from_refs}. "
                        f"Please update the upstream CSV to use the correct tag from image-references."
                    )

        # For Konflux engine on layered products (non-OCP), resolve operand NVRs
        # from DB instead of relying on predicted tags from operator rebase.
        # OCP operators still use the legacy tag-based resolution until this
        # is validated with layered products first.
        is_layered = not metadata.runtime.group.startswith("openshift-")
        resolved_operands: dict[str, tuple[str, str, str]] = {}
        if operator_build.engine is Engine.KONFLUX and image_references and is_layered:
            delivery_override_map, delivery_namespace_map = self._build_delivery_maps(metadata)
            resolved_operands = await self._resolve_operands_from_db(
                metadata, image_references, delivery_override_map, delivery_namespace_map
            )

        # Copy the operator's manifests to the bundle directory, replacing image
        # reference tags by their corresponding SHA for disconnected installs
        bundle_manifests_dir.mkdir(parents=True, exist_ok=True)
        all_found_operands: Dict[
            str, Tuple[str, str, str]
        ] = {}  # map of image name to (old_pullspec, new_pullspec, nvr)
        for src in operator_bundle_dir.iterdir():
            if src.name == "image-references":
                continue
            logger.info(f"Processing {src}...")
            async with aiofiles.open(src, "r") as f:
                content = await f.read()

            if operator_build.engine is Engine.KONFLUX and resolved_operands:
                # Replace operand references using upstream specs from image-references.
                # For layered products, operator rebase no longer writes predicted tags
                # (ART-18061), so the CSV retains original upstream specs. We replace
                # them directly with DB-resolved SHA pullspecs.
                found_images: dict[str, tuple[str, str, str]] = {}
                for delivery_name, (upstream_spec, new_pullspec, operand_nvr) in resolved_operands.items():
                    replaced_content = _replace_pullspec(content, upstream_spec, new_pullspec)
                    if replaced_content != content:
                        content = replaced_content
                        found_images[delivery_name] = (upstream_spec, new_pullspec, operand_nvr)
                found_images.update(self._find_external_digest_images(content, found_images))
            else:
                # Brew engine: use tag-based regex resolution (legacy path)
                content, found_images = await self._replace_image_references(
                    str(csv_config["registry"]), content, operator_build.engine, metadata
                )

            for _, (old_pullspec, new_pullspec, operand_nvr) in found_images.items():
                logger.info(f"Replaced image reference {old_pullspec} ({operand_nvr}) by {new_pullspec}")
            all_found_operands.update(found_images)

            dest = bundle_manifests_dir / src.name
            async with aiofiles.open(dest, "w") as f:
                if "clusterserviceversion.yaml" in src.name:
                    csv = yaml.safe_load(content)
                    csv["metadata"]["annotations"]["operators.openshift.io/valid-subscription"] = csv_config[
                        "valid-subscription-label"
                    ]
                    if found_images:
                        csv["spec"]["relatedImages"] = [
                            {"name": name, "image": new_pullspec} for name, (_, new_pullspec, _) in found_images.items()
                        ]
                    content = yaml.safe_dump(csv)
                await f.write(content)

        # Warn if the number of images found in the bundle doesn't match the image-references file
        if len(all_found_operands) != len(image_references):
            logger.warning(
                f"Found {len(all_found_operands)} images in the bundle, but {len(image_references)} in image-references"
                f" ({refs_path or 'file not found'})"
            )
            logger.warning(f"Found operands: {all_found_operands}")

        # Generate bundle's operator-framework tags
        operator_framework_tags = self._get_operator_framework_tags(channel_name, package_name)

        # Generate bundle's annotations.yaml
        bundle_metadata_dir = bundle_dir / "metadata"
        bundle_metadata_dir.mkdir(parents=True, exist_ok=True)
        dest_annotations_path = bundle_metadata_dir / "annotations.yaml"
        async with aiofiles.open(dest_annotations_path, 'w') as f:
            await f.write(yaml.safe_dump({'annotations': operator_framework_tags}))

        if not metadata.runtime.group.startswith("openshift-"):
            # Non-OCP products, such as MTC, may use dependencies.yaml
            dependencies_path = operator_manifests_dir / "metadata" / "dependencies.yaml"
            if dependencies_path.exists():
                dest_dependencies_path = bundle_metadata_dir / "dependencies.yaml"
                shutil.copy2(dependencies_path, dest_dependencies_path)

            # Non-OCP products, such as Logging, may use properties.yaml
            # Check for properties.yaml (in preferred location order)
            candidate_paths = [
                operator_manifests_dir / "openshift" / "metadata" / "properties.yaml",
                operator_manifests_dir / "metadata" / "properties.yaml",
            ]
            for candidate in candidate_paths:
                if candidate.exists():
                    dest_properties_path = bundle_metadata_dir / "properties.yaml"
                    shutil.copy2(candidate, dest_properties_path)
                    break

        # Generate bundle's Dockerfile
        nvr = await asyncio.to_thread(
            self._create_dockerfile, metadata, operator_dir, bundle_dir, operator_framework_tags, input_release
        )

        # Write .oit files. Those files are used by Doozer for additional information about the bundle
        await self._create_oit_files(package_name, csv_name, bundle_dir, operator_build.nvr, all_found_operands)
        return nvr

    async def _create_oit_files(
        self,
        package_name: str,
        csv_name: str,
        bundle_dir: Path,
        operator_nvr: str,
        operands: Dict[str, Tuple[str, str, str]],
    ):
        """Create .oit files

        :param bundle_dir: The directory where the bundle is located.
        :param all_found_operands: A map of all found operands in the bundle, in format of {image_name: (old_pullspec, new_pullspec, nvr)}
        """
        # Create a .oit/olm_bundle_info.yaml file to store additional information about the bundle
        oit_dir = bundle_dir / '.oit'
        oit_dir.mkdir(exist_ok=True)
        content = yaml.safe_dump(
            {
                "package_name": package_name,
                "csv_name": csv_name,
                "operator": {
                    "nvr": operator_nvr,
                },
                "operands": {
                    name: {
                        "nvr": nvr,
                        "internal_pullspec": old_pullspec,
                        "public_pullspec": new_pullspec,
                    }
                    for name, (old_pullspec, new_pullspec, nvr) in operands.items()
                },
            }
        )
        async with aiofiles.open(oit_dir / 'olm_bundle_info.yaml', 'w') as f:
            await f.write(content)

    def _build_delivery_maps(
        self,
        metadata: ImageMetadata,
    ) -> tuple[dict[str, str], dict[str, str]]:
        """
        Build delivery override and namespace maps from ocp-build-data image YAMLs.

        Arg(s):
            metadata: ImageMetadata providing runtime.data_dir.
        Return Value(s):
            tuple: (delivery_override_map, delivery_namespace_map)
                override_map: {versioned_short_name: unversioned_delivery_short_name}
                namespace_map: {image_short_name: delivery_namespace}
        """
        delivery_override_map: dict[str, str] = {}
        delivery_namespace_map: dict[str, str] = {}
        data_dir = metadata.runtime.data_dir
        for yml_path in glob.glob(f"{data_dir}/images/*.yml") + glob.glob(f"{data_dir}/images/*.yaml"):
            try:
                with open(yml_path) as yf:
                    img_data = yaml.safe_load(yf)
                if not isinstance(img_data, dict):
                    continue
                delivery = img_data.get("delivery", {}) or {}
                repo_names = delivery.get("delivery_repo_names") or []
                img_short = str(img_data.get("name", "")).rsplit("/", 1)[-1]
                if repo_names and "/" in str(repo_names[0]):
                    delivery_namespace_map[img_short] = str(repo_names[0]).rsplit("/", 1)[0]
                if not delivery.get("delivery_repo_name_override"):
                    continue
                if len(repo_names) != 1:
                    raise ValueError(
                        f"delivery_repo_name_override is set in {yml_path} but delivery_repo_names has "
                        f"{len(repo_names)} entries (expected exactly 1)"
                    )
                override_short = str(repo_names[0]).rsplit("/", 1)[-1]
                delivery_override_map[img_short] = override_short
            except (yaml.YAMLError, OSError) as e:
                self._logger.warning("Failed to parse image YAML %s: %s", yml_path, e)
        return delivery_override_map, delivery_namespace_map

    def _build_delivery_pullspec(
        self,
        image_short_name: str,
        image_sha: str,
        original_namespace: str,
        metadata: ImageMetadata,
        delivery_override_map: dict[str, str],
        delivery_namespace_map: dict[str, str],
    ) -> tuple[str, str]:
        """
        Build the final delivery pullspec for an operand image.

        Arg(s):
            image_short_name: Short name of the image (e.g. "ose-csi-driver-4.18-rhel9").
            image_sha: SHA digest (e.g. "sha256:abc123...").
            original_namespace: Namespace from the original pullspec.
            metadata: ImageMetadata for group context.
            delivery_override_map: Versioned-to-unversioned name overrides.
            delivery_namespace_map: Image-to-namespace overrides.
        Return Value(s):
            tuple: (delivery_short_name, new_pullspec)
        """
        csv_namespace = self._group_config.get("csv_namespace", "openshift")
        try:
            major = int(self._group_config.vars.get("MAJOR", 4))
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid MAJOR version in group config for {metadata.runtime.group}: "
                f"{self._group_config.vars.get('MAJOR')}"
            ) from e
        default_delivery_namespace = f"openshift{major}"
        if not metadata.runtime.group.startswith("openshift-"):
            new_namespace = delivery_namespace_map.get(image_short_name, original_namespace)
        else:
            new_namespace = (
                delivery_namespace_map.get(image_short_name, default_delivery_namespace)
                if original_namespace == csv_namespace
                else original_namespace
            )
        delivery_short_name = delivery_override_map.get(image_short_name, image_short_name)
        new_pullspec = f"registry.redhat.io/{new_namespace}/{delivery_short_name}@{image_sha}"
        return delivery_short_name, new_pullspec

    @lru_cache
    @staticmethod
    def _get_image_reference_pattern(registry: str):
        """Get a compiled regex pattern to match image references in the format of `registry/namespace/image:tag`."""
        pattern = r'{}\/([^:]+):([^\'"\\\s]+)'.format(re.escape(registry))
        return re.compile(pattern)

    @staticmethod
    def _get_digest_image_pattern():
        """Get a compiled regex pattern to match digest-pinned image references.

        Matches images in the format: registry/namespace/image@sha256:digest
        Examples:
            - registry.redhat.io/rhel9/postgresql-15@sha256:abc123...
            - quay.io/openshift/image@sha256:def456...
        """
        # Match: registry/path@sha256:hexdigest
        pattern = r'([a-zA-Z0-9][-a-zA-Z0-9.]*(?::[0-9]+)?/[^@\s]+)@(sha256:[a-fA-F0-9]{64})'
        return re.compile(pattern)

    @staticmethod
    def _build_component_to_meta_map(runtime) -> Dict[str, "ImageMetadata"]:
        """
        Build a map of component name (as it appears in the com.redhat.component label / NVR) to
        the ART ImageMetadata that builds it, covering every image declared in the group -- not
        just the images actually loaded for this invocation.

        `doozer beta:images:konflux:bundle <operator-nvr>` restricts `runtime.images` to just the
        given operator(s) (see KonfluxBundleCli.get_operator_builds()), so `runtime.image_metas()`
        alone would miss an embargoed operand image that belongs to a different (unloaded) image
        in the same group, e.g. a dependent image referenced by the operator's CSV. Fall back to
        `Runtime.late_resolve_image()`, which can resolve any image declared in the group's
        `images/` directory on demand without adding it to `image_map` or triggering dependents.
        """
        component_to_meta = {im.get_component_name(): im for im in runtime.image_metas()}
        for distgit_key in set(runtime.image_name_map.values()):
            if distgit_key in runtime.image_map:
                continue  # already covered by runtime.image_metas() above
            other_meta = runtime.late_resolve_image(distgit_key, add=False, required=False)
            if other_meta is not None:
                component_to_meta.setdefault(other_meta.get_component_name(), other_meta)
        return component_to_meta

    async def _resolve_operands_from_db(
        self,
        metadata: ImageMetadata,
        image_references: dict[str, dict],
        delivery_override_map: dict[str, str],
        delivery_namespace_map: dict[str, str],
    ) -> dict[str, tuple[str, str, str]]:
        """
        Resolve ART-built operand images from Konflux DB instead of relying
        on predicted v-r tags baked into the operator CSV at rebase time.

        At bundle rebase time, all image builds are guaranteed complete
        (pipeline enforces: image rebase -> image build -> sync -> bundle rebase).
        So we query the DB for actual builds rather than trusting predicted tags.

        Embargoed operand builds are automatically substituted with the latest
        public build, since operator bundles must never ship embargoed content.

        Arg(s):
            metadata: ImageMetadata of the operator whose bundle is being rebased.
            image_references: Parsed image-references entries {name: {from: {name: spec}}}.
            delivery_override_map: Versioned-to-unversioned name overrides.
            delivery_namespace_map: Image-to-namespace overrides.
        Return Value(s):
            dict: {delivery_image_short_name: (upstream_spec, new_pullspec, nvr)}
        """
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        csv_namespace = self._group_config.get("csv_namespace", "openshift")

        # Phase 1a: Validate metadata and collect DB query coroutines
        entries_meta: list[tuple[str, str, ImageMetadata]] = []  # (name, spec, meta)
        build_coros = []

        for name, ref_entry in image_references.items():
            spec = ref_entry["from"]["name"]

            distgit_key = metadata.runtime.name_in_bundle_map.get(name)
            if not distgit_key:
                raise ValueError(f"Unable to find {name} in name_in_bundle_map for {metadata.distgit_key}")

            meta = metadata.runtime.image_map.get(distgit_key)
            if not meta:
                meta = metadata.runtime.late_resolve_image(distgit_key, required=False)
                if meta is None:
                    raise DoozerFatalError(
                        f"Attempted to load image {distgit_key} but it has mode disabled; "
                        f"{metadata.distgit_key} references it in image-references"
                    )

            entries_meta.append((name, spec, meta))
            el_target = f"el{meta.branch_el_target()}"
            build_coros.append(
                meta.get_latest_konflux_build(
                    el_target=el_target,
                    exclude_large_columns=True,
                )
            )

        # Phase 1b: Fetch all builds concurrently, then check embargo status
        builds = await asyncio.gather(*build_coros)

        operand_entries: list[tuple[str, str, str, ImageMetadata]] = []
        image_info_coros = []
        for (name, spec, meta), build in zip(entries_meta, builds, strict=True):
            if not build:
                raise ValueError(f"Could not find latest Konflux build for {meta.distgit_key}")

            # Operator bundles must never reference embargoed operand images.
            # If the latest build is embargoed, substitute with the latest public build.
            try:
                embargoed = is_nvr_embargoed(build.nvr)
            except ValueError as e:
                raise KonfluxOlmBundleRebaseError(
                    f"Unable to determine embargo status for operand {build.nvr} referenced "
                    f"by operator {metadata.distgit_key}: {e}"
                ) from e
            if embargoed:
                public_build = await meta.get_latest_konflux_build(
                    default=None,
                    el_target=f"el{meta.branch_el_target()}",
                    embargoed=False,
                    exclude_large_columns=True,
                )
                if not public_build:
                    raise KonfluxOlmBundleRebaseError(
                        f"Operand {build.nvr} referenced by operator {metadata.distgit_key} is "
                        f"embargoed, and no public (non-embargoed) build of '{meta.distgit_key}' "
                        f"is available to substitute. Cannot safely rebase this operator bundle."
                    )
                logger.warning(
                    "Operand %s referenced by operator %s is embargoed; substituting with public build %s",
                    build.nvr,
                    metadata.distgit_key,
                    public_build.nvr,
                )
                build = public_build

            build_pullspec = f"{self.image_repo}:{meta.image_name_short}-{build.version}-{build.release}"
            logger.info(f"Resolved {name} -> {build.nvr} (pullspec: {build_pullspec})")

            operand_entries.append((name, spec, build_pullspec, meta))
            image_info_coros.append(
                util.oc_image_info_for_arch_async(
                    build_pullspec,
                    registry_config=os.getenv("QUAY_AUTH_FILE"),
                )
            )

        # Phase 2: Fetch all image infos concurrently
        image_infos = await asyncio.gather(*image_info_coros)

        resolved: dict[str, tuple[str, str, str]] = {}
        for (_name, spec, _build_pullspec, meta), image_info in zip(operand_entries, image_infos, strict=True):
            image_labels = image_info["config"]["config"]["Labels"]
            image_nvr = f"{image_labels['com.redhat.component']}-{image_labels['version']}-{image_labels['release']}"

            image_sha = (
                image_info["contentDigest"]
                if self._group_config.operator_image_ref_mode == "by-arch"
                else image_info["listDigest"]
            )

            # Derive namespace and image name from the operand's delivery
            # config in ocp-build-data rather than from the upstream spec in
            # image-references (which may point to an unrelated upstream
            # registry, e.g. quay.io/konveyor/* for OADP).
            repo_names = meta.config.get("delivery", {}).get("delivery_repo_names") or []
            if repo_names and "/" in str(repo_names[0]):
                original_namespace = str(repo_names[0]).rsplit("/", 1)[0]
                image_short_name = str(repo_names[0]).rsplit("/", 1)[-1]
            else:
                image_short_name = meta.image_name_short
                original_namespace = csv_namespace

            delivery_short_name, new_pullspec = self._build_delivery_pullspec(
                image_short_name,
                image_sha,
                original_namespace,
                metadata,
                delivery_override_map,
                delivery_namespace_map,
            )
            resolved[delivery_short_name] = (spec, new_pullspec, image_nvr)

        return resolved

    async def _replace_image_references(self, old_registry: str, content: str, engine: Engine, metadata):
        """
        Replace image references in the content by their corresponding SHA.

        Legacy path used by Brew engine. For Konflux, _resolve_operands_from_db
        is used instead.

        Arg(s):
            old_registry: Registry prefix to match (e.g. "registry.redhat.io").
            content: File content to process.
            engine: Build engine (KONFLUX or BREW).
            metadata: ImageMetadata for the operator.
        Return Value(s):
            tuple: (new_content, found_images) where found_images is
                {image_name: (old_pullspec, new_pullspec, nvr)}
        """
        new_content = content
        found_images: Dict[str, Tuple[str, str, str]] = {}

        # Step 1: Find and process ART-built images matching the registry pattern
        pattern = KonfluxOlmBundleRebaser._get_image_reference_pattern(old_registry)
        art_references = {}
        image_info_coros = []
        for match in pattern.finditer(content):
            pullspec = match.group(0)
            namespace, image_short_name = match.group(1).rsplit('/', maxsplit=1)
            image_tag = match.group(2)
            art_references[pullspec] = (namespace, image_short_name, image_tag)

        for _pullspec, (namespace, image_short_name, image_tag) in art_references.items():
            if engine is Engine.KONFLUX:
                build_pullspec = f"{self.image_repo}:{image_short_name}-{image_tag}"
                image_info_coros.append(
                    util.oc_image_info_for_arch_async(
                        build_pullspec,
                        registry_config=os.getenv("QUAY_AUTH_FILE"),
                    )
                )
            elif engine is Engine.BREW:
                build_pullspec = (
                    f"{constants.REGISTRY_PROXY_BASE_URL}/rh-osbs/{namespace}-{image_short_name}:{image_tag}"
                )
                image_info_coros.append(
                    util.oc_image_info_for_arch_async(
                        build_pullspec,
                    )
                )
        image_infos = await asyncio.gather(*image_info_coros)

        delivery_override_map, delivery_namespace_map = self._build_delivery_maps(metadata)
        _component_to_meta: Optional[Dict[str, "ImageMetadata"]] = None

        for pullspec, image_info in zip(art_references, image_infos, strict=True):
            image_labels = image_info['config']['config']['Labels']
            image_component_name = image_labels['com.redhat.component']
            image_nvr = f"{image_component_name}-{image_labels['version']}-{image_labels['release']}"

            # Operator bundles are only ever shipped publicly, so they must never reference an
            # embargoed (private-fix) operand image. If the resolved operand build is embargoed,
            # substitute it with the latest public (non-embargoed) build of that same component.
            # If no public build exists, fail the rebase rather than shipping embargoed content.
            try:
                operand_is_embargoed = engine is Engine.KONFLUX and is_nvr_embargoed(image_nvr)
            except ValueError as e:
                raise KonfluxOlmBundleRebaseError(
                    f"Unable to determine embargo status for operand image {image_nvr} referenced "
                    f"by operator {metadata.distgit_key}: {e}"
                ) from e
            if operand_is_embargoed:
                if _component_to_meta is None:
                    _component_to_meta = self._build_component_to_meta_map(metadata.runtime)
                operand_meta = _component_to_meta.get(image_component_name)
                if operand_meta is None:
                    raise KonfluxOlmBundleRebaseError(
                        f"Operand image {image_nvr} referenced by operator {metadata.distgit_key} is "
                        f"embargoed, but no ART image metadata could be found for component "
                        f"'{image_component_name}' to look up a public substitute."
                    )
                public_build = await operand_meta.get_latest_konflux_build(
                    default=None,
                    el_target=operand_meta.branch_el_target(),
                    embargoed=False,
                    exclude_large_columns=True,
                )
                if not public_build:
                    raise KonfluxOlmBundleRebaseError(
                        f"Operand image {image_nvr} referenced by operator {metadata.distgit_key} is "
                        f"embargoed, and no public (non-embargoed) build of '{operand_meta.distgit_key}' "
                        f"is available to substitute. Cannot safely rebase this operator bundle."
                    )
                self._logger.warning(
                    "Operand image %s referenced by operator %s is embargoed; substituting with public build %s",
                    image_nvr,
                    metadata.distgit_key,
                    public_build.nvr,
                )
                image_info = await util.oc_image_info_for_arch_async(
                    public_build.image_pullspec,
                    registry_config=os.getenv("QUAY_AUTH_FILE"),
                )
                image_nvr = public_build.nvr

            namespace, image_short_name, _image_tag = art_references[pullspec]
            image_sha = (
                image_info['contentDigest']
                if self._group_config.operator_image_ref_mode == 'by-arch'
                else image_info['listDigest']
            )
            delivery_short_name, new_pullspec = self._build_delivery_pullspec(
                image_short_name,
                image_sha,
                namespace,
                metadata,
                delivery_override_map,
                delivery_namespace_map,
            )
            new_content = new_content.replace(pullspec, new_pullspec)
            found_images[delivery_short_name] = (pullspec, new_pullspec, image_nvr)

        # Step 2: Find digest-pinned images (external, already resolved)
        found_images.update(self._find_external_digest_images(new_content, found_images))

        return new_content, found_images

    @staticmethod
    def _find_external_digest_images(
        content: str,
        already_resolved: dict[str, tuple[str, str, str]],
    ) -> dict[str, tuple[str, str, str]]:
        """
        Scan content for digest-pinned external images not already in already_resolved.

        Arg(s):
            content: File content to scan.
            already_resolved: Images already resolved (to skip).
        Return Value(s):
            dict: {image_short_name: (pullspec, pullspec, "external")}
        """
        found: dict[str, tuple[str, str, str]] = {}
        digest_pattern = KonfluxOlmBundleRebaser._get_digest_image_pattern()
        for match in digest_pattern.finditer(content):
            image_path = match.group(1)
            digest = match.group(2)
            pullspec = f"{image_path}@{digest}"
            image_short_name = image_path.rsplit("/", 1)[-1]
            if image_short_name not in already_resolved and image_short_name not in found:
                found[image_short_name] = (pullspec, pullspec, "external")
        return found

    @cached_property
    def _operator_index_mode(self):
        mode = self._group_config.operator_index_mode or 'ga'  # default when missing
        if mode in {'pre-release', 'ga', 'ga-plus'}:
            # pre-release: label for pre-release operator index (unsupported)
            # ga: label for only this release's operator index
            # ga-plus: label for this release's operator index and future release indexes as well
            # [lmeyer 20240108] ref https://chat.google.com/room/AAAAZrx3KlI/6tf0phEdCF8
            # We may never use ga-plus, since the original motivation no longer seems important, and
            # it results in a problem: stage pushes for ga-plus v4.y fail when there is staged
            # v4.(y+1) content already with `skipVersion: v4.y` (because new v4.y content would be
            # immediately pruned). If we need `ga-plus` again, we can likely find a way around it.
            return mode
        self._logger.warning(f'{mode} is not a valid group_config.operator_index_mode. Defaulting to "ga"')
        return 'ga'

    @cached_property
    def _redhat_delivery_tags(self):
        mode = self._operator_index_mode
        versions = 'v{MAJOR}.{MINOR}' if mode == 'ga-plus' else '=v{MAJOR}.{MINOR}'

        labels = {
            'com.redhat.delivery.operator.bundle': 'true',
            'com.redhat.openshift.versions': versions.format(**self._group_config.vars),
        }
        # TODO: deprecate pre-release mode support
        if mode == 'pre-release':
            labels['com.redhat.prerelease'] = 'true'
        return labels

    def _get_operator_framework_tags(self, channel_name: str, package_name: str):
        override_channel = channel_name
        override_default = channel_name
        stable_channel = "stable"
        # see: issues.redhat.com/browse/ART-3107
        if self._group_config.operator_channel_stable in ['default', 'extra']:
            override_channel = ','.join((channel_name, stable_channel))
        if self._group_config.operator_channel_stable == 'default':
            override_default = stable_channel
        tags = {
            'operators.operatorframework.io.bundle.channel.default.v1': override_default,
            'operators.operatorframework.io.bundle.channels.v1': override_channel,
            'operators.operatorframework.io.bundle.manifests.v1': 'manifests/',
            'operators.operatorframework.io.bundle.mediatype.v1': 'registry+v1',
            'operators.operatorframework.io.bundle.metadata.v1': 'metadata/',
            'operators.operatorframework.io.bundle.package.v1': package_name,
        }
        return tags

    def _create_dockerfile(
        self,
        metadata: ImageMetadata,
        operator_dir: Path,
        bundle_dir: Path,
        operator_framework_tags: Dict[str, str],
        input_release: str,
    ) -> str:
        operator_df = DockerfileParser(str(operator_dir.joinpath('Dockerfile')))
        bundle_df = DockerfileParser(str(bundle_dir.joinpath('Dockerfile')))

        bundle_df.content = 'FROM scratch\nCOPY ./manifests /manifests\nCOPY ./metadata /metadata'

        component_name = metadata.get_olm_bundle_brew_component_name()
        bundle_version = f'{operator_df.labels["version"]}.{operator_df.labels["release"]}'
        # Copy the operator's Dockerfile labels to the bundle's Dockerfile
        # and add additional labels required by the bundle
        bundle_df.labels = {
            **operator_df.labels,
            **self._redhat_delivery_tags,
            **operator_framework_tags,
            'com.redhat.component': component_name,
            'com.redhat.delivery.appregistry': '',  # This is a bundle, not an operator
            'name': (bundle_name := metadata.get_olm_bundle_image_name()),
            'version': bundle_version,
            'release': input_release,
        }
        # NVR is constructed from the component name, version, and release
        # and not the `name` label
        nvr = f'{component_name}-{bundle_version}-{input_release}'

        # The following labels are required by Conforma
        if 'distribution-scope' not in bundle_df.labels:
            # If the operator doesn't have a distribution-scope label, default to public
            bundle_df.labels['distribution-scope'] = 'public'
        if 'url' not in bundle_df.labels:
            # If the operator doesn't have a URL label, default to what OSBS uses for bundle images
            # (https://redhat-internal.slack.com/archives/C02AX10EQJW/p1749699266047229).
            bundle_df.labels['url'] = (
                f'https://access.redhat.com/containers/#/registry.access.redhat.com/{bundle_name}/images/{bundle_version}-{input_release}'
            )
        return nvr


class KonfluxOlmBundleBuildError(Exception):
    def __init__(self, message: str, pipelinerun_name: str, pipelinerun_dict: Optional[Dict]) -> None:
        super().__init__(message)
        self.pipelinerun_name = pipelinerun_name
        self.pipelinerun_dict = pipelinerun_dict


class KonfluxOlmBundleBuilder:
    def __init__(
        self,
        base_dir: Path,
        group: str,
        assembly: str,
        source_resolver: SourceResolver,
        db: KonfluxDb,
        konflux_namespace: str,
        konflux_kubeconfig: Optional[str] = None,
        konflux_context: Optional[str] = None,
        image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO,
        skip_checks: bool = False,
        skip_tasks: Sequence[str] = (),
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL,
        dry_run: bool = False,
        skip_ec_verify: bool = False,
        assembly_type: Optional[AssemblyTypes] = None,
        record_logger: Optional[RecordLogger] = None,
        logger: logging.Logger = _LOGGER,
    ) -> None:
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self._source_resolver = source_resolver
        self._db = db
        self.konflux_namespace = konflux_namespace
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.skip_tasks = tuple(skip_tasks)
        self.pipelinerun_template_url = pipelinerun_template_url
        self.dry_run = dry_run
        self.skip_ec_verify = skip_ec_verify
        self.assembly_type = assembly_type
        self._record_logger = record_logger
        self._logger = logger
        self._konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_namespace,
            config_file=self.konflux_kubeconfig,
            context=self.konflux_context,
            dry_run=self.dry_run,
        )

    async def build(self, metadata: ImageMetadata, git_auth_secret: Optional[str] = None):
        """Build a bundle with Konflux."""
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        konflux_client = self._konflux_client
        bundle_dir = self.base_dir.joinpath(metadata.get_olm_bundle_short_name())
        df_path = bundle_dir.joinpath("Dockerfile")

        record = {
            'status': -1,  # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            "message": "Unknown failure",
            "task_id": "n/a",
            "task_url": "n/a",
            "operator_nvr": "n/a",
            "operand_nvrs": "n/a",
            "bundle_nvr": "n/a",
        }

        try:
            if bundle_dir.exists():
                # Load exiting build source repository
                logger.info("Loading existing bundle repository...")
                bundle_build_repo = await BuildRepo.from_local_dir(bundle_dir, self._logger)
                logger.info("Bundle repository loaded from %s", bundle_dir)
            else:
                source = None
                if metadata.has_source():
                    logger.info("Resolving source...")
                    source = cast(
                        SourceResolution,
                        await exectools.to_thread(self._source_resolver.resolve_source, metadata, no_clone=True),
                    )
                else:
                    raise IOError(
                        f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported."
                    )
                # Clone the build source repository
                bundle_build_branch = "art-{group}-assembly-{assembly_name}-bundle-{distgit_key}".format_map(
                    {
                        "group": self.group,
                        "assembly_name": self.assembly,
                        "distgit_key": metadata.distgit_key,
                    }
                )
                logger.info("Cloning bundle repository...")
                bundle_build_repo = BuildRepo(
                    url=source.url, branch=bundle_build_branch, local_dir=bundle_dir, logger=self._logger
                )
                await bundle_build_repo.ensure_source()
                logger.info("Bundle repository cloned to %s", bundle_dir)
            if not bundle_build_repo.commit_hash:
                raise IOError(f"Bundle repository {bundle_build_repo.url} doesn't have any commits to build")

            # Parse bundle's Dockerfile
            bundle_df = DockerfileParser(str(df_path))
            component_name = bundle_df.labels.get('com.redhat.component')
            if not component_name:
                raise IOError(f"{metadata.distgit_key}: Label 'com.redhat.component' is not set. Did you run rebase?")
            version = bundle_df.labels.get('version')
            if not version:
                raise IOError(f"{metadata.distgit_key}: Label 'version' is not set. Did you run rebase?")
            release = bundle_df.labels.get('release')
            if not release:
                raise IOError(f"{metadata.distgit_key}: Label 'release' is not set. Did you run rebase?")
            nvr = f"{component_name}-{version}-{release}"
            record['bundle_nvr'] = nvr
            output_image = f"{self.image_repo}:{nvr}"

            # Load olm_bundle_info.yaml to get the operator and operand NVRs
            async with aiofiles.open(bundle_build_repo.local_dir / '.oit' / 'olm_bundle_info.yaml', 'r') as f:
                bundle_info = yaml.safe_load(await f.read())
            package_name = bundle_info['package_name']
            csv_name = bundle_info['csv_name']
            operator_nvr = bundle_info['operator']['nvr']
            record['operator_nvr'] = operator_nvr
            operand_nvrs = sorted({info['nvr'] for info in bundle_info['operands'].values()})
            record['operand_nvrs'] = ','.join(operand_nvrs)

            # Start the bundle build
            logger.info("Starting Konflux bundle image build for %s...", metadata.distgit_key)
            build_attempts = metadata.get_konflux_build_attempts()
            pipelinerun_dict = None  # Initialize to handle cases where the loop doesn't set it
            for attempt in range(build_attempts):
                logger.info("Build attempt %d/%d", attempt + 1, build_attempts)
                pipelinerun_info, url = await self._start_build(
                    metadata,
                    bundle_build_repo,
                    output_image,
                    self.konflux_namespace,
                    self.skip_checks,
                    skip_tasks=self.skip_tasks,
                    git_auth_secret=git_auth_secret,
                )
                pipelinerun_name = pipelinerun_info.name
                record["task_id"] = pipelinerun_name
                record["task_url"] = url

                # Update the Konflux DB with status PENDING
                outcome = KonfluxBuildOutcome.PENDING
                if not self.dry_run:
                    await self._update_konflux_db(
                        metadata,
                        bundle_build_repo,
                        package_name,
                        csv_name,
                        pipelinerun_info,
                        outcome,
                        operator_nvr,
                        operand_nvrs,
                    )
                else:
                    logger.warning("Dry run: Would update Konflux DB for %s with outcome %s", pipelinerun_name, outcome)

                # Wait for the PipelineRun to complete
                pipelinerun_info = await konflux_client.wait_for_pipelinerun(pipelinerun_name, self.konflux_namespace)
                logger.info("PipelineRun %s completed", pipelinerun_name)

                pipelinerun_dict = pipelinerun_info.to_dict()
                succeeded_condition = pipelinerun_info.find_condition('Succeeded')
                outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)

                ec_failed = False
                ec_pipeline_url = ''
                if not self.dry_run:
                    results = pipelinerun_dict.get('status', {}).get('results', [])
                    image_pullspec = next((r['value'] for r in results if r['name'] == 'IMAGE_URL'), None)
                    image_digest = next((r['value'] for r in results if r['name'] == 'IMAGE_DIGEST'), None)

                    if not (image_pullspec and image_digest):
                        raise ValueError(
                            f"[{metadata.distgit_key}] Could not find expected results in konflux "
                            f"pipelinerun {pipelinerun_name}"
                        )

                    # Sync the bundle to art-images-share
                    await sync_to_quay(
                        f"{image_pullspec.split(':')[0]}@{image_digest}", KONFLUX_DEFAULT_IMAGE_SHARE_REPO
                    )

                    # Run EC verification after a successful bundle build
                    is_ocp_group = self.group.startswith("openshift-")
                    if outcome is KonfluxBuildOutcome.SUCCESS and is_ocp_group and not self.skip_ec_verify:
                        app_name = util.konflux_application_name(metadata.runtime.group)
                        bundle_name = metadata.get_olm_bundle_short_name()
                        component_name = util.konflux_image_component_name(app_name, bundle_name)
                        image_with_digest = f"{image_pullspec.split(':')[0]}@{image_digest}"
                        source_url = artlib_util.convert_remote_git_to_https(bundle_build_repo.url)

                        is_test_assembly = self.assembly == "test"
                        if self.assembly_type == AssemblyTypes.PREVIEW:
                            ec_policy = (
                                constants.KONFLUX_TEST_PREGA_EC_POLICY_CONFIGURATION
                                if is_test_assembly
                                else constants.KONFLUX_PREGA_EC_POLICY_CONFIGURATION
                            )
                        else:
                            ec_policy = (
                                constants.KONFLUX_TEST_EC_POLICY_CONFIGURATION
                                if is_test_assembly
                                else constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
                            )

                        ec_result = await konflux_client.verify_enterprise_contract(
                            namespace=self.konflux_namespace,
                            application_name=app_name,
                            component_name=component_name,
                            image_pullspec=image_with_digest,
                            source_url=source_url,
                            commit_sha=bundle_build_repo.commit_hash,
                            ec_policy=ec_policy,
                            logger=logger,
                        )
                        ec_pipeline_url = ec_result.ec_pipeline_url
                        if ec_result.ec_failed:
                            outcome = KonfluxBuildOutcome.ITS_ERROR
                            ec_failed = True
                    elif outcome is KonfluxBuildOutcome.SUCCESS:
                        if self.skip_ec_verify:
                            logger.info("Skipping EC verification for %s: skip_ec_verify is set", metadata.distgit_key)
                        elif not is_ocp_group:
                            logger.info(
                                "Skipping EC verification for %s: non-OCP group '%s'",
                                metadata.distgit_key,
                                self.group,
                            )

                    # Update the Konflux DB with the final outcome
                    await self._update_konflux_db(
                        metadata,
                        bundle_build_repo,
                        package_name,
                        csv_name,
                        pipelinerun_info,
                        outcome,
                        operator_nvr,
                        operand_nvrs,
                        ec_pipeline_url=ec_pipeline_url,
                    )
                else:
                    logger.warning("Dry run: Would update Konflux DB for %s with outcome %s", pipelinerun_name, outcome)
                if outcome is not KonfluxBuildOutcome.SUCCESS:
                    error = KonfluxOlmBundleBuildError(
                        f"Konflux bundle image build for {metadata.distgit_key} failed",
                        pipelinerun_name,
                        pipelinerun_dict,
                    )
                    logger.error(f"{error}: {url}")
                    if ec_failed:
                        break
                else:
                    error = None
                    record["message"] = "Success"
                    record['status'] = 0
                    break
            if error:
                record['message'] = str(error)
                raise error
        finally:
            if self._record_logger:
                self._record_logger.add_record("build_olm_bundle_konflux", **record)
        return pipelinerun_name, pipelinerun_dict

    @staticmethod
    def get_old_component_name(application_name: str, bundle_name: str):
        # TODO: (2025-Jul-09) remove this once we have new builds using the new component name
        return f"{application_name}-{bundle_name}".replace(".", "-").replace("_", "-")

    async def _start_build(
        self,
        metadata: ImageMetadata,
        bundle_build_repo: BuildRepo,
        output_image: str,
        namespace: str,
        skip_checks: bool = False,
        skip_tasks: Sequence[str] = (),
        additional_tags: Optional[Sequence[str]] = None,
        git_auth_secret: Optional[str] = None,
    ) -> Tuple[PipelineRunInfo, str]:
        """Start a build with Konflux."""
        if not bundle_build_repo.commit_hash:
            raise IOError("Bundle repository must have a commit to build. Did you rebase?")
        konflux_client = self._konflux_client
        if additional_tags is None:
            additional_tags = []
        target_branch = bundle_build_repo.branch or bundle_build_repo.commit_hash
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        # Ensure the Application resource exists
        app_name = util.konflux_application_name(metadata.runtime.group)
        logger.info(f"Using Konflux application: {app_name}")
        await konflux_client.ensure_application(name=app_name, display_name=app_name)
        logger.info(f"Konflux application {app_name} created")
        # Ensure the Component resource exists
        bundle_name = metadata.get_olm_bundle_short_name()
        component_name = util.konflux_image_component_name(app_name, bundle_name)
        logger.info(f"Creating Konflux component: {component_name}")
        dest_image_repo = output_image.split(":")[0]
        await konflux_client.ensure_component(
            name=component_name,
            application=app_name,
            component_name=component_name,
            image_repo=dest_image_repo,
            source_url=bundle_build_repo.https_url,
            revision=target_branch,
        )
        logger.info(f"Konflux component {component_name} created")
        # Start a PipelineRun
        build_kwargs = dict(
            generate_name=f"{component_name}-",
            namespace=namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=bundle_build_repo.https_url,
            commit_sha=bundle_build_repo.commit_hash,
            target_branch=target_branch,
            output_image=output_image,
            building_arches=["x86_64"],
            pipelinerun_template_url=self.pipelinerun_template_url,
            build_params=ImageBuildParams(
                additional_tags=list(additional_tags),
                skip_checks=skip_checks,
                skip_tasks=skip_tasks,
                hermetic=True,
                fetch_tags=False,
                artifact_type="operatorbundle",
                build_priority=BUNDLE_BUILD_PRIORITY,
            ),
        )
        if git_auth_secret:
            build_kwargs["git_auth_secret"] = git_auth_secret
        pipelinerun_info = await konflux_client.start_pipeline_run_for_image_build(**build_kwargs)
        url = konflux_client.resource_url(pipelinerun_info.to_dict())
        logger.info(f"PipelineRun {pipelinerun_info.name} created: {url}")
        return pipelinerun_info, url

    async def _update_konflux_db(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        bundle_package_name: str,
        bundle_csv_name: str,
        pipelinerun_info: PipelineRunInfo,
        outcome: KonfluxBuildOutcome,
        operator_nvr: str,
        operand_nvrs: list[str],
        ec_pipeline_url: str = '',
    ):
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        db = self._db
        if not db or db.record_cls != KonfluxBundleBuildRecord:
            logger.warning('Konflux DB connection is not initialized, not writing build record to the Konflux DB.')
            return
        try:
            rebase_repo_url = build_repo.https_url
            rebase_commit = build_repo.commit_hash

            df_path = build_repo.local_dir.joinpath("Dockerfile")
            df = DockerfileParser(str(df_path))

            source_repo = df.labels['io.openshift.build.source-location']
            commitish = df.labels['io.openshift.build.commit.id']

            component_name = df.labels['com.redhat.component']
            version = df.labels['version']
            release = df.labels['release']
            nvr = "-".join([component_name, version, release])

            pipelinerun_name = pipelinerun_info.name
            pipelinerun_dict = pipelinerun_info.to_dict()
            build_pipeline_url = KonfluxClient.resource_url(pipelinerun_dict)
            build_component = pipelinerun_dict['metadata']['labels'].get('appstudio.openshift.io/component')

            build_record_params = {
                'name': metadata.get_olm_bundle_short_name(),
                'version': version,
                'release': release,
                'start_time': datetime.now(tz=timezone.utc),
                'end_time': None,
                'nvr': nvr,
                'group': metadata.runtime.group,
                'assembly': metadata.runtime.assembly,
                'source_repo': source_repo,
                'commitish': commitish,
                'rebase_repo_url': rebase_repo_url,
                'rebase_commitish': rebase_commit,
                'engine': Engine.KONFLUX,
                'outcome': str(outcome),
                'art_job_url': os.getenv('BUILD_URL', 'n/a'),
                'build_id': pipelinerun_name,
                'build_pipeline_url': build_pipeline_url,
                'pipeline_commit': 'n/a',  # TODO: populate this
                'bundle_package_name': bundle_package_name,
                'bundle_csv_name': bundle_csv_name,
                'operator_nvr': operator_nvr,
                'operand_nvrs': operand_nvrs,
                'build_component': build_component,
                'ec_pipeline_url': ec_pipeline_url,
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
            status = pipelinerun_dict.get('status', {})
            if status:
                start_time = status.get('startTime')
                if start_time:
                    build_record_params['start_time'] = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                        tzinfo=timezone.utc
                    )
                completion_time = status.get('completionTime')
                if completion_time:
                    build_record_params['end_time'] = datetime.strptime(completion_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                        tzinfo=timezone.utc
                    )

            build_record = KonfluxBundleBuildRecord(**build_record_params)
            db.add_build(build_record)
            logger.info('Konflux build %s info stored successfully with status %s', build_record.nvr, outcome)

        except Exception:
            logger.exception('Failed writing record to the konflux DB')
