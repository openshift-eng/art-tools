import asyncio
import glob
import logging
import os
import re
from datetime import datetime, timezone
from functools import cached_property, lru_cache
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple, cast

import aiofiles
import yaml
from artcommonlib import exectools
from artcommonlib import util as artlib_util
from artcommonlib.constants import KONFLUX_ART_IMAGES_SHARE
from artcommonlib.exectools import limit_concurrency
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord, KonfluxBundleBuildRecord
from artcommonlib.konflux.konflux_db import Engine, KonfluxDb
from artcommonlib.model import Model
from artcommonlib.util import sync_to_quay
from dockerfile_parse import DockerfileParser
from doozerlib import constants, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient, resource
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
from doozerlib.source_resolver import SourceResolution, SourceResolver

_LOGGER = logging.getLogger(__name__)


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

    async def rebase(self, metadata: ImageMetadata, operator_build: KonfluxBuildRecord, input_release: str) -> str:
        """Rebase an operator with Konflux.

        :param metadata: The metadata of the operator to rebase.
        :param operator_build_record: The build record of the operator to rebase. If not provided, the latest build record will be used.
        :param input_release: The release string for the new bundle. None to let the build backend generate it.
        """
        assert input_release, "input_release must be provided"
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")

        source = None
        if metadata.has_source():
            logger.info("Resolving source...")
            source = cast(
                SourceResolution,
                await exectools.to_thread(self._source_resolver.resolve_source, metadata, no_clone=True),
            )
        else:
            raise IOError(f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported.")

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
        # commit and push the changes
        logger.info("Committing and pushing bundle content...")
        await bundle_build_repo.commit(f"Update bundle manifests for {operator_build.nvr}", allow_empty=True)
        if not self.dry_run:
            await bundle_build_repo.push()
        return nvr

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

        # Copy the operator's manifests to the bundle directory
        bundle_manifests_dir.mkdir(parents=True, exist_ok=True)
        # Iterate through all bundle manifests files, replacing any image reference tag by its
        # corresponding SHA.
        # That is used to allow disconnected installs, where a cluster can't reach external registries
        # in order to translate image tags into something "pullable"
        all_found_operands: Dict[
            str, Tuple[str, str, str]
        ] = {}  # map of image name to (old_pullspec, new_pullspec, nvr)
        for src in operator_bundle_dir.iterdir():
            if src.name == "image-references":
                continue  # skip image-references file
            logger.info(f"Processing {src}...")
            # Read the file content and replace image references
            async with aiofiles.open(src, 'r') as f:
                content = await f.read()
            content, found_images = await self._replace_image_references(
                str(csv_config['registry']), content, operator_build.engine
            )
            for _, (old_pullspec, new_pullspec, operand_nvr) in found_images.items():
                logger.info(f"Replaced image reference {old_pullspec} ({operand_nvr}) by {new_pullspec}")
            all_found_operands.update(found_images)
            # Write the content to the dest bundle directory
            dest = bundle_manifests_dir / src.name
            async with aiofiles.open(dest, 'w') as f:
                if "clusterserviceversion.yaml" in src.name:
                    csv = yaml.safe_load(content)
                    csv['metadata']['annotations']['operators.openshift.io/valid-subscription'] = csv_config[
                        'valid-subscription-label'
                    ]
                    if found_images:
                        csv["spec"]["relatedImages"] = [
                            {"name": name, "image": new_pullspec} for name, (_, new_pullspec, _) in found_images.items()
                        ]
                    content = yaml.safe_dump(csv)
                await f.write(content)

        # Read image references from the operator's image-references file
        image_references = {}
        refs_path = operator_bundle_dir / "image-references"
        if refs_path.exists():
            async with aiofiles.open(refs_path, 'r') as f:
                image_refs = yaml.safe_load(await f.read())
            for entry in image_refs.get('spec', {}).get('tags', []):
                image_references[entry["name"]] = entry
        # Warn if the number of images found in the bundle doesn't match the image-references file
        if len(all_found_operands) != len(image_references):
            logger.warning(
                f"Found {len(all_found_operands)} images in the bundle, but {len(image_references)} in the operator's image-references file"
            )

        # Generate bundle's operator-framework tags
        operator_framework_tags = self._get_operator_framework_tags(channel_name, package_name)

        # Generate bundle's annotations.yaml
        bundle_metadata_dir = bundle_dir / "metadata"
        bundle_metadata_dir.mkdir(parents=True, exist_ok=True)
        dest_annotations_path = bundle_metadata_dir / "annotations.yaml"
        async with aiofiles.open(dest_annotations_path, 'w') as f:
            await f.write(yaml.safe_dump({'annotations': operator_framework_tags}))

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

    @lru_cache
    @staticmethod
    def _get_image_reference_pattern(registry: str):
        """Get a compiled regex pattern to match image references in the format of `registry/namespace/image:tag`."""
        pattern = r'{}\/([^:]+):([^\'"\\\s]+)'.format(re.escape(registry))
        return re.compile(pattern)

    async def _replace_image_references(self, old_registry: str, content: str, engine: Engine):
        """
        Replace image references in the content by their corresponding SHA.
        Returns the content with the replacements and a map of found images in format of {image_name: (old_pullspec, new_pullspec, nvr)}
        """
        new_content = content
        found_images: Dict[str, Tuple[str, str, str]] = {}
        # Find all image references in the content
        pattern = KonfluxOlmBundleRebaser._get_image_reference_pattern(old_registry)
        matches = pattern.finditer(content)
        references = {}  # map of image pullspec to (namespace, image_short_name, image_tag)
        image_info_tasks = []
        for match in matches:
            pullspec = match.group(0)
            namespace, image_short_name = match.group(1).rsplit('/', maxsplit=1)
            image_tag = match.group(2)
            references[pullspec] = (namespace, image_short_name, image_tag)
        # Get image infos for all found images
        for pullspec, (namespace, image_short_name, image_tag) in references.items():
            if engine is Engine.KONFLUX:
                build_pullspec = f"{self.image_repo}:{image_short_name}-{image_tag}"
                image_info_tasks.append(
                    asyncio.create_task(
                        util.oc_image_info_for_arch_async__caching(
                            build_pullspec,
                            registry_config=os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE"),
                        )
                    )
                )
            elif engine is Engine.BREW:
                build_pullspec = (
                    f"{constants.REGISTRY_PROXY_BASE_URL}/rh-osbs/{namespace}-{image_short_name}:{image_tag}"
                )
                image_info_tasks.append(
                    asyncio.create_task(
                        util.oc_image_info_for_arch_async__caching(
                            build_pullspec,
                        )
                    )
                )
        image_infos = await asyncio.gather(*image_info_tasks)

        # Replace image references in the content
        csv_namespace = self._group_config.get('csv_namespace', 'openshift')
        for pullspec, image_info in zip(references, image_infos):
            image_labels = image_info['config']['config']['Labels']
            image_version = image_labels['version']
            image_release = image_labels['release']
            image_component_name = image_labels['com.redhat.component']
            image_nvr = f"{image_component_name}-{image_version}-{image_release}"
            namespace, image_short_name, image_tag = references[pullspec]
            image_sha = (
                image_info['listDigest']
                if self._group_config.operator_image_ref_mode == 'manifest-list'
                else image_info['contentDigest']
            )
            new_namespace = 'openshift4' if namespace == csv_namespace else namespace
            new_pullspec = '{}/{}@{}'.format(
                'registry.redhat.io',  # hardcoded until appregistry is dead
                f'{new_namespace}/{image_short_name}',
                image_sha,
            )
            new_content = new_content.replace(pullspec, new_pullspec)
            found_images[image_short_name] = (pullspec, new_pullspec, image_nvr)
        return new_content, found_images

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
    def __init__(self, message: str, pipelinerun_name: str, pipelinerun: Optional[resource.ResourceInstance]) -> None:
        super().__init__(message)
        self.pipelinerun_name = pipelinerun_name
        self.pipelinerun = pipelinerun


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
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL,
        dry_run: bool = False,
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
        self.pipelinerun_template_url = pipelinerun_template_url
        self.dry_run = dry_run
        self._record_logger = record_logger
        self._logger = logger
        self._konflux_client = KonfluxClient.from_kubeconfig(
            self.konflux_namespace, self.konflux_kubeconfig, self.konflux_context, dry_run=self.dry_run
        )

    async def build(self, metadata: ImageMetadata):
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
            retries = 3
            for attempt in range(retries):
                logger.info("Build attempt %d/%d", attempt + 1, retries)
                pipelinerun, url = await self._start_build(
                    metadata, bundle_build_repo, output_image, self.konflux_namespace, self.skip_checks
                )
                pipelinerun_name = pipelinerun.metadata.name
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
                        pipelinerun,
                        outcome,
                        operator_nvr,
                        operand_nvrs,
                    )
                else:
                    logger.warning("Dry run: Would update Konflux DB for %s with outcome %s", pipelinerun_name, outcome)

                # Wait for the PipelineRun to complete
                pipelinerun, pods = await konflux_client.wait_for_pipelinerun(pipelinerun_name, self.konflux_namespace)
                logger.info("PipelineRun %s completed", pipelinerun_name)

                succeeded_condition = artlib_util.KubeCondition.find_condition(pipelinerun, 'Succeeded')
                outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)

                if not self.dry_run:
                    image_pullspec = next(
                        (r['value'] for r in pipelinerun.status.results if r['name'] == 'IMAGE_URL'), None
                    )
                    image_digest = next(
                        (r['value'] for r in pipelinerun.status.results if r['name'] == 'IMAGE_DIGEST'), None
                    )

                    if not (image_pullspec and image_digest):
                        raise ValueError(
                            f"[{metadata.distgit_key}] Could not find expected results in konflux "
                            f"pipelinerun {pipelinerun_name}"
                        )

                    # Sync the bundle to art-images-share
                    await sync_to_quay(f"{image_pullspec.split(':')[0]}@{image_digest}", KONFLUX_ART_IMAGES_SHARE)

                    # Update the Konflux DB with the final outcome
                    await self._update_konflux_db(
                        metadata,
                        bundle_build_repo,
                        package_name,
                        csv_name,
                        pipelinerun,
                        outcome,
                        operator_nvr,
                        operand_nvrs,
                    )
                else:
                    logger.warning("Dry run: Would update Konflux DB for %s with outcome %s", pipelinerun_name, outcome)
                if outcome is not KonfluxBuildOutcome.SUCCESS:
                    error = KonfluxOlmBundleBuildError(
                        f"Konflux bundle image build for {metadata.distgit_key} failed", pipelinerun_name, pipelinerun
                    )
                    logger.error(f"{error}: {url}")
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
        return pipelinerun_name, pipelinerun

    @staticmethod
    def get_application_name(group_name: str):
        # Openshift doesn't allow dots or underscores in any of its fields, so we replace them with dashes
        # "openshift-4.18" -> "openshift-4-18"
        return group_name.replace(".", "-")

    @staticmethod
    def get_component_name(application_name: str, bundle_name: str):
        # Openshift doesn't allow dots or underscores in any of its fields, so we replace them with dashes
        name = f"{application_name}-{bundle_name}".replace(".", "-").replace("_", "-")
        # Remove the 'openshift-' prefix and replace it with 'ose-'
        # 'openshift-4-18-ose-installer-terraform' -> 'ose-4-18-ose-installer-terraform'
        # A component resource name must start with a lower case letter and must be no more than 63 characters long.
        name = f"ose-{name[10:]}" if name.startswith("openshift-") else name
        return name

    @staticmethod
    def get_old_component_name(application_name: str, bundle_name: str):
        # TODO: (2025-Jul-09) remove this once we have new builds using the new component name
        return f"{application_name}-{bundle_name}".replace(".", "-").replace("_", "-")

    @limit_concurrency(limit=constants.MAX_KONFLUX_BUILD_QUEUE_SIZE)
    async def _start_build(
        self,
        metadata: ImageMetadata,
        bundle_build_repo: BuildRepo,
        output_image: str,
        namespace: str,
        skip_checks: bool = False,
        additional_tags: Optional[Sequence[str]] = None,
    ):
        """Start a build with Konflux."""
        if not bundle_build_repo.commit_hash:
            raise IOError("Bundle repository must have a commit to build. Did you rebase?")
        konflux_client = self._konflux_client
        if additional_tags is None:
            additional_tags = []
        target_branch = bundle_build_repo.branch or bundle_build_repo.commit_hash
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        # Ensure the Application resource exists
        app_name = self.get_application_name(metadata.runtime.group)
        logger.info(f"Using Konflux application: {app_name}")
        await konflux_client.ensure_application(name=app_name, display_name=app_name)
        logger.info(f"Konflux application {app_name} created")
        # Ensure the Component resource exists
        bundle_name = metadata.get_olm_bundle_short_name()
        component_name = self.get_component_name(app_name, bundle_name)
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
        pipelinerun = await konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{component_name}-",
            namespace=namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=bundle_build_repo.https_url,
            commit_sha=bundle_build_repo.commit_hash,
            target_branch=target_branch,
            output_image=output_image,
            vm_override={},
            building_arches=["x86_64"],  # We always build bundles on x86_64
            additional_tags=list(additional_tags),
            skip_checks=skip_checks,
            hermetic=True,
            pipelinerun_template_url=self.pipelinerun_template_url,
            artifact_type="operatorbundle",
        )
        url = konflux_client.resource_url(pipelinerun)
        logger.info(f"PipelineRun {pipelinerun.metadata.name} created: {url}")
        return pipelinerun, url

    async def _update_konflux_db(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        bundle_package_name: str,
        bundle_csv_name: str,
        pipelinerun: resource.ResourceInstance,
        outcome: KonfluxBuildOutcome,
        operator_nvr: str,
        operand_nvrs: list[str],
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

            pipelinerun_name = pipelinerun.metadata.name
            build_pipeline_url = KonfluxClient.resource_url(pipelinerun)
            build_component = pipelinerun.metadata.labels.get('appstudio.openshift.io/component')

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
            }

            match outcome:
                case KonfluxBuildOutcome.SUCCESS:
                    # results:
                    # - name: IMAGE_URL
                    #   value: quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:ose-network-metrics-daemon-rhel9-v4.18.0-20241001.151532
                    # - name: IMAGE_DIGEST
                    #   value: sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807

                    image_pullspec = next(
                        (r['value'] for r in pipelinerun.status.results if r['name'] == 'IMAGE_URL'), None
                    )
                    image_digest = next(
                        (r['value'] for r in pipelinerun.status.results if r['name'] == 'IMAGE_DIGEST'), None
                    )

                    if not (image_pullspec and image_digest):
                        raise ValueError(
                            f"[{metadata.distgit_key}] Could not find expected results in konflux "
                            f"pipelinerun {pipelinerun_name}"
                        )

                    start_time = pipelinerun.status.startTime
                    end_time = pipelinerun.status.completionTime

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
                    start_time = pipelinerun.status.startTime
                    end_time = pipelinerun.status.completionTime
                    build_record_params.update(
                        {
                            'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                                tzinfo=timezone.utc
                            ),
                            'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
                        }
                    )

            build_record = KonfluxBundleBuildRecord(**build_record_params)
            db.add_build(build_record)
            logger.info(f'Konflux build info stored successfully with status {outcome}')

        except Exception as err:
            logger.error('Failed writing record to the konflux DB: %s', err)
