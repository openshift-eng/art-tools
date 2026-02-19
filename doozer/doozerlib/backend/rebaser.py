import asyncio
import copy
import hashlib
import io
import json
import logging
import os
import pathlib
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, cast

import aiofiles
import bashlex
import bashlex.errors
import semver
import yaml
from artcommonlib import exectools, release_util
from artcommonlib.build_visibility import BuildVisibility, get_visibility_suffix, is_release_embargoed
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildRecord
from artcommonlib.model import ListModel, Missing, Model
from artcommonlib.telemetry import start_as_current_span_async
from artcommonlib.util import deep_merge, detect_package_managers, is_cachito_enabled
from artcommonlib.variants import BuildVariant
from dockerfile_parse import DockerfileParser
from doozerlib import constants, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.image import ImageMetadata, extract_builder_info_from_pullspec
from doozerlib.lockfile import ArtifactLockfileGenerator, RPMLockfileGenerator
from doozerlib.record_logger import RecordLogger
from doozerlib.repos import Repos
from doozerlib.runtime import Runtime
from doozerlib.source_modifications import SourceModifierFactory
from doozerlib.source_resolver import SourceResolution, SourceResolver
from opentelemetry import trace
from tenacity import retry, stop_after_attempt, wait_fixed

# Product name mapping for CPE labels
CPE_PRODUCT_NAME_MAPPING = {
    'rhmtc': 'rhmt',
    'oadp': 'openshift_api_data_protection',
    'mta': 'migration_toolkit_applications',
    'logging': 'logging',
    'openshift-logging': 'logging',
}

LOGGER = logging.getLogger(__name__)
TRACER = trace.get_tracer(__name__)


CONTAINER_YAML_HEADER = """
# This file is managed by Doozer: https://github.com/openshift-eng/art-tools/tree/main/doozer
# operated by the OpenShift Automated Release Tooling team (#forum-ocp-art on CoreOS Slack).

# Any manual changes will be overwritten by Doozer on the next build.
#
# See https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/odcs_integration_with_osbs
# for more information on maintaining this file and the format and examples

---
"""


# Doozer used to be part of OIT
OIT_COMMENT_PREFIX = '#oit##'
OIT_BEGIN = '##OIT_BEGIN'
OIT_END = '##OIT_END'


class KonfluxRebaser:
    """Rebase images to a new branch in the build source repository.

    FIXME: Some features that are known to be missing:
    - Handling of alternate upstreams
    - Handling of private fix bit in parent images
    """

    def __init__(
        self,
        runtime: Runtime,
        base_dir: Path,
        source_resolver: SourceResolver,
        repo_type: str,
        upcycle: bool = False,
        force_private_bit: bool = False,
        record_logger: Optional[RecordLogger] = None,
        source_modifier_factory=SourceModifierFactory(),
        logger: Optional[logging.Logger] = None,
        variant: BuildVariant = BuildVariant.OCP,
        image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO,
    ) -> None:
        self._runtime = runtime
        self._base_dir = base_dir
        self._source_resolver = source_resolver
        self.repo_type = repo_type
        self.upcycle = upcycle
        self.force_private_bit = force_private_bit
        self._record_logger = record_logger
        self._source_modifier_factory = source_modifier_factory
        self._logger = logger or LOGGER
        self.rpm_lockfile_generator = RPMLockfileGenerator(runtime.repos, runtime=runtime)
        self.artifact_lockfile_generator = ArtifactLockfileGenerator(runtime=runtime)
        self.image_repo = image_repo
        self.uuid_tag = ''
        self.variant = variant

        self.konflux_db = self._runtime.konflux_db
        if self.konflux_db:
            self.konflux_db.bind(KonfluxBuildRecord)
        if self.variant is BuildVariant.OKD:
            major, minor = runtime.get_major_minor_fields()
            self.group = f'okd-{major}.{minor}'
        else:
            self.group = runtime.group

    @staticmethod
    def construct_dest_branch(group: str, assembly_name: Optional[str], distgit_key: str) -> str:
        """
        Construct the destination branch name for Konflux builds.

        :param group: The group name (e.g., 'openshift-4.17', 'okd-4.17')
        :param assembly_name: The assembly name (e.g., 'stream', '4.17.1'). If None, assemblies are disabled.
        :param distgit_key: The distgit key for the image
        :return: The constructed branch name
        """
        if assembly_name is None:
            # Assemblies are disabled, use simplified format
            return f"art-{group}-dgk-{distgit_key}"
        return f"art-{group}-assembly-{assembly_name}-dgk-{distgit_key}"

    def _get_el_target_string(self, el_version: int) -> str:
        """
        Generate the el_target string based on the build variant.

        Arg(s):
            el_version (int): The EL version number (e.g., 9 for el9/scos9)
        Return Value(s):
            str: The el_target string (e.g., 'scos9' for OKD, 'el9' for OCP)
        """
        prefix = 'scos' if self.variant is BuildVariant.OKD else 'el'
        return f'{prefix}{el_version}'

    @start_as_current_span_async(TRACER, "rebase.rebase_to")
    async def rebase_to(
        self,
        metadata: ImageMetadata,
        version: str,
        input_release: str,
        force_yum_updates: bool,
        commit_message: str,
        push: bool,
    ) -> None:
        # Add business context to span
        current_span = trace.get_current_span()
        current_span.update_name(f"rebase.rebase_to {metadata.distgit_key}")
        current_span.set_attribute("rebase.image_key", metadata.qualified_key)
        current_span.set_attribute("rebase.version", version)
        current_span.set_attribute("rebase.release", input_release)
        current_span.set_attribute("rebase.force_yum_updates", force_yum_updates)
        current_span.set_attribute("rebase.push", push)

        # If there is a konflux stanza in the image config, merge it with the main config
        if metadata.config.konflux is not Missing:
            metadata.config = Model(deep_merge(metadata.config.primitive(), metadata.config.konflux.primitive()))

        try:
            # If this image has an upstream source, resolve it
            if metadata.has_source():
                self._logger.info(f"Resolving source for {metadata.qualified_key}")
                source = cast(
                    SourceResolution, await exectools.to_thread(self._source_resolver.resolve_source, metadata)
                )
            else:
                raise IOError(
                    f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported."
                )

            if self.variant is BuildVariant.OKD:
                major, minor = self._runtime.get_major_minor_fields()
                group = f'okd-{major}.{minor}'
            else:
                group = self._runtime.group

            dest_branch = self.construct_dest_branch(
                group=group,
                assembly_name=self._runtime.assembly,
                distgit_key=metadata.distgit_key,
            )

            self._logger.info(f"Rebasing {metadata.qualified_key} to {dest_branch}")

            dest_dir = self._base_dir.joinpath(metadata.qualified_key)

            # Clone the build repository
            build_repo = BuildRepo(
                url=source.url, branch=dest_branch, local_dir=dest_dir, logger=self._logger, pull_url=source.pull_url
            )
            await build_repo.ensure_source(upcycle=self.upcycle)

            # Rebase the image in the build repository
            self._logger.info("Rebasing image %s to %s in %s...", metadata.distgit_key, dest_branch, dest_dir)
            actual_version, actual_release, _ = await self._rebase_dir(
                metadata,
                source,
                build_repo,
                version,
                input_release,
                force_yum_updates,
            )

            # Commit changes
            await build_repo.commit(commit_message, allow_empty=True, force=True)

            # Tag the commit
            # 1. components should have tagging modes in metadata; tagging_mode:  `disabled | enabled | legacy` .
            # 2. images should default to `disabled` / not tagging the repo (to reduce unnecessary noise)
            # 3. `enabled`, for images, would be the new standard, including `+`.
            # 4. `legacy`, for images, would not include the `+` .
            # 5. rpms components should default to using `legacy`
            tagging_mode = metadata.config.konflux.get(
                "tagging_mode", "disabled"
            )  # For images, default is disabled. i.e. don't tag

            tag = None
            if tagging_mode == "legacy":
                tag = f"{actual_version}-{actual_release}"
            elif tagging_mode == "enabled":
                tag = f"{actual_version}-{actual_release}+{metadata.distgit_key}"

            if tag:
                await build_repo.tag(tag)

            # Push changes
            if push:
                self._logger.info("Pushing changes to %s...", build_repo.url)
                force = (self._runtime.assembly != "stream") or (build_repo.url != build_repo.pull_url)
                await build_repo.push(force=force)

            metadata.rebase_status = True
        finally:
            # notify child images
            metadata.rebase_event.set()

    @start_as_current_span_async(TRACER, "rebase.rebase_dir")
    async def _rebase_dir(
        self,
        metadata: ImageMetadata,
        source: Optional[SourceResolution],
        build_repo: BuildRepo,
        version: str,
        input_release: str,
        force_yum_updates: bool,
    ):
        """
        Rebase the image in the build repository.
        :return: Tuple of version, release, private_fix
        """
        # Add business context to span
        current_span = trace.get_current_span()
        current_span.set_attribute("rebase.image_key", metadata.qualified_key)
        current_span.set_attribute("rebase.version", version)
        current_span.set_attribute("rebase.release", input_release)
        current_span.set_attribute("rebase.has_source", source is not None)

        # Whether or not the source contains private fixes; None means we don't know yet
        private_fix = None
        if self.force_private_bit:  # --embargoed is set, force private_fix to True
            private_fix = True

        dest_dir = build_repo.local_dir
        df_path = dest_dir.joinpath('Dockerfile')
        source_dir = None

        await self._generate_config_digest(metadata, dest_dir)

        # Use a separate Dockerfile for konflux if required
        # For cachito images, we need an override since we now have a separate file for konflux, with cachi2 support
        dockerfile_override = metadata.config.konflux.content.source.dockerfile
        if dockerfile_override and self.variant is not BuildVariant.OKD:
            self._logger.info(f"Override dockerfile for konflux, using: {dockerfile_override}")
            metadata.config.content.source.dockerfile = dockerfile_override

        # If this image has upstream source, merge it into the build repo
        if source:
            source_dir = SourceResolver.get_source_dir(source, metadata)
            await self._merge_source(metadata=metadata, source=source, source_dir=source_dir, dest_dir=dest_dir)

        # Load Dockerfile from the build repo
        dfp = await exectools.to_thread(DockerfileParser, str(df_path))

        # Determine if this image contains private fixes
        if private_fix is None:
            if source and source_dir and source.url == source.pull_url:
                # If the private org branch commit doesn't exist in the public org,
                # this image contains private fixes
                is_commit_in_public_upstream = await util.is_commit_in_public_upstream_async(
                    source.commit_hash, source.public_upstream_branch, source_dir
                )

                if (
                    source.has_public_upstream
                    and not SourceResolver.is_branch_commit_hash(source.public_upstream_branch)
                    and not is_commit_in_public_upstream
                ):
                    private_fix = True
            else:
                # If we don't have upstream source, we need to extract the private_fix_bit from the Dockerfile in the build repo
                _, _, prev_private_fix = self.extract_version_release_private_fix(dfp)
                if prev_private_fix:
                    private_fix = True

        self.uuid_tag = f"{version}-{self._runtime.uuid}"

        # Determine if parent images contain private fixes
        downstream_parents: Optional[List[str]] = None
        if "from" in metadata.config:
            # If this image is FROM another group member, we need to wait on that group
            # member to determine if there is a private fix in it.
            LOGGER.info("Waiting for parent members of %s...", metadata.distgit_key)
            parent_members = await self._wait_for_parent_members(metadata)
            failed_parents = [
                parent.distgit_key for parent in parent_members if parent is not None and not parent.rebase_status
            ]
            if failed_parents:
                raise IOError(
                    f"Couldn't rebase {metadata.distgit_key} because the following parent images failed to rebase: {', '.join(failed_parents)}"
                )
            downstream_parents, parent_private_fix = await self._resolve_parents(metadata, dfp)
            # If any of the parent images are private, this image is private
            if parent_private_fix:
                private_fix = True
            # Replace registry URLs for Konflux
            downstream_parents = [
                image.replace(constants.REGISTRY_PROXY_BASE_URL, constants.BREW_REGISTRY_BASE_URL)
                for image in downstream_parents
            ]

        if private_fix:
            self._logger.warning("The source of image %s contains private fixes.", metadata.distgit_key)
        else:
            private_fix = False  # Didn't find any private fixes in the source or parents

        # Source or not, we should find a Dockerfile in the root at this point or something is wrong
        if not df_path.is_file():
            raise FileNotFoundError("Unable to find Dockerfile in distgit root")

        # If this image defines source modifications, apply them
        if metadata.config.content.source.modifications:
            metadata_scripts_path = os.path.join(self._runtime.data_dir, "modifications")
            await self._run_modifications(
                metadata=metadata,
                dest_dir=dest_dir,
                metadata_scripts_path=metadata_scripts_path,
            )

        # Given an input release string, make an actual release string
        # e.g. 4.17.0-202407241200.p? -> 4.17.0-202407241200.p2.assembly.stream.gdeadbee.el9
        release = self._make_actual_release_string(metadata, input_release, private_fix, source)
        await self._update_build_dir(
            metadata,
            dest_dir,
            source,
            version,
            release,
            downstream_parents,
            force_yum_updates,
        )
        metadata.private_fix = private_fix

        await self._update_dockerignore(build_repo.local_dir)

        if self._runtime.group.startswith("oadp-"):
            await self._remove_oadp_docs(build_repo.local_dir)

        return version, release, private_fix

    @start_as_current_span_async(TRACER, "rebase.remove_oadp_docs")
    async def _remove_oadp_docs(self, path):
        """
        Remove OADP docs from the build directory. They contain example secrets.
        GitHub complains when we are trying to push those secrets, even if they are example ones
        """
        oadp_docs_paths = [
            f"{path}/restic/doc/",  # oadp-velero-container
            f"{path}/velero/restic/doc/",  # oadp-mustgather-container
        ]
        for oadp_docs_path in oadp_docs_paths:
            if os.path.exists(oadp_docs_path):
                self._logger.info(f"Remove OADP doc directory {oadp_docs_path}")
                shutil.rmtree(oadp_docs_path)

    @start_as_current_span_async(TRACER, "rebase.update_dockerignore")
    async def _update_dockerignore(self, path):
        """
        If a .dockerignore file exists, we need to update it to allow .oit dir
        """
        docker_ignore_path = f"{path}/.dockerignore"
        if os.path.exists(docker_ignore_path):
            self._logger.info(f".dockerignore file found at {docker_ignore_path}, adding excludes")
            async with aiofiles.open(docker_ignore_path, "a") as file:
                await file.write("\n!/.oit/**\n")
                await file.write("\n!labels.json\n")

    @start_as_current_span_async(TRACER, "rebase.resolve_parents")
    async def _resolve_parents(self, metadata: ImageMetadata, dfp: DockerfileParser):
        """Resolve the parent images for the given image metadata."""
        image_from = metadata.config.get('from', {})
        parents = image_from.get("builder", []).copy()
        parents.append(image_from)

        if len(parents) != len(dfp.parent_images):
            raise ValueError(
                f"Build metadata for {metadata.distgit_key} expected {len(parents)} parent images, but found {len(dfp.parent_images)} in Dockerfile"
            )

        mapped_images: List[Tuple[str, bool]] = []
        for parent, original_parent in zip(parents, dfp.parent_images):
            if "member" in parent:
                mapped_images.append(await self._resolve_member_parent(parent["member"], original_parent))
            elif "image" in parent:
                mapped_images.append((parent["image"], False))
            elif "stream" in parent:
                mapped_images.append(
                    (await self._resolve_stream_parent(metadata, parent['stream'], original_parent, dfp), False)
                )
            else:
                raise ValueError(f"Image in 'from' for [{metadata.distgit_key}] is missing its definition.")

        downstream_parents = [pullspec for pullspec, _ in mapped_images]
        private_fix = any(private_fix for _, private_fix in mapped_images)
        return downstream_parents, private_fix

    @start_as_current_span_async(TRACER, "rebase.resolve_member_parent")
    async def _resolve_member_parent(self, member: str, original_parent: str):
        """Resolve the parent image for the given image metadata."""
        parent_metadata: ImageMetadata = self._runtime.resolve_image(member, required=False)

        if parent_metadata is None:
            parent_loaded = False
            parent_metadata = self._runtime.late_resolve_image(member, required=False)

        else:
            parent_loaded = True

        if self.variant is BuildVariant.OKD:
            okd_alignment_config = parent_metadata.config.content.source.okd_alignment
            if okd_alignment_config.resolve_as.stream:
                stream_config = self._runtime.resolve_stream(okd_alignment_config.resolve_as.stream)
                return stream_config.image, False

        private_fix = False
        if not parent_loaded:
            if not self._runtime.ignore_missing_base:
                raise IOError(f"Parent image {member} is not loaded.")
            if self._runtime.latest_parent_version or self._runtime.assembly_basis_event:
                parent_metadata = self._runtime.late_resolve_image(member)
                if not parent_metadata:
                    raise IOError(f"Metadata config for parent image {member} is not found.")

                build = await parent_metadata.get_latest_build(
                    el_target=self._get_el_target_string(parent_metadata.branch_el_target()),
                    engine=Engine.KONFLUX,
                    group=self.group,
                    exclude_large_columns=True,
                )

                if not build:
                    raise IOError(f"A build of parent image {member} is not found.")
                return build.image_pullspec, build.embargoed

            return original_parent, False
        else:
            if not self.image_repo:
                raise ValueError("image_repo must be set to resolve member parents.")

            if parent_metadata.private_fix is None:
                raise IOError(
                    f"Parent image {member} doesn't have .p? flag determined. "
                    "This indicates a bug in Doozer. Please report this issue.",
                )
            private_fix = parent_metadata.private_fix
            return f"{self.image_repo}:{parent_metadata.image_name_short}-{self.uuid_tag}", private_fix

    @start_as_current_span_async(TRACER, "rebase.resolve_stream_parent")
    async def _resolve_stream_parent(
        self, metadata: ImageMetadata, stream_name: str, original_parent: str, dfp: DockerfileParser
    ):
        stream_image = None
        if metadata.canonical_builders_enabled:
            # canonical_builders_from_upstream is enabled: try matching upstream golang builders
            stream_image = await self._resolve_image_from_upstream_parent(original_parent, dfp)

        # do typical stream resolution
        if not stream_image:
            stream = self._runtime.resolve_stream(stream_name)
            stream_image = str(stream.image)

        # For OKD variant, return stream image as-is without transformation
        if self.variant is BuildVariant.OKD:
            return stream_image

        # Transform the stream pullspec to brew registry format
        return self._transform_stream_pullspec(stream_image)

    @start_as_current_span_async(TRACER, "rebase.wait_for_parent_members")
    async def _wait_for_parent_members(self, metadata: ImageMetadata):
        # If this image is FROM another group member, we need to wait on that group
        # member before determining if there is a private fix in it.
        parent_members = list(metadata.get_parent_members().values())
        for parent_member in parent_members:
            if parent_member is None:
                continue  # Parent member is not included in the group; no need to wait
            # wait for parent member to be rebased
            while not parent_member.rebase_event.is_set():
                self._logger.info(
                    "[%s] Parent image %s is being rebased; waiting...",
                    metadata.distgit_key,
                    parent_member.distgit_key,
                )
                # parent_member.rebase_event.wait() shouldn't be used here because it will block the event loop
                await asyncio.sleep(10)  # check every 10 seconds
        return parent_members

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))
    @start_as_current_span_async(TRACER, "rebase.recursive_overwrite")
    async def _recursive_overwrite(self, src, dest, ignore=set()):
        """
        Use rsync to copy one file tree to a new location
        """

        self._logger.info('Copying files from %s to %s', src, dest)
        exclude = ' --exclude .git '
        for i in ignore:
            exclude += ' --exclude="{}" '.format(i)
        cmd = 'rsync -av {} {}/ {}/'.format(exclude, src, dest)
        await exectools.cmd_assert_async(cmd, suppress_output=True)

    @start_as_current_span_async(TRACER, "rebase.merge_source")
    async def _merge_source(self, metadata: ImageMetadata, source: SourceResolution, source_dir: Path, dest_dir: Path):
        """
        Pulls source defined in content.source and overwrites most things in the distgit
        clone with content from that source.
        """

        # Initialize env_vars_from source.
        # update_distgit_dir makes a distinction between None and {}
        env_vars_from_source = {}
        with exectools.Dir(source_dir):
            env_vars_from_source.update(metadata.extract_kube_env_vars())

            # See if the config is telling us a file other than "Dockerfile" defines the
            # distgit image content.
            dockerfile_name = str(metadata.config.content.source.dockerfile or "Dockerfile")

        # The path to the source Dockerfile we are reconciling against.
        source_dockerfile_path = os.path.join(source_dir, dockerfile_name)

        # Clean up any files not special to the distgit repo
        BASE_IGNORE = [".git", ".oit"]
        ignore_list = BASE_IGNORE
        # ignore_list.extend(self._runtime.group_config.get('dist_git_ignore', []))
        # ignore_list.extend(metadata.config.get('dist_git_ignore', []))

        for ent in dest_dir.iterdir():
            if ent.name in ignore_list:
                continue

            # Otherwise, clean up the entry
            if ent.is_file() or ent.is_symlink():
                ent.unlink()
            else:
                await exectools.to_thread(shutil.rmtree, str(ent.resolve()))

        # Copy all files and overwrite where necessary
        await self._recursive_overwrite(source_dir, dest_dir)

        df_path = dest_dir.joinpath('Dockerfile')

        if df_path.exists():
            # The w+ below will not overwrite a symlink file with real content (it will
            # be directed to the target file). So unlink explicitly.
            df_path.unlink()

        # The source Dockerfile could be named virtually anything (e.g. Dockerfile.rhel) or
        # be a symlink. Ultimately, we don't care - we just need its content in distgit
        # as /Dockerfile (which OSBS requires). Read in the content and write it back out
        # to the required distgit location.
        async with aiofiles.open(source_dockerfile_path, mode='r', encoding='utf-8') as source_dockerfile:
            source_dockerfile_content = await source_dockerfile.read()

        async with aiofiles.open(df_path, mode='w+', encoding='utf-8') as distgit_dockerfile:
            await distgit_dockerfile.write(source_dockerfile_content)

        gomod_path = dest_dir.joinpath('go.mod')
        if gomod_path.exists():
            # Read the gomod contents
            new_lines = []
            with open(gomod_path, "r") as file:
                lines = file.readlines()
                for line in lines:
                    stripped_line = line.strip()
                    match = re.match(r"(^go \d\.\d+$)", stripped_line)
                    if match:
                        # Append a .0 to the go mod version, if it exists
                        # Replace the line 'go 1.22' with 'go 1.22.0' for example
                        go_version_string = match.group(1)  # eg. 'go 1.23'
                        version_part = go_version_string.split(" ")[-1]  # eg. '1.23'

                        # Use semver for reliable version comparison
                        try:
                            # Convert to semver format (add .0 patch version for comparison)
                            current_version = semver.VersionInfo.parse(f"{version_part}.0")
                            min_version = semver.VersionInfo.parse("1.22.0")

                            if current_version >= min_version:
                                self._logger.info(f"Missing patch in golang version: {stripped_line}. Appending .0")
                                stripped_line = stripped_line.replace(go_version_string, f"{go_version_string}.0")
                                new_lines.append(f"{stripped_line}\n")
                                continue
                        except ValueError as e:
                            self._logger.warning(
                                f"Failed to parse go version '{version_part}': {e}. Skipping version check."
                            )
                            # Fall back to original behavior if version parsing fails
                            new_lines.append(line)
                            continue

                    # If there is no match or if the go version is not >= 1.22, use the same go version
                    new_lines.append(line)
            with open(gomod_path, "w") as file:
                file.writelines(new_lines)

        # Clean up any extraneous Dockerfile.* that might be distractions (e.g. Dockerfile.centos)
        for ent in dest_dir.iterdir():
            if ent.name.startswith("Dockerfile."):
                ent.unlink()

        # Workaround for https://issues.redhat.com/browse/STONEBLD-1929
        containerfile = dest_dir.joinpath('Containerfile')
        if containerfile.is_file():
            containerfile.unlink()

        owners = []
        if metadata.config.owners is not Missing and isinstance(metadata.config.owners, list):
            owners = list(metadata.config.owners)

        dockerfile_notify = False

        # Create a sha for Dockerfile. We use this to determine if we've reconciled it before.
        source_dockerfile_hash = hashlib.sha256(open(source_dockerfile_path, 'rb').read()).hexdigest()

        reconciled_path = dest_dir.joinpath('.oit', 'reconciled')
        util.mkdirs(reconciled_path)
        reconciled_df_path = reconciled_path.joinpath(f'{source_dockerfile_hash}.Dockerfile')

        # If the file does not exist, the source file has not been reconciled before.
        if not reconciled_df_path.is_file():
            # Something has changed about the file in source control
            dockerfile_notify = True
            # Record that we've reconciled against this source file so that we do not notify the owner again.
            shutil.copy(str(source_dockerfile_path), str(reconciled_df_path))

        if dockerfile_notify:
            # Leave a record for external processes that owners will need to be notified.
            with exectools.Dir(source_dir) as curdir:
                author_email = None
                err = None
                rc, sha, err = await exectools.cmd_gather_async(
                    # --no-merges because the merge bot is not the real author
                    # --diff-filter=a to omit the "first" commit in a shallow clone which may not be the author
                    #   (though this means when the only commit is the initial add, that is omitted)
                    f'git -C {curdir} log --no-merges --diff-filter=a -n 1 --pretty=format:%H {dockerfile_name}',
                )
                if rc == 0:
                    rc, ae, err = await exectools.cmd_gather_async(f"git -C {curdir} show -s --pretty=format:%ae {sha}")
                    if rc == 0:
                        if ae.lower().endswith('@redhat.com'):
                            self._logger.info('Last Dockerfile committer: {}'.format(ae))
                            author_email = ae
                        else:
                            err = 'Last committer email found, but is not @redhat.com address: {}'.format(ae)
                if err:
                    self._logger.info('Unable to get author email for last {} commit: {}'.format(dockerfile_name, err))

            if author_email:
                owners.append(author_email)

            sub_path = metadata.config.content.source.path
            if not sub_path:
                source_dockerfile_subpath = dockerfile_name
            else:
                source_dockerfile_subpath = "{}/{}".format(sub_path, dockerfile_name)
            # there ought to be a better way to determine the source alias that was registered:
            source_root = self._source_resolver.resolve_source(metadata).source_path
            source_alias = metadata.config.content.source.get('alias', os.path.basename(source_root))

            if self._record_logger:
                self._record_logger.add_record(
                    "dockerfile_notify",
                    distgit=metadata.qualified_name,
                    image=metadata.config.name,
                    owners=','.join(owners),
                    source_alias=source_alias,
                    source_dockerfile_subpath=source_dockerfile_subpath,
                    dockerfile=str(dest_dir.joinpath('Dockerfile')),
                )

    @staticmethod
    def extract_version_release_private_fix(
        dfp: DockerfileParser,
    ) -> Tuple[Optional[str], Optional[str], Optional[bool]]:
        """
        Extract version, release, and private_fix fields from Dockerfile.

        :param dfp: DockerfileParser object
        :return: Tuple of version, release, and private_fix
        """
        # extract previous release to enable incrementing it
        prev_release = dfp.labels.get("release")
        private_fix = None
        if prev_release:
            private_fix = is_release_embargoed(prev_release, 'konflux')
        version = dfp.labels.get("version")
        return version, prev_release, private_fix

    @start_as_current_span_async(TRACER, "rebase.run_modifications")
    async def _run_modifications(self, metadata: ImageMetadata, dest_dir: Path, metadata_scripts_path: Path):
        """
        Interprets and applies content.source.modifications steps in the image metadata.
        """
        df_path = dest_dir.joinpath('Dockerfile')
        async with aiofiles.open(df_path, 'r', encoding='utf-8') as df:
            dockerfile_data = await df.read()

        self._logger.debug("About to start modifying Dockerfile [%s]:\n%s\n" % (metadata.distgit_key, dockerfile_data))

        # add build data modifications dir to path; we *could* add more
        # specific paths for the group and the individual config but
        # expect most scripts to apply across multiple groups.
        # metadata_scripts_path = self._runtime.data_dir + "/modifications"
        path = os.pathsep.join([os.environ['PATH'], str(metadata_scripts_path)])
        new_dockerfile_data = dockerfile_data

        for modification in metadata.config.content.source.modifications:
            if self._source_modifier_factory.supports(modification.action):
                # run additional modifications supported by source_modifier_factory
                modifier = self._source_modifier_factory.create(**modification, distgit_path=str(dest_dir))
                # pass context as a dict so that the act function can modify its content
                # FIXME: Some env vars are not set here
                context = {
                    "component_name": metadata.distgit_key,
                    "kind": "Dockerfile",
                    "content": new_dockerfile_data,
                    "build_system": self._runtime.build_system,
                    "set_env": {
                        "PATH": path,
                        # "BREW_EVENT": f'{self._runtime.brew_event}',
                        # "BREW_TAG": f'{metadata.candidate_brew_tag()}'
                    },
                    # "distgit_path": str(dest_dir),
                }
                modifier.act(context=context, ceiling_dir=str(dest_dir))
                new_dockerfile_data = context.get("result", new_dockerfile_data)
            else:
                raise IOError("Don't know how to perform modification action: %s" % modification.action)

        if new_dockerfile_data is not None and new_dockerfile_data != dockerfile_data:
            async with aiofiles.open(df_path, 'w', encoding='utf-8') as df:
                await df.write(new_dockerfile_data)

    @start_as_current_span_async(TRACER, "rebase.update_build_dir")
    async def _update_build_dir(
        self,
        metadata: ImageMetadata,
        dest_dir: Path,
        source: Optional[SourceResolution],
        version: str,
        release: str,
        downstream_parents: Optional[List[str]],
        force_yum_updates: bool,
    ):
        with exectools.Dir(dest_dir):
            await self._generate_repo_conf(metadata, dest_dir, self._runtime.repos)

            await self._write_cvp_owners(metadata, dest_dir)

            await self._write_fetch_artifacts(metadata, dest_dir)

            await self._write_osbs_image_config(metadata, dest_dir, source, version)

            await self._write_rpms_lock_file(metadata, dest_dir)

            await self._write_artifacts_lock_file(metadata, dest_dir)

            df_path = dest_dir.joinpath('Dockerfile')
            await self._update_dockerfile(
                metadata,
                source,
                df_path,
                version,
                release,
                downstream_parents,
                force_yum_updates,
                dest_dir,
            )

            await self._update_csv(metadata, dest_dir, version, release)

            return version, release

    async def _write_rpms_lock_file(self, metadata: ImageMetadata, dest_dir: Path):
        if not metadata.is_lockfile_generation_enabled():
            self._logger.debug(f'RPM lockfile generation is disabled for {metadata.distgit_key}')
            return

        self._logger.info(f'Generating RPM lockfile for {metadata.distgit_key}')

        await self.rpm_lockfile_generator.generate_lockfile(metadata, dest_dir)

    async def _write_artifacts_lock_file(self, metadata: ImageMetadata, dest_dir: Path):
        if not metadata.is_artifact_lockfile_enabled():
            self._logger.debug(f'Artifact lockfile generation is disabled for {metadata.distgit_key}')
            return

        self._logger.info(f'Generating artifact lockfile for {metadata.distgit_key}')

        await self.artifact_lockfile_generator.generate_artifact_lockfile(metadata, dest_dir)

    def _make_actual_release_string(
        self, metadata: ImageMetadata, input_release: str, private_fix: bool, source: Optional[SourceResolution]
    ) -> str:
        """Given a input_release string (may contain .p?), make an actual release string.

        e.g. 4.17.0-202407241200.p? -> 4.17.0-202407241200.p0.assembly.stream.gdeadbee.el9 (OCP)
             4.17.0-202407241200.p? -> 4.17.0-202407241200.p0.assembly.stream.gdeadbee.scos9 (OKD)
        """
        sb = io.StringIO()
        if input_release.endswith(".p?"):
            sb.write(input_release[:-3])  # strip .p?
            visibility = BuildVisibility.PRIVATE if private_fix else BuildVisibility.PUBLIC
            pval = get_visibility_suffix('konflux', visibility)
            sb.write(f'.{pval}')
        else:
            if self._runtime.group_config.public_upstreams:
                raise ValueError(
                    f"'release' must end with '.p?' for an image with a public upstream but its actual value is {input_release}"
                )
            sb.write(input_release)

        if source and source.commit_hash:
            sb.write(".g")
            sb.write(source.commit_hash_short)

        if self._runtime.assembly:
            sb.write(".assembly.")
            sb.write(self._runtime.assembly)

        el_ver = 0
        try:
            el_ver = metadata.branch_el_target()
        except ValueError:
            pass
        if el_ver:
            if self.variant is BuildVariant.OKD:
                sb.write(".scos")
            else:
                sb.write(".el")
            sb.write(str(el_ver))
        return sb.getvalue()

    source_labels = dict(
        old=dict(
            sha='io.openshift.source-repo-commit',
            source='io.openshift.source-repo-url',
            source_commit='io.openshift.source-commit-url',
        ),
        now=dict(
            sha='io.openshift.build.commit.id',
            source='io.openshift.build.source-location',
            source_commit='io.openshift.build.commit.url',
        ),
    )

    @staticmethod
    def split_dockerfile_into_stages(dfp):
        df_stages = []
        df_stage = []

        for df_line in json.loads(dfp.json):
            if "FROM" in df_line.keys():
                if df_stage:
                    df_stages.append(df_stage)
                df_stage = [df_line]
                continue
            df_stage.append(df_line)
        df_stages.append(df_stage)
        return df_stages

    @staticmethod
    def _transform_stream_pullspec(pullspec: str) -> str:
        """
        Transform a stream image pullspec to the appropriate brew registry format.

        For pullspecs starting with "openshift/" (without full registry URL),
        prepend the brew registry URL and replace the namespace:
        - "openshift/golang-builder:..." -> "brew.registry.redhat.io/rh-osbs/openshift-golang-builder:..."
        - "openshift/foo:..." -> "brew.registry.redhat.io/rh-osbs/openshift-foo:..."

        Args:
            pullspec: The stream image pullspec

        Returns:
            Transformed pullspec with full brew registry URL
        """
        if pullspec.startswith("openshift/"):
            # Prepend brew registry and replace openshift/ with rh-osbs/openshift-
            return f"{constants.BREW_REGISTRY_BASE_URL}/{pullspec.replace('openshift/', 'rh-osbs/openshift-')}"
        return pullspec

    @start_as_current_span_async(TRACER, "rebase.update_dockerfile")
    async def _update_dockerfile(
        self,
        metadata: ImageMetadata,
        source: Optional[SourceResolution],
        df_path: Path,
        version: str,
        release: str,
        downstream_parents: Optional[List[str]],
        force_yum_updates: bool,
        dest_dir: Path,
    ):
        """Update the Dockerfile in the build repo with the correct labels and version information."""
        dfp = DockerfileParser(str(df_path))
        self._clean_repos(dfp)

        # Split the version number v4.3.4 => [ 'v4', '3, '4' ]
        vsplit = version.split(".")

        major_version = vsplit[0].lstrip('v')
        # Click validation should have ensured user specified semver, but double check because of version=None flow.
        minor_version = '0' if len(vsplit) < 2 else vsplit[1]
        patch_version = '0' if len(vsplit) < 3 else vsplit[2]

        self._logger.debug("Dockerfile contains the following labels:")
        for k, v in dfp.labels.items():
            self._logger.debug("  '%s'='%s'" % (k, v))

        # Set all labels in from config into the Dockerfile content
        if metadata.config.labels is not Missing:
            for k, v in metadata.config.labels.items():
                dfp.labels[k] = str(v)

        # Set the image name
        dfp.labels["name"] = metadata.config.name

        # The vendor should always be Red Hat, Inc.
        dfp.labels["vendor"] = "Red Hat, Inc."

        # "v4.20.0" -> "4.20"
        cleaned_version = version.lstrip('v').rsplit('.', 1)[0]
        # "202509030239.p2.gfe588cb.assembly.stream.el9" -> "el9"
        rhel_version = release.split(".")[-1]
        product = self._runtime.group_config.product if self._runtime.group_config.product else "openshift"
        # Apply product name mapping for CPE labels
        cpe_product_name = CPE_PRODUCT_NAME_MAPPING.get(product, product)
        dfp.labels["cpe"] = f"cpe:/a:redhat:{cpe_product_name}:{cleaned_version}::{rhel_version}"

        # Set the distgit repo name
        dfp.labels["com.redhat.component"] = metadata.get_component_name()

        # appregistry is managed in a separately-built metadata container (ref. ART-874)
        if "com.redhat.delivery.appregistry" in dfp.labels:
            dfp.labels["com.redhat.delivery.appregistry"] = "False"

        jira_project, jira_component = metadata.get_jira_info()
        dfp.labels['io.openshift.maintainer.project'] = jira_project
        dfp.labels['io.openshift.maintainer.component'] = jira_component

        if 'from' in metadata.config and downstream_parents is not None:
            dfp.parent_images = downstream_parents.copy()

        # Set image name in case it has changed
        dfp.labels["name"] = metadata.config.name

        # Set version and release labels
        dfp.labels['version'] = version
        dfp.labels['release'] = release

        # log nvr
        self._logger.info(f"nvr={metadata.get_component_name()}-{version}-{release}")

        # Delete differently cased labels that we override or use newer versions of
        for deprecated in ["Release", "Architecture", "BZComponent"]:
            if deprecated in dfp.labels:
                del dfp.labels[deprecated]

        # remove old labels from dist-git
        for _, label in self.source_labels['old'].items():
            if label in dfp.labels:
                del dfp.labels[label]

        # set with new source if known, otherwise leave alone for a refresh
        srclab = self.source_labels['now']
        if source:
            dfp.labels[srclab['sha']] = source.commit_hash
            if source.public_upstream_url:
                dfp.labels[srclab['source']] = source.public_upstream_url
                dfp.labels[srclab['source_commit']] = '{}/commit/{}'.format(
                    source.public_upstream_url, source.commit_hash
                )

        # Remove any programmatic oit comments from previous management
        df_lines = dfp.content.splitlines(False)
        df_lines = [line for line in df_lines if not line.strip().startswith(OIT_COMMENT_PREFIX)]

        filtered_content = []
        in_mod_block = False
        for line in df_lines:
            # Check for begin/end of mod block, skip any lines inside
            if OIT_BEGIN in line:
                in_mod_block = True
                continue
            elif OIT_END in line:
                in_mod_block = False
                continue

            # if in mod, skip all
            if in_mod_block:
                continue

            if line.endswith('\n'):
                line = line[0:-1]  # remove trailing newline, if exists
            filtered_content.append(line)

        df_lines = filtered_content

        # ART-8476 assert rhel version equivalence
        if metadata.canonical_builders_enabled and self.variant is not BuildVariant.OKD:
            el_version = metadata.branch_el_target()
            df_lines.extend(
                [
                    '',
                    '# RHEL version in final image must match the one in ART\'s config',
                    f'RUN source /etc/os-release && [ "$PLATFORM_ID" == platform:el{el_version} ]',
                ]
            )

        df_content = "\n".join(df_lines)

        if release:
            release_suffix = f'-{release}'
        else:
            release_suffix = ''

        # Environment variables that will be injected into the Dockerfile
        # unless content.set_build_variables=False
        build_update_env_vars = {  # Set A
            'OS_GIT_MAJOR': major_version,
            'OS_GIT_MINOR': minor_version,
            'OS_GIT_PATCH': patch_version,
            'OS_GIT_VERSION': f'{major_version}.{minor_version}.{patch_version}{release_suffix}',
            'OS_GIT_TREE_STATE': 'clean',
            'SOURCE_GIT_TREE_STATE': 'clean',
            'BUILD_VERSION': version,
            'BUILD_RELEASE': release if release else '',
        }

        # Unlike update_env_vars (which can be disabled in metadata with content.set_build_variables=False),
        # metadata_envs are always injected into doozer builds.
        metadata_envs: Dict[str, str] = {
            '__doozer_group': self._runtime.group,
            '__doozer_key': metadata.distgit_key,
            '__doozer_version': version,  # Useful when build variables are not being injected, but we still need "version" during the build.
            '__doozer_uuid_tag': f"{metadata.image_name_short}-{self.uuid_tag}",
        }
        if self.variant is BuildVariant.OKD:
            metadata_envs['TAGS'] = 'scos'

        if metadata.config.envs:
            # Allow environment variables to be specified in the ART image metadata
            metadata_envs.update(metadata.config.envs.primitive())

        df_fileobj = self._update_yum_update_commands(metadata, force_yum_updates, io.StringIO(df_content))
        with Path(dfp.dockerfile_path).open('w', encoding="utf-8") as df:
            shutil.copyfileobj(df_fileobj, df)
            df_fileobj.close()

        await self._update_environment_variables(
            metadata, source, df_path, build_update_envs=build_update_env_vars, metadata_envs=metadata_envs
        )

        # Inject build repos for Konflux
        self._add_build_repos(dfp, metadata, dest_dir)

        self._modify_cachito_commands(metadata, dfp)
        if self.variant is BuildVariant.OKD:
            self._apply_okd_labels(dfp)

        await self._reflow_labels(df_path)

    def _apply_okd_labels(self, dfp):
        """
        If rebasing for OKD, remove all unnecessary labels
        """

        self._logger.info('Deleting unneeded labels fo OKD')
        unneeded_labels = ['cpe', 'io.openshift.maintainer.project']
        for label in unneeded_labels:
            del dfp.labels[label]

    def _find_matching_artifact(self, metadata: ImageMetadata, url_pattern: str) -> Optional[str]:
        """Find artifact resource matching URL pattern."""
        if not metadata.is_artifact_lockfile_enabled():
            return None

        required_artifacts = metadata.get_required_artifacts()
        for artifact in required_artifacts:
            artifact_url = artifact['url']
            if url_pattern.lower() in artifact_url.lower():
                return artifact_url
        return None

    def _validate_required_artifacts(
        self, metadata: ImageMetadata, network_mode: str, artifact_type: str, url_pattern: str
    ) -> Optional[str]:
        """Generic validation for required artifacts before Dockerfile modification."""
        if network_mode != "hermetic":
            return None

        matching_artifact = self._find_matching_artifact(metadata, url_pattern)
        if not matching_artifact:
            from doozerlib.exceptions import DoozerFatalError

            raise DoozerFatalError(
                f"Hermetic build requires {artifact_type} artifact definition in image metadata. "
                f"Add {artifact_type} resource URL to konflux.cachi2.artifact_lockfile.resources in {metadata.distgit_key}. "
                f"Expected URL pattern: {url_pattern}"
            )

        self._logger.info(f"Found required {artifact_type} artifact: {matching_artifact}")
        return matching_artifact

    def _get_module_enablement_commands(self, metadata: ImageMetadata) -> List[str]:
        """
        Generate DNF module enable commands for RHEL 9+ images with lockfile modules.

        Returns:
            List[str]: List of RUN commands to enable modules, or empty list if no modules needed
        """
        if not metadata.is_lockfile_generation_enabled():
            return []

        # Check if DNF module enablement is disabled
        if not metadata.is_dnf_modules_enable_enabled():
            self._logger.info(f"DNF module enablement disabled for {metadata.distgit_key}")
            return []

        try:
            el_ver = metadata.branch_el_target()
            if el_ver < 9:
                return []
        except ValueError:
            return []

        modules_to_install = metadata.get_lockfile_modules_to_install()
        if not modules_to_install:
            return []

        modules_list = ' '.join(sorted(modules_to_install))
        self._logger.info(f"Enabling modules for {metadata.distgit_key}: {modules_list}")

        return [
            f"RUN dnf module enable -y {modules_list}",
        ]

    def _add_build_repos(self, dfp: DockerfileParser, metadata: ImageMetadata, dest_dir: Path):
        # Populating the repo file needs to happen after every FROM before the original Dockerfile can invoke yum/dnf.
        network_mode = metadata.get_konflux_network_mode()

        konflux_lines = ["\n# Start Konflux-specific steps"]

        # Set ENV variables for cachi2 configuration
        # Ref policy doc: https://docs.google.com/document/d/1Ajc6MSNJz20b34L7QNwoPcWKxId7Fy-eRt7oqyaJl8c
        # ART_BUILD_ENGINE=brew|konflux
        # ART_BUILD_DEPS_METHOD=cachito|cachi2
        # ART_BUILD_DEPS_MODE=default|cachito-emulation
        # ART_BUILD_NETWORK=hermetic|internal-only|open for konflux | ART_BUILD_NETWORK=internal-only for brew
        konflux_lines += [
            "ENV ART_BUILD_ENGINE=konflux",
            "ENV ART_BUILD_DEPS_METHOD=cachi2",
            f"ENV ART_BUILD_NETWORK={network_mode}",
        ]

        # A current cachi2 issue allows cached go artifacts to persist through image build stages.
        # This was detected when one builder stage was rhel8 and another rhel9, leaving rhel8
        # files in the cache, and causing the rhel9 go build to make inappropriate decisions.
        # As a temporary guard against this cache pollution, clean the cache after every stage.
        # Use || true to prevent an error if this not a builder stage.
        # Can be disabled via konflux.no_shell for build stages without /bin/sh
        no_shell = metadata.config.konflux.get("no_shell", False)
        if not no_shell:
            konflux_lines.append("RUN go clean -cache || true")

        # Three modes for handling upstreams depending on old
        # cachito functionality
        # 1. default (no cachito mode specified) - Do nothing special for the Dockerfile. This may be appropriate for Dockerfiles
        #    well enough to handle cachito & non-cachito environments already.
        # 2. "cachito-emulation" - inject cachito-like environment variables & resources into Dockerfile.
        # 3. "cachito-removal" - Comment out lines referencing REMOTE_SOURCES.
        cachito_mode = metadata.config.konflux.cachito.mode
        if cachito_mode is Missing:
            build_deps_mode = 'default'
        elif cachito_mode in ['emulation', 'removal']:
            build_deps_mode = f'cachito-{metadata.config.konflux.cachito.mode}'
        else:
            raise IOError(f'Unexpected konflux.cachito.mode: {cachito_mode}')

        if metadata.config.konflux.cachito.mode == 'emulation':
            # In cachito emulation mode, we make allowances for the upstream
            # Dockerfile to have references to cachito env vars that no longer
            # exist in cachi2 builds.
            # The most important env vars are "REMOTE_SOURCES" and "REMOTE_SOURCES_DIR"
            # which cachito injects and are used in Dockerfile commands like
            # COPY "$REMOTE_SOURCES" "$REMOTE_SOURCES_DIR"
            # RUN source "${REMOTE_SOURCES_DIR}/cachito-gomod-with-deps/cachito.env" && make
            # Our emulation mode ensures that these env vars are defined
            # and both the COPY and "source" command will not fail.
            # This may not satisfy all builds cachito expectations, but can help
            # folks write Dockerfiles that can be built with cachito and cachi2.

            # Here is an example Dockerfile snippet we are trying to satisfy. Notice
            # the special directory structures and files (.npmc, registry-ca.pem, cachito.env).
            #   RUN test -d ${REMOTE_SOURCES_DIR}/cachito-gomod-with-deps || exit 1; \
            #       cp -f $REMOTE_SOURCES_DIR/cachito-gomod-with-deps/app/registry-ca.pem . \
            #       && cp -f $REMOTE_SOURCES_DIR/cachito-gomod-with-deps/app/web/{.npmrc,package-lock.json} web/ \
            #       && source ${REMOTE_SOURCES_DIR}/cachito-gomod-with-deps/cachito.env \
            #       RUN make install-frontend-ci && make build-frontend

            # The directory will serve as REMOTE_SOURCES content in the rebased
            # upstream repository.
            # i.e. COPY $REMOTE_SOURCES $REMOTE_SOURCES_DIR will resolve as
            #      COPY cachito-emulation $REMOTE_SOURCES_DIR
            # During the rebase, populate this directory in the git repo with
            # files & directories cachito users expect to find.
            emulation_dir = 'cachito-emulation'

            # Create a path to the location where files that will
            # populate REMOTE_SOURCES should be written.
            emulation_path = pathlib.Path(dest_dir).joinpath(emulation_dir)

            # Cachito creates a directory under REMOTE_SOURCES called app
            # where source code would be extracted. We don't reproduce
            # this behavior, but we do at least ensure the directory exists
            # so that Dockerfiles can safely reference it.
            app_path = emulation_path.joinpath('app')
            app_path.mkdir(parents=True, exist_ok=True)
            # A file must exist in the dir for it to be established in git.
            # Create a stub file.
            app_path.joinpath('.dir').touch(exist_ok=True)

            # Cachito requires Dockerfiles to source
            # ${REMOTE_SOURCES_DIR}/cachito-gomod-with-deps/cachito.env . We don't populate
            # any env vars here, but we do make it safe to source (i.e. an empty file)
            gomod_deps_path = emulation_path.joinpath('cachito-gomod-with-deps')
            gomod_deps_path.mkdir(parents=True, exist_ok=True)
            # Now ensure the cachito.env file exists in REMOTE_SOURCES so it will
            # be copied into REMOTE_SOURCES_DIR
            gomod_deps_path.joinpath('cachito.env').touch(exist_ok=True)

            # The value we will set REMOTE_SOURCES_DIR to.
            remote_source_dir_env = '/tmp/art/cachito-emulation'
            pkg_managers = []
            if metadata.config.content.source.pkg_managers is not Missing:
                pkg_managers = metadata.config.content.source.pkg_managers.primitive()
            if "npm" in pkg_managers:
                flag = False
                for npm_entry in metadata.config.cachito.packages.npm:
                    # cachito creates an .npmrc file in the "npm" package manager paths. This
                    # used to cause NPM to use an NPM server, supplied by cachito,
                    # which only supplied the component's NPM deps.
                    # See https://github.com/containerbuildsystem/cachito/blob/fe2ef761d7d8169597166e9f92ee8ef3d62a091c/README.md .
                    # Dockerfiles may reference this file. We create it in emulation mode, but do
                    # not populate it. cachi2 works fundamentally differently
                    # by re-writing package-lock.json to reference local files.
                    # https://github.com/hermetoproject/cachi2/blob/2235854bfc5cb76ea168cb04d50be16efce9fa0a/docs/npm.md
                    app_path = gomod_deps_path.joinpath('app').joinpath(npm_entry.get('path', '.'))
                    app_path.mkdir(parents=True, exist_ok=True)
                    app_path.joinpath('.npmrc').touch(exist_ok=True)
                    flag = True

                if not flag:
                    # In some cases, in image config, metadata.config.content.source.pkg_managers will be populated
                    # ref: https://github.com/openshift-eng/art-tools/blob/main/doozer/doozerlib/backend/rebaser.py#L1217
                    # but metadata.config.cachito.packages.npm will be empty.
                    # In that case, we assume that the path is the current directory
                    app_path = gomod_deps_path.joinpath('app')
                    app_path.mkdir(parents=True, exist_ok=True)
                    app_path.joinpath('.npmrc').touch(exist_ok=True)

            if "yarn" in pkg_managers:
                flag = False
                for npm_entry in metadata.config.cachito.packages.yarn:
                    # cachito creates an .npmrc/.yarnrc files in the "yarn" package manager paths. This
                    # used to cause NPM/yarn to use servers supplied by cachito,
                    # cachi2 works fundamentally differently
                    # by re-writing package-lock.json to reference local files.
                    # https://github.com/hermetoproject/cachi2/blob/2235854bfc5cb76ea168cb04d50be16efce9fa0a/docs/npm.md
                    app_path = gomod_deps_path.joinpath('app').joinpath(npm_entry.get('path', '.'))
                    app_path.mkdir(parents=True, exist_ok=True)
                    app_path.joinpath('.npmrc').touch(exist_ok=True)
                    app_path.joinpath('.yarnrc').touch(exist_ok=True)
                    flag = True

                if not flag:
                    # In some cases, in image config, metadata.config.content.source.pkg_managers will be populated
                    # but metadata.config.cachito.packages.npm will be empty.
                    # ref https://github.com/openshift-eng/art-tools/blob/main/doozer/doozerlib/backend/rebaser.py#L1217
                    # In that case, we assume that the path is the current directory
                    app_path = gomod_deps_path.joinpath('app')
                    app_path.mkdir(parents=True, exist_ok=True)
                    app_path.joinpath('.npmrc').touch(exist_ok=True)
                    app_path.joinpath('.yarnrc').touch(exist_ok=True)

            konflux_lines += [
                f"ENV REMOTE_SOURCES={emulation_dir}",
                f"ENV REMOTE_SOURCES_DIR={remote_source_dir_env}",
                # Cachito makes application source code and dependencies available inside the app/ directory. We only make the
                # repo source code available there.
                "COPY . $REMOTE_SOURCES_DIR/cachito-gomod-with-deps/app/",
            ]

            if network_mode != "hermetic":
                konflux_lines += [
                    # Needed by s390x builds: https://redhat-internal.slack.com/archives/C04PZ7H0VA8/p1751464077655919
                    "RUN curl https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem",
                    # Cachito also writes a pem file which some builds reference: https://github.com/openshift/console/blob/52510bcb417e44808c07970f09d448fc49787087/Dockerfile#L41 .
                    "ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem $REMOTE_SOURCES_DIR/cachito-gomod-with-deps/app/registry-ca.pem",
                ]
            elif metadata.is_artifact_lockfile_enabled():
                konflux_lines += [
                    "USER 0",
                    "RUN cp /cachi2/output/deps/generic/Current-IT-Root-CAs.pem /tmp/art/Current-IT-Root-CAs.pem",
                    "RUN cp /cachi2/output/deps/generic/Current-IT-Root-CAs.pem $REMOTE_SOURCES_DIR/cachito-gomod-with-deps/app/registry-ca.pem",
                ]

        konflux_lines += [
            f"ENV ART_BUILD_DEPS_MODE={build_deps_mode}",
        ]

        if network_mode != "hermetic" and not no_shell:
            konflux_lines.append("USER 0")
            if self.variant is BuildVariant.OKD:
                konflux_lines.append("RUN mkdir -p /tmp/art")

            else:
                konflux_lines.append(
                    "RUN mkdir -p /tmp/art/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/art/yum_temp/ || true"
                )

            konflux_lines += [
                f"COPY .oit/art-{self.repo_type}.repo /etc/yum.repos.d/",
                # Needed by s390x builds: https://redhat-internal.slack.com/archives/C04PZ7H0VA8/p1751464077655919
                f"RUN curl {constants.KONFLUX_REPO_CA_BUNDLE_HOST}/{constants.KONFLUX_REPO_CA_BUNDLE_FILENAME}",
                f"ADD {constants.KONFLUX_REPO_CA_BUNDLE_HOST}/{constants.KONFLUX_REPO_CA_BUNDLE_FILENAME} {constants.KONFLUX_REPO_CA_BUNDLE_TMP_PATH}",
            ]

        if network_mode == "internal-only":
            konflux_lines += [
                "ENV NO_PROXY='localhost,127.0.0.1,::1,.redhat.com'",
                "ENV HTTP_PROXY='http://127.0.0.1:9999'",
                "ENV HTTPS_PROXY='http://127.0.0.1:9999'",
            ]

        module_enable_commands = self._get_module_enablement_commands(metadata)
        if module_enable_commands:
            konflux_lines.extend(module_enable_commands)

        konflux_lines += ["# End Konflux-specific steps\n\n"]

        dfp.add_lines(
            *konflux_lines,
            at_start=True,
            all_stages=True,
        )

        config_final_stage_user = (
            f"USER {metadata.config.final_stage_user}"
            if metadata.config.final_stage_user not in [None, Missing]
            else None
        )
        config_final_stage_user_set = False

        # Just for last stage
        if network_mode != "hermetic":
            last_stage = self.split_dockerfile_into_stages(dfp)[-1]

            # Find all the USERs in the last stage
            final_stage_user = None
            for line in last_stage:
                if "USER" in line.keys():
                    final_stage_user = f"USER {line['USER']}"

            if final_stage_user.split()[-1] == '0':
                final_stage_user = None  # Avoid redundant USER 0 statement after repo removal

            # But if set in image config, that supersedes the USER that doozer remembers
            # If it's not set in the image config, default to the existing value of final_stage_user
            if config_final_stage_user:
                user_to_set = config_final_stage_user
                config_final_stage_user_set = True
            else:
                user_to_set = final_stage_user

            # Put back original yum config
            # By default, .add_lines adds lines to the end
            lines = [
                "\n# Start Konflux-specific steps",
                "USER 0",
                "RUN rm -f /etc/yum.repos.d/art-* && mv /tmp/art/yum_temp/* /etc/yum.repos.d/ || true",
                "RUN rm -rf /tmp/art",
                f"{user_to_set if user_to_set else ''}",
                "# End Konflux-specific steps\n\n",
            ]

            dfp.add_lines(*lines)

        # metadata.config.final_stage_user has to be honored in all network modes
        if config_final_stage_user and not config_final_stage_user_set:
            dfp.add_lines(*[f"{config_final_stage_user}"])

    def _modify_cachito_commands(self, metadata: ImageMetadata, dfp: DockerfileParser):
        """
        Konflux does not support cachito, comment it out to support green non-hermetic builds
        For yarn, download it if its missing.
        """
        network_mode = metadata.get_konflux_network_mode()
        lines = dfp.lines
        updated_lines = []
        line_commented = False
        yarn_line_updated = False
        for line in lines:
            if 'echo "need yarn at ${CACHED_YARN}"' in line:
                yarn_artifact = self._validate_required_artifacts(metadata, network_mode, "yarn", "yarn-v")

                if network_mode == "hermetic" and yarn_artifact:
                    import urllib.parse

                    filename = urllib.parse.urlparse(yarn_artifact).path.split('/')[-1]
                    line = line.replace(
                        'echo "need yarn at ${CACHED_YARN}"',
                        f"npm install -g /cachi2/output/deps/generic/{filename}",
                    )
                    self._logger.info(f"yarn line overridden. Using pre-fetched yarn from hermetic path: {filename}")
                else:
                    line = line.replace(
                        'echo "need yarn at ${CACHED_YARN}"',
                        "npm install -g https://github.com/yarnpkg/yarn/releases/download/${YARN_VERSION}/yarn-${YARN_VERSION}.tar.gz",
                    )
                    self._logger.info("yarn line overridden. Adding line to download from yarnpkg")

                updated_lines.append(line)
                yarn_line_updated = True
                continue

            if yarn_line_updated:
                if "exit 1" in line:
                    # The 'exit 1' command follows the 'need yarn ...' message. We need to skip that
                    yarn_line_updated = False
                    continue

            # If konflux cachito mode is set to "removal", comment out
            # all lines which reference cachito environment variables. If this doesn't work
            # for a build, you can try 'mode: emulation' which will inject cachito env vars
            # into the Dockerfile.
            cachito_mode = metadata.config.konflux.cachito.mode
            cachito_commenting_enabled = cachito_mode == 'removal'

            if cachito_commenting_enabled and ("REMOTE_SOURCES" in line or "REMOTE_SOURCE_DIR" in line):
                # Comment lines containing 'REMOTE_SOURCES' or 'REMOTE_SOURCES_DIR' based on cachito mode.
                updated_lines.append(f"#{line.strip()}\n")
                line_commented = True
                self._logger.info(
                    "Lines containing 'REMOTE_SOURCES' and 'REMOTE_SOURCES_DIR' have been commented out, since cachito is not supported on konflux"
                )
            else:
                if line_commented:
                    # Sometimes there will be '&& yarn install ...' command. So replace the leading && with RUN, since
                    # we comment that out earlier
                    if line.strip().startswith("&&"):
                        line = line.replace("&&", "RUN", 1)

                    # Due to network flakiness, yarn install commands error out due to insufficient retries.
                    # So increase timeout to 600000 ms, i.e. 10 mins
                    line = line.replace("RUN yarn", "RUN yarn config set network-timeout 600000 && yarn")
                updated_lines.append(line)
                line_commented = False

        if updated_lines:
            dfp.lines = updated_lines

    @start_as_current_span_async(TRACER, "rebase.generate_repo_conf")
    async def _generate_repo_conf(self, metadata: ImageMetadata, dest_dir: Path, repos: Repos):
        """
        Generates a repo file in .oit/repo.conf
        """

        self._logger.debug("Generating repo file for Dockerfile {}".format(metadata.distgit_key))

        # Make our metadata directory if it does not exist
        util.mkdirs(dest_dir.joinpath('.oit'))

        # repos = self._runtime.repos
        enabled_repos = metadata.config.get('enabled_repos', [])
        non_shipping_repos = metadata.config.get('non_shipping_repos', [])

        for t in repos.repotypes:
            rc_path = dest_dir.joinpath('.oit', f'art-{t}.repo')
            async with aiofiles.open(rc_path, 'w', encoding='utf-8') as rc:
                content = repos.repo_file(t, enabled_repos=enabled_repos, konflux=True)
                await rc.write(content)

        rc_path = dest_dir.joinpath('content_sets.yml')
        async with aiofiles.open(rc_path, 'w', encoding='utf-8') as rc:
            await rc.write(repos.content_sets(enabled_repos=enabled_repos, non_shipping_repos=non_shipping_repos))

    @start_as_current_span_async(TRACER, "rebase.generate_config_digest")
    async def _generate_config_digest(self, metadata: ImageMetadata, dest_dir: Path):
        # The config digest is used by scan-sources to detect config changes
        self._logger.debug("Calculating config digest...")
        digest = metadata.calculate_config_digest(self._runtime.group_config, self._runtime.streams)
        os.makedirs(f"{dest_dir}/.oit", exist_ok=True)

        config_path = dest_dir.joinpath(".oit", "config_digest")
        async with aiofiles.open(config_path, 'w') as f:
            await f.write(digest)

        self._logger.info("Saved config digest %s to .oit/config_digest", digest)

    @start_as_current_span_async(TRACER, "rebase.write_cvp_owners")
    async def _write_cvp_owners(self, metadata: ImageMetadata, dest_dir: Path):
        """
        The Container Verification Pipeline will notify image owners when their image is
        not passing CVP tests. ART knows these owners and needs to write them into distgit
        for CVP to find.
        :return:
        """

        if metadata.config.owners:  # Not missing and non-empty
            # only spam owners on failure; ref. https://red.ht/2x0edYd
            self._logger.debug("Generating cvp-owners.yml for {}".format(metadata.distgit_key))
            owners = {owner: "FAILURE" for owner in metadata.config.owners}
            yaml_str = yaml.safe_dump(owners, default_flow_style=False)

            async with aiofiles.open(dest_dir.joinpath('cvp-owners.yml'), 'w', encoding='utf-8') as co:
                await co.write(yaml_str)

    @start_as_current_span_async(TRACER, "rebase.write_fetch_artifacts")
    async def _write_fetch_artifacts(self, metadata: ImageMetadata, dest_dir: Path):
        # Write fetch-artifacts-url.yaml for OSBS to fetch external artifacts
        # See https://osbs.readthedocs.io/en/osbs_ocp3/users.html#using-artifacts-from-koji-or-project-newcastle-aka-pnc
        config_value = None
        if metadata.config.content.source.artifacts.from_urls is not Missing:
            config_value = metadata.config.content.source.artifacts.from_urls.primitive()
        path = dest_dir.joinpath('fetch-artifacts-url.yaml')
        if path.exists():  # upstream provides its own fetch-artifacts-url.yaml
            if not config_value:
                self._logger.info("Use fetch-artifacts-url.yaml provided by upstream.")
                return
            raise ValueError(
                "Image config option content.source.artifacts.from_urls cannot be used if upstream source has fetch-artifacts-url.yaml"
            )
        if not config_value:
            return  # fetch-artifacts-url.yaml is not needed.

        self._logger.info('Generating fetch-artifacts-url.yaml')
        yaml_str = yaml.safe_dump(config_value)
        async with aiofiles.open(path, "w") as f:
            await f.write(yaml_str)

    def _clean_repos(self, dfp):
        """
        Remove any calls to yum --enable-repo or
        yum-config-manager in RUN instructions
        """
        for entry in reversed(dfp.structure):
            if entry['instruction'] == 'RUN':
                if self._runtime.group.startswith('openshift-'):
                    changed, new_value = self._mangle_pkgmgr(entry['value'])
                else:
                    # Only parse if command contains package manager keywords
                    if not re.search(r'\b(yum|dnf|microdnf|yum-config-manager)\b', entry['value']):
                        continue
                    try:
                        changed, new_value = self._mangle_pkgmgr(entry['value'])
                    except (IOError, NotImplementedError) as e:
                        self._logger.warning(f"Cannot parse RUN command, skipping: {str(e)[:100]}")
                        continue
                if changed:
                    dfp.add_lines_at(entry, "RUN " + new_value, replace=True)

    @staticmethod
    def _mangle_pkgmgr(cmd):
        # alter the arg by splicing its content
        def splice(pos, replacement):
            return cmd[: pos[0]] + replacement + cmd[pos[1] :]

        changed = False  # were there changes aside from whitespace?

        # build a list of nodes we may want to alter from the AST
        cmd_nodes = []

        def append_nodes_from(node):
            if node.kind in ["list", "compound"]:
                sublist = node.parts if node.kind == "list" else node.list
                for subnode in sublist:
                    append_nodes_from(subnode)
            elif node.kind in ["operator", "command"]:
                cmd_nodes.append(node)

        # remove dockerfile directive options that bashlex doesn't parse (e.g "RUN --mount=foobar")
        # https://docs.docker.com/reference/dockerfile/#run
        docker_cmd_options = []
        for word in cmd.split():
            if word.startswith("--"):
                docker_cmd_options.append(word)
                cmd = cmd.replace(word, "")
            else:
                break

        try:
            append_nodes_from(bashlex.parse(cmd)[0])
        except bashlex.errors.ParsingError as e:
            raise IOError("Error while parsing Dockerfile RUN command:\n{}\n{}".format(cmd, e))

        # note: make changes working back from the end so that positions to splice don't change
        for subcmd in reversed(cmd_nodes):
            if subcmd.kind == "operator":
                # we lose the line breaks in the original dockerfile,
                # so try to format nicely around operators -- more readable git diffs.
                cmd = splice(subcmd.pos, "\\\n " + subcmd.op)
                continue  # not "changed" logically however

            # replace package manager config with a no-op
            if re.search(r'(^|/)(microdnf\s+|dnf\s+|yum-)config-manager$', subcmd.parts[0].word):
                cmd = splice(subcmd.pos, ": 'removed yum-config-manager'")
                changed = True
                continue
            if (
                re.search(r'(^|/)(micro)?dnf$', subcmd.parts[0].word)
                and len(subcmd.parts) > 1
                and subcmd.parts[1].word == "config-manager"
            ):
                cmd = splice(subcmd.pos, ": 'removed dnf config-manager'")
                changed = True
                continue

            # clear repo options from yum and dnf commands
            if not re.search(r'(^|/)(yum|dnf|microdnf)$', subcmd.parts[0].word):
                continue
            next_word = None
            for word in reversed(subcmd.parts):
                if word.kind != "word":
                    next_word = None
                    continue

                # seek e.g. "--enablerepo=foo" or "--disablerepo bar"
                match = re.match(r'--(en|dis)ablerepo(=|$)', word.word)
                if match:
                    if next_word and match.group(2) != "=":
                        # no "=", next word is the repo so remove it too
                        cmd = splice(next_word.pos, "")
                    cmd = splice(word.pos, "")
                    changed = True
                next_word = word

            # note: there are a number of ways to defeat this logic, for instance by
            # wrapping commands/args in quotes, or with commands that aren't valid
            # to begin with. let's not worry about that; it need not be invulnerable.

        if docker_cmd_options:
            cmd = " ".join(docker_cmd_options) + " " + cmd
        return changed, cmd

    @start_as_current_span_async(TRACER, "rebase.write_osbs_image_config")
    async def _write_osbs_image_config(
        self, metadata: ImageMetadata, dest_dir: Path, source: Optional[SourceResolution], version: str
    ):
        # Writes OSBS image config (container.yaml).
        # For more info about the format, see https://osbs.readthedocs.io/en/latest/users.html#image-configuration.

        self._logger.info('Generating container.yaml')
        container_config = self._generate_osbs_image_config(metadata, dest_dir, source, version)

        if 'compose' in container_config:
            self._logger.info("Rebasing with ODCS support")
        else:
            self._logger.info("Rebasing without ODCS support")

        # generate yaml data with header
        content_yml = yaml.safe_dump(container_config, default_flow_style=False)
        async with aiofiles.open(dest_dir.joinpath('container.yaml'), 'w', encoding="utf-8") as rc:
            await rc.write(CONTAINER_YAML_HEADER + content_yml)

    def _generate_osbs_image_config(
        self, metadata: ImageMetadata, dest_dir: Path, source: Optional[SourceResolution], version: str
    ) -> Dict:
        """
        Generates OSBS image config (container.yaml).
        Returns a dict for the config.

        Example in image yml file:
        odcs:
            packages:
                mode: auto (default) | manual
                # auto - If container.yaml with packages is given from source, use them.
                #        Otherwise all packages with from the Koji build tag will be included.
                # manual - only use list below
                list:
                  - package1
                  - package2
        arches: # Optional list of image specific arches. If given, it must be a subset of group arches.
          - x86_64
          - s390x
        container_yaml: ... # verbatim container.yaml content (see https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/odcs_integration_with_osbs)
        """

        # list of platform (architecture) names to build this image for
        arches = metadata.get_arches()

        # override image config with this dict
        config_overrides = {}
        if metadata.config.container_yaml is not Missing:
            config_overrides = copy.deepcopy(cast(Dict, metadata.config.container_yaml.primitive()))

        cachito_enabled = is_cachito_enabled(
            metadata=metadata, group_config=self._runtime.group_config, logger=self._logger
        )

        if cachito_enabled:
            if config_overrides.get("go", {}).get("modules"):
                raise ValueError(
                    f"Cachito integration is enabled for image {metadata.name}. Specifying `go.modules` in `container.yaml` is not allowed."
                )
            pkg_managers = []  # Note if cachito is enabled but `pkg_managers` is set to an empty array, Cachito will provide the sources with no package manager magic.
            if isinstance(metadata.config.content.source.pkg_managers, ListModel):
                # Use specified package managers
                pkg_managers = metadata.config.content.source.pkg_managers.primitive()
            elif metadata.config.content.source.pkg_managers in [Missing, None]:
                # Auto-detect package managers
                pkg_managers = detect_package_managers(metadata, dest_dir)
            else:
                raise ValueError(
                    f"Invalid content.source.pkg_managers config for image {metadata.name}: {metadata.config.content.source.pkg_managers}"
                )
            # Configure Cachito flags
            # https://github.com/containerbuildsystem/cachito#flags
            flags = []
            if isinstance(metadata.config.cachito.flags, ListModel):
                flags = metadata.config.cachito.flags.primitive()
            elif isinstance(self._runtime.group_config.cachito.flags, ListModel):
                flags = set(self._runtime.group_config.cachito.flags.primitive())
                if 'gomod' not in pkg_managers:
                    # Remove gomod related flags if gomod is not used.
                    flags -= {"cgo-disable", "gomod-vendor", "gomod-vendor-check"}
                elif not dest_dir.joinpath('vendor').is_dir():
                    # Remove gomod-vendor-check flag if vendor/ is not present when gomod is used
                    flags -= {"gomod-vendor-check"}
                flags = list(flags)

            remote_source = {
                'repo': source.https_url,
                'ref': source.commit_hash,
                'pkg_managers': pkg_managers,
            }
            if flags:
                remote_source['flags'] = flags
            # Allow user to customize `packages` option for Cachito configuration.
            # See https://osbs.readthedocs.io/en/osbs_ocp3/users.html#remote-source-keys for details.
            if metadata.config.cachito.packages is not Missing:
                remote_source['packages'] = metadata.config.cachito.packages.primitive()
            elif metadata.config.content.source.path:  # source is in subdirectory
                remote_source['packages'] = {
                    pkg_manager: [{"path": metadata.config.content.source.path}] for pkg_manager in pkg_managers
                }
            config_overrides.update(
                {
                    'remote_sources': [
                        {
                            'name': 'cachito-gomod-with-deps',  # The remote source name is always `cachito-gomod-with-deps` for backward compatibility even if gomod is not used.
                            'remote_source': remote_source,
                        },
                    ],
                }
            )

        if metadata.image_build_method is not Missing and metadata.image_build_method != "osbs2":
            config_overrides['image_build_method'] = metadata.image_build_method

        if arches:
            config_overrides.setdefault('platforms', {})['only'] = arches

        # Request OSBS to apply specified tags to the newly-built image as floating tags.
        # See https://osbs.readthedocs.io/en/latest/users.html?highlight=tags#image-tags
        #
        # Include the UUID in the tags. This will allow other images being rebased
        # to have a known tag to refer to this image if they depend on it - even
        # before it is built.
        floating_tags = {f"{version}.{self._runtime.uuid}"}
        if self._runtime.assembly:
            floating_tags.add(f"assembly.{self._runtime.assembly}")
        vsplit = version.split(".")  # Split the version number: v4.3.4 => [ 'v4', '3, '4' ]
        if len(vsplit) > 1:
            floating_tags.add(f"{vsplit[0]}.{vsplit[1]}")
        if len(vsplit) > 2:
            floating_tags.add(f"{vsplit[0]}.{vsplit[1]}.{vsplit[2]}")
        if metadata.config.additional_tags:
            floating_tags |= set(metadata.config.additional_tags)
        if floating_tags:
            config_overrides["tags"] = sorted(floating_tags)

        if not self._runtime.group_config.doozer_feature_gates.odcs_enabled and not self._runtime.odcs_mode:
            # ODCS mode is not enabled
            return config_overrides

        odcs = metadata.config.odcs
        if odcs is Missing:
            # image yml doesn't have `odcs` field defined
            if not self._runtime.group_config.doozer_feature_gates.odcs_aggressive:
                # Doozer's odcs_aggressive feature gate is off, disable ODCS mode for this image
                return config_overrides
            self._logger.warning("Enforce ODCS auto mode because odcs_aggressive feature gate is on")

        package_mode = odcs.packages.get('mode', 'auto')
        valid_package_modes = ['auto', 'manual']
        if package_mode not in valid_package_modes:
            raise ValueError('odcs.packages.mode must be one of {}'.format(', '.join(valid_package_modes)))

        # generate container.yaml content for ODCS
        config = {}
        if source:  # if upstream source provides container.yaml, load it.
            source_dir = SourceResolver.get_source_dir(source, metadata)
            source_container_yaml = os.path.join(source_dir, 'container.yaml')
            if os.path.isfile(source_container_yaml):
                with open(source_container_yaml, 'r') as scy:
                    config = yaml.full_load(scy)

        # ensure defaults
        config.setdefault('compose', {}).setdefault('pulp_repos', True)

        # create package list for ODCS, see https://osbs.readthedocs.io/en/latest/users.html#compose
        if package_mode == 'auto':
            if isinstance(config["compose"].get("packages"), list):
                # container.yaml with packages was given from source
                self._logger.info("Use ODCS package list from source")
            else:
                config["compose"]["packages"] = []  # empty list composes all packages from the current Koji target
        elif package_mode == 'manual':
            if not odcs.packages.list:
                raise ValueError('odcs.packages.mode == manual but none specified in odcs.packages.list')
            config["compose"]["packages"] = list(odcs.packages.list)

        # apply overrides
        config.update(config_overrides)
        return config

    def _update_yum_update_commands(
        self, metadata: ImageMetadata, force_yum_updates: bool, df_fileobj: io.TextIOBase
    ) -> io.StringIO:
        """If force_yum_updates is True, inject "yum updates -y" in the final build stage; Otherwise, remove the lines we injected.
        Returns an in-memory text stream for the new Dockerfile content
        """
        if force_yum_updates and not metadata.config.get('enabled_repos'):
            # If no yum repos are disabled in image meta, "yum update -y" will fail with "Error: There are no enabled repositories in ...".
            # Remove "yum update -y" lines intead.
            self._logger.warning("Will not inject \"yum updates -y\" for this image because no yum repos are enabled.")
            force_yum_updates = False
        if force_yum_updates:
            self._logger.info("Injecting \"yum updates -y\" in each stage...")

        parser = DockerfileParser(fileobj=df_fileobj)

        df_lines_iter = iter(parser.content.splitlines(False))
        build_stage_num = len(parser.parent_images)
        final_stage_user = metadata.config.final_stage_user or 0

        yum_update_line_flag = "__doozer=yum-update"

        # The yum repo supplied to the image build is going to be appropriate for the base/final image in a
        # multistage build. If one of the stages of, for example, a RHEL8 image is a RHEL7 builder image,
        # we should not run yum update as it will fail loudly. Instead, we check the RHEL version of the
        # image at each stage at *build time* to ensure yum update only runs in appropriate stages.
        el_ver = metadata.branch_el_target()
        if el_ver == 7:
            # For rebuild logic, we need to be able to prioritize repos; RHEL7 requires a plugin to be installed.
            yum_update_line = "RUN yum install -y yum-plugin-priorities && yum update -y && yum clean all"
        else:
            yum_update_line = "RUN yum update -y && yum clean all"
        output = io.StringIO()
        build_stage = 0
        for line in df_lines_iter:
            if yum_update_line_flag in line:
                # Remove the lines we have injected by skipping 2 lines
                next(df_lines_iter)
                continue
            output.write(f'{line}\n')
            if not force_yum_updates or not line.startswith('FROM '):
                continue
            build_stage += 1

            if build_stage != build_stage_num:
                # If this is not the final stage, ignore this FROM
                continue

            # This should be directly after the last 'FROM' (i.e. in the final stage of the Dockerfile).
            # If the current user inherited from the base image for this stage is not root, `yum update -y` will fail
            # and we must change the user to be root.
            # However for the final stage, injecting "USER 0" without changing the original base image user
            # may cause unexpected behavior if the container makes assumption about the user at runtime.
            # Per https://github.com/openshift-eng/doozer/pull/428#issuecomment-861795424,
            # introduce a new metadata `final_stage_user` for images so we can switch the user back later.
            if final_stage_user:
                output.write(f"# {yum_update_line_flag}\nUSER 0\n")
            else:
                self._logger.warning(
                    "Will not inject `USER 0` before `yum update -y` for the final build stage because `final_stage_user` is missing (or 0) in image meta."
                    " If this build fails with `yum update -y` permission denied error, please set correct `final_stage_user` and rebase again."
                )
            output.write(
                f"# {yum_update_line_flag}\n{yum_update_line}  # set final_stage_user in ART metadata if this fails\n"
            )
            if final_stage_user:
                output.write(f"# {yum_update_line_flag}\nUSER {final_stage_user}\n")
        output.seek(0)
        return output

    @start_as_current_span_async(TRACER, "rebase.resolve_image_from_upstream_parent")
    async def _resolve_image_from_upstream_parent(self, original_parent: str, dfp: DockerfileParser) -> Optional[str]:
        """
        Given an upstream image (CI) pullspec, find a matching entry in streams.yml by comparing the rhel version,
        and the builder X.Y fields. If no match is found, return None
        :param original_parent: The upstream image e.g.
        registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.20-openshift-4.15
        :param dfp: DockerfileParser object for the image

        Example: as of 3/5/2024, registry.ci.openshift.org/ocp/builder:rhel-9-golang-1.21-openshift-4.16 should match
        openshift/golang-builder:v1.21.3-202401221732.el9.g00c615b as defined for the rhel-9-golang stream
        """

        try:
            self._logger.debug('Retrieving image info for image %s', original_parent)

            # Use the cached function to extract builder info
            el_version, golang_version = extract_builder_info_from_pullspec(original_parent)

            if not el_version or not golang_version:
                raise ValueError(f'Could not extract RHEL version or golang version from {original_parent}')

            major, minor = golang_version

            # Get expected stream name
            for name, stream in self._runtime.streams.items():
                image = str(stream['image'])
                if ':' not in image:
                    raise ValueError(f"Invalid stream image `{image}` in stream `{name}`")
                image_tag = image.split(':')[-1]

                # Compare builder X.Y
                stream_major, stream_minor, _ = util.extract_version_fields(image_tag)
                if stream_major != major or stream_minor != minor:
                    continue

                # Compare el version
                if release_util.isolate_el_version_in_release(image_tag) == el_version:
                    # We found a match
                    return image

        except (ValueError, ChildProcessError) as e:
            # We could get:
            #   - a ChildProcessError when the upstream equivalent is not found
            #   - a ValueError when 'version' or 'release' labels are undefined
            # In all of the above, we'll just do typical stream resolution

            self._logger.warning(f'Could not match upstream parent {original_parent}: {e}')

        # If we got here, we couldn't match upstream so add a warning in the Dockerfile, and return None
        dfp.add_lines_at(
            0,
            "",
            "# Failed matching upstream equivalent, ART configuration was used to rebase parent images",
            "",
        )

        return None

    @start_as_current_span_async(TRACER, "rebase.update_environment_variables")
    async def _update_environment_variables(
        self,
        metadata: ImageMetadata,
        source: Optional[SourceResolution],
        df_path: Path,
        build_update_envs: Dict[str, str],
        metadata_envs: Dict[str, str],
    ):
        """
        There are three distinct sets of environment variables we need to consider
        in a Dockerfile:
        Set A) build environment variables which doozer calculates on every Dockerfile update (build_update_envs)
        Set B) merge environment variables which doozer can calculate only when upstream source code is available
               (self.env_vars_from_source). If self.env_vars_from_source=None, no merge
               has occurred and we should not try to update envs from source we find in distgit.
        Set C) metadata environment variable which are those which the Dockerfile author has set for their own purposes (these cannot
               override doozer's build env values, but they are free to use other variables names).

        Sets (A) and (B) can be disabled with .content.set_build_variables=False.

        :param build_update_envs: The update environment variables to set (Set A).
        :param metadata_envs: Environment variables that should always be included in the rebased Dockerfile.
        :param filename: The Dockerfile name in the distgit dir to edit.
        :return: N/A
        """

        do_set_build_variables = True
        # set_build_variables must be explicitly set to False. If left unset, default to True. If False,
        # we do not inject environment variables into the Dockerfile. This is occasionally necessary
        # for images like the golang builders where these environment variables pollute the environment
        # for code trying to establish their OWN src commit hash, etc.
        if (
            metadata.config.content.set_build_variables is not Missing
            and not metadata.config.content.set_build_variables
        ):
            do_set_build_variables = False

        env_vars_from_source = None

        # The DockerfileParser can find & set environment variables, but is written such that it only updates the
        # last instance of the env in the Dockerfile. For example, if, at different build stages, A=1 and later A=2
        # are set, DockerfileParser will only manage the A=2 instance. We want to remove variables that doozer is
        # going to set. To do so, we repeatedly load the Dockerfile and remove the env variable.
        # In this way, from our example, on the second load and removal, A=1 should be removed.

        # Build a dict of everything we want to remove from the Dockerfile.
        all_envs_to_remove = dict()
        all_envs_to_remove.update(metadata_envs)

        if do_set_build_variables:
            all_envs_to_remove.update(build_update_envs)
            env_vars_from_source = metadata.extract_kube_env_vars()
            all_envs_to_remove.update(env_vars_from_source)

        while True:
            dfp = DockerfileParser(str(df_path))
            # Find the intersection between envs we want to set and those present in parser
            envs_intersect = set(all_envs_to_remove.keys()).intersection(set(dfp.envs.keys()))

            if not envs_intersect:  # We've removed everything we want to ultimately set
                break

            self._logger.debug(f'Removing old env values from Dockerfile: {envs_intersect}')

            for k in envs_intersect:
                del dfp.envs[k]

            dfp_content = dfp.content
            # Write the file back out
            async with aiofiles.open(df_path, 'w', encoding='utf-8') as df:
                await df.write(dfp_content)

        # The env vars we want to set have been removed from the target Dockerfile.
        # Now, we want to inject the values we have available. In a Dockerfile, ENV must
        # be set for each build stage. So the ENVs must be set after each FROM.

        # Envs that will be written into the Dockerfile every time it is updated
        actual_update_envs: Dict[str, str] = dict()
        actual_update_envs.update(metadata_envs or {})

        if do_set_build_variables:
            actual_update_envs.update(build_update_envs)

        env_update_line_flag = '__doozer=update'
        env_merge_line_flag = '__doozer=merge'

        def get_env_set_list(env_dict):
            """
            Returns a list of 'key1=value1 key2=value2'. Used mainly to ensure
            ENV lines we inject don't change because of key iteration order.
            """
            sets = ''
            for key in sorted(env_dict.keys()):
                sets += f'{key}={env_dict[key]} '
            return sets

        # Build up an UPDATE mode environment variable line we want to inject into each stage.
        update_env_line = None
        if actual_update_envs:
            update_env_line = f"ENV {env_update_line_flag} " + get_env_set_list(actual_update_envs)

        # If a merge has occurred, build up a MERGE mode environment variable line we want to inject into each stage.
        merge_env_line = None
        if (
            do_set_build_variables and source and env_vars_from_source is not None
        ):  # If None, no merge has occurred. Anything else means it has.
            env_vars_from_source.update(
                dict(
                    SOURCE_GIT_COMMIT=source.commit_hash,
                    SOURCE_GIT_TAG=source.latest_tag,
                    SOURCE_GIT_URL=source.public_upstream_url,
                    SOURCE_DATE_EPOCH=str(int(source.committer_date.timestamp())),
                    OS_GIT_VERSION=f'{build_update_envs["OS_GIT_VERSION"]}-{source.commit_hash_short}',
                    OS_GIT_COMMIT=f'{source.commit_hash_short}',
                )
            )

            merge_env_line = f"ENV {env_merge_line_flag} " + get_env_set_list(env_vars_from_source)

        # Open again!
        dfp = DockerfileParser(str(df_path))
        df_lines = dfp.content.splitlines(False)

        async with aiofiles.open(df_path, 'w', encoding="utf-8") as df:
            for line in df_lines:
                # Always remove the env line we update each time.
                if env_update_line_flag in line:
                    continue

                # If we are adding environment variables from source, remove any previous merge line.
                if merge_env_line and env_merge_line_flag in line:
                    continue

                await df.write(f'{line}\n')

                if line.startswith('FROM '):
                    if update_env_line:
                        await df.write(f'{update_env_line}\n')
                    if merge_env_line:
                        await df.write(f'{merge_env_line}\n')

                if line.startswith('COPY . .') and 'KUBE_GIT_VERSION' in env_vars_from_source:
                    # https://issues.redhat.com/browse/OCPBUGS-63749
                    # Go mod will only record a module's version based on tags. To make sure that
                    # kube-apiserver shows up with the proper go module version, we tag directly
                    # after copying .git.
                    # KUBE_GIT_VERSION=v1.34.1+9ca6086 => a tag v1.34.1 .
                    # which should be reported as the go module version for
                    # /usr/bin/kube-apiserver (i.e. go version -m ./kube-apiserver | grep -e k8s.io/kubernetes -e go1.2)
                    await df.write('RUN [ -n "$KUBE_GIT_VERSION" ] && git tag -f "${KUBE_GIT_VERSION%%+*}" || true\n')

    @staticmethod
    @start_as_current_span_async(TRACER, "rebase.reflow_labels")
    async def _reflow_labels(df_path: Path):
        """
        The Dockerfile parser we are presently using writes all labels on a single line
        and occasionally makes multiple LABEL statements. Calling this method with a
        Dockerfile in the current working directory will rewrite the file with
        labels at the end in a single statement.
        """
        dfp = DockerfileParser(str(df_path))
        labels = dict(dfp.labels)  # Make a copy of the labels we need to add back

        # Delete any labels from the modeled content
        for key in dfp.labels:
            del dfp.labels[key]

        # Capture content without labels
        df_content = dfp.content.strip()

        # Write the file back out and append the labels to the end
        async with aiofiles.open(df_path, 'w', encoding="utf-8") as df:
            await df.write("%s\n\n" % df_content)
            if labels:
                additional_labels = {}

                for override_label in [
                    "io.k8s.description",
                    "io.k8s.display-name",
                    "io.openshift.tags",
                    "description",
                    "summary",
                ]:
                    if override_label not in labels:
                        additional_labels[override_label] = "Empty"

                labels.update(additional_labels)

                await df.write("LABEL")
                for k, v in labels.items():
                    await df.write(" \\\n")  # All but the last line should have line extension backslash "\"
                    escaped_v = v.replace('"', '\\"')  # Escape any " with \"
                    await df.write("        %s=\"%s\"" % (k, escaped_v))
                await df.write("\n\n")

    def _get_bundle_paths(self, csv_config: dict) -> Tuple[str, str, str]:
        """Returns (manifests_dir, bundle_dir, bundle_manifests_dir)."""
        manifests_dir = csv_config.get('manifests-dir', 'manifests')
        gvars = self._runtime.group_config.vars
        bundle_dir = csv_config.get('bundle-dir', f'{gvars["MAJOR"]}.{gvars["MINOR"]}')
        bundle_manifests_dir = os.path.join(manifests_dir, bundle_dir)
        return manifests_dir, bundle_dir, bundle_manifests_dir

    def _find_image_refs_path(self, repo_dir: Path, csv_config: dict) -> Optional[Path]:
        """Find the image-references file path, returns None if not found."""
        manifests_dir, bundle_dir, bundle_manifests_dir = self._get_bundle_paths(csv_config)
        ref_candidates = [
            repo_dir.joinpath(dirpath, 'image-references')
            for dirpath in [bundle_manifests_dir, manifests_dir, bundle_dir]
        ]
        return next((cand for cand in ref_candidates if cand.is_file()), None)

    def _get_csv_file_and_refs(self, metadata: ImageMetadata, repo_dir: Path, csv_config):
        manifests_dir, bundle_dir, bundle_manifests_dir = self._get_bundle_paths(csv_config)

        refs = self._find_image_refs_path(repo_dir, csv_config)
        if not refs:
            ref_candidates = [
                repo_dir.joinpath(dirpath, 'image-references')
                for dirpath in [bundle_manifests_dir, manifests_dir, bundle_dir]
            ]
            raise FileNotFoundError(
                '{}: image-references file not found in any location: {}'.format(metadata.distgit_key, ref_candidates)
            )

        with io.open(refs, 'r') as f_ref:
            ref_data = yaml.full_load(f_ref)
        image_refs = ref_data.get('spec', {}).get('tags', {})
        if not image_refs:
            raise ValueError('Data in {} not valid'.format(refs))

        csvs = list(repo_dir.joinpath(bundle_manifests_dir).glob('*.clusterserviceversion.yaml'))
        if len(csvs) < 1:
            raise FileNotFoundError(
                '{}: did not find a *.clusterserviceversion.yaml file @ {}'.format(
                    metadata.distgit_key, bundle_manifests_dir
                )
            )
        elif len(csvs) > 1:
            raise IOError(
                '{}: Must be exactly one *.clusterserviceversion.yaml file but found more than one @ {}'.format(
                    metadata.distgit_key, bundle_manifests_dir
                )
            )
        return str(csvs[0]), image_refs

    @start_as_current_span_async(TRACER, "rebase.replace_internal_image_refs")
    async def _replace_internal_image_refs(
        self,
        metadata: ImageMetadata,
        csv_file: str,
        image_refs: list,
        registry: str,
        image_map: dict,
        external_image_names: Optional[Set[str]] = None,
    ) -> None:
        """Replace internal ART-built image references in the CSV file.

        Collects all replacements first, then performs a single read-modify-write cycle.

        Args:
            metadata: The ImageMetadata for this operator bundle.
            csv_file: Path to the CSV file.
            image_refs: List of image references from image-references file.
            registry: The registry to use for replacements.
            image_map: Mapping of image names.
            external_image_names: Set of image names defined in external-images config.
                                  These will be skipped as they are handled by _replace_external_image_refs.
        """

        def _map_image_name(name, image_map):
            for match, replacement in image_map.items():
                if name.find(match) != -1:
                    return name.replace(match, replacement)
            return name

        namespace = self._runtime.group_config.get('csv_namespace', None)
        if not namespace:
            raise ValueError('csv_namespace is required in group.yaml when any image defines update-csv')

        if external_image_names is None:
            external_image_names = set()

        # Collect all replacements first
        replacements: List[Tuple[str, str]] = []
        for ref in image_refs:
            name = ref['name']
            name = _map_image_name(name, image_map)
            spec = ref['from']['name']

            # Skip images that are defined in external-images config
            # They will be handled by _replace_external_image_refs
            if name in external_image_names:
                self._logger.info(f"Skipping {name} - defined in external-images config")
                continue

            distgit = self._runtime.name_in_bundle_map.get(name, None)
            # fail if upstream is referring to an image we don't actually build
            if not distgit:
                raise ValueError('Unable to find {} in image-references data for {}'.format(name, metadata.distgit_key))

            meta = self._runtime.image_map.get(distgit, None)
            if meta:  # image is currently being processed
                image_tag = f"{meta.image_name_short}:{self.uuid_tag}"
            else:
                meta = self._runtime.late_resolve_image(distgit)
                assert meta is not None
                build = await meta.get_latest_build(
                    el_target=self._get_el_target_string(meta.branch_el_target()),
                    engine=Engine.KONFLUX,
                    exclude_large_columns=True,
                )
                if not build:
                    raise ValueError(f'Could not find latest build for {meta.distgit_key}')
                v = build.version
                r = build.release
                image_tag = '{}:{}-{}'.format(meta.image_name_short, v, r)

            if metadata.distgit_key != meta.distgit_key:
                if metadata.distgit_key not in meta.config.dependents:
                    raise ValueError(
                        f'Related image contains {meta.distgit_key} but this does not have {metadata.distgit_key} in dependents'
                    )

            replace = '{}/{}/{}'.format(registry, namespace, image_tag)
            replacements.append((spec, replace))

        # Perform single read-modify-write cycle
        async with aiofiles.open(csv_file, 'r', encoding='utf-8') as f:
            content = await f.read()

        for spec, replace in replacements:
            content = content.replace(spec + '\n', replace + '\n')
            content = content.replace(spec + '"', replace + '"')

        async with aiofiles.open(csv_file, 'w', encoding='utf-8') as f:
            await f.write(content)

    @start_as_current_span_async(TRACER, "rebase.apply_art_yaml_updates")
    async def _apply_art_yaml_updates(
        self,
        metadata: ImageMetadata,
        dest_dir: Path,
        csv_config: dict,
        version: str,
        release: str,
    ) -> Optional[dict]:
        """Apply art.yaml search-and-replace updates to manifests.

        This function reads the art.yaml file from the operator's manifests directory
        and performs search-and-replace operations defined in the 'updates' section.
        The art.yaml supports template variables like {MAJOR}, {MINOR}, {SUBMINOR},
        {RELEASE}, and {FULL_VER} which are substituted before processing.

        Args:
            metadata: The ImageMetadata for this operator bundle.
            dest_dir: The destination directory containing the operator bundle.
            csv_config: The 'update-csv' configuration from the image metadata.
            version: The version string (e.g., "4.17.0" or "v4.17.0").
            release: The release string for template substitution.

        Returns:
            The parsed art.yaml data as a dict for use by other functions
            (e.g., _replace_external_image_refs for processing 'external-images'),
            or None if art.yaml doesn't exist.
        """
        if version.startswith('v'):
            version = version[1:]  # strip off leading v

        x, y, z = version.split('.')[0:3]

        replace_args = {
            'MAJOR': x,
            'MINOR': y,
            'SUBMINOR': z,
            'RELEASE': release,
            'FULL_VER': '{}-{}'.format(version, release.split('.')[0]),
        }

        manifests_dir, _, _ = self._get_bundle_paths(csv_config)
        manifests_base = os.path.join(dest_dir, manifests_dir)
        art_yaml = os.path.join(manifests_base, 'art.yaml')

        if not os.path.isfile(art_yaml):
            return None

        with io.open(art_yaml, 'r', encoding="utf-8") as art_file:
            art_yaml_str = art_file.read()

        try:
            art_yaml_str = art_yaml_str.format(**replace_args)
            art_yaml_data = yaml.full_load(art_yaml_str)
        except Exception as ex:  # exception is low level, need to pull out the details and rethrow
            raise IOError('Error processing art.yaml!\n{}\n\n{}'.format(str(ex), art_yaml_str))

        updates = art_yaml_data.get('updates', [])
        if updates:
            if not isinstance(updates, list):
                raise TypeError('`updates` key must be a list in art.yaml')

            for u in updates:
                f = u.get('file', None)
                u_list = u.get('update_list', [])
                if not f:
                    raise ValueError('No file to update specified in art.yaml')
                if not u_list:
                    raise ValueError('update_list empty for {} in art.yaml'.format(f))

                f_path = os.path.join(manifests_base, f)
                if not os.path.isfile(f_path):
                    raise ValueError('{} does not exist as defined in art.yaml'.format(f_path))

                self._logger.info('Updating {}'.format(f_path))
                # Read the content
                async with aiofiles.open(f_path, 'r+', encoding="utf-8") as sr_file:
                    sr_file_str = await sr_file.read()
                    for sr in u_list:
                        s = sr.get('search', None)
                        r = sr.get('replace', None)
                        if s is None or r is None:
                            raise ValueError('Must provide `search` and `replace` fields in art.yaml `update_list`')
                        if not isinstance(s, str) or not isinstance(r, str):
                            raise ValueError('`search` and `replace` fields in art.yaml `update_list` must be strings')
                        if not s:
                            raise ValueError('`search` field in art.yaml `update_list` cannot be empty')

                        original_string = sr_file_str
                        sr_file_str = sr_file_str.replace(s, r)
                        if sr_file_str == original_string:
                            self._logger.error(f'Search `{s}` and replace was ineffective for {metadata.distgit_key}')

                # Overwrite file with updated content
                async with aiofiles.open(f_path, 'w', encoding='utf-8') as sr_file:
                    await sr_file.write(sr_file_str)

        return art_yaml_data

    @start_as_current_span_async(TRACER, "rebase.replace_external_image_refs")
    async def _replace_external_image_refs(
        self,
        csv_file: str,
        csv_config: dict,
        dest_dir: Path,
        art_yaml_data: Optional[dict],
    ) -> None:
        """Replace external image references with digest-pinned pullspecs.

        This function processes the 'external-images' section from art.yaml to resolve
        external (non-ART-built) container images to their digest-pinned form. For each
        external image, it:
        1. Resolves the target image to get its SHA256 digest
        2. Strips any tag from the target to get the base image reference
        3. Constructs the digest-pinned pullspec (e.g., registry.redhat.io/rhel9/postgresql-15@sha256:...)
        4. Replaces the source placeholder with the resolved pullspec in CSV and image-references files

        The 'external-images' configuration in art.yaml should have entries like:
            external-images:
              - name: postgresql
                source: RELATED_IMAGE_POSTGRESQL  # placeholder in CSV
                target: registry.redhat.io/rhel9/postgresql-15:latest  # image to resolve

        Args:
            csv_file: Path to the ClusterServiceVersion YAML file.
            csv_config: The 'update-csv' configuration from the image metadata.
            dest_dir: The destination directory containing the operator bundle.
            art_yaml_data: Parsed art.yaml data from _apply_art_yaml_updates, or None
                           if art.yaml doesn't exist. This avoids duplicate file I/O.
        """
        if not art_yaml_data:
            return

        external_images = art_yaml_data.get('external-images', [])
        if not external_images:
            return

        refs_path = self._find_image_refs_path(dest_dir, csv_config)

        for ext_img in external_images:
            name = ext_img.get('name')
            source = ext_img.get('search')
            target = ext_img.get('replace')

            if not source or not target:
                self._logger.warning(f"External image configuration for {name} is missing search or replace")
                continue

            self._logger.info(f"Resolving digest for external image {target}")
            try:
                registry_auth = os.getenv("KONFLUX_OPERATOR_INDEX_AUTH_FILE")
                image_info = await util.oc_image_info_for_arch_async__caching(target, registry_config=registry_auth)
                # Use listDigest for multi-arch images, if no listDigest, use digest
                digest = image_info.get('listDigest')
                if not digest:
                    digest = image_info.get('digest')
            except Exception as e:
                self._logger.error(f"Failed to resolve digest for {target}: {e}")
                raise

            if not digest:
                raise IOError(f"Could not resolve digest for {target}")

            # Strip the tag from target to get the base image reference
            # e.g., registry.redhat.io/rhel9/postgresql-15:latest -> registry.redhat.io/rhel9/postgresql-15
            # Handle edge cases like registry.example.com:5000/image:tag (port vs tag)
            if '@' in target:
                # Already has a digest - strip it to get the base
                base_target = target.split('@')[0]
            else:
                # Check for tag: split by '/' and check if last segment has ':'
                # This handles registry:port/image:tag correctly
                parts = target.rsplit('/', 1)
                if len(parts) == 2 and ':' in parts[1]:
                    # The last segment has a tag, strip it
                    base_target = parts[0] + '/' + parts[1].rsplit(':', 1)[0]
                elif len(parts) == 1 and ':' in parts[0]:
                    # No '/' in target, check if it's just image:tag
                    # This is less common but handle it
                    base_target = parts[0].rsplit(':', 1)[0]
                else:
                    # No tag
                    base_target = target

            resolved_target = f"{base_target}@{digest}"

            self._logger.info(f"Replacing {source} with {resolved_target} in CSV and {refs_path}")

            # Update CSV
            async with aiofiles.open(csv_file, 'r', encoding='utf-8') as f:
                content = await f.read()

            if source in content:
                content = content.replace(source, resolved_target)
                async with aiofiles.open(csv_file, 'w', encoding='utf-8') as f:
                    await f.write(content)
            else:
                self._logger.info(f"Source image {source} not found in CSV {csv_file}, nothing to replace")

            # Update image-references
            if refs_path:
                async with aiofiles.open(refs_path, 'r', encoding='utf-8') as f:
                    content = await f.read()

                if source in content:
                    content = content.replace(source, resolved_target)
                    async with aiofiles.open(refs_path, 'w', encoding='utf-8') as f:
                        await f.write(content)
                else:
                    self._logger.info(f"Source image {source} not found in {refs_path}, nothing to replace")

    def _get_external_image_names(self, dest_dir: Path, csv_config: dict) -> Set[str]:
        """Get the set of image names defined in external-images config from art.yaml.

        These images are not ART-built and will be handled separately by _replace_external_image_refs.

        Args:
            dest_dir: The destination directory containing the operator bundle.
            csv_config: The 'update-csv' configuration from the image metadata.

        Returns:
            Set of image names defined in external-images config, or empty set if none.
        """
        manifests_dir, _, _ = self._get_bundle_paths(csv_config)
        manifests_base = os.path.join(dest_dir, manifests_dir)
        art_yaml_path = os.path.join(manifests_base, 'art.yaml')

        if not os.path.isfile(art_yaml_path):
            return set()

        try:
            with io.open(art_yaml_path, 'r', encoding="utf-8") as art_file:
                art_yaml_data = yaml.full_load(art_file.read())
        except Exception:
            return set()

        external_images = art_yaml_data.get('external-images', [])
        if not external_images:
            return set()

        return {ext_img.get('name') for ext_img in external_images if ext_img.get('name')}

    @start_as_current_span_async(TRACER, "rebase.update_csv")
    async def _update_csv(self, metadata: ImageMetadata, dest_dir: Path, version: str, release: str):
        csv_config = metadata.config.get('update-csv', None)
        if not csv_config:
            return

        csv_file, image_refs = self._get_csv_file_and_refs(metadata, dest_dir, csv_config)
        registry = csv_config['registry'].rstrip("/")
        image_map = csv_config.get('image-map', {})

        # Get external image names to skip in internal image replacement
        external_image_names = self._get_external_image_names(dest_dir, csv_config)

        # Replace internal ART-built image references
        await self._replace_internal_image_refs(
            metadata, csv_file, image_refs, registry, image_map, external_image_names
        )

        # Apply art.yaml search-and-replace updates and get parsed art.yaml data.
        # The returned art_yaml_data is passed to _replace_external_image_refs to avoid
        # reading art.yaml twice (optimization to reduce file I/O).
        art_yaml_data = await self._apply_art_yaml_updates(metadata, dest_dir, csv_config, version, release)

        # Replace external image references with digest-pinned pullspecs.
        # Uses 'external-images' config from the art_yaml_data returned above.
        await self._replace_external_image_refs(csv_file, csv_config, dest_dir, art_yaml_data)
