import copy
import hashlib
import io
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, cast

import bashlex
import bashlex.errors
import yaml
from artcommonlib import exectools, release_util
from artcommonlib.model import ListModel, Missing
from dockerfile_parse import DockerfileParser

from doozerlib import constants, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.brew import BuildStates
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
from doozerlib.repos import Repos
from doozerlib.runtime import Runtime
from doozerlib.source_modifications import SourceModifierFactory
from doozerlib.source_resolver import SourceResolution, SourceResolver

LOGGER = logging.getLogger(__name__)


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
    """ Rebase images to a new branch in the build source repository.

    FIXME: Some features that are known to be missing:
    - Handling of alternate upstreams
    - Handling of private fix bit in parent images
    """

    def __init__(self,
                 runtime: Runtime,
                 base_dir: Path,
                 source_resolver: SourceResolver,
                 repo_type: str,
                 upcycle: bool = False, force_private_bit: bool = False,
                 record_logger: Optional[RecordLogger] = None,
                 source_modifier_factory=SourceModifierFactory(),
                 logger: Optional[logging.Logger] = None,
                 ) -> None:
        self._runtime = runtime
        self._base_dir = base_dir
        self._source_resolver = source_resolver
        self.repo_type = repo_type
        self.upcycle = upcycle
        self.force_private_bit = force_private_bit
        self._record_logger = record_logger
        self._source_modifier_factory = source_modifier_factory
        self.should_match_upstream = False  # FIXME: Matching upstream is not supported yet
        self._logger = logger or LOGGER

    async def rebase_to(
            self,
            metadata: ImageMetadata,
            version: str,
            input_release: str,
            force_yum_updates: bool,
            image_repo: str,
            commit_message: str,
            push: bool,
    ) -> None:
        try:
            # If this image has an upstream source, resolve it
            source = None
            if metadata.has_source():
                self._logger.info(f"Resolving source for {metadata.qualified_key}")
                source = cast(SourceResolution, await exectools.to_thread(self._source_resolver.resolve_source, metadata))
            else:
                raise IOError(f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported.")

            dest_branch = "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format_map({
                "group": self._runtime.group,
                "assembly_name": self._runtime.assembly,
                "distgit_key": metadata.distgit_key
            })

            self._logger.info(f"Rebasing {metadata.qualified_key} to {dest_branch}")

            dest_dir = self._base_dir.joinpath(metadata.qualified_key)

            # Clone the build repository
            build_repo = BuildRepo(url=source.url, branch=dest_branch, local_dir=dest_dir, logger=self._logger)
            await build_repo.ensure_source(upcycle=self.upcycle)

            # Rebase the image in the build repository
            self._logger.info("Rebasing image %s to %s in %s...", metadata.distgit_key, dest_branch, dest_dir)
            await exectools.to_thread(self._rebase_dir, metadata, source, build_repo, version, input_release, force_yum_updates, image_repo)

            # Commit changes
            await build_repo.commit(commit_message, allow_empty=True)

            # Push changes
            if push:
                self._logger.info("Pushing changes to %s...", build_repo.url)
                await build_repo.push()

            metadata.rebase_status = True
        finally:
            # notify child images
            metadata.rebase_event.set()

    def _rebase_dir(
            self,
            metadata: ImageMetadata,
            source: Optional[SourceResolution],
            build_repo: BuildRepo,
            version: str,
            input_release: str,
            force_yum_updates: bool,
            image_repo: str,
    ):
        """ Rebase the image in the build repository. """

        # Whether or not the source contains private fixes; None means we don't know yet
        private_fix = None
        if self.force_private_bit:  # --embargoed is set, force private_fix to True
            private_fix = True

        dest_dir = build_repo.local_dir
        df_path = dest_dir.joinpath('Dockerfile')
        source_dir = None

        # If this image has upstream source, merge it into the build repo
        if source:
            source_dir = SourceResolver.get_source_dir(source, metadata)
            self._merge_source(metadata=metadata, source=source, source_dir=source_dir, dest_dir=dest_dir)

        # Load Dockerfile from the build repo
        dfp = DockerfileParser(str(df_path))

        # Determine if this image contains private fixes
        if private_fix is None:
            if source and source_dir:
                # If the private org branch commit doesn't exist in the public org,
                # this image contains private fixes
                if source.has_public_upstream \
                        and not SourceResolver.is_branch_commit_hash(source.public_upstream_branch) \
                        and not util.is_commit_in_public_upstream(source.commit_hash, source.public_upstream_branch, source_dir):
                    private_fix = True
            else:
                # If we don't have upstream source, we need to extract the private_fix_bit from the Dockerfile in the build repo
                _, _, prev_private_fix = self.extract_version_release_private_fix(dfp)
                if prev_private_fix:
                    private_fix = True

        # Determine if parent images contain private fixes
        downstream_parents: Optional[List[str]] = None
        if "from" in metadata.config:
            # If this image is FROM another group member, we need to wait on that group
            # member to determine if there is a private fix in it.
            LOGGER.info("Waiting for parent members of %s...", metadata.distgit_key)
            parent_members = self._wait_for_parent_members(metadata)
            failed_parents = [parent.distgit_key for parent in parent_members if parent is not None and not parent.rebase_status]
            if failed_parents:
                raise IOError(f"Couldn't rebase {metadata.distgit_key} because the following parent images failed to rebase: {', '.join(failed_parents)}")
            uuid_tag = f"{version}-{self._runtime.uuid}"
            downstream_parents, parent_private_fix = self._resolve_parents(metadata, dfp, image_repo, uuid_tag)
            # If any of the parent images are private, this image is private
            if parent_private_fix:
                private_fix = True
            # Replace registry URLs for Konflux
            downstream_parents = [image.replace(constants.REGISTRY_PROXY_BASE_URL, constants.BREW_REGISTRY_BASE_URL) for image in downstream_parents]

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
            self._run_modifications(metadata=metadata, dest_dir=dest_dir, metadata_scripts_path=metadata_scripts_path)

        # Given an input release string, make an actual release string
        # e.g. 4.17.0-202407241200.p? -> 4.17.0-202407241200.p0.assembly.stream.gdeadbee.el9
        release = self._make_actual_release_string(metadata, input_release, private_fix, source)
        self._update_build_dir(metadata, dest_dir, source, version, release, downstream_parents, force_yum_updates,
                               image_repo, uuid_tag)
        metadata.private_fix = private_fix

    def _resolve_parents(self, metadata: ImageMetadata, dfp: DockerfileParser, image_repo: str, uuid_tag: str):
        """ Resolve the parent images for the given image metadata."""
        image_from = metadata.config.get('from', {})
        parents = image_from.get("builder", []).copy()
        for builder in image_from.get("builder", []):
            parents.append(builder)
        parents.append(image_from)
        if len(parents) != len(dfp.parent_images):
            raise ValueError(f"Build metadata for {metadata.distgit_key} expected {len(parents)} parent images, but found {len(dfp.parent_images)} in Dockerfile")

        mapped_images: List[Tuple[str, bool]] = []
        for parent, original_parent in zip(parents, dfp.parent_images):
            if "member" in parent:
                mapped_images.append(self._resolve_member_parent(parent["member"], original_parent, image_repo, uuid_tag))
            elif "image" in parent:
                mapped_images.append((parent["image"], False))
            elif "stream" in parent:
                mapped_images.append((self._resolve_stream_parent(parent['stream'], original_parent, dfp), False))
            else:
                raise ValueError(f"Image in 'from' for [{metadata.distgit_key}] is missing its definition.")

        downstream_parents = [pullspec for pullspec, _ in mapped_images]
        private_fix = any(private_fix for _, private_fix in mapped_images)
        return downstream_parents, private_fix

    def _resolve_member_parent(self, member: str, original_parent: str, image_repo: str, uuid_tag: str):
        """ Resolve the parent image for the given image metadata."""
        parent_metadata = self._runtime.resolve_image(member, required=False)
        private_fix = False
        if parent_metadata is None:
            if not self._runtime.ignore_missing_base:
                raise IOError(f"Parent image {member} is not loaded.")
            if self._runtime.latest_parent_version or self._runtime.assembly_basis_event:
                parent_metadata = self._runtime.late_resolve_image(member)
                if not parent_metadata:
                    raise IOError(f"Parent image {member} is not found.")
                _, v, r = parent_metadata.get_latest_build_info()
                if util.isolate_pflag_in_release(r) == 'p1':
                    private_fix = True
                return f"{parent_metadata.config.name}:{v}-{r}", private_fix
            return original_parent, False
        else:
            if parent_metadata.private_fix is None:
                raise IOError(
                    f"Parent image {member} doesn't have .p0/.p1 flag determined. "
                    "This indicates a bug in Doozer. Please report this issue."
                )
            private_fix = parent_metadata.private_fix
            return f"{image_repo}:{parent_metadata.image_name_short}-{uuid_tag}", private_fix

    def _resolve_stream_parent(self, stream_name: str, original_parent: str, dfp: DockerfileParser):
        stream = self._runtime.resolve_stream(stream_name)

        if not self.should_match_upstream:
            # Do typical stream resolution.
            return str(stream.image)

        # canonical_builders_from_upstream flag is either True, or 'auto' and we are before feature freeze
        image = self._resolve_image_from_upstream_parent(original_parent, dfp)
        if image:
            return image

        # Didn't find a match in streams.yml: do typical stream resolution
        return str(stream.image)

    def _wait_for_parent_members(self, metadata: ImageMetadata):
        # If this image is FROM another group member, we need to wait on that group
        # member before determining if there is a private fix in it.
        parent_members = list(metadata.get_parent_members().values())
        for parent_member in parent_members:
            if parent_member is None:
                continue  # Parent member is not included in the group; no need to wait
            # wait for parent member to be rebased
            if not parent_member.rebase_event.is_set():
                self._logger.info("[%s] Parent image %s is being rebasing; waiting...", metadata.distgit_key, parent_member.distgit_key)
                parent_member.rebase_event.wait()
        return parent_members

    @staticmethod
    def _recursive_overwrite(src, dest, ignore=set()):
        """
        Use rsync to copy one file tree to a new location
        """
        exclude = ' --exclude .git '
        for i in ignore:
            exclude += ' --exclude="{}" '.format(i)
        cmd = 'rsync -av {} {}/ {}/'.format(exclude, src, dest)
        exectools.cmd_assert(cmd, retries=3)

    def _merge_source(self, metadata: ImageMetadata, source: SourceResolution, source_dir: Path, dest_dir: Path):
        """
        Pulls source defined in content.source and overwrites most things in the distgit
        clone with content from that source.
        """
        class MergeSourceResult:
            pass

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
                shutil.rmtree(str(ent.resolve()))

        # Copy all files and overwrite where necessary
        self._recursive_overwrite(source_dir, dest_dir)

        df_path = dest_dir.joinpath('Dockerfile')

        if df_path.exists():
            # The w+ below will not overwrite a symlink file with real content (it will
            # be directed to the target file). So unlink explicitly.
            df_path.unlink()

        with open(source_dockerfile_path, mode='r', encoding='utf-8') as source_dockerfile, \
                open(str(df_path), mode='w+', encoding='utf-8') as distgit_dockerfile:
            # The source Dockerfile could be named virtually anything (e.g. Dockerfile.rhel) or
            # be a symlink. Ultimately, we don't care - we just need its content in distgit
            # as /Dockerfile (which OSBS requires). Read in the content and write it back out
            # to the required distgit location.
            source_dockerfile_content = source_dockerfile.read()
            distgit_dockerfile.write(source_dockerfile_content)

        # Clean up any extraneous Dockerfile.* that might be distractions (e.g. Dockerfile.centos)
        for ent in dest_dir.iterdir():
            if ent.name.startswith("Dockerfile."):
                ent.unlink()

        # Workaround for https://issues.redhat.com/browse/STONEBLD-1929
        containerfile = dest_dir.joinpath('Containerfile')
        if containerfile.is_file():
            containerfile.unlink()

        # Delete .gitignore since it may block full sync and is not needed here
        gitignore_path = dest_dir.joinpath('.gitignore')
        if gitignore_path.is_file():
            gitignore_path.unlink()

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
            with exectools.Dir(source_dir):
                author_email = None
                err = None
                rc, sha, err = exectools.cmd_gather(
                    # --no-merges because the merge bot is not the real author
                    # --diff-filter=a to omit the "first" commit in a shallow clone which may not be the author
                    #   (though this means when the only commit is the initial add, that is omitted)
                    'git log --no-merges --diff-filter=a -n 1 --pretty=format:%H {}'.format(dockerfile_name)
                )
                if rc == 0:
                    rc, ae, err = exectools.cmd_gather('git show -s --pretty=format:%ae {}'.format(sha))
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
                    dockerfile=str(dest_dir.joinpath('Dockerfile')))

    def extract_version_release_private_fix(self, dfp: DockerfileParser) -> Tuple[Optional[str], Optional[str], Optional[bool]]:
        """
        Extract version, release, and private_fix fields from Dockerfile.

        :param dfp: DockerfileParser object
        :return: Tuple of version, release, and private_fix
        """
        # extract previous release to enable incrementing it
        prev_release = dfp.labels.get("release")
        private_fix = None
        if prev_release:
            if util.isolate_pflag_in_release(prev_release) == 'p1':
                private_fix = True
            elif util.isolate_pflag_in_release(prev_release) == 'p0':
                private_fix = False
        version = dfp.labels.get("version")
        return version, prev_release, private_fix

    def _run_modifications(self, metadata: ImageMetadata, dest_dir: Path, metadata_scripts_path: Path):
        """
        Interprets and applies content.source.modifications steps in the image metadata.
        """
        df_path = dest_dir.joinpath('Dockerfile')
        with df_path.open('r') as df:
            dockerfile_data = df.read()

        self._logger.debug(
            "About to start modifying Dockerfile [%s]:\n%s\n" %
            (metadata.distgit_key, dockerfile_data))

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
            with df_path.open('w', encoding="utf-8") as df:
                df.write(new_dockerfile_data)

    def _update_build_dir(self, metadata: ImageMetadata, dest_dir: Path,
                          source: Optional[SourceResolution],
                          version: str, release: str, downstream_parents: Optional[List[str]],
                          force_yum_updates: bool, image_repo: str, uuid_tag: str):
        with exectools.Dir(dest_dir):
            self._generate_repo_conf(metadata, dest_dir, self._runtime.repos)

            self._generate_config_digest(metadata, dest_dir)

            self._write_cvp_owners(metadata, dest_dir)

            self._write_fetch_artifacts(metadata, dest_dir)

            self._write_osbs_image_config(metadata, dest_dir, source, version)

            df_path = dest_dir.joinpath('Dockerfile')
            self._update_dockerfile(metadata, source, df_path, version, release, downstream_parents,
                                    force_yum_updates, uuid_tag)

            self._update_csv(metadata, dest_dir, version, release, image_repo, uuid_tag)

            return version, release

    def _make_actual_release_string(self, metadata: ImageMetadata, input_release: str, private_fix: bool, source: Optional[SourceResolution]) -> str:
        """ Given a input_release string (may contain .p?), make an actual release string.

        e.g. 4.17.0-202407241200.p? -> 4.17.0-202407241200.p0.assembly.stream.gdeadbee.el9
        """
        sb = io.StringIO()
        if input_release.endswith(".p?"):
            sb.write(input_release[:-3])  # strip .p?
            pval = ".p1" if private_fix else ".p0"
            sb.write(pval)
        elif self._runtime.group_config.public_upstreams:
            raise ValueError(f"'release' must end with '.p?' for an image with a public upstream but its actual value is {input_release}")

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

    def _update_dockerfile(self, metadata: ImageMetadata, source: Optional[SourceResolution],
                           df_path: Path, version: str, release: str, downstream_parents: Optional[List[str]],
                           force_yum_updates: bool, uuid_tag: str):
        """ Update the Dockerfile in the build repo with the correct labels and version information.
        """
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
                dfp.labels[srclab['source_commit']] = '{}/commit/{}'.format(source.public_upstream_url, source.commit_hash)

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

            # remove any old instances of empty.repo mods that aren't in mod block
            if 'empty.repo' not in line:
                if line.endswith('\n'):
                    line = line[0:-1]  # remove trailing newline, if exists
                filtered_content.append(line)

        df_lines = filtered_content

        # ART-8476 assert rhel version equivalence
        if self.should_match_upstream:
            el_version = metadata.branch_el_target()
            df_lines.extend([
                '',
                '# RHEL version in final image must match the one in ART\'s config',
                f'RUN source /etc/os-release && [ "$PLATFORM_ID" == platform:el{el_version} ]'
            ])

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
            '__doozer_uuid_tag': f"{metadata.image_name_short}-{uuid_tag}",
        }
        if metadata.config.envs:
            # Allow environment variables to be specified in the ART image metadata
            metadata_envs.update(metadata.config.envs.primitive())

        df_fileobj = self._update_yum_update_commands(metadata, force_yum_updates, io.StringIO(df_content))
        with Path(dfp.dockerfile_path).open('w', encoding="utf-8") as df:
            shutil.copyfileobj(df_fileobj, df)
            df_fileobj.close()

        self._update_environment_variables(metadata, source, df_path, build_update_envs=build_update_env_vars, metadata_envs=metadata_envs)

        # Inject build repos for Konflux
        self._add_build_repos(dfp)

        self._reflow_labels(df_path)

    def _add_build_repos(self, dfp: DockerfileParser):
        # Populating the repo file needs to happen after every FROM before the original Dockerfile can invoke yum/dnf.
        dfp.add_lines(
            "\n# Start Konflux-specific steps",
            "RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true",
            f"COPY .oit/{self.repo_type}.repo /etc/yum.repos.d/",
            f"ADD {constants.KONFLUX_REPO_CA_BUNDLE_HOST}/{constants.KONFLUX_REPO_CA_BUNDLE_FILENAME} {constants.KONFLUX_REPO_CA_BUNDLE_TMP_PATH}",
            "# End Konflux-specific steps\n\n",
            at_start=True,
            all_stages=True,
        )

        # Put back original yum config
        dfp.add_lines(
            "\n# Start Konflux-specific steps",
            "RUN cp /tmp/yum_temp/* /etc/yum.repos.d/ || true",
            "# End Konflux-specific steps\n\n"
        )

    def _generate_repo_conf(self, metadata: ImageMetadata, dest_dir: Path, repos: Repos):
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
            with dest_dir.joinpath('.oit', f'{t}.repo').open('w', encoding="utf-8") as rc:
                content = repos.repo_file(t, enabled_repos=enabled_repos, konflux=True)
                rc.write(content)

        with dest_dir.joinpath('content_sets.yml').open('w', encoding="utf-8") as rc:
            rc.write(repos.content_sets(enabled_repos=enabled_repos, non_shipping_repos=non_shipping_repos))

    def _generate_config_digest(self, metadata: ImageMetadata, dest_dir: Path):
        # The config digest is used by scan-sources to detect config changes
        self._logger.debug("Calculating config digest...")
        digest = metadata.calculate_config_digest(self._runtime.group_config, self._runtime.streams)
        with dest_dir.joinpath(".oit", "config_digest").open('w') as f:
            f.write(digest)
        self._logger.info("Saved config digest %s to .oit/config_digest", digest)

    def _write_cvp_owners(self, metadata: ImageMetadata, dest_dir: Path):
        """
        The Container Verification Pipeline will notify image owners when their image is
        not passing CVP tests. ART knows these owners and needs to write them into distgit
        for CVP to find.
        :return:
        """
        self._logger.debug("Generating cvp-owners.yml for {}".format(metadata.distgit_key))
        with dest_dir.joinpath('cvp-owners.yml').open('w', encoding="utf-8") as co:
            if metadata.config.owners:  # Not missing and non-empty
                # only spam owners on failure; ref. https://red.ht/2x0edYd
                owners = {owner: "FAILURE" for owner in metadata.config.owners}
                yaml.safe_dump(owners, co, default_flow_style=False)

    def _write_fetch_artifacts(self, metadata: ImageMetadata, dest_dir: Path):
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
            raise ValueError("Image config option content.source.artifacts.from_urls cannot be used if upstream source has fetch-artifacts-url.yaml")
        if not config_value:
            return  # fetch-artifacts-url.yaml is not needed.
        self._logger.info('Generating fetch-artifacts-url.yaml')
        with path.open("w") as f:
            yaml.safe_dump(config_value, f)

    def _clean_repos(self, dfp):
        """
        Remove any calls to yum --enable-repo or
        yum-config-manager in RUN instructions
        """
        for entry in reversed(dfp.structure):
            if entry['instruction'] == 'RUN':
                changed, new_value = self._mangle_pkgmgr(entry['value'])
                if changed:
                    dfp.add_lines_at(entry, "RUN " + new_value, replace=True)

    @staticmethod
    def _mangle_pkgmgr(cmd):
        # alter the arg by splicing its content
        def splice(pos, replacement):
            return cmd[:pos[0]] + replacement + cmd[pos[1]:]
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
            if re.search(r'(^|/)(micro)?dnf$', subcmd.parts[0].word) and len(subcmd.parts) > 1 and subcmd.parts[1].word == "config-manager":
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

    def _write_osbs_image_config(self, metadata: ImageMetadata, dest_dir: Path, source: Optional[SourceResolution], version: str):
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
        with dest_dir.joinpath('container.yaml').open('w', encoding="utf-8") as rc:
            rc.write(CONTAINER_YAML_HEADER + content_yml)

    def _generate_osbs_image_config(self, metadata: ImageMetadata, dest_dir: Path, source: Optional[SourceResolution], version: str) -> Dict:
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

        # Cachito will be configured if `cachito.enabled` is True in image metadata
        # or `cachito.enabled` is True in group config.
        # https://osbs.readthedocs.io/en/latest/users.html#remote-sources
        cachito_enabled = False
        if metadata.config.cachito.enabled:
            cachito_enabled = True
        elif metadata.config.cachito.enabled is Missing:
            if self._runtime.group_config.cachito.enabled:
                cachito_enabled = True
            elif isinstance(metadata.config.content.source.pkg_managers, ListModel):
                self._logger.warning(f"pkg_managers directive for {metadata.name} has no effect since cachito is not enabled in "
                                     "image metadata or group config.")
        if cachito_enabled and not metadata.has_source():
            self._logger.warning("Cachito integration for distgit-only image %s is not supported.", metadata.name)
            cachito_enabled = False
        if cachito_enabled:
            if config_overrides.get("go", {}).get("modules"):
                raise ValueError(f"Cachito integration is enabled for image {metadata.name}. Specifying `go.modules` in `container.yaml` is not allowed.")
            pkg_managers = []  # Note if cachito is enabled but `pkg_managers` is set to an empty array, Cachito will provide the sources with no package manager magic.
            if isinstance(metadata.config.content.source.pkg_managers, ListModel):
                # Use specified package managers
                pkg_managers = metadata.config.content.source.pkg_managers.primitive()
            elif metadata.config.content.source.pkg_managers in [Missing, None]:
                # Auto-detect package managers
                pkg_managers = self._detect_package_managers(metadata, dest_dir)
            else:
                raise ValueError(f"Invalid content.source.pkg_managers config for image {metadata.name}: {metadata.config.content.source.pkg_managers}")
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
                remote_source['packages'] = {pkg_manager: [{"path": metadata.config.content.source.path}] for pkg_manager in pkg_managers}
            config_overrides.update({
                'remote_sources': [
                    {
                        'name': 'cachito-gomod-with-deps',  # The remote source name is always `cachito-gomod-with-deps` for backward compatibility even if gomod is not used.
                        'remote_source': remote_source,
                    }
                ]
            })

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

    def _detect_package_managers(self, metadata: ImageMetadata, dest_dir: Path):
        """ Detect and return package managers used by the source
        :return: a list of package managers
        """
        if not dest_dir or not dest_dir.is_dir():
            raise FileNotFoundError(f"Distgit directory for image {metadata.distgit_key} hasn't been cloned.")
        pkg_manager_files = {
            "gomod": ["go.mod"],
            "npm": ["npm-shrinkwrap.json", "package-lock.json"],
            "pip": ["requirements.txt", "requirements-build.txt"],
            "yarn": ["yarn.lock"],
        }
        pkg_managers: List[str] = []
        for pkg_manager, files in pkg_manager_files.items():
            if any(dest_dir.joinpath(file).is_file() for file in files):
                pkg_managers.append(pkg_manager)
        return pkg_managers

    def _update_yum_update_commands(self, metadata: ImageMetadata, force_yum_updates: bool, df_fileobj: io.TextIOBase) -> io.StringIO:
        """ If force_yum_updates is True, inject "yum updates -y" in the final build stage; Otherwise, remove the lines we injected.
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
                self._logger.warning("Will not inject `USER 0` before `yum update -y` for the final build stage because `final_stage_user` is missing (or 0) in image meta."
                                     " If this build fails with `yum update -y` permission denied error, please set correct `final_stage_user` and rebase again.")
            output.write(f"# {yum_update_line_flag}\n{yum_update_line}  # set final_stage_user in ART metadata if this fails\n")
            if final_stage_user:
                output.write(f"# {yum_update_line_flag}\nUSER {final_stage_user}\n")
        output.seek(0)
        return output

    def _resolve_image_from_upstream_parent(self, original_parent: str, dfp: DockerfileParser) -> Optional[str]:
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
            labels = util.oc_image_info__caching(original_parent)['config']['config']['Labels']

            # Get builder X.Y
            major, minor, _ = util.extract_version_fields(labels['version'])

            # Get builder EL version
            el_version = release_util.isolate_el_version_in_release(labels['release'])

            # Get expected stream name
            for stream in self._runtime.streams.values():
                image = stream['image']
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
            ""
        )

        return None

    def _update_environment_variables(self, metadata: ImageMetadata, source: Optional[SourceResolution],
                                      df_path: Path, build_update_envs: Dict[str, str],
                                      metadata_envs: Dict[str, str]):
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
        if metadata.config.content.set_build_variables is not Missing and not metadata.config.content.set_build_variables:
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
            with df_path.open('w', encoding="utf-8") as df:
                df.write(dfp_content)

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
        if do_set_build_variables and source and env_vars_from_source is not None:  # If None, no merge has occurred. Anything else means it has.
            env_vars_from_source.update(dict(
                SOURCE_GIT_COMMIT=source.commit_hash,
                SOURCE_GIT_TAG=source.latest_tag,
                SOURCE_GIT_URL=source.public_upstream_url,
                SOURCE_DATE_EPOCH=str(int(source.committer_date.timestamp())),
                OS_GIT_VERSION=f'{build_update_envs["OS_GIT_VERSION"]}-{source.commit_hash_short}',
                OS_GIT_COMMIT=f'{source.commit_hash_short}'
            ))

            merge_env_line = f"ENV {env_merge_line_flag} " + get_env_set_list(env_vars_from_source)

        # Open again!
        dfp = DockerfileParser(str(df_path))
        df_lines = dfp.content.splitlines(False)

        with df_path.open('w', encoding="utf-8") as df:
            for line in df_lines:

                # Always remove the env line we update each time.
                if env_update_line_flag in line:
                    continue

                # If we are adding environment variables from source, remove any previous merge line.
                if merge_env_line and env_merge_line_flag in line:
                    continue

                df.write(f'{line}\n')

                if line.startswith('FROM '):
                    if update_env_line:
                        df.write(f'{update_env_line}\n')
                    if merge_env_line:
                        df.write(f'{merge_env_line}\n')

    def _reflow_labels(self, df_path: Path):
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
        with df_path.open('w', encoding="utf-8") as df:
            df.write("%s\n\n" % df_content)
            if labels:
                df.write("LABEL")
                for k, v in labels.items():
                    df.write(" \\\n")  # All but the last line should have line extension backslash "\"
                    escaped_v = v.replace('"', '\\"')  # Escape any " with \"
                    df.write("        %s=\"%s\"" % (k, escaped_v))
                df.write("\n\n")

    def _get_csv_file_and_refs(self, metadata: ImageMetadata, repo_dir: Path, csv_config):
        #           bundle-dir: stable/
        #   manifests-dir: manifests/
        manifests_dir = csv_config.get('manifests-dir', 'manifests')
        gvars = self._runtime.group_config.vars
        bundle_dir = csv_config.get('bundle-dir', f'{gvars["MAJOR"]}.{gvars["MINOR"]}')
        bundle_manifests_dir = os.path.join(manifests_dir, bundle_dir)

        ref_candidates = [
            repo_dir.joinpath(dirpath, 'image-references')
            for dirpath in [bundle_manifests_dir, manifests_dir, bundle_dir]
        ]
        refs = next((cand for cand in ref_candidates if cand.is_file()), None)
        if not refs:
            raise FileNotFoundError('{}: image-references file not found in any location: {}'.format(metadata.distgit_key, ref_candidates))

        with io.open(refs, 'r') as f_ref:
            ref_data = yaml.full_load(f_ref)
        image_refs = ref_data.get('spec', {}).get('tags', {})
        if not image_refs:
            raise ValueError('Data in {} not valid'.format(refs))

        csvs = list(repo_dir.joinpath(bundle_manifests_dir).glob('*.clusterserviceversion.yaml'))
        if len(csvs) < 1:
            raise FileNotFoundError('{}: did not find a *.clusterserviceversion.yaml file @ {}'.format(metadata.distgit_key, bundle_manifests_dir))
        elif len(csvs) > 1:
            raise IOError('{}: Must be exactly one *.clusterserviceversion.yaml file but found more than one @ {}'.format(metadata.distgit_key, bundle_manifests_dir))
        return str(csvs[0]), image_refs

    def _update_csv(self, metadata: ImageMetadata, dest_dir: Path, version: str, release: str, image_repo: str, uuid_tag: str):
        csv_config = metadata.config.get('update-csv', None)
        if not csv_config:
            return

        csv_file, image_refs = self._get_csv_file_and_refs(metadata, dest_dir, csv_config)
        registry = csv_config['registry'].rstrip("/")
        image_map = csv_config.get('image-map', {})

        def _map_image_name(name, image_map):
            for match, replacement in image_map.items():
                if name.find(match) != -1:
                    return name.replace(match, replacement)
            return name

        for ref in image_refs:
            name = ref['name']
            name = _map_image_name(name, image_map)
            spec = ref['from']['name']

            distgit = self._runtime.name_in_bundle_map.get(name, None)
            # fail if upstream is referring to an image we don't actually build
            if not distgit:
                raise ValueError('Unable to find {} in image-references data for {}'.format(name, metadata.distgit_key))

            meta = self._runtime.image_map.get(distgit, None)
            if meta:  # image is currently be processed
                image_tag = f"{image_repo}:{meta.image_name_short}-{uuid_tag}"
            else:
                meta = self._runtime.late_resolve_image(distgit)
                assert meta is not None
                _, v, r = meta.get_latest_build_info()
                image_tag = '{}:{}-{}'.format(meta.image_name_short, v, r)

            if metadata.distgit_key != meta.distgit_key:
                if metadata.distgit_key not in meta.config.dependents:
                    raise ValueError(f'Related image contains {meta.distgit_key} but this does not have {metadata.distgit_key} in dependents')

            namespace = self._runtime.group_config.get('csv_namespace', None)
            if not namespace:
                raise ValueError('csv_namespace is required in group.yaml when any image defines update-csv')
            replace = '{}/{}/{}'.format(registry, namespace, image_tag)

            with io.open(csv_file, 'r+', encoding="utf-8") as f:
                content = f.read()
                content = content.replace(spec + '\n', replace + '\n')
                content = content.replace(spec + '\"', replace + '\"')
                f.seek(0)
                f.truncate()
                f.write(content)

        if version.startswith('v'):
            version = version[1:]  # strip off leading v

        x, y, z = version.split('.')[0:3]

        replace_args = {
            'MAJOR': x,
            'MINOR': y,
            'SUBMINOR': z,
            'RELEASE': release,
            'FULL_VER': '{}-{}'.format(version, release.split('.')[0])
        }

        manifests_dir = csv_config.get('manifests-dir', 'manifests')
        manifests_base = os.path.join(dest_dir, manifests_dir)

        art_yaml = os.path.join(manifests_base, 'art.yaml')

        if os.path.isfile(art_yaml):
            with io.open(art_yaml, 'r', encoding="utf-8") as art_file:
                art_yaml_str = art_file.read()

            try:
                art_yaml_str = art_yaml_str.format(**replace_args)
                art_yaml_data = yaml.full_load(art_yaml_str)
            except Exception as ex:  # exception is low level, need to pull out the details and rethrow
                raise IOError('Error processing art.yaml!\n{}\n\n{}'.format(str(ex), art_yaml_str))

            updates = art_yaml_data.get('updates', [])
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
                with io.open(f_path, 'r+', encoding="utf-8") as sr_file:
                    sr_file_str = sr_file.read()
                    for sr in u_list:
                        s = sr.get('search', None)
                        r = sr.get('replace', None)
                        if not s or not r:
                            raise ValueError('Must provide `search` and `replace` fields in art.yaml `update_list`')

                        original_string = sr_file_str
                        sr_file_str = sr_file_str.replace(s, r)
                        if sr_file_str == original_string:
                            self._logger.error(f'Search `{s}` and replace was ineffective for {metadata.distgit_key}')
                    sr_file.seek(0)
                    sr_file.truncate()
                    sr_file.write(sr_file_str)

        previous_build_versions = self._find_previous_versions(metadata)
        if previous_build_versions:
            # We need to inject "skips" versions for https://issues.redhat.com/browse/OCPBUGS-6066 .
            # We have the versions, but it needs to be written into the CSV under spec.skips.
            # First, find the "name" of this plugin that precedes the version. If everything
            # is correctly replaced in the CSV by art-config.yml, then metadata.name - spec.version
            # should leave us with the name the operator uses.
            csv_obj = yaml.safe_load(Path(csv_file).read_text())
            olm_name = csv_obj['metadata']['name']  # "nfd.4.11.0-202205301910"
            olm_version = csv_obj['spec']['version']  # "4.11.0-202205301910"

            if not olm_name.endswith(olm_version):
                raise IOError(f'Expected {metadata.name} CSV metadata.name field ("{olm_name}" after rebase) to be suffixed by spec.version ("{olm_version}" after rebase). art-config.yml / upstream CSV metadata may be incorrect.')

            olm_name_prefix = olm_name[:-1 * len(olm_version)]  # "nfd."

            # Inject the skips..
            csv_obj['spec']['skips'] = [f'{olm_name_prefix}{old_version}' for old_version in previous_build_versions]

            # Re-write the CSV content.
            Path(csv_file).write_text(yaml.dump(csv_obj))

    def _find_previous_versions(self, metadata: ImageMetadata, pattern_suffix='') -> Set[str]:
        """
        Returns: Searches brew for builds of this operator in order and processes them into a set of versions.
        These version may or may not have shipped.
        """
        # FIXME: This is based on Brew/OSBS. Needs to figure out how to do this with Konflux
        with self._runtime.pooled_koji_client_session() as koji_api:
            component_name = metadata.get_component_name()
            package_info = koji_api.getPackage(component_name)  # e.g. {'id': 66873, 'name': 'atomic-openshift-descheduler-container'}
            if not package_info:
                raise IOError(f'No brew package is defined for {component_name}')
            package_id = package_info['id']  # we could just constrain package name using pattern glob, but providing package ID # should be a much more efficient DB query.
            pattern_prefix = f'{component_name}-v{metadata.branch_major_minor()}.'
            builds = koji_api.listBuilds(packageID=package_id,
                                         state=BuildStates.COMPLETE.value,
                                         pattern=f'{pattern_prefix}{pattern_suffix}*')
            nvrs: Set[str] = set([build['nvr'] for build in builds])
            # NVRS should now be a set including entries like 'cluster-nfd-operator-container-v4.10.0-202211280957.p0.ga42b581.assembly.stream'
            # We need to convert these into versions like "4.11.0-202205250107"
            versions: Set[str] = set()
            for nvr in nvrs:
                without_component = nvr[len(f'{component_name}-v'):]  # e.g. "4.10.0-202211280957.p0.ga42b581.assembly.stream"
                version_components = without_component.split('.')[0:3]  # e.g. ['4', '10', '0-202211280957']
                version = '.'.join(version_components)
                versions.add(version)

            return versions
