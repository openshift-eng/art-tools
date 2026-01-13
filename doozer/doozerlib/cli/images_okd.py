import asyncio
import io
import logging
import os
import pathlib
import re
import shutil
import sys
import tempfile
from collections import OrderedDict
from copy import copy
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import aiohttp
import click
import openshift_client as oc
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import yellow_print
from artcommonlib.git_helper import git_clone
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from artcommonlib.util import (
    convert_remote_git_to_ssh,
    deep_merge,
    download_file_from_github,
    remove_prefix,
    split_git_url,
)
from artcommonlib.variants import BuildVariant
from dockerfile_parse import DockerfileParser
from github import Github, UnknownObjectException

from doozerlib import Runtime, constants
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli import (
    cli,
    click_coroutine,
    option_commit_message,
    option_push,
    pass_runtime,
    validate_semver_major_minor_patch,
)
from doozerlib.image import ImageMetadata
from doozerlib.source_resolver import SourceResolver
from doozerlib.state import STATE_FAIL, STATE_PASS
from doozerlib.util import extract_version_fields, what_is_in_master

OKD_DEFAULT_IMAGE_REPO = 'quay.io/redhat-user-workloads/ocp-art-tenant/art-okd-images'


class ImageCoordinate(NamedTuple):
    namespace: str
    name: str
    tag: str

    def unique_key(self):
        return f'{self.namespace}_{self.name}_{self.tag}'

    def as_dict(self):
        return {
            'namespace': self.namespace,
            'name': self.name,
            'tag': self.tag,
        }


@cli.group("images:okd", short_help="Manage OKD builds in prow.")
def images_okd():
    """
    Sub-verbs to managing the content of OKD builds.
    """
    pass


class OkdRebaseCli:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        release: str,
        image_repo: str,
        message: str,
        push: bool,
    ):
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.version = version
        self.release = release
        self.image_repo = image_repo
        self.message = message
        self.push = push
        self.upcycle = runtime.upcycle
        self.state = {}

    async def run(self):
        self.runtime.initialize(mode='images', clone_distgits=False, build_system='konflux')

        group_config = self.runtime.group_config.copy()
        if group_config.get('okd', None):
            self.logger.info('Using OKD group configuration')
            group_config = deep_merge(group_config, group_config['okd'])
            self.runtime.group_config = Model(group_config)

        # For OKD, we need to use the OKD group variant (e.g., okd-4.20 instead of openshift-4.20)
        # This ensures the Konflux DB cache is loaded for the correct group
        major, minor = self.runtime.get_major_minor_fields()
        self.runtime.group = f'okd-{major}.{minor}'
        self.logger.info(f'Changed runtime group to OKD variant: {self.runtime.group}')

        base_dir = Path(self.runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_OKD_SOURCES)
        rebaser = KonfluxRebaser(
            runtime=self.runtime,
            base_dir=base_dir,
            source_resolver=self.runtime.source_resolver,
            repo_type='unsigned',
            upcycle=self.upcycle,
            variant=BuildVariant.OKD,
            image_repo=self.image_repo,
        )

        metas = self.runtime.ordered_image_metas()
        tasks = [self.rebase_image(image_meta, rebaser) for image_meta in metas]
        await asyncio.gather(*tasks, return_exceptions=False)

        # Check rebase results, log errors if any in state.yml
        self.runtime.state.setdefault('images:okd:rebase', {})['images'] = self.state

        if any((state == 'failure' for state in self.state.values())):
            self.runtime.state['status'] = STATE_FAIL
            sys.exit(1)

        else:
            self.runtime.state['status'] = STATE_PASS

    async def rebase_image(self, image_meta: ImageMetadata, rebaser: KonfluxRebaser):
        image_meta.config = self.get_okd_image_config(image_meta)
        image_name = image_meta.distgit_key

        if image_meta.config.mode == 'disabled':
            # Raise an exception to be caught in okd4 pipeline; image will be removed from the building list.
            self.logger.warning('Image %s is disabled for OKD: skipping rebase', image_name)
            image_meta.rebase_status = True
            image_meta.rebase_event.set()
            self.state[image_name] = 'skipped'
            return

        try:
            await rebaser.rebase_to(
                image_meta,
                self.version,
                self.release,
                force_yum_updates=False,
                commit_message=self.message,
                push=self.push,
            )
            self.state[image_name] = 'success'

        except Exception as e:
            self.logger.warning('Failed rebasing %s: %s', image_name, e)
            self.state[image_name] = 'failure'

    def get_okd_image_config(self, image_meta: ImageMetadata):
        image_config = copy(image_meta.config)
        image_config.enabled_repos = []

        if image_config.okd is not Missing:
            # Merge the rest of the config, with okd taking precedence
            # Certain fields like 'from' should be completely replaced, not merged
            self.logger.info('Merging OKD configuration into image configuration for %s', image_meta.distgit_key)
            base_config = image_config.primitive()
            okd_config = image_meta.config.okd.primitive()

            # Fields that should be completely replaced rather than merged
            replace_fields = {'from'}

            # First, do a deep merge for fields that should be merged
            merged_config = deep_merge(base_config, okd_config)

            # Then, replace fields that should be completely replaced
            for field in replace_fields:
                if field in okd_config:
                    merged_config[field] = okd_config[field]

            image_config = Model(merged_config)

        return image_config


@images_okd.command("rebase", short_help="Refresh a group's OKD source content.")
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=validate_semver_major_minor_patch,
    help="Version string to populate in Dockerfiles.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@click.option('--image-repo', default=OKD_DEFAULT_IMAGE_REPO, help='Image repo for base images')
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def images_okd_rebase(
    runtime: Runtime,
    version: str,
    release: str,
    image_repo: str,
    message: str,
    push: bool,
):
    """
    Refresh a group's konflux content from source content.
    """

    runtime.network_mode_override = 'open'  # OKD builds must be done in open mode.

    await OkdRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        image_repo=image_repo,
        message=message,
        push=push,
    ).run()


def resolve_okd_from_stream(runtime, stream_name):
    stream_config = runtime.resolve_stream(stream_name)
    okd_preferred = stream_config.okd.resolve_as.image
    if okd_preferred:
        return okd_preferred
    # Fall back to a standard CI image.
    if stream_config.upstream_image:
        return stream_config.upstream_image
    # No "upstream_image", fallback to just "image".
    return stream_config.image


def resolve_okd_from_image_meta(runtime, image_meta, okd_version):
    """
    Given an ImageMeta, return the OKD pullspec that represents
    the image in the CI registry.
    """
    ci_alignment_config = image_meta.config.content.source.ci_alignment
    okd_alignment_config = image_meta.config.content.source.okd_alignment
    if okd_alignment_config.resolve_as:
        resolve_as = image_meta.config.content.source.okd_alignment.resolve_as
        # The image is specifying that it normal rules should not apply and
        # to resolve this image_meta as the specified value. This can
        # be a .stream or .image .
        if resolve_as.stream:
            return resolve_okd_from_stream(runtime, resolve_as.stream)
        elif resolve_as.image:
            return resolve_as.image  # Return literal pullspec
        else:
            raise IOError(f'Unable to interpret resolve_as for {image_meta.distgit_key}')

    if okd_alignment_config.tag_name:
        ci_payload_tag_name = okd_alignment_config.tag_name
    elif ci_alignment_config.upstream_image:
        return ci_alignment_config.upstream_image
    else:
        name = image_meta.config.payload_name or image_meta.config.name
        image_name = name.split('/')[-1]
        # In release payloads, images are promoted into an imagestream
        # tag name without the ose- prefix.
        image_name = remove_prefix(image_name, 'ose-')
        ci_payload_tag_name = image_name

    # e.g. registry.ci.openshift.org/origin/scos-4.16:base
    return f'registry.ci.openshift.org/origin/scos-{okd_version}:{ci_payload_tag_name}'


def resolve_okd_from_entry(runtime, image_meta, from_entry, okd_version):
    """
    Resolves a "from" entry into an OKD pullspec.
    :param runtime: The runtime object
    :param image_meta: The image metadata which owns the from entry.
    :param from_entry: A builder or from entry. e.g. { 'member': 'openshift-enterprise-base' }  or { 'stream': 'golang; }
    :return: The upstream CI pullspec to which this entry should resolve.
    """

    if from_entry.member:
        target_meta = runtime.resolve_image(from_entry.member, True)
        return resolve_okd_from_image_meta(runtime, target_meta, okd_version=okd_version)

    if from_entry.stream:
        return resolve_okd_from_stream(runtime, from_entry.stream)

    if from_entry.image:
        # A literal image is already being provided.
        return from_entry.image

    raise IOError(f'Unable to resolve from entry for {image_meta.distgit_key}: {from_entry}')


def convert_to_imagestream_coordinate(registry_ci_pullspec: str) -> ImageCoordinate:
    if not registry_ci_pullspec.startswith('registry.ci.openshift.org/'):
        raise IOError(f'OKD images must be sourced from registry.ci.openshift.org; cannot use {registry_ci_pullspec}')
    parts = registry_ci_pullspec.split('/')
    return ImageCoordinate(namespace=parts[1], name=parts[2].split(':')[0], tag=parts[2].split(':')[1])


def _get_upstream_source(runtime, image_meta, skip_branch_check=False):
    """
    Analyzes an image metadata to find the upstream URL and branch associated with its content.
    :param runtime: The runtime object
    :param image_meta: The metadata to inspect
    :param skip_branch_check: Skip check branch exist every time
    :return: A tuple containing (url, branch) for the upstream source OR (None, None) if there
            is no upstream source.
    """
    if "git" in image_meta.config.content.source:
        source_repo_url = image_meta.config.content.source.git.url
        source_repo_branch = image_meta.config.content.source.git.branch.target
        if not skip_branch_check:
            branch_check, err = exectools.cmd_assert(
                f'git ls-remote --heads {source_repo_url} {source_repo_branch}', strip=True, retries=3
            )
            if not branch_check:
                # Output is empty if branch does not exist
                source_repo_branch = image_meta.config.content.source.git.branch.fallback
                if source_repo_branch is Missing:
                    raise IOError(f'Unable to detect source repository branch for {image_meta.distgit_key}')
    elif "alias" in image_meta.config.content.source:
        alias = image_meta.config.content.source.alias
        if alias not in runtime.group_config.sources:
            raise IOError(f'Unable to find source alias {alias} for {image_meta.distgit_key}')
        source_repo_url = runtime.group_config.sources[alias].url
        source_repo_branch = runtime.group_config.sources[alias].branch.target
    else:
        # No upstream source, no PR to open
        return None, None

    return source_repo_url, source_repo_branch


@images_okd.group("prs", short_help="Manage pull requests for OKD ci-operator configuration.")
def prs():
    pass


OrgRepoKey = str
GitHubOrgName = str
GitHubRepoName = str
CiOperatorImageName = str


def get_okd_payload_tag_name(image_meta) -> str:
    payload_tag = image_meta.config.content.source.okd_alignment.tag_name
    if not payload_tag:
        if image_meta.is_payload:
            payload_tag, _ = image_meta.get_payload_tag_info()
        else:
            # If there is no okd_alignment tag_name override AND no payload tag (e.g. for a builder or base
            # image), use the stripped image_name as the tag name.
            image_name_short = image_meta.image_name_short
            payload_tag = image_name_short[4:] if image_name_short.startswith("ose-") else image_name_short
    return payload_tag


def get_needed_for_okd_payload_constructions(image_meta):
    """
    For a CI imagestream, we need tags for all relevant images, even if they
    are just non-final stage images. So search to determine if the image is a
    parent of an image in the payload.
    """
    if image_meta.config.for_payload:
        return True
    for child in image_meta.children:
        if get_needed_for_okd_payload_constructions(child):
            return True
    # See if this image is used as a builder image.
    for test_meta in image_meta.runtime.ordered_image_metas():
        for test_builder in test_meta.config['from'].builder:
            if test_builder.member == image_meta.distgit_key:
                return True
    return False


class CiOperatorImageConfig:
    def __init__(self, image_meta: ImageMetadata, promotion_namespace: str, promotion_imagestream: str):
        self.image_meta = image_meta
        self.promotion_namespace = promotion_namespace
        self.promotion_imagestream = promotion_imagestream
        self.dockerfile_path = 'Dockerfile'
        self.context_dir: Optional[str] = None
        self.raw_steps = list()
        self.resources = dict()

        self.payload_tag = get_okd_payload_tag_name(image_meta)

        self.base_image: Optional[ImageCoordinate] = None
        self.coordinate = ImageCoordinate(
            namespace=promotion_namespace, name=promotion_imagestream, tag=self.payload_tag
        )
        self.replacements: Dict[ImageCoordinate, List[str]] = dict()

    def add_replacement(self, image_coordinate: ImageCoordinate, pullspecs_or_stages_to_replace: List[str]):
        self.replacements[image_coordinate] = pullspecs_or_stages_to_replace

    def add_from(self, image_coordinate: ImageCoordinate):
        self.base_image = image_coordinate

    def set_dockerfile_path(self, dockerfile_path: str):
        self.dockerfile_path = dockerfile_path.lstrip('/')

    def set_context_dir(self, context_dir: str):
        self.context_dir = context_dir

    def get_obj(self, ci_operator_image_names: Dict[ImageCoordinate, str]) -> Tuple[Dict, Dict]:
        image_meta = self.image_meta
        base_obj = {}
        raw_step = {}

        build_args = [  # Used by some images to differentiate output during prow/ci-operator based builds.
            {
                'name': 'TAGS',
                'value': 'scos',
            },
        ]
        if image_meta.config.content.source.okd_alignment.build_args:
            build_args.extend(image_meta.config.content.source.okd_alignment.build_args.primitive())

        if image_meta.config.content.source.okd_alignment.resources:
            self.resources.update(image_meta.config.content.source.okd_alignment.resources)

        if image_meta.config.content.source.okd_alignment.inject_rpm_repositories:
            intermediate_tag = f'pre-repo-{self.payload_tag}'

            repo_def_lines: List[str] = list()
            for entry in image_meta.config.content.source.okd_alignment.inject_rpm_repositories:
                repo_id = entry.id
                baseurl = entry.baseurl
                if not repo_id or not baseurl:
                    raise IOError(f'Incomplete repo injection data for {image_meta.distgit_key}: {repo_id} / {baseurl}')

                repo_def_lines.extend(
                    [
                        f'[{repo_id}]',
                        f'id = {repo_id}',
                        f'name = {repo_id}',
                        f'baseurl = {baseurl}',
                        'enabled = 1',
                        'gpgcheck = 0',
                        'sslverify = false',
                        'skip_if_unavailable = true',
                        '',
                    ]
                )

            repo_lines = '\n'.join(repo_def_lines)
            raw_step = {
                'pipeline_image_cache_step': {
                    'commands': f"""
cat << EOF > /etc/yum.repos.d/art.repo
{repo_lines}
EOF
        """,
                    'from': ci_operator_image_names[self.base_image],
                    'to': intermediate_tag,
                },
            }

            base_obj = {
                'build_args': build_args,
                'from': intermediate_tag,
                'to': self.payload_tag,
            }

        else:
            base_obj = {
                'build_args': build_args,
                'to': self.payload_tag,
            }

            if self.base_image:
                base_obj['from'] = ci_operator_image_names[self.base_image]

        prowjob_dockerfile_path = self.dockerfile_path

        if self.context_dir:
            # Very rarely, a prow base build will require a context_dir to be set.
            base_obj['context_dir'] = self.context_dir
            if prowjob_dockerfile_path.startswith(self.context_dir):
                prowjob_dockerfile_path = prowjob_dockerfile_path[len(self.context_dir) :].lstrip('/')
            else:
                raise IOError('Expected path to start with prowjob context_dir')

        if self.dockerfile_path:
            base_obj['dockerfile_path'] = prowjob_dockerfile_path

        inputs = dict()

        for coordinate, replacements in self.replacements.items():
            inputs[ci_operator_image_names[coordinate]] = {
                'as': replacements,
            }

        if inputs:
            base_obj['inputs'] = inputs

        return base_obj, raw_step


class CiOperatorConfig:
    def __init__(
        self,
        org: GitHubOrgName,
        repo: GitHubRepoName,
        branch_name: str,
        promotion_namespace: str,
        promotion_imagestream: str,
    ):
        self.org = org
        self.repo = repo
        self.branch_name = branch_name
        self.ci_operator_image_configs: OrderedDict[CiOperatorImageName, CiOperatorImageConfig] = OrderedDict()

        self.promotion_namespace = promotion_namespace
        self.promotion_imagestream = promotion_imagestream
        self.promotion_to = list()
        self.promotion = {
            'to': [
                {
                    'namespace': promotion_namespace,
                    'name': promotion_imagestream,
                },
            ],
        }
        self.tests = None
        self.build_root = None
        self.releases = dict()
        self.complete = False  # There are stages to preparing a ci-operator config. If it is not completed successfully, don't write it to openshift/release.

    def get_image_config(self, image_meta: ImageMetadata) -> CiOperatorImageConfig:
        if image_meta.distgit_key not in self.ci_operator_image_configs:
            self.ci_operator_image_configs[image_meta.distgit_key] = CiOperatorImageConfig(
                image_meta, self.promotion_namespace, self.promotion_imagestream
            )
        return self.ci_operator_image_configs[image_meta.distgit_key]

    def add_release(self, release_name: str, release_def):
        self.releases[release_name] = release_def

    def set_build_root(self, build_root: ImageCoordinate):
        self.build_root = build_root.as_dict()

    def set_tests(self, tests: List):
        self.tests = tests

    def get_ci_operator_config_path(self, release_clone_dir: Path):
        return release_clone_dir.joinpath(
            'ci-operator', 'config', self.org, self.repo, f'{self.org}-{self.repo}-{self.branch_name}__okd-scos.yaml'
        )

    def write_config(self, release_clone_dir: Path):
        output_path = self.get_ci_operator_config_path(release_clone_dir)

        if not self.complete:
            print(f'Refusing to write: {output_path} as reconciliation did not complete')
            return

        # Default resources, which can be adjusted by okd_alignment stanzas
        resources = {'*': {'requests': {'cpu': '100m', 'memory': '200Mi'}}}

        ci_operator_image_names: Dict[ImageCoordinate, str] = dict()
        base_image_defs: OrderedDict[ImageCoordinate, True] = OrderedDict()
        # Start by assuming that all image dependencies are base images. We will remove from
        # base images as we discover images that are built by this config.
        for _, image_config in self.ci_operator_image_configs.items():
            for coordinate, _ in image_config.replacements.items():
                base_image_defs[coordinate] = True
                ci_operator_image_names[coordinate] = coordinate.unique_key()
            base_image = image_config.base_image
            if base_image:
                base_image_defs[base_image] = True
                ci_operator_image_names[base_image] = base_image.unique_key()

        # For any image that we build in the config, remove from base images
        for _, image_config in self.ci_operator_image_configs.items():
            if image_config.coordinate in base_image_defs:
                base_image_defs.pop(image_config.coordinate)
            ci_operator_image_names[image_config.coordinate] = image_config.payload_tag

        images = []
        raw_steps = []
        for _, image_config in self.ci_operator_image_configs.items():
            image_obj, raw_step = image_config.get_obj(ci_operator_image_names)
            images.append(image_obj)
            if raw_step:
                raw_steps.append(raw_step)
            if image_config.resources:
                # If an image has a resource request, include it in the overall request
                resources.update(image_config.resources)

        base_images: Dict[str, Any] = dict()
        for coordinate, _ in base_image_defs.items():
            base_images[ci_operator_image_names[coordinate]] = coordinate.as_dict()

        config = {
            'images': images,
            'promotion': self.promotion,
            'resources': resources,
        }

        if self.tests:
            config['tests'] = self.tests
        if raw_steps:
            config['raw_steps'] = raw_steps
        if base_images:
            config['base_images'] = base_images
        if self.build_root:
            config['build_root'] = {
                'image_stream_tag': self.build_root,
            }
        if self.releases:
            config['releases'] = self.releases
        output_path.write_text(yaml.safe_dump(config))
        print(f'Wrote: {str(output_path)}')


class CiOperatorConfigs:
    """
    A data structure which builds up and stores ci-operator configurations for
    each repository. Each repository has a set of images that need to be built
    for OKD. Each repo will have a ci-operator configuration specific to OKD.
    """

    def __init__(self, promotion_namespace: str, promotion_imagestream: str):
        self.configs_by_repo: Dict[OrgRepoKey, CiOperatorConfig] = dict()
        self.promotion_namespace = promotion_namespace
        self.promotion_imagestream = promotion_imagestream

    def get_ci_operator_config(self, org: GitHubOrgName, repo: GitHubRepoName, branch_name: str) -> CiOperatorConfig:
        config_key = f'{org}:{repo}:{branch_name}'
        if config_key not in self.configs_by_repo:
            self.configs_by_repo[config_key] = CiOperatorConfig(
                org,
                repo,
                branch_name,
                promotion_namespace=self.promotion_namespace,
                promotion_imagestream=self.promotion_imagestream,
            )
        return self.configs_by_repo[config_key]

    def write_configs(self, release_clone_dir: Path):
        for _, config in self.configs_by_repo.items():
            config.write_config(release_clone_dir)


def image_from(from_value):
    """
    :param from_value: string like "image:tag" or "image:tag AS name"
    :return: tuple of the image and stage name, e.g. ("image:tag", None)
    """
    regex = re.compile(r"""(?xi)     # readable, case-insensitive regex
        \s*                          # ignore leading whitespace
        (?P<image> \S+ )             # image and optional tag
        (?:                          # optional "AS name" clause for stage
            \s+ AS \s+
            (?P<name> \S+ )
        )?
        """)
    match = re.match(regex, from_value)
    return match.group('image', 'name') if match else (None, None)


def extract_stage_names(dfp: DockerfileParser) -> List[str]:
    """
    :return: list of stage names in the parsed Dockerfile. If a FROM has no AS, None is included in the list.
    """
    stage_names: List[str] = list()
    for instr in dfp.structure:
        if instr['instruction'] == 'FROM':
            image, stage_name = image_from(instr['value'])
            stage_names.append(stage_name)
    return stage_names


@images_okd.command('check', short_help='Check whether images in OKD are SCOS based on app.ci.')
@pass_runtime
def images_okg_check(runtime):
    runtime.initialize(clone_distgits=False, clone_source=False)
    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']
    okd_version = f'{major}.{minor}'

    payload_tag_names = set()
    for image_meta in runtime.ordered_image_metas():
        if get_needed_for_okd_payload_constructions(image_meta):
            dest_pull_spec = resolve_okd_from_image_meta(runtime, image_meta, okd_version)
            image_coordinate = convert_to_imagestream_coordinate(dest_pull_spec)
            if image_coordinate.namespace == 'origin' and image_coordinate.name == f'scos-{okd_version}':
                payload_tag_names.add(image_coordinate.tag)

    with oc.project('origin'):
        scos_imagestream = oc.selector(f'is/scos-{okd_version}').object()
        print('TAG,SCOS,DETAIL,PULLSPEC')
        for tag_entry in scos_imagestream.model.status.tags:
            detail = ''
            tag = tag_entry.tag
            latest_pullspec = None
            scos_status = "No"

            if tag_entry.items:
                latest_pullspec: str = tag_entry['items'][0].dockerImageReference
                latest_pullspec = latest_pullspec.replace(
                    'image-registry.openshift-image-registry.svc:5000', 'registry.ci.openshift.org'
                )

            if tag == 'branding':
                scos_status = "N/A"
                detail = "branching is from scratch"
            elif latest_pullspec:
                temp_dir = tempfile.mkdtemp()
                try:
                    oc.invoke('image', cmd_args=['extract', f'--path=/etc/centos-release:{temp_dir}', latest_pullspec])
                    if tag not in payload_tag_names:
                        detail = 'not in ART built OKD tags'

                    if os.path.exists(f'{temp_dir}/centos-release'):
                        scos_status = "Yes"
                    else:
                        if tag not in payload_tag_names:
                            scos_status = "N/A?"

                except:
                    detail = 'error checking image'
                shutil.rmtree(temp_dir)
            else:
                detail = 'does not have any items in its pullspec'

            print(f'{tag},{scos_status},{detail},{latest_pullspec}')
            if tag in payload_tag_names:
                payload_tag_names.remove(tag)

        for remaining_tag in payload_tag_names:
            scos_status = 'No'
            detail = 'Required in payload but not found in imagestream'
            latest_pullspec = ''
            print(f'{remaining_tag},{scos_status},{detail},{latest_pullspec}')


@prs.command(
    'open', short_help='Open PRs against upstream component repos that have a FROM that differs from ART metadata.'
)
@click.option('--github-access-token', metavar='TOKEN', required=True, help='Github access token for user.')
@click.option('--okd-version', metavar='VERSION', required=True, help='Which OKD stream to promote to.')
@click.option(
    '--ignore-missing-images', default=False, is_flag=True, help='Do not exit if an image is missing upstream.'
)
@click.option('--draft-prs', default=False, is_flag=True, help='Open PRs as draft PRs')
@click.option('--moist-run', default=False, is_flag=True, help='Do everything except opening the final PRs')
@click.option(
    '--add-auto-labels',
    default=False,
    is_flag=True,
    help='Add auto_labels to PRs; unless running as openshift-bot, you probably lack the privilege to do so',
)
@click.option(
    '--add-label',
    default=[],
    multiple=True,
    help='Add a label to all open PRs (new and existing) - Requires being openshift-bot',
)
@click.option(
    '--non-master', default=False, is_flag=True, help='Acknowledge that this is a non-master branch and proceed anyway'
)
@pass_runtime
@click_coroutine
async def images_okd_prs(
    runtime,
    github_access_token,
    ignore_missing_images,
    okd_version,
    draft_prs,
    moist_run,
    add_auto_labels,
    add_label,
    non_master,
):
    # OKD images are marked as disabled: true in their metadata. So make sure to load
    # disabled images.
    runtime.initialize(clone_distgits=False, clone_source=False, disabled=True)
    g = Github(login_or_token=github_access_token)
    github_user = g.get_user()

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']

    master_major, master_minor = extract_version_fields(what_is_in_master(), at_least=2)
    if major != master_major or minor != master_minor:
        if not non_master:
            # These verbs should normally only be run against the version in master.
            runtime.logger.warning(
                f'Target {major}.{minor} is not in master (it is tracking {master_major}.{master_minor}); skipping PRs'
            )
            exit(0)
    else:
        non_master = False

    # Most OKD specific configuration is housed on github.com/openshift/release . It boils down
    # to generated ci-operator configuration files. We generate those configuration files
    # in our fork of openshift/release. Establish that fork if it is not already present.
    openshift_release_repo_name = 'release'
    public_release_repo = f'openshift/{openshift_release_repo_name}'
    try:
        release_fork_repo_name = f'{github_user.login}/{openshift_release_repo_name}'
        release_fork_repo = g.get_repo(release_fork_repo_name)
    except UnknownObjectException:
        # fork of openshift/release repo doesn't exist; fork it
        release_fork_repo = github_user.create_fork(public_release_repo)

    # Clone openshift/release and populate our fork as a remote
    release_repo_url = convert_remote_git_to_ssh(f'http://github.com/openshift/{openshift_release_repo_name}')
    release_clone_dir = Path(runtime.working_dir).joinpath('clones', 'release')

    # Clone the openshift/release repo so we can open a PR if necessary.
    runtime.logger.info(f'Cloning {release_repo_url}')

    git_clone(release_repo_url, release_clone_dir, git_cache_dir=runtime.git_cache_dir)
    with Dir(release_clone_dir):
        exectools.cmd_gather('git remote remove fork')  # In case we are reusing a cloned path
        exectools.cmd_assert(f'git remote add fork {convert_remote_git_to_ssh(release_fork_repo.git_url)}')
        exectools.cmd_assert('git fetch --all', retries=3)
        exectools.cmd_assert('git checkout origin/master')
        # All content will be produced in the staging branch. It will be pushed to the public
        # reconcile branch if and only if staging differs.
        exectools.cmd_gather('git branch -d staging')  # In case we are reusing a cloned path
        exectools.cmd_assert('git checkout -b staging')

    ci_operator_configs = CiOperatorConfigs('origin', f'scos-{okd_version}')

    for image_meta in runtime.ordered_image_metas():
        dgk = image_meta.distgit_key
        logger = image_meta.logger
        logger.info(f'Analyzing image: {dgk}')

        ci_alignment_config = image_meta.config.content.source.ci_alignment
        okd_alignment_config = image_meta.config.content.source.okd_alignment

        if okd_alignment_config and okd_alignment_config.enabled is not Missing and not okd_alignment_config.enabled:
            # Make sure this is an explicit False. Missing means the default or True.
            logger.info('The image has OKD alignment disabled; ignoring')
            continue

        from_config = image_meta.config['from']
        if not from_config:
            logger.info('Skipping PR check since there is no configured .from')
            continue

        desired_parents = []

        if okd_alignment_config.resolve_as:
            # This image will not be built but instead resolved using a different image or stream.
            continue

        # There are two methods to find the desired parents for upstream Dockerfiles.
        # 1. We can analyze the image_metadata "from:" stanza and determine the upstream
        #    equivalents for streams and members. This is the most common flow of the code.
        # 2. In rare circumstances, we need to circumvent this analysis and specify the
        #    desired upstreams equivalents directly in the image metadata. This is only
        #    required when the upstream Dockerfile uses something wholly different
        #    than the downstream build. Use this method by specifying:
        #      source.okd_alignment.from: [ list of full pullspecs ]
        # We check for option 2 first
        if okd_alignment_config['from'] is not Missing:
            desired_parents = okd_alignment_config['from'].primitive()  # This should be list; so we're done.
        else:
            builders = from_config.builder or []
            for builder in builders:
                upstream_image = resolve_okd_from_entry(runtime, image_meta, builder, okd_version=okd_version)
                if not upstream_image:
                    logger.warning(f'Unable to resolve upstream image for: {builder}')
                    break
                desired_parents.append(upstream_image)

            parent_upstream_image = resolve_okd_from_entry(runtime, image_meta, from_config, okd_version=okd_version)
            if len(desired_parents) != len(builders) or not parent_upstream_image:
                logger.warning('Unable to find all ART equivalent upstream images for this image')
                continue

            desired_parents.append(parent_upstream_image)

        desired_ci_build_root_coordinate = None
        if okd_alignment_config.ci_build_root is not Missing:
            desired_ci_build_root_image = resolve_okd_from_entry(
                runtime, image_meta, okd_alignment_config.ci_build_root, okd_version=okd_version
            )

            # Split the pullspec into an openshift namespace, imagestream, and tag.
            # e.g. registry.openshift.org:999/ocp/release:golang-1.16 => tag=golang-1.16, namespace=ocp, imagestream=release
            # https://docs.ci.openshift.org/docs/architecture/ci-operator/#build-root-image
            desired_ci_build_root_coordinate = convert_to_imagestream_coordinate(desired_ci_build_root_image)
            logger.info(f'Found desired build_root state of: {desired_ci_build_root_coordinate}')
        elif ci_alignment_config.streams_prs.ci_build_root is not Missing:
            desired_ci_build_root_image = resolve_okd_from_entry(
                runtime, image_meta, ci_alignment_config.streams_prs.ci_build_root, okd_version=okd_version
            )

            # Split the pullspec into an openshift namespace, imagestream, and tag.
            # e.g. registry.openshift.org:999/ocp/release:golang-1.16 => tag=golang-1.16, namespace=ocp, imagestream=release
            # https://docs.ci.openshift.org/docs/architecture/ci-operator/#build-root-image
            desired_ci_build_root_coordinate = convert_to_imagestream_coordinate(desired_ci_build_root_image)
            logger.info(f'Found desired build_root state of: {desired_ci_build_root_coordinate}')
        else:
            # ci-operator configuration, even if it is not building anything, fails if there is no
            # build root. Just get the default golang.
            default_build_root = resolve_okd_from_entry(
                runtime,
                image_meta,
                Model(
                    {
                        'stream': 'rhel-9-golang',
                    }
                ),
                okd_version=okd_version,
            )
            desired_ci_build_root_coordinate = convert_to_imagestream_coordinate(default_build_root)

        source_repo_url, source_repo_branch = _get_upstream_source(runtime, image_meta, skip_branch_check=True)

        if not source_repo_url or 'github.com' not in source_repo_url:
            # No upstream to clone; no PRs to open
            logger.info('Skipping PR check since there is no configured github source URL')
            continue

        needed_for_payload_construction = get_needed_for_okd_payload_constructions(image_meta=image_meta)
        if not needed_for_payload_construction and not okd_alignment_config.resolve_as.tag_name:
            logger.info(f'Skipping non-OKD image: {image_meta.distgit_key}')
            continue

        public_repo_url, public_branch, _ = SourceResolver.get_public_upstream(
            source_repo_url, runtime.group_config.public_upstreams
        )
        if not public_branch:
            public_branch = source_repo_branch

        # There are two standard upstream branching styles:
        # release-4.x   : CI fast-forwards from default branch (master or main) when appropriate
        # openshift-4.x : Upstream team manages completely.
        # For the former style, we may need to open the PRs against the default branch (master or main).
        # For the latter style, always open directly against named branch
        if public_branch.startswith('release-'):
            public_branches, _ = exectools.cmd_assert(f'git ls-remote --heads {public_repo_url}', strip=True)
            public_branches = public_branches.splitlines()
            priv_branches, _ = exectools.cmd_assert(f'git ls-remote --heads {source_repo_url}', strip=True)
            priv_branches = priv_branches.splitlines()

            if non_master:
                if [bl for bl in public_branches if bl.endswith(f'/release-{major}.{minor}')]:
                    public_branch = f'release-{major}.{minor}'
                elif [bl for bl in public_branches if bl.endswith(f'/openshift-{major}.{minor}')]:
                    public_branch = f'openshift-{major}.{minor}'
                else:
                    raise IOError(f'Did not find release branch among: {public_branches}')
            else:
                if [bl for bl in public_branches if bl.endswith('/main')] and [
                    bl for bl in priv_branches if bl.endswith('/main')
                ]:
                    public_branch = 'main'
                elif [bl for bl in public_branches if bl.endswith('/master')] and [
                    bl for bl in priv_branches if bl.endswith('/master')
                ]:
                    public_branch = 'master'
                else:
                    # There are ways of determining default branch without using naming conventions, but as of today, we don't need it.
                    raise IOError(
                        f'Did not find master or main branch; unable to detect default branch: {public_branches}'
                    )

        _, org, repo_name = split_git_url(public_repo_url)

        # The upstream source for the component.
        ci_operator_config = ci_operator_configs.get_ci_operator_config(org, repo_name, public_branch)

        public_repo_url = convert_remote_git_to_ssh(public_repo_url)
        component_source_path = pathlib.Path(runtime.working_dir).joinpath('snapshots', dgk)
        component_source_path.mkdir(exist_ok=True, parents=True)

        # The path to the Dockerfile in the target branch
        if image_meta.config.content.source.okd_alignment.dockerfile is not Missing:
            # Be aware that this attribute sometimes contains path elements too.
            dockerfile_path = image_meta.config.content.source.okd_alignment.dockerfile
        elif image_meta.config.content.source.dockerfile is not Missing:
            # Be aware that this attribute sometimes contains path elements too.
            dockerfile_path = image_meta.config.content.source.dockerfile
        else:
            dockerfile_path = "Dockerfile"

        if image_meta.config.content.source.okd_alignment.path is not Missing:
            dockerfile_path = os.path.join(image_meta.config.content.source.okd_alignment.path, dockerfile_path)
        elif image_meta.config.content.source.path:
            dockerfile_path = os.path.join(image_meta.config.content.source.path, dockerfile_path)

        while True:  # Allows symlinks to be followed
            dockerfile_abs_path = component_source_path.joinpath(os.path.basename(dockerfile_path))

            async with aiohttp.ClientSession() as session:
                await download_file_from_github(
                    repository=public_repo_url,
                    branch=public_branch,
                    path=dockerfile_path,
                    token=github_access_token,
                    destination=dockerfile_abs_path,
                    session=session,
                )

            content = dockerfile_abs_path.read_text()
            if len(content.strip().splitlines()) == 1:
                # This seems to be a symlink file which just contains
                # a single line like "./Dockerfile", pointing to another
                # file which is the real destination. Follow it.
                dockerfile_path = os.path.join(os.path.dirname(dockerfile_path), content.strip())
                continue
            dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO())
            dfp.content = content
            break

        if len(desired_parents) != len(dfp.parent_images):
            # The number of FROM statements in the ART metadata does not match the number
            # of FROM statements in the upstream Dockerfile. ART's normal build process will alert
            # artists on this condition, so assume it will be handled and skip for now.
            yellow_print(
                f'Parent count mismatch for {image_meta.get_component_name()}; skipping reconciliation.\nDesired: [{desired_parents}]\nDetected[{dfp.parent_images}]'
            )
            continue

        # reconcile_url = f'{convert_remote_git_to_https(runtime.gitdata.origin_url)}/tree/{runtime.gitdata.commit_hash}/images/{os.path.basename(image_meta.config_filename)}'
        # reconcile_info = f"Reconciling with {reconcile_url}"

        ci_operator_image_config = ci_operator_config.get_image_config(image_meta)
        ci_operator_config.add_release(
            'latest',
            {'integration': {'include_built_images': True, 'namespace': 'origin', 'name': f'scos-{okd_version}'}},
        )

        stage_names = extract_stage_names(dfp)
        for index, stage_name in enumerate(
            stage_names[:-1]
        ):  # For all stages except the last (i.e. except for the base image)
            if image_meta.config['from']['builder'][index].image is not Missing:
                # An explicit image was specified. Don't try to create a coordinate.
                continue
            replace = list()
            if stage_name:
                replace.append(stage_name)
            replace.append(dfp.parent_images[index])
            ci_operator_image_config.add_replacement(convert_to_imagestream_coordinate(desired_parents[index]), replace)

        ci_operator_image_config.add_from(convert_to_imagestream_coordinate(desired_parents[-1]))
        ci_operator_image_config.set_dockerfile_path(dockerfile_path)

        if image_meta.config.content.source.okd_alignment.context_dir is not Missing:
            ci_operator_image_config.set_context_dir(image_meta.config.content.source.okd_alignment.context_dir)

        if desired_ci_build_root_coordinate:
            ci_operator_config.set_build_root(desired_ci_build_root_coordinate)

        test_on_change = {  # Set a default for all projects
            "skip_if_only_changed": "^docs/|\\.md$|^(?:.*/)?(?:\\.gitignore|OWNERS|PROJECT|LICENSE)$"
        }
        # Override if okd_alignment metadata asks us to.
        if image_meta.config.content.source.okd_alignment.run_if_changed:
            test_on_change = {"run_if_changed": image_meta.config.content.source.okd_alignment.run_if_changed}
        elif image_meta.config.content.source.okd_alignment.skip_if_only_changed:
            test_on_change = {
                "skip_if_only_changed": image_meta.config.content.source.okd_alignment.skip_if_only_changed
            }

        if image_meta.config.content.source.okd_alignment.ci_always_run:
            ci_always_run = True
        else:
            ci_always_run = False
            # When auto_run=False, prow will still run the job if the run_if_changed or skip_if_only_changed
            # files trigger the job. In virtually all cases, this is not what people expect. So just clear
            # the values to prevent jobs from starting unexpectedly.
            test_on_change = {}

        ci_operator_config.set_tests(
            [
                {
                    "always_run": ci_always_run,
                    "as": "e2e-aws-ovn",
                    "optional": True,
                    **test_on_change,
                    "steps": {"cluster_profile": "aws", "workflow": "openshift-e2e-aws"},
                }
            ]
        )

        # We made it through everything, so assume the config is safe to write out.
        ci_operator_config.complete = True

    ci_operator_configs.write_configs(release_clone_dir)
    print(
        'It is necessary to run "make ci-operator-config", "make jobs", "make openshift-image-mirror-mappings" on the generated release repository'
    )
    print('Push for PR with "git push --set-upstream fork HEAD:okd_updates --force"')


@prs.command(
    'set-optional-presubmit',
    short_help='Find OKD configs in MODIFIED openshift/release and set optional & always_run:false.',
)
@click.option('--release-dir', metavar='DIR', required=True, help='Path to the openshift/release directory')
@pass_runtime
def images_okd_prs_optional(runtime, release_dir):
    release_path = Path(release_dir)
    if not release_path.is_dir():
        print(f'Path does not exist: {str(release_path.absolute())}')
        exit(1)

    print('Finding presubmit YAMLs..')
    config_path = release_path.joinpath('ci-operator/config')
    jobs_path = release_path.joinpath('ci-operator/jobs')
    okd_scos_paths = config_path.rglob('**/*__okd-scos.yaml')  # for any config with okd-scos
    # If a config has an okd-scos variant, it is worth scanning its presubmit jobs

    okd_repos = set()
    for okd_scos_path in okd_scos_paths:
        github_repo = okd_scos_path.parent.name
        gitub_org = okd_scos_path.parent.parent.name
        okd_repos.add(f'{gitub_org}/{github_repo}')

    for target_repo in okd_repos:
        target_repo_path = jobs_path.joinpath(target_repo)
        presubmit_paths = target_repo_path.glob('*-presubmits.yaml')
        for target_path in presubmit_paths:
            print(f'Checking: {str(target_path)}')
            file_changed = False
            with open(str(target_path), mode='r') as file:
                data = yaml.safe_load(file)
                presubmits = data['presubmits']
                for repo, entries in presubmits.items():
                    for entry in entries:
                        if entry['context'] == 'ci/prow/okd-scos-images':
                            if not entry.get('optional', False):
                                entry['optional'] = True
                                file_changed = True
                            if entry.get('always_run', True):
                                entry['always_run'] = False
                                file_changed = True

            if file_changed:
                with open(f'{str(target_path)}', mode='w+') as nfile:
                    yaml.dump(data, nfile)
