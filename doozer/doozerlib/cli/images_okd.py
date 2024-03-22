import io
import os
import click
import yaml
from pathlib import Path
import re
from typing import Dict, List, Any, NamedTuple, Optional
from collections import OrderedDict

from github import Github, UnknownObjectException
from dockerfile_parse import DockerfileParser

from artcommonlib import exectools
from artcommonlib.format_util import green_print, yellow_print
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from doozerlib.cli import cli, pass_runtime
from doozerlib import constants, util
from doozerlib.image import ImageMetadata
from doozerlib.util import what_is_in_master, extract_version_fields
from artcommonlib.util import split_git_url, remove_prefix, convert_remote_git_to_ssh


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


def resolve_okd_from(runtime, image_entry, okd_version, is_base_image: bool = True):
    """
    :param runtime: The runtime object
    :param image_entry: A builder or from entry. e.g. { 'member': 'openshift-enterprise-base' }  or { 'stream': 'golang; }
    :return: The upstream CI pullspec to which this entry should resolve.
    """

    if image_entry.member:
        target_meta = runtime.resolve_image(image_entry.member, True)

        if target_meta.config.content.source.okd_alignment.image is not Missing:
            # If the upstream is specified in the metadata, use this information
            # directly instead of a heuristic.
            return target_meta.config.content.source.okd_alignment.image
        else:
            # If payload_name is specified, this is what we need
            # Otherwise, fallback to the legacy "name" field
            image_name = target_meta.config.content.source.okd_alignment.tag_name
            if not image_name:
                name = target_meta.config.payload_name or target_meta.config.name
                image_name = name.split('/')[-1]
                # In release payloads, images are promoted into an imagestream
                # tag name without the ose- prefix.
                image_name = remove_prefix(image_name, 'ose-')
            # e.g. registry.ci.openshift.org/origin/scos-4.16:base
            return f'registry.ci.openshift.org/origin/scos-{okd_version}:{image_name}'

    if image_entry.image:
        # CI is on its own. We can't give them an image that isn't available outside the firewall.
        return None
    elif image_entry.stream:
        resolved = runtime.resolve_stream(image_entry.stream)
        okd_preferred = resolved.okd.image
        if okd_preferred or is_base_image:
            return okd_preferred
        # Fall back to a standard CI image, but only if it not a base image.
        return resolved.upstream_image


def convert_to_imagestream_coordinate(registry_ci_pullspec: str) -> ImageCoordinate:
    if not registry_ci_pullspec.startswith('registry.ci.openshift.org/'):
        raise IOError(f'OKD images must be sourced from registry.ci.openshift.org; cannot use {registry_ci_pullspec}')
    parts = registry_ci_pullspec.split('/')
    return ImageCoordinate(namespace=parts[1], name=parts[2].split(':')[0], tag=parts[2].split(':')[1] )


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
            branch_check, err = exectools.cmd_assert(f'git ls-remote --heads {source_repo_url} {source_repo_branch}', strip=True, retries=3)
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


class CiOperatorImageConfig:

    def __init__(self, image_meta: ImageMetadata, promotion_namespace: str, promotion_imagestream: str):
        self.image_meta = image_meta
        self.promotion_namespace = promotion_namespace
        self.promotion_imagestream = promotion_imagestream

        payload_tag = image_meta.config.content.source.okd_alignment.tag_name
        if not payload_tag:
            payload_tag, _ = image_meta.get_payload_tag_info()
        self.payload_tag = payload_tag

        self.base_obj = {
            'to': payload_tag
        }

        self.base_image: Optional[ImageCoordinate] = None
        self.coordinate = ImageCoordinate(namespace=promotion_namespace, name=promotion_imagestream, tag=payload_tag)
        self.replacements: Dict[ImageCoordinate, List[str]] = dict()

    def add_replacement(self, image_coordinate: ImageCoordinate, pullspecs_or_stages_to_replace: List[str]):
        self.replacements[image_coordinate] = pullspecs_or_stages_to_replace

    def add_from(self, image_coordinate: ImageCoordinate):
        self.base_image = image_coordinate

    def set_dockerfile_path(self, dockerfile_path: str):
        components = dockerfile_path.rsplit('/', maxsplit=1)
        self.base_obj['dockerfile_path'] = components[-1]
        if len(components) > 1:
            self.base_obj['context_dir'] = components[0]

    def get_obj(self, ci_operator_image_names: Dict[ImageCoordinate, str]):
        obj = dict()
        obj.update(self.base_obj)
        if self.base_image:
            obj['from'] = ci_operator_image_names[self.base_image]

        inputs = dict()
        for coordinate, replacements in self.replacements.items():
            inputs[ci_operator_image_names[coordinate]] = {
                'as': replacements
            }
        if inputs:
            obj['inputs'] = inputs
        return obj


class CiOperatorConfig:

    def __init__(self, org: GitHubOrgName, repo: GitHubRepoName, branch_name: str, promotion_namespace: str, promotion_imagestream: str):
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
                }
            ]
        }
        self.build_root = None
        self.releases = dict()

    def get_image_config(self, image_meta: ImageMetadata) -> CiOperatorImageConfig:
        if image_meta.distgit_key not in self.ci_operator_image_configs:
            self.ci_operator_image_configs[image_meta.distgit_key] = CiOperatorImageConfig(image_meta, self.promotion_namespace, self.promotion_imagestream)
        return self.ci_operator_image_configs[image_meta.distgit_key]

    def add_release(self, release_name: str, release_def):
        self.releases[release_name] = release_def

    def set_build_root(self, build_root: ImageCoordinate):
        self.build_root = build_root.as_dict()

    def get_ci_operator_config_path(self, release_clone_dir: Path):
        return release_clone_dir.joinpath('ci-operator', 'config', self.org, self.repo, f'{self.org}-{self.repo}-{self.branch_name}__okd-scos.yaml')

    def write_config(self, release_clone_dir: Path):
        output_path = self.get_ci_operator_config_path(release_clone_dir)

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
        for _, image_config in self.ci_operator_image_configs.items():
            images.append(image_config.get_obj(ci_operator_image_names))

        base_images: Dict[str, Any] = dict()
        for coordinate, _ in base_image_defs.items():
            base_images[ci_operator_image_names[coordinate]] = coordinate.as_dict()

        config = {
            'images': images,
            'promotion': self.promotion,
            'resources': {
                '*': {
                    'requests': {
                        'cpu': '100m',
                        'memory': '200Mi'
                    }
                }
            }
        }
        if base_images:
            config['base_images'] = base_images
        if self.build_root:
            config['build_root'] = {
                'image_stream_tag': self.build_root
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
            self.configs_by_repo[config_key] = CiOperatorConfig(org, repo, branch_name, promotion_namespace=self.promotion_namespace, promotion_imagestream=self.promotion_imagestream)
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


@prs.command('open', short_help='Open PRs against upstream component repos that have a FROM that differs from ART metadata.')
@click.option('--github-access-token', metavar='TOKEN', required=True, help='Github access token for user.')
@click.option('--okd-version', metavar='VERSION', required=True, help='Which OKD stream to promote to.')
@click.option('--ignore-missing-images', default=False, is_flag=True, help='Do not exit if an image is missing upstream.')
@click.option('--draft-prs', default=False, is_flag=True, help='Open PRs as draft PRs')
@click.option('--moist-run', default=False, is_flag=True, help='Do everything except opening the final PRs')
@click.option('--add-auto-labels', default=False, is_flag=True, help='Add auto_labels to PRs; unless running as openshift-bot, you probably lack the privilege to do so')
@click.option('--add-label', default=[], multiple=True, help='Add a label to all open PRs (new and existing) - Requires being openshift-bot')
@pass_runtime
def images_okd_prs(runtime, github_access_token, ignore_missing_images, okd_version,
                   draft_prs, moist_run, add_auto_labels, add_label):
    runtime.initialize(clone_distgits=False, clone_source=True)
    g = Github(login_or_token=github_access_token)
    github_user = g.get_user()

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']

    master_major, master_minor = extract_version_fields(what_is_in_master(), at_least=2)
    if major != master_major or minor != master_minor:
        # These verbs should only be run against the version in master.
        runtime.logger.warning(f'Target {major}.{minor} is not in master (it is tracking {master_major}.{master_minor}); skipping PRs')
        exit(0)

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

    runtime.git_clone(release_repo_url, release_clone_dir)
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
        logger.info('Analyzing image')

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
                upstream_image = resolve_okd_from(runtime, builder, okd_version=okd_version, is_base_image=False)
                if not upstream_image:
                    logger.warning(f'Unable to resolve upstream image for: {builder}')
                    break
                desired_parents.append(upstream_image)

            parent_upstream_image = resolve_okd_from(runtime, from_config, okd_version=okd_version)
            if len(desired_parents) != len(builders) or not parent_upstream_image:
                logger.warning('Unable to find all ART equivalent upstream images for this image')
                continue

            desired_parents.append(parent_upstream_image)

        desired_ci_build_root_coordinate = None
        if okd_alignment_config.ci_build_root is not Missing:
            desired_ci_build_root_image = resolve_okd_from(runtime, okd_alignment_config.ci_build_root, okd_version=okd_version, is_base_image=False)

            # Split the pullspec into an openshift namespace, imagestream, and tag.
            # e.g. registry.openshift.org:999/ocp/release:golang-1.16 => tag=golang-1.16, namespace=ocp, imagestream=release
            # https://docs.ci.openshift.org/docs/architecture/ci-operator/#build-root-image
            desired_ci_build_root_coordinate = convert_to_imagestream_coordinate(desired_ci_build_root_image)
            logger.info(f'Found desired build_root state of: {desired_ci_build_root_coordinate}')
        else:
            # ci-operator configuration, even if it is not building anything, fails if there is no
            # build root. Just get the default golang.
            default_build_root = resolve_okd_from(runtime, Model({
                'stream': 'golang'
            }), okd_version=okd_version, is_base_image=False)
            desired_ci_build_root_coordinate = convert_to_imagestream_coordinate(default_build_root)

        source_repo_url, source_repo_branch = _get_upstream_source(runtime, image_meta)

        if not source_repo_url or 'github.com' not in source_repo_url:
            # No upstream to clone; no PRs to open
            logger.info('Skipping PR check since there is no configured github source URL')
            continue

        if not image_meta.config.for_payload and not okd_alignment_config.tag_name:
            logger.info(f'Skipping non-OKD image: {image_meta.distgit_key}')
            continue

        public_repo_url, public_branch = runtime.get_public_upstream(source_repo_url)
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

            if [bl for bl in public_branches if bl.endswith('/main')] and \
               [bl for bl in priv_branches if bl.endswith('/main')]:
                public_branch = 'main'
            elif [bl for bl in public_branches if bl.endswith('/master')] and \
                 [bl for bl in priv_branches if bl.endswith('/master')]:
                public_branch = 'master'
            else:
                # There are ways of determining default branch without using naming conventions, but as of today, we don't need it.
                raise IOError(f'Did not find master or main branch; unable to detect default branch: {public_branches}')

        _, org, repo_name = split_git_url(public_repo_url)

        # The upstream source for the component.
        ci_operator_config = ci_operator_configs.get_ci_operator_config(org, repo_name, public_branch)

        public_repo_url = convert_remote_git_to_ssh(public_repo_url)
        component_src_clone_dir = os.path.join(runtime.working_dir, 'clones', dgk)
        # Clone the private url to make the best possible use of our doozer_cache
        runtime.git_clone(public_repo_url, component_src_clone_dir)

        with Dir(component_src_clone_dir):
            # Change over to the target branch in the actual public repo
            exectools.cmd_assert(f'git checkout origin/{public_branch}')

            # The path to the Dockerfile in the target branch
            if image_meta.config.content.source.dockerfile is not Missing:
                # Be aware that this attribute sometimes contains path elements too.
                dockerfile_name = image_meta.config.content.source.dockerfile
            else:
                dockerfile_name = "Dockerfile"

            component_src_df_path = Dir.getpath()
            if image_meta.config.content.source.path:
                dockerfile_name = os.path.join(image_meta.config.content.source.path, dockerfile_name)

            component_src_df_path = component_src_df_path.joinpath(dockerfile_name).resolve()
            with component_src_df_path.open(mode='r') as handle:
                dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO())
                dfp.content = handle.read()

            if len(desired_parents) != len(dfp.parent_images):
                # The number of FROM statements in the ART metadata does not match the number
                # of FROM statements in the upstream Dockerfile. ART's normal build process will alert
                # artists on this condition, so assume it will be handled and skip for now.
                yellow_print(f'Parent count mismatch for {image_meta.get_component_name()}; skipping reconciliation.')
                continue

            # reconcile_url = f'{convert_remote_git_to_https(runtime.gitdata.origin_url)}/tree/{runtime.gitdata.commit_hash}/images/{os.path.basename(image_meta.config_filename)}'
            # reconcile_info = f"Reconciling with {reconcile_url}"

            ci_operator_image_config = ci_operator_config.get_image_config(image_meta)
            ci_operator_config.add_release('latest', {
                'integration': {
                    # 'include_built_images': True,
                    'namespace': 'origin',
                    'name': f'scos-{okd_version}'
                }
            })

            stage_names = extract_stage_names(dfp)
            for index, stage_name in enumerate(stage_names[:-1]):  # For all stages except the last (i.e. except for the base image)
                if not stage_name:
                    stage_name = str(index)  # Not certain this will work; we might need to require named stages.
                ci_operator_image_config.add_replacement(convert_to_imagestream_coordinate(desired_parents[index]), [stage_name, dfp.parent_images[index]])

            ci_operator_image_config.add_from(convert_to_imagestream_coordinate(desired_parents[-1]))
            ci_operator_image_config.set_dockerfile_path(dockerfile_name)

            if desired_ci_build_root_coordinate:
                ci_operator_config.set_build_root(desired_ci_build_root_coordinate)

    ci_operator_configs.write_configs(release_clone_dir)
