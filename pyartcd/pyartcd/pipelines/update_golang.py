import click
import koji
import logging
import re
import os
import asyncio
import base64
from typing import List
from datetime import datetime, timezone
from ghapi.all import GhApi
from ruamel.yaml import YAML

from artcommonlib.constants import BREW_HUB
from artcommonlib.release_util import split_el_suffix_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib import exectools
from pyartcd import jenkins
from pyartcd.constants import GITHUB_OWNER
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.git import GitRepository
from doozerlib.brew import BuildStates
from elliottlib.constants import GOLANG_BUILDER_CVE_COMPONENT
from elliottlib import util as elliottutil

_LOGGER = logging.getLogger(__name__)

yaml = YAML(typ="rt")
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.width = 4096


def is_latest_build(ocp_version: str, el_v: int, nvr: str, koji_session) -> bool:
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    parsed_nvr = parse_nvr(nvr)
    latest_build = koji_session.getLatestBuilds(build_tag, package=parsed_nvr['name'])
    if not latest_build:  # if this happens, investigate
        raise ValueError(f'Cannot find latest {parsed_nvr["name"]} build in {build_tag}. Please investigate.')
    if nvr == latest_build[0]['nvr']:
        _LOGGER.info(f'{nvr} is the latest build in {build_tag}')
        return True

    if el_v == 8 and 'module' in nvr:
        _LOGGER.info(f'{nvr} is not the latest build in build tag {build_tag}. Since it is a module build, '
                     f'we need to update tag inheritance. Use the flag --create-tagging-ticket to create a jira '
                     'ticket to do that.')
    else:
        override_tag = f'rhaos-{ocp_version}-rhel-{el_v}-override'
        _LOGGER.info(f'{nvr} is not the latest build in {build_tag}. Run `brew tag {override_tag} {nvr}` to tag the '
                     f'build and then run `brew regen-repo {build_tag}` to make it available.')
    return False


def get_latest_nvr_in_tag(tag: str, package: str, koji_session) -> str:
    latest_build = koji_session.listTagged(tag, latest=True, package=package, inherit=False)
    if not latest_build:
        return None
    return latest_build[0]['nvr']


async def is_latest_and_available(ocp_version: str, el_v: int, nvr: str, koji_session) -> bool:
    if not is_latest_build(ocp_version, el_v, nvr, koji_session):
        return False

    # If regen repo has been run this would take a few seconds
    # sadly --timeout cannot be less than 1 minute, so we wait for 1 minute
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    cmd = f'brew wait-repo {build_tag} --build {nvr} --timeout=1'
    rc, _, _ = await exectools.cmd_gather_async(cmd, check=False)
    if rc != 0:
        _LOGGER.info(f'Build {nvr} is tagged but not available in {build_tag}. Run `brew regen-repo {build_tag} to '
                     'make the build available.')
        return False
    _LOGGER.info(f'{nvr} is available in {build_tag}')
    return True


def extract_and_validate_golang_nvrs(ocp_version: str, go_nvrs: List[str]):
    match = re.fullmatch(r"(\d).(\d+)", ocp_version)
    if not match:
        raise ValueError(f'Invalid OCP version: {ocp_version}')
    major, minor = int(match[1]), int(match[2])
    if major != 4:
        raise ValueError(f'Only OCP major version 4 is supported, found: {major}')
    if minor < 12:
        raise ValueError(f'Only OCP 4.12+ is supported, found: {ocp_version}')

    # only rhel 8 and 9 are supported 4.12 onwards
    supported_els = {8, 9}

    if len(go_nvrs) > len(supported_els):
        raise click.BadParameter(f'There should be max 1 nvr for each supported rhel version: {supported_els}')

    el_nvr_map = dict()  # {8: 'golang-1.16.7-1.el8', 9: 'golang-1.16.7-1.el9'}
    go_version = None
    for nvr in go_nvrs:
        parsed_nvr = parse_nvr(nvr)
        name = parsed_nvr['name']
        if name != 'golang':
            raise ValueError(f'Only `golang` nvrs are supported, found package name: {name}')
        if go_version and go_version != parsed_nvr['version']:
            raise ValueError(f'All nvrs should have the same golang version, found: {go_version} and'
                             f' {parsed_nvr["version"]}')
        go_version = parsed_nvr['version']

        _, el_version = split_el_suffix_in_release(parsed_nvr['release'])
        # el_version is either None or something like "el8"
        if not el_version:
            raise ValueError(f'Cannot detect an el version in NVR {nvr}')
        el_version = int(el_version[2:])
        if el_version not in supported_els:
            raise ValueError(f'Unsupported RHEL version detected for nvr {nvr}, supported versions are:'
                             f' {supported_els}')
        if el_version in el_nvr_map:
            raise ValueError(f'Cannot have two nvrs for the same rhel version: {nvr},'
                             f' {el_nvr_map[el_version]}')
        el_nvr_map[el_version] = nvr
    return go_version, el_nvr_map


async def move_golang_bugs(ocp_version: str,
                           cves: List[str] = None,
                           nvrs: List[str] = None,
                           components: List[str] = None,
                           force_update_tracker: bool = False,
                           dry_run: bool = False,
                           ):
    cmd = [
        'elliott',
        '--group', f'openshift-{ocp_version}',
        '--assembly', 'stream',
        'find-bugs:golang',
        '--analyze',
        '--update-tracker',
    ]
    if cves:
        for cve in cves:
            cmd.extend(['--cve-id', cve])
    if nvrs:
        for nvr in nvrs:
            cmd.extend(['--fixed-in-nvr', nvr])
    if components:
        for component in components:
            cmd.extend(['--component', component])
    if force_update_tracker:
        cmd.append('--force-update-tracker')
    if dry_run:
        cmd.append('--dry-run')
    await exectools.cmd_assert_async(cmd)


class UpdateGolangPipeline:
    def __init__(self, runtime: Runtime, ocp_version: str, create_ticket: bool, cves: List[str],
                 force_update_tracker: bool, go_nvrs: List[str], art_jira: str, scratch: bool = False):
        self.runtime = runtime
        self.dry_run = runtime.dry_run
        self.scratch = scratch
        self.ocp_version = ocp_version
        self.create_ticket = create_ticket
        self.cves = cves
        self.force_update_tracker = force_update_tracker
        self.go_nvrs = go_nvrs
        self.art_jira = art_jira
        self.koji_session = koji.ClientSession(BREW_HUB)

        self._doozer_working_dir = self.runtime.working_dir / "doozer-working"
        self._doozer_env_vars = os.environ.copy()

        self.github_token = os.environ.get('GITHUB_TOKEN')
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to fetch build data repo contents")

    async def run(self):
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(self.ocp_version, self.go_nvrs)
        _LOGGER.info(f'Golang version detected: {go_version}')
        _LOGGER.info(f'NVRs by rhel version: {el_nvr_map}')

        running_in_jenkins = os.environ.get('BUILD_ID', False)
        if running_in_jenkins:
            title_update = f" {self.ocp_version} - {go_version} - el{list(el_nvr_map.keys())}"
            if self.create_ticket:
                title_update += ' [create-ticket]'
            if self.dry_run:
                title_update += ' [dry-run]'
            jenkins.init_jenkins()
            jenkins.update_title(title_update)

        # Do a sanity check for rhel8 module build
        # a module build should never be tagged directly in override tag
        # if it is, we should make sure it is available via module inheritance
        # and then untag it from override tag
        if 8 in el_nvr_map and 'module' in el_nvr_map[8]:
            tag = f'rhaos-{self.ocp_version}-rhel-8-override'
            package = parse_nvr(el_nvr_map[8])['name']
            nvr = get_latest_nvr_in_tag(tag, package, self.koji_session)
            if nvr:
                raise ValueError(f'{nvr} is tagged in {tag}, please make sure it is available via module inheritance '
                                 f'(brew list-tagged {tag} {package} --inherit). If not, use --create-tagging-ticket. '
                                 f'Once it is available, untag it from the override tag (brew untag-build {tag} {nvr})')

        # if requested, create ticket for tagging request
        if self.create_ticket:
            _LOGGER.info('Making sure that builds are not already tagged')
            for el_v, nvr in el_nvr_map.items():
                if is_latest_build(self.ocp_version, el_v, nvr, self.koji_session):
                    raise ValueError(f'{nvr} is already the latest build, run '
                                     'only with nvrs that are not latest or run without --create-tagging-ticket')

            _LOGGER.info('Creating Jira ticket to tag golang builds in buildroots')
            self.create_jira_ticket_for_el8(el_nvr_map, go_version)
            return

        cannot_proceed = False
        for el_v, nvr in el_nvr_map.items():
            if not await is_latest_and_available(self.ocp_version, el_v, nvr, self.koji_session):
                cannot_proceed = True
        if cannot_proceed:
            raise ValueError('Cannot proceed until all builds are tagged and available')

        _LOGGER.info('All builds are tagged and available!')

        # Check if openshift-golang-builder builds exist for the provided compiler builds in brew
        builder_nvrs = self.get_existing_builders(el_nvr_map, go_version)
        if len(builder_nvrs) != len(el_nvr_map):  # builders not found for all rhel versions
            missing_in = el_nvr_map.keys() - builder_nvrs.keys()
            _LOGGER.info(f"Builder images are missing for rhel versions: {missing_in}. "
                         "Verifying builder branches are updated for building")
            for el_v in missing_in:
                self.verify_golang_builder_repo(el_v, go_version)

            # rebase sequentially, since distgit operations are not thread safe
            # fire builds in parallel
            for el_v in missing_in:
                await self._rebase(el_v, go_version)
            await asyncio.gather(*[
                await self._build(el_v, go_version) for el_v in missing_in
            ])

            # Now all builders should be available in brew, try to fetch again
            builder_nvrs = self.get_existing_builders(el_nvr_map, go_version)
            if len(builder_nvrs) != len(el_nvr_map):
                missing_in = el_nvr_map.keys() - builder_nvrs.keys()
                raise ValueError(f'Failed to find existing builder(s) for rhel version(s): {missing_in}')

        _LOGGER.info("Updating streams.yml with found builder images")
        await self.update_golang_streams(go_version, builder_nvrs)

        await move_golang_bugs(
            ocp_version=self.ocp_version,
            cves=self.cves,
            nvrs=self.go_nvrs if self.cves else None,
            components=[GOLANG_BUILDER_CVE_COMPONENT],
            force_update_tracker=self.force_update_tracker,
            dry_run=self.dry_run,
        )

    def get_existing_builders(self, el_nvr_map, go_version):
        component = GOLANG_BUILDER_CVE_COMPONENT
        _LOGGER.info(f"Checking if {component} builds exist for given golang builds")
        package_info = self.koji_session.getPackage(component)
        if not package_info:
            raise IOError(f'Cannot find brew package info for {component}')
        package_id = package_info['id']
        builder_nvrs = {}
        for el_v, go_nvr in el_nvr_map.items():
            pattern = f"{component}-v{go_version}-*el{el_v}*"
            builds = self.koji_session.listBuilds(packageID=package_id,
                                                  state=BuildStates.COMPLETE.value,
                                                  pattern=pattern,
                                                  queryOpts={'limit': 1, 'order': '-creation_event_id'})
            if builds:
                build = builds[0]
                go_nvr_map = elliottutil.get_golang_container_nvrs(
                    [(build['name'], build['version'], build['release'])],
                    _LOGGER
                )  # {'1.20.12-2.el9_3': {('openshift-golang-builder-container', 'v1.20.12',
                # '202403212137.el9.g144a3f8.el9')}}
                builder_go_vr = list(go_nvr_map.keys())[0]
                if builder_go_vr in go_nvr:
                    _LOGGER.info(f"Found existing builder image: {build['nvr']} built with {go_nvr}")
                    builder_nvrs[el_v] = build['nvr']
        return builder_nvrs

    async def update_golang_streams(self, go_version, builder_nvrs):
        build_data_path = self.runtime.working_dir / "ocp-build-data-push"
        build_data = GitRepository(build_data_path, dry_run=self.dry_run)
        ocp_build_data_repo_push_url = self.runtime.config["build_config"]["ocp_build_data_repo_push_url"]
        await build_data.setup(ocp_build_data_repo_push_url)

        # Here we also check the previous and next minor version of given OCP version
        # and try to update the golang builders for them as well
        # This is because golang builders are used across minor versions
        # and if we are not careful, we can miss updating them
        major, minor = self.ocp_version.split('.')
        next_minor = f"{major}.{int(minor)+1}"
        previous_minor = f"{major}.{int(minor)-1}"
        for ocp_version in [previous_minor, self.ocp_version, next_minor]:
            group_name = f"openshift-{ocp_version}"
            branch = f"update-golang-{ocp_version}-{go_version}"
            _LOGGER.info(f"Checking if {group_name}:streams.yml needs update...")
            try:
                await build_data.fetch_switch_branch(branch, group_name)
            except Exception as e:
                message = f"Failed to fetch {group_name} branch: {e}"
                if ocp_version == next_minor:
                    _LOGGER.warning(message)
                    continue
                else:
                    raise

            streams_yaml_path = build_data_path / "streams.yml"
            streams_yaml = yaml.load(streams_yaml_path)

            # update image nvrs in streams.yml
            need_update = False
            major_go, minor_go, _ = go_version.split('.')
            for stream_name, info in streams_yaml.items():
                if 'golang' not in stream_name:
                    continue
                image_nvr_like = info['image']
                if 'golang-builder' not in image_nvr_like:
                    continue
                name, vr = image_nvr_like.split(':')
                if not vr.startswith(f"v{major_go}.{minor_go}"):
                    continue

                _, el_version = split_el_suffix_in_release(vr)  # 'el8'
                el_version = int(el_version[2:])  # 8
                if el_version not in builder_nvrs:
                    continue

                parsed_nvr = parse_nvr(builder_nvrs[el_version])
                expected_vr = f'{parsed_nvr["version"]}-{parsed_nvr["release"]}'
                expected_nvr_like = f'{name}:{expected_vr}'
                if image_nvr_like == expected_nvr_like:
                    _LOGGER.info(f'stream:{stream_name} has the desired builder nvr:{expected_nvr_like}')
                else:
                    _LOGGER.info(f"Updating stream:{stream_name} image from {image_nvr_like} to {expected_nvr_like}")
                    streams_yaml[stream_name]['image'] = expected_nvr_like
                    need_update = True

            if not need_update:
                _LOGGER.info(f"No update needed in {group_name}:streams.yml")
                continue

            yaml.dump(streams_yaml, streams_yaml_path)
            title = f"{self.art_jira} - Bump {ocp_version} golang builders to {go_version}"

            # Create a PR
            build_url = jenkins.get_build_url()
            body = ""
            if build_url:
                body = f"Created by job run {build_url}"
            match = re.search(r"github\.com[:/](.+)/(.+)(?:.git)?", ocp_build_data_repo_push_url)
            if not match:
                raise ValueError(
                    f"Cannot push: {ocp_build_data_repo_push_url} is not a valid github repo url")
            head = f"{match[1]}:{branch}"
            base = group_name
            if self.dry_run:
                _LOGGER.info(f"[DRY RUN] Would have created commit on {head=} with {title=} and {body=}")
            else:
                pushed = await build_data.commit_push(f"{title}\n{body}")
                if not pushed:
                    raise RuntimeError("Commit not pushed: Please investigate")

            repo = "ocp-build-data"
            api = GhApi(owner=GITHUB_OWNER, repo=repo, token=self.github_token)
            existing_prs = api.pulls.list(state="open", base=base, head=head)
            if not existing_prs.items:
                try:
                    if self.dry_run:
                        _LOGGER.info(f"[DRY RUN] Would have created PR from {head=} to {base=}")
                    else:
                        result = api.pulls.create(head=head, base=base, title=title, body=body, maintainer_can_modify=True)
                        _LOGGER.info(f"PR created: {result['html_url']}")
                except Exception as e:
                    _LOGGER.warning(f"Failed to create PR: {e}")
                    manual_pr_url = f"https://github.com/{GITHUB_OWNER}/{repo}/compare/{base}...{head}?expand=1"
                    _LOGGER.info(f"Create a PR manually {manual_pr_url}")
            else:
                _LOGGER.info(f"Existing PR updated: {existing_prs.items[0]['html_url']}")

    async def _rebase(self, el_v, go_version):
        _LOGGER.info("Rebasing...")
        branch = self.get_golang_branch(el_v, go_version)
        version = f"v{go_version}"
        release = datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M')
        cmd = [
            "doozer",
            f"--working-dir={self._doozer_working_dir}-{el_v}",
            "--group", branch,
            "images:rebase",
            "--version", version,
            "--release", release,
            "--message", f"bumping to {version}-{release}"
        ]
        if not self.dry_run:
            cmd.append("--push")
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _build(self, el_v, go_version):
        _LOGGER.info("Building...")
        branch = self.get_golang_branch(el_v, go_version)
        cmd = [
            "doozer",
            f"--working-dir={self._doozer_working_dir}-{el_v}",
            "--group", branch,
            "images:build",
            "--repo-type", "unsigned",
            "--push-to-defaults"
        ]
        if self.dry_run:
            cmd.append("--dry-run")
        if self.scratch:
            cmd.append("--scratch")
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _rebase_and_build(self, el_v, go_version):
        await self._rebase(el_v, go_version)
        await self._build(el_v, go_version)

    @staticmethod
    def get_golang_branch(el_v, go_version):
        major_go, minor_go, _ = go_version.split('.')
        go_v = f"{major_go}.{minor_go}"
        return f'rhel-{el_v}-golang-{go_v}'

    def verify_golang_builder_repo(self, el_v, go_version):
        # read group.yml from the golang branch using ghapi
        owner, repo = 'openshift-eng', 'ocp-build-data'
        branch = self.get_golang_branch(el_v, go_version)
        filename = 'group.yml'

        api = GhApi(owner=owner, repo=repo, token=self.github_token)
        blob = api.repos.get_content(filename, ref=branch)
        group_config = yaml.load(base64.b64decode(blob['content']))
        content_repo_url = self.get_content_repo_url(el_v)

        golang_repo = f'rhel-{el_v}-golang-rpms'
        if golang_repo not in group_config['repos']:
            raise ValueError(f"Did not find {golang_repo} defined at "
                             f"https://github.com/{owner}/{repo}/blob/{branch}/{filename}. If it's with a different "
                             "name please correct it.")

        expected = {arch: f'{content_repo_url}/{arch}/'
                    for arch in group_config['repos'][golang_repo]['conf']['baseurl'].keys()}

        major, minor = group_config['vars']['MAJOR'], group_config['vars']['MINOR']
        actual = {arch: val.format(MAJOR=major, MINOR=minor)
                  for arch, val in group_config['repos'][golang_repo]['conf']['baseurl'].items()}

        if expected != actual:
            raise ValueError(f"Did not find repo {golang_repo} to have the expected urls. \nexpected="
                             f"{expected}\nactual={actual}")

        _LOGGER.info(f"Builder branch {branch} has the expected content set urls")

    def get_content_repo_url(self, el_v):
        url = 'https://download-node-02.eng.bos.redhat.com/brewroot/repos/{repo}/latest'
        return url.format(repo=f'rhaos-{self.ocp_version}-rhel-{el_v}-build')

    def get_module_tag(self, nvr, el_v) -> str:
        tags = [t['name'] for t in self.koji_session.listTags(build=nvr)]
        prefix = f'module-go-toolset-rhel{el_v}-'
        return next((t for t in tags if t.startswith(prefix) and not t.endswith('-build')), None)

    def create_jira_ticket_for_el8(self, el_nvr_map, go_version):
        # project = 'CWFCONF'
        # labels = ['releng']
        # components = ['BLD', 'cat-brew', 'prod-RHOSE']
        # due_date = (datetime.datetime.today() + datetime.timedelta(days=3)).strftime('%Y/%m/%d')

        nvr_list_string = ''
        el_instructions = ''
        for el_v, nvr in el_nvr_map.items():
            nvr_list_string += f'- RHEL{el_v}: {nvr}\n'
            el_instructions += f'For rhel{el_v}:\n'
            if el_v == 8:
                module_tag = self.get_module_tag(nvr, el_v)
                if not module_tag:
                    raise click.BadParameter(f'Cannot find module tag for {nvr}')
                commit_link = "https://gitlab.cee.redhat.com/rcm/rcm-ansible/-/commit/e838f59751cebe86347e6a82252dec0a1593c735"
                el_instructions += (f'- Update inheritance for each `rhaos-{self.ocp_version}-rhel-{el_v}-override` '
                                    f'tag to include the module tag `{module_tag}`. This is usually done via a '
                                    f'commit in rcm-ansible repo ({commit_link}). Please do not '
                                    'directly tag the module build in the override tag.\n')
            elif el_v in (7, 9):
                el_instructions += f'- `brew tag rhaos-{self.ocp_version}-rhel-{el_v}-override {nvr}`\n'
            el_instructions += f'- Run `brew regen-repo` for `rhaos-{self.ocp_version}-rhel-{el_v}-build`\n'

        template = f'''OpenShift requests that buildroots for version {self.ocp_version} provide a new \
golang compiler version {go_version} , reference: {self.art_jira}

The new NVRs are:
{nvr_list_string}
{el_instructions}
'''
        title = f'Request Golang {go_version} for OCP {self.ocp_version}'
        _LOGGER.info('Please create https://issues.redhat.com/browse/CWFCONF Jira ticket with \n'
                     f'title: {title}\ncontent:\n{template}')


@cli.command('update-golang')
@click.option('--ocp-version', required=True, help='OCP version to update golang for, e.g. 4.16')
@click.option('--scratch', is_flag=True, default=False, help='Build images in scratch mode')
@click.option('--create-tagging-ticket', 'create_ticket', is_flag=True, default=False,
              help='Create CWFCONF Jira ticket for tagging request')
@click.option('--art-jira', required=True, help='Related ART Jira ticket e.g. ART-1234')
@click.option('--cves', help='CVEs that are confirmed to be fixed in all given golang nvrs (comma separated). '
                             'This will be used to fetch relevant Tracker bugs and move them to ON_QA state if '
                             'determined to be fixed (nightly is found containing fixed builds). e.g. CVE-2024-1234')
@click.option('--force-update-tracker', is_flag=True, default=False,
              help='Force update found tracker bugs for the given CVEs, even if the latest nightly is not found containing fixed builds')
@click.option('--confirm', is_flag=True, default=False, help='Confirm to proceed with rebase and build')
@click.argument('go_nvrs', metavar='GO_NVRS...', nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def update_golang(runtime: Runtime, ocp_version: str, scratch: bool, create_ticket: bool, art_jira: str,
                        cves: str, force_update_tracker: bool, confirm: bool, go_nvrs: List[str]):
    if not runtime.dry_run and not confirm:
        _LOGGER.info('--confirm is not set, running in dry-run mode')
        runtime.dry_run = True
    if cves:
        cves = cves.split(',')
    if force_update_tracker and not cves:
        raise ValueError('CVEs must be provided with --force-update-tracker')

    await UpdateGolangPipeline(runtime, ocp_version, create_ticket, cves, force_update_tracker,
                               go_nvrs, art_jira, scratch).run()
