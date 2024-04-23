import click
import koji
import logging
import re
import os
import yaml
import base64
from typing import List
from datetime import datetime
from ghapi.all import GhApi

from artcommonlib.constants import BREW_HUB
from artcommonlib.format_util import green_print, yellow_print, red_print
from artcommonlib.release_util import split_el_suffix_in_release
from pyartcd import exectools
from pyartcd import jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from doozerlib.rpm_utils import parse_nvr
from doozerlib.brew import BuildStates
from elliottlib.constants import GOLANG_BUILDER_CVE_COMPONENT
from elliottlib import util as elliottutil

_LOGGER = logging.getLogger(__name__)


def is_latest_build(ocp_version: str, el_v: int, nvr: str, koji_session) -> bool:
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    _LOGGER.info(f'Checking build root {build_tag} for latest build')
    parsed_nvr = parse_nvr(nvr)
    latest_build = koji_session.getLatestBuilds(build_tag, package=parsed_nvr['name'])
    if not latest_build:  # if this happens, investigate
        raise ValueError(f'Cannot find latest {parsed_nvr["name"]} build in {build_tag}. Please investigate.')
    if nvr == latest_build[0]['nvr']:
        return True
    return False


async def is_latest_and_available(ocp_version: str, el_v: int, nvr: str, koji_session) -> bool:
    if not is_latest_build(ocp_version, el_v, nvr, koji_session):
        return False

    # If regen repo has been run this would take a few seconds
    # sadly --timeout cannot be less than 1 minute, so we wait for 1 minute
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    _LOGGER.info(f'Checking build root {build_tag} if latest build is available')
    cmd = f'brew wait-repo {build_tag} --build {nvr} --timeout=1'
    rc, _, _ = await exectools.cmd_gather_async(cmd)
    return rc == 0


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


class UpdateGolangPipeline:
    def __init__(self, runtime: Runtime, ocp_version: str, create_ticket: bool, go_nvrs: List[str], art_jira: str,
                 scratch: bool = False):
        self.runtime = runtime
        self.dry_run = runtime.dry_run
        self.scratch = scratch
        self.ocp_version = ocp_version
        self.create_ticket = create_ticket
        self.go_nvrs = go_nvrs
        self.art_jira = art_jira
        self.koji_session = koji.ClientSession(BREW_HUB)

        self._doozer_working_dir = self.runtime.working_dir / "doozer-working"
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._doozer_working_dir)

        self.github_token = os.environ.get('GITHUB_TOKEN')
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to fetch build data repo contents")

    async def run(self):
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(self.ocp_version, self.go_nvrs)
        _LOGGER.info(f'Golang version detected: {go_version}')
        _LOGGER.info(f'NVRs by rhel version: {el_nvr_map}')

        title_update = f" {self.ocp_version} - {go_version} - el{list(el_nvr_map.keys())}"
        if self.create_ticket:
            title_update += ' [create-ticket]'
        if self.dry_run:
            title_update += ' [dry-run]'
        jenkins.init_jenkins()
        jenkins.update_title(title_update)

        # if requested, create ticket for tagging request
        if self.create_ticket:
            _LOGGER.info('Making sure that builds are not already tagged & have necessary qe tags')
            for el_v, nvr in el_nvr_map.items():
                if is_latest_build(self.ocp_version, el_v, nvr, self.koji_session):
                    raise ValueError(f'{nvr} is already the latest build, run '
                                     'only with nvrs that are not latest or run without --create-tagging-ticket')

            _LOGGER.info('Creating Jira ticket to tag golang builds in buildroots')
            self.create_jira_ticket(el_nvr_map, go_version)
            return

        for el_v, nvr in el_nvr_map.items():
            if not await is_latest_and_available(self.ocp_version, el_v, nvr, self.koji_session):
                raise ValueError(f'{nvr} is not the latest build in buildroot, tag yourself or get them tagged via'
                                 '--create-tagging-ticket')
        _LOGGER.info('All builds are tagged and available!')

        # Make a sanity check if a builder nvr exists for this version and if it's already pinned
        component = GOLANG_BUILDER_CVE_COMPONENT
        _LOGGER.info(f"Checking if {component} builds exist for given golang builds")
        package_info = self.koji_session.getPackage(component)
        if not package_info:
            raise IOError(f'No brew package is defined for {component}')
        package_id = package_info['id']
        builder_nvrs = {}
        for el_v, go_nvr in el_nvr_map.items():
            pattern = f"{component}-v{go_version}-*el{el_v}*"
            builds = self.koji_session.listBuilds(packageID=package_id,
                                                  state=BuildStates.COMPLETE.value,  # complete
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

        need_update = False
        if len(builder_nvrs) == len(el_nvr_map):  # existing builders found for all rhel versions
            _LOGGER.info("Checking if existing builder images are being used in streams.yml")

            owner, repo = 'openshift-eng', 'ocp-build-data'
            branch, filename = f'openshift-{self.ocp_version}', 'streams.yml'

            api = GhApi(owner=owner, repo=repo, token=self.github_token)
            blob = api.repos.get_content(filename, ref=branch)
            group_config = yaml.safe_load(base64.b64decode(blob['content']))
            major_go, minor_go, _ = go_version.split('.')
            for stream_name, info in group_config.items():
                if 'golang' not in stream_name:
                    continue
                image_nvr_like = info['image']
                if 'golang-builder' not in image_nvr_like:
                    continue
                name, vr = image_nvr_like.split(':')
                nvr = f"{name.replace('/', '-')}-container-{vr}"
                pattern = f"{component}-v{major_go}.{minor_go}"
                if not nvr.startswith(pattern):
                    continue

                _, el_version = split_el_suffix_in_release(vr)
                el_version = int(el_version[2:])
                if nvr == builder_nvrs[el_version]:
                    _LOGGER.info(f'stream:{stream_name} has the desired builder nvr:{nvr}')
                else:
                    yellow_print(f'update stream:{stream_name}:image to {builder_nvrs[el_version]}')
                    need_update = True

            if not need_update:
                green_print("Builders don't need update since nvr are built and pinned in streams.yml")
            else:
                red_print("Please update streams.yml")
        else:
            missing_in = el_nvr_map.keys() - builder_nvrs.keys()
            # Make sure builder branches are updated for building
            _LOGGER.info(f"Builder images are missing for rhel versions: {missing_in}. "
                         "Verifying builder branches are updated for building")
            for el_v in missing_in:
                self.verify_golang_builder_repo(el_v, go_version)
                await self._rebase(go_version)
                await self._build()

    async def _rebase(self, go_version):
        _LOGGER.info("Rebasing...")
        version = f"v{go_version}"
        release = datetime.now().strftime('%Y%m%d%H%M')
        cmd = [
            "doozer",
            "--group", f"openshift-{self.ocp_version}",
            "images:rebase",
            "--version", version,
            "--release", release,
            "--message", f"bumping to {version}-{release}"
        ]
        if not self.dry_run:
            cmd += " --push"
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    async def _build(self):
        _LOGGER.info("Building...")
        cmd = [
            "doozer",
            "--group", f"openshift-{self.ocp_version}",
            "images:build",
            "--repo-type", "unsigned",
            "--push-to-defaults"
        ]
        if self.dry_run:
            cmd += "--dry-run"
        if self.scratch:
            cmd += "--scratch"
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars)

    def verify_golang_builder_repo(self, el_v, go_version):
        # read group.yml from the branch rhel-{el_v}-golang-{go_v} using ghapi
        owner, repo = 'openshift-eng', 'ocp-build-data'
        major_go, minor_go, patch_go = go_version.split('.')
        go_v = f"{major_go}.{minor_go}"
        branch, filename = f'rhel-{el_v}-golang-{go_v}', 'group.yml'

        api = GhApi(owner=owner, repo=repo, token=self.github_token)
        blob = api.repos.get_content(filename, ref=branch)
        group_config = yaml.safe_load(base64.b64decode(blob['content']))
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

        # # create a new branch rhel-{el_v}-golang-{go_v}-update using ghapi
        # branch = f'rhel-{el_v}-golang-{go_v}-update'
        # title = f'Update golang rpms for {nvr}'
        # body = f'Update golang rpms for {nvr} by updating group.yml'
        # api.repos.create_branch(branch, 'rhel-{el_v}-golang-{go_v}')
        #
        # # create a pull request with the updated group.yml using ghapi
        # branch = f'rhel-{el_v}-golang-{go_v}-update'
        # title = f'Rebuild golang rpms for {nvr}'
        # body = f'Rebuild golang rpms for {nvr} by updating group.yml'
        # api.repos.create_pull(title=title, body=body, head=branch, base=branch)

    def get_content_repo_url(self, el_v):
        url = 'https://download-node-02.eng.bos.redhat.com/brewroot/repos/{repo}/latest'
        return url.format(repo=f'rhaos-{self.ocp_version}-rhel-{el_v}-build')

    def get_module_tag(self, nvr, el_v) -> str:
        tags = [t['name'] for t in self.koji_session.listTags(build=nvr)]
        prefix = f'module-go-toolset-rhel{el_v}-'
        return next((t for t in tags if t.startswith(prefix) and not t.endswith('-build')), None)

    def create_jira_ticket(self, el_nvr_map, go_version):
        # project = 'CWFCONF'
        # labels = ['releng']
        # components = ['BLD', 'cat-brew', 'prod-RHOSE']
        # due_date = (datetime.datetime.today() + datetime.timedelta(days=3)).strftime('%Y/%m/%d')

        nvr_list_string = ''
        el_instructions = ''
        for el_v, nvr in el_nvr_map.items():
            nvr_list_string += f'- RHEL{el_v}: {nvr}\n'
            el_instructions += f'\nFor rhel{el_v}:\n'
            if el_v == 8:
                module_tag = self.get_module_tag(nvr, el_v)
                if not module_tag:
                    raise click.BadParameter(f'Cannot find module tag for {nvr}')
                el_instructions += (f'- Update inheritance for each `rhaos-{self.ocp_version}-rhel-{el_v}-override` '
                                    f'tag to include the module tag `{module_tag}`\n')
            elif el_v in (7, 9):
                el_instructions += f'- `brew tag rhaos-{self.ocp_version}-rhel-{el_v}-override {nvr}`\n'
            el_instructions += f'- Run `brew regen-repo` for `rhaos-{self.ocp_version}-rhel-{el_v}-build`\n'

        template = f'''OpenShift requests that buildroots for version {self.ocp_version} provide a new
golang compiler version {go_version} , reference: {self.art_jira}

The new NVRs are:
{nvr_list_string}
{el_instructions}
'''

        _LOGGER.info(f'Please create https://issues.redhat.com/browse/CWFCONF Jira ticket with the following content:\n{template}')


@cli.command('update-golang')
@click.option('--ocp-version', required=True, help='OCP version to update golang for')
@click.option('--scratch', is_flag=True, default=False, help='Build images in scratch mode')
@click.option('--create-tagging-ticket', 'create_ticket', is_flag=True, default=False,
              help='Create CWFCONF Jira ticket for tagging request')
@click.option('--art-jira', required=True, help='Related ART Jira ticket e.g. ART-1234')
@click.argument('go_nvrs', metavar='GO_NVRS...', nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def update_golang(runtime: Runtime, ocp_version: str, create_ticket: bool, go_nvrs: List[str], art_jira: str,
                        scratch: bool = False):
    await UpdateGolangPipeline(runtime, ocp_version, create_ticket, go_nvrs, art_jira, scratch).run()
