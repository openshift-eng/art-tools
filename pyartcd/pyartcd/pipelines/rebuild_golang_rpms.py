import click
import koji
import datetime
import logging
import os
from typing import List
from ghapi.all import GhApi
from specfile import Specfile

from artcommonlib.constants import BREW_HUB
from artcommonlib.release_util import split_el_suffix_in_release
from pyartcd import exectools
from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from doozerlib.rpm_utils import parse_nvr
from elliottlib import util as elliottutil

_LOGGER = logging.getLogger(__name__)


class RebuildGolangRPMsPipeline:
    def __init__(self, runtime: Runtime, ocp_version: str, go_nvrs: List[str], art_jira: str):
        self.runtime = runtime
        self.ocp_version = ocp_version
        self.go_nvrs = go_nvrs
        self.art_jira = art_jira
        self.koji_session = koji.ClientSession(BREW_HUB)

    async def run(self):
        # validate that the ocp version allows given rhel versions
        # TODO: we can deduce this from streams.yml keys that are defined for the group
        # for now we will hardcode this mapping
        ocp_el_map = {
            '4.10': {7, 8},
            '4.11': {7, 8},
            '4.12': {8, 9},
            '4.13': {8, 9},
            '4.14': {8, 9},
            '4.15': {8, 9},
            '4.16': {8, 9},
        }
        if self.ocp_version not in ocp_el_map:
            raise click.BadParameter(f'OCP version is not supported right now: {self.ocp_version}')

        supported_els = ocp_el_map[self.ocp_version]
        if len(self.go_nvrs) > len(supported_els):
            raise click.BadParameter(f'There should be max 1 nvr for each supported rhel version: {supported_els}')

        # extract & validate rhel versions and golang versions from nvrs
        el_nvr_map = dict()  # {8: 'golang-1.16.7-1.el8', 9: 'golang-1.16.7-1.el9'}
        go_version = None
        for nvr in self.go_nvrs:
            parsed_nvr = parse_nvr(nvr)
            name = parsed_nvr['name']
            if not (name == 'golang' or name.startswith('go-toolset')):
                raise ValueError(f'Only golang/go-toolset nvrs are supported, found: {name}')
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

        _LOGGER.info(f'Golang version detected: {go_version}')
        _LOGGER.info(f'NVRs by rhel version: {el_nvr_map}')

        # For rpms - first make sure builds are tagged and available
        _LOGGER.info('Checking if golang builds are tagged and available in rpm buildroots')
        error_msg = ""
        for el_v, nvr in el_nvr_map.items():
            if await self.is_latest_and_available(el_v, nvr):
                _LOGGER.info(f"{nvr} is tagged and available in buildroot")
            else:
                msg = f"{nvr} is not tagged and available in buildroot. "
                error_msg += msg
                _LOGGER.error(msg)

        if error_msg:
            raise ValueError(error_msg)

        # Check which rpms need to be rebuilt
        _LOGGER.info('Checking if rpms need to be rebuilt')
        _LOGGER.info('Will ignore microshift rpm since it is special and rebuilds for payload image')

        # fetch art_built_rpms
        art_built_rpms = self.get_art_built_rpms()

        non_art_rpms_for_rebuild = dict()
        art_rpms_for_rebuild = set()
        for el_v, nvr in el_nvr_map.items():
            _LOGGER.info(f'Fetching all rhel{el_v} rpms in our candidate tag')
            rpms = [parse_nvr(n) for n in self.get_rpms(el_v) if 'microshift' not in n]
            _LOGGER.info('Determining golang rpms and their build versions')
            go_nvr_map = elliottutil.get_golang_rpm_nvrs(
                [(n['name'], n['version'], n['release']) for n in rpms],
                _LOGGER
            )
            for go_v, nvrs in go_nvr_map.items():
                nvr_s = [f'{n[0]}-{n[1]}-{n[2]}' for n in nvrs]
                if go_v in nvr:
                    _LOGGER.info(f'Builds on latest go version {go_v}: {nvr_s}')
                    continue
                _LOGGER.info(f'Builds on previous go version {go_v}: {nvr_s}')
                art_rpms_for_rebuild.update([n[0] for n in nvrs if n[0] in art_built_rpms])
                if el_v not in non_art_rpms_for_rebuild:
                    non_art_rpms_for_rebuild[el_v] = []
                non_art_rpms_for_rebuild[el_v].extend([n[0] for n in nvrs if n[0] not in art_built_rpms])

            if non_art_rpms_for_rebuild.get(el_v):
                _LOGGER.info(f'These non-ART rhel{el_v} rpms are on previous golang versions, they need to be '
                             f'rebuilt: {sorted(non_art_rpms_for_rebuild[el_v])}')

        if art_rpms_for_rebuild:
            _LOGGER.info(f'These ART rpms are on previous golang versions, they need to be rebuilt: {sorted(art_rpms_for_rebuild)}')
            self.rebuild_art_rpms(art_rpms_for_rebuild)

        if not non_art_rpms_for_rebuild:
            _LOGGER.info('All non-ART rpms are using given golang builds!')
            return

        _LOGGER.info('Building non-ART rpms...')

        _, author, _ = await exectools.cmd_gather_async('git config user.name')
        _, email, _ = await exectools.cmd_gather_async('git config user.email')
        author = author.strip()
        email = email.strip()
        if not email.endswith('@redhat.com'):
            raise ValueError(f'git config user.email {email} does not end with @redhat.com')
        if email == "noreply@redhat.com":
            email = "aos-team-art@redhat.com"
        _LOGGER.info(f"Will use author={author} email={email} for bump commit message")

        for el_v, rpms in non_art_rpms_for_rebuild.items():
            for rpm in rpms:
                try:
                    await self.bump_and_rebuild_rpm(rpm, el_v, author, email)
                except Exception as err:
                    _LOGGER.error(f'Error bumping and rebuilding {rpm}: {err}')
                    continue

    def get_art_built_rpms(self):
        github_token = os.environ.get('GITHUB_TOKEN')
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to fetch build data repo contents")
        api = GhApi(owner='openshift-eng', repo='ocp-build-data', token=github_token)
        branch = f'openshift-{self.ocp_version}'
        content = api.repos.get_content(path='rpms', ref=branch)
        return [c['name'].rstrip('.yml') for c in content if c['name'].endswith('.yml')]

    def rebuild_art_rpms(self, rpms):
        _LOGGER.info(f"Trigger job at {constants.JENKINS_UI_URL}/job/aos-cd-builds/job/build%252Focp4/build "
                     f"with params BUILD_VERSION={self.ocp_version} ASSEMBLY=stream "
                     f"PIN_BUILDS=True BUILD_IMAGES=none BUILD_RPMS=only RPM_LIST={','.join(rpms)}")
        # jenkins.start_ocp4(
        #     build_version=self.ocp_version,
        #     assembly='stream',
        #     rpm_list=rpms,
        #     dry_run=self.runtime.dry_run
        # )

    async def bump_and_rebuild_rpm(self, rpm, el_v, author, email):
        branch = f'rhaos-{self.ocp_version}-rhel-{el_v}'
        # check if dir exists
        if not os.path.isdir(rpm):
            cmd = f'rhpkg clone --branch {branch} rpms/{rpm}'
            await exectools.cmd_assert_async(cmd)
        else:
            await exectools.cmd_assert_async('git reset --hard', cwd=rpm)
            await exectools.cmd_assert_async('git fetch --all', cwd=rpm)
        await exectools.cmd_assert_async(f'git checkout {branch}', cwd=rpm)
        await exectools.cmd_assert_async('git reset --hard @{upstream}', cwd=rpm)

        # get last commit message on this branch
        bump_msg = f'Bump and rebuild with latest golang, resolves {self.art_jira}'
        rc, commit_message, _ = await exectools.cmd_gather_async('git log -1 --format=%s', cwd=rpm)
        if rc != 0:
            raise ValueError(f'Cannot get last commit message for {rpm}')
        if bump_msg in commit_message:
            _LOGGER.info(f'{rpm}/{branch} - Bump commit exists on branch, build in queue? skipping')
            return

        # get all .spec files
        specs = [f for f in os.listdir(rpm) if f.endswith('.spec')]
        if len(specs) != 1:
            raise ValueError(f'Expected to find only 1 .spec file in {rpm}, found: {specs}')

        spec = Specfile(os.path.join(rpm, specs[0]))

        _LOGGER.info(f'{rpm}/{branch} - Bumping release in specfile')

        # Increment second digit of the Release segment e.g.
        # 42.rhaos4_8.el8 -> 42.1.rhaos4_8.el8
        # 6.3.rhaos4_8.el8 -> 6.4.rhaos4_8.el8
        _LOGGER.info(f'{rpm}/{branch} - Current release in specfile: {spec.release}')
        k = spec.release.split('.')
        digits = []
        non_digit_marker = None
        for p in k:
            if p.isdigit():
                digits.append(p)
            else:
                non_digit_marker = spec.release.index(p)
                break
        if len(digits) < 1:
            raise ValueError("unexpected release in specfile")
        elif len(digits) < 2:
            digits.append(0)
        rel = f'{digits[0]}.{int(digits[1])+1}'
        if non_digit_marker:
            rel += f'.{spec.release[non_digit_marker:]}'
        spec.release = rel
        _LOGGER.info(f'{rpm}/{branch} - New release in specfile: {spec.release}')

        _LOGGER.info(f'{rpm}/{branch} - Adding changelog entry in specfile')
        spec.add_changelog_entry(
            bump_msg,
            author=author,
            email=email,
            timestamp=datetime.date.today(),
        )

        if self.runtime.dry_run:
            _LOGGER.info(f"{rpm}/{branch} - Dry run, would've committed changes and triggered build")
            return

        spec.save()
        cmd = f'git commit -am "{bump_msg}"'
        await exectools.cmd_assert_async(cmd, cwd=rpm)
        cmd = 'git push'
        await exectools.cmd_assert_async(cmd, cwd=rpm)
        cmd = 'rhpkg build --nowait'
        await exectools.cmd_assert_async(cmd, cwd=rpm)

    def get_rpms(self, el_v):
        # get all the go rpms from the candidate tag
        tag = f'rhaos-{self.ocp_version}-rhel-{el_v}-candidate'
        latest_builds = [b['nvr'] for b in self.koji_session.listTagged(tag, latest=True)]
        rpms = [b for b in latest_builds if not ('-container' in b or b.startswith('rhcos-'))]
        return rpms

    def is_latest_build(self, el_v: int, nvr: str) -> bool:
        build_tag = f'rhaos-{self.ocp_version}-rhel-{el_v}-build'
        _LOGGER.info(f'Checking build root {build_tag}')
        parsed_nvr = parse_nvr(nvr)
        latest_build = self.koji_session.getLatestBuilds(build_tag, package=parsed_nvr['name'])
        if not latest_build:  # if this happens, investigate
            raise ValueError(f'Cannot find latest {parsed_nvr["name"]} build in {build_tag}. Please investigate.')
        if nvr == latest_build[0]['nvr']:
            return True
        return False

    async def is_latest_and_available(self, el_v: int, nvr: str) -> bool:
        if not self.is_latest_build(el_v, nvr):
            return False

        # If regen repo has been run this would take a few seconds
        # sadly --timeout cannot be less than 1 minute, so we wait for 1 minute
        build_tag = f'rhaos-{self.ocp_version}-rhel-{el_v}-build'
        _LOGGER.info(f'Checking build root {build_tag}')
        cmd = f'brew wait-repo {build_tag} --build {nvr} --timeout=1'
        rc, _, _ = await exectools.cmd_gather_async(cmd)
        return rc == 0


@cli.command('rebuild-golang-rpms')
@click.option('--ocp-version', required=True, help='OCP version to rebuild golang rpms for')
@click.option('--art-jira', required=True, help='Related ART Jira ticket e.g. ART-1234')
@click.argument('go_nvrs', metavar='GO_NVRS...', nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def rebuild_golang_rpms(runtime: Runtime, ocp_version: str, go_nvrs: List[str], art_jira: str):
    await RebuildGolangRPMsPipeline(runtime, ocp_version, go_nvrs, art_jira).run()
