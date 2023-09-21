import click
import koji
import datetime
import logging
import re
import asyncio
import os
from typing import List
from ghapi.all import GhApi
from specfile import Specfile
from tenacity import (RetryCallState, RetryError, retry,
                      retry_if_exception_type, retry_if_result,
                      stop_after_attempt, wait_fixed)

from pyartcd import jenkins
from pyartcd import constants
from pyartcd import exectools
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from doozerlib.util import split_el_suffix_in_release
from doozerlib.rpm_utils import parse_nvr
from elliottlib import util as elliottutil

_LOGGER = logging.getLogger(__name__)


class UpdateGolangPipeline:
    def __init__(self, runtime: Runtime, ocp_version: str, create_ticket: bool, go_nvrs: List[str],
                 permit_missing_qe_tag: bool, art_jira: str):
        self.runtime = runtime
        self.ocp_version = ocp_version
        self.create_ticket = create_ticket
        self.go_nvrs = go_nvrs
        self.art_jira = art_jira
        self.permit_missing_qe_tag = permit_missing_qe_tag
        self.koji_session = koji.ClientSession(constants.BREW_SERVER)

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

        # if requested, create ticket for tagging request
        if self.create_ticket:
            _LOGGER.info('Making sure that builds are not already tagged & have necessary qe tags')
            for el_v, nvr in el_nvr_map.items():
                if self.is_latest_build(el_v, nvr):
                    raise ValueError(f'{nvr} is already the latest build, run '
                                     'only with nvrs that are not latest or run without --create-ticket')
                # check if all the necessary tags exist
                if not self.has_necessary_tags(nvr):
                    if self.permit_missing_qe_tag:
                        _LOGGER.warning(f'{nvr} does not have the necessary qe tags, but --permit-missing-qe-tag was used')
                        continue
                    raise ValueError(f'{nvr} does not have the necessary qe tags. Use --permit-missing-qe-tag to ignore this')

            _LOGGER.info('Creating Jira ticket to tag golang builds in buildroots')
            self.create_jira_ticket(el_nvr_map, go_version)
        else:
            _LOGGER.info('Not creating Jira ticket, run with --create-ticket to create one if one does not exist')

        _LOGGER.info('Checking if builds are tagged and available in buildroots')
        tasks = []
        for el_v, nvr in el_nvr_map.items():
            tasks.append(self.wait_for_nvr_to_be_latest(el_v, nvr))
        try:
            await asyncio.gather(*tasks)
        except RetryError as err:
            message = f"Timeout waiting for builds to show up in buildroots: {err}"
            _LOGGER.error(message)
            _LOGGER.exception(err)
            raise TimeoutError(message)
        finally:
            _LOGGER.info("Golang builds are tagged and are available")

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
            non_art_rpms_for_rebuild[el_v] = []
            for go_v, nvrs in go_nvr_map.items():
                nvr_s = [f'{n[0]}-{n[1]}-{n[2]}' for n in nvrs]
                if go_v in nvr:
                    _LOGGER.info(f'Builds on latest go version {go_v}: {nvr_s}')
                    continue
                _LOGGER.info(f'Builds on older go version {go_v}: {nvr_s}')
                art_rpms_for_rebuild.update([n[0] for n in nvrs if n[0] in art_built_rpms])
                non_art_rpms_for_rebuild[el_v].extend([n[0] for n in nvrs if n[0] not in art_built_rpms])

            if non_art_rpms_for_rebuild[el_v]:
                _LOGGER.info(f'These non-ART rhel{el_v} rpms are on older golang versions, they need to be '
                             f'rebuilt: {sorted(non_art_rpms_for_rebuild[el_v])}')

        if art_rpms_for_rebuild:
            _LOGGER.info(f'These ART rpms are on older golang versions, they need to be rebuilt: {sorted(art_rpms_for_rebuild)}')

        if non_art_rpms_for_rebuild and click.confirm("Rebuild non-ART rpms?"):
            _LOGGER.info('Rebuilding non-ART rpms')
            _, author, _ = await exectools.cmd_gather_async('git config user.name')
            _, email, _ = await exectools.cmd_gather_async('git config user.email')
            author = author.strip()
            email = email.strip()
            for el_v, rpms in non_art_rpms_for_rebuild.items():
                for rpm in rpms:
                    try:
                        await self.bump_and_rebuild_rpm(rpm, el_v, author, email)
                    except Exception as err:
                        _LOGGER.error(f'Error bumping and rebuilding {rpm}: {err}')
                        continue

        # _LOGGER.info('Rebuilding ART rpms')
        # self.rebuild_art_rpms(art_rpms_for_rebuild)

    def get_art_built_rpms(self):
        github_token = os.environ.get('GITHUB_TOKEN')
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to fetch build data repo contents")
        api = GhApi(owner='openshift-eng', repo='ocp-build-data', token=github_token)
        branch = f'openshift-{self.ocp_version}'
        content = api.repos.get_content(path='rpms', ref=branch)
        return [c['name'].rstrip('.yml') for c in content if c['name'].endswith('.yml')]

    def rebuild_art_rpms(self, rpms):
        jenkins.start_ocp4(
            build_version=self.ocp_version,
            assembly='stream',
            rpm_list=rpms,
            dry_run=self.runtime.dry_run
        )

    async def bump_and_rebuild_rpm(self, rpm, el_v, author, email):
        branch = f'rhaos-{self.ocp_version}-rhel-{el_v}'
        # check if dir exists
        if not os.path.isdir(rpm):
            cmd = f'rhpkg clone --branch {branch} rpms/{rpm}'
            await exectools.cmd_assert_async(cmd)
        else:
            await exectools.cmd_assert_async(f'git reset --hard', cwd=rpm)
            await exectools.cmd_assert_async(f'git fetch --all', cwd=rpm)
        await exectools.cmd_assert_async(f'git checkout {branch}', cwd=rpm)
        await exectools.cmd_assert_async('git reset --hard @{upstream}', cwd=rpm)

        # get last commit message on this branch
        bump_msg = 'Bump and rebuild with latest golang'
        rc, commit_message, _ = await exectools.cmd_gather_async('git log -1 --format=%s', cwd=rpm)
        if rc != 0:
            raise ValueError(f'Cannot get last commit message for {rpm}')
        if commit_message.startswith(bump_msg):
            _LOGGER.info(f'{rpm}/{branch} - Bump commit exists on branch, build in queue? skipping')
            return

        # get all .spec files
        specs = [f for f in os.listdir(rpm) if f.endswith('.spec')]
        if len(specs) != 1:
            raise ValueError(f'Expected to find only 1 .spec file in {rpm}, found: {specs}')

        spec = Specfile(os.path.join(rpm, specs[0]))

        _LOGGER.info(f'{rpm}/{branch} - Bumping release in specfile')
        # Add a .1 after the first Release segment e.g. 42.rhaos4_8.el8 becomes 42.1.rhaos4_8.el8
        k = spec.release.split('.')
        rel = f'{k[0]}.1'
        if len(k) > 1:
            rel += f'.{".".join(k[1:])}'
        spec.release = rel
        _LOGGER.info(f'{rpm}/{branch} - Adding changelog entry in specfile')
        msg = f'{bump_msg}, resolves {self.art_jira}'
        if not email.endswith('@redhat.com'):
            raise ValueError(f'git config user.email {email} does not end with @redhat.com')
        spec.add_changelog_entry(
            msg,
            author=author,
            email=email,
            timestamp=datetime.date.today(),
        )

        if self.runtime.dry_run:
            _LOGGER.info(f"{rpm}/{branch} - Dry run, would've committed changes and triggered build")
            return

        spec.save()
        cmd = f'git commit -am "{msg}"'
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

    async def wait_for_nvr_to_be_latest(self, el_v: int, nvr: str):
        def _my_before_sleep(retry_state: RetryCallState):
            if retry_state.outcome.failed:
                err = retry_state.outcome.exception()
                _LOGGER.warning(
                    'Error fetching latest build. Will check again in %s seconds. %s: %s',
                    retry_state.next_action.sleep, type(err).__name__, err,
                )
            else:
                _LOGGER.log(
                    logging.INFO if retry_state.attempt_number < 1 else logging.WARNING,
                    '%s is still not the latest build in buildroot. Will check again in %s seconds.',
                    nvr, retry_state.next_action.sleep
                )
        return await retry(
            stop=(stop_after_attempt(144)),  # wait for 20m * 144 = 2880m = 48 hours
            wait=wait_fixed(60),  # TODO: make it 1200, wait for 20 minutes between retries
            retry=(retry_if_result(lambda is_latest: not is_latest) | retry_if_exception_type()),
            before_sleep=_my_before_sleep,
        )(self.is_latest_and_available)(el_v, nvr)

    def has_necessary_tags(self, nvr: str) -> bool:
        tag_regex = r'^RH[EBS]A-.*(pending|released)$'
        tags = [t['name'] for t in self.koji_session.listTags(build=nvr)]
        if any(re.match(tag_regex, t) for t in tags):
            return True
        _LOGGER.info(f'NVR {nvr} does not have any tags matching {tag_regex}')
        return False

    def is_latest_build(self, el_v: int, nvr: str) -> bool:
        build_tag = f'rhaos-{self.ocp_version}-rhel-{el_v}-build'
        parsed_nvr = parse_nvr(nvr)
        latest_build = self.koji_session.getLatestBuilds(build_tag, package=parsed_nvr['name'])
        if not latest_build:  # if this happens, investigate
            raise ValueError(f'Cannot find latest {name} build in {build_tag}. Please investigate.')
        if nvr == latest_build[0]['nvr']:
            return True
        return False

    async def is_latest_and_available(self, el_v: int, nvr: str) -> bool:
        if not self.is_latest_build(el_v, nvr):
            return False

        # If regen repo has been run this would take a few seconds
        # sadly --timeout cannot be less than 1 minute, so we wait for 1 minute
        cmd = f'brew wait-repo rhaos-{self.ocp_version}-rhel-{el_v}-build --build {nvr} --timeout=1'
        rc, _, _ = await exectools.cmd_gather_async(cmd)
        return rc == 0

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

        _LOGGER.info(f'Creating Jira ticket with the following content:\n{template}')


@cli.command('update-golang')
@click.option('--ocp-version', required=True, help='OCP version to update golang for')
@click.option('--create-ticket', is_flag=True, default=False,
              help='Create SP Jira ticket for tagging request')
@click.option('--permit-missing-qe-tag', is_flag=True, default=False,
              help='Permit missing qe tags on builds')
@click.option('--art-jira', required=True, help='Related ART Jira ticket e.g. ART-1234')
@click.argument('go_nvrs', metavar='GO_NVRS...', nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def update_golang(runtime: Runtime, ocp_version: str, create_ticket: bool, go_nvrs: List[str],
                        permit_missing_qe_tag: bool, art_jira: str):
    await UpdateGolangPipeline(runtime, ocp_version, create_ticket, go_nvrs, permit_missing_qe_tag, art_jira).run()
