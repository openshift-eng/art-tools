import asyncio
import datetime
import logging
import os
from typing import List

import click
import koji
from artcommonlib import exectools
from artcommonlib.constants import BREW_HUB
from artcommonlib.rpm_utils import parse_nvr
from elliottlib import util as elliottutil
from ghapi.all import GhApi

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.update_golang import extract_and_validate_golang_nvrs, is_latest_and_available, move_golang_bugs
from pyartcd.runtime import Runtime

try:
    from specfile import Specfile  # Linux only
except ImportError:
    pass

_LOGGER = logging.getLogger(__name__)


class RebuildGolangRPMsPipeline:
    def __init__(
        self,
        runtime: Runtime,
        ocp_version: str,
        art_jira: str,
        cves: List[str],
        go_nvrs: List[str],
        force: bool = False,
        rpms: List[str] = None,
    ):
        if "Specfile" not in globals():
            raise RuntimeError("This pipeline requires the 'specfile' module, which is only available on Linux")
        self.runtime = runtime
        self.ocp_version = ocp_version
        self.go_nvrs = go_nvrs
        self.art_jira = art_jira
        self.cves = cves
        self.force = force
        self.rpms = rpms
        self.koji_session = koji.ClientSession(BREW_HUB)

    async def run(self):
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(self.ocp_version, self.go_nvrs)
        _LOGGER.info(f'Golang version detected: {go_version}')
        _LOGGER.info(f'NVRs by rhel version: {el_nvr_map}')

        # For rpms - first make sure builds are tagged and available
        _LOGGER.info('Checking if golang builds are tagged and available in rpm buildroots')
        error_msg = ""
        for el_v, nvr in el_nvr_map.items():
            if await is_latest_and_available(self.ocp_version, el_v, nvr, self.koji_session):
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
        if self.rpms:
            art_built_rpms = [r for r in art_built_rpms if r in self.rpms]

        non_art_rpms_for_rebuild = dict()
        art_rpms_for_rebuild = set()
        for el_v, nvr in el_nvr_map.items():
            _LOGGER.info(f'Fetching all rhel{el_v} rpms in our candidate tag')
            rpms = [parse_nvr(n) for n in self.get_rpms(el_v) if 'microshift' not in n]
            if self.rpms:
                rpms = [r for r in rpms if r['name'] in self.rpms]
            _LOGGER.info('Determining golang rpms and their build versions')
            go_nvr_map = elliottutil.get_golang_rpm_nvrs(
                [(n['name'], n['version'], n['release']) for n in rpms],
                _LOGGER,
            )
            for go_v, nvrs in go_nvr_map.items():
                nvr_s = [f'{n[0]}-{n[1]}-{n[2]}' for n in nvrs]
                if not self.force:
                    if go_v in nvr:
                        _LOGGER.info(f'Builds on latest go version {go_v}: {nvr_s}')
                        continue
                    _LOGGER.info(f'Builds on previous go version {go_v}: {nvr_s}')
                else:
                    _LOGGER.info('Forcing rebuild since --force flag is set')
                art_rpms_for_rebuild.update([n[0] for n in nvrs if n[0] in art_built_rpms])
                if el_v not in non_art_rpms_for_rebuild:
                    non_art_rpms_for_rebuild[el_v] = []
                non_art_rpms_for_rebuild[el_v].extend([n[0] for n in nvrs if n[0] not in art_built_rpms])

            if non_art_rpms_for_rebuild.get(el_v):
                _LOGGER.info(
                    f'These non-ART rhel{el_v} rpms are on previous golang versions, they need to be '
                    f'rebuilt: {sorted(non_art_rpms_for_rebuild[el_v])}'
                )

        if art_rpms_for_rebuild:
            _LOGGER.info(
                f'These ART rpms are on previous golang versions, they need to be rebuilt: {sorted(art_rpms_for_rebuild)}'
            )
            _LOGGER.info(
                "Note: RPM NVRs are fetched from candidate tags so they may include nvrs for old rhel versions that are no longer rpm targets. If an old NVR like that is the only reason to rebuild, then it is safe to ignore it. find-bugs:golang --analyze will automatically filter out these old NVRs."
            )
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

        results = await asyncio.gather(
            *[
                self.bump_and_rebuild_rpm(rpm, el_v, author, email)
                for el_v, rpms in non_art_rpms_for_rebuild.items()
                for rpm in rpms
            ],
            return_exceptions=True,
        )
        list_of_rpms = [rpm for el_v, rpms in non_art_rpms_for_rebuild.items() for rpm in rpms]
        failed_rpms = [rpm for rpm, result in zip(list_of_rpms, results) if isinstance(result, Exception)]
        if failed_rpms:
            _LOGGER.error(f'Error bumping and rebuilding these rpms: {failed_rpms}')
            await self.notify_failed_rpms(failed_rpms)
            raise RuntimeError(f'Bumping and rebuilding failed for these rpms: {failed_rpms}')

        await move_golang_bugs(
            ocp_version=self.ocp_version,
            cves=self.cves,
            nvrs=self.go_nvrs if self.cves else None,
            dry_run=self.runtime.dry_run,
        )

    async def notify_failed_rpms(self, rpms: list):
        slack_client = self.runtime.new_slack_client()
        major, minor = self.ocp_version.split('.')  # 4.18 => 4, 18
        channel = f'#art-release-{major}-{minor}'  # e.g. #art-release-4-18
        slack_client.bind_channel(channel)
        message = f'Following golang RPMs failed to rebuild in {self.ocp_version}:'
        message += '\n'.join(rpms)
        message += f'\nSee {jenkins.get_build_url()} for details'
        await slack_client.say(message)

    def get_art_built_rpms(self):
        github_token = os.environ.get('GITHUB_TOKEN')
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to fetch build data repo contents")
        api = GhApi(owner='openshift-eng', repo='ocp-build-data', token=github_token)
        branch = f'openshift-{self.ocp_version}'
        content = api.repos.get_content(path='rpms', ref=branch)
        return [c['name'].removesuffix('.yml') for c in content if c['name'].endswith('.yml')]

    def rebuild_art_rpms(self, rpms):
        jenkins.start_ocp4_konflux(
            build_version=self.ocp_version,
            assembly='stream',
            image_list=[],
            rpm_list=rpms,
            dry_run=self.runtime.dry_run
        )

    async def bump_and_rebuild_rpm(self, rpm, el_v, author, email):
        branch = f'rhaos-{self.ocp_version}-rhel-{el_v}'
        dg_dir = f'{rpm}-{el_v}'
        # check if dir exists
        if not os.path.isdir(dg_dir):
            cmd = f'rhpkg clone --branch {branch} rpms/{rpm} {dg_dir}'
            await exectools.cmd_assert_async(cmd)
        else:
            await exectools.cmd_assert_async('git reset --hard', cwd=dg_dir)
            await exectools.cmd_assert_async('git fetch --all', cwd=dg_dir)
        await exectools.cmd_assert_async(f'git checkout {branch}', cwd=dg_dir)
        await exectools.cmd_assert_async('git reset --hard @{upstream}', cwd=dg_dir)

        # get last commit message on this branch
        bump_msg = f'Bump and rebuild with latest golang, resolves {self.art_jira}'
        rc, commit_message, _ = await exectools.cmd_gather_async('git log -1 --format=%s', cwd=dg_dir)
        if rc != 0:
            raise ValueError(f'Cannot get last commit message for {rpm} for {branch}')
        if bump_msg in commit_message:
            _LOGGER.info(f'{dg_dir}/{branch} - Bump commit exists on branch, build in queue? skipping')
            return

        # get all .spec files
        specs = [f for f in os.listdir(dg_dir) if f.endswith('.spec')]
        if len(specs) != 1:
            raise ValueError(f'Expected to find only 1 .spec file in {dg_dir}, found: {specs}')

        spec = Specfile(os.path.join(dg_dir, specs[0]))

        _LOGGER.info(f'{dg_dir}/{branch} - Bumping release in specfile')

        _LOGGER.info(f'{dg_dir}/{branch} - Current release in specfile: {spec.release}')

        spec.release = self.bump_release(spec.release)

        _LOGGER.info(f'{dg_dir}/{branch} - New release in specfile: {spec.release}')

        _LOGGER.info(f'{dg_dir}/{branch} - Adding changelog entry in specfile')
        spec.add_changelog_entry(
            bump_msg,
            author=author,
            email=email,
            timestamp=datetime.date.today(),
        )

        if self.runtime.dry_run:
            _LOGGER.info(f"{dg_dir}/{branch} - Dry run, would've committed changes and triggered build")
            return

        spec.save()
        cmd = f'git commit -am "{bump_msg}"'
        await exectools.cmd_assert_async(cmd, cwd=dg_dir)
        cmd = 'git push'
        await exectools.cmd_assert_async(cmd, cwd=dg_dir)
        cmd = 'rhpkg build'
        await exectools.cmd_assert_async(cmd, cwd=dg_dir)

    def get_rpms(self, el_v):
        # get all the go rpms from the candidate tag
        tag = f'rhaos-{self.ocp_version}-rhel-{el_v}-candidate'
        latest_builds = [b['nvr'] for b in self.koji_session.listTagged(tag, latest=True)]
        rpms = [b for b in latest_builds if not ('-container' in b or b.startswith('rhcos-'))]
        return rpms

    @staticmethod
    # Squash the Release segment to it's significant number and bump
    # 42.rhaos4_8.el8 -> 43.rhaos4_8.el8
    # 42.1.rhaos4_8.el8 -> 43.rhaos4_8.el8
    # rhaos4_8.el8 -> 1.rhaos4_8.el8
    def bump_release(release):
        k = release.split('.')
        numbers = []
        non_number_marker = None
        for p in k:
            if p.isdigit():
                numbers.append(p)
            else:
                non_number_marker = release.index(p)
                break

        if len(numbers) < 1:
            rel = "1"
        else:
            rel = f'{int(numbers[0]) + 1}'

        if non_number_marker is not None:
            rel += f'.{release[non_number_marker:]}'

        return rel


@cli.command('rebuild-golang-rpms')
@click.option('--ocp-version', required=True, help='OCP version to rebuild golang rpms for')
@click.option('--art-jira', required=True, help='Related ART Jira ticket e.g. ART-1234')
@click.option(
    '--cves', help='CVE-IDs that are confirmed to be fixed with given nvrs (comma separated) e.g. CVE-2024-1234'
)
@click.option('--force', help='Force rebuild rpms even if they are on given golang', is_flag=True)
@click.option('--rpms', help='Only consider these rpm(s) for rebuild')
@click.argument('go_nvrs', metavar='GO_NVRS...', nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def rebuild_golang_rpms(
    runtime: Runtime, ocp_version: str, art_jira: str, cves: str, force: bool, rpms: str, go_nvrs: List[str]
):
    if rpms:
        rpms = rpms.split(',')
    if cves:
        cves = cves.split(',')
    await RebuildGolangRPMsPipeline(
        runtime, ocp_version=ocp_version, art_jira=art_jira, cves=cves, force=force, rpms=rpms, go_nvrs=go_nvrs
    ).run()
