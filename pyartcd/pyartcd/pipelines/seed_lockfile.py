import logging
import os
from pathlib import Path
from typing import Optional, Tuple

import click
from artcommonlib import exectools
from doozerlib.constants import ART_BUILD_HISTORY_URL

from pyartcd import constants, jenkins
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.util import build_history_link_url, default_release_suffix

LOGGER = logging.getLogger(__name__)


class SeedLockfilePipeline:
    """
    Pipeline to seed RPM lockfile generation for Konflux images.

    Phase 1 (optional): Build specified components in the 'test' assembly with
    network_mode=open to populate the DB with installed RPMs.

    Phase 2: Run a rebase in the target assembly (default: 'stream') with
    --seed-map pointing to the builds from Phase 1, so lockfile generation
    uses those builds' RPM data.
    """

    @staticmethod
    def _parse_plr_template(plr_template: str) -> tuple[str, str]:
        if not plr_template:
            return 'openshift-priv', 'main'
        if '@' not in plr_template:
            raise click.BadParameter(f"Invalid --plr-template format '{plr_template}': expected <owner>@<branch>")
        return plr_template.split('@', 1)

    def __init__(
        self,
        runtime: Runtime,
        version: str,
        image_list: str,
        assembly: str = 'stream',
        data_path: Optional[str] = None,
        data_gitref: Optional[str] = None,
        kubeconfig: Optional[str] = None,
        arches: Optional[Tuple[str, ...]] = None,
        plr_template: str = '',
        build_priority: str = 'auto',
        seed_nvrs: Optional[str] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.data_path = data_path or constants.OCP_BUILD_DATA_URL
        self.data_gitref = data_gitref
        self.kubeconfig = kubeconfig
        self.arches = arches
        self.plr_template = plr_template
        self.build_priority = build_priority
        self.release = default_release_suffix()

        self.images = [img.strip() for img in image_list.split(',') if img.strip()]
        if not self.images:
            raise click.BadParameter('--image-list must contain at least one image name')

        # Parse pre-existing seed NVRs if provided
        self.seed_map: dict[str, str] = {}
        if seed_nvrs:
            for entry in seed_nvrs.split(','):
                entry = entry.strip()
                if '@' not in entry:
                    raise click.BadParameter(f"Invalid seed-nvrs entry '{entry}': expected format name@NVR")
                name, nvr = entry.split('@', 1)
                self.seed_map[name.strip()] = nvr.strip()

        self.test_results: dict[str, dict] = {}
        self.stream_results: dict[str, dict] = {}

        self.slack_client = runtime.new_slack_client()
        self.slack_client.bind_channel(f'openshift-{self.version}')

    def _doozer_base_command(self, assembly: str) -> list[str]:
        group_param = f'--group=openshift-{self.version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        cmd = [
            'doozer',
            f'--assembly={assembly}',
            f'--working-dir={self.runtime.doozer_working}',
            f'--data-path={self.data_path}',
            '--build-system=konflux',
            group_param,
        ]
        if self.arches:
            cmd.append(f'--arches={",".join(self.arches)}')

        cmd.append(f'--images={",".join(self.images)}')
        cmd.append('--latest-parent-version')

        return cmd

    async def run(self):
        jenkins.init_jenkins()

        if not self.seed_map:
            LOGGER.info('No seed NVRs provided; building in test assembly with network_mode=open')
            await self._build_in_test_assembly()

        if not self.seed_map:
            raise RuntimeError(
                'No successful builds found in Phase 1 and no seed NVRs provided. '
                'Cannot proceed with lockfile generation.'
            )

        LOGGER.info('Generating lockfiles in %s assembly with seed map: %s', self.assembly, self.seed_map)
        await self._rebase_and_build_with_seed_map()

        await self._post_report()

    async def _build_in_test_assembly(self):
        """Phase 1: Build components in the test assembly with network_mode=open."""

        # Rebase
        LOGGER.info('Phase 1: Rebasing images in test assembly with network_mode=open')
        cmd = self._doozer_base_command(assembly='test')
        cmd.extend(
            [
                'beta:images:konflux:rebase',
                f'--version=v{self.version}.0',
                f'--release={self.release}',
                '--network-mode',
                'open',
                "--message='Updating Dockerfile version and release for lockfile seed build'",
            ]
        )
        if not self.runtime.dry_run:
            cmd.append('--push')
        await exectools.cmd_assert_async(cmd)

        # Build
        LOGGER.info('Phase 1: Building images in test assembly with network_mode=open')
        cmd = self._doozer_base_command(assembly='test')
        cmd.extend(
            [
                'beta:images:konflux:build',
                '--konflux-namespace=ocp-art-tenant',
                '--network-mode',
                'open',
            ]
        )
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
        if self.plr_template:
            owner, branch = self._parse_plr_template(self.plr_template)
            plr_template_url = constants.KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=owner, branch_name=branch
            )
            cmd.extend(['--plr-template', plr_template_url])
        if self.runtime.dry_run:
            cmd.append('--dry-run')

        cmd.extend(['--build-priority', self.build_priority])

        # Don't assert -- partial failures are expected; we extract
        # whichever NVRs succeeded from the record.log afterwards.
        rc = await exectools.cmd_assert_async(cmd, check=False)
        if rc != 0:
            LOGGER.warning('Phase 1 build exited with rc=%s; checking for partial successes', rc)

        self._extract_seed_map_from_record_log()

    def _parse_record_log(self) -> dict:
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        if not record_log_path.exists():
            LOGGER.error('record.log not found at %s', record_log_path)
            return {}
        with record_log_path.open('r') as f:
            return record_util.parse_record_log(f)

    def _extract_seed_map_from_record_log(self):
        """Extract component name -> NVR mapping from the Doozer record.log after Phase 1."""
        record_log = self._parse_record_log()

        for entry in record_log.get('image_build_konflux', []):
            name = entry['name']
            status = int(entry.get('status', -1))
            self.test_results[name] = entry
            if status == 0 and entry.get('nvrs'):
                nvr = entry['nvrs'].split(',')[0]
                self.seed_map[name] = nvr
                LOGGER.info('Seed map entry: %s -> %s', name, nvr)

        failed = [n for n, e in self.test_results.items() if int(e.get('status', -1)) != 0]
        if failed:
            LOGGER.warning('Images that failed to build in test assembly: %s', ', '.join(failed))

    def _extract_stream_results_from_record_log(self):
        """Parse record.log after Phase 2 to capture stream build outcomes."""
        record_log = self._parse_record_log()

        # record.log accumulates entries from all doozer invocations; stream
        # build entries appear after the test entries already captured, so we
        # pick the *last* entry per image name (stream build comes after test).
        for entry in record_log.get('image_build_konflux', []):
            name = entry['name']
            # Overwrite -- the last entry per name is the stream build
            self.stream_results[name] = entry

    async def _rebase_and_build_with_seed_map(self):
        """Phase 2: Rebase and build in the target assembly with --lockfile-seed-nvrs for lockfile generation."""
        seed_nvrs_str = ','.join(self.seed_map.values())

        # Rebase with seeded lockfile
        LOGGER.info('Phase 2: Rebasing images in %s assembly with seed NVRs', self.assembly)
        cmd = self._doozer_base_command(assembly=self.assembly)
        cmd.extend(
            [
                'beta:images:konflux:rebase',
                f'--version=v{self.version}.0',
                f'--release={self.release}',
                f'--lockfile-seed-nvrs={seed_nvrs_str}',
                "--message='Updating Dockerfile version and release with seeded lockfile'",
            ]
        )
        if not self.runtime.dry_run:
            cmd.append('--push')
        await exectools.cmd_assert_async(cmd)

        # Build in the target assembly with the new lockfiles
        LOGGER.info('Phase 2: Building images in %s assembly with seeded lockfiles', self.assembly)
        cmd = self._doozer_base_command(assembly=self.assembly)
        cmd.extend(
            [
                'beta:images:konflux:build',
                '--konflux-namespace=ocp-art-tenant',
            ]
        )
        if self.kubeconfig:
            cmd.extend(['--konflux-kubeconfig', self.kubeconfig])
        if self.plr_template:
            owner, branch = self._parse_plr_template(self.plr_template)
            plr_template_url = constants.KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=owner, branch_name=branch
            )
            cmd.extend(['--plr-template', plr_template_url])
        if self.runtime.dry_run:
            cmd.append('--dry-run')
        cmd.extend(['--build-priority', self.build_priority])

        rc = await exectools.cmd_assert_async(cmd, check=False)
        if rc != 0:
            LOGGER.warning('Phase 2 build exited with rc=%s; checking for partial successes', rc)

        self._extract_stream_results_from_record_log()
        LOGGER.info('Seeded lockfile rebase and build complete')

    @staticmethod
    def _build_url(group: str, entry: dict) -> str:
        """Build a direct link to a build page in art-build-history."""
        nvr = entry.get('nvrs', '').split(',')[0]
        record_id = entry.get('record_id', '')
        outcome = 'success' if int(entry.get('status', -1)) == 0 else 'failure'
        if nvr and nvr != 'n/a' and record_id:
            return (
                f'{ART_BUILD_HISTORY_URL}/build?nvr={nvr}&record_id={record_id}'
                f'&group={group}&outcome={outcome}&type=image'
            )
        if nvr and nvr != 'n/a':
            return f'{ART_BUILD_HISTORY_URL}/?nvr={nvr}&group={group}&engine=konflux'
        return ''

    def _categorize_results(self) -> tuple[list[str], list[str], list[str]]:
        test_failed: list[str] = []
        solved: list[str] = []
        stream_failed: list[str] = []

        for name in self.images:
            test_ok = int(self.test_results.get(name, {}).get('status', -1)) == 0 if self.test_results else True
            stream_entry = self.stream_results.get(name, {})
            stream_ok = int(stream_entry.get('status', -1)) == 0

            if not test_ok:
                test_failed.append(name)
            elif stream_ok:
                solved.append(name)
            else:
                stream_failed.append(name)

        return test_failed, solved, stream_failed

    def _link(self, name: str, phase: str, group: str) -> str:
        """Return an art-build-history URL for a build, or empty string if unavailable."""
        results = self.test_results if phase == 'test' else self.stream_results
        entry = results.get(name, {})
        return self._build_url(group, entry) if entry else ''

    def _build_slack_report(self, test_failed: list[str], solved: list[str], stream_failed: list[str]) -> str:
        group = f'openshift-{self.version}'
        job_url = os.getenv('BUILD_URL', '')

        lines: list[str] = []
        if job_url:
            lines.append(f':seedling: *<{job_url}|seed-lockfile>* results for *{self.version}*:')
        else:
            lines.append(f':seedling: *seed-lockfile* results for *{self.version}*:')

        if test_failed:
            lines.append('\n*`network_mode: open` build failed:*')
            for name in test_failed:
                url = self._link(name, 'test', group)
                if url:
                    lines.append(f'  - <{url}|`{name}`>')
                else:
                    lines.append(f'  - `{name}`')

        if solved:
            lines.append('\n*Solved:*')
            for name in solved:
                url = self._link(name, 'stream', group)
                if url:
                    lines.append(f'  - <{url}|`{name}`>')
                else:
                    lines.append(f'  - `{name}`')

        if stream_failed:
            lines.append('\n*Stream build failed after successful test build:*')
            for name in stream_failed:
                test_url = self._link(name, 'test', group)
                stream_url = self._link(name, 'stream', group)
                parts = [f'`{name}`']
                if test_url:
                    parts.append(f'<{test_url}|test>')
                if stream_url:
                    parts.append(f'<{stream_url}|stream>')
                lines.append(f'  - {" | ".join(parts)}')

        if not test_failed and not solved and not stream_failed:
            lines.append('No build results recorded.')

        return '\n'.join(lines)

    def _build_jenkins_description(self, test_failed: list[str], solved: list[str], stream_failed: list[str]) -> str:
        group = f'openshift-{self.version}'
        lines: list[str] = []

        if test_failed:
            lines.append('<b>network_mode: open build failed:</b><ul>')
            for name in test_failed:
                url = self._link(name, 'test', group)
                if url:
                    lines.append(f'<li><a href="{url}">{name}</a></li>')
                else:
                    lines.append(f'<li>{name}</li>')
            lines.append('</ul>')

        if solved:
            lines.append('<b>Solved:</b><ul>')
            for name in solved:
                url = self._link(name, 'stream', group)
                if url:
                    lines.append(f'<li><a href="{url}">{name}</a></li>')
                else:
                    lines.append(f'<li>{name}</li>')
            lines.append('</ul>')

        if stream_failed:
            lines.append('<b>Stream build failed after successful test build:</b><ul>')
            for name in stream_failed:
                test_url = self._link(name, 'test', group)
                stream_url = self._link(name, 'stream', group)
                parts = [name]
                if test_url:
                    parts.append(f'<a href="{test_url}">test</a>')
                if stream_url:
                    parts.append(f'<a href="{stream_url}">stream</a>')
                lines.append(f'<li>{" — ".join(parts)}</li>')
            lines.append('</ul>')

        if not lines:
            lines.append('No build results recorded.')

        return '\n'.join(lines)

    async def _post_report(self):
        test_failed, solved, stream_failed = self._categorize_results()

        slack_report = self._build_slack_report(test_failed, solved, stream_failed)
        LOGGER.info('Report:\n%s', slack_report)
        await self.slack_client.say(slack_report)

        try:
            html = self._build_jenkins_description(test_failed, solved, stream_failed)
            jenkins.update_description(html)

            # Add link to art-build-history
            job_url = os.getenv('BUILD_URL', '')
            build_history_url = build_history_link_url(
                group=f'openshift-{self.version}', assembly=self.assembly, days=2, job_url=job_url
            )
            jenkins.update_description(f'<a href="{build_history_url}">View build history</a><br/>')

            if solved:
                jenkins.update_title(f' | solved: {", ".join(solved)}')
        except Exception:
            LOGGER.warning('Could not update Jenkins build description/title', exc_info=True)


@cli.command('seed-lockfile', help='Seed RPM lockfile generation by building in test assembly and rebasing in stream')
@click.option('--version', required=True, help='OCP version, e.g. 4.22')
@click.option('--assembly', default='stream', help='Target assembly for lockfile generation (default: stream)')
@click.option(
    '--image-list',
    required=True,
    help='Comma-separated list of component names to seed, e.g. ironic,ovn-kubernetes',
)
@click.option(
    '--seed-nvrs',
    default=None,
    help='Pre-existing build NVRs to skip the test build. '
    'Format: name@NVR[,name@NVR,...]. '
    'Example: ironic@ironic-container-v4.22.0-assembly.test',
)
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option('--kubeconfig', required=False, help='Path to kubeconfig file for Konflux cluster connections')
@click.option(
    '--arch', 'arches', metavar='TAG', multiple=True, help='(Optional) [MULTIPLE] Limit included arches to this list'
)
@click.option(
    '--plr-template',
    required=False,
    default='',
    help='Override the Pipeline Run template commit; format: <owner>@<branch>',
)
@click.option(
    '--build-priority',
    type=str,
    metavar='PRIORITY',
    default='auto',
    help='Kueue build priority (1-10 or "auto")',
)
@pass_runtime
@click_coroutine
async def seed_lockfile(
    runtime: Runtime,
    version: str,
    assembly: str,
    image_list: str,
    seed_nvrs: Optional[str],
    data_path: Optional[str],
    data_gitref: Optional[str],
    kubeconfig: Optional[str],
    arches: Tuple[str, ...],
    plr_template: str,
    build_priority: str,
):
    if not kubeconfig:
        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    pipeline = SeedLockfilePipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        image_list=image_list,
        seed_nvrs=seed_nvrs,
        data_path=data_path,
        data_gitref=data_gitref,
        kubeconfig=kubeconfig,
        arches=arches,
        plr_template=plr_template,
        build_priority=build_priority,
    )
    await pipeline.run()
