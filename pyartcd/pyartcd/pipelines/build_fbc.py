import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import click
from artcommonlib import exectools
from artcommonlib.constants import GROUP_KUBECONFIG_MAP, GROUP_NAMESPACE_MAP

from pyartcd import constants, jenkins, locks
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime


class BuildFbcPipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        assembly: str,
        data_path: str,
        data_gitref: str,
        only: str,
        exclude: str,
        operator_nvrs: str,
        fbc_repo: str,
        kubeconfig: str,
        plr_template: str,
        skip_checks: bool,
        reset_to_prod: bool,
        prod_registry_auth: Optional[str],
        force: bool,
        group: str,
        major_minor: Optional[str],
        ignore_locks: bool,
    ):
        self.runtime = runtime
        self.version = version
        self.assembly = assembly
        self.group = group
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.only = only
        self.exclude = exclude
        self.operator_nvrs = operator_nvrs
        self.fbc_repo = fbc_repo
        self.kubeconfig = kubeconfig
        self.plr_template = plr_template
        self.skip_checks = skip_checks
        self.reset_to_prod = reset_to_prod
        self.prod_registry_auth = prod_registry_auth
        self.force = force
        self.major_minor = major_minor
        self.ignore_locks = ignore_locks

        self._logger = logging.getLogger(__name__)
        self._slack_client = runtime.new_slack_client()
        self._slack_client.bind_channel(version)

    async def run(self):
        try:
            release_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            self._logger.info('Rebasing and building FBC repo with release %s', release_str)
            build_records = await self._rebase_and_build(
                release=release_str,
                commit_message='Rebase FBC segment with release {}'.format(release_str),
            )

            # Parse doozer record.log
            self._logger.info('Parsing doozer record.log')
            lines = []
            for record in build_records:
                lines.append(f'{record["fbc_nvr"]} -> {record["bundle_nvrs"]}')
            self._logger.info('Successfully built: %s', '\n'.join(lines))

        except Exception as e:
            self._logger.error('Encountered error: %s', e)
            await self._slack_client.say(
                f'*:heavy_exclamation_mark: Error building FBC for {self.version} assembly {self.assembly}*\n'
            )
            raise

    async def _run_doozer(self, opts: List[str], only: str, exclude: str):
        # If unspecified, assume it's for openshift
        group = f"openshift-{self.version}" if not self.group else self.group

        cmd = [
            'doozer',
            '--build-system=konflux',
            f'--working-dir={self.runtime.doozer_working}',
            f'--assembly={self.assembly}',
            f'--group={group}{"@" + self.data_gitref if self.data_gitref else ""}',
        ]
        if self.data_path:
            cmd.append(f'--data-path={self.data_path}')
        if only:
            cmd.append(f'--images={only}')
        if exclude:
            cmd.append(f'--exclude={exclude}')
        cmd += opts
        self._logger.info(f'Running doozer command: {" ".join(cmd)}')
        await exectools.cmd_assert_async(cmd)

    async def _rebase_and_build(self, release: str, commit_message: str):
        doozer_opts = [
            'beta:fbc:rebase-and-build',
            '--version',
            self.version,
            '--release',
            release,
            '--message',
            commit_message,
        ]
        if self.runtime.dry_run:
            doozer_opts.append('--dry-run')
        if self.fbc_repo:
            doozer_opts.extend(['--fbc-repo', self.fbc_repo])
        # Set namespace based on group prefix
        group = f"openshift-{self.version}" if not self.group else self.group
        for prefix, namespace in GROUP_NAMESPACE_MAP.items():
            if group.startswith(prefix):
                doozer_opts.extend(['--konflux-namespace', namespace])
                break

        # Use kubeconfig from CLI parameter or tenant-specific environment variable
        final_kubeconfig = self.kubeconfig
        if not final_kubeconfig:
            # Determine the appropriate environment variable based on group prefix
            kubeconfig_env_var = None
            for prefix, env_var in GROUP_KUBECONFIG_MAP.items():
                if group.startswith(prefix):
                    kubeconfig_env_var = env_var
                    break

            if kubeconfig_env_var:
                final_kubeconfig = os.environ.get(kubeconfig_env_var)

            if not final_kubeconfig:
                available_env_vars = list(GROUP_KUBECONFIG_MAP.values())
                raise ValueError(
                    f"Kubeconfig required for Konflux builds. Provide --kubeconfig parameter or set one of: {', '.join(available_env_vars)}"
                )

        doozer_opts.extend(['--konflux-kubeconfig', final_kubeconfig])
        if self.plr_template:
            plr_template_owner, plr_template_branch = (
                self.plr_template.split("@") if self.plr_template else ["openshift-priv", "main"]
            )
            plr_template_url = constants.KONFLUX_FBC_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=plr_template_owner, branch_name=plr_template_branch
            )
            doozer_opts.extend(['--plr-template', plr_template_url])
        if self.skip_checks:
            doozer_opts.append('--skip-checks')
        if self.reset_to_prod:
            doozer_opts.append('--reset-to-prod')
        else:
            doozer_opts.append('--no-reset-to-prod')
        if self.prod_registry_auth:
            doozer_opts.extend(['--prod-registry-auth', self.prod_registry_auth])
        if self.force:
            doozer_opts.append('--force')
        if self.major_minor:
            doozer_opts.extend(['--major-minor', self.major_minor])
        if self.operator_nvrs:
            doozer_opts.extend([nvr for nvr in self.operator_nvrs.split(',')])
        try:
            await self._run_doozer(doozer_opts, only=self.only, exclude=self.exclude)
        finally:
            # Parse both rebase and build records from the combined operation
            successful_rebase_records, failed_rebase_records = await self._parse_record_log('rebase_fbc_konflux')
            successful_build_records, failed_build_records = await self._parse_record_log('build_fbc_konflux')

            self._logger.info(
                'Successfully rebased: %s', ', '.join([str(entry['name']) for entry in successful_rebase_records])
            )
            if failed_rebase_records:
                self._logger.error(
                    'Failed to rebase: %s', ', '.join([str(entry['name']) for entry in failed_rebase_records])
                )

            self._logger.info(
                'Successfully built: %s', ', '.join([str(entry['name']) for entry in successful_build_records])
            )
            if failed_build_records:
                self._logger.error(
                    'Failed to build: %s', ', '.join([str(entry['name']) for entry in failed_build_records])
                )

        return successful_build_records

    async def _parse_record_log(self, entry_type: str):
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        if not record_log_path.exists():
            raise FileNotFoundError('record.log not found')
        with record_log_path.open() as file:
            record_log = parse_record_log(file)
        entries = record_log.get(entry_type, [])
        successful_records = [entry for entry in entries if entry and int(str(entry['status'])) == 0]
        failed_records = [entry for entry in entries if entry and int(str(entry['status'])) != 0]
        return successful_records, failed_records


@cli.command('build-fbc', help='Rebase and build FBC segments for OLM operators')
@click.option('--version', required=True, help='OCP version')
@click.option('--assembly', required=True, help='Assembly name')
@click.option(
    '--data-path',
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=False,
    help="The group of components on which to operate. e.g. openshift-4.9",
)
@click.option('--data-gitref', required=False, help='(Optional) Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--only',
    required=False,
    help='(Optional) List **only** the operators you want to build, everything else gets ignored.\n'
    'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
    'Leave empty to build all (except EXCLUDE, if defined)',
)
@click.option(
    '--exclude',
    required=False,
    help='(Optional) List the operators you **don\'t** want to build, everything else gets built.\n'
    'Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n'
    'Leave empty to build all (or ONLY, if defined)',
)
@click.option(
    '--operator-nvrs',
    required=False,
    help='(Optional) List **only** the operator NVRs you want to build FBC segments for, everything else '
    'gets ignored. The operators should not be mode:disabled/wip in ocp-build-data',
)
@click.option('--fbc-repo', required=False, default='', help='(Optional) URL of the FBC repository')
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option(
    '--plr-template',
    required=False,
    default='',
    help='Override the Pipeline Run template commit from openshift-priv/art-konflux-template; format: <owner>@<branch>',
)
@click.option("--skip-checks", is_flag=True, help="Skip all post build checks in the FBC build pipeline")
@click.option(
    "--reset-to-prod/--no-reset-to-prod", is_flag=True, help="Reset FBC builds to the latest production version"
)
@click.option(
    "--prod-registry-auth",
    metavar='PATH',
    help="The registry authentication file to use for the production index image.",
)
@click.option("--force", is_flag=True, help="Force rebase and build even if already up-to-date")
@click.option(
    "--major-minor",
    metavar='MAJOR.MINOR',
    help="Override the MAJOR.MINOR version from group config (e.g. 4.17).",
)
@click.option(
    '--ignore-locks',
    is_flag=True,
    default=False,
    help='Do not wait for other FBC builds in this group to complete (use only if you know they will not conflict)',
)
@pass_runtime
@click_coroutine
async def build_fbc(
    runtime: Runtime,
    version: str,
    assembly: str,
    data_path: str,
    data_gitref: str,
    only: str,
    exclude: str,
    operator_nvrs: str,
    fbc_repo: str,
    kubeconfig: str,
    plr_template: str,
    skip_checks: bool,
    reset_to_prod: bool,
    prod_registry_auth: Optional[str],
    force: bool,
    group: str,
    major_minor: Optional[str],
    ignore_locks: bool,
):
    pipeline = BuildFbcPipeline(
        runtime=runtime,
        version=version,
        assembly=assembly,
        data_path=data_path,
        data_gitref=data_gitref,
        only=only,
        exclude=exclude,
        operator_nvrs=operator_nvrs,
        fbc_repo=fbc_repo,
        kubeconfig=kubeconfig,
        plr_template=plr_template,
        skip_checks=skip_checks,
        reset_to_prod=reset_to_prod,
        prod_registry_auth=prod_registry_auth,
        force=force,
        group=group,
        major_minor=major_minor,
        ignore_locks=ignore_locks,
    )

    lock_identifier = jenkins.get_build_path()
    if not lock_identifier:
        runtime.logger.warning('Env var BUILD_URL has not been defined: a random identifier will be used for the locks')

    # Use group-based lock name, default to openshift-{version} if group not specified
    final_group = f"openshift-{version}" if not group else group

    if ignore_locks:
        await pipeline.run()
    else:
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=Lock.FBC_BUILD,
            lock_name=Lock.FBC_BUILD.value.format(group=final_group),
            lock_id=lock_identifier,
        )
