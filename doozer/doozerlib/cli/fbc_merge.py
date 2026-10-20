"""CLI for merging layered-product FBC builds for one OCP target."""

import json
import os
from pathlib import Path

import click
from artcommonlib.util import resolve_konflux_kubeconfig_by_product, resolve_konflux_namespace_by_product

from doozerlib import constants
from doozerlib.backend.konflux_fbc_merge import LayeredFbcMerger
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.runtime import Runtime


@cli.command('beta:fbc:merge-layered', short_help='Build a unified layered-product FBC for one OCP target')
@pass_runtime
@click.option(
    '--fbc-build', 'builds', multiple=True, required=True, help='Source FBC NVR or pullspec. Repeat per build.'
)
@click.option('--major-minor', required=True, help='Target OCP MAJOR.MINOR version, e.g. 4.20 or 5.0.')
@click.option('--fbc-repo', default=constants.ART_FBC_GIT_REPO, show_default=True)
@click.option(
    '--image-repo',
    default=constants.KONFLUX_DEFAULT_FBC_REPO,
    show_default=True,
    help='Push images to the specified repo.',
)
@click.option('--konflux-kubeconfig', type=click.Path())
@click.option('--konflux-context')
@click.option('--konflux-namespace')
@click.option('--registry-auth', type=click.Path(exists=True), envvar='QUAY_AUTH_FILE')
@click.option('--plr-template', default=constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL)
@click.option('--skip-checks', is_flag=True, help='Skip post-build checks.')
@click.option('--skip-task', 'skip_tasks', multiple=True)
@click.option('--dry-run', is_flag=True, help='Render and validate locally without remote writes.')
@click.option('--output-file', type=click.Path(dir_okay=False), help='Write the JSON build result to this file.')
@click_coroutine
async def fbc_merge_layered(
    runtime: Runtime,
    builds: tuple[str, ...],
    major_minor: str,
    fbc_repo: str,
    image_repo: str | None,
    konflux_kubeconfig: str | None,
    konflux_context: str | None,
    konflux_namespace: str | None,
    registry_auth: str | None,
    plr_template: str,
    skip_checks: bool,
    skip_tasks: tuple[str, ...],
    dry_run: bool,
    output_file: str | None,
):
    runtime.initialize(build_system='konflux', config_only=True)
    if runtime.product == 'ocp' or runtime.group.startswith('openshift-'):
        raise click.ClickException('Use beta:fbc:merge for OpenShift FBCs')
    namespace = resolve_konflux_namespace_by_product(runtime.product, konflux_namespace)
    merger = LayeredFbcMerger(
        runtime=runtime,
        target_version=major_minor,
        registry_auth=registry_auth,
        fbc_git_repo=fbc_repo,
        fbc_git_username=os.environ.get('FBC_GIT_USERNAME'),
        fbc_git_password=os.environ.get('FBC_GIT_PASSWORD'),
        dry_run=dry_run,
        upcycle=runtime.upcycle,
        konflux_namespace=namespace,
        konflux_kubeconfig=resolve_konflux_kubeconfig_by_product(runtime.product, konflux_kubeconfig),
        konflux_context=konflux_context,
        plr_template=plr_template,
        skip_checks=skip_checks,
        skip_tasks=skip_tasks,
    )
    await merger.prepare(builds)
    result = await merger.merge(image_repo)
    output = json.dumps(result, indent=2) + '\n'
    if output_file:
        destination = Path(output_file)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(output)
    click.echo(output, nl=False)
