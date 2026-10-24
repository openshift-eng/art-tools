import json
from typing import Optional

import click
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE

from doozerlib import constants
from doozerlib.cli import cli, click_coroutine, option_commit_message, option_push, pass_runtime
from doozerlib.cli.images_konflux import KonfluxBuildCli, KonfluxRebaseCli, _validate_version
from doozerlib.runtime import Runtime


@cli.command("binaries:list", help="List the binaries members being selected.")
@click.option("--json", "is_json", is_flag=True, default=False, help="Output the list in JSON format.")
@pass_runtime
def binaries_list(runtime: Runtime, is_json: bool):
    runtime.initialize(mode='binaries', clone_distgits=False, prevent_cloning=True)
    keys = [meta.distgit_key for meta in runtime.binary_metas()]
    if is_json:
        click.echo(json.dumps({"binaries": keys}))
        return
    for key in keys:
        click.echo(key)
    click.echo(f"{len(keys)} binaries")


@cli.command("beta:binaries:konflux:rebase", short_help="Refresh the build source of a group's binaries.")
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=_validate_version,
    help="Version string to populate in Dockerfiles.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@click.option("--embargoed", is_flag=True, help="Add .p3 to the release string")
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default="unsigned",
    help="Repo group type to use (e.g. signed, unsigned).",
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Image repo for base images')
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def binaries_konflux_rebase(
    runtime: Runtime,
    version: str,
    release: str,
    embargoed: bool,
    repo_type: str,
    image_repo: str,
    message: str,
    push: bool,
):
    """
    Refresh the Konflux build source of binaries/ members, the same way beta:images:konflux:rebase does for images.

    \b
    $ doozer -g openshift-4.22 --assembly 4.22.18 --latest-parent-version --binaries openshift-install \\
        beta:binaries:konflux:rebase --version v4.22.0 --release 202610210000.p2 --message "..." --push
    """
    await KonfluxRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        embargoed=embargoed,
        force_yum_updates=False,
        repo_type=repo_type,
        image_repo=image_repo,
        message=message,
        push=push,
        kind="binary",
    ).run()


@cli.command("beta:binaries:konflux:build", short_help="Build a group's binaries with Konflux.")
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux connections.'
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Push images to the specified repo.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option('--plr-template', required=False, help='Use a custom PipelineRun template.')
@click.option(
    '--build-priority',
    type=str,
    metavar='PRIORITY',
    default='auto',
    help='Kueue build priority: "auto" or a number 1-10 (1 is highest).',
)
@click.option(
    '--build-arg',
    'build_args',
    multiple=True,
    metavar='KEY=VALUE',
    help='Extra Konflux build argument, appended after konflux.build_args of every binary built. Repeatable.',
)
@pass_runtime
@click_coroutine
async def binaries_konflux_build(
    runtime: Runtime,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    image_repo: str,
    dry_run: bool,
    plr_template: Optional[str],
    build_priority: str,
    build_args: tuple,
):
    """
    Build binaries/ members with the Konflux image build pipeline. Results go to record.log as binary_build_konflux.

    \b
    $ doozer -g openshift-4.22 --assembly 4.22.18 --binaries openshift-install \\
        beta:binaries:konflux:build --konflux-namespace ocp-art-tenant --build-arg RELEASE_VERSION=4.22.18
    """
    await KonfluxBuildCli(
        runtime=runtime,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        image_repo=image_repo,
        registry_auth_file=runtime.registry_config,
        skip_checks=False,
        dry_run=dry_run,
        plr_template=plr_template,
        build_priority=build_priority,
        extra_build_args=build_args,
        kind="binary",
    ).run()
