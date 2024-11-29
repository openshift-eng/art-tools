import asyncio
import logging
from pathlib import Path
import traceback
from typing import Optional

import click
from artcommonlib.telemetry import start_as_current_span_async
from opentelemetry import trace

from doozerlib import constants
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder, KonfluxImageBuilderConfig, KonfluxBuildRecord
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli import (cli, click_coroutine, option_commit_message,
                           option_push, pass_runtime,
                           validate_semver_major_minor_patch)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.runtime import Runtime

TRACER = trace.get_tracer(__name__)
LOGGER = logging.getLogger(__name__)


class KonfluxRebaseCli:
    def __init__(
            self,
            runtime: Runtime,
            version: str,
            release: str,
            embargoed: bool,
            force_yum_updates: bool,
            repo_type: str,
            image_repo: str,
            message: str,
            push: bool):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.embargoed = embargoed
        self.force_yum_updates = force_yum_updates
        if repo_type not in ['signed', 'unsigned']:
            raise click.BadParameter(f"repo_type must be one of 'signed' or 'unsigned'. Got: {repo_type}")
        self.repo_type = repo_type
        self.image_repo = image_repo
        self.message = message
        self.push = push
        self.upcycle = runtime.upcycle

    @start_as_current_span_async(TRACER, "beta:images:konflux:rebase")
    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False)
        assert runtime.source_resolver is not None, "source_resolver is required for this command"
        metas = runtime.ordered_image_metas()
        base_dir = Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_BUILD_SOURCES)
        rebaser = KonfluxRebaser(
            runtime=runtime,
            base_dir=base_dir,
            source_resolver=runtime.source_resolver,
            repo_type=self.repo_type,
            upcycle=self.upcycle,
            force_private_bit=self.embargoed,
        )
        tasks = []
        for image_meta in metas:
            tasks.append(asyncio.create_task(rebaser.rebase_to(
                image_meta,
                self.version,
                self.release,
                force_yum_updates=self.force_yum_updates,
                image_repo=self.image_repo,
                commit_message=self.message,
                push=self.push)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_images = []
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                image_name = metas[index].distgit_key
                failed_images.append(image_name)
                LOGGER.error(f"Failed to rebase {image_name}: {result}")
        if failed_images:
            runtime.state['images:konflux:rebase'] = {'failed-images': failed_images}
            raise DoozerFatalError(f"Failed to rebase images: {failed_images}")
        LOGGER.info("Rebase complete")


@cli.command("beta:images:konflux:rebase", short_help="Refresh a group's konflux source content from source content.")
@click.option("--version", metavar='VERSION', required=True, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@click.option("--embargoed", is_flag=True, help="Add .p1 to the release string for all images, which indicates those images have embargoed fixes")
@click.option("--force-yum-updates", is_flag=True, default=False,
              help="Inject \"yum update -y\" in the final stage of an image build. This ensures the component image will be able to override RPMs it is inheriting from its parent image using RPMs in the rebuild plashet.")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use (e.g. signed, unsigned).")
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Image repo for base images')
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def images_konflux_rebase(runtime: Runtime, version: str, release: str, embargoed: bool, force_yum_updates: bool,
                                repo_type: str, image_repo: str, message: str, push: bool):
    """
    Refresh a group's konflux content from source content.
    """
    cli = KonfluxRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        embargoed=embargoed,
        force_yum_updates=force_yum_updates,
        repo_type=repo_type,
        image_repo=image_repo,
        message=message,
        push=push,
    )
    await cli.run()


class KonfluxBuildCli:
    def __init__(
        self,
        runtime: Runtime,
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: Optional[str],
        image_repo: str,
        skip_checks: bool,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.dry_run = dry_run

    @start_as_current_span_async(TRACER, "images:konflux:build")
    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False)
        runtime.konflux_db.bind(KonfluxBuildRecord)
        assert runtime.source_resolver is not None, "source_resolver is not initialized. Doozer bug?"
        metas = runtime.ordered_image_metas()
        config = KonfluxImageBuilderConfig(
            base_dir=Path(runtime.working_dir, constants.WORKING_SUBDIR_KONFLUX_BUILD_SOURCES),
            group_name=runtime.group,
            kubeconfig=self.konflux_kubeconfig,
            context=self.konflux_context,
            namespace=self.konflux_namespace,
            image_repo=self.image_repo,
            skip_checks=self.skip_checks,
            dry_run=self.dry_run
        )
        builder = KonfluxImageBuilder(config=config)
        tasks = []
        for image_meta in metas:
            tasks.append(asyncio.create_task(builder.build(image_meta)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failed_images = []
        for index, result in enumerate(results):
            if isinstance(result, Exception):
                image_name = metas[index].distgit_key
                failed_images.append(image_name)
                stack_trace = ''.join(traceback.TracebackException.from_exception(result).format())
                LOGGER.error(f"Failed to build {image_name}: {result}; {stack_trace}")
        if failed_images:
            raise DoozerFatalError(f"Failed to build images: {failed_images}")
        LOGGER.info("Build complete")


@cli.command("beta:images:konflux:build", short_help="Build images for the group.")
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', required=True, help='The namespace to use for Konflux cluster connections.')
@click.option('--image-repo', default=constants.KONFLUX_DEFAULT_IMAGE_REPO, help='Push images to the specified repo.')
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
@click_coroutine
async def images_konflux_build(
        runtime: Runtime, konflux_kubeconfig: Optional[str], konflux_context: Optional[str],
        konflux_namespace: Optional[str], image_repo: str, skip_checks: bool, dry_run: bool):
    cli = KonfluxBuildCli(
        runtime=runtime, konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context, konflux_namespace=konflux_namespace,
        image_repo=image_repo, skip_checks=skip_checks, dry_run=dry_run)
    await cli.run()
