from typing import Optional
import click
from opentelemetry import trace

from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from doozerlib.runtime import Runtime
from doozerlib.cli import pass_runtime, cli, click_coroutine, validate_semver_major_minor_patch, option_commit_message, option_push
from doozerlib.telemetry import start_as_current_span_async


TRACER = trace.get_tracer(__name__)


class KonfluxRebaseCli:
    def __init__(
            self,
            runtime: Runtime,
            version: str,
            release: str,
            embargoed: bool,
            force_yum_updates: bool,
            message: str,
            push: bool):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.embargoed = embargoed
        self.force_yum_updates = force_yum_updates
        self.message = message
        self.push = push
        self.upcycle = runtime.upcycle
        pass

    @start_as_current_span_async(TRACER, "beta:images:konflux:rebase")
    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False)
        assert runtime.source_resolver is not None, "source_resolver is required for this command"
        # FIXME: Rebase ordering is important
        metas = runtime.ordered_image_metas()
        # metas = runtime.image_metas()
        rebaser = KonfluxRebaser(
            runtime=runtime,
            source_resolver=runtime.source_resolver,
            upcycle=self.upcycle,
            force_private_bit=self.embargoed,
        )
        for image_meta in metas:
            await rebaser.rebase_to(
                image_meta,
                self.version,
                self.release,
                force_yum_updates=self.force_yum_updates,
                commit_message=self.message,
                push=self.push)
        print("test konflux: Done")


@cli.command("beta:images:konflux:rebase", short_help="Refresh a group's konflux source content from source content.")
@click.option("--version", metavar='VERSION', required=True, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', required=True, help="Release string to populate in Dockerfiles.")
@click.option("--embargoed", is_flag=True, help="Add .p1 to the release string for all images, which indicates those images have embargoed fixes")
@click.option("--force-yum-updates", is_flag=True, default=False,
              help="Inject \"yum update -y\" in the final stage of an image build. This ensures the component image will be able to override RPMs it is inheriting from its parent image using RPMs in the rebuild plashet.")
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def images_konflux_rebase(runtime: Runtime, version: str, release: str, embargoed: bool, force_yum_updates: bool, message: str, push: bool):
    """
    Refresh a group's konflux content from source content.
    """
    cli = KonfluxRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        embargoed=embargoed,
        force_yum_updates=force_yum_updates,
        message=message,
        push=push,
    )
    await cli.run()


class KonfluxBuildCli:
    def __init__(
            self,
            runtime: Runtime,
            push_to_defaults: bool,
            push_to: list,
            scratch: bool,
            threads: int,
            filter_by_os: Optional[str],
            dry_run: bool,
            build_retries: int):
        self.runtime = runtime
        self.push_to_defaults = push_to_defaults
        self.push_to = push_to
        self.scratch = scratch
        self.threads = threads
        self.filter_by_os = filter_by_os
        self.dry_run = dry_run
        self.build_retries = build_retries

    @start_as_current_span_async(TRACER, "images:konflux:build")
    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False)
        assert runtime.source_resolver is not None, "source_resolver is required for this command"
        metas = runtime.ordered_image_metas()
        builder = KonfluxImageBuilder(runtime=runtime, source_resolver=runtime.source_resolver)

        for image_meta in metas:
            await builder.build(image_meta)
        print("test konflux build: Done")


@cli.command("beta:images:konflux:build", short_help="Build images for the group.")
@click.option('--push-to-defaults', default=False, is_flag=True,
              help='Push to default registries when build completes.')
@click.option("--push-to", default=[], metavar="REGISTRY", multiple=True,
              help="Specific registries to push to when image build completes.  [multiple]")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option("--threads", default=1, metavar="NUM_THREADS",
              help="Number of concurrent builds to execute. Only valid for --local builds.")
@click.option("--filter-by-os", default=None, metavar="ARCH",
              help="Specify an exact arch to push (golang name e.g. 'amd64').")
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option('--build-retries', type=int, default=1, help='Number of build attempts for an osbs build')
@pass_runtime
@click_coroutine
async def images_konflux_build(runtime: Runtime, push_to_defaults: bool, push_to: list, scratch: bool, threads: int, filter_by_os: Optional[str], dry_run: bool, build_retries: int):
    cli = KonfluxBuildCli(runtime=runtime, push_to_defaults=push_to_defaults, push_to=push_to, scratch=scratch, threads=threads, filter_by_os=filter_by_os, dry_run=dry_run, build_retries=build_retries)
    await cli.run()
