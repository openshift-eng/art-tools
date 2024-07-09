from typing import Optional
import click
from opentelemetry import trace

from doozerlib.konflux.rebaser import KonfluxRebaser
from doozerlib.runtime import Runtime
from doozerlib.cli import pass_runtime, cli, click_coroutine, validate_semver_major_minor_patch, option_commit_message, option_push
from doozerlib.telemetry import start_as_current_span_async


TRACER = trace.get_tracer(__name__)


class KonfluxRebaseCli:
    def __init__(self, runtime: Runtime, version: Optional[str], release: Optional[str], embargoed: bool, repo_type: str, force_yum_updates: bool, message: str, push: bool):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.embargoed = embargoed
        self.repo_type = repo_type
        self.force_yum_updates = force_yum_updates
        self.message = message
        self.push = push
        pass

    @start_as_current_span_async(TRACER, "beta:images:konflux:rebase")
    async def run(self):
        runtime = self.runtime
        runtime.initialize(mode='images', clone_distgits=False)
        assert runtime.source_resolver is not None, "source_resolver is required for this command"
        metas = runtime.ordered_image_metas()
        rebaser = KonfluxRebaser(runtime=runtime, source_resolver=runtime.source_resolver)
        for image_meta in metas:
            dest_branch = "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format_map({
                "group": runtime.group,
                "assembly_name": runtime.assembly,
                "distgit_key": image_meta.distgit_key
            })
            await rebaser.rebase_to(image_meta, dest_branch, self.version, self.release, force_yum_updates=False)
        print("test konflux: Done")


@cli.command("beta:images:konflux:rebase", short_help="Refresh a group's konflux source content from source content.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_semver_major_minor_patch,
              help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM")
@click.option("--release", metavar='RELEASE', default=None, help="Release string to populate in Dockerfiles.")
@click.option("--embargoed", is_flag=True, help="Add .p1 to the release string for all images, which indicates those images have embargoed fixes")
@click.option("--repo-type", metavar="REPO_TYPE", envvar="OIT_IMAGES_REPO_TYPE",
              default="unsigned",
              help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).")
@click.option("--force-yum-updates", is_flag=True, default=False,
              help="Inject \"yum update -y\" in the final stage of an image build. This ensures the component image will be able to override RPMs it is inheriting from its parent image using RPMs in the rebuild plashet.")
@option_commit_message
@option_push
@pass_runtime
@click_coroutine
async def images_konflux_rebase(runtime: Runtime, version: Optional[str], release: Optional[str], embargoed: bool, repo_type: str, force_yum_updates: bool, message: str, push: bool):
    """
    Refresh a group's konflux content from source content.
    """
    cli = KonfluxRebaseCli(
        runtime=runtime,
        version=version,
        release=release,
        embargoed=embargoed,
        repo_type=repo_type,
        force_yum_updates=force_yum_updates,
        message=message,
        push=push,
    )
    await cli.run()
