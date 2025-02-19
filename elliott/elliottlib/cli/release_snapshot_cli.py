import click
import sys
import os
import asyncio

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from doozerlib.util import oc_image_info_for_arch_async
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from artcommonlib import logutil
from artcommonlib.konflux.konflux_build_record import (KonfluxRecord, KonfluxBuildRecord, KonfluxBundleBuildRecord,
                                                       Engine)

LOGGER = logutil.get_logger(__name__)


class CreateSnapshotCli:
    def __init__(self, runtime: Runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                 for_bundle: bool, builds: list, dry_run: bool):
        self.runtime = runtime
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.for_bundle = for_bundle
        self.builds = builds
        self.dry_run = dry_run

    async def run(self):
        self.runtime.initialize()
        if self.runtime.konflux_db is None:
            raise RuntimeError('Must run Elliott with Konflux DB initialized')
        if self.for_bundle:
            self.runtime.konflux_db.bind(KonfluxBundleBuildRecord)
        else:
            self.runtime.konflux_db.bind(KonfluxBuildRecord)
        build_records: list[KonfluxRecord] = await self.fetch_build_records()

        # make sure pullspec is live for each build
        image_info_tasks = []
        for record in build_records:
            image_info_tasks.append(asyncio.create_task(
                oc_image_info_for_arch_async(
                    record.image_pullspec,
                    registry_username=os.environ.get('KONFLUX_ART_IMAGES_USERNAME'),
                    registry_password=os.environ.get('KONFLUX_ART_IMAGES_PASSWORD'),
                )))
        image_infos = await asyncio.gather(*image_info_tasks)
        for image_info in image_infos:
            assert image_info, "Image info not found"

        # now lets create the snapshot crd



    def new_snapshot(self, name: str, build_records) -> dict:
        application_name = KonfluxImageBuilder.get_application_name(self.runtime.group)

        def _comp(record):
            comp_name = KonfluxImageBuilder.get_component_name(application_name, record.name)
            source_url = record.source_repo
            revision = record.commitish
            return {
                "name": comp_name,
                "source": {"url": source_url},
                "revision": revision,
            }

        components = [_comp(record) for record in build_records]

        snapshot_obj = {
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "Snapshot",
            "metadata": {
                "name": name
            },
            "spec": {
                "application": application_name,
                "components": components,
            }
        }
        return snapshot_obj

    async def fetch_build_records(self) -> list[KonfluxRecord]:
        LOGGER.info("Fetching given nvrs from Konflux DB...")
        # TODO: validate that given builds have at most 1 build per component
        where = {"group": self.runtime.group, "engine": Engine.KONFLUX}
        records = await self.runtime.konflux_db.get_build_records_by_nvrs(self.builds, where=where, strict=True)
        return records


@cli.group("snapshot", short_help="Commands for managing Konflux Snapshots")
def release_snapshot_cli():
    pass


@release_snapshot_cli.command("new", short_help="Create a new Konflux Snapshot in the given namespace for the given "
                                                "builds (NVRs)")
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--for-bundle', is_flag=True, help='To indicate that the given builds are bundle builds.')
@click.argument('builds', metavar='<NVR>', nargs=-1, required=False, default=None)
@click.option(
    "--builds-file", "-f", "builds_file",
    help="File to read builds from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.option('--dry-run', is_flag=True, help='Do not actually create the snapshot, just print what would be done.')
@click.pass_obj
@click_coroutine
async def new_snapshot(runtime: Runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                       builds_file, for_bundle, builds, dry_run):
    """
    Create a new Konflux Snapshot in the given namespace for the given builds

    \b
    $ elliott snapshot new --builds-file builds.txt
    """
    if bool(builds) and bool(builds_file):
        raise click.BadParameter("Use only one of --build or --builds-file")

    if builds_file:
        if builds_file == "-":
            builds_file = sys.stdin
        builds = [line.strip() for line in builds_file.readlines()]

    pipeline = CreateSnapshotCli(runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                                 for_bundle, builds, dry_run)
    await pipeline.run()
