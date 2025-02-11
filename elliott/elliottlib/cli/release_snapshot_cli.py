import click
import sys
import os
import asyncio
from datetime import datetime, timezone

from kubernetes.dynamic import exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from doozerlib.util import oc_image_info_for_arch_async
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from doozerlib.backend.konflux_client import KonfluxClient, API_VERSION, KIND_APPLICATION, KIND_COMPONENT, KIND_SNAPSHOT
from artcommonlib import logutil
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.konflux.konflux_build_record import (KonfluxRecord,
                                                       KonfluxBuildRecord,
                                                       KonfluxBundleBuildRecord,
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
        self._konflux_client = KonfluxClient.from_kubeconfig(default_namespace=self.konflux_namespace,
                                                             config_file=self.konflux_kubeconfig,
                                                             context=self.konflux_context,
                                                             dry_run=self.dry_run)
        self._konflux_client.verify_connection()

        # These will be needed for image inspection
        for secret in ["KONFLUX_ART_IMAGES_USERNAME", "KONFLUX_ART_IMAGES_PASSWORD"]:
            if secret not in os.environ:
                raise EnvironmentError(f"Missing required environment variable {secret}")

    async def run(self):
        self.runtime.initialize()
        if self.runtime.konflux_db is None:
            raise RuntimeError('Must run Elliott with Konflux DB initialized')
        if self.for_bundle:
            self.runtime.konflux_db.bind(KonfluxBundleBuildRecord)
        else:
            self.runtime.konflux_db.bind(KonfluxBuildRecord)

        # Ensure the Snapshot CRD is accessible
        try:
            await self._konflux_client._get_api(API_VERSION, KIND_SNAPSHOT)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError(f"Cannot access {API_VERSION} {KIND_SNAPSHOT} in the cluster. Passed the right kubeconfig?")

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
        image_infos = await asyncio.gather(*image_info_tasks, return_exceptions=True)
        errors = [(record, result) for record, result in zip(build_records, image_infos)
                  if isinstance(result, BaseException)]
        if errors:
            for record, ex in errors:
                record: KonfluxRecord
                LOGGER.error("Failed to inspect nvr %s pullspec %s: %s", record.nvr, record.image_pullspec, ex)
            raise RuntimeError("Failed to inspect build pullspecs")

        snapshot_obj = await self.new_snapshot(build_records)
        created = await self._konflux_client._create(snapshot_obj)
        print(created)

    @staticmethod
    def get_timestamp():
        return datetime.strftime(datetime.now(tz=timezone.utc), "%Y%m%d%H%M")

    async def new_snapshot(self, build_records) -> dict:
        major, minor = self.runtime.get_major_minor()
        snapshot_name = f"ose-{major}-{minor}-{self.get_timestamp()}"
        application_name = KonfluxImageBuilder.get_application_name(self.runtime.group)

        # make sure application exists
        await self._konflux_client._get(API_VERSION, KIND_APPLICATION, application_name)

        async def _comp(record):
            comp_name = KonfluxImageBuilder.get_component_name(application_name, record.name)

            # make sure component exists
            await self._konflux_client._get(API_VERSION, KIND_COMPONENT, comp_name)

            source_url = record.source_repo
            revision = record.commitish

            # pullspec is in the form of registry/image:tag
            # we need to provide snapshot with digest which is stored in image_tag
            image = record.image_pullspec.split(':')
            digest = record.image_tag
            pullspec = f"{image[0]}@sha256:{digest}"

            return {
                "name": comp_name,
                "source": {
                    "git": {
                        "url": source_url,
                        "revision": revision,
                    }
                },
                "containerImage": pullspec
            }

        components = [await _comp(record) for record in build_records]

        snapshot_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_SNAPSHOT,
            "metadata": {
                "name": snapshot_name,
                "namespace": self.konflux_namespace,
                "labels": {"test.appstudio.openshift.io/type": "override"},
            },
            "spec": {
                "application": application_name,
                "components": components,
            }
        }
        return snapshot_obj

    async def fetch_build_records(self) -> list[KonfluxRecord]:
        LOGGER.info("Validating given NVRs...")
        major, minor = self.runtime.get_major_minor()
        major_minor = f"{major}.{minor}"
        components = set()
        for build in self.builds:
            nvr = parse_nvr(build)
            if nvr['name'] in components:
                raise ValueError(f"Multiple builds found for component {nvr['name']}. Please provide only one build per component.")
            components.add(nvr['name'])

            if major_minor not in nvr['version']:
                raise ValueError(f"{build} does not look to belong to given group {self.runtime.group}")

        LOGGER.info("Fetching NVRs from DB...")
        where = {"group": self.runtime.group, "engine": Engine.KONFLUX.value}
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
@click.option('--apply', is_flag=False, help='Create the snapshot in cluster. By default just print')
@click.pass_obj
@click_coroutine
async def new_snapshot_cli(runtime: Runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                           builds_file, for_bundle, builds, apply):
    """
    Create a new Konflux Snapshot in the given namespace for the given builds

    \b
    $ elliott snapshot new --builds-file builds.txt
    """
    if bool(builds) and bool(builds_file):
        raise click.BadParameter("Use only one of --build or --builds-file")

    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not konflux_kubeconfig:
        raise ValueError(f"Must pass kubeconfig using --konflux-kubeconfig or KONFLUX_SA_KUBECONFIG env var")

    if builds_file:
        if builds_file == "-":
            builds_file = sys.stdin
        builds = [line.strip() for line in builds_file.readlines()]

    pipeline = CreateSnapshotCli(runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                                 for_bundle, builds, dry_run = not apply)
    await pipeline.run()
