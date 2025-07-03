import asyncio
import os
import sys

import click
from artcommonlib import logutil
from artcommonlib.konflux.konflux_build_record import (
    Engine,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
    KonfluxRecord,
)
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import get_utc_now_formatted_str, new_roundtrip_yaml_handler
from doozerlib.backend.konflux_client import API_VERSION, KIND_APPLICATION, KIND_COMPONENT, KIND_SNAPSHOT, KonfluxClient
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.util import oc_image_info_for_arch_async
from kubernetes.dynamic import exceptions
from kubernetes.dynamic.resource import ResourceInstance

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime

yaml = new_roundtrip_yaml_handler()

LOGGER = logutil.get_logger(__name__)


async def get_build_records_by_nvrs(
    runtime: Runtime, nvrs: list[str], for_fbc: bool = False, strict: bool = True
) -> dict[str, KonfluxRecord]:
    assert runtime.konflux_db is not None, "Konflux DB must be initialized in the runtime"

    where = {"group": runtime.group, "engine": Engine.KONFLUX.value}

    async def _get(nvrs) -> list[KonfluxRecord]:
        try:
            records = await runtime.konflux_db.get_build_records_by_nvrs(nvrs, where=where, strict=strict)
        except IOError as e:
            LOGGER.warning(
                "A snapshot is expected to exclusively contain ART built image builds "
                "OR FBC builds. To indicate an fbc snapshot use --for-fbc"
            )
            raise e
        return records

    nvr_record_map = {}
    records = []
    bundle_nvrs = [n for n in nvrs if "-bundle-container" in n]
    non_bundle_nvrs = [n for n in nvrs if n not in bundle_nvrs]

    if bundle_nvrs:
        runtime.konflux_db.bind(KonfluxBundleBuildRecord)
        bundle_records = await _get(bundle_nvrs)
        records.extend(bundle_records)
    if non_bundle_nvrs:
        if for_fbc:
            runtime.konflux_db.bind(KonfluxFbcBuildRecord)
        else:
            runtime.konflux_db.bind(KonfluxBuildRecord)
        non_bundle_records = await _get(non_bundle_nvrs)
        records.extend(non_bundle_records)

    for record in records:
        nvr_record_map[record.nvr] = record
    return nvr_record_map


class CreateSnapshotCli:
    def __init__(
        self,
        runtime: Runtime,
        konflux_config: dict,
        image_repo_pull_secret: str,
        for_fbc: bool,
        builds: list,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.konflux_config = konflux_config
        self.for_fbc = for_fbc
        if not builds:
            raise ValueError("builds must be provided")
        self.builds = builds
        self.dry_run = dry_run
        self.image_repo_pull_secret = image_repo_pull_secret
        self.konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_config['namespace'],
            config_file=self.konflux_config['kubeconfig'],
            context=self.konflux_config['context'],
            dry_run=self.dry_run,
        )
        self.konflux_client.verify_connection()

    async def run(self):
        self.runtime.initialize(build_system='konflux')
        if self.runtime.konflux_db is None:
            raise RuntimeError('Must run Elliott with Konflux DB initialized')

        # Ensure the Snapshot CRD is accessible
        try:
            await self.konflux_client._get_api(API_VERSION, KIND_SNAPSHOT)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError(
                f"Cannot access {API_VERSION} {KIND_SNAPSHOT} in the cluster. Passed the right kubeconfig?"
            )

        build_records: list[KonfluxRecord] = await self.fetch_build_records()

        # make sure pullspec is live for each build
        await self.get_pullspecs([b.image_pullspec for b in build_records], self.image_repo_pull_secret)

        snapshot_obj = await self.new_snapshot(build_records)
        snapshot_obj = await self.konflux_client._create(snapshot_obj)

        snapshot_url = self.konflux_client.resource_url(snapshot_obj)
        if self.dry_run:
            LOGGER.info("[DRY-RUN] Would have created Konflux Snapshot at %s", snapshot_url)
        else:
            LOGGER.info("Created Konflux Snapshot %s", snapshot_url)

        return snapshot_obj

    @staticmethod
    async def get_pullspecs(pullspecs: list, image_repo_pull_secret: str):
        image_info_tasks = []
        for pullspec in pullspecs:
            image_info_tasks.append(
                asyncio.create_task(
                    oc_image_info_for_arch_async(
                        pullspec,
                        registry_config=image_repo_pull_secret,
                    )
                )
            )
        image_infos = await asyncio.gather(*image_info_tasks, return_exceptions=True)
        errors = [
            (pullspec, result) for pullspec, result in zip(pullspecs, image_infos) if isinstance(result, BaseException)
        ]
        if errors:
            for pullspec, ex in errors:
                LOGGER.error("Failed to inspect pullspec %s: %s", pullspec, ex)
            raise RuntimeError("Failed to inspect build pullspecs")
        return image_infos

    async def new_snapshot(self, build_records) -> dict:
        major, minor = self.runtime.get_major_minor()
        snapshot_name = f"ose-{major}-{minor}-{get_utc_now_formatted_str()}"
        application_name = KonfluxImageBuilder.get_application_name(self.runtime.group)

        # make sure application exists
        await self.konflux_client._get(API_VERSION, KIND_APPLICATION, application_name)

        async def _comp(record):
            comp_name = KonfluxImageBuilder.get_component_name(application_name, record.name)

            # make sure component exists
            await self.konflux_client._get(API_VERSION, KIND_COMPONENT, comp_name)

            source_url = record.rebase_repo_url
            revision = record.rebase_commitish
            digest = record.image_tag

            if not (source_url or revision or digest):
                raise ValueError(f"Could not find all required nvr details {source_url=} {revision=} {digest=}")

            return {
                "name": comp_name,
                "source": {
                    "git": {
                        "url": source_url,
                        "revision": revision,
                    },
                },
                "containerImage": record.image_pullspec,
            }

        components = [await _comp(record) for record in build_records]

        snapshot_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_SNAPSHOT,
            "metadata": {
                "name": snapshot_name,
                "namespace": self.konflux_config['namespace'],
                "labels": {"test.appstudio.openshift.io/type": "override"},
            },
            "spec": {
                "application": application_name,
                "components": components,
            },
        }
        return snapshot_obj

    async def fetch_build_records(self) -> list[KonfluxRecord]:
        LOGGER.info("Validating given NVRs...")
        components = set()
        for build in self.builds:
            nvr = parse_nvr(build)
            if nvr['name'] in components:
                raise ValueError(
                    f"Multiple builds found for component {nvr['name']}. Please provide only one build per component."
                )
            components.add(nvr['name'])

        LOGGER.info("Fetching NVRs from DB...")
        records = await get_build_records_by_nvrs(self.runtime, self.builds, for_fbc=self.for_fbc, strict=True)
        return list(records.values())


@cli.group("snapshot", short_help="Commands for managing Konflux Snapshots")
def snapshot_cli():
    pass


@snapshot_cli.command(
    "new", short_help="Create a new Konflux Snapshot in the given namespace for the given builds (NVRs)"
)
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option(
    '--pull-secret',
    metavar='PATH',
    help='Path to the pull secret file to use. For example, if the images are in quay.io/org/repo then provide the pull secret to read from that repo.',
)
@click.option('--for-fbc', is_flag=True, help='To indicate that the given builds are fbc builds.')
@click.argument('builds', metavar='<NVR>', nargs=-1, required=False, default=None)
@click.option(
    "--builds-file",
    "-f",
    "builds_file",
    help="File to read builds from, `-` to read from STDIN.",
    type=click.File("rt"),
)
@click.option('--apply', is_flag=True, default=False, help='Create the snapshot in cluster (False by default)')
@click.pass_obj
@click_coroutine
async def new_snapshot_cli(
    runtime: Runtime,
    konflux_kubeconfig,
    konflux_context,
    konflux_namespace,
    pull_secret,
    builds_file,
    for_fbc,
    builds,
    apply,
):
    """
    Create a new Konflux Snapshot in the given namespace for the given builds

    \b
    $ elliott -g openshift-4.18 snapshot new --builds-file builds.txt

    \b
    $ elliott -g openshift-4.18 snapshot new nvr1 nvr2 nvr3 --apply
    """
    if bool(builds) and bool(builds_file):
        raise click.BadParameter("Use only one of --build or --builds-file")

    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not konflux_kubeconfig:
        raise ValueError("Must pass kubeconfig using --konflux-kubeconfig or KONFLUX_SA_KUBECONFIG env var")

    if builds_file:
        if builds_file == "-":
            builds_file = sys.stdin
        builds = [line.strip() for line in builds_file.readlines()]

    konflux_config = {
        'kubeconfig': konflux_kubeconfig,
        'namespace': konflux_namespace,
        'context': konflux_context,
    }

    pipeline = CreateSnapshotCli(
        runtime=runtime,
        konflux_config=konflux_config,
        image_repo_pull_secret=pull_secret,
        for_fbc=for_fbc,
        builds=builds,
        dry_run=not apply,
    )
    snapshot = await pipeline.run()
    yaml.dump(snapshot.to_dict(), sys.stdout)


class GetSnapshotCli:
    def __init__(
        self,
        runtime: Runtime,
        konflux_config: dict,
        image_repo_pull_secret: dict,
        for_fbc: bool,
        dry_run: bool,
        snapshot: str,
    ):
        self.runtime = runtime
        self.konflux_config = konflux_config
        self.for_fbc = for_fbc
        self.dry_run = dry_run
        self.snapshot = snapshot
        self.image_repo_pull_secret = image_repo_pull_secret
        self.konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_config['namespace'],
            config_file=self.konflux_config['kubeconfig'],
            context=self.konflux_config['context'],
            dry_run=self.dry_run,
        )
        self.konflux_client.verify_connection()

    async def run(self):
        self.runtime.initialize()
        if self.runtime.konflux_db is None:
            raise RuntimeError('Konflux DB is not initialized')

        # Ensure the Snapshot CRD is accessible
        try:
            await self.konflux_client._get_api(API_VERSION, KIND_SNAPSHOT)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError(
                f"Cannot access {API_VERSION} {KIND_SNAPSHOT} in the cluster. Passed the right kubeconfig?"
            )

        snapshot_obj = await self.konflux_client._get(API_VERSION, KIND_SNAPSHOT, self.snapshot)
        nvrs = await self.extract_nvrs_from_snapshot(snapshot_obj)

        # validate that the nvrs exist in the DB tables
        # not existing would indicate inconsistency between nvr construction & DB nvr field
        # or something more atypical like nvr/image not belonging to ART
        LOGGER.info(f"Validating {len(nvrs)} NVRs from DB...")

        if self.dry_run:
            LOGGER.info("[DRY-RUN] Skipped DB validation")
            return nvrs

        await get_build_records_by_nvrs(self.runtime, nvrs, for_fbc=self.for_fbc, strict=True)
        return nvrs

    async def extract_nvrs_from_snapshot(self, snapshot_obj: ResourceInstance) -> list[str]:
        if self.dry_run:
            major, minor = self.runtime.get_major_minor()
            LOGGER.info("[DRY-RUN] Returning mock NVR")
            return [f"test-component-v{major}.{minor}.0-202503121435.p0.gc0eec47.assembly.{self.runtime.assembly}.el9"]

        nvrs = []
        pullspecs = [c.containerImage for c in snapshot_obj.spec.components]
        image_infos = await CreateSnapshotCli.get_pullspecs(pullspecs, self.image_repo_pull_secret)

        # FBC images are special and have a different label to capture NVR
        if self.for_fbc:
            expected_labels = ["com.redhat.art.nvr"]
        else:
            expected_labels = ["com.redhat.component", "version", "release"]

        for image_info in image_infos:
            labels = image_info['config']['config']['Labels']
            if self.for_fbc:
                nvr = labels.get('com.redhat.art.nvr')
            else:
                name = labels.get('com.redhat.component')
                version = labels.get('version')
                release = labels.get('release')
                nvr = f"{name}-{version}-{release}"

            if nvr:
                nvrs.append(nvr)
            else:
                raise RuntimeError(f"Could not find expected labels in image: {expected_labels}")
        return nvrs


@snapshot_cli.command("get", short_help="Get NVRs from a Konflux Snapshot")
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option(
    '--pull-secret',
    metavar='PATH',
    help='Path to the pull secret file to use. For example, if the snapshot contains images from quay.io/org/repo then provide the pull secret to read from that repo.',
)
@click.option('--for-fbc', is_flag=True, help='To indicate that the given builds are fbc builds.')
@click.option('--dry-run', is_flag=True, help='Do not fetch, just print what would happen')
@click.argument('snapshot', metavar='SNAPSHOT', nargs=1)
@click.pass_obj
@click_coroutine
async def get_snapshot_cli(
    runtime: Runtime,
    konflux_kubeconfig,
    konflux_context,
    konflux_namespace,
    pull_secret,
    for_fbc,
    dry_run,
    snapshot,
):
    """
    Get NVRs from an existing Konflux Snapshot

    \b
    $ elliott -g openshift-4.18 snapshot get ose-4-18-202503121723
    """
    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')

    if not konflux_kubeconfig:
        raise ValueError("Must pass kubeconfig using --konflux-kubeconfig or KONFLUX_SA_KUBECONFIG env var")

    konflux_config = {
        'kubeconfig': konflux_kubeconfig,
        'namespace': konflux_namespace,
        'context': konflux_context,
    }

    pipeline = GetSnapshotCli(
        runtime=runtime,
        konflux_config=konflux_config,
        image_repo_pull_secret=pull_secret,
        for_fbc=for_fbc,
        dry_run=dry_run,
        snapshot=snapshot,
    )
    nvrs = await pipeline.run()
    click.echo('\n'.join(sorted(nvrs)))
