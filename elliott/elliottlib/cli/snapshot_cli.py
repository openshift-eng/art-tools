import asyncio
import os
import sys
from collections import defaultdict
from functools import lru_cache
from itertools import chain
from typing import List

import click
from artcommonlib import logutil
from artcommonlib.konflux.konflux_build_record import (
    Engine,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
    KonfluxRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import get_utc_now_formatted_str, new_roundtrip_yaml_handler
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT, KonfluxClient
from doozerlib.backend.konflux_fbc import KonfluxFbcBuilder
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from doozerlib.backend.konflux_olm_bundler import KonfluxOlmBundleBuilder
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.util import oc_image_info_for_arch_async
from kubernetes.dynamic import exceptions
from kubernetes.dynamic.resource import ResourceInstance

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime

yaml = new_roundtrip_yaml_handler()
yaml.explicit_start = True
yaml.default_flow_style = False

LOGGER = logutil.get_logger(__name__)


@lru_cache
def _get_konflux_db(record_cls: type[KonfluxRecord]):
    """
    Get the Konflux DB instance for the given record class.
    This is a helper function to ensure that the correct record class is bound to the Konflux DB.
    """
    db = KonfluxDb()
    db.bind(record_cls)
    return db


async def get_build_records_by_nvrs(runtime: Runtime, nvrs: list[str], strict: bool = True) -> dict[str, KonfluxRecord]:
    where = {"group": runtime.group, "engine": Engine.KONFLUX.value}

    async def _get(db: KonfluxDb, nvrs: list[str]) -> list[KonfluxRecord]:
        try:
            records = await db.get_build_records_by_nvrs(nvrs, where=where, strict=strict)
        except IOError as e:
            LOGGER.warning(
                "A snapshot is expected to exclusively contain ART built image builds "
                "OR FBC builds. To indicate an fbc snapshot use --for-fbc"
            )
            raise e
        return records

    # Group NVRs by their type to fetch them in parallel
    type_nvrs = defaultdict(list)
    for nvr in nvrs:
        nvr_dict = parse_nvr(nvr)
        if nvr_dict['name'].endswith('-bundle-container') or nvr_dict['name'].endswith('-metadata-container'):
            type_nvrs[KonfluxBundleBuildRecord].append(nvr)
        elif nvr_dict['name'].endswith('-fbc'):
            type_nvrs[KonfluxFbcBuildRecord].append(nvr)
        elif nvr_dict['name'].endswith('-container'):
            type_nvrs[KonfluxBuildRecord].append(nvr)
        else:
            raise ValueError(f"Unknown NVR type for {nvr}. Expected bundle-container, fbc, or container.")
    build_records = await asyncio.gather(
        *(_get(_get_konflux_db(record_cls), nvrs) for record_cls, nvrs in type_nvrs.items())
    )

    # Flatten the list of records
    build_records = chain.from_iterable(build_records)

    # Create a mapping of NVR to record
    nvr_record_map = {str(record.nvr): record for record in build_records}
    return nvr_record_map


class CreateSnapshotCli:
    def __init__(
        self,
        runtime: Runtime,
        konflux_config: dict,
        image_repo_pull_secret: str,
        builds: list,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.konflux_config = konflux_config
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

        snapshot_objs = await self.new_snapshots(build_records)
        # TODO: `_create` is a private method, should be replaced with a public method in the future
        snapshot_objs = await asyncio.gather(
            *(self.konflux_client._create(snapshot_obj) for snapshot_obj in snapshot_objs)
        )
        snapshot_urls = [self.konflux_client.resource_url(snapshot_obj) for snapshot_obj in snapshot_objs]
        if self.dry_run:
            LOGGER.info("[DRY-RUN] Would have created Konflux Snapshots: %s", ", ".join(snapshot_urls))
        else:
            LOGGER.info("Created Konflux Snapshot(s): %s", ", ".join(snapshot_urls))
        return snapshot_objs

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

    async def new_snapshots(self, build_records: List[KonfluxRecord]) -> list[dict]:
        major, minor = self.runtime.get_major_minor()
        snapshot_name = f"ose-{major}-{minor}-{get_utc_now_formatted_str()}"

        async def _comp(record: KonfluxRecord):
            # get application and component names from PLR url
            # note: this will change once we have component name stored in the DB
            app_name = record.get_konflux_application_name()
            comp_name = record.get_konflux_component_name()

            # make sure application exists
            await self.konflux_client.get_application__caching(app_name, strict=True)

            # make sure component exists, if not, try to get it from the relevant Builder class
            # note: this will change once we have component name stored in the DB
            try:
                await self.konflux_client.get_component__caching(comp_name, strict=True)
            except Exception as e:
                if isinstance(record, KonfluxBuildRecord):
                    comp_name = KonfluxImageBuilder.get_component_name(app_name, record.name)
                    await self.konflux_client.get_component__caching(comp_name, strict=True)
                elif isinstance(record, KonfluxBundleBuildRecord):
                    comp_name = KonfluxOlmBundleBuilder.get_component_name(app_name, record.name)
                    try:
                        await self.konflux_client.get_component__caching(comp_name, strict=True)
                    except Exception:
                        # if we still can't find the component, use the old component name
                        comp_name = KonfluxOlmBundleBuilder.get_old_component_name(app_name, record.name)
                        await self.konflux_client.get_component__caching(comp_name, strict=True)
                else:
                    # fbc component name is determined from the image it builds for, which is not stored in the DB
                    # rather than hack something up, let it fail for now
                    raise e

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
            }, app_name

        # Collect components and their applications
        app_components: dict[str, list] = defaultdict(list)
        for component, app in await asyncio.gather(*(_comp(record) for record in build_records)):
            app_components[app].append(component)

        # Sort components by application name and then by component name
        # This is to ensure that the snapshot objects are created in a consistent order
        snapshot_objs: list[dict] = []
        for index, (application_name, components) in enumerate(sorted(app_components.items()), start=1):
            components = sorted(components, key=lambda c: c['name'])
            snapshot_obj = {
                "apiVersion": API_VERSION,
                "kind": KIND_SNAPSHOT,
                "metadata": {
                    "name": f"{snapshot_name}-{index}",
                    "namespace": self.konflux_config['namespace'],
                    "labels": {
                        "test.appstudio.openshift.io/type": "override",
                        "appstudio.openshift.io/application": application_name,
                    },
                },
                "spec": {
                    "application": application_name,
                    "components": components,
                },
            }
            snapshot_objs.append(snapshot_obj)
        return snapshot_objs

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
        records = await get_build_records_by_nvrs(self.runtime, self.builds, strict=True)
        return list(records.values())


@cli.group("snapshot", short_help="Commands for managing Konflux Snapshots")
def snapshot_cli():
    pass


@snapshot_cli.command(
    "new", short_help="Create new Konflux Snapshot(s) in the given namespace for the given builds (NVRs)"
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
    builds,
    apply,
):
    """
    Create new Konflux Snapshot(s) in the given namespace for the given builds

    If builds are from multiple Konflux applications, multiple snapshots will be created, one for each application.

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
        builds=builds,
        dry_run=not apply,
    )
    snapshots = await pipeline.run()
    yaml.dump_all([snapshot.to_dict() for snapshot in snapshots], sys.stdout)


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

        await get_build_records_by_nvrs(self.runtime, nvrs, strict=True)
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
