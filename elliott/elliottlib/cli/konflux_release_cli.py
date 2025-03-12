import click
import os
import yaml
from datetime import datetime, timezone

from kubernetes.dynamic import exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_client import KonfluxClient, API_VERSION, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KIND_RELEASE
from artcommonlib import logutil


LOGGER = logutil.get_logger(__name__)

CONTEXT_KEYS = ["stage", "prod", "fbc"]
KONFLUX_KEYS = ["releasePlan", "snapshot", "releaseNotes"]


class CreateReleaseCli:
    def __init__(self, runtime: Runtime, filename: str, context: str, konflux_config: dict, dry_run: bool):
        self.runtime = runtime
        self.filename = filename
        self.konflux_config = konflux_config
        self.dry_run = dry_run
        self.konflux_client = KonfluxClient.from_kubeconfig(default_namespace=self.konflux_config['namespace'],
                                                            config_file=self.konflux_config['kubeconfig'],
                                                            context=self.konflux_config['context'],
                                                            dry_run=self.dry_run)
        self.konflux_client.verify_connection()
        self.release_context = context
        self.release_config = None

    async def run(self):
        self.runtime.initialize()

        if not self.filename:
            self.filename = f"{self.runtime.assembly}.yml"
        path = f"{self.runtime.group}/releases/{self.filename}"
        release_config_raw = self.runtime.konflux_gitdata.load_yaml_file(path)

        if not release_config_raw:
            raise ValueError(f"Could not find/load release config at {self.runtime.konflux_gitdata.data_path}/"
                             f"{path}/{self.filename}")

        # Ensure CRDs are accessible
        try:
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE)
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE_PLAN)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError("Cannot access release resources in the cluster. Passed the right kubeconfig?")

        # Merge global and local context into release_config
        # all keys except these will be ignored for creating releases
        allowed_keys = CONTEXT_KEYS + KONFLUX_KEYS
        def get_release_config(raw_config, context):
            config = {k: v for k, v in raw_config.items() if k in allowed_keys}
            if context:
                print(f"processing {context=} in {raw_config.keys()=}")
                keys = context.split('.')
                top_key = keys[0]
                if top_key not in CONTEXT_KEYS:
                    raise ValueError(f"context can only be one of {CONTEXT_KEYS}. found `{top_key}`")
                next_context = '.'.join(keys[1:])
                local_config_raw = raw_config.get(top_key, {})
                if not isinstance(local_config_raw, dict):
                    raise ValueError(f"context value should be a dict. found {type(local_config_raw)}")
                local_config = {k: v for k, v in local_config_raw.items() if k in allowed_keys}
                config.update(local_config)
                return get_release_config(config, next_context)
            return config

        self.release_config = get_release_config(release_config_raw, self.release_context)
        release_obj = await self.new_release(self.release_config)
        created = await self.konflux_client._create(release_obj)
        print(created)

    async def new_release(self, release_config: dict) -> dict:
        missing = [k for k in KONFLUX_KEYS if k not in release_config.keys()]
        if missing:
            raise ValueError(f"Required konflux keys for release not found: {missing}")

        major, minor = self.runtime.get_major_minor()
        timestamp = datetime.strftime(datetime.now(tz=timezone.utc), "%Y%m%d%H%M")
        release_name = f"ose-{major}-{minor}-{timestamp}"

        release_plan = release_config['releasePlan']
        snapshot = release_config['snapshot']

        # make sure releasePlan exists
        try:
            await self.konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, release_plan)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {release_plan} in the cluster. Does it exist?")

        # make sure snapshot exists
        try:
            await self.konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {snapshot} in the cluster. Does it exist?")

        release_obj = {
            "apiVersion": API_VERSION,
            "kind": "Release",
            "metadata": {
                "name": release_name,
                "namespace": self.konflux_config['namespace'],
            },
            "spec": {
                "releasePlan": release_plan,
                "snapshot": snapshot,
                "data": {
                    "releaseNotes": release_config['releaseNotes'],
                }
            }
        }
        return release_obj


@cli.group("release", short_help="Commands for managing Konflux Releases")
def konflux_release_cli():
    pass


@konflux_release_cli.command("new", short_help="Create a new Konflux Release in the given namespace for the given "
                                               "konflux-release-data configuration")
@click.option('--filename', metavar='FILENAME', help='Release config filename to use. Defaults to passed assembly')
@click.option('--context', metavar='RELEASE_CONTEXT',
              help='The release context to use from the config file e.g. stage, prod, fbc.stage, etc.')
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--apply', is_flag=True, help='Create the release in cluster (False by default)')
@click.pass_obj
@click_coroutine
async def new_release_cli(runtime: Runtime, filename, context, konflux_kubeconfig, konflux_context, konflux_namespace,
                          apply):
    """
    Create a new Konflux Release in the given namespace based on the config provided
    \b
    $ elliott -g openshift-4.18 --assembly 4.18.2 --konflux-release-path
    "https://gitlab.cee.redhat.com/sidsharm/ocp-konflux-release-data@add_4.19.0_test_release"
    release new
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

    pipeline = CreateReleaseCli(runtime, filename, context, konflux_config, dry_run=not apply)
    await pipeline.run()
