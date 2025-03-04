import click
from datetime import datetime, timezone

from kubernetes.dynamic import exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_client import KonfluxClient, API_VERSION, KIND_APPLICATION, KIND_COMPONENT, KIND_SNAPSHOT
from artcommonlib import logutil


LOGGER = logutil.get_logger(__name__)


class CreateReleaseCli:
    def __init__(self, runtime: Runtime, context, konflux_kubeconfig, konflux_context, konflux_namespace,
                 dry_run: bool):
        self.runtime = runtime
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.dry_run = dry_run
        self._konflux_client = KonfluxClient.from_kubeconfig(default_namespace=self.konflux_namespace,
                                                             config_file=self.konflux_kubeconfig,
                                                             context=self.konflux_context,
                                                             dry_run=self.dry_run)

        self.release_context = context
        self.release_config = None

    async def run(self):
        self.runtime.initialize()

        # Ensure release CRDs are accessible
        try:
            await self._konflux_client._get_api(API_VERSION, "Release")
            await self._konflux_client._get_api(API_VERSION, "ReleasePlan")
        except exceptions.ResourceNotFoundError:
            raise RuntimeError("Cannot access release resources in the cluster. Passed the right kubeconfig?")

        def get_nested_key(a_dict, nested_key):
            # nested_key is like key1.key2
            if '.' in nested_key:
                keys = nested_key.split('.')
                top_key = keys[0]
                next_key = '.'.join(keys[1:])
                return get_nested_key(a_dict.get(top_key, {}), next_key)
            return a_dict.get(nested_key, {})



        # Merge global and local context into release_config
        def get_merged_dict(global_dict, local_dict):
            new_dict = {}
            for key, value in global_dict.items():
                if key in ['fbc', 'stage', 'prod']:
                    continue
                elif key in ['']
                    if key in local_dict:
                        new_dict[key] = local_dict[key]
                    else:
                        new_dict[key] = value
            return new_dict

        release_config = get_merged_dict({}, self.release_context)





        release_obj = await self.new_release(release_config)
        created = await self._konflux_client._create(release_obj)
        print(created)

    async def new_release(self, release_config) -> dict:
        if not all([k in release_config for k in ['releasePlan', 'snapshot', 'releaseNotes']]):
            raise RuntimeError("releasePlan, snapshot, and releaseNotes are required")

        major, minor = self.runtime.get_major_minor()
        timestamp = datetime.strftime(datetime.now(tz=timezone.utc), "%Y%m%d%H%M")
        release_name = f"ose-{major}-{minor}-{timestamp}"

        # make sure releasePlan exists
        await self._konflux_client._get(API_VERSION, 'ReleasePlan', release_config['releasePlan'])

        # make sure snapshot exists
        await self._konflux_client._get(API_VERSION, KIND_SNAPSHOT, release_config['snapshot'])

        release_obj = {
            "apiVersion": API_VERSION,
            "kind": "Release",
            "metadata": {
                "name": release_name,
                "namespace": self.konflux_namespace,
            },
            "spec": {
                "releasePlan": release_config['releasePlan'],
                "snapshot": release_config['snapshot'],
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
@click.option('--context', metavar='CONTEXT', help='The release context to use from the config file, e.g. stage, '
                                                   'prod, fbc.stage, etc.')
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--dry-run', is_flag=True, help='Just print what would be done.')
@click.pass_obj
@click_coroutine
async def new_release_cli(runtime: Runtime, context, konflux_kubeconfig, konflux_context, konflux_namespace, dry_run):
    """
    Create a new Konflux Release in the given namespace for the given builds
    \b
    $ elliott -g openshift-4.18 --konflux-release-path "https://gitlab.cee.redhat.com/sidsharm/ocp-konflux-release-data@add_4.19.0_test_release"
      release new --dry-run
    """
    pipeline = CreateReleaseCli(runtime, context, konflux_kubeconfig, konflux_context, konflux_namespace, dry_run)
    await pipeline.run()
