import click
import os
from datetime import datetime, timezone

from kubernetes.dynamic import exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_client import KonfluxClient, API_VERSION, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KIND_RELEASE
from artcommonlib import logutil
from artcommonlib.constants import KONFLUX_RELEASE_DATA_URL


LOGGER = logutil.get_logger(__name__)

ENV_KEYS = ["stage", "prod"]

# Konflux keys required to create a release
REQUIRED_KONFLUX_KEYS = ["releasePlan", "snapshot", "releaseNotes"]

# Konflux keys that can be globally set
# releasePlan is specific to env so do not allow it to be globally set
ALLOWED_TOP_LEVEL_KONFLUX_KEYS = ["snapshot", "releaseNotes"]

# Keys that contain metadata about an existing Konflux release/advisory
EXISTING_RELEASE_KEYS = ["releaseName", "advisoryName", "advisoryInternalUrl"]

ALLOWED_ENV_KEYS = REQUIRED_KONFLUX_KEYS + EXISTING_RELEASE_KEYS
ALLOWED_TOP_LEVEL_KEYS = ALLOWED_TOP_LEVEL_KONFLUX_KEYS + ENV_KEYS


class CreateReleaseCli:
    def __init__(self, runtime: Runtime, filename: str, release_env: str, konflux_config: dict, dry_run: bool, force: bool):
        self.runtime = runtime
        self.filename = filename
        self.konflux_config = konflux_config
        self.dry_run = dry_run
        self.force = force
        self.konflux_client = KonfluxClient.from_kubeconfig(default_namespace=self.konflux_config['namespace'],
                                                            config_file=self.konflux_config['kubeconfig'],
                                                            context=self.konflux_config['context'],
                                                            dry_run=self.dry_run)
        self.konflux_client.verify_connection()
        self.release_env = release_env
        self.release_config = None

    async def run(self):
        if not self.runtime.konflux_release_path:
            self.runtime.konflux_release_path = KONFLUX_RELEASE_DATA_URL
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

        self.release_config = self.get_release_config(release_config_raw, self.release_env)

        if not self.force:
            for key in EXISTING_RELEASE_KEYS:
                val = self.release_config.get(key)
                if val and val != 'N/A':
                    raise ValueError(f"existing release key is not empty: {key}={val}. "
                                     "use --force if you still want to proceed")

        release_obj = await self.new_release(self.release_config)
        created = await self.konflux_client._create(release_obj)
        print(created)

    @staticmethod
    def get_release_config(global_config_raw: dict, env: str):
        """
        Construct a konflux release config based on env and konflux keys
        """
        global_config = {k: v for k, v in global_config_raw.items() if k in ALLOWED_TOP_LEVEL_KEYS}
        if env not in ENV_KEYS:
            raise ValueError(f"env can only be one of {ENV_KEYS}. found `{env}`")

        env_config_raw = global_config.get(env, {})
        if not isinstance(env_config_raw, dict):
            raise ValueError(f"env value should be a dict. found {type(env_config_raw)}")
        env_config = {k: v for k, v in env_config_raw.items() if k in ALLOWED_ENV_KEYS}
        global_config.update(env_config)
        return global_config

    async def new_release(self, release_config: dict) -> dict:
        missing = [k for k in REQUIRED_KONFLUX_KEYS if k not in release_config.keys()]
        if missing:
            raise ValueError(f"Required konflux keys for release not found: {missing}")

        major, minor = self.runtime.get_major_minor()
        timestamp = datetime.strftime(datetime.now(tz=timezone.utc), "%Y%m%d%H%M")

        # make sure no '.' in env name
        env_str = self.release_env.replace(".", "-")
        release_name = f"ose-{major}-{minor}-{env_str}-{timestamp}"

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
@click.option('--filename', metavar='FILENAME', help='Release config filename to use. Defaults to assembly name')
@click.option('--env', metavar='RELEASE_ENV', required=True,
              help='The release environment to operate on in the config file e.g. stage, prod')
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--apply', is_flag=True, help='Create the release in cluster (False by default)')
@click.option('--force', is_flag=True, help='Proceed even if an associated release/advisory detected')
@click.pass_obj
@click_coroutine
async def new_release_cli(runtime: Runtime, filename, env, konflux_kubeconfig, konflux_context, konflux_namespace,
                          apply, force):
    """
    Create a new Konflux Release in the given namespace based on the config provided
    \b
    $ elliott -g openshift-4.18 --assembly 4.18.2 --konflux-release-path
    "https://gitlab.cee.redhat.com/sidsharm/ocp-konflux-release-data@add_4.19.0_test_release"
    release new --env stage
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

    pipeline = CreateReleaseCli(runtime,
                                filename=filename,
                                release_env=env,
                                konflux_config=konflux_config,
                                dry_run=not apply,
                                force=force)
    await pipeline.run()
