import datetime
import click
import os
import sys
import jsonschema
from datetime import datetime, timezone, timedelta

from ruamel.yaml import YAML, scalarstring
from kubernetes.dynamic import exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.cli.snapshot_cli import GetSnapshotCli
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_client import KonfluxClient, API_VERSION, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KIND_RELEASE
from artcommonlib import logutil
from artcommonlib.constants import KONFLUX_RELEASE_DATA_URL
from artcommonlib.model import Model

yaml = YAML()
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)

LOGGER = logutil.get_logger(__name__)

ENV_SCHEMA = {
    "type": "object",
    "required": ["releasePlan"],
    "properties": {
        "releasePlan": {"type": "string"},
        "releaseName": {"type": "string"},
        "advisoryName": {"type": "string"},
        "advisoryInternalUrl": {"type": "string"},
    },
}
SHIPMENT_SCHEMA = {
    "type": "object",
    "required": ["shipment"],
    "properties": {
        "shipment": {
            "type": "object",
            "required": ["metadata", "environments", "snapshot", "data"],
            "properties": {
                "metadata": {
                    "type": "object",
                    "required": [],
                    "properties": {
                        "product": {"type": "string"},
                        "application": {"type": "string"},
                        "group": {"type": "string"},
                        "assembly": {"type": "string"},
                    }
                },
                "environments": {
                    "type": "object",
                    "required": ["stage", "prod"],
                    "properties": {
                        "stage": ENV_SCHEMA,
                        "prod": ENV_SCHEMA,
                    },
                },
                "snapshot": {
                    "type": "object",
                    "required": ["name", "spec"],
                    "properties": {
                        "name": {"type": "string"},
                        "spec": {
                            "type": "object",
                            "required": ["nvrs"],
                            "properties": {
                                "nvrs": {"type": "array"}
                            },
                        },
                    },
                },
                "data": {
                    "type": "object",
                    "required": ["releaseNotes"],
                    "properties": {
                        "releaseNotes": {
                            "type": "object",
                            "required": ["type", "synopsis", "topic", "description", "solution"],
                            "properties": {
                                "type": {"type": "string"},
                                "synopsis": {"type": "string"},
                                "topic": {"type": "string"},
                                "description": {"type": "string"},
                                "solution": {"type": "string"},
                                "issues": {"type": "object"},
                                "cves": {"type": "array"},
                                "references": {"type": "array"},
                            }
                        },
                    },
                },
            }
        }
    }
}


class CreateReleaseCli:
    def __init__(self, runtime: Runtime, filename: str, release_env: str, konflux_config: dict,
                 image_repo_creds_config: dict, dry_run: bool, force: bool):
        self.runtime = runtime
        self.filename = filename
        self.konflux_config = konflux_config
        self.image_repo_creds_config = image_repo_creds_config
        self.dry_run = dry_run
        self.force = force
        self.konflux_client = KonfluxClient.from_kubeconfig(default_namespace=self.konflux_config['namespace'],
                                                            config_file=self.konflux_config['kubeconfig'],
                                                            context=self.konflux_config['context'],
                                                            dry_run=self.dry_run)
        self.konflux_client.verify_connection()
        self.release_env = release_env

    async def run(self):
        if not self.runtime.konflux_release_path:
            self.runtime.konflux_release_path = KONFLUX_RELEASE_DATA_URL
        self.runtime.initialize()

        if not self.filename:
            self.filename = f"{self.runtime.assembly}.yml"
        path = f"{self.runtime.group}/releases/{self.filename}"
        config_raw = self.runtime.konflux_gitdata.load_yaml_file(path)
        if not config_raw:
            raise ValueError(f"Could not find/load release config at {self.runtime.konflux_gitdata.data_path}/"
                             f"{path}/{self.filename}")

        LOGGER.info("Validating yaml config...")
        jsonschema.validate(instance=config_raw, schema=SHIPMENT_SCHEMA)

        config = Model(config_raw)

        # Ensure CRDs are accessible
        try:
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE)
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE_PLAN)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError("Cannot access release resources in the cluster. Passed the right kubeconfig?")

        release_config = self.get_release_config(config.shipment, self.release_env)
        LOGGER.info("Constructed release config: %s", release_config)

        if not self.force:
            env_config = config.shipment.environments.release_env
            for key in ["releaseName", "advisoryName", "advisoryInternalUrl"]:
                val = env_config[key]
                if val and val != 'N/A':
                    raise ValueError(f"existing release key is not empty: {key}={val}. "
                                     "use --force if you still want to proceed")

        # verify snapshot
        # TODO: make it work for bundle/fbc
        get_snapshot_cli = GetSnapshotCli(self.runtime, self.konflux_config, self.image_repo_creds_config,
                                          for_bundle=False, for_fbc=False, snapshot=release_config['snapshot'])
        actual_nvrs = set(await get_snapshot_cli.run())
        expected_nvrs = set(config.shipment.snapshot.spec.nvrs)
        if actual_nvrs != expected_nvrs:
            missing = expected_nvrs - actual_nvrs
            extra = actual_nvrs - expected_nvrs
            raise ValueError(f"snapshot includes missing or extra nvrs than what's defined in spec: {missing=} "
                             f"{extra=}")

        release_obj = await self.new_release(release_config)
        return await self.konflux_client._create(release_obj)

    @staticmethod
    def get_release_config(shipment_config: Model, env: str):
        """
        Construct a konflux release config based on env and raw config
        """
        release_config = {
            "snapshot": shipment_config.snapshot.name,
            "releasePlan": shipment_config.environments[env].releasePlan,
            "data": shipment_config.data,
        }
        return release_config

    async def new_release(self, release_config: dict) -> dict:
        major, minor = self.runtime.get_major_minor()
        timestamp = datetime.strftime(datetime.now(tz=timezone.utc), "%Y%m%d%H%M")
        release_name = f"ose-{major}-{minor}-{self.release_env}-{timestamp}"

        release_plan = release_config['releasePlan']
        snapshot = release_config['snapshot']
        data = release_config['data']

        # make sure releasePlan exists
        LOGGER.info(f"Fetching release plan {release_plan} ...")
        try:
            await self.konflux_client._get(API_VERSION, KIND_RELEASE_PLAN, release_plan)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {release_plan} in the cluster. Does it exist?")

        # make sure snapshot exists
        LOGGER.info(f"Fetching snapshot {snapshot} ...")
        try:
            await self.konflux_client._get(API_VERSION, KIND_SNAPSHOT, snapshot)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {snapshot} in the cluster. Does it exist?")

        # description and solution are special text fields
        # configure these to be LiteralScalarString so ruamel preserves `|`
        data['releaseNotes']['description'] = scalarstring.LiteralScalarString(data['releaseNotes']['description'])
        data['releaseNotes']['solution'] = scalarstring.LiteralScalarString(data['releaseNotes']['solution'])

        release_obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_RELEASE,
            "metadata": {
                "name": release_name,
                "namespace": self.konflux_config['namespace'],
            },
            "spec": {
                "releasePlan": release_plan,
                "snapshot": snapshot,
                "data": data,
            }
        }
        return release_obj


@cli.group("release", short_help="Commands for managing Konflux Releases")
def konflux_release_cli():
    pass


@konflux_release_cli.command("new", short_help="Create a new Konflux Release in the given namespace for the given "
                                               "konflux-release-data configuration")
@click.option('--filename', metavar='FILENAME', help='Release config filename to use. Defaults to assembly name')
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--apply', is_flag=True, default=False,
              help='Create the release in cluster (False by default)')
@click.option('--force', is_flag=True, default=False,
              help='Proceed even if an associated release/advisory detected')
@click.argument('env', metavar='RELEASE_ENV', nargs=1, required=True, type=click.Choice(["stage", "prod"]))
@click.pass_obj
@click_coroutine
async def new_release_cli(runtime: Runtime, filename, konflux_kubeconfig, konflux_context, konflux_namespace,
                          apply, force, env):
    """
    Create a new Konflux Release in the given namespace based on the config provided
    \b
    $ elliott -g openshift-4.18 --assembly 4.18.2 --konflux-release-path
    "https://gitlab.cee.redhat.com/sidsharm/ocp-konflux-release-data@add_4.19.0_test_release"
    release new stage
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

    image_repo_creds_config = {
        'username': os.environ.get('KONFLUX_ART_IMAGES_USERNAME'),
        'password': os.environ.get('KONFLUX_ART_IMAGES_PASSWORD'),
    }

    pipeline = CreateReleaseCli(runtime,
                                filename=filename,
                                release_env=env,
                                konflux_config=konflux_config,
                                image_repo_creds_config=image_repo_creds_config,
                                dry_run=not apply,
                                force=force)
    release = await pipeline.run()
    yaml.dump(release.to_dict(), sys.stdout)


class WatchReleaseCli:
    def __init__(self, runtime: Runtime, release: str, konflux_config: dict, timeout: int, dry_run: bool):
        self.runtime = runtime
        self.release = release
        self.konflux_config = konflux_config
        self.timeout = timeout
        self.dry_run = dry_run
        self.konflux_client = KonfluxClient.from_kubeconfig(default_namespace=self.konflux_config['namespace'],
                                                            config_file=self.konflux_config['kubeconfig'],
                                                            context=self.konflux_config['context'],
                                                            dry_run=self.dry_run)
        self.konflux_client.verify_connection()

    async def run(self):
        self.runtime.initialize(no_group=True)
        release_obj = await self.konflux_client.wait_for_release(self.release, overall_timeout_timedelta=timedelta(hours=self.timeout))

        # Assume that these will be available
        released_condition = next(c for c in release_obj['status']['conditions'] if c['type'] == "Released")
        reason = released_condition.get('reason')
        status = released_condition.get('status')
        success = reason == "Succeeded" and status == "True"

        if success:
            print("Release successful!")
            exit(0)
        else:
            message = released_condition.get('message')
            if message == "Release processing failed on managed pipelineRun":
                managed_plr = release_obj['status'].get('managedProcessing', {}).get('pipelineRun', '')
                message += f" {managed_plr}"
            print(f"Release failed! Konflux message: {message}")
            exit(1)


@konflux_release_cli.command("watch", short_help="Watch and report on status of a given Konflux Release")
@click.argument("release", metavar='RELEASE_NAME', nargs=1)
@click.option('--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--timeout', metavar='TIMEOUT_HOURS', type=click.INT, default=5,
              help='Time to wait, in hours. Set 0 to report and exit.')
@click.option('--dry-run', is_flag=True, help='Init and exit')
@click.pass_obj
@click_coroutine
async def watch_release_cli(runtime: Runtime, release: str, konflux_kubeconfig: str, konflux_context: str,
                            konflux_namespace, timeout: int, dry_run: bool):
    """
    Watch the given Konflux Release and report on its status
    \b

    $ elliott release watch ose-4-18-stage-202503131819 --konflux-namespace ocp-art-tenant
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

    pipeline = WatchReleaseCli(runtime, release=release, konflux_config=konflux_config, timeout=timeout,
                               dry_run=dry_run)
    await pipeline.run()
