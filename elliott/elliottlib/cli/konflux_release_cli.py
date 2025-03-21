import click
import os
import sys

from ruamel.yaml import YAML, scalarstring
from kubernetes.dynamic import exceptions

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.cli.snapshot_cli import GetSnapshotCli
from doozerlib.constants import KONFLUX_DEFAULT_NAMESPACE
from doozerlib.backend.konflux_client import (KonfluxClient, API_VERSION, KIND_RELEASE_PLAN, KIND_SNAPSHOT, KIND_RELEASE,
                                              KIND_APPLICATION)
from artcommonlib import logutil
from artcommonlib.model import Model
from artcommonlib.util import get_utc_timestamp

from validator.schema import shipment_schema
from validator.exceptions import ValidationFailed


yaml = YAML()
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)

LOGGER = logutil.get_logger(__name__)


class CreateReleaseCli:
    def __init__(self, runtime: Runtime, config_path: str, release_env: str, konflux_config: dict,
                 image_repo_creds_config: dict, dry_run: bool, force: bool):
        self.runtime = runtime
        self.config_path = config_path
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
        self.runtime.initialize(with_shipment=True)

        LOGGER.info(f"Loading {self.config_path}...")
        config_raw = self.runtime.shipment_gitdata.load_yaml_file(self.config_path)
        if not config_raw:
            raise ValueError(f"Error loading shipment config from {self.runtime.shipment_gitdata.data_path}/{self.config_path}")

        LOGGER.info("Validating yaml config...")
        err = shipment_schema.validate(None, data=config_raw)
        if err:
            raise ValidationFailed(err)

        config = Model(config_raw)

        # Validate shipment metadata with group metadata and with what's passed via args
        meta = config.shipment.metadata
        if meta.product != self.runtime.product:
            raise ValueError(f"shipment.metadata.product={meta.product} is expected to be the "
                             f"same as runtime.product={self.runtime.product}")
        elif meta.group != self.runtime.group:
            raise ValueError(f"shipment.metadata.group={meta.group} is expected to be the "
                             f"same as runtime.group={self.runtime.group}")
        elif meta.assembly != self.runtime.assembly:
            raise ValueError(f"shipment.metadata.assembly={meta.assembly} is expected to be the "
                             f"same as runtime.assembly={self.runtime.assembly}")

        # Ensure CRDs are accessible
        try:
            await self.konflux_client._get_api(API_VERSION, KIND_APPLICATION)
            await self.konflux_client._get_api(API_VERSION, KIND_SNAPSHOT)
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE)
            await self.konflux_client._get_api(API_VERSION, KIND_RELEASE_PLAN)
        except exceptions.ResourceNotFoundError:
            raise RuntimeError("Cannot access release resources in the cluster. Passed the right kubeconfig?")

        # make sure application exists
        LOGGER.info(f"Fetching application {meta.application} ...")
        try:
            await self.konflux_client._get(API_VERSION, KIND_APPLICATION, meta.application)
        except exceptions.NotFoundError:
            raise RuntimeError(f"Cannot access {meta.application} in the cluster. Does it exist?")

        release_config = self.get_release_config(config.shipment, self.release_env)
        LOGGER.info(f"Constructed release config with snapshot={release_config['snapshot']}"
                    f" releasePlan={release_config['releasePlan']}")

        if not self.force:
            env_config = config.shipment.environments[self.release_env]
            for key in ["releaseName", "advisoryName", "advisoryInternalUrl"]:
                val = env_config[key]
                if val and val != 'N/A':
                    raise ValueError(f"existing release key is not empty: {key}={val}. "
                                     "use --force if you still want to proceed")

        # verify snapshot
        # TODO: make it work for bundle/fbc
        get_snapshot_cli = GetSnapshotCli(self.runtime, self.konflux_config, self.image_repo_creds_config,
                                          for_bundle=False, for_fbc=meta.fbc, snapshot=release_config['snapshot'],
                                          dry_run=False)
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
        release_name = f"ose-{major}-{minor}-{self.release_env}-{get_utc_timestamp()}"

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
                                               "shipment-data configuration")
@click.option('--konflux-kubeconfig', metavar='KUBECONF_PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.')
@click.option('--konflux-context', metavar='CONTEXT', help='The name of the kubeconfig context to use for Konflux cluster connections.')
@click.option('--konflux-namespace', metavar='NAMESPACE', default=KONFLUX_DEFAULT_NAMESPACE, help='The namespace to use for Konflux cluster connections.')
@click.option('--config', metavar='CONFIG_PATH', required=True,
              help='Path of the shipment config file to use for creating release. The path should be from the root of the  '
                   'given shipment-data repo.')
@click.option('--env', metavar='RELEASE_ENV', required=True, type=click.Choice(["stage", "prod"]),
              help='Release environment to create the release for')
@click.option('--apply', is_flag=True, default=False,
              help='Create the release in cluster (False by default)')
@click.option('--force', is_flag=True, default=False,
              help='Proceed even if an associated release/advisory detected')
@click.pass_obj
@click_coroutine
async def new_release_cli(runtime: Runtime, konflux_kubeconfig, konflux_context, konflux_namespace,
                          config, env, apply, force):
    """
    Create a new Konflux Release in the given namespace based on the config provided
    \b
    $ elliott -g openshift-4.18 --assembly 4.18.2 --shipment-path
    "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data@add_4.18.2_test_release"
    release new --env stage --config shipment/ocp/openshift-4.18/openshift-4-18/4.18.2.202503210000.yml
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
                                config_path=config,
                                release_env=env,
                                konflux_config=konflux_config,
                                image_repo_creds_config=image_repo_creds_config,
                                dry_run=not apply,
                                force=force)
    release = await pipeline.run()
    yaml.dump(release.to_dict(), sys.stdout)
