import click
import sys
from ruamel.yaml import YAML

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from artcommonlib import logutil
from elliottlib.shipment_model import (
    Shipment,
    Metadata,
    Environments,
    ShipmentEnv,
    Snapshot,
    Spec,
    ShipmentConfig,
    Data,
    ReleaseNotes,
)
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder

LOGGER = logutil.get_logger(__name__)

yaml = YAML()
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)


@cli.group("shipment", short_help="Commands for managing release Shipment config")
def shipment_cli():
    pass


class InitShipmentCli:
    def __init__(
        self,
        runtime: Runtime,
        application: str,
        snapshot: str,
        stage_rpa: str,
        prod_rpa: str,
        for_fbc: bool,
        advisory_key: str,
    ):
        self.runtime = runtime
        self.application = application
        self.snapshot = snapshot
        self.stage_rpa = stage_rpa
        self.prod_rpa = prod_rpa
        self.for_fbc = for_fbc
        self.advisory_key = advisory_key

    async def run(self):
        self.runtime.initialize()

        data = None
        if not self.for_fbc:
            data = Data(
                releaseNotes=ReleaseNotes(
                    type="RHBA",
                    synopsis="Red Hat Openshift Test Release",
                    topic="Topic for a test release for Red Hat Openshift.",
                    description="Description for a test release for Red Hat Openshift.",
                    solution="Solution for a test release for Red Hat Openshift.",
                ),
            )

            if self.advisory_key:
                et_data = self.runtime.get_errata_config()
                if "boilerplates" not in et_data:
                    raise ValueError("`boilerplates` is required in erratatool.yml")
                if self.advisory_key not in et_data["boilerplates"]:
                    raise ValueError(f"Boilerplate {self.advisory_key} not found in erratatool.yml")
                boilerplate = et_data["boilerplates"][self.advisory_key]

                data = Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis=boilerplate["synopsis"],
                        topic=boilerplate["topic"],
                        description=boilerplate["description"],
                        solution=boilerplate["solution"],
                    ),
                )

        default_application = KonfluxImageBuilder.get_application_name(self.runtime.group)
        shipment = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product=self.runtime.product,
                    application=self.application or default_application,
                    group=self.runtime.group,
                    assembly=self.runtime.assembly,
                    fbc=self.for_fbc,
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan=self.stage_rpa or "test-stage-rpa"),
                    prod=ShipmentEnv(releasePlan=self.prod_rpa or "test-prod-rpa"),
                ),
                snapshot=Snapshot(name=self.snapshot or "test-snapshot", spec=Spec(nvrs=[])),
                data=data,
            ),
        )

        return shipment.model_dump(exclude_unset=True, exclude_none=True)


@shipment_cli.command("init", short_help="Init a new shipment config for a Konflux release")
@click.option("--application", help="Konflux application to use for shipment")
@click.option("--snapshot", help="Konflux snapshot to use for shipment")
@click.option("--stage-rpa", help="Konflux RPA to use for stage env")
@click.option("--prod-rpa", help="Konflux RPA to use for prod env")
@click.option("--for-fbc", is_flag=True, help="Configure shipment for an FBC release")
@click.option(
    "--advisory-key",
    help="Boilerplate for the advisory releaseNotes. This will be looked up from group's erratatool.yml",
)
@click.pass_obj
@click_coroutine
async def init_shipment_cli(
    runtime: Runtime, application: str, snapshot: str, stage_rpa: str, prod_rpa: str, for_fbc: bool, advisory_key: str
):
    """
    Init a new shipment config based on the given group and assembly, for a Konflux release.
    Shipment config will include advisory information if applicable.
    \b

    Init shipment config for a 4.18 microshift advisory

    $ elliott -g openshift-4.18 --assembly 4.18.2 snapshot init --advisory-key microshift
    """
    if advisory_key and for_fbc:
        raise ValueError(
            "Cannot use --for-fbc and --advisory-key together. An fbc shipment is not expected to have an advisory"
        )

    pipeline = InitShipmentCli(
        runtime=runtime,
        application=application,
        snapshot=snapshot,
        stage_rpa=stage_rpa,
        prod_rpa=prod_rpa,
        for_fbc=for_fbc,
        advisory_key=advisory_key,
    )

    shipment_yaml = await pipeline.run()
    yaml.dump(shipment_yaml, sys.stdout)
