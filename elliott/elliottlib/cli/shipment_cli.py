import sys

import click
from artcommonlib import logutil
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from ruamel.yaml import YAML

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.runtime import Runtime
from elliottlib.shipment_model import (
    Data,
    Environments,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
)
from elliottlib.util import get_advisory_boilerplate

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
        stage_rpa: str,
        prod_rpa: str,
        for_fbc: bool,
        advisory_key: str,
    ):
        self.runtime = runtime
        self.application = application or KonfluxImageBuilder.get_application_name(self.runtime.group)
        self.stage_rpa = stage_rpa
        self.prod_rpa = prod_rpa
        self.for_fbc = for_fbc
        self.advisory_key = advisory_key

    async def run(self):
        self.runtime.initialize(build_system='konflux', with_shipment=True)

        # if stage/prod rpa are not given in cli, try to load them from shipment repo config
        # where defaults are set per application
        if not (self.stage_rpa and self.prod_rpa):
            shipment_config = self.runtime.shipment_gitdata.load_yaml_file('config.yaml', strict=False) or {}
            app_env_config = shipment_config.get("applications", {}).get(self.application, {}).get("environments", {})
            self.stage_rpa = self.stage_rpa or app_env_config.get("stage", {}).get("releasePlan", "test-stage-rpa")
            self.prod_rpa = self.prod_rpa or app_env_config.get("prod", {}).get("releasePlan", "test-prod-rpa")

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
                _, minor, patch = self.runtime.get_major_minor_patch()
                advisory_boilerplate = get_advisory_boilerplate(
                    runtime=self.runtime, et_data=et_data, art_advisory_key=self.advisory_key, errata_type="RHBA"
                )
                synopsis = advisory_boilerplate['synopsis'].format(MINOR=minor, PATCH=patch)
                advisory_topic = advisory_boilerplate['topic'].format(MINOR=minor, PATCH=patch)
                advisory_description = advisory_boilerplate['description'].format(MINOR=minor, PATCH=patch)
                advisory_solution = advisory_boilerplate['solution'].format(MINOR=minor, PATCH=patch)

                data = Data(
                    releaseNotes=ReleaseNotes(
                        type="RHBA",
                        synopsis=synopsis,
                        topic=advisory_topic,
                        description=advisory_description,
                        solution=advisory_solution,
                    ),
                )

        shipment = ShipmentConfig(
            shipment=Shipment(
                metadata=Metadata(
                    product=self.runtime.product,
                    application=self.application,
                    group=self.runtime.group,
                    assembly=self.runtime.assembly,
                    fbc=self.for_fbc,
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan=self.stage_rpa),
                    prod=ShipmentEnv(releasePlan=self.prod_rpa),
                ),
                data=data,
            ),
        )

        return shipment.model_dump(exclude_unset=True, exclude_none=True)


@shipment_cli.command("init", short_help="Init a new shipment config for a Konflux release")
@click.option("--application", help="Konflux application to use for shipment")
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
    runtime: Runtime, application: str, stage_rpa: str, prod_rpa: str, for_fbc: bool, advisory_key: str
):
    """
    Init a new shipment config based on the given group and assembly, for a Konflux release.
    Shipment config will include advisory information if applicable.
    \b

    Init shipment config for a 4.18 microshift advisory

    $ elliott -g openshift-4.18 --assembly 4.18.2 shipment init --advisory-key microshift
    """
    if advisory_key and for_fbc:
        raise ValueError(
            "Cannot use --for-fbc and --advisory-key together. An fbc shipment is not expected to have an advisory"
        )

    pipeline = InitShipmentCli(
        runtime=runtime,
        application=application,
        stage_rpa=stage_rpa,
        prod_rpa=prod_rpa,
        for_fbc=for_fbc,
        advisory_key=advisory_key,
    )

    shipment_yaml = await pipeline.run()
    yaml.dump(shipment_yaml, sys.stdout)
