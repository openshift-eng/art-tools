import sys

import click
from artcommonlib import logutil
from doozerlib.backend.konflux_fbc import KonfluxFbcBuilder
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
from elliottlib.util import get_advisory_boilerplate, get_advisory_docs_info

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
        kind: str,
    ):
        self.runtime = runtime
        self.kind = kind

    async def run(self):
        self.runtime.initialize(build_system='konflux', with_shipment=True)

        if self.kind == "fbc":
            application = KonfluxFbcBuilder.get_application_name(self.runtime.group)
        else:
            application = KonfluxImageBuilder.get_application_name(self.runtime.group)

        # load stage/prod rpa from shipment repo config
        # where defaults are set per application
        shipment_config = self.runtime.shipment_gitdata.load_yaml_file('config.yaml', strict=False) or {}
        app_env_config = shipment_config.get("applications", {}).get(application, {}).get("environments", {})
        stage_rpa = app_env_config.get("stage", {}).get("releasePlan", "n/a")
        prod_rpa = app_env_config.get("prod", {}).get("releasePlan", "n/a")

        data = None
        if self.kind != "fbc":
            et_data = self.runtime.get_errata_config()
            _, minor, patch = self.runtime.get_major_minor_patch()
            advisory_boilerplate = get_advisory_boilerplate(
                runtime=self.runtime, et_data=et_data, art_advisory_key=self.kind, errata_type="RHBA"
            )

            # Get advisory docs info from shipment config
            advisory_type, live_id, current_year = get_advisory_docs_info(self.runtime, self.kind)

            synopsis = advisory_boilerplate['synopsis'].format(MINOR=minor, PATCH=patch)
            advisory_topic = advisory_boilerplate['topic'].format(MINOR=minor, PATCH=patch)
            advisory_description = advisory_boilerplate['description'].format(
                MINOR=minor, PATCH=patch, ADVISORY_TYPE=advisory_type, YEAR=current_year, LIVE_ID=live_id
            )
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
                    application=application,
                    group=self.runtime.group,
                    assembly=self.runtime.assembly,
                    fbc=self.kind == "fbc",
                ),
                environments=Environments(
                    stage=ShipmentEnv(releasePlan=stage_rpa),
                    prod=ShipmentEnv(releasePlan=prod_rpa),
                ),
                data=data,
            ),
        )

        return shipment.model_dump(exclude_unset=True, exclude_none=True)


@shipment_cli.command("init", short_help="Init a new shipment config for a Konflux release")
@click.argument(
    "kind",
    metavar="<KIND>",
    type=click.Choice(["image", "extras", "metadata", "microshift-bootc", "fbc"]),
)
@click.pass_obj
@click_coroutine
async def init_shipment_cli(runtime: Runtime, kind: str):
    """
    Init a new shipment config based on the given group and assembly, for a Konflux release.
    Shipment config will include advisory information if applicable.
    \b

    Init shipment config for a 4.18 microshift advisory

    $ elliott -g openshift-4.18 --assembly 4.18.2 shipment init image
    """
    if runtime.assembly in ["stream", "test"]:
        raise ValueError("Please init shipment config with a named assembly")

    pipeline = InitShipmentCli(
        runtime=runtime,
        kind=kind,
    )

    shipment_yaml = await pipeline.run()
    yaml.dump(shipment_yaml, sys.stdout)
