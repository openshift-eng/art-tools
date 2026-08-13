"""
doozer golang-builder-shipment — create a shipment MR in ocp-shipment-data for a golang builder image.

Invocation::

    doozer --group rhel-9-golang-1.25 golang-builder-shipment \\
        openshift-golang-builder-container-v1.25.11-....el9
"""

import logging
from typing import Optional, Tuple

import click

from doozerlib.backend.golang_builder_shipment import GolangBuilderShipmentHandler
from doozerlib.cli import cli, click_coroutine, pass_runtime

_LOGGER = logging.getLogger(__name__)


@cli.command("golang-builder-shipment", short_help="Create shipment MR in ocp-shipment-data for a golang builder image")
@click.option("--ocp-version", required=False, default=None, help="OCP version label (e.g. 4.22)")
@click.option("--art-jira", default="", help="Related ART Jira ticket (e.g. ART-20930)")
@click.option("--shipment-data-repo-url", default=None, help="Override ocp-shipment-data repo URL")
@click.argument("nvrs", nargs=-1, required=True)
@pass_runtime
@click_coroutine
async def golang_builder_shipment(
    runtime,
    ocp_version: Optional[str],
    art_jira: str,
    shipment_data_repo_url: Optional[str],
    nvrs: Tuple[str, ...],
):
    """Create a shipment MR in ocp-shipment-data for golang builder images.

    Pass one or more Konflux image NVRs as arguments. The golang group is derived
    from the NVRs automatically.
    """
    handler = GolangBuilderShipmentHandler(
        runtime=runtime,
        art_jira=art_jira,
        ocp_version=ocp_version,
        shipment_data_repo_pull_url=shipment_data_repo_url,
        shipment_data_repo_push_url=shipment_data_repo_url,
    )
    mr_url = await handler.create_shipment_from_nvrs(list(nvrs))
    click.echo(f"Shipment MR: {mr_url}")
