"""
CLI command for release readiness checks.
"""

import asyncio
import json
import logging

import click

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.release_readiness.pipeline import ReleaseReadinessPipeline
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)


@cli.command("release-readiness")
@click.option(
    "--group",
    "groups",
    required=True,
    multiple=True,
    help="OCP version group (e.g., openshift-4.21). May be repeated to check multiple versions.",
)
@click.option("--build-system", type=click.Choice(["brew", "konflux"]), default="konflux")
@click.option("--output-file", required=True, help="Write combined JSON report to this file, keyed by group")
@pass_runtime
@click_coroutine
async def release_readiness(
    runtime: Runtime,
    groups: tuple[str, ...],
    build_system: str,
    output_file: str,
):
    """
    Check release readiness for one or more OCP versions and output a combined JSON report.

    Example:
        artcd release-readiness --group openshift-4.21 --output-file report.json
        artcd release-readiness --group openshift-4.20 --group openshift-4.21 --output-file report.json
    """

    reports = await asyncio.gather(*[ReleaseReadinessPipeline(runtime, group, build_system).run() for group in groups])

    report_by_group = {report.group: report.model_dump(mode="json") for report in reports}
    with open(output_file, "w") as f:
        json.dump(report_by_group, f, indent=2, ensure_ascii=False)
    _LOGGER.info("JSON report written to %s", output_file)
