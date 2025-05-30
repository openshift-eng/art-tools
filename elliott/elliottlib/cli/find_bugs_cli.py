import json
from typing import Dict, List, Set

import click
from artcommonlib import logutil

from elliottlib import Runtime, constants
from elliottlib.bzutil import Bug
from elliottlib.cli import common
from elliottlib.cli.common import click_coroutine
from elliottlib.cli.find_bugs_sweep_cli import (
    FindBugsSweep,
    categorize_bugs_by_type,
    get_assembly_bug_ids,
    get_bugs_sweep,
)
from elliottlib.errata import get_advisory_nvrs

LOGGER = logutil.get_logger(__name__)
type_bug_list = List[Bug]
type_bug_set = Set[Bug]


@common.cli.command("find-bugs", short_help="Find eligible bugs for the given assembly")
@click.option(
    "--filter-attached/--no-filter-attached",
    is_flag=True,
    default=False,
    help="Filter out bugs that are already attached to advisories on ErrataTool",
)
@click.option(
    "--permissive",
    is_flag=True,
    default=False,
    help="Ignore bugs that are determined to be invalid and continue",
)
@click.option(
    '--output',
    '-o',
    type=click.Choice(['json', 'text']),
    help='Output in the specified format',
)
@click.pass_obj
@click_coroutine
async def find_bugs_cli(
    runtime: Runtime,
    filter_attached,
    permissive,
    output,
):
    """Find OCP bugs for the given group and assembly, eligible for release.

    The --group and --assembly sets the criteria for the bugs to be found.
    default jira search statuses: ['MODIFIED', 'ON_QA', 'VERIFIED']
    By default, bugs that are already attached to other advisories are filtered out (--filter-attached)

    Security Tracker Bugs are validated and categorized based on attached builds
    to advisories that are in the assembly. The assumption is that:
    - For every tracker bug, there is a corresponding build attached to an advisory in the assembly.
    - The tracker bug will follow the advisory kind of that corresponding build.
    To disable, use --permissive

    Find bugs for all advisory types in assembly and output them in JSON:

    \b
        $ elliott -g openshift-4.18 --assembly 4.18.5 find-bugs -o json

    """
    cli = FindBugsCli(
        runtime=runtime,
        filter_attached=filter_attached,
        permissive=permissive,
        output=output,
    )
    await cli.run()


class FindBugsCli:
    def __init__(
        self,
        runtime: Runtime,
        filter_attached: bool,
        permissive: bool,
        output: str,
    ):
        self.runtime = runtime
        self.filter_attached = filter_attached
        self.permissive = permissive
        self.output = output
        self.bug_tracker = None

    async def run(self):
        self.runtime.initialize(mode="both")
        self.bug_tracker = self.runtime.get_bug_tracker('jira')

        bugs_by_kind: type_bug_list = await self.find_bugs()
        bugs_by_kind_formatted = {kind: sorted([b.id for b in bugs]) for kind, bugs in bugs_by_kind.items()}

        if self.output == 'json':
            click.echo(json.dumps(bugs_by_kind_formatted, indent=4))
        else:
            for kind, bugs in bugs_by_kind_formatted.items():
                click.echo(f"{kind} bugs: {', '.join(bugs)}")

    def get_builds_by_advisory_kind(self) -> Dict[str, Dict]:
        advisory_id_by_kind = self.runtime.get_default_advisories()
        builds_by_advisory_kind = {}
        if self.runtime.build_system == "brew":
            for kind, advisory_id in advisory_id_by_kind.items():
                if not advisory_id:
                    continue
            builds_by_advisory_kind[kind] = get_advisory_nvrs(advisory_id)
        else:
            group_config = (
                self.releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})
            )
            shipments = group_config.get("shipments", [])

            # restrict to only one shipment for now
            if len(shipments) != 1:
                raise ValueError("Operation not supported: shipments should have atleast and only one entry (for now)")

            shipment_config = shipments[0]
            shipment_url = shipment_config.get("url", "")
            if not shipment_url:
                LOGGER.info(
                    "No shipment MR found in the group config for the assembly. This will disable tracker bug categorization."
                )
                return builds_by_advisory_kind
            shipment_advisory_kinds = {advisory.get("kind") for advisory in shipment_config.get("advisories", [])}
            for kind in shipment_advisory_kinds:
                builds_by_advisory_kind[kind] = self.get_builds_from_shipment_mr(kind, shipment_url)

    def get_builds_from_shipment_mr(self, kind: str, shipment_mr_url: str) -> Dict:
        pass

    async def find_bugs(self) -> Dict[str | int, type_bug_set]:
        """Find all eligible bugs for shipping for the current assembly and categorize them by advisory kind.
        :return: A dictionary where keys are advisory kinds and values are sets of Bug objects.
        """

        find_bugs_obj = FindBugsSweep()
        statuses = sorted(find_bugs_obj.status)
        tr = self.bug_tracker.target_release()
        LOGGER.info(f"Searching {self.bug_tracker.type} for bugs with status {statuses} and target releases: {tr}\n")

        bugs = await get_bugs_sweep(self.runtime, find_bugs_obj, self.bug_tracker, filter_attached=self.filter_attached)
        included_bug_ids, _ = get_assembly_bug_ids(self.runtime, bug_tracker_type=self.bug_tracker.type)
        major_version, _ = self.runtime.get_major_minor()

        builds_by_advisory_kind = self.get_builds_by_advisory_kind()
        bugs_by_type, _ = categorize_bugs_by_type(
            bugs,
            builds_by_advisory_kind,
            included_bug_ids,
            permissive=self.permissive,
            major_version=major_version,
            advance_release=self.advance_release,
        )
        for kind, kind_bugs in bugs_by_type.items():
            LOGGER.info(f'{kind} bugs: {[b.id for b in kind_bugs]}')

        return bugs_by_type
