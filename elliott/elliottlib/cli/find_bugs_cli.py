import json
from typing import Dict, List, Set

import click
from artcommonlib import logutil

from elliottlib import Runtime
from elliottlib.bzutil import Bug
from elliottlib.cli import common
from elliottlib.cli.common import click_coroutine
from elliottlib.cli.find_bugs_sweep_cli import (
    FindBugsSweep,
    categorize_bugs_by_type,
    get_assembly_bug_ids,
    get_bugs_sweep,
    get_builds_by_advisory_kind,
)

LOGGER = logutil.get_logger(__name__)
type_bug_list = List[Bug]
type_bug_set = Set[Bug]


@common.cli.command("find-bugs", short_help="Find eligible bugs for the given assembly")
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
    permissive,
    output,
):
    """Find OCP bugs for the given group and assembly, eligible for release.

    The --group and --assembly sets the criteria for the bugs to be found.
    default jira search statuses: ['MODIFIED', 'ON_QA', 'VERIFIED']

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
        permissive=permissive,
        output=output,
    )
    await cli.run()


class FindBugsCli:
    def __init__(
        self,
        runtime: Runtime,
        permissive: bool,
        output: str,
    ):
        self.runtime = runtime
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

    async def find_bugs(self) -> Dict[str | int, type_bug_set]:
        """Find all eligible bugs for shipping for the current assembly and categorize them by advisory kind.
        :return: A dictionary where keys are advisory kinds and values are sets of Bug objects.
        """

        find_bugs_obj = FindBugsSweep()
        statuses = sorted(find_bugs_obj.status)
        tr = self.bug_tracker.target_release()
        LOGGER.info(f"Searching {self.bug_tracker.type} for bugs with status {statuses} and target releases: {tr}\n")

        bugs = await get_bugs_sweep(self.runtime, find_bugs_obj, self.bug_tracker)
        included_bug_ids, _ = get_assembly_bug_ids(self.runtime, bug_tracker_type=self.bug_tracker.type)
        major_version, minor_version = self.runtime.get_major_minor()

        builds_by_advisory_kind = get_builds_by_advisory_kind(self.runtime)
        bugs_by_type, _ = categorize_bugs_by_type(
            bugs=bugs,
            builds_by_advisory_kind=builds_by_advisory_kind,
            permitted_bug_ids=included_bug_ids,
            major_version=major_version,
            minor_version=minor_version,
            operator_bundle_advisory="metadata",
            permissive=self.permissive,
        )
        for kind, kind_bugs in bugs_by_type.items():
            LOGGER.info(f'{kind} bugs: {[b.id for b in kind_bugs]}')

        return bugs_by_type
