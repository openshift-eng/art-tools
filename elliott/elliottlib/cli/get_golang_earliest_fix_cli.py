import click
import re

from typing import Dict, List

from elliottlib import Runtime, constants, early_kernel
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.bzutil import JIRABugTracker, JIRABug, BugzillaBugTracker, BugzillaBug, Bug
from elliottlib.util import get_nvrs_from_payload, get_golang_container_nvrs, get_golang_rpm_nvrs
from elliottlib.rpm_utils import parse_nvr
from elliottlib.assembly import AssemblyTypes, assembly_type, assembly_basis_event
from elliottlib import errata


class GetGolangEarliestFixCli:
    def __init__(self, runtime: Runtime, bug_ids: List[str]):
        self._runtime = runtime
        self._logger = runtime.logger
        self.bug_ids = bug_ids

        # cache
        self.flaw_bugs: Dict[int, BugzillaBug] = {}
        self.advisory_build_cache = {}

        self.jira_tracker: JIRABugTracker = self._runtime.get_bug_tracker("jira")
        self.bz_tracker: BugzillaBugTracker = self._runtime.get_bug_tracker("bugzilla")

    def get_advisory_builds(self, advisory_id):
        if advisory_id not in self.advisory_build_cache:
            self.advisory_build_cache[advisory_id] = errata.get_brew_builds(advisory_id)
        return self.advisory_build_cache[advisory_id]

    def flaw_fixed_in(self, flaw_id):
        if flaw_id in self.flaw_bugs:
            flaw_bug = self.flaw_bugs[flaw_id]
        else:
            flaw_bug = self.bz_tracker.get_bug(flaw_id)
            if not flaw_bug:
                self._logger.warning(f"Could not find flaw bug {flaw_id} in bugzilla, please investigate. Ignoring "
                                     "flaw bug for now")
                return None
            self.flaw_bugs[flaw_id] = flaw_bug
        if 'golang:' not in flaw_bug.summary.lower():
            self._logger.info(f"{flaw_bug.id} doesn't have `golang:` in title. title=`{flaw_bug.summary}`. "
                              "Is it a golang compiler cve? Ignoring flaw bug")
            return None

        fixed_in = flaw_bug.fixed_in
        # value can be "golang 1.20.9, golang 1.21.2"
        # or "Go 1.20.7, Go 1.19.12"
        # or "Go 1.20.2 and Go 1.19.7"
        # or "golang 1.20" -> 1.20.0
        fixed_in_versions = re.findall(r'(\d+\.\d+\.\d+)', fixed_in)
        if not fixed_in_versions:
            # TODO: Sometimes bugzilla do not have accurate fixed_in version values
            # See if you can query https://pkg.go.dev/vuln/GO-2023-2375
            # or https://cveawg.mitre.org/api/cve/CVE-2023-45287
            # to get accurate affected versions information
            fixed_in_versions = re.findall(r'(\d+\.\d+)', fixed_in)
            if fixed_in_versions:
                fixed_in_versions = {f"{v}.0" for v in fixed_in_versions}
            else:
                self._logger.warning(f"{flaw_bug.id} doesn't have valid fixed_in value: {fixed_in}")
                return None
        return set(fixed_in_versions)

    def tracker_fixed_in(self, bug):
        f_ids: List[int] = bug.corresponding_flaw_bug_ids
        if not f_ids:
            self._logger.warning(f"{bug.id} doesn't have any flaw bugs, please investigate")
            return None

        tracker_fixed_in = set()
        for f_id in f_ids:
            flaw_fixed_in = self.flaw_fixed_in(f_id)
            if not flaw_fixed_in:
                self._logger.warning(
                    f"Could not determine fixed in version for {f_id}. Ignoring it for now")
                continue
            tracker_fixed_in.update(flaw_fixed_in)
        return sorted(tracker_fixed_in)

    def _is_fixed(self, bug, tracker_fixed_in, go_nvr_map):
        versions_to_build_map = {}
        total_builds = 0
        for go_build in go_nvr_map.keys():
            # extract go version from nvr
            v = go_build
            match = re.search(r'(\d+\.\d+\.\d+)', v)
            version = match.group(1)
            if version not in versions_to_build_map:
                versions_to_build_map[version] = 0
            versions_to_build_map[version] += len(go_nvr_map[go_build])
            total_builds += len(go_nvr_map[go_build])

        self._logger.debug(f'Found parent go build versions {sorted(versions_to_build_map.keys())}')

        fixed_in_versions = set()
        for existing_version in versions_to_build_map.keys():
            e_major, e_minor, e_patch = (int(x) for x in existing_version.split('.'))
            for fixed_version in tracker_fixed_in:
                f_major, f_minor, f_patch = (int(x) for x in fixed_version.split('.'))
                if e_major == f_major and e_minor == f_minor:
                    if e_patch >= f_patch:
                        self._logger.debug(f"{bug.id} for {bug.whiteboard_component} is fixed in {existing_version}")
                        fixed_in_versions.add(existing_version)

        unfixed_versions = set(versions_to_build_map.keys()) - fixed_in_versions
        return not unfixed_versions

    async def earliest_fix_rpm(self, bug, tracker_fixed_in, rpm_name):
        # We want to find the earliest rpm build that had the fix and which got shipped in an errata
        # example https://errata.devel.redhat.com/package/show/skopeo
        # But there is no ET endpoint which serves us this mapping {advisory: shipped_brew_build} for a package
        # So we need to build this mapping ourselves

        # get all advisories that this rpm has shipped in
        package_info = errata.get_package(rpm_name)
        attached_in_erratas = {e['id']: e['status'] for e in package_info['data']['relationships']['errata']}

        # get all ga/z-stream rpm advisories from releases.yml that overlap with above advisories
        # this is faster than querying ET for all those advisories
        releases_config = self._runtime.get_releases_config()
        fixed_results = []
        for assembly_name, info in releases_config['releases'].items():
            a_type = assembly_type(releases_config, assembly_name)
            if a_type != AssemblyTypes.STANDARD:
                continue

            rpm_advisory = info['assembly'].get('group', {}).get('advisories', {}).get('rpm', 0)
            if rpm_advisory not in attached_in_erratas:
                continue

            status = 'shipped' if attached_in_erratas[rpm_advisory] == 'SHIPPED_LIVE' else 'is shipping'
            nvrs = []
            for b in self.get_advisory_builds(rpm_advisory):
                parsed = parse_nvr(b.nvr)
                if parsed['name'] == rpm_name:
                    nvrs.append((parsed['name'], parsed['version'], parsed['release']))
            self._logger.info(f"{rpm_name} {status} in {assembly_name} advisory {rpm_advisory}: {nvrs}")
            go_nvr_map = get_golang_rpm_nvrs(nvrs, self._logger)
            fixed = self._is_fixed(bug, tracker_fixed_in, go_nvr_map)
            status = "fixed" if fixed else "not fixed"
            self._logger.info(f"{bug.id} is {status} in {assembly_name}")
            fixed_results.append((assembly_name, rpm_advisory, fixed))

        previous_fixed = None
        for assembly_name, rpm_advisory, fixed in fixed_results[::-1]:
            if fixed and not previous_fixed:
                click.echo(f'Fix for {bug.id} was first introduced in {assembly_name}, advisory: {rpm_advisory}')
                break
            previous_fixed = fixed

    async def run(self):
        logger = self._logger

        for b in self.bug_ids:
            if not b.startswith("OCPBUGS"):
                raise ValueError(f"bug_id is expected to be `OCPBUGS-XXXX` instead is {b}")

        bugs: List[JIRABug] = self.jira_tracker.get_bugs(self.bug_ids)
        major, minor = self._runtime.get_major_minor()
        version = f'{major}.{minor}'
        tr = Bug.get_target_release(bugs)
        if not tr.startswith(version):
            raise ValueError(f"expected given bugs to have target_release={version}* but found {tr}")

        def is_valid(b: JIRABug) -> (bool, str):
            # golang compiler cve title text always has `golang:`
            # this ignores golang lib cves like `podman: net/http, golang.org/x/net/http2:`
            marker = 'golang:'
            if marker not in b.summary:
                return False, f'`{marker}` not found in bug summary. Is it a valid golang compiler cve tracker?'

            comp = b.whiteboard_component
            if not comp:
                return False, 'cannot determine bug pscomponent'
            not_art = ["sandboxed-containers"]
            if comp in not_art:
                return False, f'bug has pscomponent={comp} which is not owned by ART team'
            if comp == constants.GOLANG_BUILDER_CVE_COMPONENT:
                return False, 'bug is a golang builder tracker. Only rpm trackers are supported'
            if comp.endswith("-container"):
                return False, f'bug has pscomponent={comp} which is not owned by ART team'
            return True, ''

        bugs = [b for b in bugs if is_valid(b)]
        valid_bugs, invalid_bugs = [], {}
        for b in bugs:
            valid, invalid_reason = is_valid(b)
            if not valid:
                invalid_bugs[b.id] = invalid_reason
            else:
                valid_bugs.append(b)
        if invalid_bugs:
            raise ValueError(f"Found invalid golang tracker bugs: {invalid_bugs}. Exclude them and rerun")

        bugs = sorted(valid_bugs, key=lambda b: b.id)
        for b in bugs:
            logger.info(f"{b.id} status={b.status} component={b.whiteboard_component}")

        for bug in bugs:
            component = bug.whiteboard_component
            tracker_fixed_in = self.tracker_fixed_in(bug)
            if not tracker_fixed_in:
                self._logger.warning(
                    f"Could not determine fixed in versions for {bug.id}. Ignoring it for now")
                continue
            logger.info(f"{bug.id} is fixed in: {tracker_fixed_in}")

            await self.earliest_fix_rpm(bug, tracker_fixed_in, component)


@cli.command("go:earliest-fix", short_help="Find earliest fix for golang tracker bugs")
@click.option("--bugs", "bug_ids", help="Tracker bugs to analyze, comma separated", required=True)
@click.pass_obj
@click_coroutine
async def get_golang_earliest_fix_cli(runtime: Runtime, bug_ids: str):
    """Find golang tracker bugs in jira and determine if they are fixed.
    Trackers are fetched from the OCPBUGS project that are assigned to Release component
    Passing in an assembly is the most straightforward way to analyze golang-builder as well as rpm trackers.
    Use --pullspec <nightly_pullspec> to determine for builds in a nightly.
    Note: rpm trackers cannot be processed if --pullspec is used, for that rely on --assembly.
    --update-tracker: If a tracker bug is fixed then comment on it with analysis and move the bug state to ON_QA

    # Determine if currently open golang tracker bugs are fixed against 4.14.8 assembly.

    $ elliott -g openshift-4.14 --assembly 4.14.8 find-bugs:golang

    $ elliott -g openshift-4.14 find-bugs:golang --pullspec quay.io/openshift-release-dev/ocp-release:4.14.8-x86_64

    """
    # We want to load all configs for rpms, include disabled so microshift is included
    runtime.initialize(mode="rpms", disabled=True)
    bug_ids = [b.strip() for b in bug_ids.split(",")] if bug_ids else []

    cli = GetGolangEarliestFixCli(runtime=runtime, bug_ids=bug_ids)
    await cli.run()
