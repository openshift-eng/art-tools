import click
import re

from typing import Dict, List

from elliottlib import Runtime, constants, early_kernel
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.find_builds_cli import _fetch_builds_by_kind_rpm
from elliottlib.exceptions import ElliottFatalError
from elliottlib.bzutil import JIRABugTracker, JIRABug, BugzillaBugTracker, BugzillaBug
from artcommonlib.rhcos import get_container_configs
from artcommonlib.format_util import green_print, red_print
from elliottlib.util import get_nvrs_from_payload, get_golang_container_nvrs, get_golang_rpm_nvrs
from elliottlib.rpm_utils import parse_nvr
from elliottlib import errata
from doozerlib.cli.get_nightlies import find_rc_nightlies
from pyartcd.util import get_release_name_for_assembly, load_releases_config
from pyartcd import constants as pyartcd_constants


class FindBugsGolangCli:
    def __init__(self, runtime: Runtime, pullspec: str, cve_id, fixed_in_nvr,
                 update_tracker: bool, dry_run: bool):
        self._runtime = runtime
        self._logger = runtime.logger
        self.cve_id = cve_id
        self.fixed_in_nvr = fixed_in_nvr
        self.update_tracker = update_tracker
        self.dry_run = dry_run

        # cache
        self.pullspec = pullspec
        self.flaw_bugs = None
        self.go_nvr_map = None
        self.rpm_nvrps = None

        self.jira_tracker: JIRABugTracker = self._runtime.get_bug_tracker("jira")
        self.bz_tracker: BugzillaBugTracker = self._runtime.get_bug_tracker("bugzilla")

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
            if constants.GOLANG_BUILDER_CVE_COMPONENT in go_build:
                v = parse_nvr(go_build)['version']

            match = re.search(r'(\d+\.\d+\.\d+)', v)
            version = match.group(1)
            if version not in versions_to_build_map:
                versions_to_build_map[version] = 0
            versions_to_build_map[version] += len(go_nvr_map[go_build])
            total_builds += len(go_nvr_map[go_build])

        self._logger.info(f'Found parent go build versions {sorted(versions_to_build_map.keys())}')

        fixed_in_versions = set()
        for existing_version in versions_to_build_map.keys():
            e_major, e_minor, e_patch = (int(x) for x in existing_version.split('.'))
            for fixed_version in tracker_fixed_in:
                f_major, f_minor, f_patch = (int(x) for x in fixed_version.split('.'))
                if e_major == f_major and e_minor == f_minor:
                    if e_patch >= f_patch:
                        self._logger.info(f"{bug.id} for {bug.whiteboard_component} is fixed in {existing_version}")
                        fixed_in_versions.add(existing_version)

        fixed = False
        if fixed_in_versions:
            self._logger.info(f"Fix is found in versions {fixed_in_versions}")

        not_fixed_in = set(versions_to_build_map.keys()) - fixed_in_versions
        if not_fixed_in:
            self._logger.info(f"Couldn't determine if fix is in builders for versions {not_fixed_in}")
            vuln_builds = sum([versions_to_build_map[v] for v in not_fixed_in])
            self._logger.info(f"Potentially vulnerable builds: {vuln_builds}")

            # In case this is for builder image
            # and if vulnerable builds make up for less than 10% of total builds, consider it fixed
            # this is due to etcd and a few payload images lagging behind due to special reasons
            if bug.whiteboard_component == constants.GOLANG_BUILDER_CVE_COMPONENT and vuln_builds / total_builds < 0.1:
                self._logger.info("Vulnerable builds make up for less than 10% of total builds, considering it fixed")
                fixed = True
        else:
            fixed = True

        if bug.whiteboard_component == constants.GOLANG_BUILDER_CVE_COMPONENT:
            build_artifacts = f"Images in release payload {self.pullspec}"
        else:
            nvrs = []
            for nvr_group in go_nvr_map.values():
                nvrs.extend([f"{n[0]}-{n[1]}-{n[2]}" for n in nvr_group])
            if fixed:
                self._logger.info(f"Fix is in nvrs: {nvrs}")
                # find_errata_for_fixed_nvrs
                advisory_cache = {}

                def advisory_synopsis(advisory_id):
                    if advisory_id in advisory_cache:
                        return advisory_cache[advisory_id]
                    erratum = errata.get_raw_erratum(advisory_id)
                    errata_dict = erratum["errata"]
                    s = None
                    for _, info in errata_dict.items():
                        s = info["synopsis"]
                        advisory_cache[advisory_id] = info["synopsis"]
                        break
                    if not s:
                        self._logger.warning(f"Could not fetch advisory {advisory_id} synopsis")
                    return s

                for nvr in nvrs:
                    build = errata.get_brew_build(nvr)
                    # filter out dropped advisories
                    advisories = [ad for ad in build.all_errata if ad["status"] != "DROPPED_NO_SHIP"]
                    if not advisories:
                        red_print(f"Build {nvr} is not attached to any advisories.")
                        continue
                    for advisory in advisories:
                        if advisory["status"] == "SHIPPED_LIVE":
                            synopsis = advisory_synopsis(advisory['id'])
                            if "OpenShift Container Platform" in synopsis:
                                message = f"{nvr} has shipped with OCP advisory {advisory['name']} - {synopsis}."
                                green_print(message)

            build_artifacts = f"These nvrs {sorted(nvrs)}"

        comment = f"{bug.id} is associated with flaw bug(s) {bug.corresponding_flaw_bug_ids} " \
                  f"which are fixed in golang version(s) {tracker_fixed_in}. {build_artifacts} are built by " \
                  f"parent golang build versions {sorted(go_nvr_map.keys())}. " \
                  f"Fix is determined to be in builder versions {fixed_in_versions}."

        return fixed, comment

    async def is_fixed_rpm(self, bug, rpm_name, tracker_fixed_in=None, fixed_in_nvr=None):
        if not self.rpm_nvrps:
            # fetch assembly selected nvrs
            replace_vars = self._runtime.group_config.vars.primitive() if self._runtime.group_config.vars else {}
            et_data = self._runtime.get_errata_config(replace_vars=replace_vars)
            tag_pv_map = et_data.get('brew_tag_product_version_mapping')
            brew_session = self._runtime.build_retrying_koji_client(caching=True)
            self.rpm_nvrps = await _fetch_builds_by_kind_rpm(self._runtime, tag_pv_map, brew_session,
                                                             include_shipped=True, member_only=False)
            self.rpm_nvrps = set(self.rpm_nvrps)

            # fetch microshift nvrs
            member_rpm_nvrps = await _fetch_builds_by_kind_rpm(self._runtime, tag_pv_map, brew_session,
                                                               include_shipped=True, member_only=True)
            self.rpm_nvrps.update(set(member_rpm_nvrps))

        nvrs = []
        for nvrp in self.rpm_nvrps:
            if nvrp[0] == rpm_name:
                nvrs.append((nvrp[0], nvrp[1], nvrp[2]))
        if not nvrs:
            self._logger.warning(f"rpm {rpm_name} not found for assembly {self._runtime.assembly}. Is it a valid rpm?")
            return False, None

        go_nvr_map = get_golang_rpm_nvrs(nvrs, self._logger)
        if fixed_in_nvr:
            fixed_nvrs = []
            comment = ''
            for go_build in go_nvr_map.keys():
                fix_found = False
                for f in fixed_in_nvr:
                    if go_build in f:
                        fix_found = True
                        formatted_nvrs = [f'{n[0]}-{n[1]}-{n[2]}' for n in go_nvr_map[go_build]]
                        self._logger.info(f'NVRs found to be built with the desired golang build {go_build}: '
                                          f'{formatted_nvrs}')
                        fixed_nvrs.extend(formatted_nvrs)
                if not fix_found:
                    self._logger.info(f'NVRs found to be on different golang build {go_build}: {go_nvr_map[go_build]}')

            fixed = len(fixed_nvrs) == len(nvrs)
            if fixed:
                comment = f"Component NVRs found in to be built with golang builds containing fix: {formatted_nvrs}"
            return fixed, comment
        else:
            return self._is_fixed(bug, tracker_fixed_in, go_nvr_map)

    async def is_fixed_golang_builder(self, bug, tracker_fixed_in=None, fixed_in_nvr=None):
        if not self.pullspec:
            self._logger.info('Fetching latest accepted nightly...')
            nightlies = await find_rc_nightlies(self._runtime, arches={'x86_64'}, allow_pending=False,
                                                allow_rejected=False)
            if len(nightlies['x86_64']) < 1:
                raise ElliottFatalError("Could not find any accepted nightlies. Please investigate")
            self.pullspec = nightlies['x86_64'][0]['pullSpec']

        if not self.go_nvr_map:
            self._logger.info(f"Fetching go build nvrs for {self.pullspec}...")
            rhcos_images = {c['name'] for c in get_container_configs(self._runtime)}
            nvr_map = await get_nvrs_from_payload(self.pullspec, rhcos_images)
            nvrs = [(n, vr_tuple[0], vr_tuple[1]) for n, vr_tuple in nvr_map.items()]
            self.go_nvr_map = get_golang_container_nvrs(nvrs, self._logger)

        if fixed_in_nvr:
            # TODO: fix this
            for go_build in self.go_nvr_map.keys():
                for f in fixed_in_nvr:
                    if go_build in f:
                        return True, ''
            return False, ''
        else:
            return self._is_fixed(bug, tracker_fixed_in, self.go_nvr_map)

    async def run(self):
        logger = self._logger
        target_release = self.jira_tracker.target_release()
        tr = ','.join(target_release)
        logger.info(f"Searching for open golang security trackers with target version {tr}")

        query = ('project = "OCPBUGS" and summary ~ "golang" and statusCategory != done '
                 'and status not in (MODIFIED, ON_QA, Verified) and labels = "SecurityTracking" '
                 f'and "Target Version" in ({tr})')

        bugs: List[JIRABug] = self.jira_tracker._search(query, verbose=self._runtime.debug)

        def is_valid(b: JIRABug):
            if self.cve_id and not b.summary.startswith(self.cve_id):
                return False

            # Do not touch embargoed bugs
            if b.bug.fields.security.name == "Embargoed Security Issue":
                return False

            comp = b.whiteboard_component
            if not comp:
                return False
            not_art = ["sandboxed-containers"]
            if comp in not_art:
                return False
            return not (comp.endswith("-container") and comp != constants.GOLANG_BUILDER_CVE_COMPONENT)

        bugs = [b for b in bugs if is_valid(b)]
        bugs = sorted(bugs, key=lambda b: b.id)
        logger.info(f"Found {len(bugs)} bugs")
        for b in bugs:
            logger.info(f"{b.id} status={b.status} component={b.whiteboard_component}")

        self.flaw_bugs: Dict[int, BugzillaBug] = {}
        fixed_bugs, unfixed_bugs = [], []
        for bug in bugs:
            component = bug.whiteboard_component
            logger.info(f"{bug.id} has security component: {component}")
            fixed, comment = False, ''

            if self.fixed_in_nvr:
                if component == constants.GOLANG_BUILDER_CVE_COMPONENT:
                    fixed, comment = await self.is_fixed_golang_builder(bug, fixed_in_nvr=self.fixed_in_nvr)
                else:
                    fixed, comment = await self.is_fixed_rpm(bug, component, fixed_in_nvr=self.fixed_in_nvr)
            else:
                tracker_fixed_in = self.tracker_fixed_in(bug)
                if not tracker_fixed_in:
                    self._logger.warning(
                        f"Could not determine fixed in versions for {bug.id}. Ignoring it for now")
                    continue
                logger.info(f"{bug.id} is fixed in: {tracker_fixed_in}")

                if component == constants.GOLANG_BUILDER_CVE_COMPONENT:
                    fixed, comment = await self.is_fixed_golang_builder(bug, tracker_fixed_in=tracker_fixed_in)
                else:
                    fixed, comment = await self.is_fixed_rpm(bug, component, tracker_fixed_in=tracker_fixed_in)

            if fixed:
                if self.update_tracker:
                    # TODO: fix this
                    message = "Refer to ART ticket for details"
                    comment = f"{comment}. {message}"
                    if bug.status in ['New', 'ASSIGNED', 'POST']:
                        self.jira_tracker.update_bug_status(bug, 'MODIFIED', comment=comment, noop=self.dry_run)
                    else:
                        self.jira_tracker.add_comment(bug.id, comment, private=True, noop=self.dry_run)
                fixed_bugs.append(bug.id)
            else:
                unfixed_bugs.append(bug.id)

        if fixed_bugs:
            green_print(f'Fixed bugs: {sorted(fixed_bugs)}')
        if unfixed_bugs:
            red_print(f'Not fixed / unsure bugs: {sorted(unfixed_bugs)}')


@cli.command("find-bugs:golang", short_help="Find, analyze and update golang tracker bugs")
@click.option("--pullspec", default=None,
              help="Pullspec of release payload to check against. If not provided, latest accepted nightly will be used")
@click.option("--cve-id",
              help="CVE ID (example: CVE-2024-1394) that trackers should be fetched for")
@click.option("--fixed-in-nvr", multiple=True,
              help="golang build nvr (example: golang-1.20.12-2.el9_3) that given CVE(s) fixed in")
@click.option("--update-tracker",
              is_flag=True,
              default=False,
              help="If a tracker bug is fixed then comment with analysis and move to ON_QA")
@click.option("--dry-run", "--noop",
              is_flag=True,
              default=False,
              help="Don't change anything")
@click.pass_obj
@click_coroutine
async def find_bugs_golang_cli(runtime: Runtime, pullspec: str, cve_id, fixed_in_nvr, update_tracker: bool,
                               dry_run: bool):
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
    runtime.initialize(mode="rpms")  # disabled=True for microshift
    if runtime.assembly != 'stream':
        if pullspec:
            raise click.BadParameter('Cannot use --pullspec and --assembly at the same time')
        else:
            releases_config = runtime.get_releases_config()
            release_name = get_release_name_for_assembly(runtime.group, releases_config,
                                                         runtime.assembly)
            pullspec = f'{pyartcd_constants.RELEASE_IMAGE_REPO}:{release_name}-x86_64'

    if cve_id:
        cve_id = cve_id.upper()

    # We want to load all configs for rpms, include disabled so microshift is included
    cli = FindBugsGolangCli(
        runtime=runtime,
        pullspec=pullspec,
        cve_id=cve_id,
        fixed_in_nvr=fixed_in_nvr,
        update_tracker=update_tracker,
        dry_run=dry_run
    )
    await cli.run()
