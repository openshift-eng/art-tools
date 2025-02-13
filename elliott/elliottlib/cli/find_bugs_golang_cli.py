import functools
import requests
import click
import re
import logging

from typing import Dict, List, Union, Set, Tuple
from prettytable import PrettyTable
from semver.version import Version

from artcommonlib.rpm_utils import parse_nvr
from elliottlib import Runtime, constants
from elliottlib.cli.common import cli, click_coroutine
from elliottlib.cli.find_builds_cli import _fetch_builds_by_kind_rpm
from elliottlib.cli.get_golang_report_cli import golang_report_for_version
from elliottlib.exceptions import ElliottFatalError
from elliottlib.bzutil import JIRABugTracker, JIRABug, BugzillaBugTracker, BugzillaBug
from artcommonlib.rhcos import get_container_configs
from elliottlib.util import get_nvrs_from_release, get_golang_container_nvrs, get_golang_rpm_nvrs
from elliottlib import errata
from doozerlib.cli.get_nightlies import find_rc_nightlies
from pyartcd.util import get_release_name_for_assembly, load_releases_config
from pyartcd import constants as pyartcd_constants

LOGGER = logging.getLogger(__name__)


def get_cve_from_prodsec_db(cve_id):
    cve_url = f"https://access.redhat.com/hydra/rest/securitydata/cve/{cve_id}.json"
    response = requests.get(cve_url)
    try:
        data = response.json()
    except Exception as e:
        LOGGER.warning(f"Could not fetch CVE data for {cve_id}. Is bug embargoed?: {e}")
        return None
    return data


def get_fixed_in_version_go_db(go_vuln_id: str):
    go_vuln_db_api_url = f"https://vuln.go.dev/ID/{go_vuln_id}.json"
    res = requests.get(go_vuln_db_api_url)
    for entry in res.json()['affected']:
        if entry['package']['ecosystem'] != "Go" or entry['package']['name'] != "stdlib":
            continue
        for r in entry['ranges']:
            if r['type'] == "SEMVER":
                return {Version.parse(e['fixed']) for e in r['events'] if e.get('fixed')}
    return None


def get_cve_from_go_db(cve_id: str):
    pkg_go_dev_search_url = f"https://pkg.go.dev/search?q={cve_id}"
    res = requests.get(pkg_go_dev_search_url)
    go_vuln_id = res.url.split("/")[-1]
    if go_vuln_id.startswith("GO-"):
        LOGGER.info(f"Found {cve_id} in go vulnerability database: {go_vuln_id}")
        return go_vuln_id
    return None


def _fmt(version_list):
    return ", ".join(sorted(str(v) for v in version_list)) if version_list else ""


def get_component_from_bug_title(title):
    # example `golang: html/template: errors returned from MarshalJSON methods may break template escaping`
    # extract `golang: html/template` which is the most important bit
    try:
        return title.rsplit(':', 1)[0].strip()
    except Exception as e:
        LOGGER.warning(f"Could not extract component from title {title}: {e}")
        return 'Unknown'


class FindBugsGolangCli:
    def __init__(self, runtime: Runtime, pullspec: str, cve_ids: Tuple[str], components: Tuple[str],
                 tracker_ids: Tuple[str], analyze: bool,
                 fixed_in_nvrs: Tuple[str], update_tracker: bool, force_update_tracker: bool, art_jira: str,
                 exclude_bug_statuses: List[str], dry_run: bool):
        self._runtime = runtime
        self._logger = LOGGER
        self.cve_ids = cve_ids
        self.components = components
        self.tracker_ids = tracker_ids
        self.analyze = analyze
        self.fixed_in_nvrs = fixed_in_nvrs
        self.update_tracker = update_tracker
        self.force_update_tracker = force_update_tracker
        self.art_jira = art_jira
        self.exclude_bug_statuses = exclude_bug_statuses
        self.dry_run = dry_run

        # cache
        self.pullspec = pullspec
        self.flaw_bugs: Dict[int, BugzillaBug] = {}
        self.go_nvr_map = {}
        self.rpm_nvrps = None
        self.compatible_cves = set()

        self.jira_tracker: JIRABugTracker = self._runtime.get_bug_tracker("jira")
        self.bz_tracker: BugzillaBugTracker = self._runtime.get_bug_tracker("bugzilla")

    def get_flaw_bug(self, flaw_id: str):
        if flaw_id in self.flaw_bugs:
            return self.flaw_bugs[flaw_id]

        flaw_bug = self.bz_tracker.get_bug(flaw_id)
        if not flaw_bug:
            self._logger.warning(f"Could not find flaw bug {flaw_id} in bugzilla, please investigate. Ignoring "
                                 "flaw bug for now")
            return None
        self.flaw_bugs[flaw_id] = flaw_bug
        return flaw_bug

    def flaw_fixed_in(self, flaw_id: Union[str, BugzillaBug]) -> Union[None, Set[Version]]:
        if isinstance(flaw_id, BugzillaBug):
            flaw_bug = flaw_id
        else:
            flaw_bug = self.get_flaw_bug(flaw_id)
            if not flaw_bug:
                return None

        fixed_in = flaw_bug.fixed_in
        # value can be "golang 1.20.9, golang 1.21.2"
        # or "Go 1.20.7, Go 1.19.12"
        # or "Go 1.20.2 and Go 1.19.7"
        # or "golang 1.20" -> 1.20.0
        fixed_in_versions = re.findall(r'(\d+\.\d+\.\d+)', fixed_in)
        if not fixed_in_versions:
            fixed_in_versions = re.findall(r'(\d+\.\d+)', fixed_in)
            if fixed_in_versions:
                fixed_in_versions = {f"{v}.0" for v in fixed_in_versions}
            else:
                self._logger.warning(f"{flaw_bug.id} doesn't have valid fixed_in value: {fixed_in}")
                return None
        return {Version.parse(v) for v in set(fixed_in_versions)}

    def _is_fixed(self, bug: JIRABug, tracker_fixed_in: Set[Version], go_nvr_map) -> (bool, str):
        versions_to_build_map = {}
        total_builds = 0
        for go_build in go_nvr_map.keys():
            # extract go version from nvr
            v = go_build
            if constants.GOLANG_BUILDER_CVE_COMPONENT in go_build:
                v = parse_nvr(go_build)['version']

            match = re.search(r'(\d+\.\d+\.\d+)', v)
            version = Version.parse(match.group(1))
            if version not in versions_to_build_map:
                versions_to_build_map[version] = 0
            versions_to_build_map[version] += len(go_nvr_map[go_build])
            total_builds += len(go_nvr_map[go_build])

        self._logger.info(f'Found parent go build versions {[str(v) for v in sorted(versions_to_build_map.keys())]}')

        fixed_in_versions = set()
        for existing_version in versions_to_build_map.keys():
            for fixed_version in tracker_fixed_in:
                if (existing_version.major == fixed_version.major and existing_version.minor == fixed_version.minor
                   and existing_version.patch >= fixed_version.patch):
                    self._logger.info(f"{bug.id} for {bug.whiteboard_component} is fixed in {str(existing_version)}")
                    fixed_in_versions.add(existing_version)

        fixed = False
        if fixed_in_versions:
            self._logger.info(f"Fix is found in versions {[str(v) for v in fixed_in_versions]}")

        not_fixed_in = set(versions_to_build_map.keys()) - fixed_in_versions
        if not_fixed_in:
            self._logger.info(f"Couldn't determine if fix is in builders for versions {[str(v) for v in not_fixed_in]}")
            vuln_builds = sum([versions_to_build_map[v] for v in not_fixed_in])
            vuln_builds_by_version = {str(k): v for k, v in versions_to_build_map.items() if k in not_fixed_in}
            self._logger.info(f"Vulnerable builds by version: {vuln_builds_by_version}")
            self._logger.info(f"Total vulnerable builds: {vuln_builds}")

            # In case this is for builder image
            # and if vulnerable builds make up for less than 10% of total builds, consider it fixed
            # this is due to etcd and a few payload images lagging behind due to special reasons
            if bug.whiteboard_component == constants.GOLANG_BUILDER_CVE_COMPONENT and vuln_builds / total_builds < 0.1:
                self._logger.info("Vulnerable builds make up for less than 10% of total builds, considering it fixed")
                fixed = True
        else:
            fixed = True

        if bug.whiteboard_component == constants.GOLANG_BUILDER_CVE_COMPONENT:
            build_artifacts = f"Images in {self.pullspec}"
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
                    try:
                        build = errata.get_brew_build(nvr)
                    except Exception as e:
                        self._logger.warning(f"Could not fetch info about build {nvr} from errata: {e}")
                        continue
                    # filter out dropped advisories
                    advisories = [ad for ad in build.all_errata if ad["status"] != "DROPPED_NO_SHIP"]
                    if not advisories:
                        self._logger.info(f"Build {nvr} is not attached to any advisories.")
                        continue
                    for advisory in advisories:
                        if advisory["status"] == "SHIPPED_LIVE":
                            synopsis = advisory_synopsis(advisory['id'])
                            if "OpenShift Container Platform" in synopsis:
                                message = f"{nvr} has shipped with OCP advisory {advisory['name']} - {synopsis}."
                                self._logger.info(message)

            build_artifacts = f"These nvrs {sorted(nvrs)}"

        comment = (f"{bug.id} is associated with flaw bug(s) {bug.corresponding_flaw_bug_ids} "
                   f"which are fixed in golang version(s) {[str(v) for v in tracker_fixed_in]}. {build_artifacts} are built by "
                   f"parent golang build versions {[str(v) for v in sorted(go_nvr_map.keys())]}. "
                   f"Fix is determined to be in builder versions {[str(v) for v in fixed_in_versions]}.")

        return fixed, comment

    async def is_fixed_rpm(self, bug: JIRABug, rpm_name: str, tracker_fixed_in: Set[Version] = None) -> (bool, str):
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
        if self.fixed_in_nvrs:
            final_fixed_nvrs = []
            final_fixed_in_nvrs = []
            comment = ''
            for go_build_string in go_nvr_map.keys():
                fix_found = False
                for nvr_string in self.fixed_in_nvrs:
                    if go_build_string in nvr_string:
                        fix_found = True
                        formatted_nvrs = [f'{n[0]}-{n[1]}-{n[2]}' for n in go_nvr_map[go_build_string]]
                        self._logger.info(f'NVRs found to be built with the desired golang build {go_build_string}: '
                                          f'{formatted_nvrs}')
                        final_fixed_in_nvrs.append(nvr_string)
                        final_fixed_nvrs.extend(formatted_nvrs)
                        break
                if not fix_found:
                    self._logger.info(f'NVRs found to be on different golang build {go_build_string}: '
                                      f'{go_nvr_map[go_build_string]}')

            fixed = len(final_fixed_nvrs) == len(nvrs)
            if fixed:
                comment = (f"Component NVRs {final_fixed_nvrs} in ART candidate tags found to be built with golang "
                           f"builds containing fix: {final_fixed_in_nvrs}")
            return fixed, comment
        else:
            return self._is_fixed(bug, tracker_fixed_in, go_nvr_map)

    async def is_fixed_golang_builder(self, bug: JIRABug, tracker_fixed_in: Set[Version] = None) -> (bool, str):
        if not self.pullspec:
            self._logger.info('Fetching latest accepted nightly...')
            # we fetch pending and rejected nightlies as well since
            # we only need to determine if image builds are complete and have the fix
            nightlies = await find_rc_nightlies(self._runtime, arches={'x86_64'}, allow_pending=True,
                                                allow_rejected=True)
            if len(nightlies['x86_64']) < 1:
                raise ElliottFatalError("Could not find any accepted nightlies. Please investigate")
            self.pullspec = nightlies['x86_64'][0]['pullSpec']

        if not self.go_nvr_map:
            self._logger.info(f"Fetching go build nvrs for {self.pullspec}...")
            rhcos_images = {c['name'] for c in get_container_configs(self._runtime)}
            try:
                nvr_map = await get_nvrs_from_release(self.pullspec, rhcos_images)
            except Exception as e:
                self._logger.error("Does pullspec exist? To override use --pullspec. "
                                   f"Could not fetch go build nvrs for {self.pullspec}: {e}")
                raise e
            nvrs = [(n, vr_tuple[0], vr_tuple[1]) for n, vr_tuple in nvr_map.items()]
            self.go_nvr_map = get_golang_container_nvrs(nvrs, self._logger)

        if self.fixed_in_nvrs:
            builder_nvrs = [(p['name'], p['version'], p['release']) for p in [parse_nvr(n) for n in
                                                                              self.go_nvr_map.keys()]]
            go_builder_nvr_map = get_golang_container_nvrs(builder_nvrs, self._logger)
            go_builder_nvr_map = {k: v.pop() for k, v in go_builder_nvr_map.items()}
            total_builds = sum([len(v) for v in self.go_nvr_map.values()])
            self._logger.info(f"Total images in payload: {total_builds}")
            self._logger.info(f"Found parent golang builder nvrs: {sorted(self.go_nvr_map.keys())}")
            self._logger.info(f"Found parent go builds: {sorted(go_builder_nvr_map.keys())}")
            fixed_builds = 0
            fixed_in_builds = []
            for nvr_string in self.fixed_in_nvrs:
                for go_build_string in go_builder_nvr_map.keys():
                    if go_build_string in nvr_string:
                        builder_nvr = "-".join(go_builder_nvr_map[go_build_string])
                        fixed_builds += len(self.go_nvr_map[builder_nvr])
                        fixed_in_builds.append((builder_nvr, go_build_string))
                        self._logger.info(f'Images in payload found to be built with the desired golang'
                                          f' {go_build_string}: {len(self.go_nvr_map[builder_nvr])}')

            vuln_builds = total_builds - fixed_builds
            self._logger.info(f"Images found not built with desired golang: {vuln_builds}")
            # if vulnerable builds make up for less than 10% of total builds, consider it fixed
            # this is due to etcd and a few payload images lagging behind due to special reasons
            if vuln_builds / total_builds < 0.1:
                self._logger.info("Vulnerable images make up for less than 10% of total images, considering it fixed")
                comment = (f"{bug.cve_id} is fixed in golang builder(s) {fixed_in_builds} which are found to be "
                           f"the parent images in {self.pullspec}. Bug is considered fixed.")
                return True, comment

            return False, ''
        else:
            return self._is_fixed(bug, tracker_fixed_in, self.go_nvr_map)

    def move_to_qa_and_comment(self, bug: JIRABug, comment: str):
        if bug.status in ['New', 'ASSIGNED', 'POST', 'MODIFIED']:
            self.jira_tracker.update_bug_status(bug, 'ON_QA', comment=comment, noop=self.dry_run)
        else:
            self.jira_tracker.add_comment(bug.id, comment, private=True, noop=self.dry_run)

    async def run(self):
        logger = self._logger

        # fetch golang report for the version, fetched and compiled from streams.yml and brew buildroot
        # e.g. [{'go_version': '1.21.9', 'building_image_count': 239, 'building_rpm_count': 2}, {'go_version':
        # '1.19.13', 'building_image_count': 1}]
        # this will be used later to compare to flaw fixed in versions
        major, minor = self._runtime.get_major_minor()
        ocp_version = f"{major}.{minor}"
        golang_report: List[Dict] = golang_report_for_version(self._runtime, ocp_version, ignore_rhel=True)
        logger.info(f"Current golang versions being used in {ocp_version}: {golang_report}")

        if self.tracker_ids:
            logger.info(f"Fetching {len(self.tracker_ids)} given tracker(s)")
            bugs: List[JIRABug] = self.jira_tracker.get_bugs(self.tracker_ids)
        else:
            target_release = self.jira_tracker.target_release()
            tr = ','.join(target_release)
            logger.info(f"Searching for open security trackers with target version {tr}. "
                        "Then will filter to just the golang CVEs and trackers")

            exclude_status_clause = (f"and status not in ({', '.join(self.exclude_bug_statuses)}) "
                                     if self.exclude_bug_statuses else "")

            query = ('project = "OCPBUGS" '
                     'and statusCategory != done '
                     'and labels = "SecurityTracking" '
                     f'and "Target Version" in ({tr}) '
                     f'{exclude_status_clause}')

            bugs: List[JIRABug] = self.jira_tracker._search(query, verbose=self._runtime.debug)

        def is_valid(b: JIRABug):
            if not b.cve_id:
                logger.warning(f"{b.id} does not have a CVE ID set. Skipping")
                return False

            comp = b.whiteboard_component
            if not comp:
                logger.warning(f"{b.id} does not have a component set. Skipping")
                return False

            # if cve_ids are given, only operate on those
            if self.cve_ids and b.cve_id not in self.cve_ids:
                return False

            # if components are given, only operate on those
            if self.components and comp not in self.components:
                return False

            # Do not operate on embargoed bugs
            if b.bug.fields.security.name == "Embargoed Security Issue":
                return False

            not_art = ["sandboxed-containers"]
            if comp in not_art:
                return False
            if comp.endswith("-container") and comp != constants.GOLANG_BUILDER_CVE_COMPONENT:
                return False
            return True

        bugs = [b for b in bugs if is_valid(b)]
        logger.debug(f"Found {len(bugs)} total tracker bugs. Filtering them to golang trackers")
        if not bugs:
            return

        cves = sorted(set([b.cve_id for b in bugs]), reverse=True)
        cve_table = PrettyTable()
        cve_table.align = "l"
        cve_table.field_names = ["Bugzilla ID", "CVE", "Component in title", "Fixed in Versions", "Fix Compatible"]
        golang_cves_fixed_in = {}
        for cve_id in cves:
            go_vuln_id = get_cve_from_go_db(cve_id)
            if not go_vuln_id:
                logger.info(f"Could not find {cve_id} in go vulnerability database. Assuming it's not a "
                            "golang CVE. Skipping")
                continue
            logger.info(f"Found {cve_id} in go vulnerability database: {go_vuln_id}")

            fixed_in_version_go_db = get_fixed_in_version_go_db(go_vuln_id)
            if not fixed_in_version_go_db:
                logger.info(f"Could not find stdlib fixed in versions for {cve_id} in go vulnerability database. "
                            "Assuming it's not a golang stdlib CVE. Skipping")
                continue
            logger.info(f"Found fixed in versions for {cve_id} in go vulnerability database: "
                        f"{_fmt(fixed_in_version_go_db)}")
            golang_cves_fixed_in[cve_id] = fixed_in_version_go_db

            cve_data = get_cve_from_prodsec_db(cve_id)
            flaw_id = cve_data['bugzilla']['id']
            title = cve_data['bugzilla']['description']
            comp_in_title = get_component_from_bug_title(title)

            # Check rough compatibility of fixed-in versions with in-use golang versions
            # example, if fixed-in is 1.21.x and in-use are [1.20.y, 1.19.z] then they are incompatible
            # this is a rough check to exit early if there is no compatible version found
            # we will do a more detailed check later
            compatible = False
            flaw_bug = self.get_flaw_bug(flaw_id)
            fixed_in_version_bz = self.flaw_fixed_in(flaw_bug)
            if fixed_in_version_go_db != fixed_in_version_bz:
                self._logger.info(
                    f"{flaw_id} - Fixed in versions in go vulnerability database and bugzilla do not "
                    f"match. Go vulnerability database: {_fmt(fixed_in_version_go_db)}, "
                    f"Bugzilla: {_fmt(fixed_in_version_bz)}. Will use go vulnerability database as the source of truth.")

            if self.fixed_in_nvrs:
                compatible = True
            else:
                for fixed_in_version in fixed_in_version_go_db:
                    for go_version in [entry['go_version'] for entry in golang_report]:
                        go_v = Version.parse(go_version)
                        if fixed_in_version.major == go_v.major and fixed_in_version.minor == go_v.minor:
                            compatible = True
                            break
                    if compatible:
                        break
            if compatible:
                self.compatible_cves.add(cve_id)
            else:
                logger.warning(f"{cve_id} is not compatible with in-use golang versions for {ocp_version}.")

            cve_table.add_row([flaw_id, cve_id, comp_in_title, _fmt(fixed_in_version_go_db), compatible])

        self._logger.info(f"Found {len(golang_cves_fixed_in)} golang CVEs")
        self._logger.info(f"\n{cve_table}")

        def compare(b1, b2):
            # compare function for a bug
            # sort by CVE ID
            # CVEIDs created earlier would be at the top
            # CVE-2023-XYZ takes precedence over CVE-2024-XYZ
            if b1.cve_id < b2.cve_id:
                return 1
            elif b1.cve_id == b2.cve_id:
                # if CVE IDs are same, sort by created date
                # bugs created earlier would be at the top
                return b1.created_days_ago() < b2.created_days_ago()
            else:
                return -1
        bugs = sorted(bugs, key=functools.cmp_to_key(compare))

        table = PrettyTable()
        table.align = "l"
        table.field_names = ["Jira ID", "CVE", "pscomponent", "Status", "Age (days)"]
        bugs = [b for b in bugs if b.cve_id in golang_cves_fixed_in]
        for b in bugs:
            table.add_row([b.id, b.cve_id, b.whiteboard_component, b.status, b.created_days_ago()])
        self._logger.info(f"Found {len(bugs)} golang trackers in Jira")
        self._logger.info(f"\n{table}")

        if not self.analyze:
            return

        if not self.fixed_in_nvrs:
            invalid_bugs = sorted(b.id for b in bugs if b.cve_id not in golang_cves_fixed_in)
            bugs = [b for b in bugs if b.cve_id in golang_cves_fixed_in]
            if invalid_bugs:
                logger.warning("These bugs do not look like golang compiler cves and we cannot determine their "
                               "fixed-in-golang-version. Run with --fixed-in-nvr to specify the golang compiler nvr(s) "
                               f"the corresponding CVEs are fixed in: {invalid_bugs}")

            incompatible_fix_bugs = sorted(b.id for b in bugs if b.cve_id not in self.compatible_cves)
            bugs = [b for b in bugs if b.cve_id in self.compatible_cves]
            if incompatible_fix_bugs:
                logger.warning(f"These bugs have fixed-in versions incompatible with in-use golang versions for {ocp_version}. "
                               "Run with --fixed-in-nvr to specify the golang compiler nvr(s) the corresponding "
                               f"CVEs are fixed in: {incompatible_fix_bugs}")

        if not bugs:
            exit(0)

        fixed_bugs, unfixed_bugs, updated_bugs = [], [], []
        for bug in bugs:
            component = bug.whiteboard_component
            logger.info(f"{bug.id} has security component: {component}")
            fixed, comment = False, ''

            if self.fixed_in_nvrs:
                if component == constants.GOLANG_BUILDER_CVE_COMPONENT:
                    fixed, comment = await self.is_fixed_golang_builder(bug)
                else:
                    fixed, comment = await self.is_fixed_rpm(bug, component)
            else:
                tracker_fixed_in = golang_cves_fixed_in.get(bug.cve_id)
                if not tracker_fixed_in:
                    self._logger.warning(
                        f"Could not determine fixed in versions for {bug.id}. Ignoring it for now")
                    continue
                logger.info(f"{bug.id} is fixed in: {[str(v) for v in tracker_fixed_in]}")

                if component == constants.GOLANG_BUILDER_CVE_COMPONENT:
                    fixed, comment = await self.is_fixed_golang_builder(bug, tracker_fixed_in=tracker_fixed_in)
                else:
                    fixed, comment = await self.is_fixed_rpm(bug, component, tracker_fixed_in=tracker_fixed_in)

            art_ticket_message = ''
            if self.art_jira:
                art_ticket_message = f"Refer to {self.art_jira} for details."
            if fixed:
                if self.update_tracker:
                    comment = f"{comment} {art_ticket_message}"
                    self.move_to_qa_and_comment(bug, comment)
                    updated_bugs.append(bug.id)
                fixed_bugs.append(bug.id)
            elif self.force_update_tracker:
                self.move_to_qa_and_comment(bug, art_ticket_message)
                updated_bugs.append(bug.id)
            else:
                unfixed_bugs.append(bug.id)

        if fixed_bugs:
            self._logger.info(f'Bugs determined to be fixed with current builds: {sorted(fixed_bugs)}')
        if unfixed_bugs:
            self._logger.info(f'Bugs determined to be not fixed with current builds: {sorted(unfixed_bugs)}')
        if updated_bugs:
            self._logger.info(f'{"DRY-RUN: " if self.dry_run else ""}Bugs updated: {sorted(updated_bugs)}')


@cli.command("find-bugs:golang", short_help="Find, analyze and update golang tracker bugs")
@click.option("--pullspec", default=None,
              help="Pullspec of release payload to check against. If not provided, latest accepted nightly will be "
                   "used")
@click.option("--cve-id", "cve_ids", multiple=True,
              help="CVE ID(s) (example: CVE-2024-1394) that trackers should be fetched for")
@click.option("--tracker-id", "tracker_ids", multiple=True,
              help="Only fetch and analyze these tracker(s) e.g. OCPBUGS-2024")
@click.option("--analyze", is_flag=True,
              help="Analyze if found bugs are fixed")
@click.option("--fixed-in-nvr", "fixed_in_nvrs", multiple=True,
              help="golang NVR(s) (example: golang-1.20.12-2.el9_3) that given CVE(s) are fixed in")
@click.option("--update-tracker",
              is_flag=True,
              default=False,
              help="If a tracker bug is fixed then comment with analysis and move to ON_QA")
@click.option("--force-update-tracker",
              is_flag=True,
              default=False,
              help="Move to ON_QA even if tracker bug is not determined to be fixed")
@click.option("--component", "components", multiple=True,
              help="Only operate on trackers for these JIRA Bug components e.g. openshift-golang-builder-container")
@click.option('--art-jira', help='Related ART Jira ticket for reference e.g. ART-1234')
@click.option("--exclude-bug-statuses", default=None,
              help="Exclude bugs in these statuses. By default Verified,ON_QA are excluded."
                   "Pass empty string to include all open statuses or comma separated list of statuses to exclude")
@click.option("--dry-run", "--noop",
              is_flag=True,
              default=False,
              help="Don't change anything")
@click.pass_obj
@click_coroutine
async def find_bugs_golang_cli(runtime: Runtime, pullspec: str, cve_ids, tracker_ids,
                               analyze: bool, fixed_in_nvrs, update_tracker: bool,
                               force_update_tracker: bool, components: List[str],
                               art_jira: str, exclude_bug_statuses: str,
                               dry_run: bool):
    """Find golang security tracker bugs in jira and determine if they are fixed.
    Trackers are fetched from the OCPBUGS project

    Pass in --cve-id to fetch bugs for specific CVE ID(s). Multiple CVE IDs can be
    specified e.g. "--cve-id CVE-A --cve-id CVE-B"

    Pass in --analyze to determine if found bugs are fixed.

    By default, fixed-in-golang-version is fetched from flaw bug metadata

    Pass in --fixed-in-nvr to specify that given CVE(s) are fixed in given golang NVR(s). Multiple NVRs can be
    specified e.g. "--fixed-in-nvr NVR1 --fixed-in-nvr NVR2"

    Bugs are compared with latest builds in `stream` assembly by default. Pass --assembly to specify.

    For openshift-golang-builder-container build, use --pullspec <payload_pullspec> to determine if fixed for builds in
    given pullspec

    Note: rpm trackers cannot be processed if --pullspec is used, for that rely on --assembly.

    --update-tracker: If a tracker bug is fixed then comment on it with analysis and move the bug state to ON_QA

    --force-update-tracker: Move to ON_QA even if tracker bug is not determined to be fixed. This is useful in case of
    bugs like openshift-golang-builder where we want to move the bug to ON_QA after or close to when mass rebuild is
    triggered.

    --component: Only operate on trackers for these JIRA Bug components e.g. openshift-golang-builder-container.
    This is useful when using with --force-update-tracker to only operate on certain bugs.

    --exclude-bug-statuses: Exclude bugs in these statuses. If you wanted to analyze and report on all open bugs
    you would pass --exclude-bug-statuses="". Only used when not using --tracker-id.

    # Fetch open golang tracker bugs in 4.14

    $ elliott -g openshift-4.14 find-bugs:golang

    # Determine if currently open golang tracker bugs are fixed in stream assembly.

    $ elliott -g openshift-4.14 find-bugs:golang --analyze

    # Specify fixed in nvrs

    $ elliott -g openshift-4.14 find-bugs:golang --analyze --fixed-in-nvr golang-1.20.12-2.el9_3 --fixed-in-nvr
    golang-1.20.12-1.el8_9

    # Determine if currently open golang tracker bugs are fixed against 4.14.8 assembly.

    $ elliott -g openshift-4.14 --assembly 4.14.8 find-bugs:golang --analyze

    # Update bugs which are determined to be fixed

    $ elliott -g openshift-4.14 find-bugs:golang --analyze --update-tracker --art-jira ART-1234 --dry-run

    """
    if exclude_bug_statuses is not None:
        if tracker_ids:
            raise click.BadParameter("Cannot use --exclude-bug-statuses with --tracker-id")
        exclude_bug_statuses = exclude_bug_statuses.split(',') if exclude_bug_statuses else []
    else:
        exclude_bug_statuses = ['Verified', 'ON_QA']

    if fixed_in_nvrs:
        if not analyze:
            raise click.BadParameter('Cannot use --fixed-in-nvr without --analyze')
        if not cve_ids and not tracker_ids:
            raise click.BadParameter('Cannot use --fixed-in-nvr without --cve-id or --tracker-id')

    if update_tracker and not analyze:
        raise click.BadParameter('Cannot use --update-tracker without --analyze')

    if force_update_tracker and not update_tracker:
        raise click.BadParameter('Cannot use --force-update-tracker without --update-tracker')

    if force_update_tracker and not cve_ids:
        raise click.BadParameter('Cannot use --force-update-tracker without --cve-id')

    runtime.initialize(mode="both")
    if runtime.assembly != 'stream' and analyze and not pullspec:
        releases_config = runtime.get_releases_config()
        release_name = get_release_name_for_assembly(runtime.group, releases_config, runtime.assembly)
        pullspec = f'{pyartcd_constants.RELEASE_IMAGE_REPO}:{release_name}-x86_64'
        LOGGER.info(f"Will try to use x86 payload for analysis: {pullspec}. To override, use --pullspec")

    if cve_ids:
        cve_ids = tuple(c.upper() for c in cve_ids)

    # We want to load all configs for rpms, include disabled so microshift is included
    cli = FindBugsGolangCli(
        runtime=runtime,
        pullspec=pullspec,
        cve_ids=cve_ids,
        components=components,
        tracker_ids=tracker_ids,
        analyze=analyze,
        fixed_in_nvrs=fixed_in_nvrs,
        update_tracker=update_tracker,
        force_update_tracker=force_update_tracker,
        art_jira=art_jira,
        exclude_bug_statuses=exclude_bug_statuses,
        dry_run=dry_run
    )
    await cli.run()
