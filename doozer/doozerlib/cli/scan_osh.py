import re
import click
import koji
import asyncio
import requests
import yaml
from enum import Enum
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.runtime import Runtime
from doozerlib.exectools import fire_and_forget, cmd_gather_async, limit_concurrency, cmd_gather
from doozerlib.util import cprint
from doozerlib.image import ImageMetadata
from doozerlib.rpmcfg import RPMMetadata
from typing import Optional
from jira import JIRA, Issue
from tenacity import retry, stop_after_attempt, wait_fixed
from doozerlib.rpm_utils import parse_nvr

SCAN_RESULTS_URL_TEMPLATE = "https://cov01.lab.eng.brq2.redhat.com/osh/task/{task_id}/log/{nvr}/scan-results-imp.js"

MESSAGE = """
One or more software security scan issues have been detected for the package *{package_name}*.

*Important scan results*: [html|https://cov01.lab.eng.brq2.redhat.com/osh/task/{scan_id}/log/{nvr}/scan-results-imp.html] / [json|https://cov01.lab.eng.brq2.redhat.com/osh/task/{scan_id}/log/{nvr}/scan-results-imp.js] (Check 'defects')
*All scan results*: https://cov01.lab.eng.brq2.redhat.com/osh/task/{scan_id}
*Build record*: [{nvr}|https://brewweb.engineering.redhat.com/brew/buildinfo?buildID={brew_build_id}]
*Upstream commit*: [{upstream_git_commit}|{upstream_repo_url}/commit/{upstream_git_commit}]

Once these issues are addressed, the Bug can be closed. But a new one will be opened if new issues are found in the next build.

For information on triaging these issues and dispositioning this ticket, please review the [policy document for SAST Scanning Jira Tickets|https://docs.google.com/document/d/1GsT3tvw5qf3sOSAkTSVWnMIO6vzIuirhsVhhsbK_qf0].

For other questions please reach out to @release-artists in #forum-ocp-art.
"""


class BuildType(Enum):
    IMAGE = 1
    RPM = 2


class ScanOshCli:
    def __init__(self, runtime: Runtime, last_brew_event: int, dry_run: bool, check_triggered: Optional[bool],
                 all_builds: Optional[bool], create_jira_tickets: Optional[bool], skip_diff_check: Optional[bool]):
        self.runtime = runtime
        self.last_brew_event = last_brew_event
        self.dry_run = dry_run
        self.brew_tags = []
        self.check_triggered = check_triggered
        self.all_builds = all_builds
        self.create_jira_tickets = create_jira_tickets
        self.error_nvrs = []
        self.version = self.runtime.group.split("-")[-1]
        self.jira_project = "OCPBUGS"
        self.skip_diff_check = skip_diff_check
        self.brew_distgit_mapping = self.get_brew_distgit_mapping()
        self.brew_distgit_mapping_rpms = self.get_brew_distgit_mapping(kind=BuildType.RPM)
        self.jira_target_version = self.get_target_version()

        # Initialize runtime and brewhub session
        self.runtime.initialize(mode="both", disabled=True, clone_distgits=False)
        self.koji_session = koji.ClientSession(self.runtime.group_config.urls.brewhub)

        # Initialize JIRA client
        self.jira_client: JIRA = self.runtime.build_jira_client()

    def get_target_version(self):
        """
        Non GA versions have target version set to .0, while the others with .z
        """
        # https://github.com/openshift-eng/art-dashboard-server/tree/master/api#get-apiv1ga-version
        url = "https://art-dash-server-art-dashboard-server.apps.artc2023.pc3z.p1.openshiftapps.com/api/v1/ga-version"

        response = requests.get(url).json()

        if response["status"] == "success":
            latest_ga = response["payload"]
            if int(self.version.split(".")[-1]) <= int(latest_ga.split(".")[-1]):
                return f"{self.version}.z"
            return f"{self.version}.0"

    def get_brew_distgit_mapping(self, kind: BuildType = BuildType.IMAGE):
        # By default lets assume its image
        cmd = f"doozer --load-disabled --disable-gssapi -g {self.runtime.group} images:print --show-base --show-non-release " \
              f"--short '{{component}}: {{name}}'"

        if kind == BuildType.RPM:
            cmd = f"doozer --load-disabled --disable-gssapi -g {self.runtime.group} rpms:print " \
                  f"--short '{{component}}: {{name}}'"

        _, output, _ = cmd_gather(cmd)
        result = []
        for line in output.splitlines():
            result.append(line.split(": "))

        dict_data = {}
        for line in result:
            if line:
                dict_data[line[0]] = line[1]

        return dict_data

    def get_scan_info(self, brew_package_name: str):
        """
        Find the last successful scan for a particular package and retrieve its ID
        """
        # Eg: osh-cli find-tasks --regex ose-network-tools-container-v4.15
        # Needs to be updated once https://issues.redhat.com/browse/OSH-376 is completed
        cmd = f"osh-cli find-tasks --regex {brew_package_name}"

        _, result, _ = cmd_gather(cmd)

        finished_tasks = result.strip().split("\n")

        successful_scan_task_id = None
        successful_scan_nvr = None
        previous_scan_nvr = None
        task_id = None

        # Some components do not have any successful scans, so lets time out and go to the next one if so
        failed_counter = 0
        failed_counter_limit = 20
        for index, task_id in enumerate(finished_tasks):
            if failed_counter >= failed_counter_limit:
                self.runtime.logger.error(f"Reached failed scan limit, skipping component: {brew_package_name}")
                return None, None, None, None
            cmd = f"osh-cli task-info {task_id}"
            self.runtime.logger.info(f"Running: {cmd}")

            _, result, _ = cmd_gather(cmd)

            if "state_label = CLOSED" in result:
                # Reset counter since we found a successful scan
                failed_counter = 0
                if not successful_scan_task_id:
                    # Found the first successful scan.
                    successful_scan_task_id = task_id
                    successful_scan_nvr = yaml.safe_load(result.replace(" =", ":"))["label"]

                    # Need to find the previous successful scan as well
                    continue

                # Found the latest and previous successful scan
                previous_scan_nvr = yaml.safe_load(result.replace(" =", ":"))["label"]  # returns NVR

                if previous_scan_nvr == successful_scan_nvr:
                    # Sometimes we have two scans with the same NVR
                    continue

                # task_id here would be the one of the previous successful scan
                return successful_scan_task_id, successful_scan_nvr, task_id, previous_scan_nvr

            failed_counter += 1

        # If there is no successful scans
        return successful_scan_task_id, successful_scan_nvr, task_id, previous_scan_nvr

    def check_if_scan_issues_exist(self, task_id: str, nvr: str) -> bool:
        """
        ProdSec only wants us to check the scan-results-imp.js file of a scan and see if scan issues are reported.
        """

        url = f"{SCAN_RESULTS_URL_TEMPLATE.format(task_id=task_id, nvr=nvr)}?format=raw"
        self.runtime.logger.info(f"Checking OSH Scan for issues: {url}")

        try:
            response = requests.get(url)
            if len(response.json()["defects"]) > 0:
                return True
        except Exception:
            self.runtime.logger.warning(f"Don't see scan-results-imp.js for {nvr}")

        return False

    def get_distgit_name_from_brew_nvr(self, nvr: str, kind: BuildType) -> str:
        """
            Returns the distgit name from the brew nvr

            :param nvr: Full NVR eg: openshift-enterprise-pod-container-v4.14.0-202310031045.p0.g9ef6de6.assembly.stream
            :param kind: Either IMAGE or RPM
            """
        brew_package_name = self.koji_session.getBuild(buildInfo=nvr, strict=True)["package_name"]

        if kind == BuildType.IMAGE:
            return self.brew_distgit_mapping.get(brew_package_name)

        return self.brew_distgit_mapping_rpms.get(brew_package_name)

    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    def search_issues(self, query):
        return self.jira_client.search_issues(query)

    def get_scan_defects(self, scan_id, scan_nvr):
        url = f"{SCAN_RESULTS_URL_TEMPLATE.format(task_id=scan_id, nvr=scan_nvr)}?format=raw"
        self.runtime.logger.info(f"Checking Scan defects for url: {url}")

        scan_response = requests.get(url)

        return scan_response.json()["defects"]

    def is_latest_scan_different_from_previous(self, latest_scan_id, latest_scan_nvr, previous_scan_id,
                                               previous_scan_nvr):
        # If this is the first scan, then there won't be a previous one
        # Or there is no previous successful scan
        if not previous_scan_id:
            return True

        latest_scan_defects = self.get_scan_defects(scan_id=latest_scan_id, scan_nvr=latest_scan_nvr)
        previous_scan_defects = self.get_scan_defects(scan_id=previous_scan_id, scan_nvr=previous_scan_nvr)

        for defect in latest_scan_defects:
            if defect not in previous_scan_defects:
                self.runtime.logger.info("Detected that the scan of the latest build has new issues, "
                                         "compared to the previous one")
                return True

        return False

    def has_latest_scan_resolved_all_issues(self, latest_scan_id, latest_scan_nvr, previous_scan_id, previous_scan_nvr):
        """
        Check if the latest scan has resolved all issues from the previous scan
        """
        latest_scan_defects = self.get_scan_defects(scan_id=latest_scan_id, scan_nvr=latest_scan_nvr)
        previous_scan_defects = self.get_scan_defects(scan_id=previous_scan_id, scan_nvr=previous_scan_nvr)

        if not latest_scan_defects and previous_scan_defects:
            # If the previous scan results are not empty but the latest scan results are empty
            return True
        return False

    def get_upstream_git_commit(self, nvr: str):
        # Get "ead7616" from sriov-network-operator-container-v4.15.0-202311171551.p0.gead7616.assembly.stream
        pattern = r".+\.g([a-z0-9]+)\..+"

        # Use re.search to find the match in the filename
        match = re.search(pattern, nvr)

        # Check if a match is found
        if match:
            result = match.group(1)
            return result
        else:
            self.runtime.logger.error("Regex match not found")

    def create_update_ocpbugs_ticket(self, packages: dict):
        """
        Check if an OCPBUGS ticket already exists for a particular package. If it doesn't create one and update
        the contents to point to the latest scan results
        """
        for brew_package_name in packages:
            data = packages[brew_package_name]

            # Check if ticket exits
            summary = f"{self.version} SAST scan issues for {brew_package_name}"

            # Find if there is already a "OPEN" ticket for this component.
            # A ticket is said to be "OPEN" if status <= 'Assigned'.
            # If we detect a net-new security issue, we should open a new ticket instead of updating the one
            # that is in the process of being fixed.
            query = f"project={self.jira_project} AND ( summary ~ '{summary}' ) AND " \
                    "status in ('New', 'Assigned')"
            # Can use open_issues.pop().raw['fields'] to see all the fields for the JIRA issue, to test
            # also jira title search is fuzzy, so we need to check if an issue is really the one we want
            open_issues = [i for i in self.search_issues(query) if i.fields.summary == summary]
            self.runtime.logger.info(f"Issues found with query '{query}': {open_issues}")

            # Check if this is the first time that we are raising the ticket for this component
            query = f"project={self.jira_project} AND ( summary ~ '{summary}' ) AND " \
                    "status not in ('New', 'Assigned')"
            closed_issues = [i for i in self.search_issues(query) if i.fields.summary == summary]
            previous_ticket_id = None
            if not closed_issues and not open_issues:
                # There are no tickets for this component, raise a ticket
                self.skip_diff_check = True
            else:
                self.runtime.logger.info(f"Closed Issues: {closed_issues}")
                try:
                    previous_ticket_id = closed_issues.pop()  # Returned in LIFO (last-in, first-out) order.
                except IndexError:
                    # If it's an empty list, pop will fail
                    # previous_ticket_id is already set to None, so no action needed
                    pass

            upstream_git_commit = self.get_upstream_git_commit(nvr=data["latest_coverity_scan_nvr"])

            description = MESSAGE.format(package_name=brew_package_name,
                                         scan_id=data["latest_coverity_scan"],
                                         nvr=data["latest_coverity_scan_nvr"],
                                         brew_build_id=data["brew_build_id"],
                                         upstream_repo_url=data["upstream_repo_url"],
                                         upstream_git_commit=upstream_git_commit)

            fields = {
                "project": {"key": f"{self.jira_project}"},
                "issuetype": {"name": "Bug"},
                "versions": [{"name": self.jira_target_version}],  # Affects Version/s
                "components": [{"name": data["jira_potential_component"]}],
                "security": {"id": "11697"},  # Restrict to Red Hat Employee
                "summary": summary,
                "description": description
            }

            if len(open_issues) == 0:
                # No tickets exist in ('New', 'Assigned') so we could consider opening a new one
                # Do not reraise a ticket if the diff has not changed, but we would still need to update the ticket
                # with the new build and scan
                if not self.skip_diff_check and not self.is_latest_scan_different_from_previous(
                        latest_scan_id=data["latest_coverity_scan"],
                        latest_scan_nvr=data["latest_coverity_scan_nvr"],
                        previous_scan_id=data["previous_scan_id"],
                        previous_scan_nvr=data["previous_scan_nvr"]):
                    self.runtime.logger.info(f"No new defects found for package {data['package_name']}")
                    continue

                # Report the latest and previous scan on the ticket as a comment
                comment = f"This ticket was raised because ART automation found new issues in the " \
                          f"[latest completed scan|{SCAN_RESULTS_URL_TEMPLATE.format(task_id=data['latest_coverity_scan'], nvr=data['latest_coverity_scan_nvr'])}], " \
                          f"when compared to the [the previous one|{SCAN_RESULTS_URL_TEMPLATE.format(task_id=data['previous_scan_id'], nvr=data['previous_scan_nvr'])}]"

                # Create a new JIRA ticket
                if self.dry_run:
                    self.runtime.logger.info(f"[DRY RUN]: Would have created a new bug in {self.jira_project} "
                                             f"JIRA project with fields {fields}")
                    self.runtime.logger.info(f"Would have commented: {comment}")
                    continue

                issue: Issue = self.jira_client.create_issue(
                    fields
                )

                self.runtime.logger.info(f"Created a new issue {issue.key}")

                # Create a "Relates to" link to the previously closed ticket, if it exists
                if previous_ticket_id:
                    self.jira_client.create_issue_link(
                        type="Related",
                        inwardIssue=previous_ticket_id.key,
                        outwardIssue=issue.key,
                    )

                    self.runtime.logger.info(f"Linked {previous_ticket_id.key} to {issue.key}")

                if not self.skip_diff_check:
                    # https://jira.readthedocs.io/examples.html#comments
                    self.jira_client.add_comment(issue.key, comment)

            elif len(open_issues) == 1:
                issue = open_issues.pop()

                if self.has_latest_scan_resolved_all_issues(
                        latest_scan_id=data["latest_coverity_scan"],
                        latest_scan_nvr=data["latest_coverity_scan_nvr"],
                        previous_scan_id=data["previous_scan_id"],
                        previous_scan_nvr=data["previous_scan_nvr"]):
                    if not self.dry_run:
                        self.jira_client.transition_issue(issue.key, "Closed")
                        self.runtime.logger.info(f"Closed issue {issue.key}")
                    else:
                        self.runtime.logger.info(f"[DRY RUN]: Would have closed ticket {issue.key}")

                else:
                    # Update existing JIRA ticket if there is a change
                    self.runtime.logger.info(f"A {self.jira_project} ticket already exists: {issue.key}")

                    if not self.dry_run:
                        # Keep notify as False since this description will constantly be updated everytime there's a
                        # new build
                        issue.update(
                            fields={"description": fields["description"]},
                            notify=False)
                        self.runtime.logger.info(f"The fields of {issue.key} has been updated to {fields}")
                    else:
                        self.runtime.logger.info(f"[DRY RUN]: Would have updated {issue.key} with new description: "
                                                 f"{fields['description']}")

            else:
                self.runtime.logger.error(f"More than one JIRA ticket exists: {open_issues}")

    def get_packages_with_scan_issues(self, mapping: dict):
        """
        Check the latest scan of a particular package for a version (which we already found) and check if
        scan issues exist. Collect those and return back.

        :param mapping: Eg: {'ose-network-tools-container': {'package_name_with_version': 'ose-network-tools-container-v4.15.0-',
                                                           'nvr': 'ose-network-tools-container-v4.15.0-202310170244.p0.gdf85e45.assembly.stream',
                                                           'package_name': 'ose-network-tools-container',
                                                           'latest_coverity_scan': '337873'}
        }
        """

        nvrs_with_scan_issues = {}

        # Check if scan issues detected for a NVR
        for brew_package_name, data in mapping.items():
            if not data["latest_coverity_scan"]:
                self.runtime.logger.info(f"No successful scan found for package {brew_package_name}")
                continue

            if self.check_if_scan_issues_exist(task_id=data["latest_coverity_scan"],
                                               nvr=data["latest_coverity_scan_nvr"]):
                nvrs_with_scan_issues[brew_package_name] = data

            else:
                self.runtime.logger.info(f"No scan issues found for NVR: {data['latest_coverity_scan_nvr']}")

        return nvrs_with_scan_issues

    @staticmethod
    def is_sast_jira_disabled(meta):
        """
        Check if the OCPBUGS workflow is enabled in ocp-build-data image meta config
        """
        flag = meta.config.get("external_scanners", {}).get("sast_scanning", {}).get("jira_integration", {}).get("enabled")

        # Disable only if specifically defined in config
        return flag in [False, "False", "false", "no"]

    async def ocp_bugs_workflow_run(self, brew_components: dict):
        # List of brew components to check
        brew_package_names = []

        # We only care about the value
        for build in brew_components.values():
            nvr = build["nvr"]
            brew_info = build["brew_info"]

            kind: BuildType = BuildType.IMAGE if "container" in nvr else BuildType.RPM

            self.runtime.logger.info(f"[OCPBUGS] Checking build: {nvr} for kind {kind.name}")

            distgit_name = self.get_distgit_name_from_brew_nvr(nvr=nvr, kind=kind)
            if not distgit_name:
                self.runtime.logger.warning(f"Looks like we are not building: {nvr}")

                # Go to the next NVR
                continue

            meta = None
            # Metadata can be that of images or RPMs.
            if kind == BuildType.IMAGE:
                meta: ImageMetadata = self.runtime.image_map.get(distgit_name)
            elif kind == BuildType.RPM:
                meta: RPMMetadata = self.runtime.rpm_map.get(distgit_name)

            # If there is no metadata for the component, the ART might not be building it
            if not meta:
                self.runtime.logger.error(f"Looks like ART is not building image/rpm with distgit name: {distgit_name}")
                continue
            if self.is_sast_jira_disabled(meta):
                self.runtime.logger.info(f"Skipping OCPBUGS creation for distgit {distgit_name} "
                                         f"since disabled in image metadata")
                continue

            # Upstream repo URL from image config
            upstream_repo_url = meta.config.get("content", {}).get("source", {}).get("git", {}).get("web")

            # Returns project and component name
            _, potential_component = meta.get_jira_info()

            nvr = parse_nvr(brew_info["nvr"])

            # Get ose-network-tools-container-v4.15.0 from ose-network-tools-container-v4.15.0-202310170244.p0.gdf85e45.assembly.stream
            pkg_name_w_version = f"{nvr['name']}-v{self.version}.0"

            # For RPMS, the nvr doesn't have a 'v' in them
            # Egs: microshift-4.15.0~0.nightly_2023_11_20_004519-202311200428.p0.ged001ce.assembly.microshift.el9 or
            # openshift-ansible-4.10.0-202310200811.p0.g72c7be6.assembly.stream.el7
            if kind == BuildType.RPM:
                pkg_name_w_version = pkg_name_w_version.replace("v", "")

            self.runtime.logger.info(f"Package name with version: {pkg_name_w_version}")

            brew_package_names.append({
                "package_name_with_version": pkg_name_w_version,
                "nvr": brew_info["nvr"],
                "brew_build_id": brew_info["id"],
                "package_name": brew_info["package_name"],
                "jira_potential_component": potential_component,
                "upstream_repo_url": upstream_repo_url
            })

        # Get latest scan results
        brew_coverity_mapping = {}

        for package in brew_package_names:
            package["latest_coverity_scan"], package["latest_coverity_scan_nvr"], package["previous_scan_id"], \
                package["previous_scan_nvr"] = self.get_scan_info(package["package_name_with_version"])
            brew_coverity_mapping[package["package_name"]] = package

        # Collect packages that have scan issues detected
        packages_with_scan_issues = self.get_packages_with_scan_issues(brew_coverity_mapping)

        # Create or update OCPBUGS ticket
        self.create_update_ocpbugs_ticket(packages_with_scan_issues)

    @staticmethod
    async def get_untriggered_nvrs(nvrs):
        """
        So we might have already triggered a scan for the same NVR. If this flag is enabled, we want to check
        if it has and not re-trigger it again.
        But please note that this will take a lot of time since it has to run for all 200+ packages.
        """

        @limit_concurrency(16)
        async def run_get_untriggered_nvrs(nvr):
            rc, _, _ = await cmd_gather_async(f"osh-cli find-tasks --nvr {nvr}")

            return None if rc == 0 else nvr

        tasks = []
        for nvr in nvrs:
            tasks.append(run_get_untriggered_nvrs(nvr))

        nvrs = await asyncio.gather(*tasks)

        untriggered_nvrs = [nvr for nvr in nvrs if nvr]

        return untriggered_nvrs

    def get_tagged_latest(self, tag):
        """
        Returns the latest RPMs and builds tagged in to the candidate tag received as input
        """
        latest_tagged = self.koji_session.listTagged(tag=tag, latest=True)
        if latest_tagged:
            return latest_tagged
        else:
            return []

    def get_tagged_all(self, tag):
        """
        Returns all the RPMs and builds that are currently in the candidate tag received as input
        """
        latest_tagged = self.koji_session.listTagged(tag=tag, latest=False)
        if latest_tagged:
            return latest_tagged
        else:
            return []

    def generate_commands(self, nvrs) -> Optional[list]:
        """
        Generates the list of osh-cli commands that we use to trigger the scans.
        """
        cmds = []

        cmd_template = "osh-cli mock-build --config={config} --brew-build {nvr} --nowait"
        for nvr in nvrs:
            if "container" in nvr:
                cmds.append(cmd_template.format(config="cspodman", nvr=nvr))

            else:
                if "el7" in nvr:
                    rhel_version = 7
                elif "el8" in nvr:
                    rhel_version = 8
                elif "el9" in nvr or nvr.startswith("rhcos"):
                    rhel_version = 9
                else:
                    self.runtime.logger.error("Invalid RHEL version")
                    raise Exception("Invalid RHEL Version")

                cmds.append(cmd_template.format(config=f"rhel-{rhel_version}-x86_64", nvr=nvr))

        return cmds

    def trigger_scans(self, nvrs: list):
        cmds = self.generate_commands(nvrs=nvrs)

        for cmd in cmds:
            message = f"Ran command: {cmd}"

            if not self.dry_run:
                fire_and_forget(self.runtime.cwd, cmd)
            else:
                message = "[DRY RUN] " + message

            self.runtime.logger.info(message)

        self.runtime.logger.info(f"Total number of build scans kicked off: {len(cmds)}")

    def brew_candidate_workflow(self):
        """
        Collect all the builds since the previous run. If this is the first run, get all the builds in the candidate
        tags
        """
        tags = self.runtime.get_errata_config()["brew_tag_product_version_mapping"].keys()
        for tag in tags:
            major, minor = self.runtime.get_major_minor_fields()
            self.brew_tags.append(tag.format(MAJOR=major, MINOR=minor))

        builds = []

        if self.last_brew_event or self.all_builds:
            for tag in self.brew_tags:
                builds += self.get_tagged_all(tag=tag)

            if self.last_brew_event:
                builds = [build for build in builds if build["create_event"] > self.last_brew_event]
        else:
            # If no --since field is specified, find all the builds that have been tagged into our candidate tags
            for tag in self.brew_tags:
                builds += self.get_tagged_latest(tag=tag)

        # Sort the builds based on the event ID by ascending order so that latest is at the end of the list
        # The reason is that we can have builds of the same component. So we should keep only the
        # latest build NVR in the OCPBUGS ticket
        # Note: create_event is the event on which the build was tagged in the tag and not the build creation time
        builds = sorted(builds, key=lambda x: x["create_event"])

        nvrs = []

        for build in builds:
            # Skip rhcos for now
            if build["nvr"].startswith("rhcos"):
                self.runtime.logger.warning(f"Skipping RHCOS builds. Scan is not triggered for {build['nvr']}")
                continue

            nvrs.append(build["nvr"])

        nvr_brew_mapping = [(build["nvr"], build["create_event"]) for build in builds]

        # To store the final list of NVRs that we will kick off scans for
        nvrs_for_scans = []

        if nvr_brew_mapping:
            self.runtime.logger.info(f"NVRs to trigger scans for {nvr_brew_mapping}")

        if builds:
            latest_event_id = nvr_brew_mapping[-1][1]

            nvrs_for_scans = nvrs

            # Return back the latest brew event ID
            cprint(latest_event_id)
        else:
            self.runtime.logger.warning(
                f"No new NVRs have been found since last brew event: {self.last_brew_event}")
            return None

        return nvrs_for_scans

    async def run(self):
        nvrs_for_scans = self.brew_candidate_workflow()

        if not nvrs_for_scans:
            self.runtime.logger.info("No new builds to scan")
            return

        # For raising JIRA tickets, we ideally need to check them even if they are already scanned
        all_nvrs = nvrs_for_scans

        if self.check_triggered:
            nvrs_for_scans = await self.get_untriggered_nvrs(nvrs_for_scans)

        # Trigger the scans
        self.trigger_scans(nvrs_for_scans)

        # Check if the OCP version is enabled for raising Jira tickets
        if self.runtime.group_config.external_scanners.sast_scanning.jira_integration.enabled not in [True, "True", "true", "yes"]:
            self.runtime.logger.info(f"Skipping OCPBUGS creation workflow since not enabled in group.yml for "
                                     f"{self.version}")
            return

        if self.create_jira_tickets:
            brew_components = {}
            for nvr in all_nvrs:
                if nvr.endswith(".test"):
                    # Exclude all test builds
                    self.runtime.logger.info(f"Skipping test build: {nvr}")
                    continue

                if "bundle-container" in nvr or "metadata-container" in nvr:
                    # Exclude all bundle builds
                    self.runtime.logger.info(f"Skipping bundle build: {nvr}")
                    continue

                brew_info = self.koji_session.getBuild(nvr)
                package_name = brew_info["package_name"]

                # More than one build of a component can be present after the previous run,
                # so lets get just the latest build. The nvrs are ordered from earliest to newest
                brew_components[package_name] = {
                    "nvr": nvr,
                    "brew_info": brew_info
                }

            await self.ocp_bugs_workflow_run(brew_components)


@cli.command("images:scan-osh", help="Trigger scans for builds with brew event IDs greater than the value specified")
@click.option("--since", required=False, help="Builds after this brew event. If empty, latest builds will retrieved")
@click.option("--dry-run", default=False, is_flag=True,
              help="Do not trigger anything, but only print build operations.")
@click.option("--check-triggered", required=False, is_flag=True, default=False,
              help="Triggers scans for NVRs only after checking if they haven't already")
@click.option("--all-builds", required=False, is_flag=True, default=False, help="Check all builds in candidate tags")
@click.option("--create-jira-tickets", required=False, is_flag=True, default=False,
              help="Create OCPBUGS ticket for a package if scan issues exist")
@click.option("--skip-diff-check", required=False, is_flag=True, default=False,
              help="Used along with --create-jira-tickets. Skip checking the diff between current and previous scan")
@pass_runtime
@click_coroutine
async def scan_osh(runtime: Runtime, since: str, dry_run: bool, check_triggered: bool, all_builds: bool,
                   create_jira_tickets: bool, skip_diff_check: bool):
    cli_pipeline = ScanOshCli(runtime=runtime,
                              last_brew_event=int(since) if since else None,
                              dry_run=dry_run,
                              check_triggered=check_triggered,
                              all_builds=all_builds,
                              create_jira_tickets=create_jira_tickets,
                              skip_diff_check=skip_diff_check)
    await cli_pipeline.run()
