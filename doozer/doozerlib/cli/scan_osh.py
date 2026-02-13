import asyncio
import re
from enum import Enum
from typing import Optional

import click
import koji
import requests
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import cprint
from jira import JIRA, Issue
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.image import ImageMetadata
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.runtime import Runtime

SCAN_RESULTS_URL_TEMPLATE = "https://cov01.lab.eng.brq2.redhat.com/osh/task/{task_id}/log/{nvr}/scan-results-imp.js"

MESSAGE = """
One or more software security scan issues have been detected for the package *{package_name}*.

*Important scan results*: [html|https://cov01.lab.eng.brq2.redhat.com/osh/task/{scan_id}/log/{nvr}/scan-results-imp.html] / [json|https://cov01.lab.eng.brq2.redhat.com/osh/task/{scan_id}/log/{nvr}/scan-results-imp.js] (Check 'defects')
*All scan results*: https://cov01.lab.eng.brq2.redhat.com/osh/task/{scan_id}
*Build record*: [{nvr}|https://brewweb.engineering.redhat.com/brew/buildinfo?buildID={brew_build_id}]
*Upstream commit*: {upstream_commit}

Once these issues are addressed, the Bug can be closed. But a new one will be opened if new issues are found in the next build.

For information on triaging these issues and dispositioning this ticket, please review the [policy document for SAST Scanning Jira Tickets|https://docs.google.com/document/d/1GsT3tvw5qf3sOSAkTSVWnMIO6vzIuirhsVhhsbK_qf0].

For other questions please reach out to @release-artists in #forum-ocp-art.

_(Ticket title and description are maintained by the bot. Please do not modify)_
"""


class BuildType(Enum):
    IMAGE = 1
    RPM = 2


class JiraStatus(Enum):
    OPEN = 1
    CLOSED = 2


class ScanOshCli:
    def __init__(
        self,
        runtime: Runtime,
        last_brew_event: int,
        dry_run: bool,
        check_triggered: Optional[bool],
        all_builds: Optional[bool],
        create_jira_tickets: Optional[bool],
    ):
        self.tag_rhel_mapping = {}
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
        self.brew_distgit_mapping = self.get_brew_distgit_mapping()
        self.brew_distgit_mapping_rpms = self.get_brew_distgit_mapping(kind=BuildType.RPM)
        self.jira_target_version = self.get_target_version()

        # Initialize runtime and brewhub session
        self.runtime.initialize(mode="both", disabled=True, clone_distgits=False, clone_source=False)
        self.koji_session = koji.ClientSession(self.runtime.group_config.urls.brewhub)

        # Initialize JIRA client
        self.jira_client: JIRA = self.runtime.build_jira_client()

        # record run as unstable if any errors occur
        self.unstable = False

    # Static functions
    @staticmethod
    def is_jira_workflow_component_disabled(meta):
        """
        Check if the OCPBUGS workflow is enabled in ocp-build-data image meta config
        """
        flag = meta.config.external_scanners.sast_scanning.jira_integration.enabled

        # Disable only if specifically defined in config
        return flag in [False, "False", "false", "no"]

    def is_jira_workflow_group_enabled(self):
        """
        Check if JIRA workflow is disabled for this particular OCP version in group.yml
        """
        flag = self.runtime.group_config.external_scanners.sast_scanning.jira_integration.enabled
        return flag in [True, "True", "true", "yes"]

    @staticmethod
    async def get_untriggered_nvrs(nvrs):
        """
        So we might have already triggered a scan for the same NVR. If this flag is enabled, we want to check
        if it has and not re-trigger it again.
        But please note that this will take a lot of time since it has to run for all 200+ packages.
        """

        @exectools.limit_concurrency(16)
        async def run_get_untriggered_nvrs(nvr):
            rc, _, _ = await exectools.cmd_gather_async(f"osh-cli find-tasks --nvr {nvr}", check=False)

            return None if rc == 0 else nvr

        tasks = []
        for nvr in nvrs:
            tasks.append(run_get_untriggered_nvrs(nvr))

        nvrs = await asyncio.gather(*tasks)

        untriggered_nvrs = [nvr for nvr in nvrs if nvr]

        return untriggered_nvrs

    # Retry
    @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
    def search_issues(self, query):
        return self.jira_client.search_issues(query)

    # Get functions
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
        cmd = (
            f"doozer --load-disabled --disable-gssapi -g {self.runtime.group} images:print --show-base --show-non-release "
            f"--short '{{component}}: {{name}}'"
        )

        if kind == BuildType.RPM:
            cmd = (
                f"doozer --load-disabled --disable-gssapi -g {self.runtime.group} rpms:print "
                f"--short '{{component}}: {{name}}'"
            )

        _, output, _ = exectools.cmd_gather(cmd)
        result = []
        for line in output.splitlines():
            result.append(line.split(": "))

        dict_data = {}
        for line in result:
            if line:
                dict_data[line[0]] = line[1]

        return dict_data

    def get_scan_info(self, nvr: str):
        """
        Find the last successful scan for a particular package and retrieve its ID
        """
        # Eg: osh-cli find-tasks --regex ose-network-tools-container-v4.15
        # Needs to be updated once https://issues.redhat.com/browse/OSH-376 is completed
        cmd = f"osh-cli find-tasks --nvr {nvr} --latest"

        _, result, _ = exectools.cmd_gather(cmd)

        tasks = result.strip().split("\n")

        if tasks == [""]:
            # Skip components that have no scans
            return None, None

        # This will always be a list with len 1, since we're using --latest flag
        task_id = tasks[0]

        cmd = f"osh-cli task-info {task_id}"
        self.runtime.logger.info(f"[osh-cli] Running: {cmd}")

        _, result, _ = exectools.cmd_gather(cmd)
        result_parsed_yml = yaml.safe_load(result.replace(" =", ":"))

        return task_id, result_parsed_yml["state_label"]

    def get_distgit_name_from_brew(self, kind: BuildType, brew_package_name: str) -> str:
        """
        Returns the distgit name from the brew nvr

        :param kind: Metadata kind
        :param brew_package_name: Name of the brew package
        """
        if kind == BuildType.IMAGE:
            return self.brew_distgit_mapping.get(brew_package_name)

        return self.brew_distgit_mapping_rpms.get(brew_package_name)

    def get_scan_defects(self, scan_id, scan_nvr):
        url = f"{SCAN_RESULTS_URL_TEMPLATE.format(task_id=scan_id, nvr=scan_nvr)}?format=raw"
        self.runtime.logger.info(f"Checking Scan defects for url: {url}")

        scan_response = requests.get(url)

        return scan_response.json()["defects"]

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

    @staticmethod
    def get_scan_details_from_ticket(description):
        pattern = r"\*All scan results\*: https://cov01.lab.eng.brq2.redhat.com/osh/task/(?P<task_id>[0-9]+)"
        match = re.search(pattern=pattern, string=description)
        task_id = None
        if match:
            task_id = match.group("task_id")

        pattern = r"\*Build record\*: \[(?P<nvr>[a-z0-9~._-]+)\|.+"
        match = re.search(pattern=pattern, string=description)
        nvr = None
        if match:
            nvr = match.group("nvr")

        return task_id, nvr

    def get_jira_issues(self, summary, status):
        if status == JiraStatus.OPEN:
            condition = "in"
        else:
            condition = "not in"

        query = (
            f"project={self.jira_project} AND ( summary ~ '{summary}' ) AND "
            f"status {condition} ('New', 'Assigned') order by created ASC"
        )
        return [i for i in self.search_issues(query) if i.fields.summary == summary]

    def get_non_art_upstream_repo_and_jira_component(self, brew_info):
        """
        Get upstream repo URL for images or RPMs not built by ART
        """
        # The source URL can be retrieved only with getBuild
        distgit_url = self.koji_session.getBuild(brew_info["nvr"])["source"]

        upstream_repo_url = distgit_url

        if distgit_url.startswith("git:"):
            distgit_url = distgit_url.replace("git:", "git+https:")
            upstream_repo_url = distgit_url.split("git+")[1]

            # Find JIRA component
        product_config = self.runtime.get_product_config()
        component_mapping = product_config.bug_mapping.components
        component_entry = component_mapping[brew_info["package_name"]]
        potential_component = component_entry.issue_component

        if not potential_component:
            potential_component = "Unknown"

        return upstream_repo_url, potential_component

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

    def get_commit_text(self, upstream_repo_url, nvr):
        if "github.com" in upstream_repo_url:
            # ART built components will have the GitHub URL which are almost all images and a handful of RPMs
            upstream_github_commit = self.get_upstream_git_commit(nvr=nvr)
            jira_commit_text = f"[{upstream_github_commit}|{upstream_repo_url}/commit/{upstream_github_commit}]"
        else:
            # non-ART components will have distgit URLs
            distgit_url = upstream_repo_url
            distgit_commit = distgit_url.split("#")[-1]

            jira_commit_text = f"[{distgit_commit}|{distgit_url}]"

        return jira_commit_text

    # Other util functions
    def does_scan_issues_exist(self, task_id: str, nvr: str) -> bool:
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

    def is_latest_scan_different_from_previous(
        self, latest_scan_id, latest_scan_nvr, previous_scan_id, previous_scan_nvr
    ):
        # If this is the first scan, then there won't be a previous one
        # Or there is no previous successful scan
        if not previous_scan_id:
            return True

        latest_scan_defects = self.get_scan_defects(scan_id=latest_scan_id, scan_nvr=latest_scan_nvr)
        previous_scan_defects = self.get_scan_defects(scan_id=previous_scan_id, scan_nvr=previous_scan_nvr)

        for defect in latest_scan_defects:
            if defect not in previous_scan_defects:
                self.runtime.logger.info(
                    "Detected that the scan of the latest build has new issues, compared to the previous one"
                )
                return True

        return False

    def generate_commands(self, nvrs) -> Optional[list]:
        """
        Generates the list of osh-cli commands that we use to trigger the scans.
        """
        cmds = []

        for nvr in nvrs:
            # default priority is 10, set it to 9 so that OCP scans don't cause
            # long scan delays for other OSH users
            cmd = f"osh-cli mock-build --config=auto --priority=9 --profile custom-ocp --nvr {nvr} --nowait"
            cmds.append(cmd)
            self.runtime.logger.debug(f"Generating command: {cmd}")

        return cmds

    # Main workflow functions
    def categorize_components(self, components):
        components_with_issues = []
        components_without_issues = []

        for component in components:
            build = component["build"]
            task_id = component["osh_task_id"]
            nvr = build["nvr"]

            # Check if the scan result has any issues reported.
            if self.does_scan_issues_exist(task_id=task_id, nvr=nvr):
                components_with_issues.append(component)
            else:
                components_without_issues.append(component)

        return components_with_issues, components_without_issues

    @exectools.limit_concurrency(16)
    async def get_latest_task_ids(self, nvr, package_name):
        # There is a bug in the osh-cli that sometimes, tasks are not in the order of created time
        # But task IDs are generated based on that order, so sorting in descending order and get the latest task for
        # a particular NVR
        cmd = f"osh-cli find-tasks --nvr {nvr} --latest"

        _, result, _ = await exectools.cmd_gather_async(cmd)
        tasks = result.strip().split("\n")

        # This will always be a list with len 1, since we're using --latest flag
        return (package_name, None) if tasks == [""] else (package_name, tasks[0])

    @exectools.limit_concurrency(16)
    async def get_osh_task_state(self, task_id, package_name):
        cmd = f"osh-cli task-info {task_id}"

        _, result, _ = await exectools.cmd_gather_async(cmd)
        result_parsed_yml = yaml.safe_load(result.replace(" =", ":"))

        return package_name, result_parsed_yml["state_label"]

    async def process_tasks_osh(self, components):
        components_with_closed_scans = []
        nvrs_to_retrigger_scans = []

        tasks = []
        for component in components.values():
            build = component["build"]
            nvr = build["nvr"]
            package_name = build["package_name"]

            tasks.append(self.get_latest_task_ids(nvr, package_name))
        result = await asyncio.gather(*tasks)

        components_with_task_ids = {}
        for package_name, task_id in result:
            component = components[package_name]
            if not task_id:
                nvr = component["build"]["nvr"]
                # Does not have a valid scan
                nvrs_to_retrigger_scans.append(nvr)
                continue

            component["osh_task_id"] = task_id

            components_with_task_ids[package_name] = component

        tasks = []
        for component in components_with_task_ids.values():
            build = component["build"]
            task_id = component["osh_task_id"]
            package_name = build["package_name"]

            tasks.append(self.get_osh_task_state(task_id=task_id, package_name=package_name))
        result = await asyncio.gather(*tasks)

        for package_name, state in result:
            if state == "OPEN":
                # Scan is running. Let's check again on the next run
                continue

            if state == "FAILED":
                # Skip for now, verify with OSH if it's an issue or not
                self.runtime.logger.error(f"Looks like scan has failed for package: {package_name}")
                continue

            components_with_closed_scans.append(components[package_name])

        return components_with_closed_scans, nvrs_to_retrigger_scans

    def create_update_jira_ticket(self, component):
        build = component["build"]
        metadata = component["metadata"]
        task_id = component["osh_task_id"]
        summary = component["jira_search_summary"]
        kind = component["kind"]
        nvr = build["nvr"]

        if metadata:
            # ART is building this component
            # Upstream repo URL from image config
            upstream_repo_url = metadata.config.content.source.git.web

            # Returns project and component name
            _, potential_component = metadata.get_jira_info()
        else:
            # ART is NOT building this component
            # If no GitHub url, mention the distgit URL instead
            upstream_repo_url, potential_component = self.get_non_art_upstream_repo_and_jira_component(build)

        jira_commit_text = self.get_commit_text(upstream_repo_url, nvr)

        description = MESSAGE.format(
            package_name=build["package_name"],
            scan_id=task_id,
            nvr=nvr,
            brew_build_id=build["id"],
            upstream_commit=jira_commit_text,
        )

        fields = {
            "project": {"key": f"{self.jira_project}"},
            "issuetype": {"name": "Bug"},
            "versions": [{"name": self.jira_target_version}],  # Affects Version/s
            "components": [{"name": potential_component}],
            "security": {"id": "11697"},  # Restrict to Red Hat Employee
            "summary": summary,
            "description": description,
            "labels": ["art:sast", f"art:package:{build['package_name']}"],
        }

        # Get the latest OPEN tickets (if any) for the component
        # Find if there is already a "OPEN" ticket for this component.
        # A ticket is said to be "OPEN" if status <= 'Assigned'.
        # If we detect a net-new security issue, we should open a new ticket instead of updating the one
        # that is in the process of being fixed.
        open_issues = self.get_jira_issues(summary, JiraStatus.OPEN)

        if len(open_issues) == 0:
            # Check if there are any closed tickets

            closed_issues = self.get_jira_issues(summary, JiraStatus.CLOSED)

            if len(closed_issues) == 0:
                # This is the first time we are raising a ticket for this component
                # Create a new JIRA ticket
                if not self.dry_run:
                    _: Issue = self.jira_client.create_issue(fields)
                else:
                    self.runtime.logger.info(
                        f"[DRY RUN]: Would have created a new bug in {self.jira_project} "
                        f"JIRA project with fields {fields}"
                    )
                    return

            # There are closed issues.
            try:
                previous_ticket = closed_issues.pop()  # Returned in LIFO (last-in, first-out) order.
            except IndexError:
                # If it's an empty list, pop will fail
                self.runtime.logger.info(f"No previous ticket exists for NVR: {nvr}")
                return

            # Check if the current NVR is in the ticket description
            if nvr in previous_ticket.fields.description:
                # Looks like it's the same NVR
                return

            # Not the same NVR
            # Pass the description from the previous NVR to be processed
            self.runtime.logger.info(f"Retrieving OSH task ID and NVR, from the previous ticket: {previous_ticket.key}")
            previous_task_id, previous_nvr = self.get_scan_details_from_ticket(
                description=previous_ticket.fields.description
            )

            # We should always be able to retrieve the scan ID from the ticket
            assert previous_task_id is not None
            assert previous_nvr is not None

            if previous_task_id == task_id:
                # It's the same task
                return

            # Check the diff
            if not self.is_latest_scan_different_from_previous(
                latest_scan_id=task_id,
                latest_scan_nvr=nvr,
                previous_scan_id=previous_task_id,
                previous_scan_nvr=previous_nvr,
            ):
                # No diff
                return

            # There is a diff
            # Since it's different, raise a ticket and link the old ticket to the new one
            if not self.dry_run:
                issue: Issue = self.jira_client.create_issue(fields)
                self.runtime.logger.info(f"Created new issue: {issue.key}")

                # Create a "Relates to" link to the previously closed ticket, if it exists
                self.jira_client.create_issue_link(
                    type="Related",
                    inwardIssue=previous_ticket.key,
                    outwardIssue=issue.key,
                )

                self.runtime.logger.info(f"Linked {previous_ticket.key} to {issue.key}")
            else:
                self.runtime.logger.info(
                    f"[DRY RUN]: Would have created a new bug in {self.jira_project} JIRA project with fields {fields}"
                )
                if previous_ticket:
                    self.runtime.logger.info(f"Would have linked {previous_ticket.key} to the issue")
                return

        elif len(open_issues) == 1:
            issue = open_issues.pop()
            self.runtime.logger.info(f"A {self.jira_project} ticket already exists: {issue.key}")

            # Check if the current NVR is in the ticket description
            if nvr in issue.fields.description:
                # Looks like it's the same NVR
                return

            if kind == BuildType.RPM:
                # Get the build in the currently "open" ticket
                _, build_nvr_on_ticket = self.get_scan_details_from_ticket(description=issue.fields.description)

                assert build_nvr_on_ticket is not None

                build_on_ticket = self.koji_session.getBuild(build_nvr_on_ticket)

                if int(build_on_ticket["build_id"]) > int(build["build_id"]):
                    # We do not want to update the ticket with an older build, even if its of a later OCP version
                    # A higher build ID will mean a newer build
                    self.runtime.logger.info(f"Ticket {issue.key} has the newer build, compared to {build['nvr']}")
                    return

            if not self.dry_run:
                # Keep notify as False since this description will constantly be updated everytime there's a
                # new build
                issue.update(fields={"description": fields["description"]}, notify=False)
                self.runtime.logger.info(f"The fields of {issue.key} has been updated to {fields}")
            else:
                self.runtime.logger.info(
                    f"[DRY RUN]: Would have updated {issue.key} with new description: {fields['description']}"
                )

        else:
            self.runtime.logger.error(f"More than one JIRA ticket exists: {open_issues}")

    async def ocp_bugs_workflow(self, components: dict):
        # Get components that has successful scans. If no scans, retrigger
        components_with_closed_scans, nvrs_to_retrigger_scans = await self.process_tasks_osh(components)

        # Trigger scans for ones that do not exist
        self.trigger_scans(nvrs_to_retrigger_scans)

        components_with_issues, components_without_issues = self.categorize_components(components_with_closed_scans)
        for component in components_without_issues:
            summary = component["jira_search_summary"]
            kind = component["kind"]
            osh_task_id = component["osh_task_id"]

            if kind == BuildType.IMAGE:
                # Close open issues, if they exist, for Images.
                # For RPMs, its OCP version independent, so skipping
                open_issues = self.get_jira_issues(summary, status=JiraStatus.OPEN)

                # [asdas] Auto-ticket closing feature disabled.
                # for open_issue in open_issues:
                #     if not self.dry_run:
                #         self.jira_client.transition_issue(open_issue.key, "Closed")
                #         self.runtime.logger.info(f"Closed issue {open_issue.key}")
                #     else:
                #         self.runtime.logger.info(f"Would have closed issue: {open_issue.key}")
                for open_issue in open_issues:
                    self.runtime.logger.info(
                        f"Would have closed issue: {open_issue.key}, since no issues found in OSH task {osh_task_id}"
                    )

        for c in components_with_issues:
            try:
                self.create_update_jira_ticket(c)
            except Exception as e:
                self.runtime.logger.error(f"Error while creating/updating JIRA ticket for component {c}: {e}")
                self.unstable = True

    def trigger_scans(self, nvrs: list):
        cmds = self.generate_commands(nvrs=nvrs)

        for cmd in cmds:
            message = f"Ran command: {cmd}"

            if not self.dry_run:
                exectools.fire_and_forget(self.runtime.cwd, cmd)
            else:
                message = "[DRY RUN] " + message

            self.runtime.logger.info(message)

        self.runtime.logger.info(f"Total number of build scans kicked off: {len(cmds)}")

    def brew_candidate_workflow(self):
        """
        Collect all the builds since the previous run. If this is the first run, get all the builds in the candidate
        tags
        """
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
        # Get the list of excluded package names from group.yml
        excluded_components = (
            self.runtime.group_config.external_scanners.sast_scanning.jira_integration.exclude_components
        )
        self.runtime.logger.debug(
            f"Retrieved components that have been excluded, from group.yml: {excluded_components}"
        )

        for build in builds:
            # Skip rhcos for now
            if build["nvr"].startswith("rhcos"):
                self.runtime.logger.debug(f"Skipping RHCOS builds. Scan is not triggered for {build['nvr']}")
                continue

            # Exclude all test builds
            if "assembly.test" in build["nvr"]:
                self.runtime.logger.debug(f"Skipping test build: {build['nvr']}")
                continue

            # Exclude CI builds
            if build["nvr"].startswith("ci-openshift"):
                self.runtime.logger.debug(f"Skipping CI build: {build['nvr']}")
                continue

            if build["package_name"] in excluded_components:
                self.runtime.logger.debug(f"Skipping excluded component: {build['nvr']}")
                continue

            nvrs.append(build["nvr"])

        nvr_brew_mapping = [(build["nvr"], build["create_event"]) for build in builds]

        if nvr_brew_mapping:
            self.runtime.logger.info(f"NVRs to trigger scans for {nvr_brew_mapping}")

        if builds:
            latest_event_id = nvr_brew_mapping[-1][1]

            nvrs_for_scans = nvrs

            # Return back the latest brew event ID
            cprint(latest_event_id)
        else:
            self.runtime.logger.warning(f"No new NVRs have been found since last brew event: {self.last_brew_event}")
            return None

        return nvrs_for_scans

    async def trigger_scans_workflow(self):
        """
        Trigger scans incrementally by looking at the brew event
        """
        nvrs_for_scans = self.brew_candidate_workflow()

        if not nvrs_for_scans:
            self.runtime.logger.info("No new builds to scan")
            return

        if self.check_triggered:
            nvrs_for_scans = await self.get_untriggered_nvrs(nvrs_for_scans)

        # Trigger the scans
        self.trigger_scans(nvrs_for_scans)

    async def process_data_for_ocpbugs_workflow(self):
        # Get all builds from candidate tags
        build_package_mapping = {}

        for tag in self.brew_tags:
            # Get all the latest builds from the candidate tags
            builds = self.get_tagged_latest(tag=tag)
            self.runtime.logger.info(f"Retrieved all latest builds from tag {tag}")

            for build in builds:
                build_package_mapping[build["package_name"]] = {
                    "build": build,
                    "tag": tag,
                }

        # Get the list of excluded package names from group.yml
        excluded_components = (
            self.runtime.group_config.external_scanners.sast_scanning.jira_integration.exclude_components
        )
        self.runtime.logger.info(f"Retrieved components that have been excluded, from group.yml: {excluded_components}")

        # Components that are not excluded
        components_under_consideration = {}
        for component in build_package_mapping.values():
            tag = component["tag"]
            build = component["build"]
            nvr = build["nvr"]
            package_name = build["package_name"]

            # Exclude all test builds
            if "assembly.test" in nvr:
                self.runtime.logger.debug(f"Skipping test build: {nvr}")
                continue

            # Exclude RHCOS builds
            if nvr.startswith("rhcos"):
                self.runtime.logger.debug(f"Skipping rhcos build: {nvr}")
                continue

            if nvr.startswith("ci-openshift"):
                self.runtime.logger.debug(f"Skipping CI builds: {nvr}")
                continue

            # Exclude all bundle builds
            if "bundle-container" in nvr or "metadata-container" in nvr:
                self.runtime.logger.debug(f"Skipping bundle build: {nvr}")
                continue

            # Exclude components in the "excluded list" in group.yml
            if package_name in excluded_components:
                self.runtime.logger.debug(f"Skipping build: {nvr} (since it's in excluded components)")
                continue

            kind: BuildType = BuildType.IMAGE if "-container" in nvr and nvr.endswith(".stream") else BuildType.RPM
            component["kind"] = kind

            distgit_name = self.get_distgit_name_from_brew(kind=kind, brew_package_name=build["package_name"])
            metadata = None
            # Either ART is building this component or not
            if distgit_name:
                # ART is building this component
                # Metadata can be that of images or RPMs.
                if kind == BuildType.IMAGE:
                    metadata: ImageMetadata = self.runtime.image_map.get(distgit_name)
                elif kind == BuildType.RPM:
                    metadata: RPMMetadata = self.runtime.rpm_map.get(distgit_name)

                # To make sure that the image metadata is correctly loaded at runtime.
                # For ART managed components, this should always work
                # Choosing to assert, instead of assuming
                assert metadata is not None

                # Exclude components that are disabled in component metadata yml
                if self.is_jira_workflow_component_disabled(metadata):
                    self.runtime.logger.info(
                        f"Skipping OCPBUGS creation for distgit {distgit_name} since disabled in component metadata"
                    )
                    continue

            if kind == BuildType.IMAGE:
                jira_search_summary = f"{self.version} SAST scan issues for {build['package_name']}"
            else:
                rhel_version = self.tag_rhel_mapping[tag]
                jira_search_summary = f"{rhel_version} SAST scan issues for {build['package_name']}"

            component["metadata"] = metadata
            component["jira_search_summary"] = jira_search_summary
            component["kind"] = kind

            components_under_consideration[package_name] = component

        await self.ocp_bugs_workflow(components_under_consideration)

    async def run(self):
        # Setup brew tags
        tags = self.runtime.get_errata_config()["brew_tag_product_version_mapping"].keys()
        for tag in tags:
            major, minor = self.runtime.get_major_minor_fields()
            self.brew_tags.append(tag.format(MAJOR=major, MINOR=minor))
        self.runtime.logger.info(f"Retrieved candidate tags: {self.brew_tags}")

        # Check if the OCP version is enabled for raising Jira tickets
        if not self.is_jira_workflow_group_enabled():
            self.runtime.logger.info(f"Skipping SAST workflow since not enabled in group.yml for {self.version}")
            return

        # Trigger scans workflow
        await self.trigger_scans_workflow()

        if not self.create_jira_tickets:
            return

        # JIRA integration workflow

        # Create rhel mapping
        for tag in self.brew_tags:
            pattern = r"rhaos-\d.\d+(-ironic){0,1}-(?P<rhel_version>rhel-\d+)-(candidate|hotfix)"
            match = re.search(pattern=pattern, string=tag)
            rhel_version = None
            if match:
                rhel_version = match.group("rhel_version")

            assert rhel_version is not None

            self.tag_rhel_mapping[tag] = rhel_version
        self.runtime.logger.info(f"Created RHEL-brew-tag mapping: {self.tag_rhel_mapping}")

        await self.process_data_for_ocpbugs_workflow()

        if self.unstable:
            raise Exception("One or more errors occurred during execution. Details in log.")


@cli.command("images:scan-osh", help="Trigger scans for builds with brew event IDs greater than the value specified")
@click.option("--since", required=False, help="Builds after this brew event. If empty, latest builds will retrieved")
@click.option(
    "--dry-run", default=False, is_flag=True, help="Do not trigger anything, but only print build operations."
)
@click.option(
    "--check-triggered",
    required=False,
    is_flag=True,
    default=False,
    help="Triggers scans for NVRs only after checking if they haven't already",
)
@click.option("--all-builds", required=False, is_flag=True, default=False, help="Check all builds in candidate tags")
@click.option(
    "--create-jira-tickets",
    required=False,
    is_flag=True,
    default=False,
    help="Create OCPBUGS ticket for a package if scan issues exist",
)
@pass_runtime
@click_coroutine
async def scan_osh(
    runtime: Runtime, since: str, dry_run: bool, check_triggered: bool, all_builds: bool, create_jira_tickets: bool
):
    cli_pipeline = ScanOshCli(
        runtime=runtime,
        last_brew_event=int(since) if since else None,
        dry_run=dry_run,
        check_triggered=check_triggered,
        all_builds=all_builds,
        create_jira_tickets=create_jira_tickets,
    )
    await cli_pipeline.run()
