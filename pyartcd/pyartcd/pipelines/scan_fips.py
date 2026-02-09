"""
For this command to work, https://github.com/openshift/check-payload binary has to exist in PATH and run as root
This job is deployed on ART cluster
"""

import json
import os
import re
import sys
from collections import defaultdict
from typing import Optional

import click
from artcommonlib import exectools
from artcommonlib.constants import ACTIVE_OCP_VERSIONS
from artcommonlib.jira_config import JIRA_DOMAIN, JIRA_SERVER_URL

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.jira_client import JIRAClient
from pyartcd.runtime import Runtime

JIRA_PROJECT = "OCPBUGS"

INITIAL_SLACK_MSG = ":warning: FIPS scan has failed for some builds. Please verify (Triage <https://art-docs.engineering.redhat.com/sop/triage-fips/|docs>)"
FAILING_BUILDS_MSG_HEADER = "The listed versions of the following packages did not pass the FIPS scan:"
PACKAGES_WITHOUT_TICKET_MSG_HEADER = "Report of missing/incomplete Jira tickets:"


class ScanFips:
    def __init__(self, runtime: Runtime, data_path: str, all_images: bool, nvrs: Optional[list]):
        self.runtime = runtime
        self.data_path = data_path
        self.nvrs = nvrs
        self.all_images = all_images

        #  Call JIRAClient.from_url() directly because Runtime.new_jira_client() does not work currently
        self.jira_client = JIRAClient.from_url(server_url=JIRA_SERVER_URL, token_auth=os.environ["JIRA_TOKEN"])

        # Setup slack client
        self.slack_client = self.runtime.new_slack_client()
        self.slack_client.bind_channel("#art-release")

    @staticmethod
    def extract_package_name(nvr: str) -> Optional[str]:
        match = re.match(r"(.+)-v4\..*", nvr)
        return match.group(1) if match else None

    @staticmethod
    def construct_failing_packages_report(
        failing_packages: dict[str, set[str]], package_ticket_details: dict[str, tuple[str, set[str]]]
    ) -> str:
        """
        Construct Slack message describing packages failing the FIPS scan and their versions
        """
        report_msg_parts = [FAILING_BUILDS_MSG_HEADER]

        for package, versions in failing_packages.items():
            if package not in package_ticket_details:
                report_msg_parts.append(f"`{package}` (No ticket raised)")
            else:
                ticket_url, _ = package_ticket_details[package]
                report_msg_parts.append(f"`{package}` (<{ticket_url}|Ticket> already raised)")
            report_msg_parts.append(f"• {', '.join(versions)}")
            report_msg_parts.append("\n")

        return "\n".join(report_msg_parts)

    @staticmethod
    def construct_packages_without_ticket_report(
        failing_packages: dict[str, set[str]], package_ticket_details: dict[str, tuple[str, set[str]]]
    ) -> str:
        """
        Construct Slack message showing which of the failing builds do not have
        an active JIRA ticket
        """
        report_msg_parts = [PACKAGES_WITHOUT_TICKET_MSG_HEADER]

        for package in failing_packages:
            if package not in package_ticket_details:
                report_msg_parts.append(f"• `{package}` does not have a corresponding active ticket")
            else:
                ticket_url, ticket_versions = package_ticket_details[package]
                versions_not_in_ticket = failing_packages[package].difference(ticket_versions)
                if not versions_not_in_ticket:
                    continue
                use_plural = len(versions_not_in_ticket) > 1
                report_msg_parts.append(
                    f"• {'Versions' if use_plural else 'Version'} {', '.join(versions_not_in_ticket)} of `{package}` {'are' if use_plural else 'is'} not included in the <{ticket_url}|ticket>"
                )

        return "\n".join(report_msg_parts) if len(report_msg_parts) > 1 else ""

    def get_package_ticket_details(self, failing_packages: dict[str, set[str]]) -> dict[str, tuple[str, set[str]]]:
        """
        Returns a dict of (failing) packages with the URL and affected versions of their
        corresponding active tickets
        """

        def get_active_fips_tickets(self):
            query = f'project = "{JIRA_PROJECT}" AND labels = "art:fips" \
            AND status in ("New", "Modified", "ASSIGNED", "POST")'
            active_tickets = self.jira_client._client.search_issues(jql_str=query)
            return active_tickets

        active_fips_tickets = get_active_fips_tickets(self)

        package_to_ticket = {}
        for ticket in active_fips_tickets:
            for label in ticket.fields.labels:
                if label.startswith("art:package:"):
                    package_name = label[len("art:package:") :]
                    package_to_ticket[package_name] = ticket

        package_ticket_details = {}
        for package in failing_packages:
            if package in package_to_ticket:
                ticket = package_to_ticket[package]
                ticket_versions = set(map(lambda version: version.name[:-2], ticket.fields.versions))
                package_ticket_details[package] = (f"{JIRA_DOMAIN}/browse/{ticket.key}", ticket_versions)

        return package_ticket_details

    async def message_on_slack(self, failing_packages: dict[str, set[str]]):
        """
        Send 2 messages to a new Slack thread - report of packages (and their versions)
        that are currently failing the FIPS scan, and a report of packages which don't have
        a jira ticket raised (or their ticket doesn't list all the versions currently failing)
        """
        package_ticket_details = self.get_package_ticket_details(failing_packages)

        failing_packages_report_msg = self.construct_failing_packages_report(failing_packages, package_ticket_details)
        packages_without_ticket_report_msg = self.construct_packages_without_ticket_report(
            failing_packages, package_ticket_details
        )

        await self.slack_client.say_in_thread(message=INITIAL_SLACK_MSG)
        await self.slack_client.say_in_thread(message=failing_packages_report_msg)
        if packages_without_ticket_report_msg:
            await self.slack_client.say_in_thread(message=packages_without_ticket_report_msg)

    async def run(self):
        results = {}
        failing_packages = defaultdict(set)

        for version in ACTIVE_OCP_VERSIONS:
            cmd = [
                "doozer",
                "--group",
                f"openshift-{version}",
                "--data-path",
                self.data_path,
                "images:scan-fips",
            ]

            if self.nvrs and self.all_images:
                raise Exception("Cannot specify both --nvrs and --all-images")

            if self.nvrs:
                cmd.extend(["--nvrs", ",".join(self.nvrs)])

            if self.all_images:
                cmd.append("--all-images")

            _, result, _ = await exectools.cmd_gather_async(cmd, stderr=True)

            if result:
                result_json = json.loads(result)
                results.update(result_json)

                package_names = map(lambda nvr: self.extract_package_name(nvr), list(result_json.keys()))
                for package_name in package_names:
                    failing_packages[package_name].add(version)

            if self.all_images:
                # Clean all the images, if we are checking for all images since this mode is used on prod only
                # Since this command will be run for all versions, clean after each run will be more efficient. Otherwise
                # the pod storage limit will be reached quite quickly.
                # If on local, and do not want to clean, feel free to comment this function out
                await exectools.cmd_assert_async("podman image prune --all --force")

        self.runtime.logger.info(f"Result: {results}")

        if failing_packages:
            # Post on Slack
            if not self.runtime.dry_run:
                # Exit as error so that we see in the PipelineRun
                self.runtime.logger.error("FIPS issues were found")
                await self.message_on_slack(failing_packages)
            else:
                self.runtime.logger.info("[DRY RUN] Would have messaged slack")
            sys.exit(1)
        else:
            self.runtime.logger.info("No issues")


@cli.command("scan-fips", help="Trigger FIPS check for specified NVRs")
@click.option("--data-path", required=True, help="OCP build data url")
@click.option("--nvrs", required=False, help="Comma separated list to trigger scans for")
@click.option("--all-images", is_flag=True, default=False, help="Scan all latest images in our tags")
@pass_runtime
@click_coroutine
async def scan_osh(runtime: Runtime, data_path: str, all_images: bool, nvrs: str):
    pipeline = ScanFips(
        runtime=runtime,
        data_path=data_path,
        all_images=all_images,
        nvrs=nvrs.split(",") if nvrs else None,
    )
    await pipeline.run()
