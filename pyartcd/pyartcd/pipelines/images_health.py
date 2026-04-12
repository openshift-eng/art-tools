import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import click
from artcommonlib import exectools
from artcommonlib.constants import ACTIVE_OCP_VERSIONS
from doozerlib.cli.images_health import DELTA_DAYS, LIMIT_BUILD_RESULTS, ConcernCode
from doozerlib.constants import ART_BUILD_HISTORY_URL

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.jira_client import JIRAClient
from pyartcd.runtime import Runtime

_LOGGER = logging.getLogger(__name__)

FAILURE_CODES = {ConcernCode.FAILING_AT_LEAST_FOR.value, ConcernCode.LATEST_ATTEMPT_FAILED.value}


class ImagesHealthPipeline:
    def __init__(
        self,
        runtime: Runtime,
        versions: str,
        send_to_release_channel: bool,
        send_to_forum_ocp_art: bool,
        data_path: str,
        data_gitref: str,
        image_list: str,
        assembly: str,
        sync_jira: bool = False,
    ):
        self.runtime = runtime
        self.versions = versions.split(',') if versions else ACTIVE_OCP_VERSIONS
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.send_to_release_channel = send_to_release_channel
        self.send_to_forum_ocp_art = send_to_forum_ocp_art
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.image_list = image_list.split(',') if image_list else []
        self.assembly = assembly
        self._sync_jira = sync_jira
        self.report = []
        self.slack_client = self.runtime.new_slack_client()
        self.scanned_versions = []
        self.rebase_failures = {}  # version -> {image: {failure_count, url, build_system}}
        self.jira_tickets: dict[tuple[str, str], str] = {}  # (image_name, group) -> ticket key

    async def run(self):
        await asyncio.gather(*(self.get_report(v) for v in self.versions))
        await asyncio.gather(*(self.get_rebase_failures(v) for v in self.versions))
        self.runtime.logger.info('Found %s concerns', len(self.report))

        if self._sync_jira:
            self.sync_jira()

        if self.send_to_release_channel:
            for version in self.scanned_versions:
                await self.notify_release_channel(version)

        if self.send_to_forum_ocp_art:
            await self.notify_forum_ocp_art()

    async def get_report(self, version: str) -> Optional[list]:
        doozer_working = f'{self.doozer_working}-{version}'
        # Check if automation is frozen for current group
        if not await util.is_build_permitted(
            version,
            doozer_working=str(doozer_working),
            data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        ):
            self.runtime.logger.info('Skipping %s scan as the group is frozen', version)
            return

        self.scanned_versions.append(version)

        # Get doozer report for the given version
        group_param = f'--group=openshift-{version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'
        cmd = [
            'doozer',
            f'--working-dir={doozer_working}',
            f'--data-path={self.data_path}',
            group_param,
        ]
        if self.image_list:
            cmd.append(f'--images={",".join(self.image_list)}')
        cmd.append('images:health')

        if self.assembly:
            cmd.append(f'--assembly={self.assembly}')

        _, out, err = await exectools.cmd_gather_async(cmd, stderr=None)
        report = json.loads(out.strip())
        self.runtime.logger.info('images:health output for openshift-%s:\n%s', version, out)
        self.report.extend(report)

    async def get_rebase_failures(self, version: str):
        """
        Fetch OCP rebase failure data from Redis for a specific version.
        Checks both Brew and Konflux build systems.
        Populates self.rebase_failures[version] with {image: {failure_count, url, build_system}}.

        Arg(s):
            version (str): OCP version (e.g., "4.18")
        """
        self.rebase_failures[version] = await util.get_rebase_failures(
            version=version,
            branches=['rebase-failure'],
            build_systems=['brew', 'konflux'],
            logger=self.runtime.logger,
        )

    def sync_jira(self):
        jira_client = self.runtime.new_jira_client()

        # Build current failure set keyed by (image_name, group)
        current_failures: dict[tuple[str, str], dict] = {}
        for concern in self.report:
            if concern['code'] in FAILURE_CODES:
                key = (concern['image_name'], concern['group'])
                current_failures[key] = concern

        # Fetch all open image-build-failure tickets
        jql = 'project = ART AND labels = "art:image-build-failure" AND statusCategory != Done'
        open_tickets = jira_client.search_issues(jql)

        # Index open tickets by (image_name, group) extracted from labels
        ticket_index: dict[tuple[str, str], object] = {}
        for ticket in open_tickets:
            image_name = None
            group = None
            for label in ticket.fields.labels:
                if label.startswith("art:package:"):
                    image_name = label[len("art:package:") :]
                elif label.startswith("art:group:"):
                    group = label[len("art:group:") :]
            if image_name and group:
                ticket_index[(image_name, group)] = ticket

        # Record existing tickets and create missing ones
        scanned_groups = {f"openshift-{v}" for v in self.scanned_versions}
        for (image_name, group), concern in current_failures.items():
            if (image_name, group) in ticket_index:
                ticket = ticket_index[(image_name, group)]
                self.jira_tickets[(image_name, group)] = ticket.key
                _LOGGER.info("Ticket already exists for %s (%s): %s", image_name, group, ticket.key)
                continue
            new_ticket = self._create_failure_ticket(jira_client, concern)
            self.jira_tickets[(image_name, group)] = new_ticket.key

        # Close resolved tickets (with scope guard)
        for (image_name, group), ticket in ticket_index.items():
            if group not in scanned_groups:
                _LOGGER.info("Skipping close check for %s (%s): version not scanned", image_name, group)
                continue
            if self.image_list and image_name not in self.image_list:
                _LOGGER.info("Skipping close check for %s (%s): image not in scan list", image_name, group)
                continue
            if (image_name, group) not in current_failures:
                _LOGGER.info("Closing resolved ticket %s for %s (%s)", ticket.key, image_name, group)
                jira_client.close_task(ticket.key)

    def _create_failure_ticket(self, jira_client: JIRAClient, concern: dict):
        image_name = concern['image_name']
        group = concern['group']
        code = concern['code']
        nvr = concern.get('latest_failed_nvr', 'N/A')
        logs_url = self.get_logs_url(concern)
        search_url = self.get_search_url(concern)

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            category = f"Persistent failure (>{LIMIT_BUILD_RESULTS} consecutive failures)"
        else:
            category = f"Recent failure ({concern.get('latest_success_idx', '?')} attempts since last success)"

        last_good_url = concern.get("latest_successful_task_url", "")

        description = (
            f"Image build failure detected by images-health.\n\n"
            f"*Image:* {image_name}\n"
            f"*Group:* {group}\n"
            f"*NVR:* {nvr}\n"
            f"*Category:* {category}\n"
            f"*Build history:* {search_url}\n"
            f"*Error logs:* {logs_url}\n"
        )
        if last_good_url:
            description += f"*Last successful build:* {last_good_url}\n"

        labels = [
            "art:image-build-failure",
            f"art:package:{image_name}",
            f"art:group:{group}",
        ]

        ticket = jira_client.create_issue(
            project="ART",
            issue_type="Task",
            summary=f"Image build failure: {image_name} ({group})",
            description=description,
            labels=labels,
            components=["daily-ops"],
        )
        _LOGGER.info("Created ticket %s for %s (%s)", ticket.key, image_name, group)
        return ticket

    def _jira_link(self, concern: dict) -> str:
        key = self.jira_tickets.get((concern['image_name'], concern['group']))
        if not key:
            return ''
        jira_url = f'{self.runtime.config["jira"]["url"]}/browse/{key}'
        return f' | {self.url_text(jira_url, key)}'

    async def notify_release_channel(self, version):
        self.slack_client.bind_channel(version)

        concerns = [
            concern
            for concern in self.report
            if concern.get('group', '') == f'openshift-{version}'
            and concern['code'] != ConcernCode.LATEST_BUILD_SUCCEEDED.value
        ]

        # Get rebase failures for this version
        rebase_failures = self.rebase_failures.get(version, {})

        version_tag = f'`openshift-{version}`'
        if self.assembly != 'stream':
            version_tag += f' (assembly `{self.assembly}`)'

        # If no concerns and no rebase failures, report all healthy
        if not concerns and not rebase_failures:
            await self.slack_client.say(f':white_check_mark: All images are healthy for {version_tag}')
            return

        # Build summary message
        summary_parts = []
        if concerns:
            summary_parts.append(self.get_component_tag(concerns))
        if rebase_failures:
            rebase_count = len(rebase_failures)
            summary_parts.append(f'{rebase_count} image{"s" if rebase_count > 1 else ""} with rebase failures')

        # Post parent message
        issues = '\n- '.join(summary_parts)
        response = await self.slack_client.say(
            f':alert: There are some issues to look into for {version_tag}:\n- {issues}'
        )

        # Post detailed report in thread
        report = ''

        # Add build concerns
        if concerns:
            report += '*Build Issues:*\n'
            for concern in concerns:
                report += f'{self.get_message_for_release(concern)}\n'
            report += '\n'

        # Add rebase failures
        if rebase_failures:
            report += '*Rebase Failures:*\n'
            for image_name, failure_info in sorted(rebase_failures.items()):
                failure_count = failure_info.get('failure_count', 0)
                job_url = failure_info.get('url', '')
                build_system = failure_info.get('build_system', 'unknown')
                report += (
                    f'- `{image_name}` ({build_system}): Failed {failure_count} time{"s" if failure_count != 1 else ""}'
                )
                if job_url:
                    report += f' ({self.url_text(job_url, "Last failure job")})'
                report += '\n'

        await self.slack_client.say(report, thread_ts=response['ts'])

    async def notify_forum_ocp_art(self):
        self.slack_client.bind_channel('#forum-ocp-art')

        # Group build concerns by image name
        image_concerns = {}
        for concern in self.report:
            if (
                concern['code'] == ConcernCode.NEVER_BUILT.value
                or concern['code'] == ConcernCode.LATEST_BUILD_SUCCEEDED.value
            ):
                # We don't report NEVER_BUILT concerns to forum-ocp-art. Latest built succeeded is not a concern.
                continue
            image_name = concern['image_name']
            image_concerns.setdefault(image_name, []).append(concern)

        # Group rebase failures by image name across all versions
        image_rebase_failures = {}
        for version, failures in self.rebase_failures.items():
            for image_name, failure_info in failures.items():
                if image_name not in image_rebase_failures:
                    image_rebase_failures[image_name] = []
                image_rebase_failures[image_name].append(
                    {
                        'version': version,
                        'failure_count': failure_info.get('failure_count', 0),
                        'url': failure_info.get('url', ''),
                        'build_system': failure_info.get('build_system', 'unknown'),
                    }
                )

        # If no concerns and no rebase failures, report all healthy
        if not image_concerns and not image_rebase_failures:
            await self.slack_client.say(':white_check_mark: All images are healthy for all monitored releases')
            return

        # Build summary message
        summary_parts = []
        if image_concerns:
            n_build_issues = len(image_concerns)
            summary_parts.append(f'{n_build_issues} image{"s" if n_build_issues > 1 else ""} with build issues')
        if image_rebase_failures:
            n_rebase_issues = len(image_rebase_failures)
            summary_parts.append(f'{n_rebase_issues} image{"s" if n_rebase_issues > 1 else ""} with rebase failures')

        issues = '\n- '.join(summary_parts)
        response = await self.slack_client.say(
            f':alert: There are some issues to look into for Openshift builds:\n- {issues}',
            link_build_url=False,
        )

        # Post detailed report in thread
        # Build concerns
        for image_name, concerns in image_concerns.items():
            image_message = f'`{image_name}` (build issues):'
            for concern in concerns:
                image_message += f'\n• {self.get_message_for_forum(concern)}'
            await self.slack_client.say(image_message, thread_ts=response['ts'], link_build_url=False)

        # Rebase failures
        for image_name, failures in sorted(image_rebase_failures.items()):
            image_message = f'`{image_name}` (rebase failures):'
            for failure in failures:
                version = failure['version']
                failure_count = failure['failure_count']
                job_url = failure['url']
                build_system = failure['build_system']
                image_message += (
                    f'\n• openshift-{version} ({build_system}): '
                    f'Failed {failure_count} time{"s" if failure_count != 1 else ""}'
                )
                if job_url:
                    image_message += f' ({self.url_text(job_url, "Last failure job")})'
            await self.slack_client.say(image_message, thread_ts=response['ts'], link_build_url=False)

    def get_message_for_release(self, concern: dict):
        """
        Format a concern message for the release channel (#art-release-4-X).

        Arg(s):
            concern (dict): Concern data from doozer images:health
        Return Value(s):
            str: Formatted message with links and details
        """
        code = concern['code']
        image_name = concern['image_name']

        # No build history link if never built
        if code == ConcernCode.NEVER_BUILT.value:
            return f'- `{image_name}`: No builds attempted during last {DELTA_DAYS} days'

        # Include search page link for this component
        search_url = self.get_search_url(concern)
        message = f'- `{image_name}`: {self.url_text(search_url, "Build history")}'

        # Add logs link for failures
        if code in [ConcernCode.LATEST_ATTEMPT_FAILED.value, ConcernCode.FAILING_AT_LEAST_FOR.value]:
            logs_url = self.get_logs_url(concern)
            message += f' | {self.url_text(logs_url, "Latest failure logs")}'
            message += self._jira_link(concern)

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f' - Failing for at least {LIMIT_BUILD_RESULTS} attempts'
            return message

        # ConcernCode.LATEST_ATTEMPT_FAILED
        message += f' - Latest attempt failed ({concern["latest_success_idx"]} attempts since last success)'
        return message

    def get_message_for_forum(self, concern: dict):
        code = concern['code']
        group = concern['group']

        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime('%Y-%m-%d')
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        art_dash_link = f'{ART_BUILD_HISTORY_URL}/?name=^{concern["image_name"]}$&group={group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=success&outcome=failure'
        logs_link = self.url_text(self.get_logs_url(concern), "logs")

        message = f'{self.url_text(art_dash_link, f"{group}")}: '

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f'more than {LIMIT_BUILD_RESULTS} failures ({logs_link}).'

        else:  # ConcernCode.LATEST_ATTEMPT_FAILED
            message += f'{concern["latest_success_idx"]} failures ({logs_link}).'

        message += self._jira_link(concern)
        return message

    @staticmethod
    def get_logs_url(concern):
        """
        Build the ART build history logs URL for a failed build.

        Arg(s):
            concern (dict): Concern data containing build failure details
        Return Value(s):
            str: URL to the build logs
        """
        dt = datetime.fromisoformat(concern['latest_failed_build_time'])
        formatted = dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        logs_url = f'{ART_BUILD_HISTORY_URL}/logs?nvr={concern["latest_failed_nvr"]}&record_id={concern["latest_failed_build_record_id"]}&after={formatted}'
        return logs_url

    @staticmethod
    def get_search_url(concern):
        """
        Build the ART build history search page URL for a component.

        Arg(s):
            concern (dict): Concern data containing image name and group
        Return Value(s):
            str: URL to the build history search page
        """
        image_name = concern['image_name']
        group = concern['group']
        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime('%Y-%m-%d')
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        return f'{ART_BUILD_HISTORY_URL}/?name=^{image_name}$&group={group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=success&outcome=failure'

    @staticmethod
    def get_component_tag(report):
        n_components = len(report)

        if n_components > 1:
            return f'{n_components} components have failed!'
        else:
            return '1 component has failed!'

    def url_text(self, url, text):
        """
        Slack requires URLs to be encoded in a specific way when using the <url|text> format.
        This function ensures that the URL is properly encoded while keeping certain characters safe.
        """

        try:
            safe_chars = ":/?&=+%.-"  # keep URL structure intact
            safe_url = quote(url, safe=safe_chars)
            return f"<{safe_url}|{text}>"

        except Exception as e:
            self.runtime.logger.warning('invalid URL: %s', e)


@cli.command('images-health')
@click.option('--versions', required=False, default='', help='OCP versions to scan')
@click.option('--send-to-release-channel', is_flag=True, help='If true, send output to #art-release-4-<version>')
@click.option('--send-to-forum-ocp-art', is_flag=True, help='"If true, send notification to #forum-ocp-art')
@click.option(
    '--data-path',
    required=False,
    default=OCP_BUILD_DATA_URL,
    help='ocp-build-data fork to use (e.g. assembly definition in your own fork)',
)
@click.option('--data-gitref', required=False, default='', help='Doozer data path git [branch / tag / sha] to use')
@click.option(
    '--image-list',
    required=False,
    help='Comma/space-separated list to include/exclude per --image-build-strategy (e.g. ironic,hypershift)',
)
@click.option(
    '--assembly',
    required=False,
    help='(Optional) override the runtime assembly name',
)
@click.option(
    '--sync-jira/--no-sync-jira', default=True, help='Create/close Jira tickets for build failures (default: enabled)'
)
@pass_runtime
@click_coroutine
async def images_health(
    runtime: Runtime,
    versions: str,
    send_to_release_channel: bool,
    send_to_forum_ocp_art: bool,
    data_path: str,
    data_gitref: str,
    image_list: str,
    assembly: str,
    sync_jira: bool,
):
    await ImagesHealthPipeline(
        runtime,
        versions,
        send_to_release_channel,
        send_to_forum_ocp_art,
        data_path,
        data_gitref,
        image_list,
        assembly,
        sync_jira=sync_jira,
    ).run()
