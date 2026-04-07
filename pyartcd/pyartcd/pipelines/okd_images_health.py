import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import click
from artcommonlib import exectools
from doozerlib.cli.images_health import DELTA_DAYS, LIMIT_BUILD_RESULTS, ConcernCode
from doozerlib.constants import ART_BUILD_HISTORY_URL

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL, OKD_ENABLED_VERSIONS
from pyartcd.runtime import Runtime

OKD_GROUP_TEMPLATE = "okd-{}"


class ImagesHealthPipeline:
    def __init__(
        self,
        runtime: Runtime,
        versions: str,
        send_to_release_channel: bool,
        send_to_okd_channel: bool,
        data_path: str,
        data_gitref: str,
        image_list: str,
        assembly: str,
    ):
        self.runtime = runtime
        self.versions = versions.split(',') if versions else OKD_ENABLED_VERSIONS
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.send_to_release_channel = send_to_release_channel
        self.send_to_okd_channel = send_to_okd_channel
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.image_list = image_list.split(',') if image_list else []
        self.assembly = assembly if assembly else 'stream'

        self.report = []
        self.slack_client = self.runtime.new_slack_client()
        self.scanned_versions = []
        self.rebase_failures = {}  # version -> {image: {failure_count, url}}

    async def run(self):
        await asyncio.gather(*(self.get_report(v) for v in self.versions))
        await asyncio.gather(*(self.get_rebase_failures(v) for v in self.versions))
        self.runtime.logger.info('Found %s concerns', len(self.report))

        if self.send_to_release_channel:
            for version in self.scanned_versions:
                await self.notify_release_channel(version)

        if self.send_to_okd_channel:
            await self.notify_okd_channel()

    async def get_report(self, version: str) -> Optional[list]:
        # Get doozer report for the given version
        doozer_working = f'{self.doozer_working}-{version}'
        group_param = f'--group=openshift-{version}'
        if self.data_gitref:
            group_param += f'@{self.data_gitref}'

        cmd = [
            'doozer',
            f'--working-dir={doozer_working}',
            f'--data-path={self.data_path}',
            '--variant=okd',
            group_param,
        ]

        if self.image_list:
            cmd.append(f'--images={",".join(self.image_list)}')

        cmd.extend(['images:health', f'--group={OKD_GROUP_TEMPLATE.format(version)}'])

        if self.assembly:
            cmd.append(f'--assembly={self.assembly}')

        _, out, err = await exectools.cmd_gather_async(cmd, stderr=None)
        report = json.loads(out.strip())

        self.runtime.logger.info('images:health output for openshift-%s:\n%s', version, out)
        self.report.extend(report)
        self.scanned_versions.append(version)

    async def get_rebase_failures(self, version: str):
        """
        Fetch OKD rebase failure data from Redis for a specific version.
        Populates self.rebase_failures[version] with {image: {failure_count, url}}.

        Arg(s):
            version (str): OKD version (e.g., "4.21")
        """
        self.rebase_failures[version] = await util.get_rebase_failures(
            version=version,
            branches=['okd-rebase-failure'],
            build_systems=['konflux'],
            logger=self.runtime.logger,
        )

    async def notify_release_channel(self, version):
        """
        Send notifications to version-specific release channel (e.g., #art-release-4-21) for a specific OKD version.
        Uses the same channels as OCP releases.
        Filters out LATEST_BUILD_SUCCEEDED concerns (successful builds not reported).
        Posts parent message with concern count, details in thread.
        Includes rebase failure information from Redis.

        Arg(s):
            version (str): OKD version (e.g., "4.21")
        """
        # Bind to version-specific channel (same as OCP: #art-release-4-21)
        channel = f"#art-release-{version.replace('.', '-')}"
        self.slack_client.bind_channel(channel)

        # Filter concerns for this version, excluding successful builds
        concerns = [
            concern
            for concern in self.report
            if concern.get('group', '') == f'openshift-{version}'
            and concern['code'] != ConcernCode.LATEST_BUILD_SUCCEEDED.value
        ]

        # Get rebase failures for this version
        rebase_failures = self.rebase_failures.get(version, {})

        version_tag = f'`okd-{version}`'
        if self.assembly != 'stream':
            version_tag += f' (assembly `{self.assembly}`)'

        # If no concerns and no rebase failures, report all healthy
        if not concerns and not rebase_failures:
            await self.slack_client.say(f':white_check_mark: All OKD images are healthy for {version_tag}')
            return

        # Build summary message
        summary_parts = []
        if concerns:
            summary_parts.append(self.get_component_tag(concerns))
        if rebase_failures:
            rebase_count = len(rebase_failures)
            summary_parts.append(f'{rebase_count} image{"s" if rebase_count > 1 else ""} with rebase failures')

        # Post parent message with concern count
        issues = '\n- '.join(summary_parts)
        response = await self.slack_client.say(
            f':alert: There are some issues to look into for OKD {version_tag}:\n- {issues}'
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
                report += f'- `{image_name}`: Failed {failure_count} time{"s" if failure_count != 1 else ""}'
                if job_url:
                    report += f' ({self.url_text(job_url, "Last failure job")})'
                report += '\n'

        await self.slack_client.say(report, thread_ts=response['ts'])

    async def notify_okd_channel(self):
        """
        Send aggregated notifications to #art-okd-release channel for all OKD versions.
        Groups concerns and rebase failures by image name across all versions.
        Excludes NEVER_BUILT and LATEST_BUILD_SUCCEEDED concerns.
        """
        self.slack_client.bind_channel('#art-okd-release')

        # Group concerns by image name
        image_concerns = {}
        for concern in self.report:
            if (
                concern['code'] == ConcernCode.NEVER_BUILT.value
                or concern['code'] == ConcernCode.LATEST_BUILD_SUCCEEDED.value
            ):
                # We don't report NEVER_BUILT concerns to art-okd-release. Latest built succeeded is not a concern.
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
                    }
                )

        # If no concerns and no rebase failures, report all healthy
        if not image_concerns and not image_rebase_failures:
            await self.slack_client.say(':white_check_mark: All OKD images are healthy for all monitored releases')
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
            f':alert: There are some issues to look into for OKD builds:\n- {issues}',
            link_build_url=False,
        )

        # Post detailed report in thread
        # Build concerns
        for image_name, concerns in image_concerns.items():
            image_message = f'`{image_name}` (build issues):'
            for concern in concerns:
                image_message += f'\n• {self.get_message_for_okd_channel(concern)}'
            await self.slack_client.say(image_message, thread_ts=response['ts'], link_build_url=False)

        # Rebase failures
        for image_name, failures in sorted(image_rebase_failures.items()):
            image_message = f'`{image_name}` (rebase failures):'
            for failure in failures:
                version = failure['version']
                failure_count = failure['failure_count']
                job_url = failure['url']
                image_message += f'\n• okd-{version}: Failed {failure_count} time{"s" if failure_count != 1 else ""}'
                if job_url:
                    image_message += f' ({self.url_text(job_url, "Last failure job")})'
            await self.slack_client.say(image_message, thread_ts=response['ts'], link_build_url=False)

    def get_message_for_release(self, concern: dict):
        """
        Format a concern message for the release channel (#art-okd-release).

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

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f' - Failing for at least {LIMIT_BUILD_RESULTS} attempts'
            return message

        # ConcernCode.LATEST_ATTEMPT_FAILED
        message += f' - Latest attempt failed ({concern["latest_success_idx"]} attempts since last success)'
        return message

    def get_message_for_okd_channel(self, concern: dict):
        """
        Format a concern message for the general #art-okd-release channel.
        Shows group (version) and failure details with links.

        Arg(s):
            concern (dict): Concern data from doozer images:health
        Return Value(s):
            str: Formatted message with links and details
        """
        code = concern['code']
        group = concern['group']
        # Transform openshift-X.Y to okd-X.Y for display
        okd_group = group.replace('openshift-', 'okd-')

        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime('%Y-%m-%d')
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        art_dash_link = f'{ART_BUILD_HISTORY_URL}/?name=^{concern["image_name"]}$&group={okd_group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=success&outcome=failure'
        logs_link = self.url_text(self.get_logs_url(concern), "logs")

        message = f'{self.url_text(art_dash_link, f"{okd_group}")}: '

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f'more than {LIMIT_BUILD_RESULTS} failures ({logs_link}).'
        else:  # ConcernCode.LATEST_ATTEMPT_FAILED
            message += f'{concern["latest_success_idx"]} failures ({logs_link}).'

        return message

    @staticmethod
    def get_search_url(concern):
        """
        Build the OKD build history search page URL for a component.

        Arg(s):
            concern (dict): Concern data containing image name and group
        Return Value(s):
            str: URL to the build history search page
        """
        image_name = concern['image_name']
        group = concern['group']
        # Transform openshift-X.Y to okd-X.Y for the OKD dashboard
        okd_group = group.replace('openshift-', 'okd-')
        start_date = (datetime.now(timezone.utc) - timedelta(days=DELTA_DAYS)).strftime('%Y-%m-%d')
        end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        return f'{ART_BUILD_HISTORY_URL}/?name=^{image_name}$&group={okd_group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}&outcome=success&outcome=failure'

    @staticmethod
    def get_logs_url(concern):
        """
        Build the OKD build history logs URL for a failed build.

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
    def get_component_tag(report):
        """
        Create a component count tag for the summary message.

        Arg(s):
            report (list or dict): Either a list of concerns or dict of concerns by image
        Return Value(s):
            str: Formatted component count message
        """
        n_components = len(report)

        if n_components > 1:
            return f'{n_components} components have failed'
        else:
            return '1 component has failed'

    def url_text(self, url, text):
        """
        Slack requires URLs to be encoded in a specific way when using the <url|text> format.
        This function ensures that the URL is properly encoded while keeping certain characters safe.

        Arg(s):
            url (str): The URL to encode
            text (str): The display text for the link
        Return Value(s):
            str: Slack-formatted link <url|text>
        """
        try:
            safe_chars = ":/?&=+%.-"  # keep URL structure intact
            safe_url = quote(url, safe=safe_chars)
            return f"<{safe_url}|{text}>"

        except Exception as e:
            self.runtime.logger.warning('invalid URL: %s', e)


@cli.command('okd-images-health')
@click.option('--versions', required=False, default='', help='OCP versions to scan')
@click.option('--send-to-release-channel', is_flag=True, help='If true, send output to #art-release-4-<version>')
@click.option('--send-to-okd-channel', is_flag=True, help='If true, send aggregated notification to #art-okd-release')
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
@click.option('--assembly', required=False, default='', help='Assembly override')
@pass_runtime
@click_coroutine
async def okd_images_health(
    runtime: Runtime,
    versions: str,
    send_to_release_channel: bool,
    send_to_okd_channel: bool,
    data_path: str,
    data_gitref: str,
    image_list: str,
    assembly: str,
):
    await ImagesHealthPipeline(
        runtime,
        versions,
        send_to_release_channel,
        send_to_okd_channel,
        data_path,
        data_gitref,
        image_list,
        assembly,
    ).run()
