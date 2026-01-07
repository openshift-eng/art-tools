import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import quote

import click
from artcommonlib import exectools, redis
from doozerlib.cli.images_health import DELTA_DAYS, LIMIT_BUILD_RESULTS, ConcernCode
from doozerlib.constants import ART_BUILD_HISTORY_URL

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime

OCP4_VERSIONS = [
    "4.21",
    "4.20",
]
OKD_GROUP_TEMPLATE = "okd-{}"


class ImagesHealthPipeline:
    def __init__(
        self,
        runtime: Runtime,
        versions: str,
        send_to_release_channel: str,
        data_path: str,
        data_gitref: str,
        image_list: str,
        assembly: str,
    ):
        self.runtime = runtime
        self.versions = versions.split(',') if versions else OCP4_VERSIONS
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.send_to_release_channel = send_to_release_channel
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
            self.slack_client.bind_channel(self.send_to_release_channel)
            for version in self.scanned_versions:
                await self.notify_release_channel(version)

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
            group_param,
        ]

        if self.image_list:
            cmd.append(f'--images={",".join(self.image_list)}')

        cmd.extend(['images:health', f'--group={OKD_GROUP_TEMPLATE.format(version)}', '--variant=okd'])

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
        try:
            # Get all rebase failure keys for this version
            pattern = f'count:okd-rebase-failure:konflux:{version}:*:failure'
            failure_keys = await redis.get_keys(pattern)

            if not failure_keys:
                self.runtime.logger.info('No rebase failures found in Redis for version %s', version)
                self.rebase_failures[version] = {}
                return

            self.runtime.logger.info('Found %d rebase failure keys for version %s', len(failure_keys), version)

            # Parse failure keys to extract image names and fetch counts + URLs
            version_failures = {}
            for failure_key in failure_keys:
                # Extract image name from key: count:okd-rebase-failure:konflux:4.21:ptp-operator:failure
                parts = failure_key.split(':')
                if len(parts) >= 5:
                    image_name = parts[4]  # ptp-operator
                    url_key = f'count:okd-rebase-failure:konflux:{version}:{image_name}:url'

                    # Fetch failure count and URL
                    failure_count = await redis.get_value(failure_key)
                    job_url = await redis.get_value(url_key)

                    version_failures[image_name] = {
                        'failure_count': int(failure_count) if failure_count else 0,
                        'url': job_url or '',
                    }

            self.rebase_failures[version] = version_failures
            self.runtime.logger.info(
                'Rebase failures for version %s: %s', version, json.dumps(version_failures, indent=2)
            )

        except Exception as e:
            self.runtime.logger.warning('Failed to fetch rebase failures from Redis for version %s: %s', version, e)
            self.rebase_failures[version] = {}

    async def notify_release_channel(self, version):
        """
        Send notifications to #art-okd-release channel for a specific OKD version.
        Filters out LATEST_BUILD_SUCCEEDED concerns (successful builds not reported).
        Posts parent message with concern count, details in thread.
        Includes rebase failure information from Redis.

        Arg(s):
            version (str): OKD version (e.g., "4.21")
        """
        # Filter concerns for this version, excluding successful builds
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
        return f'{ART_BUILD_HISTORY_URL}/?name={image_name}&group={group}'

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
@click.option('--send-to-release-channel', default='#art-okd-release', help='If set, send output to the Slack channel')
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
    send_to_release_channel: str,
    data_path: str,
    data_gitref: str,
    image_list: str,
    assembly: str,
):
    await ImagesHealthPipeline(
        runtime,
        versions,
        send_to_release_channel,
        data_path,
        data_gitref,
        image_list,
        assembly,
    ).run()
