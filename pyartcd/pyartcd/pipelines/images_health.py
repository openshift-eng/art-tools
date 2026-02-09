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
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime

OCP4_VERSIONS = [
    "4.23",
    "4.22",
    "4.21",
    "4.20",
    "4.19",
    "4.18",
    "4.17",
    "4.16",
    "4.15",
    "4.14",
    "4.13",
    "4.12",
]


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
    ):
        self.runtime = runtime
        self.versions = versions.split(',') if versions else OCP4_VERSIONS
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.send_to_release_channel = send_to_release_channel
        self.send_to_forum_ocp_art = send_to_forum_ocp_art
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.image_list = image_list.split(',') if image_list else []
        self.assembly = assembly
        self.report = []
        self.slack_client = self.runtime.new_slack_client()
        self.scanned_versions = []

    async def run(self):
        await asyncio.gather(*(self.get_report(v) for v in self.versions))
        self.runtime.logger.info('Found %s concerns', len(self.report))

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

    async def notify_release_channel(self, version):
        self.slack_client.bind_channel(version)

        concerns = [
            concern
            for concern in self.report
            if concern.get('group', '') == f'openshift-{version}'
            and concern['code'] != ConcernCode.LATEST_BUILD_SUCCEEDED.value
        ]

        version_tag = f'`openshift-{version}`'
        if self.assembly != 'stream':
            version_tag += f' (assembly `{self.assembly}`)'

        if not concerns:
            await self.slack_client.say(f':white_check_mark: All images are healthy for {version_tag}')
            return

        response = await self.slack_client.say(
            f':alert: There are some issues to look into for {version_tag}. {self.get_component_tag(concerns)}'
        )
        report = ''
        for concern in concerns:
            report += f'{self.get_message_for_release(concern)}\n'
        await self.slack_client.say(report, thread_ts=response['ts'])

    async def notify_forum_ocp_art(self):
        self.slack_client.bind_channel('#forum-ocp-art')

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

        if not image_concerns:
            await self.slack_client.say(':white_check_mark: All images are healthy for all monitored releases')
            return

        response = await self.slack_client.say(
            f':alert: There are some issues to look into for Openshift builds:  {self.get_component_tag(image_concerns)}',
            link_build_url=False,
        )

        for image_name, concerns in image_concerns.items():
            image_message = f'`{image_name}:`'
            for concern in concerns:
                image_message += f'\n• {self.get_message_for_forum(concern)}'

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
        art_dash_link = f'{ART_BUILD_HISTORY_URL}/?name=^{concern["image_name"]}$&group={group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}'
        logs_link = self.url_text(self.get_logs_url(concern), "logs")

        message = f'{self.url_text(art_dash_link, f"{group}")}: '

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f'more than {LIMIT_BUILD_RESULTS} failures ({logs_link}).'

        else:  # ConcernCode.LATEST_ATTEMPT_FAILED
            message += f'{concern["latest_success_idx"]} failures ({logs_link}).'

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
        return f'{ART_BUILD_HISTORY_URL}/?name=^{image_name}$&group={group}&assembly=stream&engine=konflux&dateRange={start_date}+to+{end_date}'

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
    ).run()
