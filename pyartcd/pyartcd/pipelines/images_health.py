import asyncio
import json
from datetime import datetime, timezone
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
    ):
        self.runtime = runtime
        self.versions = versions.split(',') if versions else OCP4_VERSIONS
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.send_to_release_channel = send_to_release_channel
        self.send_to_forum_ocp_art = send_to_forum_ocp_art
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.image_list = image_list.split(',') if image_list else []
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

        _, out, err = await exectools.cmd_gather_async(cmd, stderr=None)
        report = json.loads(out.strip())
        self.runtime.logger.info('images:health output for openshift-%s:\n%s', version, out)
        self.report.extend(report)

    async def notify_release_channel(self, version):
        self.slack_client.bind_channel(version)

        concerns = [concern for concern in self.report if concern.get('group', '') == f'openshift-{version}']

        if not concerns:
            await self.slack_client.say(f':white_check_mark: All images are healthy for openshift-{version}')
            return

        response = await self.slack_client.say(
            f':alert: There are some issues to look into for `openshift-{version}`. {self.get_component_tag(concerns)}'
        )
        report = ''
        for concern in concerns:
            report += f'{self.get_message_for_release(concern)}\n'
        await self.slack_client.say(report, thread_ts=response['ts'])

    async def notify_forum_ocp_art(self):
        self.slack_client.bind_channel('forum-ocp-art')

        if not self.report:
            await self.slack_client.say(':white_check_mark: All images are healthy for all monitored releases')
            return

        image_concerns = {}
        for concern in self.report:
            image_name = concern['image_name']
            image_concerns.setdefault(image_name, []).append(concern)

        response = await self.slack_client.say(
            f':alert: There are some issues to look into for Openshift builds:  {self.get_component_tag(image_concerns)}'
        )

        for image_name, concerns in image_concerns.items():
            image_message = f'`{image_name}:`'
            for concern in concerns:
                image_message += f'\n- {self.get_message_for_forum(concern)}'

            await self.slack_client.say(image_message, thread_ts=response['ts'])

    def get_message_for_release(self, concern: dict):
        code = concern['code']
        message = f'- `{concern["image_name"]}`: '

        if code == ConcernCode.NEVER_BUILT.value:
            message += f'\nImage build has never been attempted during last {DELTA_DAYS} days'
            return message

        message += f'{self.get_last_attempt_tag(concern)} {self.get_art_job_tag(concern)}'

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f' failed, and has been failing for at least {LIMIT_BUILD_RESULTS} attempts.'
            return message

        # ConcernCode.LATEST_ATTEMPT_FAILED
        message += f' failed. {self.get_latest_success_tag(concern)} was {concern["latest_success_idx"]} attempts ago.'
        return message

    def get_message_for_forum(self, concern: dict):
        code = concern['code']
        group = concern['group']

        if code == ConcernCode.NEVER_BUILT.value:
            return f'\nImage build has never been attempted in `{group}` during last {DELTA_DAYS} days'

        message = f'{self.get_last_attempt_tag(concern)} in `{group}`'

        logs_url = self.get_logs_url(concern)

        if code == ConcernCode.FAILING_AT_LEAST_FOR.value:
            message += f' failed, and has been failing for at least {LIMIT_BUILD_RESULTS} attempts.'
            message += f'\nSee {self.url_text(logs_url, "build logs")}'
            return message

        # ConcernCode.LATEST_ATTEMPT_FAILED
        message += f' failed (see {self.url_text(logs_url, "build logs")}). {self.get_latest_success_tag(concern)} was {concern["latest_success_idx"]} attempts ago.'
        return message

    @staticmethod
    def get_logs_url(concern):
        dt = datetime.fromisoformat(concern['latest_failed_build_time'])
        formatted = dt.astimezone(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        logs_url = f'{ART_BUILD_HISTORY_URL}/logs?nvr={concern["latest_failed_nvr"]}&record_id={concern["latest_failed_build_record_id"]}&after={formatted}'
        return logs_url

    def get_last_attempt_tag(self, concern):
        try:
            return f'{self.url_text(concern["latest_attempt_task_url"], "Last attempt")}'
        except Exception as e:
            self.runtime.logger.warning('failed to create last attempt tag from concern %s: %s', concern, e)
            raise

    def get_art_job_tag(self, concern):
        return f'{self.url_text(concern["latest_failed_job_url"], "(Jenkins job)")}'

    def get_latest_success_tag(self, concern):
        latest_successful_task_url = concern['latest_successful_task_url']
        return f'{self.url_text(latest_successful_task_url, "Latest successful build")}'

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
):
    await ImagesHealthPipeline(
        runtime,
        versions,
        send_to_release_channel,
        send_to_forum_ocp_art,
        data_path,
        data_gitref,
        image_list,
    ).run()
