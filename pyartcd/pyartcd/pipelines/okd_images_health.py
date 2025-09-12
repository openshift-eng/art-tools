import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import click
from artcommonlib import exectools
from artcommonlib.release_util import isolate_timestamp_in_release
from doozerlib.cli.images_health import DELTA_DAYS, ConcernCode

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
        send_to_release_channel: bool,
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

        cmd.extend(['images:health', f'--group={OKD_GROUP_TEMPLATE.format(version)}'])

        if self.assembly:
            cmd.append(f'--assembly={self.assembly}')

        _, out, err = await exectools.cmd_gather_async(cmd, stderr=None)
        report = json.loads(out.strip())

        self.runtime.logger.info('images:health output for openshift-%s:\n%s', version, out)
        self.report.extend(report)
        self.scanned_versions.append(version)

    async def notify_release_channel(self, version):
        self.slack_client.bind_channel('#art-okd-release')

        concerns = [concern for concern in self.report if concern.get('group', '') == f'openshift-{version}']

        if not concerns:
            # This should never happen, but just in case
            return

        response = await self.slack_client.say(f':sparkles: OKD build report for `{version}`:')

        # Notify images what were successfully built within last 24 hours
        await self.notify_successful_images(concerns, response)

        # Notify images whose latest attempt failed
        await self.notify_latest_attempt_failed_images(concerns, response)

        # Notify images that were never built
        await self.notify_never_built_images(concerns, response)

    async def notify_never_built_images(self, concerns: list, response: dict):
        never_built_concerns = [concern for concern in concerns if concern['code'] == ConcernCode.NEVER_BUILT.value]

        if not never_built_concerns:
            return

        msg = f'The following images have *never* been built in the last {DELTA_DAYS} days:\n'
        for concern in never_built_concerns:
            msg += f' - `{concern["image_name"]}`\n'
        await self.slack_client.say(msg, thread_ts=response['ts'])

    async def notify_latest_attempt_failed_images(self, concerns: list, response: dict):
        failed_concerns = [
            concern
            for concern in concerns
            if concern['code']
            in {
                ConcernCode.LATEST_ATTEMPT_FAILED.value,
                ConcernCode.FAILING_AT_LEAST_FOR.value,
            }
        ]

        if not failed_concerns:
            return

        msg = 'The following images have *failed to build*:\n'
        for concern in failed_concerns:
            msg += f' - `{concern["image_name"]}`\n'

        await self.slack_client.say(msg, thread_ts=response['ts'])

    async def notify_successful_images(self, concerns: list, response: dict):
        successful_concerns = [
            concern for concern in concerns if concern['code'] == ConcernCode.LATEST_BUILD_SUCCEEDED.value
        ]

        if not successful_concerns:
            return

        successfully_built_images = []
        for concern in successful_concerns:
            latest_built_nvr = concern['latest_built_nvr']
            timestamp_str = isolate_timestamp_in_release(latest_built_nvr)
            if not timestamp_str:
                self.runtime.logger.warning('Could not extract timestamp from nvr: %s', latest_built_nvr)
                continue

            build_time = datetime.strptime(timestamp_str, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - build_time < timedelta(days=1):
                successfully_built_images.append(f' - `{concern["image_name"]}`')

        if successfully_built_images:
            msg = 'The following images have been *successfully built* in the last 24 hours:\n'
            msg += '\n'.join(successfully_built_images)
            await self.slack_client.say(msg, thread_ts=response['ts'])


@cli.command('okd-images-health')
@click.option('--versions', required=False, default='', help='OCP versions to scan')
@click.option('--send-to-release-channel', is_flag=True, help='If true, send output to #art-release-4-<version>')
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
