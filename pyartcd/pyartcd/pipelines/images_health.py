import asyncio
import json

import click
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime


class ImagesHealthPipeline:
    def __init__(self, runtime: Runtime, version: str, send_to_release_channel: bool, send_to_forum_ocp_art: bool):
        self.runtime = runtime
        self.doozer_working = self.runtime.working_dir / "doozer_working"
        self.version = version
        self.send_to_release_channel = send_to_release_channel
        self.send_to_forum_ocp_art = send_to_forum_ocp_art
        self.report = {}

    async def run(self):
        # Check if automation is frozen for current group
        if not await util.is_build_permitted(self.version, doozer_working=self.doozer_working):
            self.runtime.logger.info('Skipping this build as it\'s not permitted')
            return

        # Get doozer report
        cmd = [
            'doozer',
            f'--working-dir={self.doozer_working}',
            f'--group=openshift-{self.version}',
            'images:health',
        ]
        _, out, err = await exectools.cmd_gather_async(cmd, stderr=None)
        self.report = json.loads(out.strip())
        self.runtime.logger.info('images:health output for openshift-%s:\n%s', self.version, out)

        if any([self.send_to_release_channel, self.send_to_forum_ocp_art]):
            await asyncio.gather(*[self._send_notifications(engine) for engine in ['brew', 'konflux']])

    def send_for_engine(self, engine):
        if engine == "konflux":
            return True
        if engine == "brew":
            return self.version not in KONFLUX_IMAGESTREAM_OVERRIDE_VERSIONS
        raise ValueError(f'Engine {self.engine} not recognized')

    async def _send_notifications(self, engine):
        slack_client = self.runtime.new_slack_client()
        engine_report = self.report.get(engine, None)

        if not engine_report:
            if self.send_to_release_channel:
                slack_client.bind_channel(self.version)
                await slack_client.say(
                    f':white_check_mark: [{engine}] All images are healthy for openshift-{self.version}'
                )
            return

        msg = (
            f':alert: [{engine}] There are some issues to look into for openshift-{self.version}. '
            f'{len(engine_report)} components have failed!'
        )

        report = ''
        for image_name, concern in engine_report.items():
            report += f'\n`{image_name}`:\n- ' + concern

        if self.send_to_release_channel:
            slack_client.bind_channel(self.version)
            response = await slack_client.say(msg)
            await slack_client.say(report, thread_ts=response['ts'])

        # For now, only notify public channels about Brew failures
        if self.send_to_forum_ocp_art and self.send_for_engine(engine):
            slack_client.bind_channel('#forum-ocp-art')
            response = await slack_client.say(msg)
            await slack_client.say(report, thread_ts=response['ts'])


@cli.command('images-health')
@click.option('--version', required=True, help='OCP version to scan')
@click.option('--send-to-release-channel', is_flag=True, help='If true, send output to #art-release-4-<version>')
@click.option('--send-to-forum-ocp-art', is_flag=True, help='"If true, send notification to #forum-ocp-art')
@pass_runtime
@click_coroutine
async def images_health(runtime: Runtime, version: str, send_to_release_channel: bool, send_to_forum_ocp_art: bool):
    await ImagesHealthPipeline(runtime, version, send_to_release_channel, send_to_forum_ocp_art).run()
