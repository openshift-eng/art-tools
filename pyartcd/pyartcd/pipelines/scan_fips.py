"""
For this command to work, https://github.com/openshift/check-payload binary has to exist in PATH and run as root
This job is deployed on ART cluster
"""
import json
import sys

import click
from typing import Optional
from pyartcd.runtime import Runtime
from pyartcd.cli import cli, pass_runtime, click_coroutine
from artcommonlib.constants import ACTIVE_OCP_VERSIONS
from artcommonlib import exectools


class ScanFips:
    def __init__(self, runtime: Runtime, data_path: str, all_images: bool, nvrs: Optional[list]):
        self.runtime = runtime
        self.data_path = data_path
        self.nvrs = nvrs
        self.all_images = all_images

        # Setup slack client
        self.slack_client = self.runtime.new_slack_client()
        self.slack_client.bind_channel("#art-release")

    async def run(self):
        results = {}

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
            result_json = json.loads(result)

            results.update(result_json)

        self.runtime.logger.info(f"Result: {results}")

        if results:
            # alert release artists
            if not self.runtime.dry_run:
                message = ":warning: FIPS scan has failed for some builds. Please verify. (Triage <https://art-docs.engineering.redhat.com/sop/triage-fips/|docs>)"
                slack_response = await self.slack_client.say(message=message)
                slack_thread = slack_response["message"]["ts"]

                await self.slack_client.upload_content(
                    content=json.dumps(results),
                    filename="report.json",
                    thread_ts=slack_thread
                )

                # Exit as error so that we see in the PipelineRun
                self.runtime.logger.error("FIPS issues were found")

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
    pipeline = ScanFips(runtime=runtime,
                        data_path=data_path,
                        all_images=all_images,
                        nvrs=nvrs.split(",") if nvrs else None
                        )
    await pipeline.run()
