"""
For this command to work, https://github.com/openshift/check-payload binary has to exist in PATH and run as root
This job is deployed on ART cluster
"""
import json
import click
from typing import Optional
from pyartcd.runtime import Runtime
from pyartcd.cli import cli, pass_runtime, click_coroutine
from pyartcd import exectools


class ScanFips:
    def __init__(self, runtime: Runtime, version: str, nvrs: Optional[list]):
        self.runtime = runtime
        self.version = version
        self.nvrs = nvrs

        # Setup slack client
        self.slack_client = self.runtime.new_slack_client()
        self.slack_client.bind_channel("#art-release")

    async def run(self):
        cmd = [
            "doozer",
            "--group",
            f"openshift-{self.version}",
            "--data-path",
            "https://github.com/openshift-eng/ocp-build-data",
            "images:scan-fips",
            "--nvrs",
            f"{','.join(self.nvrs)}"
        ]

        _, result, _ = await exectools.cmd_gather_async(cmd, stderr=True)

        result_json = json.loads(result)

        self.runtime.logger.info(f"Result: {result_json}")

        if result_json:
            # alert release artists
            if not self.runtime.dry_run:
                message = ":warning: @ashwin FIPS scan has failed for some builds. Please verify. (Triage <https://art-docs.engineering.redhat.com/sop/triage-fips|docs>)"
                slack_response = await self.slack_client.say(message=message)
                slack_thread = slack_response["message"]["ts"]

                await self.slack_client.upload_content(
                    content=result,
                    filename="report.json",
                    thread_ts=slack_thread
                )
            else:
                self.runtime.logger.info("[DRY RUN] Would have messaged slack")
        else:
            self.runtime.logger.info("No issues")


@cli.command("scan-fips", help="Trigger FIPS check for specified NVRs")
@click.option("--version", required=True, help="openshift version eg: 4.15")
@click.option("--nvrs", required=False, help="Comma separated list to trigger scans for")
@pass_runtime
@click_coroutine
async def scan_osh(runtime: Runtime, version: str, nvrs: str):
    pipeline = ScanFips(runtime=runtime,
                        version=version,
                        nvrs=nvrs.split(",") if nvrs else None
                        )
    await pipeline.run()
