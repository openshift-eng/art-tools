import asyncio

import click
from artcommonlib import exectools

from pyartcd import util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime

BASE_URL = "https://api.openshift.com/api/upgrades_info/v1/graph?arch=amd64&channel=fast"


class CheckBugsPipeline:
    def __init__(self, runtime: Runtime, version: str, data_path: str) -> None:
        self.runtime = runtime
        self.version = version
        self.data_path = data_path
        self.group_config = None
        self.logger = runtime.logger
        self.issues = []
        self.unstable = False  # This is set to True if any of the commands in the pipeline fail
        self.artcd_working = f"{self.version}-working"

    async def run(self):
        # Load group config
        self.group_config = await util.load_group_config(
            group=f"openshift-{self.version}", assembly="stream", doozer_data_path=self.data_path
        )

        # Check bugs only for GA releases
        if self.group_config["software_lifecycle"]["phase"] != "release":
            return None

        # Find issues
        # Note: don't run them concurrently since their working dir is the same and they can conflict
        await self._find_blockers()
        await self._find_regressions()

        # Return report
        if not self.issues:
            return None

        return {
            "version": self.version,
            "issues": self.issues,
        }

    async def _find_blockers(self):
        self.logger.info(f"Checking blocker bugs for Openshift {self.version}")

        cmd = [
            "elliott",
            f"--group=openshift-{self.version}",
            f"--working-dir={self.artcd_working}",
            "find-bugs:blocker",
            "--output=slack",
        ]
        rc, out, err = await exectools.cmd_gather_async(cmd)

        if rc:
            self.unstable = True
            self.logger.error(f'Command "{cmd}" failed with status={rc}: {err.strip()}')
            return None

        out = out.strip().splitlines()
        if not out:
            self.logger.info("No blockers found for version %s", self.version)
            return

        self.logger.info("Command returned: %s", out)
        self.issues.extend(out)

    async def _find_regressions(self):
        self.logger.info(f"Checking possible regressions for Openshift {self.version}")

        # Verify bugs
        cmd = [
            "elliott",
            f"--group=openshift-{self.version}",
            "--assembly=stream",
            f"--working-dir={self.artcd_working}",
            "verify-bugs",
            "--output=slack",
        ]
        rc, out, err = await exectools.cmd_gather_async(cmd, check=False)

        # If returncode is 0 then no regressions were found
        if not rc:
            self.logger.info("No regressions found for version %s", self.version)
            return

        out = out.strip().splitlines()
        if out:
            self.issues.extend(out)

        self.unstable = True
        self.logger.error(f'Command "{cmd}" failed with status={rc}: {err.strip()}')


async def slack_report(results, slack_client):
    slack_response = await slack_client.say(":red-siren: Bug(s) requiring attention:")
    slack_thread = slack_response["message"]["ts"]

    for result in results:
        message = f":warning: *{result['version']}*"
        for issue in result["issues"]:
            message += f"\n{issue}"
        await slack_client.say(message, slack_thread)


@cli.command("check-bugs")
@click.option("--slack_channel", required=False, help="Slack channel to be notified for failures")
@click.option("--version", "versions", required=True, multiple=True, help="OCP version to check for blockers e.g. 4.7")
@click.option(
    "--data-path",
    required=False,
    default=OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use (e.g. assembly definition in your own fork)",
)
@pass_runtime
@click_coroutine
async def check_bugs(runtime: Runtime, slack_channel: str, versions: list, data_path: str):
    tasks = [CheckBugsPipeline(runtime, version=version, data_path=data_path).run() for version in versions]
    results = await asyncio.gather(*tasks)

    if any(results):
        if not slack_channel.startswith("#"):
            raise ValueError("Invalid Slack channel name provided")

        slack_client = runtime.new_slack_client()
        slack_client.bind_channel(slack_channel)
        await slack_report(filter(lambda result: result, results), slack_client)
