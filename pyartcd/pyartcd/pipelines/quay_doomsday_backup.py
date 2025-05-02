import os
from typing import Optional
import click
import shutil
import asyncio
from tenacity import AsyncRetrying, stop_after_attempt

from pyartcd.runtime import Runtime
from pyartcd.cli import cli, pass_runtime, click_coroutine
from artcommonlib.exectools import cmd_assert_async
from doozerlib.util import mkdirs


N_RETRIES = 4
ALL_ARCHES_LIST = ["x86_64", "s390x", "ppc64le", "aarch64", "multi"]


class QuayDoomsdaySync:
    """
    Backup promoted payloads to AWS bucket, in the event we need to recover a payload or if quay goes down
    """

    def __init__(self, runtime: Runtime, version: str, arches: Optional[str]):
        self.runtime = runtime
        self.version = version
        self.workdir = "./workspace"
        self.slack_client = self.runtime.new_slack_client()
        self.slack_client.bind_channel(version)

        self.arches = arches.split(",") if arches else ALL_ARCHES_LIST

    async def sync_arch(self, arch: str) -> bool:
        if arch not in ALL_ARCHES_LIST:
            raise Exception(f"Invalid arch: {arch}")

        major_minor = ".".join(self.version.split(".")[:2])
        path = f"{major_minor}/{self.version}/{arch}"

        mirror_cmd = [
            "oc", "adm", "release", "mirror",
            f"quay.io/openshift-release-dev/ocp-release:{self.version}-{arch}",
            "--keep-manifest-list",
            f"--to-dir={self.workdir}/{path}",
        ]
        aws_cmd = [
            "aws", "s3", "sync", f"{self.workdir}/{path}",
            f"s3://ocp-doomsday-registry/release-image/{path}",
        ]

        # Setup tenacity retry behavior for calling mirror_cmd and aws_cmd
        # because cmd_assert_async does not have retry logic
        retry = AsyncRetrying(reraise=True, stop=stop_after_attempt(N_RETRIES))
        try:
            self.runtime.logger.info("[%s] Running mirror command: %s", arch, mirror_cmd)
            await retry(cmd_assert_async, mirror_cmd)
            self.runtime.logger.info("[%s] Mirror command ran successfully", arch)
            if self.runtime.dry_run:
                self.runtime.logger.info("[DRY RUN] [%s] Would have run %s", arch, " ".join(aws_cmd))
                self.runtime.logger.info("[DRY RUN] [%s] Would have messaged Slack", arch)
            else:
                await asyncio.sleep(5)
                self.runtime.logger.info("[%s] Running aws command: %s", arch, aws_cmd)
                await retry(cmd_assert_async, aws_cmd)
                self.runtime.logger.info("[%s] AWS command ran successfully", arch)
                await asyncio.sleep(5)

                await self.slack_client.say_in_thread(f":white_check_mark: Successfully synced {self.version}-{arch}")
            return True

        except ChildProcessError as e:
            self.runtime.logger.error("[%s] Failed to sync: %s", arch, e)
            if self.runtime.dry_run:
                self.runtime.logger.info("[DRY RUN] [%s] Would have messaged Slack", arch)
            else:
                await self.slack_client.say_in_thread(f":warning: Failed to sync {self.version}-{arch}: {e}")
            return False

        finally:
            if os.path.exists(f"{self.workdir}/{path}"):
                self.runtime.logger.info("[%s] Cleaning dir: %s", arch, f"{self.workdir}/{path}")
                shutil.rmtree(f"{self.workdir}/{path}")

    async def run(self) -> None:
        mkdirs(self.workdir)

        if not self.runtime.dry_run:
            slack_response = await self.slack_client.say_in_thread(f":construction: Syncing arches {', '.join(self.arches)} of {self.version} to AWS S3 Bucket :construction:")
            slack_channel_id = slack_response["channel"]
            main_message_ts = slack_response["message"]["ts"]
        else:
            self.runtime.logger.info("[DRY RUN] Would have messaged Slack")

        # Synchronize individual arches sequentially to help with quay returning 502
        results = []
        for arch in self.arches:
            results.append(await self.sync_arch(arch))

        # Report the results to Slack
        if not self.runtime.dry_run:
            if all(results):
                await self.slack_client._client.reactions_add(channel=slack_channel_id, timestamp=main_message_ts, name="done_it_is")
            else:
                await self.slack_client.say_in_thread(":x: Failed to sync some arches", broadcast=True)
        else:
            self.runtime.logger.info("[DRY RUN] Would have messaged Slack")


@cli.command("quay-doomsday-backup", help="Run doomsday pipeline for the specified version and all arches unless --arches is specified")
@click.option("--arches", required=False, help="Comma separated list of arches to sync")
@click.option("--version", required=True, help="Release to sync, e.g. 4.15.3")
@pass_runtime
@click_coroutine
async def quay_doomsday_backup(runtime: Runtime, arches: str, version: str):

    # In 4.12 and 4.13 we sync only x86_64
    if version.startswith("4.12") or version.startswith("4.13"):
        arches = "x86_64"

    doomsday_pipeline = QuayDoomsdaySync(runtime=runtime,
                                         arches=arches,
                                         version=version)
    await doomsday_pipeline.run()
