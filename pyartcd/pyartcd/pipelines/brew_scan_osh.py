"""
This job scans the candidate tags for a particular version, and triggers scans for builds that are tagged into it.
"""
import click
from typing import Optional
from artcommonlib import exectools
from artcommonlib import redis
from pyartcd.runtime import Runtime
from pyartcd.cli import cli, pass_runtime, click_coroutine


class OshScan:
    def __init__(self, runtime: Runtime, version: str, check_triggered: Optional[bool], email: Optional[str],
                 nvrs: Optional[list], all_builds: Optional[bool], create_jira_tickets: Optional[bool]):
        self.runtime = runtime
        self.email = email
        self.version = version
        self.major, self.minor = self.version.split(".")
        self.check_triggered = check_triggered
        self.specific_nvrs = nvrs
        self.all_builds = all_builds
        self.create_jira_tickets = create_jira_tickets

        self.redis_key = f"appdata:sast-scan:{self.version}"

    async def store_brew_event(self, brew_event_id: str):
        await redis.set_value(self.redis_key, brew_event_id)

    async def retrieve_brew_event(self):
        last_brew_event = await redis.get_value(self.redis_key)

        # Remove trailing \n
        if last_brew_event:
            last_brew_event = last_brew_event.strip()

        return last_brew_event

    async def run(self):
        cmd = [
            "doozer",
            "--group",
            f"openshift-{self.version}",
            "images:scan-osh",
        ]

        last_brew_event = await self.retrieve_brew_event()

        self.runtime.logger.info(f"Found last brew event from REDIS: {last_brew_event}")

        if self.runtime.dry_run:
            cmd.append("--dry-run")

        if self.check_triggered:
            cmd.append("--check-triggered")

        if self.specific_nvrs or self.all_builds:
            if self.specific_nvrs:
                cmd += [
                    "--nvrs",
                    f"{','.join(self.specific_nvrs)}",
                ]

            if self.all_builds:
                cmd.append("--all-builds")
        else:
            # So if we give the specific list of nvrs, we don't need to pass the last brew event
            if last_brew_event:
                cmd += [
                    "--since",
                    f"{last_brew_event}",
                ]

        if self.create_jira_tickets:
            cmd.append("--create-jira-tickets")

        _, brew_event, _ = await exectools.cmd_gather_async(cmd, stderr=True)
        if not brew_event and not self.specific_nvrs:
            self.runtime.logger.warning(f"No new builds found for candidate tags in {self.version}")
            return

        if not self.specific_nvrs and not self.all_builds:
            # We do not need to write to Redis if we're manually triggering scans for specific builds
            if not self.runtime.dry_run:
                # Write the latest brew event back to the file
                if last_brew_event != brew_event:
                    # No need to set if it's the same value
                    await self.store_brew_event(brew_event)
            else:
                self.runtime.logger.info("[DRY RUN] Would have written to Redis")


@cli.command("scan-osh")
@click.option("--version", required=True, help="openshift version eg: 4.15")
@click.option("--email", required=False, help="Additional email to which the results of the scan should be sent out to")
@click.option("--nvrs", required=False, help="Comma separated list to trigger scans specifically. Will not check candidate tags")
@click.option("--check-triggered", required=False, is_flag=True, default=False, help="Triggers scans for NVRs only after checking if they haven't already")
@click.option("--all-builds", required=False, is_flag=True, default=False, help="Check all builds in candidate tags")
@click.option("--create-jira-tickets", required=False, is_flag=True, default=False, help="Create OCPBUGS ticket for a package if vulnerabilities exist")
@pass_runtime
@click_coroutine
async def scan_osh(runtime: Runtime, version: str, email: str, nvrs: str, check_triggered: bool, all_builds: bool,
                   create_jira_tickets: bool):
    pipeline = OshScan(runtime=runtime,
                       email=email,
                       version=version,
                       nvrs=nvrs.split(",") if nvrs else None,
                       check_triggered=check_triggered,
                       all_builds=all_builds,
                       create_jira_tickets=create_jira_tickets)
    await pipeline.run()
