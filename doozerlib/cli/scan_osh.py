import click
import koji
import asyncio
from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.runtime import Runtime
from doozerlib.exectools import fire_and_forget, cmd_gather_async, limit_concurrency
from doozerlib.util import cprint
from typing import Optional


class ScanOshCli:
    def __init__(self, runtime: Runtime, last_brew_event: int, dry_run: bool, nvrs: Optional[list],
                 check_triggered: Optional[bool], all_builds: Optional[bool]):
        self.runtime = runtime
        self.last_brew_event = last_brew_event
        self.dry_run = dry_run
        self.brew_tags = []
        self.specific_nvrs = nvrs
        self.check_triggered = check_triggered
        self.all_builds = all_builds
        self.error_nvrs = []

        # Initialize runtime and brewhub session
        self.runtime.initialize(clone_distgits=False)
        self.koji_session = koji.ClientSession(self.runtime.group_config.urls.brewhub)

    @staticmethod
    async def get_untriggered_nvrs(nvrs):
        """
        So we might have already triggered a scan for the same NVR. If this flag is enabled, we want to check
        if it has and not re-trigger it again.
        But please note that this will take a lot of time since it has to run for all 200+ packages.
        """

        @limit_concurrency(16)
        async def run_get_untriggered_nvrs(nvr):
            rc, _, _ = await cmd_gather_async(f"osh-cli find-tasks --nvr {nvr}")

            return None if rc == 0 else nvr

        tasks = []
        for nvr in nvrs:
            tasks.append(run_get_untriggered_nvrs(nvr))

        nvrs = await asyncio.gather(*tasks)

        untriggered_nvrs = [nvr for nvr in nvrs if nvr]

        return untriggered_nvrs

    def get_tagged_latest(self, tag):
        """
        Returns the latest RPMs and builds tagged in to the candidate tag received as input
        """
        latest_tagged = self.koji_session.listTagged(tag=tag, latest=True)
        if latest_tagged:
            return latest_tagged
        else:
            return []

    def get_tagged_all(self, tag):
        """
        Returns all the RPMs and builds that are currently in the candidate tag received as input
        """
        latest_tagged = self.koji_session.listTagged(tag=tag, latest=False)
        if latest_tagged:
            return latest_tagged
        else:
            return []

    def trigger_scans(self, nvrs: list):
        cmd_template = "osh-cli mock-build --config={config} --brew-build {nvr} --nowait"
        for nvr in nvrs:
            # Skip rhcos for now
            if nvr.startswith("rhcos"):
                self.runtime.logger.warning(f"Skipping RHCOS builds. Scan is not triggered for {nvr}")
                continue

            if "container" in nvr:
                cmd = cmd_template.format(config="cspodman", nvr=nvr)

            else:
                if "el7" in nvr:
                    rhel_version = 7
                elif "el8" in nvr:
                    rhel_version = 8
                elif "el9" in nvr or nvr.startswith("rhcos"):
                    rhel_version = 9
                else:
                    self.runtime.logger.error("Invalid RHEL version")
                    return

                cmd = cmd_template.format(config=f"rhel-{rhel_version}-x86_64", nvr=nvr)

            message = f"Ran command: {cmd}"

            if not self.dry_run:
                fire_and_forget(self.runtime.cwd, cmd)
            else:
                message = "[DRY RUN] " + message

            self.runtime.logger.info(message)

        self.runtime.logger.info(f"Total number of build scans kicked off: {len(nvrs)}")

    async def run(self):
        if not self.specific_nvrs:
            tags = self.runtime.get_errata_config()["brew_tag_product_version_mapping"].keys()
            for tag in tags:
                major, minor = self.runtime.get_major_minor_fields()
                self.brew_tags.append(tag.format(MAJOR=major, MINOR=minor))

            builds = []

            if self.last_brew_event or self.all_builds:
                for tag in self.brew_tags:
                    builds += self.get_tagged_all(tag=tag)

                if self.last_brew_event:
                    builds = [build for build in builds if build["create_event"] > self.last_brew_event]
            else:
                # If no --since field is specified, find all the builds that have been tagged into our candidate tags
                for tag in self.brew_tags:
                    builds += self.get_tagged_latest(tag=tag)

            # Sort the builds based on the event ID by descending order so that latest is always on top
            # Note: create_event is the event on which the build was tagged in the tag and not the build creation time
            builds = sorted(builds, key=lambda x: x["create_event"], reverse=True)

            nvrs = [build["nvr"] for build in builds]
            nvr_brew_mapping = [(build["nvr"], build["create_event"]) for build in builds]

            # To store the final list of NVRs that we will kick off scans for
            nvrs_for_scans = []

            if nvr_brew_mapping:
                self.runtime.logger.info(f"NVRs to trigger scans for {nvr_brew_mapping}")

            if builds:
                latest_event_id = nvr_brew_mapping[0][1]

                nvrs_for_scans = nvrs

                # Return back the latest brew event ID
                cprint(latest_event_id)
            else:
                self.runtime.logger.warning(
                    f"No new NVRs have been found since last brew event: {self.last_brew_event}")
                return None

        else:
            self.runtime.logger.info(f"Triggering scans for this particular list of NVRs: {self.specific_nvrs}")

            nvrs_for_scans = self.specific_nvrs

        if self.check_triggered:
            nvrs_for_scans = await self.get_untriggered_nvrs(nvrs_for_scans)

        self.trigger_scans(nvrs_for_scans)


@cli.command("images:scan-osh", help="Trigger scans for builds with brew event IDs greater than the value specified")
@click.option("--since", required=False, help="Builds after this brew event. If empty, latest builds will retrieved")
@click.option("--dry-run", default=False, is_flag=True,
              help="Do not trigger anything, but only print build operations.")
@click.option("--nvrs", required=False,
              help="Comma separated list to trigger scans specifically. Will not check candidate tags")
@click.option("--check-triggered", required=False, is_flag=True, default=False,
              help="Triggers scans for NVRs only after checking if they haven't already")
@click.option("--all-builds", required=False, is_flag=True, default=False, help="Check all builds in candidate tags")
@pass_runtime
@click_coroutine
async def scan_osh(runtime: Runtime, since: str, dry_run: bool, nvrs: str, check_triggered: bool, all_builds: bool):
    cli_pipeline = ScanOshCli(runtime=runtime,
                              last_brew_event=int(since) if since else None,
                              dry_run=dry_run,
                              nvrs=nvrs.split(",") if nvrs else None,
                              check_triggered=check_triggered,
                              all_builds=all_builds)
    await cli_pipeline.run()
