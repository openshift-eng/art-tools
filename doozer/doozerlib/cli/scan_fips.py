"""
For this command to work, https://github.com/openshift/check-payload binary has to exist in PATH and run as root
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Optional

import click
import koji
from artcommonlib.exectools import cmd_gather, cmd_gather_async, limit_concurrency
from tenacity import AsyncRetrying, RetryError, retry_if_result, stop_after_attempt

from doozerlib.cli import cli, click_coroutine, pass_runtime
from doozerlib.runtime import Runtime


class ScanFipsCli:
    def __init__(self, runtime: Runtime, nvrs: Optional[list], all_images: bool, clean: Optional[bool]):
        self.runtime = runtime
        self.nvrs = nvrs if nvrs else []
        self.all_images = all_images
        self.clean = clean
        self.could_not_clean = []
        self.is_root = False

        # Initialize runtime and brewhub session
        self.runtime.initialize(clone_distgits=False)
        self.koji_session = koji.ClientSession(self.runtime.group_config.urls.brewhub)

        if self.nvrs and self.all_images:
            raise Exception("Cannot specify both --nvrs and --all-images")

        if not (self.nvrs or self.all_images):
            raise Exception("Please specify either --nvrs or --all-images")

    async def execute_and_handle_cmd(self, cmd, nvr):
        rc, out, _ = cmd_gather(cmd)
        if rc != 0:
            self.could_not_clean.append(nvr)
        return rc, out

    async def clean_image(self, nvr, pull_spec):
        if not self.clean:
            return

        name_without_sha = pull_spec.split("@")[0]
        cmd = self.make_command("podman images --format '{{.ID}} {{.Repository}}'")
        rc, out = await self.execute_and_handle_cmd(cmd, nvr)
        if rc != 0:
            return

        for image in out.strip().split("\n"):
            if not image:
                continue

            image_id, image_name = image.split(" ")
            if name_without_sha == image_name:
                self.runtime.logger.info(f"Trying to clean image {image}")
                cmd = self.make_command(f"podman rmi {image_id}")
                await self.execute_and_handle_cmd(cmd, nvr)

    @limit_concurrency(os.cpu_count())
    async def run_get_problem_nvrs(self, build: tuple):
        def should_retry_scan(scan_result: tuple[int, str, str]):
            rc_scan, out_scan, _ = scan_result
            return rc_scan != 0 and "Successful run" not in out_scan

        nvr, pull_spec = build
        cmd = self.make_command(f"check-payload -V {self.runtime.group.split('-')[-1]} scan image --spec {pull_spec}")

        self.runtime.logger.info(f"Running check-payload command: {cmd}")

        retry = AsyncRetrying(
            stop=stop_after_attempt(3),
            retry=retry_if_result(should_retry_scan),
        )

        try:
            await retry(cmd_gather_async, cmd, check=False)
            return None
        except RetryError:
            self.runtime.logger.info(f"All retry attempts exhaused for command: {cmd}")
            return build
        finally:
            await self.clean_image(nvr, pull_spec)

    def make_command(self, cmd: str):
        return "sudo " + cmd if not self.is_root else cmd

    async def am_i_root(self):
        rc, out, _ = await cmd_gather_async("whoami", check=False)
        self.runtime.logger.info(f"Running as user {out}")
        return rc == 0 and "root" in out

    def get_tagged_latest(self, tag):
        """
        Returns the latest RPMs and builds, triggered by the automation, tagged in to the brew tag received as input
        """
        latest_tagged = self.koji_session.listTagged(tag=tag, latest=True, owner="exd-ocp-buildvm-bot-prod")
        return latest_tagged or []

    def get_all_latest_builds(self):
        # Setup brew tags
        et_data = self.runtime.get_errata_config()
        brew_tags = [
            tag.format(**self.runtime.group_config.vars) for tag in et_data.get("brew_tag_product_version_mapping", {})
        ]
        self.runtime.logger.info(f"Retrieved candidate tags: {brew_tags}")

        builds = []
        for tag in brew_tags:
            builds += self.get_tagged_latest(tag=tag)

        # Find the latest build nvr per image across all tags
        latest_build_map = {}
        for build in builds:
            image_name = build["package_name"]
            completion_time = datetime.strptime(build["completion_time"], "%Y-%m-%d %H:%M:%S.%f")

            if image_name not in latest_build_map or completion_time > latest_build_map[image_name]["time"]:
                latest_build_map[image_name] = {"nvr": build["nvr"], "time": completion_time}

        nvrs = []
        for latest_build in latest_build_map.values():
            nvrs.append(latest_build["nvr"])

        return nvrs

    async def run(self):
        if self.all_images:
            self.nvrs = self.get_all_latest_builds()

        self.is_root = await self.am_i_root()
        # Get the list of NVRs to scan for
        # (nvr, pull-spec) list of tuples
        image_pullspec_mapping = []

        # Package names of base images
        base_images_package_names = [
            meta.config.distgit.component for meta in self.runtime.image_metas() if meta.base_only
        ]

        for nvr in self.nvrs:
            # Skip CI builds since it won't be shipped
            if nvr.startswith("ci-openshift"):
                self.runtime.logger.info(f"Skipping {nvr} since its a CI build")
                continue

            # Find the registry pull spec
            build_info = self.koji_session.getBuild(nvr)

            if build_info["package_name"] in base_images_package_names:
                self.runtime.logger.info(f"Skipping {nvr} since its a base image build")
                continue

            # Identify if it's an RPM, and skip it
            if "git+https://pkgs.devel.redhat.com/git/rpms/" in build_info["source"]:
                self.runtime.logger.info(f"Skipping {nvr} since its an RPM")
                continue

            # Component has moved to RHEL9
            if build_info["package_name"] == "openshift-kubernetes-nmstate-handler-rhel-8-container":
                self.runtime.logger.info(f"Skipping {nvr} since its migrated to RHEL 9")
                continue

            # Eg registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-sriov-network-operator@sha256:da95750d31cb1b9539f664d2d6255727fa8d648e93150ae92ed84a9e993753be
            # from https://brewweb.engineering.redhat.com/brew/buildinfo?buildID=2777601
            try:
                pull_spec = build_info["extra"]["image"]["index"]["pull"][0]
            except KeyError:
                self.runtime.logger.warning(
                    f"Skipping {nvr} since it doesn't have an image pull spec, probably cause its an RPM"
                )
                continue
            image_pullspec_mapping.append((nvr, pull_spec))

        tasks = [self.run_get_problem_nvrs(build) for build in image_pullspec_mapping]
        results = await asyncio.gather(*tasks)

        problem_images = {build[0]: build[1] for build in results if build}

        if problem_images:
            self.runtime.logger.info("Found FIPS issues for these components:")
        else:
            self.runtime.logger.info("No FIPS issues found!")

        click.echo(json.dumps(problem_images))

        if self.clean and self.could_not_clean:
            raise Exception(f"Could not clean images: {self.could_not_clean}")


@cli.command("images:scan-fips", help="Trigger FIPS check for specified NVRs")
@click.option("--nvrs", required=False, help="Comma separated list to trigger scans for")
@click.option("--all-images", is_flag=True, default=False, help="Scan all latest images in our tags")
@click.option("--clean", is_flag=True, default=False, help="Clean images after scanning")
@pass_runtime
@click_coroutine
async def scan_fips(runtime: Runtime, nvrs: str, all_images: bool, clean: bool):
    fips_pipeline = ScanFipsCli(
        runtime=runtime,
        nvrs=nvrs.split(",") if nvrs else None,
        all_images=all_images,
        clean=clean,
    )
    await fips_pipeline.run()
