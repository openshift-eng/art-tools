"""
For this command to work, https://github.com/openshift/check-payload binary has to exist in PATH and run as root
"""
import asyncio
import json
import os
from typing import Optional

import click
import koji

from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib.runtime import Runtime
from artcommonlib.exectools import cmd_gather_async, limit_concurrency, cmd_gather


class ScanFipsCli:
    def __init__(self, runtime: Runtime, nvrs: Optional[list], clean: Optional[bool]):
        self.runtime = runtime
        self.nvrs = nvrs
        self.clean = clean
        self.could_not_clean = []
        self.is_root = False

        # Initialize runtime and brewhub session
        self.runtime.initialize(clone_distgits=False)
        self.koji_session = koji.ClientSession(self.runtime.group_config.urls.brewhub)

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
        nvr, pull_spec = build
        cmd = self.make_command(f"check-payload -V {self.runtime.group.split('-')[-1]} scan image --spec {pull_spec}")

        self.runtime.logger.info(f"Running check-payload command: {cmd}")
        rc_scan, out_scan, _ = await cmd_gather_async(cmd)

        await self.clean_image(nvr, pull_spec)

        return None if rc_scan == 0 and "Successful run" in out_scan else build

    def make_command(self, cmd: str):
        return "sudo " + cmd if not self.is_root else cmd

    async def am_i_root(self):
        rc, out, _ = await cmd_gather_async("whoami")
        self.runtime.logger.info(f"Running as user {out}")
        return rc == 0 and "root" in out

    async def run(self):
        self.is_root = await self.am_i_root()
        # Get the list of NVRs to scan for
        # (nvr, pull-spec) list of tuples
        image_pullspec_mapping = []

        for nvr in self.nvrs:
            # Skip CI builds since it won't be shipped
            if nvr.startswith("ci-openshift"):
                self.runtime.logger.info(f"Skipping {nvr} since its a CI build")
                continue

            # Find the registry pull spec
            build_info = self.koji_session.getBuild(nvr)

            # Identify if it's an RPM, and skip it
            if "git+https://pkgs.devel.redhat.com/git/rpms/" in build_info["source"]:
                self.runtime.logger.info(f"Skipping {nvr} since its an RPM")
                continue

            # Eg registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-sriov-network-operator@sha256:da95750d31cb1b9539f664d2d6255727fa8d648e93150ae92ed84a9e993753be
            # from https://brewweb.engineering.redhat.com/brew/buildinfo?buildID=2777601
            pull_spec = build_info["extra"]["image"]["index"]["pull"][0]
            image_pullspec_mapping.append((nvr, pull_spec))

        tasks = [self.run_get_problem_nvrs(build) for build in image_pullspec_mapping]
        results = await asyncio.gather(*tasks)

        problem_images = {build[0]: build[1] for build in results if build}

        self.runtime.logger.info("Found FIPS issues for these components:")
        click.echo(json.dumps(problem_images))

        if self.clean and self.could_not_clean:
            raise Exception(f"Could not clean images: {self.could_not_clean}")


@cli.command("images:scan-fips", help="Trigger FIPS check for specified NVRs")
@click.option("--nvrs", required=False, help="Comma separated list to trigger scans for")
@click.option("--clean", is_flag=True, default=False, help="Clean images after scanning")
@pass_runtime
@click_coroutine
async def scan_fips(runtime: Runtime, nvrs: str, clean: bool):
    fips_pipeline = ScanFipsCli(runtime=runtime,
                                nvrs=nvrs.split(",") if nvrs else None,
                                clean=clean
                                )
    await fips_pipeline.run()
