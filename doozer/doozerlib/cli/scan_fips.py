"""
For this command to work, https://github.com/openshift/check-payload binary has to exist in PATH and run as root
"""
import asyncio
import json
import sys
import os
import click
import koji
from doozerlib.cli import cli, pass_runtime, click_coroutine
from doozerlib.runtime import Runtime
from typing import Optional
from artcommonlib.exectools import cmd_gather_async, limit_concurrency


class ScanFipsCli:
    def __init__(self, runtime: Runtime, nvrs: Optional[list]):
        self.runtime = runtime
        self.nvrs = nvrs

        # Initialize runtime and brewhub session
        self.runtime.initialize(clone_distgits=False)
        self.koji_session = koji.ClientSession(self.runtime.group_config.urls.brewhub)

    @limit_concurrency(16)
    async def run_get_problem_nvrs(self, build: tuple):
        rc_scan, out_scan, _ = await cmd_gather_async(f"sudo check-payload scan image --spec {build[1]}")

        # Eg: registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-sriov-network-operator
        name = build[1].split("@")[0]

        self.runtime.logger.info(f"Cleaning image {name}")
        clean_command = "sudo podman images --format '{{.ID}} {{.Repository}}' | " + f"grep {name} | " + \
                        "awk '{print $1}' | xargs -I {} podman rmi {}"
        rc_clean = os.system(clean_command)

        if rc_clean != 0:
            raise Exception(f"Could not clean image: {clean_command}")

        # The command will fail if it's not run on root, so need to make sure of that first during debugging
        # If it says successful run, it means that the command ran correctly
        return None if rc_scan == 0 and "Successful run" in out_scan else build

    async def run(self):
        # Get the list of NVRs to scan for
        # (nvr, pull-spec) list of tuples
        image_pullspec_mapping = []

        for nvr in self.nvrs:
            # Find the registry pull spec
            build_info = self.koji_session.getBuild(nvr)

            # Eg registry-proxy.engineering.redhat.com/rh-osbs/openshift-ose-sriov-network-operator@sha256:da95750d31cb1b9539f664d2d6255727fa8d648e93150ae92ed84a9e993753be
            # from https://brewweb.engineering.redhat.com/brew/buildinfo?buildID=2777601
            pull_spec = build_info["extra"]["image"]["index"]["pull"][0]
            image_pullspec_mapping.append((nvr, pull_spec))

        tasks = []
        for build in image_pullspec_mapping:
            tasks.append(self.run_get_problem_nvrs(build))

        results = await asyncio.gather(*tasks)

        problem_images = {}
        for build in results:
            if build:
                problem_images[build[0]] = build[1]

        self.runtime.logger.info(f"Found FIPS issues for these components: {problem_images}")
        click.echo(json.dumps(problem_images))


@cli.command("images:scan-fips", help="Trigger FIPS check for specified NVRs")
@click.option("--nvrs", required=False, help="Comma separated list to trigger scans for")
@pass_runtime
@click_coroutine
async def scan_fips(runtime: Runtime, nvrs: str):
    fips_pipeline = ScanFipsCli(runtime=runtime,
                                nvrs=nvrs.split(",") if nvrs else None
                                )
    await fips_pipeline.run()
