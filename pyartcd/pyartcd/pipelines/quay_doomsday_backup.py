import os
from typing import Optional
import click
from time import sleep
import shutil

from pyartcd.runtime import Runtime
from pyartcd.cli import cli, pass_runtime
from artcommonlib.exectools import cmd_assert
from doozerlib.util import mkdirs


N_RETRIES = 4
ALL_ARCHES_LIST = ["x86_64", "s390x", "ppc64le", "aarch64", "multi"]


class QuayDoomsdaySync:
    """
    Backup promoted payloads to AWS bucket, in the event we need to recover a payload or if quay goes down
    """

    def __init__(self, runtime: Runtime, all_arches: bool, version: str, arches: Optional[str]):
        self.runtime = runtime
        self.version = version
        self.workdir = "./workspace"

        if arches and all_arches:
            raise Exception("Cannot specify both --arches and --all-arches")

        if not (arches or all_arches):
            raise Exception("Either --arches or --all-arches needs to be specified")

        self.arches = arches.split(",") if not all_arches else ALL_ARCHES_LIST

    def sync_arch(self, arch: str):
        if arch not in ALL_ARCHES_LIST:
            raise Exception(f"Invalid arch: {arch}")

        major_minor = ".".join(self.version.split(".")[:2])
        path = f"{major_minor}/{self.version}/{arch}"

        mirror_cmd = [
            "oc", "adm", "release", "mirror",
            f"quay.io/openshift-release-dev/ocp-release:{self.version}-{arch}",
            "--keep-manifest-list",
            f"--to-dir={self.workdir}/{path}"
        ]
        aws_cmd = [
            "aws", "s3", "sync", f"{self.workdir}/{path}",
            f"s3://ocp-doomsday-registry/release-image/{path}"
        ]

        cmd_assert(mirror_cmd, realtime=True, retries=N_RETRIES)
        if self.runtime.dry_run:
            self.runtime.logger.info("[DRY RUN] Would have run %s", " ".join(aws_cmd))
        else:
            sleep(5)
            cmd_assert(aws_cmd, realtime=True, retries=N_RETRIES)
            sleep(5)

        if os.path.exists(path):
            self.runtime.logger.info("Cleaning dir: %s", path)
            shutil.rmtree(path)

    def run(self):
        mkdirs(self.workdir)

        for arch in self.arches:
            self.runtime.logger.info("Now syncing arch %s", arch)
            self.sync_arch(arch)


@cli.command("quay-doomsday-backup", help="Run doomsday pipeline for the specified version and arches")
@click.option("--arches", required=False, help="Comma separated list of arches to sync")
@click.option("--all-arches", is_flag=True, required=False, default=False, help="Sync all arches")
@click.option("--version", required=True, help="Release to sync, e.g. 4.15.3")
@pass_runtime
def quay_doomsday_backup(runtime: Runtime, arches: str, all_arches: bool, version: str):

    # In 4.12 we sync only x86_64 and s390x
    if version.startswith("4.12"):
        all_arches = False
        arches = "x86_64,s390x"

    doomsday_pipeline = QuayDoomsdaySync(runtime=runtime,
                                         arches=arches,
                                         all_arches=all_arches,
                                         version=version)
    doomsday_pipeline.run()
