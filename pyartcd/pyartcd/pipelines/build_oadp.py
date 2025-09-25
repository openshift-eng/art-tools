import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Optional

import click
from artcommonlib import exectools
from doozerlib.constants import KONFLUX_DEFAULT_IMAGE_REPO

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.util import default_release_suffix


class BuildOadpPipeline:
    """Rebase and build OADP for an assembly"""

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        version: str,
        assembly: str,
        image_list: str,
        data_path: str,
        kubeconfig: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.version = version
        self.assembly = assembly
        self.image_list = image_list
        self.kubeconfig = kubeconfig
        self._logger = logger or runtime.logger

        self._working_dir = self.runtime.working_dir.absolute()

        # sets environment variables for Doozer
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._working_dir / "doozer-working")

        if not data_path:
            data_path = (
                self.runtime.config.get("build_config", {}).get("ocp_build_data_url") or constants.OCP_BUILD_DATA_URL
            )
        if data_path:
            self._doozer_env_vars["DOOZER_DATA_PATH"] = data_path

    async def run(self):
        """Run the OADP rebase and build pipeline"""
        await self._rebase_and_build()

    async def _rebase_and_build(self):
        """Rebase and build OADP image"""
        release = default_release_suffix()

        # Rebase OADP image
        self._logger.info(f"Rebasing {self.image_list} image for assembly {self.assembly}")
        rebase_cmd = [
            "doozer",
            f"--assembly={self.assembly}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            f"--group={self.group}",
            "--latest-parent-version",
            f"--images={self.image_list}",
            "beta:images:konflux:rebase",
            f"--version={self.version}",
            f"--release={release}",
            f"--message='Updating Dockerfile version and release {self.version}-{release}'",
        ]
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")

        await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)
        self._logger.info(f"Successfully rebased {self.image_list}")

        # Build OADP image
        self._logger.info(f"Building {self.image_list} image for assembly {self.assembly}")
        build_cmd = [
            "doozer",
            f"--assembly={self.assembly}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            f"--group={self.group}",
            "--latest-parent-version",
            f"--images={self.image_list}",
            "beta:images:konflux:build",
            f"--image-repo={KONFLUX_DEFAULT_IMAGE_REPO}",
            "--build-priority=1",
        ]

        # Use kubeconfig from CLI parameter or environment variable
        kubeconfig = self.kubeconfig or os.environ.get('KONFLUX_SA_KUBECONFIG')
        if not kubeconfig:
            raise ValueError(
                "KONFLUX_SA_KUBECONFIG environment variable or --kubeconfig parameter is required for Konflux builds"
            )

        build_cmd.extend(
            [
                "--konflux-kubeconfig",
                kubeconfig,
                "--konflux-namespace",
                "ocp-art-tenant",
            ]
        )
        if self.runtime.dry_run:
            build_cmd.append("--dry-run")

        # Ensure KONFLUX_ART_IMAGES_AUTH_FILE is passed through environment
        build_env = self._doozer_env_vars.copy()
        konflux_registry_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
        if konflux_registry_auth_file:
            build_env["KONFLUX_ART_IMAGES_AUTH_FILE"] = konflux_registry_auth_file

        await exectools.cmd_assert_async(build_cmd, env=build_env)
        self._logger.info(f"Successfully built {self.image_list}")


@cli.command("build-oadp")
@click.option(
    "--data-path",
    metavar='BUILD_DATA',
    default=None,
    help=f"Git repo or directory containing groups metadata e.g. {constants.OCP_BUILD_DATA_URL}",
)
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group of components on which to operate. e.g. openshift-4.9",
)
@click.option(
    "--version",
    metavar='NAME',
    required=True,
    help="OADP version",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The name of an assembly to rebase & build for. e.g. 4.9.1",
)
@click.option(
    "--image-list",
    metavar="IMAGE_NAME",
    help="List of images to build",
)
@click.option(
    "--kubeconfig",
    metavar="KUBECONFIG_PATH",
    default=None,
    help="Path to the Konflux kubeconfig file (optional)",
)
@pass_runtime
@click_coroutine
async def build_oadp(
    runtime: Runtime,
    data_path: str,
    group: str,
    version: str,
    assembly: str,
    image_list: str,
    kubeconfig: Optional[str],
):
    """Rebase and build OADP image for an assembly"""
    try:
        pipeline = BuildOadpPipeline(
            runtime=runtime,
            group=group,
            version=version,
            assembly=assembly,
            image_list=image_list,
            data_path=data_path,
            kubeconfig=kubeconfig,
        )
        await pipeline.run()
    except Exception as err:
        error_message = f"build-oadp pipeline encountered error: {err}\n{traceback.format_exc()}"
        runtime.logger.error(error_message)
        raise
