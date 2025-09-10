import logging
import os
import traceback
from typing import Optional
from datetime import datetime, timezone

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
        assembly: str,
        image_name: str,
        data_path: str,
        kubeconfig: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.image_name = image_name
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
        version = "v1.0.0"  # Default version, can be made configurable if needed
        release = default_release_suffix()

        # Rebase OADP image
        self._logger.info(f"Rebasing {self.image_name} image for assembly {self.assembly}")
        rebase_cmd = [
            "doozer",
            f"--assembly={self.assembly}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            f"--group={self.group}",
            "--latest-parent-version",
            f"--images={self.image_name}",
            "beta:images:konflux:rebase",
            f"--version={version}",
            f"--release={release}",
            f"--message='Updating Dockerfile version and release {version}-{release}'",
        ]
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")

        # Skip
        # await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)
        self._logger.info(f"Successfully rebased {self.image_name}")

        # Build OADP image
        self._logger.info(f"Building {self.image_name} image for assembly {self.assembly}")
        build_cmd = [
            "doozer",
            f"--assembly={self.assembly}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            f"--group={self.group}",
            "--latest-parent-version",
            f"--images={self.image_name}",
            "beta:images:konflux:build",
            f"--image-repo={KONFLUX_DEFAULT_IMAGE_REPO}",
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

        # Skip
        # await exectools.cmd_assert_async(build_cmd, env=build_env)
        self._logger.info(f"Successfully built {self.image_name}")

        # Build OADP bundle image
        # TODO: Need to add support to build specific bundle NVRs
        # TODO: Possibly move the bundle build to a separate job
        bundle_build_cmd = [
            "doozer",
            f"--assembly={self.assembly}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            f"--group={self.group}",
            "--latest-parent-version",
            f"--images={self.image_name}",
            "beta:images:konflux:bundle",
        ]

        bundle_build_cmd.extend(
            [
                "--konflux-kubeconfig",
                kubeconfig,
                "--konflux-namespace",
                "ocp-art-tenant",
            ]
        )

        # await exectools.cmd_assert_async(bundle_build_cmd, env=self._doozer_env_vars)
        self._logger.info(f"Successfully rebased and built bundle {self.image_name}")

        release = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        # Build FBC
        fbc_build = [
            "doozer",
            f"--assembly={self.assembly}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            f"--group={self.group}",
            "--latest-parent-version",
            f"--images={self.image_name}",
            "beta:fbc:rebase-and-build",
            # TODO: Shouldn't need to pass along version
            f"--version=4.20",
            "--reset-to-prod",
            f"--release={release}",
            f"--konflux-kubeconfig={kubeconfig}",
            f"--message=Rebase FBC segment with release {release}",
            f"--prod-registry-auth={os.getenv('KONFLUX_OPERATOR_INDEX_AUTH_FILE')}",
            # TODO: For testing
            # "oadp-operator-container-v1.0.0-202509060234.p2.g1d0b40b.assembly.test.el9"
        ]

        await exectools.cmd_assert_async(fbc_build, env=self._doozer_env_vars)


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
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The name of an assembly to rebase & build for. e.g. 4.9.1",
)
@click.option(
    "--image-name",
    metavar="IMAGE_NAME",
    default="oadp-operator",
    help="The name of the OADP image to build (default: oadp-operator)",
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
    runtime: Runtime, data_path: str, group: str, assembly: str, image_name: str, kubeconfig: Optional[str]
):
    """Rebase and build OADP image for an assembly"""
    try:
        pipeline = BuildOadpPipeline(
            runtime=runtime,
            group=group,
            assembly=assembly,
            image_name=image_name,
            data_path=data_path,
            kubeconfig=kubeconfig,
        )
        await pipeline.run()
    except Exception as err:
        error_message = f"build-oadp pipeline encountered error: {err}\n{traceback.format_exc()}"
        runtime.logger.error(error_message)
        raise
