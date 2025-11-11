import logging
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
from artcommonlib import exectools
from artcommonlib.constants import PRODUCT_KUBECONFIG_MAP
from artcommonlib.util import resolve_konflux_kubeconfig_by_product, resolve_konflux_namespace_by_product
from doozerlib.constants import KONFLUX_DEFAULT_IMAGE_REPO

from pyartcd import constants, jenkins, locks
from pyartcd import record as record_util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import Lock
from pyartcd.runtime import Runtime
from pyartcd.util import default_release_suffix, load_group_config


class BuildOadpPipeline:
    """Rebase and build OADP for an assembly"""

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        version: Optional[str],
        assembly: str,
        image_list: str,
        data_path: str,
        skip_bundle_build: bool,
        skip_rebase: bool = False,
        kubeconfig: Optional[str] = None,
        data_gitref: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.version = version
        self.assembly = assembly
        self.image_list = image_list
        self.skip_bundle_build = skip_bundle_build
        self.skip_rebase = skip_rebase
        self.kubeconfig = kubeconfig
        self.data_gitref = data_gitref
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

        jenkins.init_jenkins()

    def trigger_bundle_build(self):
        if self.skip_bundle_build:
            self._logger.warning("Skipping bundle build step because --skip-bundle-build flag is set")
            return

        record_log = self.parse_record_log()
        if not record_log:
            self._logger.warning('record.log not found, skipping bundle build')
            return

        try:
            records = record_log.get('image_build_konflux', [])
            operator_nvrs = []
            for record in records:
                if record['has_olm_bundle'] == '1' and record['status'] == '0' and record.get('nvrs', None):
                    operator_nvrs.append(record['nvrs'].split(',')[0])
            if operator_nvrs:
                jenkins.start_olm_bundle_konflux(
                    build_version=self.version,
                    assembly=self.assembly,
                    group=self.group,
                    operator_nvrs=operator_nvrs,
                    doozer_data_path=self._doozer_env_vars["DOOZER_DATA_PATH"] or '',
                    doozer_data_gitref=self.data_gitref or '',
                )
        except Exception as e:
            self._logger.exception(f"Failed to trigger bundle build: {e}")

    def parse_record_log(self) -> Optional[dict]:
        record_log_path = Path(self.runtime.doozer_working, 'record.log')
        if not record_log_path.exists():
            return None

        with record_log_path.open('r') as file:
            record_log: dict = record_util.parse_record_log(file)
            return record_log

    async def run(self):
        """Run the OADP rebase and build pipeline"""
        # Load version from group config if not provided
        if not self.version:
            group_config = await load_group_config(
                group=self.group,
                assembly=self.assembly,
                doozer_data_path=self._doozer_env_vars.get('DOOZER_DATA_PATH'),
                doozer_data_gitref=self.data_gitref,
            )
            self.version = group_config.get('version')
            if not self.version:
                raise ValueError(f"No version found in group config for {self.group}")
            self._logger.info(f"Using version {self.version} from group config")

        await self._rebase_and_build()
        self.trigger_bundle_build()

    async def _rebase_and_build(self):
        """Rebase and build OADP image"""
        release = default_release_suffix()

        group_param = f"--group={self.group}"
        if self.data_gitref:
            group_param = f"--group={self.group}@{self.data_gitref}"

        # Skip rebase if the flag is set
        if self.skip_rebase:
            self._logger.warning("Skipping rebase step because --skip-rebase flag is set")
        else:
            # Rebase OADP image
            self._logger.info(f"Rebasing {self.image_list} image for assembly {self.assembly}")

            rebase_cmd = [
                "doozer",
                f"--assembly={self.assembly}",
                f"--working-dir={self.runtime.doozer_working}",
                f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
                "--build-system=konflux",
                group_param,
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
            f"--working-dir={self.runtime.doozer_working}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            group_param,
            "--latest-parent-version",
            f"--images={self.image_list}",
            "beta:images:konflux:build",
            f"--image-repo={KONFLUX_DEFAULT_IMAGE_REPO}",
            "--build-priority=1",
        ]

        # Load group config to get product information
        group_config = await load_group_config(
            group=self.group,
            assembly=self.assembly,
            doozer_data_path=self._doozer_env_vars['DOOZER_DATA_PATH'],
            doozer_data_gitref=self.data_gitref,
        )
        product = group_config.get('product', 'ocp')

        # Resolve namespace using product-based utility function
        namespace = resolve_konflux_namespace_by_product(product)
        build_cmd.append(f"--konflux-namespace={namespace}")

        # Use kubeconfig from CLI parameter or resolve from product-specific environment variable
        kubeconfig = resolve_konflux_kubeconfig_by_product(product, self.kubeconfig)
        if not kubeconfig:
            available_env_vars = list(PRODUCT_KUBECONFIG_MAP.values())
            raise ValueError(
                f"Kubeconfig required for Konflux builds. Provide --kubeconfig parameter or set one of: {', '.join(available_env_vars)}"
            )

        build_cmd.extend(
            [
                "--konflux-kubeconfig",
                kubeconfig,
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
    required=False,
    help="OADP version (if not provided, will be read from group config)",
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
@click.option("--skip-bundle-build", is_flag=True, default=False, help="(For testing) Skip the bundle build step")
@click.option("--skip-rebase", is_flag=True, default=False, help="(For testing) Skip the rebase step")
@click.option(
    "--kubeconfig",
    metavar="KUBECONFIG_PATH",
    default=None,
    help="Path to the Konflux kubeconfig file (optional)",
)
@click.option("--data-gitref", required=False, default='', help="Doozer data path git [branch / tag / sha] to use")
@click.option("--ignore-locks", is_flag=True, default=False, help="(For testing) Do not wait for locks")
@pass_runtime
@click_coroutine
async def build_oadp(
    runtime: Runtime,
    data_path: str,
    group: str,
    version: Optional[str],
    assembly: str,
    image_list: str,
    skip_bundle_build: bool,
    skip_rebase: bool,
    kubeconfig: Optional[str],
    data_gitref: Optional[str],
    ignore_locks: bool,
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
            skip_bundle_build=skip_bundle_build,
            skip_rebase=skip_rebase,
            kubeconfig=kubeconfig,
            data_gitref=data_gitref,
        )

        lock_identifier = jenkins.get_build_path()
        if not lock_identifier:
            runtime.logger.warning(
                'Env var BUILD_URL has not been defined: a random identifier will be used for the locks'
            )

        if ignore_locks:
            await pipeline.run()
        else:
            await locks.run_with_lock(
                coro=pipeline.run(),
                lock=Lock.OADP_BUILD,
                lock_name=Lock.OADP_BUILD.value.format(group=group),
                lock_id=lock_identifier,
            )
    except Exception as err:
        error_message = f"build-oadp pipeline encountered error: {err}\n{traceback.format_exc()}"
        runtime.logger.error(error_message)
        raise
