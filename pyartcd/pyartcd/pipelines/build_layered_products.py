import logging
import os
import traceback
from pathlib import Path
from typing import List, Optional

import click
import yaml
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


class BuildLayeredProductsPipeline:
    """Rebase and build layered products for an assembly"""

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
        network_mode: Optional[str] = None,
        plr_template: Optional[str] = None,
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
        self.network_mode = network_mode
        self.plr_template = plr_template
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
        if self.assembly.lower() == "test":
            jenkins.update_title(" [TEST]")

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
                # Automatically propagate parameters if set in environment
                propagate_params = jenkins.get_propagatable_params()

                jenkins.start_olm_bundle_konflux(
                    build_version=self.version,
                    assembly=self.assembly,
                    group=self.group,
                    operator_nvrs=operator_nvrs,
                    doozer_data_path=self._doozer_env_vars["DOOZER_DATA_PATH"] or '',
                    doozer_data_gitref=self.data_gitref or '',
                    propagate_params=propagate_params,
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
        """Run the layered products rebase and build pipeline"""
        # Load group config once for both version and product information
        group_config = await load_group_config(
            group=self.group,
            assembly=self.assembly,
            doozer_data_path=self._doozer_env_vars.get('DOOZER_DATA_PATH'),
            doozer_data_gitref=self.data_gitref,
        )

        # Set version from group config if not provided
        if not self.version:
            self.version = group_config.get('version')
            if not self.version:
                raise ValueError(f"No version found in group config for {self.group}")
            self._logger.info(f"Using version {self.version} from group config")

        # Extract product from group config
        product = group_config.get('product', 'ocp')
        await self._rebase_and_build(product)
        self.trigger_bundle_build()

    def _group_param(self) -> str:
        if self.data_gitref:
            return f"--group={self.group}@{self.data_gitref}"
        return f"--group={self.group}"

    def _doozer_base_command(self) -> List[str]:
        return [
            "doozer",
            f"--assembly={self.assembly}",
            f"--working-dir={self.runtime.doozer_working}",
            f"--data-path={self._doozer_env_vars['DOOZER_DATA_PATH']}",
            "--build-system=konflux",
            self._group_param(),
            "--latest-parent-version",
        ]

    async def _rebase_and_build(self, product: str):
        """Rebase and build layered product image"""
        image_list = self.image_list

        if not self.skip_rebase:
            image_list = await self._rebase(image_list)
        else:
            self._logger.warning("Skipping rebase step because --skip-rebase flag is set")

        if not image_list:
            self._logger.warning('No buildable images remaining after rebase; skipping build')
            return

        await self._build(image_list, product)

    async def _rebase(self, image_list: str) -> str:
        """Rebase layered product images.

        Returns the (possibly reduced) comma-separated image list after
        excluding images that failed to rebase, following the same pattern
        used by the OCP4 pipeline.
        """
        release = default_release_suffix()
        self._logger.info(f"Rebasing {image_list} image(s) for assembly {self.assembly}")

        rebase_cmd = self._doozer_base_command()
        rebase_cmd.extend(
            [
                f"--images={image_list}",
                "beta:images:konflux:rebase",
                f"--version={self.version}",
                f"--release={release}",
                f"--message='Updating Dockerfile version and release {self.version}-{release}'",
            ]
        )
        if self.network_mode:
            rebase_cmd.extend(['--network-mode', self.network_mode])
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")

        try:
            await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)
            self._logger.info(f"Successfully rebased {image_list}")
            return image_list

        except ChildProcessError:
            state_path = Path(self.runtime.doozer_working, 'state.yaml')
            if not state_path.exists():
                raise

            with state_path.open('r') as f:
                state = yaml.safe_load(f)
            failed_images = state.get('images:konflux:rebase', {}).get('failed-images', [])
            if not failed_images:
                raise

            self._logger.warning(
                'Following images failed to rebase and will be excluded from build: %s',
                ','.join(failed_images),
            )

            requested = [img.strip() for img in image_list.split(',') if img.strip()]
            remaining = [img for img in requested if img not in failed_images]
            return ','.join(remaining)

    async def _build(self, image_list: str, product: str):
        """Build layered product images."""
        self._logger.info(f"Building {image_list} image(s) for assembly {self.assembly}")

        build_cmd = self._doozer_base_command()
        build_cmd.extend(
            [
                f"--images={image_list}",
                "beta:images:konflux:build",
                f"--image-repo={KONFLUX_DEFAULT_IMAGE_REPO}",
                "--build-priority=1",
            ]
        )

        namespace = resolve_konflux_namespace_by_product(product)
        build_cmd.append(f"--konflux-namespace={namespace}")

        kubeconfig = resolve_konflux_kubeconfig_by_product(product, self.kubeconfig)
        if not kubeconfig:
            available_env_vars = list(PRODUCT_KUBECONFIG_MAP.values())
            raise ValueError(
                f"Kubeconfig required for Konflux builds. Provide --kubeconfig parameter "
                f"or set one of: {', '.join(available_env_vars)}"
            )

        build_cmd.extend(["--konflux-kubeconfig", kubeconfig])
        if self.plr_template:
            plr_template_owner, plr_template_branch = (
                self.plr_template.split("@") if self.plr_template else ["openshift-priv", "main"]
            )
            plr_template_url = constants.KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=plr_template_owner, branch_name=plr_template_branch
            )
            build_cmd.extend(['--plr-template', plr_template_url])
        if self.network_mode:
            build_cmd.extend(['--network-mode', self.network_mode])
        if self.runtime.dry_run:
            build_cmd.append("--dry-run")

        build_env = self._doozer_env_vars.copy()
        konflux_registry_auth_file = os.getenv("QUAY_AUTH_FILE")
        if konflux_registry_auth_file:
            build_env["QUAY_AUTH_FILE"] = konflux_registry_auth_file

        await exectools.cmd_assert_async(build_cmd, env=build_env)
        self._logger.info(f"Successfully built {image_list}")


@cli.command("build-layered-products")
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
    help="Layered product version (if not provided, will be read from group config)",
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
@click.option(
    '--network-mode',
    type=click.Choice(['hermetic', 'internal-only', 'open']),
    help='Override network mode for Konflux builds. Takes precedence over image and group config settings.',
)
@click.option("--ignore-locks", is_flag=True, default=False, help="(For testing) Do not wait for locks")
@click.option(
    '--plr-template',
    required=False,
    default='',
    help='Override the Pipeline Run template commit from openshift-priv/art-konflux-template; format: <owner>@<branch>',
)
@pass_runtime
@click_coroutine
async def build_layered_products(
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
    network_mode: Optional[str],
    ignore_locks: bool,
    plr_template: str,
):
    """Rebase and build layered product image for an assembly"""
    try:
        pipeline = BuildLayeredProductsPipeline(
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
            network_mode=network_mode,
            plr_template=plr_template,
        )

        lock_identifier = jenkins.get_build_path_or_random()

        if ignore_locks:
            await pipeline.run()
        else:
            await locks.run_with_lock(
                coro=pipeline.run(),
                lock=Lock.LAYERED_PRODUCTS_BUILD,
                lock_name=Lock.LAYERED_PRODUCTS_BUILD.value.format(group=group),
                lock_id=lock_identifier,
            )
    except Exception as err:
        error_message = f"build-layered-products pipeline encountered error: {err}\n{traceback.format_exc()}"
        runtime.logger.error(error_message)
        raise
