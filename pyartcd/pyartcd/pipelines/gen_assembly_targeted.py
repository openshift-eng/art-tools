"""
Pipeline for generating targeted assembly definitions.

Shells out to doozer's release:gen-assembly from-targeted subcommand to generate
the assembly YAML, then creates a PR against ocp-build-data. Supports kernel
NVR pinning, arbitrary image NVR pinning, or both.

Example usage (kernel fix):
    artcd gen-assembly-targeted \
        --group openshift-4.17 \
        --assembly 4.17.5 \
        --basis-assembly 4.17.4 \
        --kernel-nvr kernel-5.14.0-284.28.1.el9_2 \
        --bug-id CVE-2025-XXXX \
        --auto-trigger-build-sync

Example usage (image pin for RC transition):
    artcd gen-assembly-targeted \
        --group openshift-4.22 \
        --assembly rc.5 \
        --basis-assembly rc.4 \
        --image-nvr ose-installer-container-v4.22.0-202505200000.p0.el9
"""

import logging
import os
import traceback
from io import StringIO

import click
from artcommonlib import exectools
from artcommonlib.constants import (
    KONFLUX_DEFAULT_IMAGE_REPO,
    REGISTRY_CI_OPENSHIFT,
    REGISTRY_QUAY_OCP_RELEASE_DEV,
)
from artcommonlib.registry_config import RegistryConfig
from artcommonlib.util import new_roundtrip_yaml_handler

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.click_validators import validate_release_date
from pyartcd.runtime import Runtime
from pyartcd.util import create_or_update_assembly_pr

yaml = new_roundtrip_yaml_handler()


class GenAssemblyTargetedPipeline:
    """
    Generate a targeted assembly definition pinning specific kernel NVR(s)
    and/or image NVR(s), along with matching RHCOS and DTK builds.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        basis_assembly: str,
        kernel_nvrs: tuple[str, ...],
        bug_ids: tuple[str, ...] = (),
        cve_ids: tuple[str, ...] = (),
        image_nvrs: tuple[str, ...] = (),
        build_system: str = "konflux",
        data_path: str | None = None,
        release_date: str | None = None,
        auto_trigger_build_sync: bool = False,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.basis_assembly = basis_assembly
        self.kernel_nvrs = kernel_nvrs
        self.bug_ids = bug_ids
        self.cve_ids = cve_ids
        self.image_nvrs = image_nvrs
        self.build_system = build_system
        self.data_path = data_path
        self.release_date = release_date
        self.auto_trigger_build_sync = auto_trigger_build_sync
        self._logger = logging.getLogger(__name__)
        self._slack_client = self.runtime.new_slack_client()
        self._working_dir = self.runtime.working_dir.absolute()
        self._registry_config = None
        self._doozer_env_vars = os.environ.copy()

    async def run(self):
        """
        Entry point. Sets up registry auth and runs the pipeline.
        """
        if "XDG_RUNTIME_DIR" in os.environ:
            self._logger.info("Unsetting XDG_RUNTIME_DIR to prevent use of default registry auth")
            del os.environ["XDG_RUNTIME_DIR"]
        self._doozer_env_vars.pop("XDG_RUNTIME_DIR", None)

        quay_auth_file = os.getenv("QUAY_AUTH_FILE")
        if not quay_auth_file:
            raise ValueError(
                "QUAY_AUTH_FILE environment variable is required but not set. "
                "Ensure Jenkins credentials are properly bound."
            )

        source_files = [quay_auth_file]

        with RegistryConfig(
            kubeconfig=os.environ.get("KUBECONFIG"),
            source_files=source_files,
            registries=[
                REGISTRY_QUAY_OCP_RELEASE_DEV,
                KONFLUX_DEFAULT_IMAGE_REPO,
                REGISTRY_CI_OPENSHIFT,
            ],
        ) as global_auth_file:
            self._registry_config = global_auth_file
            self._logger.info(
                "Set registry auth file=%s for pipeline operations (cherry-picked from %d source file(s))",
                global_auth_file,
                len(source_files),
            )
            await self._run_pipeline()

    async def _run_pipeline(self):
        """
        Core pipeline logic with Slack notifications and error handling.
        """
        self._slack_client.bind_channel(self.group)
        slack_response = await self._slack_client.say(
            f":construction: Generating targeted assembly {self.assembly} "
            f"(basis: {self.basis_assembly}, kernel: {', '.join(self.kernel_nvrs)}) :construction:"
        )
        slack_thread = slack_response["message"]["ts"]

        try:
            assembly_definition = await self._gen_assembly_from_targeted()

            out = StringIO()
            yaml.dump(assembly_definition, out)
            self._logger.info("Generated targeted assembly definition:\n%s", out.getvalue())

            pr = await self._create_or_update_pull_request(assembly_definition)
            if not pr:
                self._logger.warning("No PR update was made")
                return

            message = (
                f"Targeted assembly {self.assembly} PR created: {pr.html_url}\n"
                f"Kernel: {', '.join(self.kernel_nvrs)}\n"
                f"Bugs: {', '.join(self.bug_ids)}"
            )
            await self._slack_client.say(message, slack_thread)

        except Exception as err:
            error_message = f"Error generating targeted assembly: {err}\n{traceback.format_exc()}"
            self._logger.error(error_message)
            await self._slack_client.say(
                f"Error generating targeted assembly {self.assembly}: {err}",
                slack_thread,
            )
            raise

    async def _gen_assembly_from_targeted(self) -> dict:
        """
        Run doozer release:gen-assembly from-targeted to generate the assembly definition.

        Return Value(s):
            dict: Assembly definition YAML structure
        """
        cmd = [
            "doozer",
            "--group",
            self.group,
            "--assembly",
            "stream",
            "--build-system",
            self.build_system,
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")
        if self._registry_config:
            cmd.append(f"--registry-config={self._registry_config}")
        cmd.append("release:gen-assembly")
        cmd.append(f"--name={self.assembly}")
        cmd.append("from-targeted")
        cmd.append(f"--basis-assembly={self.basis_assembly}")
        for nvr in self.kernel_nvrs:
            cmd.append(f"--kernel-nvr={nvr}")
        for bug in self.bug_ids:
            cmd.append(f"--bug-id={bug}")
        for cve in self.cve_ids:
            cmd.append(f"--cve-id={cve}")
        for nvr in self.image_nvrs:
            cmd.append(f"--image-nvr={nvr}")
        if self.release_date:
            cmd.append(f"--date={self.release_date}")

        _, out, _ = await exectools.cmd_gather_async(cmd, stderr=None, env=self._doozer_env_vars)
        return yaml.load(out)

    async def _create_or_update_pull_request(self, assembly_definition: dict):
        """
        Create or update pull request for ocp-build-data using shared PR logic.

        Arg(s):
            assembly_definition: Assembly definition to merge into releases.yml
        """
        title = f"Add targeted assembly {self.assembly}"
        body = (
            f"Targeted kernel fix assembly.\n\n"
            f"Kernel: {', '.join(self.kernel_nvrs)}\n"
            f"Basis assembly: {self.basis_assembly}\n"
            f"Bugs: {', '.join(self.bug_ids)}\n"
            f"Created by job run {jenkins.get_build_url()}"
        )

        return await create_or_update_assembly_pr(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            build_system=self.build_system,
            assembly_definition=assembly_definition,
            title=title,
            body=body,
            working_dir=self._working_dir,
            logger=self._logger,
            auto_trigger_build_sync=self.auto_trigger_build_sync,
        )


@cli.command("gen-assembly-targeted")
@click.option(
    "--data-path",
    metavar="BUILD_DATA",
    default=None,
    help=f"Git repo or directory containing groups metadata e.g. {constants.OCP_BUILD_DATA_URL}",
)
@click.option(
    "-g",
    "--group",
    metavar="NAME",
    required=True,
    help="The group of components on which to operate. e.g. openshift-4.17",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The name of the targeted assembly to generate. e.g. 4.17.5",
)
@click.option(
    "--basis-assembly",
    metavar="BASIS_ASSEMBLY",
    required=True,
    help="The basis assembly to inherit from. e.g. 4.17.4",
)
@click.option(
    "--kernel-nvr",
    "kernel_nvrs",
    metavar="NVR",
    multiple=True,
    required=False,
    help="[MULTIPLE] Kernel NVR(s) to pin. Provide one per RHEL target if needed.",
)
@click.option(
    "--bug-id",
    "bug_ids",
    metavar="BUG_ID",
    multiple=True,
    required=False,
    help="[MULTIPLE] Jira issue IDs to include (e.g. OCPBUGS-85292). When provided, sets targeted_fixes_only.",
)
@click.option(
    "--cve-id",
    "cve_ids",
    metavar="CVE_ID",
    multiple=True,
    required=False,
    help="[MULTIPLE] CVE identifiers for the 'why' text (e.g. CVE-2026-43284).",
)
@click.option(
    "--image-nvr",
    "image_nvrs",
    metavar="NVR",
    multiple=True,
    required=False,
    help="[MULTIPLE] Additional image NVR(s) to pin alongside kernel/DTK.",
)
@click.option(
    "--build-system",
    metavar="BUILD_SYSTEM",
    default="konflux",
    help="What build system we're operating on ('brew'|'konflux')",
)
@click.option(
    "--date",
    "release_date",
    metavar="YYYY-Mon-DD",
    default=None,
    callback=validate_release_date,
    help="Expected release date (e.g. 2026-Mar-31 or 2026-03-31)",
)
@click.option(
    "--auto-trigger-build-sync",
    is_flag=True,
    help="Trigger build-sync automatically after PR creation",
)
@pass_runtime
@click_coroutine
async def gen_assembly_targeted(
    runtime: Runtime,
    data_path: str | None,
    group: str,
    assembly: str,
    basis_assembly: str,
    kernel_nvrs: tuple[str, ...],
    bug_ids: tuple[str, ...],
    cve_ids: tuple[str, ...],
    image_nvrs: tuple[str, ...],
    build_system: str,
    release_date: str | None,
    auto_trigger_build_sync: bool,
):
    pipeline = GenAssemblyTargetedPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        basis_assembly=basis_assembly,
        kernel_nvrs=kernel_nvrs,
        bug_ids=bug_ids,
        cve_ids=cve_ids,
        image_nvrs=image_nvrs,
        build_system=build_system,
        data_path=data_path,
        release_date=release_date,
        auto_trigger_build_sync=auto_trigger_build_sync,
    )
    await pipeline.run()
