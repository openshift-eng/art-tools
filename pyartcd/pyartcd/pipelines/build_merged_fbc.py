import logging
from typing import List

import click
from artcommonlib import exectools
from ruamel.yaml import YAML

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

yaml = YAML(typ="safe")


class BuildMergedFbcPipeline:
    def __init__(
        self,
        runtime: Runtime,
        version: str,
        data_path: str,
        data_gitref: str,
        only: str,
        exclude: str,
        fbc_repo: str,
        kubeconfig: str,
        plr_template: str,
        skip_checks: bool,
    ):
        self.runtime = runtime
        self.version = version
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.only = only
        self.exclude = exclude
        self.fbc_repo = fbc_repo
        self.kubeconfig = kubeconfig
        self.plr_template = plr_template
        self.skip_checks = skip_checks

        self._logger = logging.getLogger(__name__)
        self._slack_client = runtime.new_slack_client()
        self._slack_client.bind_channel(version)

    async def run(self):
        logger = self._logger
        try:
            logger.info("Loading OCP build data for version %s", self.version)
            meta_configs = await self._get_meta_configs()
            operator_dgks = [dgk for dgk, config in meta_configs.get("images", {}).items() if "update-csv" in config]

            logger.info("Merging FBC fragments for OCP %s", self.version)
            target_index = await self._build_merged_fbc(operator_dgks)

            logger.info("Successfully built merged FBC for OCP %s: %s", self.version, target_index)

        except Exception as e:
            self._logger.error("Encountered error: %s", e)
            await self._slack_client.say(
                f"*:heavy_exclamation_mark: Error building merged stage FBC for {self.version}*\n"
            )
            raise

    async def _run_doozer(self, opts: List[str], only: str, exclude: str):
        cmd = [
            "doozer",
            "--build-system=konflux",
            f"--working-dir={self.runtime.doozer_working}",
            "--assembly=stream",
            f"--group=openshift-{self.version}{'@' + self.data_gitref if self.data_gitref else ''}",
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")
        if only:
            cmd.append(f"--images={only}")
        if exclude:
            cmd.append(f"--exclude={exclude}")
        cmd += opts
        self._logger.info(f"Running doozer command: {' '.join(cmd)}")
        return await exectools.cmd_gather_async(cmd, stderr=None)

    async def _build_merged_fbc(self, operator_dgks: List[str]):
        version = self.version
        target_index = f"quay.io/openshift-art/stage-fbc-fragments:ocp-{version}"
        doozer_opts = [
            "beta:fbc:merge",
            f"--target-index={target_index}",
        ]
        if self.runtime.dry_run:
            doozer_opts.append("--dry-run")
        if self.fbc_repo:
            doozer_opts.extend(["--fbc-repo", self.fbc_repo])
        if self.kubeconfig:
            doozer_opts.extend(["--konflux-kubeconfig", self.kubeconfig])
        if self.plr_template:
            plr_template_owner, plr_template_branch = (
                self.plr_template.split("@") if self.plr_template else ["openshift-priv", "main"]
            )
            plr_template_url = constants.KONFLUX_FBC_BUILD_PLR_TEMPLATE_URL_FORMAT.format(
                owner=plr_template_owner, branch_name=plr_template_branch
            )
            doozer_opts.extend(["--plr-template", plr_template_url])
        if self.skip_checks:
            doozer_opts.append("--skip-checks")
        doozer_opts.append("--")
        stage_index_repo = "quay.io/openshift-art/stage-fbc-fragments"
        for dgk in operator_dgks:
            doozer_opts.append(f"{stage_index_repo}:ocp__{version}__{dgk}")
        await self._run_doozer(doozer_opts, only=self.only, exclude=self.exclude)
        return target_index

    async def _get_meta_configs(self) -> List[dict]:
        """
        Retrieve the metadata configurations
        """
        # Run doozer config:print for the specified DKGs
        doozer_opts = [
            "config:print",
            "--yaml",
        ]
        _, out, _ = await self._run_doozer(doozer_opts, only=self.only, exclude=self.exclude)
        configs = yaml.load(out)
        return configs


@cli.command("build-merged-fbc", help="Merge per-component FBC fragments into one single operator index")
@click.option("--version", required=True, help="OCP version")
@click.option(
    "--data-path",
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use (e.g. assembly definition in your own fork)",
)
@click.option("--data-gitref", required=False, help="(Optional) Doozer data path git [branch / tag / sha] to use")
@click.option(
    "--only",
    required=False,
    help="(Optional) List **only** the operators you want to build, everything else gets ignored.\n"
    "Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n"
    "Leave empty to build all (except EXCLUDE, if defined)",
)
@click.option(
    "--exclude",
    required=False,
    help="(Optional) List the operators you **don't** want to build, everything else gets built.\n"
    "Format: Comma and/or space separated list of brew packages (e.g.: cluster-nfd-operator-container)\n"
    "Leave empty to build all (or ONLY, if defined)",
)
@click.option("--fbc-repo", required=False, default="", help="(Optional) URL of the FBC repository")
@click.option("--kubeconfig", required=False, help="Path to kubeconfig file to use for Konflux cluster connections")
@click.option(
    "--plr-template",
    required=False,
    default="",
    help="Override the Pipeline Run template commit from openshift-priv/art-konflux-template; format: <owner>@<branch>",
)
@click.option("--skip-checks", is_flag=True, help="Skip all post build checks in the FBC build pipeline")
@pass_runtime
@click_coroutine
async def build_merged_fbc(
    runtime: Runtime,
    version: str,
    data_path: str,
    data_gitref: str,
    only: str,
    exclude: str,
    fbc_repo: str,
    kubeconfig: str,
    plr_template: str,
    skip_checks: bool,
):
    pipeline = BuildMergedFbcPipeline(
        runtime=runtime,
        version=version,
        data_path=data_path,
        data_gitref=data_gitref,
        only=only,
        exclude=exclude,
        fbc_repo=fbc_repo,
        kubeconfig=kubeconfig,
        plr_template=plr_template,
        skip_checks=skip_checks,
    )
    await pipeline.run()
