import asyncio
import json
import logging
import os
import tempfile
import traceback
from importlib import util
from pathlib import Path
from typing import Optional, cast

import click
import requests
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.config.repo import BrewSource, BrewTag, PlashetRepo, Repo, RepoList
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import (
    get_ocp_version_from_group,
    new_roundtrip_yaml_handler,
    sync_to_quay,
)
from doozerlib.constants import ART_PROD_IMAGE_REPO, ART_PROD_PRIV_IMAGE_REPO, KONFLUX_DEFAULT_IMAGE_REPO

from pyartcd import constants, jenkins, oc
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.plashets import convert_plashet_config_to_new_style, plashet_config_for_major_minor
from pyartcd.runtime import Runtime
from pyartcd.util import (
    default_release_suffix,
    get_assembly_type,
    get_microshift_builds,
    get_release_name_for_assembly,
    isolate_el_version_in_release,
    load_group_config,
    load_releases_config,
)

yaml = new_roundtrip_yaml_handler()


class BuildMicroShiftBootcPipeline:
    """Rebase and build MicroShift for an assembly"""

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        force: bool,
        force_plashet_sync: bool,
        data_path: str,
        slack_client,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.force = force
        self.force_plashet_sync = force_plashet_sync
        self.slack_client = slack_client
        self._logger = logger or runtime.logger

        self._working_dir = self.runtime.working_dir.absolute()
        self.releases_config = None
        self.assembly_type = AssemblyTypes.STREAM
        self.group_config = None
        self.konflux_db = None

        # determines OCP version
        self._ocp_version = get_ocp_version_from_group(group)

        # sets environment variables for Elliott and Doozer
        self._elliott_env_vars = os.environ.copy()
        self._elliott_env_vars["ELLIOTT_WORKING_DIR"] = str(self._working_dir / "elliott-working")
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._working_dir / "doozer-working")

        if not data_path:
            data_path = (
                self.runtime.config.get("build_config", {}).get("ocp_build_data_url") or constants.OCP_BUILD_DATA_URL
            )
        if data_path:
            self._doozer_env_vars["DOOZER_DATA_PATH"] = data_path
            self._elliott_env_vars["ELLIOTT_DATA_PATH"] = data_path

    async def run(self):
        # Make sure our api.ci token is fresh
        await oc.registry_login()
        data_path = self._doozer_env_vars["DOOZER_DATA_PATH"]
        self.releases_config = await load_releases_config(
            group=self.group,
            data_path=data_path,
        )
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
        # Load group config
        self.group_config = await load_group_config(
            group=self.group, assembly=self.assembly, doozer_data_path=data_path
        )
        bootc_build = await self._rebase_and_build_bootc()
        if bootc_build:
            self._logger.info("Bootc image build: %s", bootc_build.nvr)
        else:
            raise ValueError(f"Could not find bootc image build for assembly {self.assembly}")

        # Login to Konflux registry
        cmd = [
            'oc',
            'registry',
            'login',
            '--registry',
            'quay.io/redhat-user-workloads',
        ]

        konflux_registry_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
        if konflux_registry_auth_file:
            cmd += [f'--registry-config={konflux_registry_auth_file}']
        await exectools.cmd_assert_async(cmd)

        # get image digests from manifest list for all arches
        cmd = [
            "skopeo",
            "inspect",
            f"docker://{bootc_build.image_pullspec}",
            "--raw",
        ]

        if konflux_registry_auth_file:
            cmd += [f'--authfile={konflux_registry_auth_file}']

        _, out, _ = await exectools.cmd_gather_async(cmd)
        manifest_list = json.loads(out)
        digest_by_arch = {m["platform"]["architecture"]: m["digest"] for m in manifest_list["manifests"]}
        self._logger.info("Bootc image digests by arch: %s", json.dumps(digest_by_arch, indent=4))

        if not self.runtime.dry_run:
            if bootc_build.embargoed:
                await sync_to_quay(bootc_build.image_pullspec, ART_PROD_PRIV_IMAGE_REPO)
            else:
                await sync_to_quay(bootc_build.image_pullspec, ART_PROD_IMAGE_REPO)
        else:
            self._logger.warning(
                "Skipping sync to quay.io/openshift-release-dev/ocp-v4.0-art-dev since in dry-run mode"
            )

    async def get_latest_bootc_build(self):
        bootc_image_name = "microshift-bootc"

        if not self.konflux_db:
            self.konflux_db = KonfluxDb()
            self.konflux_db.bind(KonfluxBuildRecord)
            self._logger.info('Konflux DB initialized')

        build = await self.konflux_db.get_latest_build(
            name=bootc_image_name,
            group=self.group,
            assembly=self.assembly,
            outcome=KonfluxBuildOutcome.SUCCESS,
            engine=Engine.KONFLUX,
            artifact_type=ArtifactType.IMAGE,
            el_target='el9',
        )
        return cast(Optional[KonfluxBuildRecord], build)

    async def _build_plashet_for_bootc(self):
        microshift_plashet_name = "rhel-9-server-microshift-rpms"
        major, minor = self._ocp_version
        # Get plashet repo configs
        assert self.group_config is not None, "Group config is not loaded; pyartcd bug?"
        if "all_repos" in self.group_config:
            # This is new-style repo config.
            # i.e. Plashet configs are defined in separate files in ocp-build-data
            # and each repo has its own config file.
            # Those repo definitions are stored in the "all_repos" key of the group config.
            self._logger.info("Using new-style plashet configs from build data")
            all_repos = self.group_config['all_repos']
            plashet_config = next(
                (
                    repo
                    for repo in RepoList.model_validate(all_repos).root
                    if not repo.disabled and repo.type == 'plashet' and repo.name == microshift_plashet_name
                ),
                None,
            )
            if not plashet_config:
                raise ValueError(f"Could not find plashet config for {microshift_plashet_name} in build data")
        else:
            old_style_config = plashet_config_for_major_minor(major, minor)[microshift_plashet_name]
            if not old_style_config:
                raise ValueError(f"Could not find plashet config for {microshift_plashet_name} for OCP {major}.{minor}")
            self._logger.warning("Using old-style plashet config. This is deprecated and will be removed in future.")
            plashet_config = convert_plashet_config_to_new_style({microshift_plashet_name: old_style_config})[0]

        async def _rebuild_needed():
            ocp_artifacts_url = next(r["url"] for r in constants.PLASHET_REMOTES if r["host"] == "ocp-artifacts")
            # check if we need to build plashet, skip if not
            # Example https://ocp-artifacts.hosts.prod.psi.rdu2.redhat.com/pub/RHOCP/plashets/4.18/microshift/microshift-el9/latest/plashet.yml
            assert plashet_config.type == "plashet" and plashet_config.plashet is not None
            slug = plashet_config.plashet.slug or microshift_plashet_name
            plashet_path = f"{major}.{minor}/{self.assembly}/{slug}"
            url = f"{ocp_artifacts_url}/{plashet_path}/latest/plashet.yml"
            self._logger.info(f"Inspecting plashet if it has the right microshift rpm: {url}")
            plashet_yaml = None
            try:
                res = requests.get(url)
                res.raise_for_status()
                plashet_yaml = yaml.load(res.content)
            except Exception as ex:
                self._logger.info(f"Could not find plashet sync details at {url}: {ex}")

            if not plashet_yaml:
                self._logger.info(f"Could not find plashet sync details at {url}. Plashet build is needed.")
                return True
            else:
                actual_nvr = next(
                    (p["nvr"] for p in plashet_yaml["assemble"]["packages"] if p["package_name"] == "microshift"), None
                )
                if not actual_nvr:
                    raise ValueError(
                        f"Expected to find microshift package in plashet.yml at"
                        f" {url}, but could not find it. Use --force to rebuild plashet."
                    )

                microshift_nvrs = await get_microshift_builds(self.group, self.assembly, env=self._elliott_env_vars)
                expected_microshift_nvr = next(
                    (n for n in microshift_nvrs if isolate_el_version_in_release(n) == 9), None
                )
                if not expected_microshift_nvr:
                    message = f"Could not find el9 microshift nvr for assembly {self.assembly}. Please investigate"
                    self._logger.info(message)
                    raise ValueError(message)
                if actual_nvr != expected_microshift_nvr:
                    self._logger.info(
                        f"Found nvr {actual_nvr} in plashet.yml is different from expected {expected_microshift_nvr}. Plashet build is needed."
                    )
                    return True
                self._logger.info(f"Plashet has the expected microshift nvr {expected_microshift_nvr}")
                return False

        rebuild_needed = self.force_plashet_sync or await _rebuild_needed()
        if not rebuild_needed:
            self._logger.info("Skipping plashet sync for %s", microshift_plashet_name)
            return

        jenkins.start_build_plashets(
            group=self.group,
            release=default_release_suffix(),
            assembly=self.assembly,
            repos=[microshift_plashet_name],
            data_path=self._doozer_env_vars["DOOZER_DATA_PATH"],
            dry_run=self.runtime.dry_run,
            block_until_complete=True,
        )

    async def _rebase_and_build_bootc(self):
        bootc_image_name = "microshift-bootc"
        major, minor = self._ocp_version
        # do not run for version < 4.18
        if major < 4 or (major == 4 and minor < 18):
            self._logger.info("Skipping bootc image build for version < 4.18")
            return

        # check if an image build already exists in Konflux DB
        if not self.force:
            build = await self.get_latest_bootc_build()
            if build:
                self._logger.info(
                    "Bootc image build exists for assembly: %s. Will use that. To force a rebuild use --force",
                    build.nvr,
                )
                return build
            else:
                self._logger.info("Bootc image build does not exist for assembly. Will proceed to build")
        else:
            self._logger.info("Force flag is set. Forcing bootc image build")

        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
        if not kubeconfig:
            raise ValueError(
                f"KONFLUX_SA_KUBECONFIG environment variable is required to build {bootc_image_name} image"
            )

        await self._build_plashet_for_bootc()

        # Rebase and build bootc image
        version = f"v{major}.{minor}.0"
        release = default_release_suffix()
        rebase_cmd = [
            "doozer",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "--latest-parent-version",
            "-i",
            bootc_image_name,
            # regardless of assembly cutoff time lock to HEAD in release branch
            # also not passing this breaks the command since we try to use brew to find the appropriate commit
            "--lock-upstream",
            bootc_image_name,
            "HEAD",
            "beta:images:konflux:rebase",
            "--version",
            version,
            "--release",
            release,
            "--message",
            f"Updating Dockerfile version and release {version}-{release}",
        ]
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")
        await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)

        build_cmd = [
            "doozer",
            "--group",
            self.group,
            "--assembly",
            self.assembly,
            "--latest-parent-version",
            "-i",
            bootc_image_name,
            # regardless of assembly cutoff time lock to HEAD in release branch
            # also not passing this breaks the command since we try to use brew to find the appropriate commit
            "--lock-upstream",
            bootc_image_name,
            "HEAD",
            "beta:images:konflux:build",
            "--image-repo",
            KONFLUX_DEFAULT_IMAGE_REPO,
            "--konflux-kubeconfig",
            kubeconfig,
            "--konflux-namespace",
            "ocp-art-tenant",
        ]
        if self.runtime.dry_run:
            build_cmd.append("--dry-run")
        await exectools.cmd_assert_async(build_cmd, env=self._doozer_env_vars)

        # sleep a little bit to account for time drift between systems
        await asyncio.sleep(10)

        # now that build is complete, fetch it
        return await self.get_latest_bootc_build()


@cli.command("build-microshift-bootc")
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
@click.option("--force", is_flag=True, help="Rebuild even if a build already exists")
@click.option("--force-plashet-sync", is_flag=True, help="Force plashet sync even if it is not needed")
@pass_runtime
@click_coroutine
async def build_microshift_bootc(
    runtime: Runtime, data_path: str, group: str, assembly: str, force: bool, force_plashet_sync: bool
):
    # slack client is dry-run aware and will not send messages if dry-run is enabled
    slack_client = runtime.new_slack_client()
    slack_client.bind_channel(group)
    try:
        pipeline = BuildMicroShiftBootcPipeline(
            runtime=runtime,
            group=group,
            assembly=assembly,
            force=force,
            force_plashet_sync=force_plashet_sync,
            data_path=data_path,
            slack_client=slack_client,
        )
        await pipeline.run()
    except Exception as err:
        slack_message = f"build-microshift-bootc pipeline encountered error: {err}"
        error_message = slack_message + f"\n {traceback.format_exc()}"
        runtime.logger.error(error_message)
        await slack_client.say_in_thread(slack_message)
        raise
