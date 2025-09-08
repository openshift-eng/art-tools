import asyncio
import copy
import io
import json
import logging
import os
import tempfile
import traceback
from pathlib import Path
from typing import Optional, cast

import click
import requests
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import (
    get_ocp_version_from_group,
    new_roundtrip_yaml_handler,
    sync_to_quay,
)
from doozerlib.constants import ART_PROD_IMAGE_REPO, ART_PROD_PRIV_IMAGE_REPO, KONFLUX_DEFAULT_IMAGE_REPO
from github import Github, GithubException

from pyartcd import constants, jenkins, oc
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.plashets import plashet_config_for_major_minor
from pyartcd.runtime import Runtime
from pyartcd.util import (
    default_release_suffix,
    get_assembly_type,
    get_image_if_pinned_directly,
    get_microshift_builds,
    get_release_name_for_assembly,
    isolate_el_version_in_release,
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

        # Check if GitHub token is available (unless in dry-run mode)
        if not runtime.dry_run:
            github_token = os.environ.get("GITHUB_TOKEN")
            if not github_token or not github_token.strip():
                raise ValueError("GITHUB_TOKEN environment variable is required to create pull requests")

        self.github_client = Github(os.environ.get("GITHUB_TOKEN"))

        self._working_dir = self.runtime.working_dir.absolute()
        self.releases_config = None
        self.assembly_type = AssemblyTypes.STREAM
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
        self.releases_config = await load_releases_config(
            group=self.group,
            data_path=self._doozer_env_vars["DOOZER_DATA_PATH"],
        )
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
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
                # sync per-arch bootc-pullspec.txt to mirror
                await asyncio.gather(
                    *(
                        self.sync_to_mirror(arch, bootc_build.el_target, f"{ART_PROD_IMAGE_REPO}@{digest}")
                        for arch, digest in digest_by_arch.items()
                    ),
                )
        else:
            self._logger.warning(
                "Skipping sync to quay.io/openshift-release-dev/ocp-v4.0-art-dev since in dry-run mode"
            )

        # Pin the image to the assembly if not STREAM
        if self.assembly_type != AssemblyTypes.STREAM:
            # Check if we need to create a PR to pin the build
            pinned_nvr = get_image_if_pinned_directly(self.releases_config, self.assembly, 'microshift-bootc')
            if bootc_build.nvr != pinned_nvr:
                self._logger.info("Creating PR to pin microshift-bootc image: %s", bootc_build.nvr)
                pr_url = await self._create_or_update_pull_request_for_image(bootc_build.nvr)
                message = f"PR to pin microshift-bootc image to the {self.assembly} assembly has been merged: {pr_url}"
                await self.slack_client.say_in_thread(message)

    async def sync_to_mirror(self, arch, el_target, pullspec):
        arch = brew_arch_for_go_arch(arch)
        release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        ocp_dir = "ocp-dev-preview" if self.assembly_type == AssemblyTypes.PREVIEW else "ocp"

        major, minor = self._ocp_version
        # This is where we sync microshift artifacts on mirror. Refer to microshift_sync job
        # The paths should be the same in both of these places
        release_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/{release_name}/{el_target}"
        latest_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/latest-{major}.{minor}/{el_target}"
        cloudflare_endpoint_url = os.environ['CLOUDFLARE_ENDPOINT']

        async def _run_for(local_path, s3_path):
            cmd = [
                "aws",
                "s3",
                "sync",
                "--no-progress",
                "--exact-timestamps",
                "--exclude",
                "*",
                "--include",
                "bootc-pullspec.txt",
                "--cache-control",
                "no-cache, no-store, must-revalidate",
                "--metadata-directive",
                "REPLACE",
                str(local_path),
                f"s3://art-srv-enterprise{s3_path}",
            ]
            if self.runtime.dry_run:
                cmd.append("--dryrun")

            await asyncio.gather(
                # Sync to S3
                exectools.cmd_assert_async(cmd),
                # Sync to Cloudflare as well
                exectools.cmd_assert_async(
                    cmd
                    + [
                        "--profile",
                        "cloudflare",
                        "--endpoint-url",
                        cloudflare_endpoint_url,
                    ]
                ),
            )

        with tempfile.TemporaryDirectory(dir=self._working_dir) as local_dir:
            local_path = Path(local_dir, "bootc-pullspec.txt")
            with open(local_path, "w") as f:
                f.write(pullspec)
            self._logger.info(f"write {pullspec} to {local_path}")
            await _run_for(local_dir, release_path)
            await _run_for(local_dir, latest_path)

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
        microshift_plashet_config = plashet_config_for_major_minor(major, minor)[microshift_plashet_name]

        async def _rebuild_needed():
            ocp_artifacts_url = next(r["url"] for r in constants.PLASHET_REMOTES if r["host"] == "ocp-artifacts")
            # check if we need to build plashet, skip if not
            # Example https://ocp-artifacts.hosts.prod.psi.rdu2.redhat.com/pub/RHOCP/plashets/4.18/microshift/microshift-el9/latest/plashet.yml
            plashet_path = f"{major}.{minor}/{self.assembly}/{microshift_plashet_config['slug']}"
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
            version=f"{major}.{minor}",
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

        # Check if an image is already pinned and don't rebuild unless forced
        if not self.force and self.assembly_type != AssemblyTypes.STREAM:
            pinned_nvr = get_image_if_pinned_directly(self.releases_config, self.assembly, bootc_image_name)
            if pinned_nvr:
                message = f"For assembly {self.assembly} microshift-bootc image is already pinned: {pinned_nvr}. Use FORCE to rebuild."
                self._logger.info(message)
                await self.slack_client.say_in_thread(message)

                # Fetch the actual build record from Konflux DB using the pinned NVR
                if not self.konflux_db:
                    self.konflux_db = KonfluxDb()
                    self.konflux_db.bind(KonfluxBuildRecord)
                    self._logger.info('Konflux DB initialized for pinned build lookup')

                build = await self.konflux_db.get_build_record_by_nvr(pinned_nvr, strict=True)
                self._logger.info("Found existing build record for pinned NVR: %s", pinned_nvr)
                return build

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

    def extract_git_repo(self, data_path: str):
        """
        extract git repo name from data path
        https://github.com/openshift-eng/ocp-build-data --> openshift-eng, ocp-build-data
        """
        data_path = data_path.rstrip(".git")
        parts = data_path.rstrip("/").split("/")
        if len(parts) < 2:
            raise ValueError(f"Invalid git repo URL: {data_path}")
        return parts[-2], parts[-1]

    def _pin_image_nvr(self, nvr: str, releases_config) -> dict:
        """Update releases.yml to pin the specified image NVR.
        Example:
            releases:
                4.18.1:
                    assembly:
                        members:
                            images:
                            - distgit_key: microshift-bootc
                              metadata:
                                is:
                                  nvr: microshift-bootc-4.18.1-202311300751.p0.g7ebffc3.assembly.4.18.1.el9
        """
        dg_key = "microshift-bootc"
        image_pin = {
            "distgit_key": dg_key,
            "metadata": {
                "is": {"nvr": nvr},
            },
            "why": "Pin microshift-bootc image to assembly",
        }

        images_entry = (
            releases_config["releases"][self.assembly]
            .setdefault("assembly", {})
            .setdefault("members", {})
            .setdefault("images", [])
        )

        # Check if microshift-bootc entry already exists
        microshift_bootc_entry = next(filter(lambda img: img.get("distgit_key") == dg_key, images_entry), None)
        if microshift_bootc_entry is None:
            images_entry.append(image_pin)
            return image_pin
        else:
            # Update existing entry
            microshift_bootc_entry["metadata"]["is"] = image_pin["metadata"]["is"]
            return microshift_bootc_entry

    async def _create_or_update_pull_request_for_image(self, nvr: str):
        branch = f"auto-pin-microshift-bootc-{self.group}-{self.assembly}"
        title = f"Pin microshift-bootc image for {self.group} {self.assembly}"
        body = f"Created by job run {jenkins.get_build_url()}"
        if self.runtime.dry_run:
            self._logger.warning(
                "[DRY RUN] Would have created pull-request with head '%s', title '%s', body '%s'",
                branch,
                title,
                body,
            )
            return "https://github.example.com/foo/bar/pull/1234"

        user, repo = self.extract_git_repo(self._doozer_env_vars["DOOZER_DATA_PATH"])
        upstream_repo = self.github_client.get_repo(f"{user}/{repo}")
        release_file_content = yaml.load(upstream_repo.get_contents("releases.yml", ref=self.group).decoded_content)
        source_file_content = copy.deepcopy(release_file_content)
        self._pin_image_nvr(nvr, release_file_content)

        if source_file_content == release_file_content:
            self._logger.warning("PR is not created: upstream already updated, nothing to change.")
            return "Nothing to change"

        # Delete existing branch if it exists
        for b in upstream_repo.get_branches():
            if b.name == branch:
                upstream_repo.get_git_ref(f"heads/{branch}").delete()

        # Create new branch
        upstream_repo.create_git_ref(f"refs/heads/{branch}", upstream_repo.get_branch(self.group).commit.sha)

        # Update releases.yml
        output = io.BytesIO()
        yaml.dump(release_file_content, output)
        output.seek(0)
        fork_file = upstream_repo.get_contents("releases.yml", ref=branch)
        upstream_repo.update_file("releases.yml", body, output.read(), fork_file.sha, branch=branch)

        # Create and merge PR
        pr = upstream_repo.create_pull(title=title, body=body, base=self.group, head=branch)
        pr.merge()
        return pr.html_url


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
