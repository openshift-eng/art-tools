import asyncio
import copy
import glob
import io
import json
import logging
import os
import shutil
import tempfile
import time
import traceback
from datetime import datetime, timezone
from io import StringIO
from importlib import util
from pathlib import Path
from typing import Optional, cast
from urllib.parse import urlparse

import click
import gitlab
import requests
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.config.repo import BrewSource, BrewTag, PlashetRepo, Repo, RepoList
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.model import Model
from artcommonlib.util import (
    convert_remote_git_to_ssh,
    get_ocp_version_from_group,
    new_roundtrip_yaml_handler,
    sync_to_quay,
)
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from doozerlib.constants import ART_PROD_IMAGE_REPO, ART_PROD_PRIV_IMAGE_REPO, KONFLUX_DEFAULT_IMAGE_REPO
from elliottlib.shipment_model import ShipmentConfig, Snapshot, SnapshotSpec
from github import Github, GithubException

from pyartcd import constants, jenkins, oc
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.plashets import plashet_config_for_major_minor
from pyartcd.plashets import convert_plashet_config_to_new_style, plashet_config_for_major_minor
from pyartcd.runtime import Runtime
from pyartcd.util import (
    default_release_suffix,
    get_assembly_type,
    get_image_if_pinned_directly,
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
        prepare_shipment: bool,
        data_path: str,
        slack_client,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.force = force
        self.force_plashet_sync = force_plashet_sync
        self.prepare_shipment = prepare_shipment
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
        self.group_config = None
        self.konflux_db = None

        # determines OCP version
        self._ocp_version = get_ocp_version_from_group(group)

        # Shipment infrastructure setup
        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.product = 'ocp'  # assume that product is ocp for now
        self._shipment_data_repo_dir = self._working_dir / "shipment-data-push"

        # Setup shipment data repo URLs
        self.shipment_data_repo_pull_url = self.runtime.config.get("shipment_config", {}).get(
            "shipment_data_url"
        ) or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)
        self.shipment_data_repo_push_url = self.runtime.config.get("shipment_config", {}).get(
            "shipment_data_push_url"
        ) or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)

        # Initialize GitRepository for shipment data (will be setup later if needed)
        self.shipment_data_repo = None
        self.gitlab_token = None

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

        # Setup Elliott base command for shipment operations
        group_param = f'--group={group}'
        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            f'--working-dir={self._working_dir / "elliott-working"}',
            f'--data-path={data_path or constants.OCP_BUILD_DATA_URL}',
        ]

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
                await self._create_or_update_pull_request_for_image(bootc_build.nvr)

            if self.prepare_shipment:
                await self._prepare_shipment(bootc_build)

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

        # Create PR
        pr = upstream_repo.create_pull(title=title, body=body, base=self.group, head=branch)
        self._logger.info("Created PR to pin microshift-bootc image: %s", pr.html_url)
        await self.slack_client.say_in_thread(f"PR created to pin microshift-bootc image: {pr.html_url}")

        # Wait for CI tests to pass, then merge
        await self._wait_for_pr_merge(pr)
        return pr.html_url

    async def _wait_for_pr_merge(self, pr):
        """Wait for a PR to pass CI tests and then merge it, with timeout."""
        timeout = 30 * 60  # 30 minutes
        start_time = time.time()
        check_interval = 30  # Check every 30 seconds

        self._logger.info("Waiting for PR CI tests to pass before merging: %s", pr.html_url)
        await self.slack_client.say_in_thread(f"Waiting for CI tests to pass for PR: {pr.html_url}")

        while True:
            if (time.time() - start_time) > timeout:
                error_msg = f"Timeout waiting for PR CI tests to pass (30 minutes): {pr.html_url}"
                self._logger.error(error_msg)
                await self.slack_client.say_in_thread(f"❌ {error_msg}")
                raise TimeoutError(error_msg)

            # Refresh PR to get latest status
            pr = self.github_client.get_repo(
                f"{self.extract_git_repo(self._doozer_env_vars['DOOZER_DATA_PATH'])[0]}/{self.extract_git_repo(self._doozer_env_vars['DOOZER_DATA_PATH'])[1]}"
            ).get_pull(pr.number)

            # Check if PR was closed/merged by someone else
            if pr.state == "closed":
                if pr.merged:
                    self._logger.info("PR was already merged: %s", pr.html_url)
                    await self.slack_client.say_in_thread(f"✅ PR was merged: {pr.html_url}")
                    return
                else:
                    error_msg = f"PR was closed without merging: {pr.html_url}"
                    self._logger.error(error_msg)
                    await self.slack_client.say_in_thread(f"❌ {error_msg}")
                    raise RuntimeError(error_msg)

            # Try to merge - let GitHub tell us if CI is ready
            try:
                merge_result = pr.merge()
                if merge_result.merged:
                    self._logger.info("PR successfully merged: %s", pr.html_url)
                    await self.slack_client.say_in_thread(f"✅ PR merged successfully: {pr.html_url}")
                    return
                else:
                    self._logger.info("PR merge not ready: %s", merge_result.message)
            except GithubException as e:
                if e.status == 405 and "Required status check" in str(e):
                    self._logger.info("CI checks still running, waiting...")
                elif e.status == 405 and "Pull Request is not mergeable" in str(e):
                    self._logger.info("PR not mergeable yet (conflicts or checks), waiting...")
                else:
                    # Other errors should be reported but we continue waiting
                    self._logger.warning("Merge attempt failed: %s", e)
                    await self.slack_client.say_in_thread(f"⚠️ Merge attempt failed: {e}")

            await asyncio.sleep(check_interval)

    async def _prepare_shipment(self, bootc_build):
        """Prepare shipment for microshift-bootc"""
        await self.slack_client.say_in_thread(f"Start preparing shipment for assembly {self.assembly}..")

        # Step 1: Check environment variables and setup
        await self._setup_shipment_environment()

        # Step 2: Setup shipment data repository first
        await self._setup_shipment_data_repo()

        # Step 3: Check for existing shipment branch and try to load existing config
        source_branch = f"prepare-microshift-bootc-shipment-{self.assembly}"
        shipment_config = await self._load_or_init_shipment_config(source_branch)

        # Step 4: Use the provided bootc build
        self._logger.info("Using bootc build: %s", bootc_build.nvr)

        # Step 5: Create snapshot from bootc build
        snapshot = await self._create_snapshot(bootc_build.nvr)
        shipment_config.shipment.snapshot = snapshot

        # Step 6: Create shipment MR
        shipment_mr_url = await self._create_shipment_mr(shipment_config)

        await self.slack_client.say_in_thread(f"Shipment MR created: {shipment_mr_url}")
        await self.slack_client.say_in_thread("Completed preparing microshift-bootc shipment.")

    async def _setup_shipment_environment(self):
        """Setup environment variables and tokens required for shipment operations"""
        # Check for GitLab token
        self.gitlab_token = os.getenv("GITLAB_TOKEN")
        if not self.gitlab_token and not self.runtime.dry_run:
            raise ValueError("GITLAB_TOKEN environment variable is required to create shipment MR")

        self._logger.info("Shipment environment setup completed")

    async def _load_or_init_shipment_config(self, source_branch: str) -> ShipmentConfig:
        """Load existing shipment config from branch or initialize new one"""
        # Check if branch already exists upstream
        branch_exists = await self.shipment_data_repo.does_branch_exist_on_remote(source_branch, remote="origin")

        if branch_exists:
            self._logger.info('Branch %s exists, checking for existing shipment config...', source_branch)
            await self.shipment_data_repo.fetch_switch_branch(source_branch, remote="origin")

            # Try to find existing shipment YAML file
            shipment_config = await self._load_existing_shipment_config()
            if shipment_config:
                self._logger.info("Found existing shipment configuration, reusing it")
                return shipment_config
            else:
                self._logger.info("No existing shipment configuration found in branch, will initialize new one")
        else:
            self._logger.info('Branch %s does not exist, will create new branch and shipment config', source_branch)

        # Initialize new shipment configuration if not found
        return await self._init_shipment_config()

    async def _load_existing_shipment_config(self) -> Optional[ShipmentConfig]:
        """Try to load existing shipment config from current branch"""
        try:
            # Look for shipment files in the expected directory structure
            # Pattern: shipment/{product}/{group}/{application}/prod/{assembly}.microshift-bootc.*.yaml
            application = KonfluxImageBuilder.get_application_name(self.group)
            env = 'prod'

            shipment_dir = (
                self.shipment_data_repo._directory / "shipment" / self.product / self.group / application / env
            )

            if not shipment_dir.exists():
                self._logger.info("Shipment directory does not exist: %s", shipment_dir)
                return None

            # Look for files matching the pattern: {assembly}.microshift-bootc.*.yaml
            pattern = f"{self.assembly}.microshift-bootc.*.yaml"
            matching_files = glob.glob(str(shipment_dir / pattern))

            if not matching_files:
                self._logger.info("No existing shipment files found matching pattern: %s", pattern)
                return None

            # Use the most recent file (by filename, which includes timestamp)
            latest_file = max(matching_files)
            self._logger.info("Loading existing shipment config from: %s", latest_file)

            with open(latest_file, 'r') as f:
                shipment_data = yaml.load(f.read())

            # Convert to ShipmentConfig object
            shipment_config = ShipmentConfig(**shipment_data)
            return shipment_config

        except Exception as e:
            self._logger.warning("Failed to load existing shipment config: %s", e)
            return None

    async def _init_shipment_config(self) -> ShipmentConfig:
        """Initialize shipment configuration using elliott shipment init"""
        self._logger.info("Initializing shipment configuration for microshift-bootc...")

        create_cmd = self._elliott_base_command + [
            f'--shipment-path={self.shipment_data_repo_pull_url}',
            "shipment",
            "init",
            "microshift-bootc",  # Using microshift-bootc as the shipment kind
        ]

        stdout = await self._execute_command_with_logging(create_cmd)
        # Convert CommentedMap to regular Python objects before creating Pydantic model
        out = Model(yaml.load(stdout)).primitive()
        shipment = ShipmentConfig(**out)

        self._logger.info("Shipment configuration initialized")
        return shipment

    async def _create_snapshot(self, nvr: str) -> Optional[Snapshot]:
        """Create snapshot from build NVR using elliott snapshot new"""
        if not nvr:
            return None

        self._logger.info("Creating snapshot from build: %s", nvr)

        # Call elliott snapshot new with NVR directly
        # explicitly include microshift-bootc as it is mode:disabled
        snapshot_cmd = self._elliott_base_command + [
            "-i",
            "microshift-bootc",
            "snapshot",
            "new",
            nvr,
        ]

        konflux_art_images_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
        if konflux_art_images_auth_file:
            snapshot_cmd.append(f"--pull-secret={konflux_art_images_auth_file}")

        stdout = await self._execute_command_with_logging(snapshot_cmd)

        # parse the output of the snapshot new command, it should be valid yaml
        new_snapshot_obj = yaml.load(stdout)
        # make some assertions that this is a valid snapshot object
        if new_snapshot_obj.get("apiVersion") != API_VERSION or new_snapshot_obj.get("kind") != KIND_SNAPSHOT:
            raise ValueError(f"Snapshot object is not valid: {stdout}")

        self._logger.info("Snapshot created successfully")
        return Snapshot(spec=SnapshotSpec(**new_snapshot_obj.get("spec")), nvrs=[nvr])

    async def _setup_shipment_data_repo(self):
        """Setup and clone shipment-data repository"""
        self._logger.info("Setting up shipment data repository...")

        # Clean up existing directory if it exists
        if self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

        # Initialize GitRepository for shipment data
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, self.runtime.dry_run)

        # Setup shipment-data repo which should reside in GitLab
        # pushing is done via basic auth
        await self.shipment_data_repo.setup(
            remote_url=self._basic_auth_url(self.shipment_data_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

        self._logger.info("Shipment data repository setup completed")

    async def _create_shipment_mr(self, shipment_config: ShipmentConfig) -> str:
        """Create or update shipment MR with the given shipment config"""
        self._logger.info("Creating or updating shipment MR...")

        # Branch handling is now done in _load_or_init_shipment_config
        source_branch = f"prepare-microshift-bootc-shipment-{self.assembly}"
        target_branch = "main"
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

        # Check if branch exists and switch to it, or create it
        branch_exists = await self.shipment_data_repo.does_branch_exist_on_remote(source_branch, remote="origin")
        if branch_exists:
            await self.shipment_data_repo.fetch_switch_branch(source_branch, remote="origin")
        else:
            await self.shipment_data_repo.create_branch(source_branch)

        # Update shipment data repo with shipment config
        release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        commit_message = f"Add microshift-bootc shipment configuration for {release_name}"
        updated = await self._update_shipment_data(shipment_config, timestamp, commit_message, source_branch)
        if not updated:
            self._logger.info("No changes in shipment data. MR will not be created or updated.")
            return "No changes to commit"

        def _get_project(url):
            parsed_url = urlparse(url)
            project_path = parsed_url.path.strip('/').removesuffix('.git')
            return self._get_gitlab().projects.get(project_path)

        source_project = _get_project(self.shipment_data_repo_push_url)
        target_project = _get_project(self.shipment_data_repo_pull_url)

        mr_title = f"Draft: Microshift-bootc shipment for {release_name}"
        job_url = os.getenv('BUILD_URL', 'N/A')
        mr_description = f"Created by job: {job_url}\n\n{commit_message}"

        if self.runtime.dry_run:
            action = "updated" if branch_exists else "created"
            self._logger.info("[DRY-RUN] Would have %s MR with title: %s", action, mr_title)
            mr_url = f"{self.gitlab_url}/placeholder/placeholder/-/merge_requests/placeholder"
        else:
            # Check if MR already exists for this branch
            existing_mrs = source_project.mergerequests.list(
                source_branch=source_branch, target_branch=target_branch, state='opened'
            )

            if existing_mrs:
                # Update existing MR
                mr = existing_mrs[0]
                mr.description = f"{mr.description}\n\nUpdated by job: {job_url}"
                mr.save()
                mr_url = mr.web_url
                self._logger.info("Updated existing Draft Merge Request: %s", mr_url)
            else:
                # Create new MR
                mr = source_project.mergerequests.create(
                    {
                        'source_branch': source_branch,
                        'target_project_id': target_project.id,
                        'target_branch': target_branch,
                        'title': mr_title,
                        'description': mr_description,
                        'remove_source_branch': True,
                    }
                )
                mr_url = mr.web_url
                self._logger.info("Created Draft Merge Request: %s", mr_url)

        return mr_url

    async def _update_shipment_data(
        self, shipment_config: ShipmentConfig, timestamp: str, commit_message: str, branch: str
    ) -> bool:
        """Update shipment data repo with the given shipment config file"""
        filename = f"{self.assembly}.microshift-bootc.{timestamp}.yaml"
        product = shipment_config.shipment.metadata.product
        group = shipment_config.shipment.metadata.group
        application = shipment_config.shipment.metadata.application
        env = "prod"  # Default to prod for microshift-bootc shipments

        relative_target_dir = Path("shipment") / product / group / application / env
        target_dir = self.shipment_data_repo._directory / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        filepath = relative_target_dir / filename

        self._logger.info("Updating shipment file: %s", filename)
        shipment_dump = shipment_config.model_dump(exclude_unset=True, exclude_none=True)
        out = StringIO()
        yaml.dump(shipment_dump, out)
        await self.shipment_data_repo.write_file(filepath, out.getvalue())

        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()

        job_url = os.getenv('BUILD_URL')
        if job_url and job_url not in commit_message:
            commit_message += f"\n{job_url}"

        return await self.shipment_data_repo.commit_push(commit_message, safe=True)

    def _get_gitlab(self):
        """Get GitLab client instance"""
        if not hasattr(self, '_gitlab_client'):
            self._gitlab_client = gitlab.Gitlab(self.gitlab_url, private_token=self.gitlab_token)
            self._gitlab_client.auth()
        return self._gitlab_client

    @staticmethod
    def _basic_auth_url(url: str, token: str) -> str:
        """Convert URL to basic auth format with token"""
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        rest_of_the_url = url[len(scheme + "://") :]
        # the assumption here is that username can be anything
        # so we use oauth2 as a placeholder username
        # and the token as the password
        return f'https://oauth2:{token}@{rest_of_the_url}'

    async def _execute_command_with_logging(self, cmd: list[str]) -> str:
        """Execute a command asynchronously and log its output"""
        _, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        if stdout:
            self._logger.info("Command stdout:\n %s", stdout)
        return stdout


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
@click.option(
    "--prepare-shipment",
    is_flag=True,
    help="(For named assemblies) Prepare shipment with found microshift-bootc build",
)
@pass_runtime
@click_coroutine
async def build_microshift_bootc(
    runtime: Runtime,
    data_path: str,
    group: str,
    assembly: str,
    force: bool,
    force_plashet_sync: bool,
    prepare_shipment: bool,
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
            prepare_shipment=prepare_shipment,
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
