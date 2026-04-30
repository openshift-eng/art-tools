import asyncio
import copy
import io
import json
import logging
import os
import shutil
import tempfile
import time
import traceback
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Optional, cast
from urllib.parse import urlparse

import click
import requests
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.assembly import AssemblyTypes, assembly_config_struct
from artcommonlib.config.repo import RepoList
from artcommonlib.constants import (
    KONFLUX_DEFAULT_IMAGE_REPO,
    REGISTRY_CI_OPENSHIFT,
    REGISTRY_QUAY_OCP_RELEASE_DEV,
    SHIPMENT_DATA_URL_TEMPLATE,
)
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.gitlab import GitLabClient
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.model import Model
from artcommonlib.registry_config import RegistryConfig
from artcommonlib.util import (
    get_art_prod_image_repo_for_version,
    get_ocp_version_from_group,
    new_roundtrip_yaml_handler,
    sync_to_quay,
)
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from doozerlib.util import isolate_git_commit_in_release
from elliottlib.shipment_model import ShipmentConfig, Snapshot, SnapshotSpec
from github import GithubException

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
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
        data_gitref: str | None = None,
        network_mode: Optional[str] = None,
        suppress_external_effects: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.data_gitref = data_gitref or ""
        self.doozer_group = f"{self.group}@{self.data_gitref}" if self.data_gitref else self.group
        self.assembly = assembly
        self.force = force
        self.force_plashet_sync = force_plashet_sync
        self.prepare_shipment = prepare_shipment
        self.slack_client = slack_client
        self.network_mode = network_mode
        self.suppress_external_effects = suppress_external_effects
        self._logger = logger or runtime.logger

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
        self.shipment_data_repo_pull_url = (
            self.runtime.config.get("shipment_config", {}).get("shipment_data_url") or SHIPMENT_DATA_URL_TEMPLATE
        )
        self.shipment_data_repo_push_url = (
            self.runtime.config.get("shipment_config", {}).get("shipment_data_push_url") or SHIPMENT_DATA_URL_TEMPLATE
        )

        # Initialize GitRepository for shipment data (will be setup later if needed)
        self.shipment_data_repo = None
        self.gitlab_token = None
        self.shipment_mr_url = None

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
        group_param = f'--group={self.doozer_group}'
        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            f'--working-dir={self._working_dir / "elliott-working"}',
            f'--data-path={data_path or constants.OCP_BUILD_DATA_URL}',
        ]

        # Registry config will be set up in run() method
        self._registry_config: Optional[str] = None

    @property
    def assembly_group_config(self) -> Model:
        """Get the assembly-specific group configuration"""
        return assembly_config_struct(Model(self.releases_config), self.assembly, "group", {})

    async def run(self):
        if 'XDG_RUNTIME_DIR' in os.environ:
            self._logger.info('Unsetting XDG_RUNTIME_DIR to prevent use of default registry auth')
            del os.environ['XDG_RUNTIME_DIR']
            self._elliott_env_vars.pop('XDG_RUNTIME_DIR', None)
            self._doozer_env_vars.pop('XDG_RUNTIME_DIR', None)

        quay_auth_file = os.getenv('QUAY_AUTH_FILE')
        if not quay_auth_file:
            raise ValueError(
                "QUAY_AUTH_FILE environment variable is required but not set. "
                "Ensure Jenkins credentials are properly bound."
            )

        source_files = [quay_auth_file]

        with RegistryConfig(
            kubeconfig=os.environ.get('KUBECONFIG'),
            source_files=source_files,
            registries=[
                REGISTRY_QUAY_OCP_RELEASE_DEV,
                KONFLUX_DEFAULT_IMAGE_REPO,
                REGISTRY_CI_OPENSHIFT,
            ],
        ) as global_auth_file:
            self._logger.info(
                'Set registry auth file=%s for pipeline operations (cherry-picked from %d source file(s))',
                global_auth_file,
                len(source_files),
            )

            # Store registry config for use in pipeline operations
            self._registry_config = global_auth_file

            await self._run_pipeline()

    async def _run_pipeline(self):
        if self.network_mode == 'open' and not self.suppress_external_effects:
            raise ValueError(
                "Open builds require --suppress-external-effects to prevent "
                "publishing non-hermetic builds to production."
            )
        if self.suppress_external_effects and self.prepare_shipment:
            raise ValueError("--prepare-shipment and --suppress-external-effects are mutually exclusive.")

        data_path = self._doozer_env_vars["DOOZER_DATA_PATH"]
        self.releases_config = await load_releases_config(group=self.doozer_group, data_path=data_path)
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
        # Load group config
        self.group_config = await load_group_config(
            group=self.group,
            assembly=self.assembly,
            doozer_data_path=data_path,
            doozer_data_gitref=self.data_gitref,
        )
        bootc_build = await self._rebase_and_build_bootc()
        if bootc_build:
            self._logger.info("Bootc image build: %s", bootc_build.nvr)
        else:
            raise ValueError(f"Could not find bootc image build for assembly {self.assembly}")

        # get image digests from manifest list for all arches
        cmd = [
            "skopeo",
            "inspect",
            f"docker://{bootc_build.image_pullspec}",
            "--raw",
        ]

        if self._registry_config:
            cmd += [f'--authfile={self._registry_config}']

        _, out, _ = await exectools.cmd_gather_async(cmd)
        manifest_list = json.loads(out)
        digest_by_arch = {m["platform"]["architecture"]: m["digest"] for m in manifest_list["manifests"]}
        self._logger.info("Bootc image digests by arch: %s", json.dumps(digest_by_arch, indent=4))

        if not self.runtime.dry_run and not self.suppress_external_effects:
            major, _ = self._ocp_version
            if bootc_build.embargoed:
                art_repo = get_art_prod_image_repo_for_version(major, "dev-priv")
                await sync_to_quay(bootc_build.image_pullspec, art_repo)
            else:
                art_repo = get_art_prod_image_repo_for_version(major, "dev")
                await sync_to_quay(bootc_build.image_pullspec, art_repo)
                # sync per-arch bootc-pullspec.txt to mirror
                if self.assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
                    self._logger.info(f"Found assembly type {self.assembly_type}. Syncing bootc build to mirror")
                    await asyncio.gather(
                        *(
                            self.sync_to_mirror(arch, bootc_build.el_target, f"{art_repo}@{digest}")
                            for arch, digest in digest_by_arch.items()
                        ),
                    )
        else:
            major, _ = self._ocp_version
            art_repo = get_art_prod_image_repo_for_version(major, "dev")
            reason = 'suppress-external-effects' if self.suppress_external_effects else 'dry-run'
            self._logger.warning(f"Skipping sync to {art_repo} ({reason})")

        # Pin the image to the assembly if not STREAM
        if self.assembly_type != AssemblyTypes.STREAM and not self.suppress_external_effects:
            # Check if we need to create a PR to pin the build
            pinned_nvr = get_image_if_pinned_directly(self.releases_config, self.assembly, 'microshift-bootc')
            if bootc_build.nvr != pinned_nvr:
                self._logger.info("Creating PR to pin microshift-bootc image: %s", bootc_build.nvr)
                await self._create_or_update_pull_request_for_image(bootc_build.nvr)

            if self.prepare_shipment:
                await self._prepare_shipment(bootc_build)

            if self.shipment_mr_url and not self.runtime.dry_run:
                await self._set_shipment_mr_ready()
                await self.slack_client.say_in_thread("Completed preparing microshift-bootc shipment.")

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
            exclude_large_columns=True,
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

                microshift_nvrs = await get_microshift_builds(
                    self.doozer_group, self.assembly, env=self._elliott_env_vars
                )
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
            data_gitref=self.data_gitref,
            dry_run=self.runtime.dry_run,
            block_until_complete=True,
        )

    async def _get_microshift_rpm_commit(self) -> str:
        """
        Extracts the upstream source commit from the microshift RPM NVR for this assembly.
        This ensures the bootc image is built from the same commit as the RPM.

        Return Value(s):
            str: The abbreviated git commit hash extracted from the RPM NVR.
        """
        microshift_nvrs = await get_microshift_builds(self.doozer_group, self.assembly, env=self._elliott_env_vars)
        if not microshift_nvrs:
            raise ValueError(
                f"Could not find microshift RPM NVRs for assembly {self.assembly}. "
                f"Ensure the microshift RPM has been built before building the bootc image."
            )

        # All EL versions are built from the same source commit; extract it from the first NVR
        # that contains a recognizable commit hash
        for nvr in microshift_nvrs:
            release = nvr.rsplit("-", 1)[-1]
            commit = isolate_git_commit_in_release(release)
            if commit:
                self._logger.info("Extracted upstream commit %s from microshift RPM NVR %s", commit, nvr)
                return commit

        raise ValueError(
            f"Could not extract git commit from any microshift RPM NVR: {microshift_nvrs}. "
            f"The NVR release fields do not contain a recognizable commit hash."
        )

    def _get_assembly_label_value(self) -> Optional[str]:
        """Compute the value for the assembly Dockerfile label.

        - STANDARD (e.g. assembly ``4.18.1``): ``v4.18.1``
        - CANDIDATE / PREVIEW (e.g. assembly ``rc.1``): ``v4.22.0-rc.1``
        - CUSTOM: raises – not supported for microshift-bootc
        - STREAM: returns None (label is omitted)
        """
        if self.assembly_type == AssemblyTypes.STREAM:
            return None
        major, minor = self._ocp_version
        if self.assembly_type == AssemblyTypes.STANDARD:
            return f"v{self.assembly}"
        if self.assembly_type in (AssemblyTypes.CANDIDATE, AssemblyTypes.PREVIEW):
            return f"v{major}.{minor}.0-{self.assembly}"
        if self.assembly_type == AssemblyTypes.CUSTOM:
            raise ValueError(
                f"Assembly type CUSTOM is not supported for microshift-bootc builds (assembly={self.assembly})"
            )
        raise ValueError(f"Assembly type {self.assembly_type} is not supported for microshift-bootc builds")

    async def _rebase_and_build_bootc(self):
        bootc_image_name = "microshift-bootc"
        major, minor = self._ocp_version
        # do not run for version < 4.18
        if (major, minor) < (4, 18):
            self._logger.info("Skipping bootc image build for version < 4.18")
            return

        # Check if an image is already pinned and don't rebuild unless forced
        if not self.force and self.assembly_type != AssemblyTypes.STREAM:
            pinned_nvr = get_image_if_pinned_directly(self.releases_config, self.assembly, bootc_image_name)
            if pinned_nvr:
                message = f"For assembly {self.assembly} microshift-bootc image is already pinned: {pinned_nvr}. Use FORCE to rebuild."
                self._logger.info(message)
                if not self.suppress_external_effects:
                    await self.slack_client.say_in_thread(message)

                # Fetch the actual build record from Konflux DB using the pinned NVR
                if not self.konflux_db:
                    self.konflux_db = KonfluxDb()
                    self.konflux_db.bind(KonfluxBuildRecord)
                    self._logger.info('Konflux DB initialized for pinned build lookup')

                build = await self.konflux_db.get_build_record_by_nvr(
                    pinned_nvr, strict=True, exclude_large_columns=True
                )
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

        # Extract the commit from the microshift RPM to ensure bootc is built from the same source
        upstream_commit = await self._get_microshift_rpm_commit()

        # Determine the assembly label value based on assembly type
        assembly_label_value = self._get_assembly_label_value()

        # Rebase and build bootc image
        version = f"v{major}.{minor}"
        release = default_release_suffix()
        rebase_cmd = [
            "doozer",
            "--group",
            self.doozer_group,
            "--assembly",
            self.assembly,
            "--latest-parent-version",
            "-i",
            bootc_image_name,
            # Lock to the same commit as the microshift RPM to ensure consistency
            # between RPM and bootc artifacts. Without this, doozer would try to
            # use brew to find the commit which fails for Konflux-built images.
            "--lock-upstream",
            bootc_image_name,
            upstream_commit,
            "--build-system",
            "konflux",
            "beta:images:konflux:rebase",
            "--version",
            version,
            "--release",
            release,
        ]
        if assembly_label_value:
            rebase_cmd += ["--extra-label", f"assembly={assembly_label_value}"]
        if self.network_mode:
            rebase_cmd += ["--network-mode", self.network_mode]
        rebase_cmd += [
            "--message",
            f"Updating Dockerfile version and release {version}-{release}",
        ]
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")
        await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)

        build_cmd = [
            "doozer",
            "--group",
            self.doozer_group,
            "--assembly",
            self.assembly,
            "--latest-parent-version",
        ]
        if self._registry_config:
            build_cmd.append(f"--registry-config={self._registry_config}")
        build_cmd.extend(
            [
                "-i",
                bootc_image_name,
                # Lock to the same commit as the microshift RPM to ensure consistency
                "--lock-upstream",
                bootc_image_name,
                upstream_commit,
                "--build-system",
                "konflux",
                "beta:images:konflux:build",
                "--image-repo",
                KONFLUX_DEFAULT_IMAGE_REPO,
                "--konflux-kubeconfig",
                kubeconfig,
                "--konflux-namespace",
                "ocp-art-tenant",
            ]
        )
        if self.network_mode:
            build_cmd.extend(["--network-mode", self.network_mode])
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
        github_client = get_github_client_for_org(user)
        upstream_repo = github_client.get_repo(f"{user}/{repo}")
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
            user, repo = self.extract_git_repo(self._doozer_env_vars['DOOZER_DATA_PATH'])
            pr = get_github_client_for_org(user).get_repo(f"{user}/{repo}").get_pull(pr.number)

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
        shipment_config = await self._load_or_init_shipment_config()

        # Check if there was an existing microshift_bootc_shipment URL
        assembly_shipment_config = self.assembly_group_config.get("microshift_bootc_shipment", {})
        had_existing_url = bool(assembly_shipment_config.get("url"))

        # Step 4: Use the provided bootc build
        self._logger.info("Using bootc build: %s", bootc_build.nvr)

        # Step 5: Create snapshot from bootc build
        snapshot = await self._create_snapshot(bootc_build.nvr)
        shipment_config.shipment.snapshot = snapshot

        # Step 6: Create shipment MR
        self.shipment_mr_url = await self._create_shipment_mr(shipment_config)

        if self.shipment_mr_url:
            await self.slack_client.say_in_thread(f"Shipment MR created: {self.shipment_mr_url}")

            # Step 7: If this is the first run (no existing URL), write the URL back to releases.yml
            if not had_existing_url:
                self._logger.info("First shipment run for this assembly - writing URL to releases.yml")
                await self._create_or_update_build_data_pr()
        else:
            await self.slack_client.say_in_thread("No changes in shipment data. MR was not created or updated.")

    async def _set_shipment_mr_ready(self):
        """
        Mark the shipment MR as ready by removing the Draft prefix from the title.
        This should be called at the end of the pipeline when all work is complete.
        """
        mr = await self._gitlab.set_mr_ready(self.shipment_mr_url)

        if mr and not self.runtime.dry_run:
            await self.slack_client.say_in_thread(f"Shipment MR marked as ready: {self.shipment_mr_url}")

            # Trigger CI pipeline after marking as ready
            # wait for 30 seconds to ensure the MR is updated
            self._logger.info("Waiting for 30 seconds to ensure MR is updated...")
            await asyncio.sleep(30)

            try:
                pipeline_url = await self._gitlab.trigger_ci_pipeline(mr)
                if pipeline_url:
                    await self.slack_client.say_in_thread(f"CI pipeline triggered: {pipeline_url}")
                else:
                    await self.slack_client.say_in_thread(
                        f"Failed to trigger CI pipeline for MR branch {mr.source_branch}"
                    )
            except Exception as e:
                self._logger.warning(f"Failed to trigger CI MR pipeline for branch {mr.source_branch}: {e}")

    async def _setup_shipment_environment(self):
        """Setup environment variables and tokens required for shipment operations"""
        # Check for GitLab token
        self.gitlab_token = os.getenv("GITLAB_TOKEN")
        if not self.gitlab_token and not self.runtime.dry_run:
            raise ValueError("GITLAB_TOKEN environment variable is required to create shipment MR")

        self._logger.info("Shipment environment setup completed")

    def _get_shipment_mr_branch(self, shipment_url: str) -> str:
        """Get the source branch from a shipment MR URL.
        :param shipment_url: The URL of the shipment MR
        :returns: The source branch name of the MR
        :raises ValueError: If the MR is not in opened state
        """
        mr = self._gitlab.get_mr_from_url(shipment_url)
        if not mr:
            raise ValueError(f"Could not find MR at URL: {shipment_url}")

        if mr.state != "opened":
            raise ValueError(f"MR state {mr.state} is not opened. This is not supported.")

        return mr.source_branch

    def _validate_shipment_mr(self, shipment_url: str):
        """Validate the shipment MR state
        :param shipment_url: The URL of the existing shipment MR to validate
        :raises ValueError: If the MR is not in opened state
        """
        mr = self._gitlab.get_mr_from_url(shipment_url)
        if not mr:
            raise ValueError(f"Could not find MR at URL: {shipment_url}")

        if mr.state != "opened":
            raise ValueError(f"MR state {mr.state} is not opened. This is not supported.")

    def _update_build_data(self, branch: str, upstream_repo) -> tuple[bool, dict]:
        """Update releases.yml in build data repo with microshift_bootc_shipment URL.
        :param branch: The branch to update in the build data repo
        :param upstream_repo: GitHub repository object
        :return: Tuple of (changed: bool, new_releases_config: dict)
        """
        # Load current releases.yml from the branch (or group branch if branch doesn't exist yet)
        try:
            # Try to get releases.yml from the target branch first
            release_file_content = yaml.load(upstream_repo.get_contents("releases.yml", ref=branch).decoded_content)
        except Exception:
            # Branch doesn't exist yet, get from group branch
            release_file_content = yaml.load(upstream_repo.get_contents("releases.yml", ref=self.group).decoded_content)

        source_file_content = copy.deepcopy(release_file_content)
        new_releases_config = copy.deepcopy(release_file_content)

        # Update the microshift_bootc_shipment section
        microshift_bootc_shipment_config = {"url": self.shipment_mr_url}

        # Get the assembly definition to check if it has a parent
        assembly_def = new_releases_config["releases"][self.assembly]["assembly"]
        has_parent = bool(assembly_def.get("basis", {}).get("assembly"))

        # If this assembly has a basis assembly, use the override marker (!)
        # to prevent inheritance from causing the URL to be lost on subsequent runs
        if has_parent:
            key_name = "microshift_bootc_shipment!"
        else:
            key_name = "microshift_bootc_shipment"

        # Set the microshift_bootc_shipment config (ensure group dict exists first)
        group_config = new_releases_config["releases"][self.assembly]["assembly"].setdefault("group", {})
        group_config[key_name] = microshift_bootc_shipment_config

        # Check if anything changed
        if source_file_content == new_releases_config:
            return False, new_releases_config

        return True, new_releases_config

    async def _create_or_update_build_data_pr(self) -> bool:
        """Create or update a pull request in the build data repo with the microshift_bootc_shipment URL.
        :return: True if the PR was created or updated successfully, False otherwise.
        """
        branch = f"update-microshift-bootc-shipment-{self.assembly}"
        pr_title = f"Update microshift-bootc shipment URL for assembly {self.assembly}"
        pr_body = f"This PR updates microshift_bootc_shipment URL for {self.assembly} assembly."
        job_url = jenkins.get_build_url()
        if job_url:
            pr_body += f"\n\nCreated by job: {job_url}"

        if self.runtime.dry_run:
            self._logger.info(
                "[DRY-RUN] Would have created pull-request with head '%s', title '%s', body '%s'",
                branch,
                pr_title,
                pr_body,
            )
            return True

        data_path = self._doozer_env_vars["DOOZER_DATA_PATH"]
        user, repo = self.extract_git_repo(data_path)
        github_client = get_github_client_for_org(user)
        upstream_repo = github_client.get_repo(f"{user}/{repo}")

        # Check if anything needs to be updated
        changed, new_releases_config = self._update_build_data(branch, upstream_repo)
        if not changed:
            self._logger.info("No changes in microshift_bootc_shipment config. PR will not be created.")
            return False

        # Delete existing branch if it exists
        for b in upstream_repo.get_branches():
            if b.name == branch:
                upstream_repo.get_git_ref(f"heads/{branch}").delete()

        # Create new branch
        upstream_repo.create_git_ref(f"refs/heads/{branch}", upstream_repo.get_branch(self.group).commit.sha)

        # Update releases.yml
        output = io.BytesIO()
        yaml.dump(new_releases_config, output)
        output.seek(0)
        fork_file = upstream_repo.get_contents("releases.yml", ref=branch)
        upstream_repo.update_file("releases.yml", pr_body, output.read(), fork_file.sha, branch=branch)

        # Create PR
        pr = upstream_repo.create_pull(title=pr_title, body=pr_body, base=self.group, head=branch)
        self._logger.info("Created PR to update microshift-bootc shipment URL: %s", pr.html_url)
        await self.slack_client.say_in_thread(f"PR created to update microshift-bootc shipment URL: {pr.html_url}")

        # Wait for CI tests to pass, then merge
        await self._wait_for_pr_merge(pr)
        return True

    async def _load_or_init_shipment_config(self) -> ShipmentConfig:
        """Load existing shipment config from branch or initialize new one

        If a shipment MR already exists (URL in config), reuse the existing branch.
        Otherwise, create a new branch with timestamp.

        Sets self._shipment_source_branch to the resolved branch name for reuse in _create_shipment_mr.
        """
        # Check if shipment already exists in assembly config
        assembly_shipment_config = self.assembly_group_config.get("microshift_bootc_shipment", {})
        existing_mr_url = assembly_shipment_config.get("url")

        if existing_mr_url:
            # Get the branch name from the existing MR (validates MR state)
            self._shipment_source_branch = self._get_shipment_mr_branch(existing_mr_url)

            self._logger.info('Found existing shipment MR, using branch: %s', self._shipment_source_branch)
            await self.shipment_data_repo.fetch_switch_branch(self._shipment_source_branch, remote="origin")

            # Initialize new config - we'll load the snapshot from existing files later if needed
            return await self._init_shipment_config()
        else:
            # No existing MR - this is the first run, initialize new config
            self._logger.info('No existing shipment MR found, will initialize new shipment config')
            self._shipment_source_branch = None
            return await self._init_shipment_config()

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

        quay_auth_file = os.getenv("QUAY_AUTH_FILE")
        if quay_auth_file:
            snapshot_cmd.append(f"--pull-secret={quay_auth_file}")

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

    async def _create_shipment_mr(self, shipment_config: ShipmentConfig) -> str | None:
        """Create or update shipment MR with the given shipment config. Returns None if no changes."""
        self._logger.info("Creating or updating shipment MR...")

        target_branch = "main"

        # Use the cached branch name from _load_or_init_shipment_config (if available)
        cached_branch = getattr(self, '_shipment_source_branch', None)
        if cached_branch:
            # Reusing existing MR branch (already switched in _load_or_init_shipment_config)
            source_branch = cached_branch
            self._logger.info('Reusing existing shipment branch: %s', source_branch)
        else:
            # Create new branch with timestamp
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            source_branch = f"prepare-microshift-bootc-shipment-{self.assembly}-{timestamp}"
            self._logger.info('Creating new shipment branch: %s', source_branch)
            await self.shipment_data_repo.create_branch(source_branch)

        # Update shipment data repo with shipment config
        release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        commit_message = f"Add microshift-bootc shipment configuration for {release_name}"
        updated = await self._update_shipment_data(shipment_config, commit_message, source_branch)
        if not updated:
            self._logger.info("No changes in shipment data. MR will not be created or updated.")
            return None

        def _get_project(url):
            parsed_url = urlparse(url)
            project_path = parsed_url.path.strip('/').removesuffix('.git')
            return self._gitlab.get_project(project_path)

        source_project = _get_project(self.shipment_data_repo_push_url)
        target_project = _get_project(self.shipment_data_repo_pull_url)

        mr_title = f"Draft: Microshift-bootc shipment for {release_name}"
        job_url = os.getenv('BUILD_URL', 'N/A')
        mr_description = f"Created by job: {job_url}\n\n{commit_message}"

        if self.runtime.dry_run:
            action = "updated" if cached_branch else "created"
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

    async def _update_shipment_data(self, shipment_config: ShipmentConfig, commit_message: str, branch: str) -> bool:
        """Update shipment data repo with the given shipment config file"""
        # Extract timestamp from branch name (last segment after splitting by "-")
        # Branch format: prepare-microshift-bootc-shipment-{assembly}-{timestamp}
        timestamp = branch.split("-")[-1]
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

    @cached_property
    def _gitlab(self) -> GitLabClient:
        """
        Get GitLab client instance.
        """
        return GitLabClient(self.gitlab_url, self.gitlab_token, self.runtime.dry_run)

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
@click.option(
    "--data-gitref",
    required=False,
    default="",
    help="Doozer data path git [branch / tag / sha] to use",
)
@click.option(
    "--network-mode",
    type=click.Choice(['hermetic', 'open']),
    default=None,
    help="Override network mode for Konflux builds.",
)
@click.option(
    "--suppress-external-effects",
    is_flag=True,
    help="Skip Quay sync, mirror sync, PR creation, shipment MR, and Slack notifications.",
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
    data_gitref: str | None = None,
    network_mode: str | None = None,
    suppress_external_effects: bool = False,
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
            data_gitref=data_gitref,
            network_mode=network_mode,
            suppress_external_effects=suppress_external_effects,
        )
        await pipeline.run()
    except Exception as err:
        slack_message = f"build-microshift-bootc pipeline encountered error: {err}"
        error_message = slack_message + f"\n {traceback.format_exc()}"
        runtime.logger.error(error_message)
        if not suppress_external_effects:
            await slack_client.say_in_thread(slack_message)
        raise
