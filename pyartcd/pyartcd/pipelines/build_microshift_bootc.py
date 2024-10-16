import json
import logging
import os
import requests
import asyncio
from datetime import datetime, timezone
from typing import Optional

from artcommonlib.assembly import AssemblyTypes
from artcommonlib.util import get_ocp_version_from_group, isolate_major_minor_in_group
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, Engine, ArtifactType, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib import exectools
from ruamel.yaml import YAML

from pyartcd import constants, oc, util
from pyartcd.runtime import Runtime
from pyartcd.util import (get_assembly_type,
                          isolate_el_version_in_release,
                          load_group_config,
                          load_releases_config,
                          default_release_suffix,
                          get_release_name_for_assembly,
                          get_microshift_builds)
from pyartcd.plashets import build_plashets, plashet_config_for_major_minor

yaml = YAML(typ="rt")
yaml.default_flow_style = False
yaml.preserve_quotes = True
yaml.width = 4096


class BuildMicroShiftBootcPipeline:
    """ Rebase and build MicroShift for an assembly """

    def __init__(self,
                 runtime: Runtime,
                 group: str,
                 assembly: str,
                 force: bool,
                 data_path: str,
                 slack_client,
                 logger: Optional[logging.Logger] = None):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.force = force
        self.slack_client = slack_client
        self._logger = logger or runtime.logger

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
            data_path = self.runtime.config.get("build_config", {}).get("ocp_build_data_url") or constants.OCP_BUILD_DATA_URL
        if data_path:
            self._doozer_env_vars["DOOZER_DATA_PATH"] = data_path
            self._elliott_env_vars["ELLIOTT_DATA_PATH"] = data_path

    async def run(self):
        # Make sure our api.ci token is fresh
        await oc.registry_login(self.runtime)
        self.releases_config = await load_releases_config(
            group=self.group,
            data_path=self._doozer_env_vars["DOOZER_DATA_PATH"]
        )
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
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
            "--raw"
        ]
        _, out, _ = await exectools.cmd_gather_async(cmd)
        manifest_list = json.loads(out)
        digest_by_arch = {m["platform"]["architecture"]: m["digest"] for m in manifest_list["manifests"]}
        self._logger.info("Bootc image digests by arch: %s", json.dumps(digest_by_arch, indent=4))
        repo_url = bootc_build.image_pullspec.rsplit(":")[0]
        for arch, digest in digest_by_arch.items():
            await self.sync_to_mirror(arch, bootc_build.el_target, f"{repo_url}@{digest}")

    async def sync_to_mirror(self, arch, el_target, pullspec):
        arch = brew_arch_for_go_arch(arch)
        pullspec_file = self._working_dir / "bootc-pullspec.txt"
        with open(pullspec_file, "w") as f:
            f.write(pullspec)

        release_name = get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        ocp_dir = "ocp-dev-preview" if self.assembly_type == AssemblyTypes.PREVIEW else "ocp"

        # This is where we sync microshift artifacts on mirror. Refer to microshift_sync job
        # The paths should be the same in both of these places
        s3_path = f"/pub/openshift-v4/{arch}/microshift/{ocp_dir}/{release_name}/{el_target}"
        cmd = [
            "aws", "s3", "sync",
            "--no-progress",
            "--exact-timestamps",
            "--exclude", "*",
            "--include", "bootc-pullspec.txt",
            str(self._working_dir),
            f"s3://art-srv-enterprise{s3_path}",
        ]
        if self.runtime.dry_run:
            cmd.append("--dryrun")
        await exectools.cmd_assert_async(cmd)

        # Sync to Cloudflare as well
        cmd.extend([
            "--profile", "cloudflare",
            "--endpoint-url", os.environ['CLOUDFLARE_ENDPOINT'],
        ])
        await exectools.cmd_assert_async(cmd)

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
            el_target='el9'
        )
        return build

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
                actual_nvr = next((p["nvr"] for p in plashet_yaml["assemble"]["packages"]
                                   if p["package_name"] == "microshift"), None)
                if not actual_nvr:
                    raise ValueError(f"Expected to find microshift package in plashet.yml at"
                                     f" {url}, but could not find it. Use --force to rebuild plashet.")

                microshift_nvrs = await get_microshift_builds(self.group, self.assembly, env=self._elliott_env_vars)
                expected_microshift_nvr = next(n for n in microshift_nvrs if isolate_el_version_in_release(n) == 9)
                if actual_nvr != expected_microshift_nvr:
                    self._logger.info(f"Found nvr {actual_nvr} in plashet.yml is different from expected {expected_microshift_nvr}. Plashet build is needed.")
                    return True
                self._logger.info(f"Plashet has the expected microshift nvr {expected_microshift_nvr}")
                return False

        rebuild_needed = await _rebuild_needed() or self.force
        if not rebuild_needed:
            self._logger.info("Skipping plashet sync for %s", microshift_plashet_name)
            return

        plashets_built = await build_plashets(
            stream=f"{major}.{minor}",
            release=default_release_suffix(),
            assembly=self.assembly,
            repos=[microshift_plashet_name],
            doozer_working=self._doozer_env_vars["DOOZER_WORKING_DIR"],
            data_path=self._doozer_env_vars["DOOZER_DATA_PATH"],
            dry_run=self.runtime.dry_run,
        )
        self._logger.info('Built plashets: %s', json.dumps(plashets_built, indent=4))

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
                self._logger.info("Bootc image build already exists: %s. Skipping image build", build.nvr)
                return build

        kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
        if not kubeconfig:
            raise ValueError(f"KONFLUX_SA_KUBECONFIG environment variable is required to build {bootc_image_name} image")

        await self._build_plashet_for_bootc()

        # Rebase and build bootc image
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M")
        version = f"v{major}.{minor}.0"
        release = f"{timestamp}.p?"
        rebase_cmd = [
            "doozer",
            "--group", self.group,
            "--assembly", self.assembly,
            "--latest-parent-version",
            "-i", bootc_image_name,
            # regardless of assembly cutoff time lock to HEAD in release branch
            # also not passing this breaks the command since we try to use brew to find the appropriate commit
            "--lock-upstream", bootc_image_name, "HEAD",
            "beta:images:konflux:rebase",
            "--version", version,
            "--release", release,
            "--message", f"Updating Dockerfile version and release {version}-{release}",
        ]
        if not self.runtime.dry_run:
            rebase_cmd.append("--push")
        await exectools.cmd_assert_async(rebase_cmd, env=self._doozer_env_vars)

        build_cmd = [
            "doozer",
            "--group", self.group,
            "--assembly", self.assembly,
            "--latest-parent-version",
            "-i", bootc_image_name,
            # regardless of assembly cutoff time lock to HEAD in release branch
            # also not passing this breaks the command since we try to use brew to find the appropriate commit
            "--lock-upstream", bootc_image_name, "HEAD",
            "beta:images:konflux:build",
            "--konflux-kubeconfig", kubeconfig,
            "--konflux-namespace", "ocp-art-tenant"
        ]
        if self.runtime.dry_run:
            build_cmd.append("--dry-run")
        await exectools.cmd_assert_async(build_cmd, env=self._doozer_env_vars)

        # sleep a little bit to account for time drift between systems
        await asyncio.sleep(10)

        # now that build is complete, fetch it
        return await self.get_latest_bootc_build()
