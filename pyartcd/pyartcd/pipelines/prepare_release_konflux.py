import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import time
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import click
import gitlab
from artcommonlib import exectools
from artcommonlib.assembly import AssemblyTypes, assembly_config_struct, assembly_group_config
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.model import Model
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import convert_remote_git_to_ssh, new_roundtrip_yaml_handler
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import Issue, Issues, ShipmentConfig, Snapshot, SnapshotSpec
from elliottlib.shipment_utils import get_shipment_configs_by_kind
from ghapi.all import GhApi
from tenacity import retry, stop_after_attempt, wait_fixed

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient
from pyartcd.util import (
    get_assembly_type,
    get_release_name_for_assembly,
)

_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()


class PrepareReleaseKonfluxPipeline:
    def __init__(
        self,
        slack_client: SlackClient,
        runtime: Runtime,
        group: str,
        assembly: str,
        github_token: str,
        gitlab_token: str,
        build_repo_url: Optional[str] = None,
        shipment_repo_url: Optional[str] = None,
        job_url: Optional[str] = None,
    ) -> None:
        self.runtime = runtime
        self.assembly = assembly
        self.group = group
        self._slack_client = slack_client
        self.github_token = github_token
        self.gitlab_token = gitlab_token

        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.application = KonfluxImageBuilder.get_application_name(self.group)
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self.doozer_working_dir = self.working_dir / "doozer-working"
        self._build_repo_dir = self.working_dir / "ocp-build-data-push"
        self._shipment_repo_dir = self.working_dir / "shipment-data-push"
        self.job_url = job_url
        self.dry_run = self.runtime.dry_run
        self.product = 'ocp'  # assume that product is ocp for now

        # Have clear pull and push targets for both the build and shipment repos
        build_repo_vars = self._build_repo_vars(build_repo_url)
        self.build_repo_pull_url, self.build_data_gitref, self.build_data_push_url = build_repo_vars
        self.shipment_repo_pull_url, self.shipment_repo_push_url = self._shipment_repo_vars(shipment_repo_url)
        self.build_data_repo = GitRepository(self._build_repo_dir, self.dry_run)
        self.shipment_data_repo = GitRepository(self._shipment_repo_dir, self.dry_run)

        # these will be initialized later
        self.assembly_type = None
        self.releases_config = None
        self.group_config = None
        self._issues_by_kind = None

        group_param = f'--group={group}'
        if self.build_data_gitref:
            group_param += f'@{self.build_data_gitref}'

        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            f'--working-dir={self.elliott_working_dir}',
            f'--data-path={self.build_repo_pull_url}',
        ]

        self._doozer_base_command = [
            'doozer',
            group_param,
            '--build-system=konflux',
            f'--working-dir={self.doozer_working_dir}',
            f'--data-path={self.build_repo_pull_url}',
        ]

    @staticmethod
    def basic_auth_url(url: str, token: str) -> str:
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme
        rest_of_the_url = url[len(scheme + "://") :]
        # the assumption here is that username can be anything
        # so we use oauth2 as a placeholder username
        # and the token as the password
        return f'https://oauth2:{token}@{rest_of_the_url}'

    def _build_repo_vars(self, build_repo_url: Optional[str]):
        build_repo_pull_url = (
            build_repo_url
            or self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
            or constants.OCP_BUILD_DATA_URL
        )
        build_data_gitref = None
        if "@" in build_repo_pull_url:
            build_repo_pull_url, build_data_gitref = build_repo_pull_url.split("@", 1)

        build_data_push_url = (
            self.runtime.config.get("build_config", {}).get("ocp_build_data_push_url") or constants.OCP_BUILD_DATA_URL
        )
        return build_repo_pull_url, build_data_gitref, build_data_push_url

    def _shipment_repo_vars(self, shipment_repo_url: Optional[str]):
        shipment_repo_pull_url = (
            shipment_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)
        )
        shipment_repo_push_url = self.runtime.config.get("shipment_config", {}).get(
            "shipment_data_push_url"
        ) or SHIPMENT_DATA_URL_TEMPLATE.format(self.product)
        return shipment_repo_pull_url, shipment_repo_push_url

    @cached_property
    def _errata_api(self) -> AsyncErrataAPI:
        return AsyncErrataAPI()

    @cached_property
    def _gitlab(self) -> gitlab.Gitlab:
        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.gitlab_token)
        gl.auth()
        return gl

    @property
    def release_name(self) -> str:
        return get_release_name_for_assembly(self.group, self.releases_config, self.assembly)

    @property
    def assembly_group_config(self) -> dict:
        return assembly_config_struct(Model(self.releases_config), self.assembly, "group", {})

    @property
    def shipment_config(self) -> dict:
        return self.assembly_group_config.get("shipment", [])

    async def run(self):
        self.setup_working_dir()
        await self.setup_repos()
        await self.validate_assembly()
        await self.prepare_shipment()

    def setup_working_dir(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self._build_repo_dir.exists():
            shutil.rmtree(self._build_repo_dir, ignore_errors=True)
        if self._shipment_repo_dir.exists():
            shutil.rmtree(self._shipment_repo_dir, ignore_errors=True)
        if self.elliott_working_dir.exists():
            shutil.rmtree(self.elliott_working_dir, ignore_errors=True)
        if self.doozer_working_dir.exists():
            shutil.rmtree(self.doozer_working_dir, ignore_errors=True)

    async def setup_repos(self):
        # setup build-data repo which should reside in GitHub
        # pushing is done via SSH
        await self.build_data_repo.setup(
            remote_url=convert_remote_git_to_ssh(self.build_data_push_url),
            upstream_remote_url=self.build_repo_pull_url,
        )
        await self.build_data_repo.fetch_switch_branch(self.build_data_gitref or self.group)

        # setup shipment-data repo which should reside in GitLab
        # pushing is done via basic auth
        await self.shipment_data_repo.setup(
            remote_url=self.basic_auth_url(self.shipment_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

        self.releases_config = yaml.load(await self.build_data_repo.read_file("releases.yml"))
        self.group_config = yaml.load(await self.build_data_repo.read_file("group.yml"))

    async def validate_assembly(self):
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
        # validate assembly and init release vars
        if self.releases_config.get("releases", {}).get(self.assembly) is None:
            raise ValueError(f"Assembly not found: {self.assembly}")
        if self.assembly_type == AssemblyTypes.STREAM:
            raise ValueError("Preparing a release from a stream assembly is no longer supported.")

        # validate product from group config
        merged_group_config = assembly_group_config(
            Model(self.releases_config), self.assembly, Model(self.group_config)
        ).primitive()
        group_product = merged_group_config.get("product", self.product)
        if group_product != self.product:
            raise ValueError(
                f"Product mismatch: {group_product} != {self.product}. This pipeline only supports {self.product}."
            )

    async def prepare_shipment(self):
        """Prepare the shipment for the assembly.
        This includes:
        - Validating the shipment advisory config
        - Generating shipment files for each advisory kind
        - Creating or updating the shipment MR
        - Creating or updating the build data PR with the shipment config
        """

        self.validate_shipment_config(self.shipment_config)

        shipment_config = self.shipment_config.copy()  # make a copy to avoid modifying the original
        env = shipment_config.get("env", "prod")
        shipment_url = shipment_config.get("url")

        shipments_by_kind: Dict[str, ShipmentConfig]
        # if shipment MR exists, load the shipment configs from it
        # assume advisory content and liveIDs are already set
        if shipment_url:
            shipments_by_kind = get_shipment_configs_by_kind(shipment_url)
        else:
            shipments_by_kind = {}
            for shipment_advisory_config in shipment_config.get("advisories", []):
                kind = shipment_advisory_config.get("kind")
                # init advisory content
                shipment: ShipmentConfig = await self.init_shipment(kind)
                # generate live ID
                shipment.shipment.data.releaseNotes.live_id = await self.reserve_live_id(shipment_advisory_config, env)
                shipments_by_kind[kind] = shipment

            # close errata API connection now that we have the live IDs
            if "_errata_api" in self.__dict__:
                await self._errata_api.close()

        # find builds for the advisories
        # if builds are already set, this will overwrite them
        # in an ideal case, content should be the same
        # and any additional builds should be pinned in the assembly config
        kind_to_builds = await self.find_builds_all()
        if kind_to_builds["olm_builds_not_found"]:
            bundle_nvrs = await self.find_or_build_bundle_builds(kind_to_builds["olm_builds_not_found"])
            kind_to_builds["metadata"] = kind_to_builds["metadata"] + bundle_nvrs

        for kind, shipment in shipments_by_kind.items():
            shipment.shipment.snapshot = await self.get_snapshot(kind, kind_to_builds[kind])

        # now that we have basic shipment configs setup, we can commit them to shipment MR
        if not shipment_url:
            shipment_url = await self.create_shipment_mr(shipments_by_kind, env)
            await self._slack_client.say_in_thread(f"Shipment MR created: {shipment_url}")
            shipment_config["url"] = shipment_url
        else:
            _LOGGER.info("Shipment MR already exists: %s. Checking if it needs an update..", shipment_url)
            updated = await self.update_shipment_mr(shipments_by_kind, env, shipment_url)
            if updated:
                await self._slack_client.say_in_thread(f"Shipment MR updated: {shipment_url}")

        # create or update build data PR
        # commit any new builds that might be getting added before bug finding
        await self.create_update_build_data_pr(shipment_config)

        # IMPORTANT: Bug Finding is special, it dynamically categorizes tracker bugs based on where the builds are found.
        # The Bug-Finder needs standardized access to shipment configs and respective builds via shipment MR.
        # Therefore bug finding should be run only after:
        # - shipment MR is created with all the right builds
        # - shipment MR is committed to build-data
        # Then the output of Bug-Finder is committed to shipment MR
        permissive = False
        if self.assembly_type in (AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE):
            permissive = True
        for kind, shipment in shipments_by_kind.items():
            shipment.shipment.data.releaseNotes.issues = await self.find_bugs(kind, permissive=permissive)

        await self.update_shipment_mr(shipments_by_kind, env, shipment_url)

    async def find_or_build_bundle_builds(self, operator_nvrs: list[str]):
        async def get_olm_operators() -> list[str]:
            cmd = self._doozer_base_command + [f'--assembly={self.assembly}', "olm-bundle:list-olm-operators"]
            rc, stdout, stderr = await exectools.cmd_gather_async(cmd)
            if stderr:
                _LOGGER.info("stderr:\n %s", stderr)
            if stdout:
                _LOGGER.info("stdout:\n %s", stdout)
            if rc != 0:
                raise RuntimeError(f"cmd failed with exit code {rc}: {cmd}")
            return [line.strip() for line in stdout.splitlines() if line.strip()]

        olm_operators = await get_olm_operators()
        olm_operator_nvrs = []
        for nvr in operator_nvrs:
            parsed_nvr = parse_nvr(nvr)
            if parsed_nvr["name"] in olm_operators:
                olm_operator_nvrs.append(nvr)

        kubeconfig = os.getenv("KONFLUX_SA_KUBECONFIG")
        if not kubeconfig:
            raise ValueError("KONFLUX_SA_KUBECONFIG environment variable is required to build bundle image")
        cmd = (
            self._doozer_base_command
            + [
                '--assembly=stream',
                "beta:images:konflux:bundle",
                f'--konflux-kubeconfig={kubeconfig}',
                "--output=json",
                "--",
            ]
            + olm_operator_nvrs
        )
        await exectools.cmd_assert_async(cmd)
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd)
        bundle_nvrs = []
        if stdout:
            out = json.loads(stdout)
            bundle_nvrs = out.get("results", [])

        return bundle_nvrs

    def validate_shipment_config(self, shipment_config: dict):
        """Validate the given shipment configuration for an assembly.
        This includes
        - validating shipment MR if it exists
        - validating shipment advisories and kinds
        - making sure no overlap with assembly group advisories
        - validating shipment env
        :raises ValueError: If the shipment configuration is invalid.
        """

        shipment_url = shipment_config.get("url")
        if shipment_url:
            self.validate_shipment_mr(shipment_url)

        shipment_advisories = shipment_config.get("advisories")
        if not shipment_advisories:
            raise ValueError("Shipment config should specify which advisories to create and prepare")

        if not all(advisory.get("kind") for advisory in shipment_advisories):
            raise ValueError("Shipment config should specify `kind` for each advisory")

        group_advisories = set(self.assembly_group_config.get("advisories", {}).keys())
        shipment_advisory_kinds = {advisory.get("kind") for advisory in shipment_advisories}
        common = shipment_advisory_kinds & group_advisories
        if common:
            raise ValueError(
                f"Shipment config should not specify advisories that are already defined in assembly.group.advisories: {common}"
            )

        env = shipment_config.get("env", "prod")
        if env not in ["prod", "stage"]:
            raise ValueError("Shipment config `env` should be either `prod` or `stage`")

    def validate_shipment_mr(self, shipment_url: str):
        """Validate the shipment MR
        :param shipment_url: The URL of the existing shipment MR to validate
        :raises ValueError: If the MR is not valid or does not match the expected repositories.
        """

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        project = self._gitlab.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)

        # Make sure MR is valid
        # and aligns with the push and pull repos
        if mr.state != "opened":
            raise ValueError(f"MR state {mr.state} is not opened. This is not supported.")

        if target_project_path not in self.shipment_repo_pull_url:
            raise ValueError(
                f"MR target project {target_project_path} does not match the pull repo {self.shipment_repo_pull_url}"
            )

        source_project_path = self._gitlab.projects.get(mr.source_project_id).path_with_namespace
        if source_project_path not in self.shipment_repo_push_url:
            raise ValueError(
                f"MR source project {source_project_path} does not match the push repo {self.shipment_repo_push_url}"
            )

        if mr.target_branch != "main":
            raise ValueError(f"MR target branch {mr.target_branch} is not main. This is not supported.")

        _LOGGER.info("Shipment MR is valid: %s", shipment_url)

    async def reserve_live_id(self, shipment_advisory_config: dict, env: str) -> Optional[str]:
        """Reserve a live ID for the shipment advisory.
        :param shipment_advisory_config: The shipment advisory configuration to reserve a live ID for
        :param env: The environment for which the live ID is being reserved (prod or stage)
        :return: The reserved live ID or None if it could not be reserved
        """

        # a liveID is required for prod, but not for stage
        # so if it is missing, we need to reserve one
        kind = shipment_advisory_config.get("kind")
        live_id = shipment_advisory_config.get("live_id")
        if env == "prod" and not live_id:
            _LOGGER.info("Requesting liveID for %s advisory", kind)
            if self.dry_run:
                _LOGGER.info("[DRY-RUN] Would've reserved liveID for %s advisory", kind)
                live_id = "DRY_RUN_LIVE_ID"
            else:
                live_id = await self._errata_api.reserve_live_id()
            if not live_id:
                raise ValueError(f"Failed to get liveID for {kind} advisory")
            shipment_advisory_config["live_id"] = live_id
        return live_id

    async def init_shipment(self, kind: str) -> ShipmentConfig:
        """Initialize a shipment for the given kind.
        :param kind: The kind for which to initialize shipment
        :return: A ShipmentConfig object initialized with the given kind
        """

        create_cmd = self._elliott_base_command + [
            f'--shipment-path={self.shipment_repo_pull_url}',
            "shipment",
            "init",
            f"--advisory-key={kind}",
            f"--application={self.application}",
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(create_cmd, check=False)
        if stderr:
            _LOGGER.info("Shipment init command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment init command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {create_cmd}")

        out = yaml.load(stdout)
        shipment = ShipmentConfig(**out)
        return shipment

    async def get_snapshot(self, kind: str, builds: list) -> Snapshot:
        """Get a snapshot for the given kind.
        :param kind: The kind for which to get a snapshot
        :return: A Snapshot object
        """

        # store builds in a temporary file, each nvr string in a new line
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            for nvr in builds:
                temp_file.write(nvr.encode())
                temp_file.write(b'\n')
            temp_file.flush()
            temp_file_path = temp_file.name

        # now call elliott snapshot new -f <temp_file_path>
        snapshot_cmd = self._elliott_base_command + [
            "snapshot",
            "new",
            f"--builds-file={temp_file_path}",
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(snapshot_cmd)
        if stderr:
            _LOGGER.info("Shipment snapshot new command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment snapshot new command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {snapshot_cmd}")

        # remove the temporary file
        os.unlink(temp_file_path)

        # parse the output of the snapshot new command, it should be valid yaml
        new_snapshot_obj = yaml.load(stdout)
        # make some assertions that this is a valid snapshot object
        if new_snapshot_obj.get("apiVersion") != API_VERSION or new_snapshot_obj.get("kind") != KIND_SNAPSHOT:
            raise ValueError(f"Snapshot object is not valid: {stdout}")

        return Snapshot(spec=SnapshotSpec(**new_snapshot_obj.get("spec")), nvrs=builds)

    async def find_builds(self, kind: str) -> List[str]:
        """Find builds for the given kind and return a list of NVRs.
        :param kind: The kind for which to find builds
        :return: A list of NVRs of the builds found
        """

        if kind not in ("image", "extras"):
            _LOGGER.warning("Shipment kind %s is not supported for build finding", kind)
            return []
        payload = True if kind == "image" else False

        find_builds_cmd = self._elliott_base_command + [
            "find-builds",
            "--kind=image",
            "--payload" if payload else "--non-payload",
            "--json=-",
        ]
        rc, stdout, stderr = await exectools.cmd_gather_async(find_builds_cmd)
        if stderr:
            _LOGGER.info("Shipment find-builds command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment find-builds command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {find_builds_cmd}")

        builds = []
        if stdout:
            out = json.loads(stdout)
            builds = out.get("builds", [])
        return builds

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    async def find_builds_all(self):
        """
        Run the elliott 'find-builds' command for images and return categorized build NVRs.
        Returns:
            tuple: Four lists containing:
                - payload_builds: NVRs of payload image builds
                - non_payload_builds: NVRs of non-payload image builds
                - olm_builds: NVRs of OLM bundle builds
                - olm_builds_not_found: NVRs of OLM operator builds for which no bundle build was found
        """
        cmd = self._elliott_base_command + ["find-builds", "--kind=image", "--all-image-types", "--json=-"]
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd)
        if not stdout:
            _LOGGER.warning("No output received from find-builds command.")
            return {"image": [], "extras": [], "metadata": [], "olm_builds_not_found": []}

        out = json.loads(stdout)
        _LOGGER.info("Find image builds: \n%s", stdout)

        kind_to_builds = {
            "image": out.get("payload", []),
            "extras": out.get("non_payload", []),
            "metadata": out.get("olm_builds", []),
            "olm_builds_not_found": out.get("olm_builds_not_found", []),
        }

        return kind_to_builds

    async def find_bugs(self, kind: str, permissive: bool = False) -> Optional[Issues]:
        """Find bugs for the given advisory kind and return an Issues object containing the bugs found.
        :param kind: The kind for which to find bugs
        :param permissive: Ignore invalid bugs that are found and continue
        :return: An Issues object containing the bugs found
        """

        if self._issues_by_kind is not None:
            return self._issues_by_kind.get(kind)

        find_bugs_cmd = self._elliott_base_command + [
            "find-bugs",
            "--output=json",
        ]
        if permissive:
            find_bugs_cmd.append("--permissive")
        rc, stdout, stderr = await exectools.cmd_gather_async(find_bugs_cmd)
        if stderr:
            _LOGGER.info("Shipment find bugs command stderr:\n %s", stderr)
        if stdout:
            _LOGGER.info("Shipment find bugs command stdout:\n %s", stdout)
        if rc != 0:
            raise RuntimeError(f"cmd failed with exit code {rc}: {find_bugs_cmd}")

        self._issues_by_kind = {}
        if stdout:
            for advisory_kind, bugs in json.loads(stdout).items():
                if not bugs:
                    continue
                issues = Issues(fixed=[Issue(id=b, source="issues.redhat.com") for b in bugs])
                self._issues_by_kind[advisory_kind] = issues

        return self._issues_by_kind.get(kind)

    async def create_shipment_mr(self, shipments_by_kind: Dict[str, ShipmentConfig], env: str) -> str:
        """Create a new shipment MR with the given shipment config files.
        :param shipments_by_kind: The shipment configurations to create the MR with, by advisory kind
        :param env: The environment for which the shipment is being prepared (prod or stage)
        :return: The URL of the created MR
        """

        _LOGGER.info("Creating shipment MR...")

        # Create branch name
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"prepare-shipment-{self.assembly}-{timestamp}"
        target_branch = "main"

        # Create and checkout branch
        await self.shipment_data_repo.create_branch(source_branch)

        # update shipment data repo with shipment configs
        commit_message = f"Add shipment configurations for {self.release_name}"
        updated = await self.update_shipment_data(shipments_by_kind, env, commit_message, source_branch)
        if not updated:
            # this should not happen
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        def _get_project(url):
            parsed_url = urlparse(url)
            project_path = parsed_url.path.strip('/').removesuffix('.git')
            return self._gitlab.projects.get(project_path)

        source_project = _get_project(self.shipment_repo_push_url)
        target_project = _get_project(self.shipment_repo_pull_url)

        mr_title = f"Shipment for {self.release_name}"
        mr_description = f"Created by job: {self.job_url}\n\n" if self.job_url else commit_message

        if self.dry_run:
            _LOGGER.info("[DRY-RUN] Would have created MR with title: %s", mr_title)
            mr_url = f"{self.gitlab_url}/placeholder/placeholder/-/merge_requests/placeholder"
        else:
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
            _LOGGER.info("Created Merge Request: %s", mr_url)

        return mr_url

    async def update_shipment_mr(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, shipment_url: str
    ) -> bool:
        """Update existing shipment MR with the given shipment config files.
        :param shipments_by_kind: The shipment configurations to update in the shipment MR by advisory kind
        :param env: The environment for which the shipment is being prepared (prod or stage)
        :param shipment_url: The URL of the existing shipment MR to update
        :return: True if the MR was updated successfully, False otherwise.
        """

        _LOGGER.info("Updating shipment MR: %s", shipment_url)

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        project = self._gitlab.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)

        # Checkout the MR branch
        source_branch = mr.source_branch
        await self.shipment_data_repo.fetch_switch_branch(source_branch, remote="origin")

        # Update shipment data
        commit_message = f"Update shipment configurations for {self.release_name}"
        updated = await self.update_shipment_data(shipments_by_kind, env, commit_message, source_branch)
        if not updated:
            _LOGGER.info("No changes in shipment data. MR will not be updated.")
            return False

        # Update the MR description
        description_update = f"Updated by job: {self.job_url}\n\n" if self.job_url else commit_message
        mr.description = f"{mr.description}\n\n{description_update}"

        if self.dry_run:
            _LOGGER.info("[DRY-RUN] Would have updated MR description: %s", mr.description)
        else:
            mr.save()
            _LOGGER.info("Shipment MR updated: %s", shipment_url)

        return True

    async def update_shipment_data(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, commit_message: str, branch: str
    ) -> bool:
        """Update shipment data repo with the given shipment config files.
        Commits the changes and push to the remote repo.
        :param shipments_by_kind: The shipment configurations to update in the shipment data repo by advisory kind
        :param env: The environment for which the shipment is being prepared (prod or stage)
        :param commit_message: The commit message to use for the changes
        :param branch: The branch to update in the shipment data repo
        :return: True if the changes were committed and pushed successfully, False otherwise.
        """

        relative_target_dir = Path("shipment") / self.product / self.group / self.application / env
        target_dir = self.shipment_data_repo._directory / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        # Get the timestamp from the branch name
        # which we need for filenames
        # The branch name is expected to be in the format: prepare-shipment-<assembly>-<timestamp>
        timestamp = branch.split("-")[-1]

        for advisory_kind, shipment_config in shipments_by_kind.items():
            filename = f"{self.assembly}.{advisory_kind}.{timestamp}.yaml"
            filepath = relative_target_dir / filename
            _LOGGER.info("Updating shipment file: %s", filename)
            shipment_dump = shipment_config.model_dump(exclude_unset=True, exclude_none=True)
            out = StringIO()
            yaml.dump(shipment_dump, out)
            await self.shipment_data_repo.write_file(filepath, out.getvalue())
        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        return await self.shipment_data_repo.commit_push(commit_message, safe=True)

    async def create_update_build_data_pr(self, shipment_config: dict) -> bool:
        """Create or update a pull request in the build data repo with the updated shipment config.
        :param shipment_config: The shipment configuration to update in the assembly definition
        :return: True if the PR was created or updated successfully, False otherwise.
        """

        branch = f"update-shipment-{self.release_name}"
        updated = await self.update_build_data(shipment_config, branch)
        if not updated:
            return False

        api = GhApi()
        target_repo = self.build_repo_pull_url.split('/')[-1].replace('.git', '')
        source_owner = self.build_data_push_url.split('/')[-2]
        target_owner = self.build_repo_pull_url.split('/')[-2]

        head = f"{source_owner}:{branch}"
        base = self.build_data_gitref or self.group
        api = GhApi(owner=target_owner, repo=target_repo, token=self.github_token)
        existing_prs = api.pulls.list(
            state="open",
            head=head,
            base=base,
        )
        pull_number = None
        if not existing_prs.items:
            pr_title = f"Update shipment for assembly {self.assembly}"
            pr_body = f"This PR updates the shipment data for assembly {self.assembly}."
            if self.job_url:
                pr_body += f"\n\nCreated by job: {self.job_url}"

            if self.dry_run:
                _LOGGER.info("[DRY-RUN] Would have created a new PR with title '%s'", pr_title)
                return True

            result = api.pulls.create(
                head=head,
                base=base,
                title=pr_title,
                body=pr_body,
                maintainer_can_modify=True,
            )
            _LOGGER.info("PR to update shipment created: %s", result.html_url)
            await self._slack_client.say_in_thread(f"PR to update shipment created: {result.html_url}")
            pull_number = result.number
        else:
            _LOGGER.info("Existing PR to update shipment found: %s", existing_prs.items[0].html_url)
            pull_number = existing_prs.items[0].number

            if self.dry_run:
                _LOGGER.info("[DRY-RUN] Would have updated PR with number %s", pull_number)
                return True

            pr_body = existing_prs.items[0].body
            if self.job_url:
                pr_body += f"\n\nUpdated by job: {self.job_url}"
            result = api.pulls.update(
                pull_number=pull_number,
                body=pr_body,
            )
            _LOGGER.info("PR to update shipment updated: %s", result.html_url)
            await self._slack_client.say_in_thread(f"PR to update shipment updated: {result.html_url}")

        await self._slack_client.say_in_thread(f"Waiting for PR to update shipment to be merged: {result.html_url}")

        # wait until the PR is merged
        timeout = 60 * 60  # 1 hour
        start_time = time.time()
        while True:
            if (time.time() - start_time) > timeout:
                raise TimeoutError(f"Timeout waiting for PR to update shipment to be merged: {result.html_url}")
            await asyncio.sleep(10)
            pr = api.pulls.get(pull_number)
            if pr.merged:
                _LOGGER.info("PR to update shipment was merged: %s", pr.html_url)
                break
            if pr.state == "closed":
                _LOGGER.info("PR to update shipment was closed: %s", pr.html_url)
                raise RuntimeError(f"PR to update shipment was closed: {pr.html_url}")
            _LOGGER.info("Waiting for PR to update shipment to be merged: %s", pr.html_url)

        return True

    async def update_build_data(self, shipment_config: dict, branch: str) -> bool:
        """Update releases.yml in build data repo with the given shipment assembly config.
        Commits the changes and push to the remote repo.
        :param shipment_config: The shipment configuration to update in the assembly definition
        :param branch: The branch to update in the build data repo
        :return: True if the changes were committed and pushed successfully, False otherwise.
        """

        group_config = (
            self.releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})
        )

        # Assembly key names are not always exact, they can end in special chars like !,?,-
        # to indicate special inheritance rules. So respect those
        # https://art-docs.engineering.redhat.com/assemblies/#inheritance-rules
        shipment_key = next(k for k in group_config.keys() if k.startswith("shipment"))
        group_config[shipment_key] = shipment_config

        if await self.build_data_repo.does_branch_exist_on_remote(branch, remote="origin"):
            await self.build_data_repo.fetch_switch_branch(branch, remote="origin")
        else:
            await self.build_data_repo.create_branch(branch)

        out = StringIO()
        yaml.dump(self.releases_config, out)
        await self.build_data_repo.write_file("releases.yml", out.getvalue())
        await self.build_data_repo.add_all()
        await self.build_data_repo.log_diff()

        commit_message = f"Update shipment for assembly {self.assembly}"
        return await self.build_data_repo.commit_push(commit_message, safe=True)


@cli.command("prepare-release-konflux")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group to operate on e.g. openshift-4.18",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly to operate on e.g. 4.18.5",
)
@click.option(
    '--build-repo-url',
    help='ocp-build-data repo to use. Defaults to group branch - to use a different branch/commit use repo@branch',
)
@click.option(
    '--shipment-repo-url',
    help='shipment-data repo to use for reading and as shipment MR target. Defaults to main branch. Should reside in gitlab.cee.redhat.com',
)
@pass_runtime
@click_coroutine
async def prepare_release(
    runtime: Runtime,
    group: str,
    assembly: str,
    build_repo_url: Optional[str],
    shipment_repo_url: Optional[str],
):
    job_url = os.getenv('BUILD_URL')

    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required to create a pull request")

    gitlab_token = os.getenv("GITLAB_TOKEN")
    if not gitlab_token:
        raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")

    if assembly == "stream":
        raise click.BadParameter("Release cannot be prepared from stream assembly.")

    slack_client = runtime.new_slack_client()
    slack_client.bind_channel(group)
    await slack_client.say_in_thread(f":construction: prepare-release-konflux for {assembly} :construction:")

    try:
        # start pipeline
        pipeline = PrepareReleaseKonfluxPipeline(
            slack_client=slack_client,
            runtime=runtime,
            group=group,
            assembly=assembly,
            github_token=github_token,
            gitlab_token=gitlab_token,
            build_repo_url=build_repo_url,
            shipment_repo_url=shipment_repo_url,
            job_url=job_url,
        )
        await pipeline.run()
        await slack_client.say_in_thread(f":white_check_mark: prepare-release-konflux for {assembly} completes.")
    except Exception as e:
        await slack_client.say_in_thread(f":warning: prepare-release-konflux for {assembly} has result FAILURE.")
        raise e
