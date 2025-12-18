import asyncio
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional
from urllib.parse import urlparse

import asyncstdlib as a
import click
import gitlab
import semver
from artcommonlib import exectools
from artcommonlib.assembly import AssemblyTypes, assembly_config_struct, assembly_group_config
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBundleBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.model import Model
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import convert_remote_git_to_ssh, new_roundtrip_yaml_handler
from doozerlib.backend.konflux_client import API_VERSION, KIND_SNAPSHOT
from doozerlib.cli.release_gen_payload import (
    assembly_imagestream_base_name_generic,
    default_imagestream_namespace_base_name,
    payload_imagestream_namespace_and_name,
)
from elliottlib.errata import push_cdn_stage
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.shipment_model import Issue, ReleaseNotes, ShipmentConfig, Snapshot, SnapshotSpec, Tools
from elliottlib.shipment_utils import get_shipment_configs_from_mr, set_jira_bug_ids
from ghapi.all import GhApi
from tenacity import retry, stop_after_attempt, wait_fixed

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.jira_client import JIRAClient
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient
from pyartcd.util import (
    get_assembly_basis,
    get_assembly_type,
    get_release_name_for_assembly,
    nightlies_with_pullspecs,
)

yaml = new_roundtrip_yaml_handler()


class PrepareReleaseKonfluxPipeline:
    def __init__(
        self,
        slack_client: SlackClient,
        runtime: Runtime,
        group: str,
        assembly: str,
        build_data_repo_url: Optional[str] = None,
        shipment_data_repo_url: Optional[str] = None,
        inject_build_data_repo: bool = False,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.assembly = assembly
        self.group = group
        self.inject_build_data_repo = inject_build_data_repo

        self._slack_client = slack_client

        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self.doozer_working_dir = self.working_dir / "doozer-working"
        self._build_data_repo_dir = self.working_dir / "ocp-build-data-push"
        self._shipment_data_repo_dir = self.working_dir / "shipment-data-push"
        self.dry_run = self.runtime.dry_run
        self.product = 'ocp'  # assume that product is ocp for now

        # Have clear pull and push targets for both the build and shipment repos
        self.build_data_repo_pull_url, self.build_data_gitref, self.build_data_push_url = self._build_data_repo_vars(
            build_data_repo_url
        )
        self.shipment_data_repo_pull_url, self.shipment_data_repo_push_url = self._shipment_data_repo_vars(
            shipment_data_repo_url
        )
        self.build_data_repo = GitRepository(self._build_data_repo_dir, self.dry_run)
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, self.dry_run)

        # these will be initialized later
        self.assembly_type = None
        self.releases_config = None
        self.release_version = None
        self.group_config = None
        self.github_token = None
        self.gitlab_token = None
        self.jira_token = None
        self.job_url = None
        self.jira_client = None
        self.updated_assembly_group_config = None
        self.olm_operators = None
        self.shipment_mr_url = None  # Track shipment MR URL for draft/ready management
        self.fbc_build_errors = []  # Track FBC build errors for UNSTABLE marking

        group_param = f'--group={group}'
        if self.build_data_gitref:
            group_param += f'@{self.build_data_gitref}'

        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=konflux',
            f'--working-dir={self.elliott_working_dir}',
            f'--data-path={self.build_data_repo_pull_url}',
        ]

        self._elliott_base_command_for_brew = [
            'elliott',
            group_param,
            f'--assembly={self.assembly}',
            '--build-system=brew',
            f'--working-dir={self.elliott_working_dir}',
            f'--data-path={self.build_data_repo_pull_url}',
        ]

        self._doozer_base_command = [
            'doozer',
            group_param,
            '--build-system=konflux',
            f'--working-dir={self.doozer_working_dir}',
            f'--data-path={self.build_data_repo_pull_url}',
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

    def _build_data_repo_vars(self, build_data_repo_url: Optional[str]):
        build_data_repo_pull_url = (
            build_data_repo_url
            or self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
            or constants.OCP_BUILD_DATA_URL
        )
        build_data_gitref = None
        if "@" in build_data_repo_pull_url:
            build_data_repo_pull_url, build_data_gitref = build_data_repo_pull_url.split("@", 1)

        build_data_push_url = (
            self.runtime.config.get("build_config", {}).get("ocp_build_data_push_url") or constants.OCP_BUILD_DATA_URL
        )
        return build_data_repo_pull_url, build_data_gitref, build_data_push_url

    def _shipment_data_repo_vars(self, shipment_data_repo_url: Optional[str]):
        shipment_data_repo_pull_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        shipment_data_repo_push_url = (
            self.runtime.config.get("shipment_config", {}).get("shipment_data_push_url") or SHIPMENT_DATA_URL_TEMPLATE
        )
        return shipment_data_repo_pull_url, shipment_data_repo_push_url

    @cached_property
    def _errata_api(self) -> AsyncErrataAPI:
        return AsyncErrataAPI()

    @cached_property
    def _gitlab(self) -> gitlab.Gitlab:
        gl = gitlab.Gitlab(self.gitlab_url, private_token=self.gitlab_token)
        gl.auth()
        return gl

    @cached_property
    def release_name(self) -> str:
        return get_release_name_for_assembly(self.group, self.releases_config, self.assembly)

    @cached_property
    def release_date(self):
        return self.assembly_group_config.get("release_date")

    @property
    def assembly_group_config(self) -> Model:
        return assembly_config_struct(Model(self.releases_config), self.assembly, "group", {})

    @property
    def shipment_config(self) -> dict:
        return self.assembly_group_config.get("shipment", {})

    async def initialize(self):
        self.check_env_vars()
        self.setup_working_dir()
        await self.setup_repos()
        await self.validate_assembly()
        self.jira_client = JIRAClient.from_url(self.runtime.config["jira"]["url"], token_auth=self.jira_token)

    async def run(self):
        await self.initialize()
        await self.check_blockers()
        err = None
        try:
            await self.prepare_et_advisories()
            await self.prepare_shipment()
            await self.handle_jira_ticket()
        except Exception as ex:
            self.logger.error(f"Unable to prepare release: {ex}", exc_info=True)
            err = ex
        finally:
            self.logger.info("Prep failed. Trying to update assembly in case of partial success ...")
            await self.create_update_build_data_pr()

        if err:
            raise err

        await self.set_shipment_mr_ready()
        await self.verify_payload()

        # Check for FBC build errors and exit with code 2 for UNSTABLE status
        if self.fbc_build_errors:
            error_count = len(self.fbc_build_errors)
            error_summary = f"Completed with {error_count} FBC build error(s):\n" + "\n".join(
                f"  - {error.split(chr(10))[0]}"
                for error in self.fbc_build_errors  # Only include first line of each error
            )
            self.logger.warning(error_summary)
            await self._slack_client.say_in_thread(
                f":warning: prepare-release-konflux completed with {error_count} FBC build error(s). "
                f"Successfully built FBC builds have been attached. Job marked as UNSTABLE.\n"
                f"Details:\n{chr(10).join(f'• {error.split(chr(10))[0]}' for error in self.fbc_build_errors)}"
            )
            # Exit with code 2 to mark Jenkins job as UNSTABLE
            # The Jenkinsfile should catch this exit code and set currentBuild.result = 'UNSTABLE'
            sys.exit(2)

    def check_env_vars(self):
        github_token = os.getenv('GITHUB_TOKEN')
        if not github_token:
            raise ValueError("GITHUB_TOKEN environment variable is required to create a pull request")
        self.github_token = github_token

        gitlab_token = os.getenv("GITLAB_TOKEN")
        if not gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")
        self.gitlab_token = gitlab_token

        jira_token = os.environ.get("JIRA_TOKEN")
        if not self.runtime.dry_run and not jira_token:
            raise ValueError("JIRA_TOKEN environment variable is not set")
        self.jira_token = jira_token

        # Get the Jenkins job URL from environment variable
        self.job_url = os.getenv('BUILD_URL')

    def setup_working_dir(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self._build_data_repo_dir.exists():
            shutil.rmtree(self._build_data_repo_dir, ignore_errors=True)
        if self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)
        if self.elliott_working_dir.exists():
            shutil.rmtree(self.elliott_working_dir, ignore_errors=True)
        if self.doozer_working_dir.exists():
            shutil.rmtree(self.doozer_working_dir, ignore_errors=True)

    async def setup_repos(self):
        # setup build-data repo which should reside in GitHub
        # pushing is done via SSH
        await self.build_data_repo.setup(
            remote_url=convert_remote_git_to_ssh(self.build_data_push_url),
            upstream_remote_url=self.build_data_repo_pull_url,
        )
        await self.build_data_repo.fetch_switch_branch(self.build_data_gitref or self.group)

        # setup shipment-data repo which should reside in GitLab
        # pushing is done via basic auth
        await self.shipment_data_repo.setup(
            remote_url=self.basic_auth_url(self.shipment_data_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

        self.releases_config = yaml.load(await self.build_data_repo.read_file("releases.yml"))
        self.group_config = yaml.load(await self.build_data_repo.read_file("group.yml"))
        self.updated_assembly_group_config = Model(self.assembly_group_config.copy())

    async def validate_assembly(self):
        self.assembly_type = get_assembly_type(self.releases_config, self.assembly)
        self.logger.info(f"Assembly type: {self.assembly_type}")
        # validate assembly and init release vars
        if self.releases_config.get("releases", {}).get(self.assembly) is None:
            raise ValueError(f"Assembly not found: {self.assembly}")
        if self.assembly_type == AssemblyTypes.STREAM:
            raise ValueError("Preparing a release from a stream assembly is no longer supported.")

        if not self.release_date:
            raise ValueError("Can't find release date in assembly config")

        # validate product from group config
        merged_group_config = assembly_group_config(
            Model(self.releases_config), self.assembly, Model(self.group_config)
        ).primitive()
        group_product = merged_group_config.get("product", self.product)
        if group_product != self.product:
            raise ValueError(
                f"Product mismatch: {group_product} != {self.product}. This pipeline only supports {self.product}."
            )

    async def check_blockers(self):
        if self.assembly_type != AssemblyTypes.STANDARD:
            self.logger.info(f"Skipping Blocker Bugs check for non-standard assembly {self.assembly}")
            return
        self.logger.info(f"Checking Blocker Bugs for release {self.assembly}")
        cmd = self._elliott_base_command + ["find-bugs:blocker", "--exclude-status=ON_QA"]
        stdout = await self.execute_command_with_logging(cmd)
        match = re.search(r"Found ([0-9]+) bugs", str(stdout))
        if match and int(match[1]) != 0:
            self.logger.warning(
                f"{int(match[1])} Blocker Bugs found! Make sure to resolve these blocker bugs before proceeding to promote the release."
            )

    async def prepare_et_advisories(self):
        """
        Prepare and manage all ET advisories for the current assembly.

        This function performs the following steps:
        1. Checks if an advisory exists for the assembly; if not, creates one and updates the build data.
        2. Sweeps builds into the advisory.
        3. Sweeps bugs into the advisory.
        4. Attaches CVE flaw bugs to the advisory.
        5. Attempts to change the advisory state to QE.
        6. Attempts to trigger a push of the advisory to the CDN stage.
        """
        SUPPORTED_IMPETUSES = {"rpm", "rhcos"}
        impetus_advisories = self.assembly_group_config.get("advisories", {}).copy()
        if not impetus_advisories:
            self.logger.warning("No advisories configured for assembly %s", self.assembly)
            return
        if invalid := impetus_advisories.keys() - SUPPORTED_IMPETUSES:
            raise ValueError(f"Invalid advisory impetuses: {', '.join(invalid)}")

        # Create advisories if needed
        for impetus, advisory_num in impetus_advisories.items():
            self.logger.info("Preparing %s advisory for assembly %s ...", impetus, self.assembly)
            if advisory_num < 0:
                # create advisory
                advisory_type = (
                    "RHEA" if self.assembly_type == AssemblyTypes.STANDARD and self.assembly.endswith(".0") else "RHBA"
                )
                advisory_num = await self.create_advisory(advisory_type, impetus, self.release_date)
                await self._slack_client.say_in_thread(
                    f"ET {impetus} advisory {advisory_num} created with release date {self.release_date}"
                )
                self.updated_assembly_group_config.advisories[impetus] = impetus_advisories[impetus] = advisory_num

        # Update assembly PR
        await self.create_update_build_data_pr()

        # Sweep builds
        base_command = [item for item in self._elliott_base_command if item != '--build-system=konflux']
        for impetus, advisory_num in impetus_advisories.items():
            if advisory_num <= 0:
                raise ValueError(f"Invalid {impetus} advisory number: {advisory_num}")
            self.logger.info("Sweep builds into the the %s advisory ...", impetus)
            sweep_opts = []
            match impetus:
                case "rpm":
                    kind = "rpm"
                case "image" | "extras" | "rhcos":
                    kind = "image"
                    if impetus == "image":
                        sweep_opts.append("--payload")
                    elif impetus == "extras":
                        sweep_opts.append("--non-payload")
                    elif impetus == "rhcos":
                        sweep_opts.append("--only-rhcos")
                case _:
                    raise ValueError(f"Unknown impetus {impetus} for advisory preparation.")
            operate_cmd = ["find-builds", f"--kind={kind}", f"--attach={advisory_num}", "--clean"] + sweep_opts
            if self.dry_run:
                operate_cmd += ["--dry-run"]
            await self.run_cmd_with_retry(base_command, operate_cmd)

        # Find bugs
        self.logger.info("Finding %s bugs...", impetus)
        impetus_bugs = await self.find_bugs(build_system='brew')

        # Process bugs
        for impetus, advisory_num in impetus_advisories.items():
            bug_ids = impetus_bugs.get(impetus)
            if not bug_ids:
                self.logger.info("No bugs found for %s advisory.", impetus)
                continue

            # attach bugs
            self.logger.info("Attaching %s bugs to %s advisory %s: %s", len(bug_ids), impetus, advisory_num, bug_ids)
            operate_cmd = ["attach-bugs"] + bug_ids + [f"--advisory={advisory_num}"]
            if self.dry_run:
                operate_cmd += ["--dry-run"]
            await self.run_cmd_with_retry(self._elliott_base_command, operate_cmd)

            # unconditionally attach cve flaws
            self.logger.info("Attaching CVE flaws to %s advisory ...", impetus)
            operate_cmd = ["attach-cve-flaws", f"--advisory={advisory_num}"]
            if self.dry_run:
                operate_cmd += ["--dry-run"]
            await self.run_cmd_with_retry(base_command, operate_cmd)

            # change status to qe
            try:
                operate_cmd = ["change-state", "-s", "QE", "--from", "NEW_FILES", "-a", str(advisory_num)]
                if self.dry_run:
                    operate_cmd += ["--dry-run"]
                await self.run_cmd_with_retry(base_command, operate_cmd)
            except Exception as ex:
                self.logger.warning(f"Unable to move {impetus} advisory {advisory_num} to QE: {ex}")
                await self._slack_client.say_in_thread(
                    f"Unable to move {impetus} advisory {advisory_num} to QE. Details in log."
                )
                continue

            # push to CDN stage
            try:
                push_cdn_stage(advisory_num)
            except Exception as ex:
                self.logger.warning(f"Unable to trigger push rpm advisory {advisory_num} to CDN stage: {ex}")
                await self._slack_client.say_in_thread(
                    f"Unable to trigger push rpm advisory {advisory_num} to CDN stage. Details in log."
                )

    async def create_advisory(
        self, advisory_type: str, art_advisory_key: str, release_date: str, batch_id: int = 0
    ) -> int:
        self.logger.info("Creating advisory with type %s art_advisory_key %s ...", advisory_type, art_advisory_key)
        create_cmd = self._elliott_base_command + [
            "create",
            f"--type={advisory_type}",
            f"--art-advisory-key={art_advisory_key}",
            f"--assigned-to={self.runtime.config['advisory']['assigned_to']}",
            f"--manager={self.runtime.config['advisory']['manager']}",
            f"--package-owner={self.runtime.config['advisory']['package_owner']}",
        ]
        if batch_id:
            create_cmd.append(f"--batch-id={batch_id}")
        else:
            create_cmd.append(f"--date={release_date}")
        if not self.dry_run:
            create_cmd.append("--yes")
        stdout = await self.execute_command_with_logging(create_cmd)
        match = re.search(
            r"https:\/\/errata\.devel\.redhat\.com\/advisory\/([0-9]+)",
            stdout,
        )
        advisory_num = int(match[1])
        self.logger.info("Created %s advisory %s", art_advisory_key, advisory_num)
        return advisory_num

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    async def run_cmd_with_retry(self, base_cmd: list[str], cmd: list[str]):
        await self.execute_command_with_logging(base_cmd + cmd)

    async def prepare_shipment(self):
        """Prepare the shipment for the assembly.
        This includes:
        - Validating the shipment advisory config
        - Generating shipment files for each advisory kind
        - Creating or updating the shipment MR
        - Creating or updating the build data PR with the shipment config
        """

        self.validate_shipment_config(self.shipment_config)

        # make a copy to avoid modifying the original
        shipment_config = self.shipment_config.copy()
        env = shipment_config.get("env", "prod")
        shipment_url = shipment_config.get("url")

        shipments_by_kind: Dict[str, ShipmentConfig]
        # if shipment MR exists, load the shipment configs from it
        # assume advisory content and liveIDs are already set
        if shipment_url:
            shipments_by_kind = get_shipment_configs_from_mr(shipment_url)
        else:
            shipments_by_kind = {}
            for shipment_advisory_config in shipment_config.get("advisories", []):
                kind = shipment_advisory_config.get("kind")
                shipment: ShipmentConfig = await self.init_shipment(kind)

                # reserve live ID
                if kind != "fbc":
                    shipment.shipment.data.releaseNotes.live_id = await self.reserve_live_id(shipment_advisory_config)

                shipments_by_kind[kind] = shipment

            # close errata API connection now that we have the live IDs
            if "_errata_api" in self.__dict__:
                await self._errata_api.close()

            shipment_url = await self.create_shipment_mr(shipments_by_kind, env)
            await self._slack_client.say_in_thread(f"Shipment MR created: {shipment_url}")
            self.updated_assembly_group_config.shipment.url = shipment_url
            await self.create_update_build_data_pr()

        # find builds for the image, extras and metadata shipments
        kind_to_builds = await self.find_builds_all()

        # make sure that metadata shipment needs to be prepared
        # if so, build any missing bundle builds
        if "metadata" in shipments_by_kind and kind_to_builds["olm_builds_not_found"]:
            bundle_nvrs = await self.find_or_build_bundle_builds(kind_to_builds["olm_builds_not_found"])
            kind_to_builds["metadata"] += bundle_nvrs

        await self.verify_attached_operators(kind_to_builds)

        # make sure that fbc shipment needs to be prepared
        # find and build any missing fbc builds
        if "fbc" in shipments_by_kind:
            fbc_builds, fbc_errors = await self.find_or_build_fbc_builds(
                kind_to_builds["extras"] + kind_to_builds["image"]
            )
            kind_to_builds["fbc"] = fbc_builds
            if fbc_errors:
                # Track FBC build errors for later reporting
                for error in fbc_errors:
                    error_summary = (
                        f"FBC build failed for operator={error.get('operator')}, "
                        f"operator_nvr={error.get('operator_nvr')}, "
                        f"bundle_nvr={error.get('bundle_nvr')}: {error.get('error')}\n"
                        f"Traceback: {error.get('traceback')}"
                    )
                    self.fbc_build_errors.append(error_summary)

        # prepare snapshot from the found builds
        for kind, shipment in shipments_by_kind.items():
            shipment.shipment.snapshot = await self.get_snapshot(kind_to_builds[kind])

        # Update shipment MR with found builds
        await self.update_shipment_mr(shipments_by_kind, env, shipment_url)

        # IMPORTANT: Bug Finding is special, it dynamically categorizes tracker bugs based on where the builds are found.
        # The Bug-Finder needs standardized access to shipment configs and respective builds via shipment MR.
        # Therefore bug finding should be run only after:
        # - shipment MR is created with all the right builds
        # - shipment MR is committed to build-data
        # Then the output of Bug-Finder is committed to shipment MR
        for kind, shipment in shipments_by_kind.items():
            if kind == "fbc":
                continue
            bug_ids = (await self.find_bugs()).get(kind, [])
            set_jira_bug_ids(shipment.shipment.data.releaseNotes, bug_ids)

        # Update shipment MR with found bugs
        await self.update_shipment_mr(shipments_by_kind, env, shipment_url)

        # Attach CVE flaws
        if self.assembly_type not in (AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE):
            for kind, shipment in shipments_by_kind.items():
                if kind == "fbc":
                    continue
                await self.attach_cve_flaws(kind, shipment)

            # Update shipment MR with found CVE flaws
            await self.update_shipment_mr(shipments_by_kind, env, shipment_url)

    async def verify_attached_operators(self, kind_to_builds: Dict[str, List[str]]):
        """
        Verifies that all operators and operands referenced by metadata (bundle) builds
        are present in the release's attached image builds.

        Args:
            kind_to_builds: A dictionary mapping build types ('metadata', 'image', 'extras')
                            to lists of NVRs.

        Raises:
            ValueError: If any referenced operator or operand NVR is not found in the
                        attached image builds.
        """
        self.logger.info("Verify_attached_operators ...")
        olm_builds = kind_to_builds.get('metadata')
        if not olm_builds:
            # No metadata builds to verify, so the check passes.
            return
        image_builds = kind_to_builds['image'] + kind_to_builds['extras']
        kdb = KonfluxDb()
        kdb.bind(KonfluxBundleBuildRecord)
        tasks = [kdb.get_latest_build(nvr=build, outcome=KonfluxBuildOutcome.SUCCESS) for build in olm_builds]
        olm_records = await asyncio.gather(*tasks)
        missing_references = []
        for record in filter(None, olm_records):
            # Check the main operator NVR
            if record.operator_nvr not in image_builds:
                missing_references.append(
                    f"Bundle {record.nvr} references operator {record.operator_nvr}, which is not in the release."
                )
            # Check all operand NVRs
            for operand in record.operand_nvrs:
                if operand not in image_builds:
                    missing_references.append(
                        f"Bundle {record.nvr} references operand {operand}, which is not in the release."
                    )
        if missing_references:
            error_details = "\n".join(missing_references)
            self.logger.warning("Verify_attached_operators check failed with the following errors:\n%s", error_details)
            raise ValueError("Verify_attached_operators check failed. See logs for details.")
        self.logger.info("Verify_attached_operators complete success")

    async def filter_olm_operators(self, nvrs: list[str]) -> list[str]:
        """
        Filter the given list of NVRs to only include OLM operators.
        """

        async def get_olm_operators() -> list[str]:
            if self.olm_operators:
                return self.olm_operators

            cmd = self._doozer_base_command + ["olm-bundle:list-olm-operators"]
            stdout = await self.execute_command_with_logging(cmd)
            self.olm_operators = [line.strip() for line in stdout.splitlines() if line.strip()]
            return self.olm_operators

        olm_operators = await get_olm_operators()
        return [nvr for nvr in nvrs if parse_nvr(nvr)["name"] in olm_operators]

    async def find_or_build_fbc_builds(self, nvrs: list[str]) -> tuple[list[str], list[dict]]:
        """
        For the given list of NVRs, determine which are OLM operators and fetch FBC builds for them.
        Trigger FBC builds for them if needed.

        Args:
            nvrs (list[str]): List of NVRs to check and build FBC for.

        Returns:
            tuple[list[str], list[dict]]: A tuple of (successful_nvrs, errors).
                - successful_nvrs: List of FBC NVRs that were successfully found or built.
                - errors: List of error dictionaries with details about failed builds.

        Raises:
            IOError: If the command produces no parseable output.
        """
        olm_operator_nvrs = await self.filter_olm_operators(nvrs)

        major, minor = self.release_name.split('.')[:2]
        version = f"{major}.{minor}"
        release_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        message = f"Rebase FBC segment with release {release_str}"

        cmd = self._doozer_base_command + [
            f'--assembly={self.assembly}',
            "beta:fbc:rebase-and-build",
            f"--version={version}",
            f"--release={release_str}",
            f"--message={message}",
            "--output=json",
        ]
        if self.dry_run:
            cmd += ["--dry-run"]
        cmd += ["--", *olm_operator_nvrs]

        # Run command and tolerate non-zero exit code
        # The doozer command returns partial results in JSON even on failure
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)

        # Parse JSON output to get both successful builds and errors
        try:
            output_data = json.loads(stdout)
        except json.JSONDecodeError as ex:
            error_msg = f"Failed to parse FBC rebase-and-build JSON output (rc={rc}): {ex}"
            if stdout:
                error_msg += f"\nStdout: {stdout}"
            if stderr:
                error_msg += f"\nStderr: {stderr}"
            self.logger.error(error_msg)
            raise IOError(error_msg)

        successful_nvrs = output_data.get("nvrs", [])
        errors = output_data.get("errors", [])
        failed_count = output_data.get("failed_count", 0)
        success_count = output_data.get("success_count", 0)

        if errors:
            self.logger.warning(
                f"FBC rebase-and-build completed with {failed_count} failure(s) and {success_count} success(es)"
            )
            for error in errors:
                error_msg = (
                    f"Failed to build FBC for operator={error.get('operator')}, "
                    f"operator_nvr={error.get('operator_nvr')}, "
                    f"bundle_nvr={error.get('bundle_nvr')}: {error.get('error')}"
                )
                self.logger.error(error_msg)

        self.logger.info(f"Returning {len(successful_nvrs)} successful FBC build(s)")
        return successful_nvrs, errors

    async def find_or_build_bundle_builds(self, nvrs: list[str]) -> list[str]:
        """
        For the given list of NVRs, determine which are OLM operators and fetch bundle builds for them.
        Trigger bundle builds for them if needed.

        Args:
            nvrs (list[str]): List of NVRs to check and build bundles for.

        Returns:
            list[str]: List of bundle NVRs that were found or built.
        """

        olm_operator_nvrs = await self.filter_olm_operators(nvrs)

        kubeconfig = os.getenv("KONFLUX_SA_KUBECONFIG")
        if not kubeconfig:
            raise ValueError("KONFLUX_SA_KUBECONFIG environment variable is required to build bundle image")
        cmd = self._doozer_base_command + [
            '--assembly=stream',
            "beta:images:konflux:bundle",
            f'--konflux-kubeconfig={kubeconfig}',
            "--output=json",
        ]
        if self.dry_run:
            cmd += ["--dry-run"]
        cmd += ["--", *olm_operator_nvrs]
        stdout = await self.execute_command_with_logging(cmd)
        bundle_nvrs = json.loads(stdout).get("nvrs", []) if stdout else []
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

        if target_project_path not in self.shipment_data_repo_pull_url:
            raise ValueError(
                f"MR target project {target_project_path} does not match the pull repo {self.shipment_data_repo_pull_url}"
            )

        source_project_path = self._gitlab.projects.get(mr.source_project_id).path_with_namespace
        if source_project_path not in self.shipment_data_repo_push_url:
            raise ValueError(
                f"MR source project {source_project_path} does not match the push repo {self.shipment_data_repo_push_url}"
            )

        if mr.target_branch != "main":
            raise ValueError(f"MR target branch {mr.target_branch} is not main. This is not supported.")

        self.logger.info("Shipment MR is valid: %s", shipment_url)

    async def reserve_live_id(self, shipment_advisory_config: dict) -> Optional[str]:
        """Reserve a live ID for the shipment advisory.
        :param shipment_advisory_config: The shipment advisory configuration to reserve a live ID for
        :return: The reserved live ID or None if it could not be reserved
        """

        # so if it is missing, we need to reserve one
        kind = shipment_advisory_config.get("kind")
        live_id = shipment_advisory_config.get("live_id")
        if not live_id:
            self.logger.info("Requesting liveID for %s advisory", kind)
            if self.dry_run:
                self.logger.info("[DRY-RUN] Would've reserved liveID for %s advisory", kind)
                live_id = "DRY_RUN_LIVE_ID"
            else:
                live_id = await self._errata_api.reserve_live_id()
            if not live_id:
                raise ValueError(f"Failed to get liveID for {kind} advisory")
            for config in self.updated_assembly_group_config.shipment.advisories:
                if config.kind == kind:
                    config.live_id = live_id
                    break  # there should be only one advisory with this kind
        return live_id

    async def init_shipment(self, kind: str) -> ShipmentConfig:
        """Initialize a shipment for the given kind.
        :param kind: The kind for which to initialize shipment
        :return: A ShipmentConfig object initialized with the given kind
        """

        create_cmd = self._elliott_base_command + [
            f'--shipment-path={self.shipment_data_repo_pull_url}',
            "shipment",
            "init",
            kind,
        ]
        stdout = await self.execute_command_with_logging(create_cmd)
        # Convert CommentedMap to regular Python objects before creating Pydantic model
        out = Model(yaml.load(stdout)).primitive()
        shipment = ShipmentConfig(**out)

        if self.inject_build_data_repo:
            # inject the build repo into the shipment config
            repo_username = self.build_data_repo_pull_url.split('/')[-2]
            build_commit = self.build_data_gitref or self.group
            if not shipment.shipment.tools:
                shipment.shipment.tools = Tools()
            shipment.shipment.tools.build_data = f"{repo_username}@{build_commit}"

        return shipment

    async def get_snapshot(self, builds: list) -> Optional[Snapshot]:
        """
        Construct a snapshot object from a list of build NVRs by invoking the elliott snapshot new command.

        Args:
            builds (list): List of build NVRs to include in the snapshot.

        Returns:
            Optional[Snapshot]: A Snapshot object containing the builds, or None if the builds list is empty.
        """
        if not builds:
            return None

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
        konflux_art_images_auth_file = os.getenv("KONFLUX_ART_IMAGES_AUTH_FILE")
        if konflux_art_images_auth_file:
            snapshot_cmd.append(f"--pull-secret={konflux_art_images_auth_file}")
        stdout = await self.execute_command_with_logging(snapshot_cmd)

        # remove the temporary file
        os.unlink(temp_file_path)

        # parse the output of the snapshot new command, it should be valid yaml
        new_snapshot_obj = yaml.load(stdout)
        # make some assertions that this is a valid snapshot object
        if new_snapshot_obj.get("apiVersion") != API_VERSION or new_snapshot_obj.get("kind") != KIND_SNAPSHOT:
            raise ValueError(f"Snapshot object is not valid: {stdout}")

        return Snapshot(spec=SnapshotSpec(**new_snapshot_obj.get("spec")), nvrs=sorted(builds))

    async def execute_command_with_logging(self, cmd: list[str]) -> str:
        """
        Execute a command asynchronously and log its output. Stream stderr in real-time.
        :param cmd: The command to execute, including arguments
        :return: The stdout of the command
        """

        _, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None)
        if stdout:
            self.logger.info("Command stdout:\n %s", stdout)
        return stdout

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    async def find_builds_all(self) -> Dict[str, List[str]]:
        """
        Run the elliott 'find-builds' command for images and return categorized build NVRs.

        This function executes the elliott find-builds command with --all-image-types flag
        to retrieve all types of image builds (payload, non-payload, OLM bundles) and
        categorizes them into separate lists.

        Returns:
            Dict[str, List[str]]: A dictionary containing four lists of NVRs:
                - 'image': NVRs of payload image builds
                - 'extras': NVRs of non-payload image builds
                - 'metadata': NVRs of OLM bundle builds
                - 'olm_builds_not_found': NVRs of OLM operator builds for which no bundle build was found
        """
        cmd = self._elliott_base_command + ["find-builds", "--kind=image", "--all-image-types", "--json=-"]
        stdout = await self.execute_command_with_logging(cmd)
        if not stdout:
            self.logger.warning("No output received from find-builds command.")
            return {"image": [], "extras": [], "metadata": [], "olm_builds_not_found": []}
        out = json.loads(stdout)
        kind_to_builds = {
            "image": out.get("payload", []),
            "extras": out.get("non_payload", []),
            "metadata": out.get("olm_builds", []),
            "olm_builds_not_found": out.get("olm_builds_not_found", []),
        }

        return kind_to_builds

    @a.functools.lru_cache
    async def find_bugs(
        self,
        build_system='konflux',
        permissive: bool | None = None,
        exclude_trackers: bool | None = None,
    ) -> Dict[str, List[str]]:
        """Find bugs for the current assembly.

        :param build_system: The build system to use (default: 'konflux').
        :param permissive: Whether to use permissive mode. None means use default behavior.
        :param exclude_trackers: Whether to exclude tracker bugs. None means use default behavior.
        :return: A dictionary mapping advisory kinds to lists of bug IDs
        """
        match build_system:
            case 'konflux':
                base_command = self._elliott_base_command
            case 'brew':
                base_command = self._elliott_base_command_for_brew
            case _:
                raise ValueError(f"Unsupported build system: {build_system}")
        if permissive is None:
            permissive = self.assembly_type in (AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE)
        if exclude_trackers is None:
            exclude_trackers = self.assembly_type in (AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE)
        find_bugs_cmd = base_command + [
            "find-bugs",
            "--output=json",
        ]
        if exclude_trackers:
            find_bugs_cmd.append("--exclude-trackers")
        if permissive:
            find_bugs_cmd.append("--permissive")
        stdout = await self.execute_command_with_logging(find_bugs_cmd)
        return json.loads(stdout)

    async def attach_cve_flaws(self, kind: str, shipment: ShipmentConfig):
        """Attach CVE flaws to the given shipment.
        :param kind: The shipment kind for which to attach CVE flaws
        :param shipment: The shipment to attach CVE flaws to
        """

        attach_cve_flaws_command = self._elliott_base_command + [
            'attach-cve-flaws',
            f'--use-default-advisory={kind}',
            '--reconcile',
            '--output=yaml',
        ]

        self.logger.info('Running elliott attach-cve-flaws for %s ...', kind)
        stdout = await self.execute_command_with_logging(attach_cve_flaws_command)
        if stdout:
            shipment.shipment.data.releaseNotes = ReleaseNotes(**Model(yaml.load(stdout)).primitive())

    async def create_shipment_mr(self, shipments_by_kind: Dict[str, ShipmentConfig], env: str) -> str:
        """Create a new shipment MR with the given shipment config files.
        :param shipments_by_kind: The shipment configurations to create the MR with, by advisory kind
        :param env: The environment for which the shipment is being prepared (prod or stage)
        :return: The URL of the created MR
        """

        self.logger.info("Creating shipment MR...")

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

        source_project = _get_project(self.shipment_data_repo_push_url)
        target_project = _get_project(self.shipment_data_repo_pull_url)

        # Include shipping date in MR title if available
        if self.release_date:
            mr_title = f"Draft: Shipment for {self.release_name} (ship date: {self.release_date})"
        else:
            mr_title = f"Draft: Shipment for {self.release_name}"
        mr_description = f"Created by job: {self.job_url}\n\n" if self.job_url else commit_message

        if self.dry_run:
            self.logger.info("[DRY-RUN] Would have created MR with title: %s", mr_title)
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
            self.logger.info("Created Draft Merge Request: %s", mr_url)

        # Store the MR URL for later use
        self.shipment_mr_url = mr_url
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

        self.logger.info("Updating shipment MR: %s", shipment_url)

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        project = self._gitlab.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)

        # Store the MR URL for later use
        self.shipment_mr_url = shipment_url

        # Ensure MR is set as draft if not already
        if not mr.title.startswith("Draft:"):
            mr.title = f"Draft: {mr.title}"
            if not self.dry_run:
                mr.save()
                self.logger.info("Set existing MR to draft status")

        # Checkout the MR branch
        source_branch = mr.source_branch
        await self.shipment_data_repo.fetch_switch_branch(source_branch, remote="origin")

        # Update shipment data
        commit_message = f"Update shipment configurations for {self.release_name}"
        updated = await self.update_shipment_data(shipments_by_kind, env, commit_message, source_branch)
        if not updated:
            self.logger.info("No changes in shipment data. MR will not be updated.")
            return False

        if self.dry_run:
            self.logger.info("[DRY-RUN] Would have updated MR description: %s", mr.description)
        else:
            mr.save()
            self.logger.info("Shipment MR updated: %s", shipment_url)

        return True

    async def set_shipment_mr_ready(self):
        """Mark the shipment MR as ready by removing the Draft prefix from the title.
        This should be called at the end of the pipeline when all work is complete.
        """
        if not self.shipment_mr_url:
            self.logger.info("No shipment MR URL stored, skipping setting to ready")
            return

        self.logger.info("Setting shipment MR to ready: %s", self.shipment_mr_url)

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(self.shipment_mr_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]

        # Load the existing MR
        project = self._gitlab.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)

        # Remove draft prefix from title
        if mr.title.startswith("Draft: "):
            mr.title = mr.title.removeprefix("Draft: ")
            if self.dry_run:
                self.logger.info("[DRY-RUN] Would have set MR to ready with title: %s", mr.title)
            else:
                mr.save()
                self.logger.info("Shipment MR marked as ready: %s", self.shipment_mr_url)
                await self._slack_client.say_in_thread(f"Shipment MR marked as ready: {self.shipment_mr_url}")

                # Trigger CI pipeline after marking as ready
                # wait for 30 seconds to ensure the MR is updated
                self.logger.info("Waiting for 30 seconds to ensure MR is updated...")
                await asyncio.sleep(30)
                await self.trigger_ci_pipeline(mr)
        else:
            self.logger.info("MR is already ready (no draft prefix found)")

    async def trigger_ci_pipeline(self, mr):
        """Trigger a GitLab Merge Request pipeline using the MR API.

        Args:
            mr: GitLab merge request object
        """
        try:
            self.logger.info(
                "Triggering CI pipeline for MR !%s on branch %s",
                mr.iid,
                mr.source_branch,
            )

            if self.dry_run:
                self.logger.info(
                    "[DRY-RUN] Would have triggered MR pipeline for MR !%s (branch: %s)",
                    mr.iid,
                    mr.source_branch,
                )
                return

            # Create a new pipeline for this merge request using MR API
            pipeline = mr.pipelines.create({'ref': mr.source_branch})

            pipeline_url = pipeline.web_url
            self.logger.info("CI MR pipeline triggered successfully: %s", pipeline_url)
            await self._slack_client.say_in_thread(f"CI pipeline triggered: {pipeline_url}")

        except Exception as ex:
            self.logger.warning(f"Failed to trigger CI MR pipeline for branch {mr.source_branch}: {ex}")
            await self._slack_client.say_in_thread(
                f"Failed to trigger CI pipeline for MR branch {mr.source_branch}: {ex}"
            )

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

        # Get the timestamp from the branch name
        # which we need for filenames
        # The branch name is expected to be in the format: prepare-shipment-<assembly>-<timestamp>
        timestamp = branch.split("-")[-1]

        for advisory_kind, shipment_config in shipments_by_kind.items():
            filename = f"{self.assembly}.{advisory_kind}.{timestamp}.yaml"
            product = shipment_config.shipment.metadata.product
            group = shipment_config.shipment.metadata.group
            application = shipment_config.shipment.metadata.application
            relative_target_dir = Path("shipment") / product / group / application / env
            target_dir = self.shipment_data_repo._directory / relative_target_dir
            target_dir.mkdir(parents=True, exist_ok=True)
            filepath = relative_target_dir / filename
            self.logger.info("Updating shipment file: %s", filename)
            shipment_dump = shipment_config.model_dump(exclude_unset=True, exclude_none=True)
            out = StringIO()
            yaml.dump(shipment_dump, out)
            await self.shipment_data_repo.write_file(filepath, out.getvalue())
        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        if self.job_url and self.job_url not in commit_message:
            commit_message += f"\n{self.job_url}"
        return await self.shipment_data_repo.commit_push(commit_message, safe=True)

    async def create_update_build_data_pr(self) -> bool:
        """Create or update a pull request in the build data repo with the updated assembly config.
        :return: True if the PR was created or updated successfully, False otherwise.
        """

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        branch = f"update-assembly-{self.release_name}-{timestamp}"
        updated = await self.update_build_data(branch)
        if not updated:
            self.logger.info("No changes in assembly config. PR will not be created or updated.")
            return False

        target_repo = self.build_data_repo_pull_url.split('/')[-1].replace('.git', '')
        source_owner = self.build_data_push_url.split('/')[-2]
        target_owner = self.build_data_repo_pull_url.split('/')[-2]

        head = f"{source_owner}:{branch}"
        base = self.build_data_gitref or self.group
        api = GhApi(owner=target_owner, repo=target_repo, token=self.github_token)
        existing_prs = api.pulls.list(
            state="open",
            head=head,
            base=base,
        )

        if not existing_prs.items:
            pr_title = f"Update assembly {self.assembly}"
            pr_body = f"This PR updates {self.assembly} assembly definition."
            if self.job_url:
                pr_body += f"\n\nCreated by job: {self.job_url}"

            if self.dry_run:
                self.logger.info("[DRY-RUN] Would have created a new PR with title '%s'", pr_title)
                return True

            result = api.pulls.create(
                head=head,
                base=base,
                title=pr_title,
                body=pr_body,
                maintainer_can_modify=True,
            )
            self.logger.info("PR to update assembly definition created: %s", result.html_url)
            await self._slack_client.say_in_thread(f"PR to update assembly definition: {result.html_url}")
            pull_number = result.number
        else:
            self.logger.info("Existing PR to update assembly found: %s", existing_prs.items[0].html_url)
            pull_number = existing_prs.items[0].number

            if self.dry_run:
                self.logger.info("[DRY-RUN] Would have updated PR with number %s", pull_number)
                return True

            pr_body = existing_prs.items[0].body
            if self.job_url:
                pr_body += f"\n\nUpdated by job: {self.job_url}"
            result = api.pulls.update(
                pull_number=pull_number,
                body=pr_body,
            )
            self.logger.info("PR to update assembly updated: %s", result.html_url)
            await self._slack_client.say_in_thread(f"PR to update assembly updated: {result.html_url}")

        # Check if we can auto-merge
        if source_owner == target_owner:
            self.logger.info(
                "Push and pull URLs are from same project (%s), attempting to auto-merge PR...", source_owner
            )
            if self.dry_run:
                self.logger.info("[DRY-RUN] Would have auto-merged PR with number %s", pull_number)
            else:
                try:
                    # Auto-merge the PR
                    api.pulls.merge(pull_number)
                    self.logger.info("PR to update assembly was auto-merged: %s", result.html_url)
                    await self._slack_client.say_in_thread(f":white_check_mark: PR auto-merged: {result.html_url}")
                except Exception as ex:
                    self.logger.warning(f"Failed to auto-merge PR {pull_number}: {ex}")
                    await self._slack_client.say_in_thread(
                        f"Failed to auto-merge PR, will wait for manual merge: {result.html_url}"
                    )
                    # Fall back to waiting for manual merge
                    await self._wait_for_pr_merge(api, pull_number, result.html_url)
        else:
            self.logger.info(
                "Push owner (%s) differs from pull owner (%s), waiting for manual merge...", source_owner, target_owner
            )
            await self._slack_client.say_in_thread(f"Waiting for PR to update assembly to be merged: {result.html_url}")
            await self._wait_for_pr_merge(api, pull_number, result.html_url)

        return True

    async def _wait_for_pr_merge(self, api, pull_number: int, pr_url: str):
        """Wait for a PR to be merged manually with timeout."""
        # wait until the PR is merged
        timeout = 60 * 60  # 1 hour
        start_time = time.time()
        while True:
            if (time.time() - start_time) > timeout:
                raise TimeoutError(f"Timeout waiting for PR to update assembly to be merged: {pr_url}")
            await asyncio.sleep(10)
            pr = api.pulls.get(pull_number)
            if pr.merged:
                self.logger.info("PR to update assembly was merged: %s", pr_url)
                break
            if pr.state == "closed":
                self.logger.info("PR to update assembly was closed: %s", pr_url)
                raise RuntimeError(f"PR to update assembly was closed: {pr_url}")
            self.logger.info("Waiting for PR to update assembly to be merged: %s", pr_url)

    async def update_build_data(self, branch: str) -> bool:
        """Update releases.yml in build data repo with the given shipment assembly config.
        Commits the changes and push to the remote repo.
        :param branch: The branch to update in the build data repo
        :return: True if the changes were committed and pushed successfully, False otherwise.
        """
        if await self.build_data_repo.does_branch_exist_on_remote(branch, remote="origin"):
            self.logger.info('Fetching and switching to existing branch %s', branch)
            await self.build_data_repo.fetch_switch_branch(branch, remote="origin")
        else:
            self.logger.info('Creating new branch %s', branch)
            await self.build_data_repo.create_branch(branch)

        new_releases_config = self.releases_config.copy()
        new_releases_config["releases"][self.assembly]["assembly"]["group"] = (
            self.updated_assembly_group_config.primitive()
        )

        out = StringIO()
        yaml.dump(new_releases_config, out)
        await self.build_data_repo.write_file("releases.yml", out.getvalue())
        await self.build_data_repo.add_all()
        await self.build_data_repo.log_diff()

        commit_message = f"Update shipment for assembly {self.assembly}"
        return await self.build_data_repo.commit_push(commit_message, safe=True)

    async def verify_payload(self):
        """
        Verify that the swept builds match the imagestreams that were updated during build-sync.
        """

        @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
        async def _verify(imagestream):
            # Create base path if it does not exist
            base_path = self.elliott_working_dir / 'verify_payload' / imagestream
            base_path.mkdir(parents=True, exist_ok=True)

            self.logger.info("Verifying payload against imagestream %s", imagestream)
            with TemporaryDirectory(prefix=str(base_path)) as elliott_working:
                # Replace the elliott working dir to allow concurrent commands execution
                verify_payload_command = [
                    arg if not arg.startswith('--working-dir=') else f'--working-dir={elliott_working}'
                    for arg in self._elliott_base_command
                ]
                verify_payload_command += ['verify-payload', imagestream]

                if self.dry_run:
                    self.logger.info("[DRY-RUN] Would have run command: %s", ' '.join(verify_payload_command))
                    return

                _, stdout, _ = await exectools.cmd_gather_async(verify_payload_command)
                results = json.loads(stdout)

                self.logger.info("Summary results for %s:\n%s", imagestream, json.dumps(results, indent=4))
                if results.get("missing_in_advisory") or results.get("payload_advisory_mismatch"):
                    raise ValueError(f"""Failed to verify payload for nightly {imagestream}.
                                Please fix advisories and nightlies to match each other, manually verify them with `elliott verify-payload`,
                                update JIRA accordingly, then notify QE and multi-arch QE for testing.""")
                self.logger.info("Payload verification succeeded for imagestream %s", imagestream)

                # Move debug.log to elliott working directory to allow Jenkins archive it
                debug_log_path = Path(self.elliott_working_dir) / 'verify-payload'  # destination dir
                debug_log_path.mkdir(parents=True, exist_ok=True)
                shutil.move(
                    f'{elliott_working}/debug.log',
                    f'{debug_log_path}/verify-payload-{imagestream.split("/")[-1]}-debug.log',
                )

        major, minor = self.release_name.split('.')[:2]
        image_stream_version = f'{major}.{minor}'

        assembly_is_base_name = assembly_imagestream_base_name_generic(
            image_stream_version, self.assembly, self.assembly_type, build_system='konflux'
        )
        arches = self.group_config.get("arches", [])
        imagestreams_per_arch = [
            payload_imagestream_namespace_and_name(
                default_imagestream_namespace_base_name(), assembly_is_base_name, arch, private=False
            )
            for arch in arches
        ]
        await asyncio.gather(
            *[
                _verify(f"{is_namespace_and_name[0]}/{is_namespace_and_name[1]}")
                for is_namespace_and_name in imagestreams_per_arch
            ]
        )

    async def handle_jira_ticket(self):
        template_issue_key = self.runtime.config["jira"]["templates"]["ocp4-konflux"]
        template_issue = self.jira_client.get_issue(template_issue_key)

        self.release_version = semver.VersionInfo.parse(self.release_name).to_tuple()
        jira_issue_key = self.assembly_group_config.release_jira
        jira_template_vars = self.get_jira_template_vars()

        if jira_issue_key and jira_issue_key != "ART-0":
            self.logger.info("Reusing existing release JIRA %s", jira_issue_key)
            jira_issue = self.jira_client.get_issue(jira_issue_key)
            self.update_release_jira(jira_issue, template_issue, jira_template_vars)
        else:
            self.logger.info("Creating a release JIRA...")
            jira_issue = self.create_release_jira(template_issue, jira_template_vars)
            if jira_issue and jira_issue.key:
                self.logger.info("Release JIRA created: %s", jira_issue.permalink())
                self.updated_assembly_group_config.release_jira = jira_issue.key
                await self.create_update_build_data_pr()

    def get_jira_template_vars(self):
        nightlies = get_assembly_basis(self.releases_config, self.assembly).get("reference_releases", {}).values()
        candidate_nightlies = nightlies_with_pullspecs(nightlies)

        return {
            "release_name": self.release_name,
            "x": self.release_version[0],
            "y": self.release_version[1],
            "z": self.release_version[2],
            "release_date": self.release_date,
            "rhcos_advisory": self.updated_assembly_group_config.advisories['rhcos'],
            "rpm_advisory": self.updated_assembly_group_config.advisories['rpm'],
            "shipment_url": self.updated_assembly_group_config.shipment.url,
            "candidate_nightlies": candidate_nightlies,
        }

    def create_release_jira(self, template_issue: Issue, template_vars: Dict) -> Optional[Issue]:
        def fields_transform(fields):
            labels = set(fields.get("labels", []))
            # change summary title for security
            if "template" not in labels:
                return fields  # no need to modify fields of non-template issue
            # remove "template" label
            fields["labels"] = list(labels - {"template"})
            return self.jira_client.render_jira_template(fields, template_vars)

        if self.dry_run:
            jira_fields = fields_transform(template_issue.raw["fields"].copy())
            self.logger.warning("[DRY RUN] Would have created release JIRA: %s", jira_fields["summary"])
            return None

        self.logger.info("Creating release JIRA from template %s...", template_issue.key)
        new_issue = self.jira_client.clone_issue(template_issue, fields_transform=fields_transform)
        self.logger.info("Created release JIRA: %s", new_issue.permalink())

        jira_issue_key = new_issue.key if new_issue else None
        if jira_issue_key:
            if not self.runtime.dry_run:
                self.logger.info("Updating Jira ticket status...")
                self.jira_client.start_task(new_issue)

            else:
                self.logger.warning("[DRY RUN] Would have updated Jira ticket status")

        return new_issue

    def update_release_jira(self, issue: Issue, template_issue: Issue, template_vars: Dict[str, int]) -> bool:
        self.logger.info("Updating release JIRA %s from template %s...", issue.key, template_issue.key)
        old_fields = {
            "summary": issue.fields.summary,
            "description": issue.fields.description.strip(),
        }
        fields = {
            "summary": template_issue.fields.summary,
            "description": template_issue.fields.description,
        }
        if "template" in template_issue.fields.labels:
            fields = self.jira_client.render_jira_template(fields, template_vars)

        jira_changed = fields != old_fields
        if jira_changed:
            if not self.dry_run:
                issue.update(fields)
            else:
                self.logger.warning("Would have updated JIRA ticket %s with summary %s", issue.key, fields["summary"])

        else:
            self.logger.info('Jira unchanged, not updating issue')

        return jira_changed


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
    '--build-data-repo-url',
    help='ocp-build-data repo to use. Defaults to group branch - to use a different branch/commit use repo@branch',
)
@click.option(
    "--inject-build-data-repo",
    is_flag=True,
    help="Inject build-data repo/commit given by --build-data-repo-url into the shipment config",
)
@click.option(
    '--shipment-data-repo-url',
    help='shipment-data repo to use for reading and as shipment MR target. Defaults to main branch. Should reside in gitlab.cee.redhat.com',
)
@pass_runtime
@click_coroutine
async def prepare_release(
    runtime: Runtime,
    group: str,
    assembly: str,
    build_data_repo_url: Optional[str],
    inject_build_data_repo: bool,
    shipment_data_repo_url: Optional[str],
):
    # Check if assembly is valid
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
            build_data_repo_url=build_data_repo_url,
            shipment_data_repo_url=shipment_data_repo_url,
            inject_build_data_repo=inject_build_data_repo,
        )
        await pipeline.run()
        await slack_client.say_in_thread(f":white_check_mark: prepare-release-konflux for {assembly} completes.")

    except Exception as e:
        await slack_client.say_in_thread(f":warning: prepare-release-konflux for {assembly} has result FAILURE.")
        raise e
