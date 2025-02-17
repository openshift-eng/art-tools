import asyncio
from functools import cached_property
import json
import logging
import os
import re
import shutil
import subprocess
import aiofiles
import click
import jinja2
import semver
from io import StringIO
from pathlib import Path
from subprocess import PIPE
from typing import Dict, List, Optional, Sequence, Tuple
from jira.resources import Issue
from tenacity import retry, stop_after_attempt, wait_fixed
from datetime import datetime, timedelta, timezone

from artcommonlib.assembly import AssemblyTypes, assembly_group_config
from artcommonlib.model import Model
from artcommonlib.util import get_assembly_release_date, new_roundtrip_yaml_handler
from doozerlib.cli.release_gen_payload import assembly_imagestream_base_name_generic, default_imagestream_namespace_base_name, payload_imagestream_namespace_and_name
from elliottlib.errata import set_blocking_advisory, get_blocking_advisories, push_cdn_stage, is_advisory_editable
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.errata import set_blocking_advisory, get_blocking_advisories
from artcommonlib import exectools
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.jira import JIRAClient
from pyartcd.slack import SlackClient
from pyartcd.record import parse_record_log
from pyartcd.runtime import Runtime
from pyartcd.util import (get_assembly_basis, get_assembly_type,
                          get_release_name_for_assembly,
                          is_greenwave_all_pass_on_advisory,
                          nightlies_with_pullspecs)


_LOGGER = logging.getLogger(__name__)
yaml = new_roundtrip_yaml_handler()


class PrepareReleasePipeline:
    def __init__(
        self,
        slack_client: SlackClient,
        runtime: Runtime,
        group: Optional[str],
        assembly: Optional[str],
        name: Optional[str],
        date: str,
        nightlies: List[str],
        package_owner: str,
        jira_token: str,
        default_advisories: bool = False,
        include_shipped: bool = False,
        skip_batch: bool = False,
    ) -> None:
        _LOGGER.info("Initializing and verifying parameters...")
        self.runtime = runtime
        self.assembly = assembly or "stream"
        self.release_name = None
        group_match = None
        if group:
            group_match = re.fullmatch(r"openshift-(\d+).(\d+)", group)
            if not group_match:
                raise ValueError(f"Invalid group name: {group}")
        self.group_name = group
        self.candidate_nightlies = {}
        if self.assembly == "stream":
            if not name:
                raise ValueError("Release name is required to prepare a release from stream assembly.")
            self.release_name = name
            self.release_version = tuple(map(int, name.split(".", 2)))
            if group and group != f"openshift-{self.release_version[0]}.{self.release_version[1]}":
                raise ValueError(f"Group name {group} doesn't match release name {name}")
            if self.release_version[0] < 4 and nightlies:
                raise ValueError("No nightly needed for OCP3 releases")
            if self.release_version[0] >= 4 and not nightlies:
                raise ValueError("You need to specify at least one nightly.")
            self.candidate_nightlies = nightlies_with_pullspecs(nightlies)
        else:
            if name:
                raise ValueError("Release name cannot be set for a non-stream assembly.")
            if nightlies:
                raise ValueError("Nightlies cannot be specified in job parameter for a non-stream assembly.")
            if not group_match:
                raise ValueError("A valid group is required to prepare a release for a non-stream assembly.")
            self.release_version = (int(group_match[1]), int(group_match[2]), 0)
            if default_advisories:
                raise ValueError("default_advisories cannot be set for a non-stream assembly.")

        self.release_date = date or get_assembly_release_date(assembly, group)
        self.package_owner = package_owner or self.runtime.config["advisory"]["package_owner"]
        self._slack_client = slack_client
        self.working_dir = self.runtime.working_dir.absolute()
        self.default_advisories = default_advisories
        self.include_shipped = include_shipped
        self.skip_batch = skip_batch

        self.dry_run = self.runtime.dry_run
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self.doozer_working_dir = self.working_dir / "doozer-working"
        self._jira_client = JIRAClient.from_url(self.runtime.config["jira"]["url"], token_auth=jira_token)
        # sets environment variables for Elliott and Doozer
        self._ocp_build_data_url = self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self.doozer_working_dir)
        self._elliott_env_vars = os.environ.copy()
        self._elliott_env_vars["ELLIOTT_WORKING_DIR"] = str(self.elliott_working_dir)
        if self._ocp_build_data_url:
            self._elliott_env_vars["ELLIOTT_DATA_PATH"] = self._ocp_build_data_url
            self._doozer_env_vars["DOOZER_DATA_PATH"] = self._ocp_build_data_url

        # This will be set to True if advance operator advisory is detected
        self.advance_release = False
        # This will be set to True if operator pre-release advisory is detected
        self.pre_release = False

    async def run(self):
        self.working_dir.mkdir(parents=True, exist_ok=True)
        build_data_repo = self.working_dir / "ocp-build-data-push"
        shutil.rmtree(build_data_repo, ignore_errors=True)
        shutil.rmtree(self.elliott_working_dir, ignore_errors=True)
        shutil.rmtree(self.doozer_working_dir, ignore_errors=True)

        group_config = await self.load_group_config()
        releases_config = await self.load_releases_config()
        assembly_type = get_assembly_type(releases_config, self.assembly)

        if assembly_type == AssemblyTypes.STREAM:
            if self.release_version[0] >= 4:
                raise ValueError("Preparing a release from a stream assembly for OCP4+ is no longer supported.")

        release_config = releases_config.get("releases", {}).get(self.assembly, {})
        self.release_name = get_release_name_for_assembly(self.group_name, releases_config, self.assembly)
        self.release_version = semver.VersionInfo.parse(self.release_name).to_tuple()
        if not release_config:
            raise ValueError(f"Assembly {self.assembly} is not explicitly defined in releases.yml for group {self.group_name}.")
        group_config = assembly_group_config(Model(releases_config), self.assembly, Model(group_config)).primitive()
        nightlies = get_assembly_basis(releases_config, self.assembly).get("reference_releases", {}).values()
        self.candidate_nightlies = nightlies_with_pullspecs(nightlies)

        if release_config and assembly_type != AssemblyTypes.STANDARD:
            _LOGGER.warning("No need to check Blocker Bugs for assembly %s", self.assembly)
        else:
            _LOGGER.info("Checking Blocker Bugs for release %s...", self.release_name)
            self.check_blockers()

        # advisories is a dictionary with keys as the impetus and values as the advisory number.
        # If the value is -1, the advisory needs to be created.
        advisories = {}
        batch = None

        if self.default_advisories:
            advisories = group_config.get("advisories", {})
        else:
            if release_config:
                advisories = group_config.get("advisories", {}).copy()

            if not self.skip_batch and assembly_type is AssemblyTypes.STANDARD:
                # Create a batch if it doesn't exist
                batch_name = f"OCP {self.release_name}"
                errata_config = await self.load_errata_config()
                batch = await self._ensure_batch(self._errata_api, errata_config['release'], batch_name, self.release_date, dry_run=self.dry_run)

            is_ga = self.release_version[2] == 0
            advisory_type = "RHEA" if is_ga else "RHBA"
            for ad in advisories:
                if advisories[ad] >= 0:
                    continue
                if ad in ["advance", "prerelease"]:
                    release_date = None
                    if ad == "advance":
                        # Set release date of advance advisory to one week before GA
                        # Eg one week before '2024-Feb-07' should be '2024-Jan-31'
                        ga_date = datetime.strptime(self.release_date, "%Y-%b-%d")
                        one_week_before = ga_date - timedelta(days=7)
                        release_date = one_week_before.strftime("%Y-%b-%d")
                    elif ad == "prerelease":
                        # Set release date of prerelease advisory to 3 days after when we prepare the release
                        # it should not be a weekend
                        today = datetime.now(tz=timezone.utc)
                        release_date = today + timedelta(days=3)
                        if release_date.weekday() == 5:  # Saturday
                            release_date += timedelta(days=2)
                        elif release_date.weekday() == 6:  # Sunday
                            release_date += timedelta(days=1)
                        release_date = release_date.strftime("%Y-%b-%d")
                    advisories[ad] = self.create_advisory(advisory_type=advisory_type,
                                                          art_advisory_key=ad,
                                                          release_date=release_date)
                    await self._slack_client.say_in_thread(
                        f"{ad} advisory created with release date {release_date}")
                    continue
                batch_id = 0
                if batch and ad != "microshift":
                    # Set batch_id for advisories other than microshift
                    batch_id = int(batch["id"])
                    # Ensure that the batch is unlocked
                    batch = await self._ensure_batch_status(self._errata_api, batch, lock=False, dry_run=self.dry_run)
                advisories[ad] = self.create_advisory(advisory_type=advisory_type,
                                                      art_advisory_key=ad,
                                                      release_date=self.release_date,
                                                      batch_id=batch_id)
            await self._slack_client.say_in_thread(f"Regular advisories created with release date {self.release_date}")

        jira_issue_key = group_config.get("release_jira")
        jira_issue = None
        jira_template_vars = {
            "release_name": self.release_name,
            "x": self.release_version[0],
            "y": self.release_version[1],
            "z": self.release_version[2],
            "release_date": self.release_date,
            "advisories": advisories,
            "candidate_nightlies": self.candidate_nightlies,
        }
        if jira_issue_key and jira_issue_key != "ART-0":
            _LOGGER.info("Reusing existing release JIRA %s", jira_issue_key)
            jira_issue = self._jira_client.get_issue(jira_issue_key)
            subtasks = [self._jira_client.get_issue(subtask.key) for subtask in jira_issue.fields.subtasks]
            self.update_release_jira(jira_issue, subtasks, jira_template_vars)
        else:
            _LOGGER.info("Creating a release JIRA...")
            jira_issues = self.create_release_jira(jira_template_vars)
            jira_issue = jira_issues[0] if jira_issues else None
            jira_issue_key = jira_issue.key if jira_issue else None
            subtasks = jira_issues[1:] if jira_issues else []

        if jira_issue_key:
            _LOGGER.info("Updating Jira ticket status...")
            if not self.runtime.dry_run:
                subtask = subtasks[1]
                self._jira_client.add_comment(
                    subtask,
                    "prepare release job : {}".format(os.environ.get("BUILD_URL"))
                )
                self._jira_client.assign_to_me(subtask)
                self._jira_client.close_task(subtask)
                self._jira_client.start_task(jira_issue)
            else:
                _LOGGER.warning("[DRY RUN] Would have updated Jira ticket status")

        _LOGGER.info("Updating ocp-build-data...")
        await self.update_build_data(advisories, jira_issue_key)

        # Set advisory dependencies
        await self.set_advisory_dependencies(advisories)

        if batch:
            # Update batch with advisories
            advisories_for_main_batch = [ad for ad_type, ad in advisories.items() if ad > 0 and ad_type not in {'advance', 'microshift'}]
            await self._ensure_batch_advisories(self._errata_api, batch, advisories_for_main_batch, self.dry_run)

        if "advance" in advisories.keys():
            # Make sure that the advisory is in editable mode
            if is_advisory_editable(advisories["advance"]):
                # Set this as an 'advance' release
                self.advance_release = True

                # Remove all builds from the metadata advisory
                await self.remove_builds_all(advisories["metadata"])
            else:
                _LOGGER.info(f"'advance' advisory {advisories['advance']} is not editable. Defaulting bundle advisory"
                             " to 'metadata'")

        if "prerelease" in advisories.keys():
            if self.advance_release:
                raise IOError("prerelease and advance release cannot be set at the same time")

            # Make sure that the advisory is in editable mode
            if is_advisory_editable(advisories["prerelease"]):
                # Set this as an 'pre-release' release
                self.pre_release = True

                # Remove all builds from the metadata advisory
                await self.remove_builds_all(advisories["metadata"])
            else:
                _LOGGER.info(f"'prerelease' advisory {advisories['prerelease']} is not editable. Defaulting bundle "
                             "advisory to 'metadata'")

        _LOGGER.info("Sweep builds into the the advisories...")
        for impetus, advisory in advisories.items():
            if not advisory:
                continue
            elif impetus == "rpm" and "prerelease" in advisories.keys():
                # Skip populating RPM advisory if prerelease is detected, for ECs and RCs
                _LOGGER.info("Skipping populating rpm advisory, since prerelease detected")
                continue
            elif impetus == "metadata":
                # Do not populate the metadata advisory if advance advisory is present
                if self.advance_release or self.pre_release:
                    _LOGGER.info("Skipping populating metadata advisory since advance/pre-release detected")
                    continue
                await self.build_and_attach_bundles(advisory)
            elif impetus in ["advance", "prerelease"]:
                await self.build_and_attach_bundles(advisory)
            else:
                # Skip populating microshift advisory since that is done later after promote
                if impetus == "microshift":
                    continue
                await self.sweep_builds_async(impetus, advisory)

        # Verify attached operators - and gather builds if needed
        if any(x in advisories for x in ("metadata", "prerelease", "advance")):
            gather_dependencies = self.advance_release or self.pre_release
            advisory_args = []
            if self.advance_release:
                advisory_args = [advisories['advance']]
            elif self.pre_release:
                advisory_args = [advisories['prerelease']]
            elif 'metadata' in advisories:
                advisory_args = [advisories["metadata"], advisories["extras"], advisories["image"]]

            try:
                await self.verify_attached_operators(*advisory_args, gather_dependencies=gather_dependencies)
            except Exception as ex:
                _LOGGER.error(f"Unable to verify attached operators: {ex}")
                message = "`elliott verify-attached-operators` failed, details in log."
                if self.advance_release:
                    message += " Could not prepare advance advisory for release."
                elif self.pre_release:
                    message += " Could not prepare prerelease advisory for release."
                await self._slack_client.say_in_thread(message, reaction="art-attention")
                raise ex

        # bugs should be attached after builds to validate tracker bugs against builds
        _LOGGER.info("Sweep bugs into the the advisories...")

        if self.advance_release:
            self.sweep_bugs(permissive=False, advance_release=True)
            for _, advisory in advisories.items():
                self.attach_cve_flaws(advisory)
        else:
            if assembly_type in (AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE):
                self.sweep_bugs(permissive=True)
                _LOGGER.info("Skipping flaw processing during pre-releases")
            else:
                self.sweep_bugs(permissive=False)
                _LOGGER.info("Processing attached Security Trackers")
                for _, advisory in advisories.items():
                    self.attach_cve_flaws(advisory)

        # Verify the swept builds match the nightlies
        if self.release_version[0] < 4:
            _LOGGER.info("Don't verify payloads for OCP3 releases")
        else:
            _LOGGER.info("Verify the swept builds match the imagestreams that were updated during build-sync...")
            self.image_stream_x_value = self.release_version[0]
            self.image_stream_y_value = self.release_version[1]
            self.image_stream_version = f'{self.image_stream_x_value}.{self.image_stream_y_value}'

            assembly_is_base_name = assembly_imagestream_base_name_generic(self.image_stream_version, self.assembly, assembly_type)
            arches = group_config.get("arches", [])
            imagestreams_per_arch = [
                payload_imagestream_namespace_and_name(default_imagestream_namespace_base_name(),
                                                       assembly_is_base_name, arch, private=False)
                for arch in arches
            ]
            for is_namespace_and_name in imagestreams_per_arch:
                is_str = f"{is_namespace_and_name[0]}/{is_namespace_and_name[1]}"
                self.verify_payload(is_str, advisories["image"])

        # Verify greenwave tests
        for impetus, advisory in advisories.items():
            if impetus in ("image", "extras", "metadata", "prerelease", "advance"):
                if not is_greenwave_all_pass_on_advisory(advisory):
                    await self._slack_client.say_in_thread(
                        "Some greenwave tests failed on "
                        f"https://errata.devel.redhat.com/advisory/{advisory}/test_run/greenwave_cvp @release-artists")

        # Move advisories to QE
        for impetus, advisory in advisories.items():
            # microshift advisory is special, and it will not be ready at this time
            if impetus == 'microshift':
                continue
            if impetus == 'metadata' and (self.advance_release or self.pre_release):
                # We don't need to move emtpy metadata advisory if it's an advance release
                _LOGGER.info("Not moving metadata advisory to QE since prerelease/advance release detected")
                continue
            try:
                await self.change_advisory_state_qe(advisory)
            except Exception as ex:
                _LOGGER.warning(f"Unable to move {impetus} advisory {advisory} to QE: {ex}")
                await self._slack_client.say_in_thread(f"Unable to move {impetus} advisory {advisory} to QE. Details in log.")
                continue
            try:
                push_cdn_stage(advisory)
            except Exception as ex:
                _LOGGER.warning(f"Unable to trigger push {impetus} advisory {advisory} to CDN stage: {ex}")
                await self._slack_client.say_in_thread(f"Unable to trigger push {impetus} advisory {advisory} to CDN stage. Details in log.")

    async def load_releases_config(self) -> Optional[None]:
        repo = self.working_dir / "ocp-build-data-push"
        if not repo.exists():
            await self.clone_build_data(repo)
        path = repo / "releases.yml"
        if not path.exists():
            return None
        async with aiofiles.open(path, "r") as f:
            content = await f.read()
        return yaml.load(content)

    @cached_property
    def _errata_api(self):
        return AsyncErrataAPI()

    @staticmethod
    async def _ensure_batch(errata_api: AsyncErrataAPI, release_name: str,
                            batch_name: str, release_date: str, dry_run: bool = False):
        """ Ensure that the batch exists and has the correct release date

        :param errata_api: Errata API object
        :param batch_name: Name of the batch
        :param release_date: Release date
        :return: Batch object
        """
        # Get batch from Errata
        if dry_run:
            _LOGGER.warning("[DRY RUN] Would have created batch %s", batch_name)
            return {"id": -1}
        _LOGGER.info("Checking if batch '%s' exists...", batch_name)
        batches = [b async for b in errata_api.get_batches(name=batch_name)]
        if not batches:
            _LOGGER.info("Creating batch '%s'...", batch_name)
            batch = await errata_api.create_batch(name=batch_name,
                                                  release_name=release_name,
                                                  release_date=release_date,
                                                  description=batch_name)
            _LOGGER.info("Created errata batch id %s", int(batch["id"]))
        else:
            batch = batches[0]
            _LOGGER.info("Found batch id %s", int(batch["id"]))
            # Update batch release date if needed
            batch_date = datetime.strptime(batch["attributes"]["release_date"], "%Y-%m-%d")
            expected_date = datetime.strptime(release_date, "%Y-%b-%d")
            if batch_date != expected_date:
                _LOGGER.info("Updating batch release date to %s...", expected_date)
                await errata_api.update_batch(batch["id"], release_date=release_date)
                _LOGGER.info("Batch release date updated")
        return batch

    @staticmethod
    async def _ensure_batch_status(errata_api: AsyncErrataAPI, batch: Dict, lock: bool, dry_run: bool = False):
        """ Ensure that the batch is locked or unlocked

        :param errata_api: Errata API object
        :param batch: Batch object
        :param lock: Lock status
        :param dry_run: Dry run flag
        :return: Batch object
        """
        locked = bool(batch.get("attributes", {}).get("is_locked", False))
        if locked == lock:
            return batch
        if locked:
            _LOGGER.info(f"Batch {batch['id']} is locked. Unlocking it...")
        else:
            _LOGGER.info(f"Batch {batch['id']} is unlocked. Locking it...")
        if dry_run:
            if lock:
                _LOGGER.warning("[DRY RUN] Would have locked batch %s", batch["id"])
            else:
                _LOGGER.warning("[DRY RUN] Would have unlocked batch %s", batch["id"])
            return
        batch = await errata_api.update_batch(batch["id"], is_locked=lock)
        if lock:
            _LOGGER.info(f"Batch {batch['id']} locked")
        else:
            _LOGGER.info(f"Batch {batch['id']} unlocked")
        return batch

    @staticmethod
    async def _ensure_batch_advisories(errata_api: AsyncErrataAPI, batch: Dict, advisories: Sequence[int], dry_run: bool = False):
        """
        Ensure that the batch contains the correct advisories and is locked.
        This will remove any advisories that are not in the list and add any that are missing.

        :param errata_api: Errata API object
        :param batch: Batch object
        :param advisories: List of advisory IDs
        :param dry_run: Dry run flag
        """
        current_advisories_in_batch = {int(ad["id"]) for ad in batch.get("relationships", {}).get("errata", [])}
        expected_advisories_in_batch = set(advisories)
        if current_advisories_in_batch != expected_advisories_in_batch:
            # Unlock batch if the batch is currently locked
            await PrepareReleasePipeline._ensure_batch_status(errata_api, batch, lock=False, dry_run=dry_run)
            # Remove advisories that should not be in the batch
            api_calls = []
            advisories_to_remove = current_advisories_in_batch - expected_advisories_in_batch
            for ad in advisories_to_remove:
                api_calls.append(errata_api.change_batch_for_advisory(ad, None))
            # Add advisories that are not in the batch
            advisories_to_add = expected_advisories_in_batch - current_advisories_in_batch
            for ad in advisories_to_add:
                api_calls.append(errata_api.change_batch_for_advisory(ad, batch["id"]))
            # Execute all API calls
            if not dry_run:
                _LOGGER.info(f"Removing advisories {advisories_to_remove} from batch {batch['id']} and adding advisories {advisories_to_add} to batch {batch['id']}")
                await asyncio.gather(*api_calls)
                _LOGGER.info(f"Advisories updated in batch {batch['id']}")
            else:
                _LOGGER.warning("[DRY RUN] Would have removed advisories %s from batch %s and added advisories %s to batch %s",
                                advisories_to_remove, batch["id"], advisories_to_add, batch["id"])
        # Lock batch if it was unlocked
        await PrepareReleasePipeline._ensure_batch_status(errata_api, batch, lock=True, dry_run=dry_run)

    async def load_group_config(self) -> Dict:
        repo = self.working_dir / "ocp-build-data-push"
        if not repo.exists():
            await self.clone_build_data(repo)
        async with aiofiles.open(repo / "group.yml", "r") as f:
            content = await f.read()
        return yaml.load(content)

    async def load_errata_config(self) -> Dict:
        repo = self.working_dir / "ocp-build-data-push"
        if not repo.exists():
            await self.clone_build_data(repo)
        async with aiofiles.open(repo / "erratatool.yml", "r") as f:
            content = await f.read()
        return yaml.load(content)

    def check_blockers(self):
        # Note: --assembly option should always be "stream". We are checking blocker bugs for this release branch regardless of the sweep cutoff timestamp.
        cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "--assembly=stream",
            "find-bugs:blocker",
            "--exclude-status=ON_QA",
        ]
        if self.runtime.dry_run:
            _LOGGER.warning("[DRY RUN] Would have run %s", cmd)
            return
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        result = subprocess.run(cmd, stdout=PIPE, stderr=PIPE, check=False, universal_newlines=True, cwd=self.working_dir)
        if result.returncode != 0:
            raise IOError(
                f"Command {cmd} returned {result.returncode}: stdout={result.stdout}, stderr={result.stderr}"
            )
        _LOGGER.info(result.stdout)
        match = re.search(r"Found ([0-9]+) bugs", str(result.stdout))
        if match and int(match[1]) != 0:
            _LOGGER.info(f"{int(match[1])} Blocker Bugs found! Make sure to resolve these blocker bugs before proceeding to promote the release.")

    def create_advisory(self, advisory_type: str, art_advisory_key: str, release_date: str, batch_id: int = 0) -> int:
        _LOGGER.info("Creating advisory with type %s art_advisory_key %s ...", advisory_type, art_advisory_key)
        create_cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
            "create",
            f"--type={advisory_type}",
            f"--art-advisory-key={art_advisory_key}",
            f"--assigned-to={self.runtime.config['advisory']['assigned_to']}",
            f"--manager={self.runtime.config['advisory']['manager']}",
            f"--package-owner={self.package_owner}",
        ]
        if batch_id:
            create_cmd.append(f"--batch-id={batch_id}")
        else:
            create_cmd.append(f"--date={release_date}")
        if not self.dry_run:
            create_cmd.append("--yes")
        _LOGGER.info("Running command: %s", ' '.join(create_cmd))
        result = subprocess.run(create_cmd, check=False, stdout=PIPE, stderr=PIPE, universal_newlines=True, cwd=self.working_dir)
        if result.returncode != 0:
            raise IOError(
                f"Command {create_cmd} returned {result.returncode}: stdout={result.stdout}, stderr={result.stderr}"
            )
        match = re.search(
            r"https:\/\/errata\.devel\.redhat\.com\/advisory\/([0-9]+)", result.stdout
        )
        advisory_num = int(match[1])
        _LOGGER.info("Created %s advisory %s", art_advisory_key, advisory_num)
        return advisory_num

    async def clone_build_data(self, local_path: Path):
        shutil.rmtree(local_path, ignore_errors=True)
        # shallow clone ocp-build-data
        cmd = [
            "git",
            "-C",
            str(self.working_dir),
            "clone",
            "-b",
            self.group_name,
            "--depth=1",
            self.runtime.config["build_config"]["ocp_build_data_repo_push_url"],
            str(local_path),
        ]
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd)

    async def update_build_data(self, advisories: Dict[str, int], jira_issue_key: Optional[str]):
        if not advisories and not jira_issue_key:
            return False
        repo = self.working_dir / "ocp-build-data-push"
        if not repo.exists():
            await self.clone_build_data(repo)

        if not self.assembly or self.assembly == "stream":
            # update advisory numbers in group.yml
            with open(repo / "group.yml", "r") as f:
                group_config = f.read()
            for impetus, advisory in advisories.items():
                new_group_config = re.sub(
                    fr"^(\s+{impetus}:)\s*[0-9]+$", fr"\1 {advisory}", group_config, count=1, flags=re.MULTILINE
                )
                group_config = new_group_config
            # freeze automation
            group_config = re.sub(r"^freeze_automation:.*", "freeze_automation: scheduled", group_config, count=1, flags=re.MULTILINE)
            # update group.yml
            with open(repo / "group.yml", "w") as f:
                f.write(group_config)
        else:
            # update releases.yml (if we are operating on a non-stream assembly)
            async with aiofiles.open(repo / "releases.yml", "r") as f:
                old = await f.read()
            releases_config = yaml.load(old)
            group_config = releases_config["releases"][self.assembly].setdefault("assembly", {}).setdefault("group", {})

            # Assembly key names are not always exact, they can end in special chars like !,?,-
            # to indicate special inheritance rules. So respect those
            # https://art-docs.engineering.redhat.com/assemblies/#inheritance-rules
            advisory_key = next(k for k in group_config.keys() if k.startswith("advisories"))
            release_jira_key = next(k for k in group_config.keys() if k.startswith("release_jira"))

            group_config[advisory_key] = advisories
            group_config[release_jira_key] = jira_issue_key
            out = StringIO()
            yaml.dump(releases_config, out)
            async with aiofiles.open(repo / "releases.yml", "w") as f:
                await f.write(out.getvalue())

        cmd = ["git", "-C", str(repo), "--no-pager", "diff"]
        await exectools.cmd_assert_async(cmd)
        cmd = ["git", "-C", str(repo), "add", "."]
        await exectools.cmd_assert_async(cmd)
        cmd = ["git", "-C", str(repo), "diff-index", "--quiet", "HEAD"]
        rc = await exectools.cmd_assert_async(cmd, check=False)
        if rc == 0:
            _LOGGER.warn("Skip saving advisories: No changes.")
            return False
        cmd = ["git", "-C", str(repo), "commit", "-m", f"Prepare release {self.release_name}"]
        await exectools.cmd_assert_async(cmd)
        if not self.dry_run:
            _LOGGER.info("Pushing changes to upstream...")
            cmd = ["git", "-C", str(repo), "push", "origin", self.group_name]
            await exectools.cmd_assert_async(cmd)
        else:
            _LOGGER.warn("Would have run %s", cmd)
            _LOGGER.warn("Would have pushed changes to upstream")
        return True

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sweep_bugs(
        self,
        advisory: Optional[int] = None,
        permissive: bool = False,
        advance_release: bool = False
    ):
        cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
            "find-bugs:sweep",
        ]
        if advisory:
            cmd.append(f"--add={advisory}")
        else:
            cmd.append("--into-default-advisories")
        if permissive:
            cmd.append("--permissive")
        if advance_release:
            cmd.append("--advance-release")
        if self.dry_run:
            cmd.append("--dry-run")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        subprocess.run(cmd, check=True, universal_newlines=True, cwd=self.working_dir)

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    def attach_cve_flaws(self, advisory: int):
        cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "attach-cve-flaws",
            f"--advisory={advisory}",
        ]
        if self.dry_run:
            cmd.append("--dry-run")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        subprocess.run(cmd, check=True, universal_newlines=True, cwd=self.working_dir)

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    async def sweep_builds_async(self, impetus: str, advisory: int):
        only_payload = False
        only_non_payload = False
        kind = ""

        if impetus == "rpm":
            kind = "rpm"
        elif impetus in ["image", "extras"]:
            kind = "image"
            if impetus == "image" and self.release_version[0] >= 4:
                only_payload = True  # OCP 4+ image advisory only contains payload images
            elif impetus == "extras":
                only_non_payload = True
        elif impetus in ("silentops", "prerelease", "advance", "bootimage", "microshift"):
            return  # do not sweep into these advisories initially; later steps may fill them

        if not kind:
            raise ValueError("Specified impetus is not supported: %s", impetus)

        cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
        ]
        cmd.extend([
            "find-builds",
            f"--kind={kind}",
        ])
        if only_payload:
            cmd.append("--payload")
        if only_non_payload:
            cmd.append("--non-payload")
        if self.include_shipped:
            cmd.append("--include-shipped")
        cmd.append(f"--attach={advisory}")
        cmd.append("--clean")
        if self.dry_run:
            cmd.append("--dry-run")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, cwd=self.working_dir)

    async def change_advisory_state_qe(self, advisory: int):
        cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
            "change-state",
            "-s", "QE",
            "--from", "NEW_FILES",
            "-a", str(advisory),
        ]
        if self.dry_run:
            cmd.append("--dry-run")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, cwd=self.working_dir)

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    def verify_payload(self, pullspec_or_imagestream: str, advisory: int):
        cmd = [
            "elliott",
            f"--working-dir={self.elliott_working_dir}",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
            "verify-payload",
            f"{pullspec_or_imagestream}",
            f"{advisory}",
        ]
        if self.dry_run:
            _LOGGER.info("Would have run: %s", cmd)
            return
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        subprocess.run(cmd, check=True, universal_newlines=True, cwd=self.working_dir)
        # elliott verify-payload always writes results to $cwd/"summary_results.json".
        # move it to a different location to avoid overwritting the result.
        results_path = self.working_dir / "summary_results.json"
        new_path = self.working_dir / f"verify-payload-results-{pullspec_or_imagestream.split(':')[-1]}.json"
        shutil.move(results_path, new_path)
        with open(new_path, "r") as f:
            results = json.load(f)
            if results.get("missing_in_advisory") or results.get("payload_advisory_mismatch"):
                raise ValueError(f"""Failed to verify payload for nightly {pullspec_or_imagestream}.
Please fix advisories and nightlies to match each other, manually verify them with `elliott verify-payload`,
update JIRA accordingly, then notify QE and multi-arch QE for testing.""")

    def create_release_jira(self, template_vars: Dict):
        template_issue_key = self.runtime.config["jira"]["templates"][f"ocp{self.release_version[0]}"]

        _LOGGER.info("Creating release JIRA from template %s...",
                     template_issue_key)

        template_issue = self._jira_client.get_issue(template_issue_key)

        def fields_transform(fields):
            labels = set(fields.get("labels", []))
            # change summary title for security
            if "template" not in labels:
                return fields  # no need to modify fields of non-template issue
            # remove "template" label
            fields["labels"] = list(labels - {"template"})
            return self._render_jira_template(fields, template_vars)
        if self.dry_run:
            fields = fields_transform(template_issue.raw["fields"].copy())
            _LOGGER.warning(
                "[DRY RUN] Would have created release JIRA: %s", fields["summary"])
            return []
        new_issues = self._jira_client.clone_issue_with_subtasks(
            template_issue, fields_transform=fields_transform)
        _LOGGER.info("Created release JIRA: %s", new_issues[0].permalink())
        return new_issues

    @staticmethod
    def _render_jira_template(fields: Dict, template_vars: Dict):
        fields.copy()
        try:
            fields["summary"] = jinja2.Template(fields["summary"], autoescape=True).render(template_vars)
        except jinja2.TemplateSyntaxError as ex:
            _LOGGER.warning("Failed to render JIRA template text: %s", ex)
        try:
            fields["description"] = jinja2.Template(fields["description"], autoescape=True).render(template_vars)
        except jinja2.TemplateSyntaxError as ex:
            _LOGGER.warning("Failed to render JIRA template text: %s", ex)
        return fields

    async def set_advisory_dependencies(self, advisories):
        # dict keys should ship after values.
        blocked_by = {
            'rpm': {'image', 'extras'},
            'metadata': {'image', 'extras'},
            'microshift': {'rpm', 'image'},
        }
        for target_kind in blocked_by.keys():
            target_advisory_id = advisories.get(target_kind, 0)
            if target_advisory_id <= 0:
                continue
            expected_blocking = {advisories[k] for k in (blocked_by[target_kind] & advisories.keys()) if advisories[k] > 0}
            _LOGGER.info(f"Setting blocking advisories ({expected_blocking}) for {target_advisory_id}")
            blocking: Optional[List] = get_blocking_advisories(target_advisory_id)
            if blocking is None:
                raise ValueError(f"Failed to fetch blocking advisories for {target_advisory_id} ")
            if expected_blocking.issubset(set(blocking)):
                continue
            for blocking_advisory_id in expected_blocking:
                try:
                    set_blocking_advisory(target_advisory_id, blocking_advisory_id, "SHIPPED_LIVE")
                except Exception as ex:
                    _LOGGER.warning(f"Unable to set blocking advisories ({expected_blocking}) for {target_advisory_id}: {ex}")
                    await self._slack_client.say_in_thread(f"Unable to set blocking advisories ({expected_blocking}) for {target_advisory_id}. Details in log.")

    def update_release_jira(self, issue: Issue, subtasks: List[Issue], template_vars: Dict[str, int]):
        template_issue_key = self.runtime.config["jira"]["templates"][f"ocp{self.release_version[0]}"]
        _LOGGER.info("Updating release JIRA %s from template %s...", issue.key, template_issue_key)
        template_issue = self._jira_client.get_issue(template_issue_key)
        old_fields = {
            "summary": issue.fields.summary,
            "description": issue.fields.description,
        }
        fields = {
            "summary": template_issue.fields.summary,
            "description": template_issue.fields.description,
        }
        if "template" in template_issue.fields.labels:
            fields = self._render_jira_template(fields, template_vars)
        jira_changed = fields != old_fields
        if not self.dry_run:
            issue.update(fields)
        else:
            _LOGGER.warning("Would have updated JIRA ticket %s with summary %s", issue.key, fields["summary"])

        _LOGGER.info("Updating subtasks for release JIRA %s...", issue.key)
        template_subtasks = [self._jira_client.get_issue(subtask.key) for subtask in template_issue.fields.subtasks]
        if len(subtasks) != len(template_subtasks):
            _LOGGER.warning("Release JIRA %s has different number of subtasks from the template ticket %s. Subtasks will not be updated.", issue.key, template_issue.key)
            return
        for subtask, template_subtask in zip(subtasks, template_subtasks):
            fields = {"summary": template_subtask.fields.summary}
            if template_subtask.fields.description:
                fields["description"] = template_subtask.fields.description
            if "template" in template_subtask.fields.labels:
                fields = self._render_jira_template(fields, template_vars)
            if not self.dry_run:
                subtask.update(fields)
            else:
                _LOGGER.warning("Would have updated JIRA ticket %s with summary %s", subtask.key, fields["summary"])

        return jira_changed

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    async def build_and_attach_bundles(self, metadata_advisory: int):
        _LOGGER.info("Finding OLM bundles (will rebuild if not present)...")
        cmd = [
            "doozer",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
            "olm-bundle:rebase-and-build",
        ]
        if self.dry_run:
            cmd.append("--dry-run")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=self._doozer_env_vars, cwd=self.working_dir)
        # parse record.log
        with open(self.doozer_working_dir / "record.log", "r") as file:
            record_log = parse_record_log(file)
        bundle_nvrs = [record["bundle_nvr"] for record in record_log["build_olm_bundle"] if record["status"] == "0"]

        if not bundle_nvrs:
            return
        _LOGGER.info("Attaching bundle builds %s to advisory %s...", bundle_nvrs, metadata_advisory)
        cmd = [
            "elliott",
            f"--group={self.group_name}",
            "--assembly", self.assembly,
            "find-builds",
            "--kind=image",
        ]
        for bundle_nvr in bundle_nvrs:
            cmd.append("--build")
            cmd.append(bundle_nvr)
        if not self.dry_run and metadata_advisory:
            cmd.append("--attach")
            cmd.append(f"{metadata_advisory}")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, cwd=self.working_dir)

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    async def verify_attached_operators(self, *advisories: List[int], gather_dependencies=False):
        cmd = [
            "elliott",
            f"--group={self.group_name}",
            f"--assembly={self.assembly}",
            "verify-attached-operators",
        ]
        if gather_dependencies:
            cmd.append("--gather-dependencies")
        cmd.append("--")
        for advisory in advisories:
            cmd.append(f"{advisory}")
        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, cwd=self.working_dir)

    async def remove_builds_all(self, advisory_id):
        """
        Remove all builds from advisory
        """
        _LOGGER.info(f"Removing all builds from advisory {advisory_id}")
        cmd = [
            "elliott",
            f"--group={self.group_name}",
            f"--assembly={self.assembly}",
            "remove-builds",
            "--all",
            f"--advisory={advisory_id}",
        ]
        if self.dry_run:
            cmd.append("--dry-run")

        _LOGGER.info("Running command: %s", ' '.join(cmd))
        await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, cwd=self.working_dir)


@cli.command("prepare-release")
@click.option("-g", "--group", metavar='NAME', required=True,
              help="The group of components on which to operate. e.g. openshift-4.9")
@click.option("--assembly", metavar="ASSEMBLY_NAME", required=True, default="stream",
              help="The name of an assembly to rebase & build for. e.g. 4.9.1")
@click.option("--name", metavar="RELEASE_NAME",
              help="release name (e.g. 4.6.42)")
@click.option("--date", metavar="YYYY-MMM-DD",
              help="Expected release date (e.g. 2020-11-25)")
@click.option("--package-owner", metavar='EMAIL',
              help="Advisory package owner; Must be an individual email address; May be anyone who wants random advisory spam")
@click.option("--nightly", "nightlies", metavar="TAG", multiple=True,
              help="[MULTIPLE] Candidate nightly")
@click.option("--default-advisories", is_flag=True,
              help="don't create advisories/jira; pick them up from ocp-build-data")
@click.option("--include-shipped", is_flag=True, required=False,
              help="Do not filter our shipped builds, attach all builds to advisory")
@click.option("--skip-batch", is_flag=True,
              help="Skip using batch")
@pass_runtime
@click_coroutine
async def prepare_release(runtime: Runtime, group: str, assembly: str, name: Optional[str], date: str,
                          package_owner: Optional[str], nightlies: Tuple[str, ...], default_advisories: bool,
                          include_shipped: bool, skip_batch: bool):
    slack_client = runtime.new_slack_client()
    slack_client.bind_channel(group)
    await slack_client.say_in_thread(f":construction: prepare-release for {name if name else assembly} "
                                     ":construction:")
    try:
        # parse environment variables for credentials
        jira_token = os.environ.get("JIRA_TOKEN")
        if not runtime.dry_run and not jira_token:
            raise ValueError("JIRA_TOKEN environment variable is not set")
        # start pipeline
        pipeline = PrepareReleasePipeline(
            slack_client=slack_client,
            runtime=runtime,
            group=group,
            assembly=assembly,
            name=name,
            date=date,
            nightlies=nightlies,
            package_owner=package_owner,
            jira_token=jira_token,
            default_advisories=default_advisories,
            include_shipped=include_shipped,
            skip_batch=skip_batch,
        )
        await pipeline.run()
        await slack_client.say_in_thread(f":white_check_mark: @release-artists prepare-release for {name if name else assembly} completes.")
    except Exception as e:
        await slack_client.say_in_thread(f":warning: @release-artists prepare-release for {name if name else assembly} has result FAILURE.")
        raise e  # return failed status to jenkins
