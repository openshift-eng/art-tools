import asyncio
import hashlib
import io
import json
import logging
import os
import re
import shutil
import ssl
import sys
import tarfile
import traceback
from collections import OrderedDict
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Union
from urllib.parse import quote, urlparse

import aiohttp
import click
import gitlab
import requests
from artcommonlib import exectools
from artcommonlib.arch_util import (
    brew_arch_for_go_arch,
    brew_suffix_for_arch,
    go_arch_for_brew_arch,
    go_suffix_for_arch,
)
from artcommonlib.assembly import AssemblyTypes
from artcommonlib.exceptions import VerificationError
from artcommonlib.exectools import manifest_tool, to_thread
from artcommonlib.rhcos import get_primary_container_name
from artcommonlib.util import isolate_major_minor_in_group, new_roundtrip_yaml_handler
from elliottlib.shipment_utils import get_shipment_config_from_mr, get_shipment_configs_from_mr
from github import Github, GithubException
from requests_gssapi import HTTPSPNEGOAuth
from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from semver import VersionInfo
from tenacity import (
    RetryCallState,
    RetryError,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)

from pyartcd import constants, jenkins, locks, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.jira_client import JIRAClient
from pyartcd.locks import Lock
from pyartcd.mail import MailService
from pyartcd.oc import (
    extract_baremetal_installer,
    extract_release_binary,
    extract_release_client_tools,
    get_release_image_info,
    get_release_image_info_from_pullspec,
    get_release_image_pullspec,
)
from pyartcd.runtime import GroupRuntime, Runtime
from pyartcd.signatory import AsyncSignatory, SigstoreSignatory

yaml = YAML(typ="safe")
yaml.default_flow_style = False

# YAML handler for shipment config dumping
shipment_yaml = YAML()
shipment_yaml.default_flow_style = False
shipment_yaml.preserve_quotes = True
shipment_yaml.indent(mapping=2, sequence=4, offset=2)


class PromotePipeline:
    DEST_RELEASE_IMAGE_REPO = constants.RELEASE_IMAGE_REPO

    @classmethod
    async def create(cls, *args, **kwargs):
        self = cls(*args, **kwargs)
        self.group_runtime = await GroupRuntime.create(
            self.runtime.config,
            self.runtime.working_dir,
            self.group,
            self.assembly,
        )
        return self

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        skip_blocker_bug_check: bool = False,
        skip_attached_bug_check: bool = False,
        skip_image_list: bool = False,
        skip_build_microshift: bool = False,
        skip_signing: bool = False,
        skip_sigstore: bool = False,
        skip_cincinnati_prs: bool = False,
        skip_ota_notification: bool = False,
        permit_overwrite: bool = False,
        no_multi: bool = False,
        multi_only: bool = False,
        skip_mirror_binaries: bool = False,
        use_multi_hack: bool = False,
        signing_env: Optional[str] = None,
    ) -> None:
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.skip_blocker_bug_check = skip_blocker_bug_check
        self.skip_attached_bug_check = skip_attached_bug_check
        self.skip_image_list = skip_image_list
        self.skip_build_microshift = skip_build_microshift
        self.skip_mirror_binaries = skip_mirror_binaries
        self.skip_signing = skip_signing
        self.skip_sigstore = skip_sigstore
        self.skip_cincinnati_prs = skip_cincinnati_prs
        self.skip_ota_notification = skip_ota_notification
        self.permit_overwrite = permit_overwrite

        if multi_only and no_multi:
            raise ValueError("Option multi_only can't be used with no_multi")
        self.no_multi = no_multi
        self.multi_only = multi_only
        self.use_multi_hack = use_multi_hack
        self._multi_enabled = False
        if not self.skip_signing and not signing_env:
            raise ValueError("--signing-env is required unless --skip-signing is set")
        if not self.skip_sigstore and not signing_env:
            raise ValueError("--signing-env is required unless --skip-sigstore is set")
        self.signing_env = signing_env

        self._logger = self.runtime.logger
        self._slack_client = self.runtime.new_slack_client()
        self._mail = self.runtime.new_mail_client()

        self._working_dir = self.runtime.working_dir
        self._doozer_working_dir = self._working_dir / "doozer-working"
        self._doozer_env_vars = os.environ.copy()
        self._doozer_env_vars["DOOZER_WORKING_DIR"] = str(self._doozer_working_dir)
        self._doozer_lock = asyncio.Lock()
        self._elliott_working_dir = self._working_dir / "elliott-working"
        self._elliott_env_vars = os.environ.copy()
        self._elliott_env_vars["ELLIOTT_WORKING_DIR"] = str(self._elliott_working_dir)
        self._elliott_lock = asyncio.Lock()
        self._ocp_build_data_url = self.runtime.config.get("build_config", {}).get("ocp_build_data_url")
        self._jira_client = JIRAClient.from_url(
            self.runtime.config["jira"]["url"], token_auth=os.environ.get("JIRA_TOKEN")
        )
        if self._ocp_build_data_url:
            self._elliott_env_vars["ELLIOTT_DATA_PATH"] = self._ocp_build_data_url
            self._doozer_env_vars["DOOZER_DATA_PATH"] = self._ocp_build_data_url

    def check_environment_variables(self):
        logger = self.runtime.logger

        required_vars = ["GITHUB_TOKEN", "JIRA_TOKEN", "QUAY_PASSWORD"]
        if not self.skip_mirror_binaries and not self.skip_signing:
            required_vars += ["AWS_SHARED_CREDENTIALS_FILE", "CLOUDFLARE_ENDPOINT"]
        if not self.skip_signing:
            required_vars += ["SIGNING_CERT", "SIGNING_KEY", "REDIS_SERVER_PASSWORD"]
        if not self.skip_sigstore:
            required_vars += ["KMS_CRED_FILE", "KMS_KEY_ID"]
        if not self.skip_build_microshift:
            required_vars += ["JENKINS_SERVICE_ACCOUNT", "JENKINS_SERVICE_ACCOUNT_TOKEN"]

        for env_var in required_vars:
            if not os.environ.get(env_var):
                msg = f"Environment variable {env_var} is not set."
                if not self.runtime.dry_run:
                    raise ValueError(msg)
                else:
                    logger.warning(msg)

    async def run(self):
        logger = self.runtime.logger
        # Check if all required environment variables are set
        self.check_environment_variables()

        # Load group config and releases.yml
        logger.info("Loading build data...")
        group_config = self.group_runtime.group_config
        releases_config = await util.load_releases_config(
            group=self.group,
            data_path=self._doozer_env_vars.get("DOOZER_DATA_PATH", None) or constants.OCP_BUILD_DATA_URL,
        )
        if releases_config.get("releases", {}).get(self.assembly) is None:
            raise ValueError(
                f"To promote this release, assembly {self.assembly} must be explicitly defined in releases.yml."
            )
        permits = util.get_assembly_promotion_permits(releases_config, self.assembly)

        # Get release name
        assembly_type = util.get_assembly_type(releases_config, self.assembly)
        release_name = util.get_release_name_for_assembly(self.group, releases_config, self.assembly)
        # Ensure release name is valid
        if not VersionInfo.is_valid(release_name):
            raise ValueError(f"Release name `{release_name}` is not a valid semver.")
        logger.info("Release name: %s", release_name)

        self._slack_client.bind_channel(release_name)
        await self._slack_client.say_in_thread(f"Promoting release `{release_name}` @release-artists")

        justifications = []
        try:
            self._multi_enabled = group_config.get("multi_arch", {}).get("enabled", False)
            if self.multi_only and not self._multi_enabled:
                raise ValueError("Can't promote a multi payload: multi_arch.enabled is not set in group config")
            # Get arches
            arches = group_config.get("arches", [])
            arches = list(set(map(brew_arch_for_go_arch, arches)))
            if not arches:
                raise ValueError("No arches specified in group config.")
            # Get previous list
            upgrades_str: Optional[str] = group_config.get("upgrades")
            if upgrades_str is None and assembly_type not in [AssemblyTypes.CUSTOM]:
                raise ValueError(
                    f"Group config for assembly {self.assembly} is missing the required `upgrades` field. If no upgrade edges are expected, please explicitly set the `upgrades` field to empty string."
                )
            previous_list = list(map(lambda s: s.strip(), upgrades_str.split(","))) if upgrades_str else []
            # Ensure all versions in previous list are valid semvers.
            if any(map(lambda version: not VersionInfo.is_valid(version), previous_list)):
                raise ValueError("Previous list (`upgrades` field in group config) has an invalid semver.")

            # Get next list
            # We do not in our normal process require populating "next" edges
            # In normal flow, each release's "next" edges are the following release's "previous" edges
            # But in case we miss adding an edge in "previous" list, we can add it in a "next" list
            # Example: 4.13.a and 4.14.b are shipping together in a week. 4.14.b has 4.13.a in its "previous" list.
            # Due to new requirements we re-promote 4.13 which becomes 4.13.(a+1)
            # 4.14.b does not have 4.13.(a+1) in its "previous" list.
            # So we need to add 4.14.b in 4.13.(a+1)'s "next" list
            upgrades_next_str: Optional[str] = group_config.get("upgrades_next")
            next_list = list(map(lambda s: s.strip(), upgrades_next_str.split(","))) if upgrades_next_str else []
            # Ensure all versions in next list are valid semvers.
            if next_list and any(map(lambda version: not VersionInfo.is_valid(version), next_list)):
                raise ValueError("Next list (`upgrades_next` field in group config) has an invalid semver.")

            impetus_advisories = group_config.get("advisories", {})

            # Check for blocker bugs
            if self.skip_blocker_bug_check or assembly_type in [
                AssemblyTypes.CANDIDATE,
                AssemblyTypes.CUSTOM,
                AssemblyTypes.PREVIEW,
            ]:
                logger.info("Blocker Bug check is skipped.")
            else:
                logger.info("Checking for blocker bugs...")
                try:
                    await self.check_blocker_bugs()
                except VerificationError as err:
                    logger.warn("Blocker bugs found for release: %s", err)
                    justification = self._reraise_if_not_permitted(err, "BLOCKER_BUGS", permits)
                    justifications.append(justification)
                logger.info("No blocker bugs found.")

            if assembly_type == AssemblyTypes.STANDARD:
                # Attempt to move all advisories to QE
                tasks_with_args = []
                for impetus, advisory in impetus_advisories.items():
                    # microshift advisory is special, and it will not be ready at this time
                    if impetus == "microshift":
                        continue
                    if not advisory or advisory <= 0:
                        continue
                    logger.info("Moving advisory %s to QE...", advisory)
                    tasks_with_args.append(
                        {"args": (impetus, advisory), "task": self.change_advisory_state_qe(advisory)}
                    )

                results = await asyncio.gather(*[t["task"] for t in tasks_with_args], return_exceptions=True)
                for i in range(len(results)):
                    if isinstance(results[i], Exception):
                        impetus, advisory = tasks_with_args[i]["args"]
                        logger.warn("Error moving advisory %s %s to QE: %s", impetus, advisory, results[i])
                        await self._slack_client.say_in_thread(
                            f"Unable to move {impetus} advisory {advisory} to QE. Details in log."
                        )

            image_advisory = impetus_advisories.get("image", 0)
            shipment_config = group_config.get("shipment")
            errata_url, full_advisory_id = "", ""

            # do a sanity check
            if shipment_config and image_advisory != 0:
                raise ValueError("Shipment config is defined but image advisory is also defined!")

            if shipment_config:
                shipment_url = shipment_config.get("url")
                if not shipment_url:
                    raise ValueError("Shipment config is defined but url is not defined!")
                image_shipment = get_shipment_config_from_mr(shipment_url, "image")
                if not image_shipment:
                    raise ValueError("Could not find image shipment config in merge request!")
                live_id = image_shipment.shipment.data.releaseNotes.live_id
                if not live_id:
                    raise ValueError("Could not find live ID in image shipment config!")

                # construct full advisory id like RHBA-2025:13660
                advisory_type = image_shipment.shipment.data.releaseNotes.type
                year = datetime.now().strftime("%Y")
                full_advisory_id = f"{advisory_type}-{year}:{live_id}"
                logger.info("Constructed full advisory ID from shipment config: %s", full_advisory_id)
                # TODO: ensure that shipment MR is open and is not in a draft state (and optionally stage push is successful)
            else:
                # Ensure the image advisory is in QE (or later) state.
                if assembly_type in [AssemblyTypes.STANDARD, AssemblyTypes.CANDIDATE]:
                    if image_advisory <= 0:
                        err = VerificationError(f"No associated image advisory for {self.assembly} is defined.")
                        justification = self._reraise_if_not_permitted(err, "NO_ERRATA", permits)
                        justifications.append(justification)
                    else:
                        logger.info("Verifying associated image advisory %s...", image_advisory)
                        image_advisory_info = await self.get_advisory_info(image_advisory)
                        try:
                            if assembly_type != AssemblyTypes.CANDIDATE:
                                self.verify_advisory_status(image_advisory_info)
                        except VerificationError as err:
                            logger.warn("%s", err)
                            justification = self._reraise_if_not_permitted(err, "INVALID_ERRATA_STATUS", permits)
                            justifications.append(justification)

                        full_advisory_id = self.get_live_id(image_advisory_info)

            if assembly_type in [AssemblyTypes.STANDARD, AssemblyTypes.CANDIDATE]:
                if not full_advisory_id:
                    raise VerificationError("Could not find live ID from image advisory. Please investigate.")
                errata_url = f"https://access.redhat.com/errata/{full_advisory_id}"  # don't quote
                logger.info("Using errata URL: %s", errata_url)

            # Verify attached bugs
            if self.skip_attached_bug_check:
                logger.info("Skip checking attached bugs.")
            else:
                logger.info("Verifying attached bugs...")
                advisories = list(filter(lambda ad: ad > 0, impetus_advisories.values()))

                # FIXME: We used to skip blocking bug check for the latest minor version,
                # because there were a lot of ON_QA bugs in the upcoming GA version blocking us
                # from preparing z-stream releases for the latest minor version.
                # Per https://coreos.slack.com/archives/GDBRP5YJH/p1662036090856369?thread_ts=1662024464.786929&cid=GDBRP5YJH,
                # we would like to try not skipping it by commenting out the following lines and see what will happen.
                # major, minor = util.isolate_major_minor_in_group(self.group)
                # next_minor = f"{major}.{minor + 1}"
                # logger.info("Checking if %s is GA'd...", next_minor)
                # graph_data = await CincinnatiAPI().get_graph(channel=f"fast-{next_minor}")
                # if not graph_data.get("nodes"):
                #     logger.info("%s is not GA'd. Blocking Bug check will be skipped.", next_minor)
                #     no_verify_blocking_bugs = True
                # else:
                #     logger.info("%s is GA'd. Blocking Bug check will be enforced.", next_minor)

                no_verify_blocking_bugs = False
                if assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE] or self.assembly.endswith(".0"):
                    no_verify_blocking_bugs = True

                verify_flaws = True
                if "prerelease" in impetus_advisories.keys() or assembly_type == AssemblyTypes.PREVIEW:
                    verify_flaws = False

                try:
                    await self.verify_attached_bugs(
                        advisories, no_verify_blocking_bugs=no_verify_blocking_bugs, verify_flaws=verify_flaws
                    )
                except ChildProcessError as err:
                    logger.warn("Error verifying attached bugs: %s", err)

                    if assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
                        await self._slack_client.say_in_thread(
                            f"Attached bugs have some issues. Permitting since assembly is of type {assembly_type}"
                        )
                        await self._slack_client.say_in_thread(str(err))
                    else:
                        justification = self._reraise_if_not_permitted(err, "ATTACHED_BUGS", permits)
                        justifications.append(justification)

            # Promote release images
            metadata = {}
            description = group_config.get("description")
            if description:
                logger.warning(
                    "The following description message will be included in the metadata of release image: %s",
                    description,
                )
                metadata["description"] = str(description)
            if errata_url:
                metadata["url"] = errata_url
            tag_stable = assembly_type in [AssemblyTypes.STANDARD, AssemblyTypes.CANDIDATE, AssemblyTypes.PREVIEW]
            release_infos = await self.promote(
                assembly_type, release_name, arches, previous_list, next_list, metadata, tag_stable=tag_stable
            )
            pullspecs = {arch: release_info["image"] for arch, release_info in release_infos.items()}
            pullspecs_repr = ", ".join(f"{arch}: {pullspecs[arch]}" for arch in sorted(pullspecs.keys()))
            self._logger.info(
                "All release images for %s have been promoted. Pullspecs: %s", release_name, pullspecs_repr
            )

            # Update shipment MR with payload SHAs immediately after promotion
            payload_shas = {}
            for arch, release_info in release_infos.items():
                payload_shas[arch] = release_info["digest"]

            shipment_config = group_config.get("shipment")
            if shipment_config and shipment_config.get("url"):
                shipment_url = shipment_config["url"]
                self._logger.info("Found shipment configuration with URL: %s", shipment_url)
                self._logger.info("Updating shipment MR with payload SHAs for %d architectures...", len(payload_shas))
                try:
                    await self.update_shipment_with_payload_shas(shipment_url, payload_shas)
                    self._logger.info("Successfully updated shipment MR with payload SHAs")
                except Exception as ex:
                    self._logger.warning("Failed to update shipment MR with payload SHAs: %s", ex)
                    await self._slack_client.say_in_thread(f"Failed to update shipment MR with payload SHAs: {ex}")
            else:
                self._logger.info("No shipment configuration found, skipping shipment MR update")

            # Signing payloads prior to adding it to the release controller assures that we are testing
            # signature verification processes in an installing/running cluster. In the future, we might
            # want to sign with a beta key before being accepted, so we don't gold sign all named releases,
            # however, ClusterImagePolicy/ImagePolicy CRDs presently only support one public key per
            # registry location.
            if not self.skip_sigstore:
                await self.sigstore_sign(release_name, release_infos)

            # Before waiting for release images to be accepted by release controllers,
            # we can start microshift build
            if "microshift" in impetus_advisories.keys():
                await self._build_microshift(releases_config)
            else:
                self._logger.warning(
                    "Skipping microshift build because microshift advisory not found. "
                    "If you need to build, first define microshift advisory in assembly config"
                )

            release_jira = group_config.get("release_jira", '')

            # Send notification to QE if it hasn't been sent yet
            # Skip ECs and RCs
            if assembly_type not in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE, AssemblyTypes.CUSTOM]:
                self.handle_qe_notification(release_jira, release_name, impetus_advisories)

            if not tag_stable:
                self._logger.warning(
                    "Release %s will not appear on release controllers. Pullspecs: %s", release_name, pullspecs_repr
                )
                await self._slack_client.say_in_thread(
                    f"Release {release_name} is ready. It will not appear on the "
                    "release controllers. Please tell the user to manually pull "
                    f"the release images: {pullspecs_repr}"
                )
                message_digests = []
                if not self.skip_mirror_binaries:
                    message_digests = await self.extract_and_publish_clients("ocp-dev-preview", release_infos)
                if not self.skip_signing:
                    await self.sign_artifacts(release_name, "ocp-dev-preview", release_infos, message_digests)
            else:
                # check if release is already accepted (in case we timeout and run the job again)
                tasks = []
                for arch, release_info in release_infos.items():
                    release_stream = self._get_release_stream_name(assembly_type, arch)
                    # Currently the multi payload uses a different release name to workaround a cincinnati issue.
                    # Use the release name in release_info instead.
                    actual_release_name = release_info["metadata"]["version"]
                    tasks.append(self.is_accepted(actual_release_name, arch, release_stream))
                accepted = await asyncio.gather(*tasks)

                if not all(accepted):
                    # Wait for release images to be accepted by the release controllers
                    self._logger.info(
                        "Waiting for release images for %s to be accepted by the release controller...", release_name
                    )
                    await self._slack_client.say_in_thread(
                        f"Release {release_name} has been tagged on release "
                        "controller, but is not accepted yet. Waiting."
                    )
                    tasks = []
                    for arch, release_info in release_infos.items():
                        release_stream = self._get_release_stream_name(assembly_type, arch)
                        # Currently the multi payload uses a different release name to workaround a cincinnati issue.
                        # Use the release name in release_info instead.
                        actual_release_name = release_info["metadata"]["version"]
                        tasks.append(self.wait_for_stable(actual_release_name, arch, release_stream))
                    try:
                        await asyncio.gather(*tasks)
                    except RetryError as err:
                        message = f"Timeout waiting for release to be accepted by the release controllers: {err}"
                        self._logger.error(message)
                        self._logger.exception(err)
                        raise TimeoutError(message)

                self._logger.info(
                    "All release images for %s have been accepted by the release controllers.", release_name
                )

                message = f"Release `{release_name}` has been accepted by the release controllers."
                await self._slack_client.say_in_thread(message)

                # Send image list
                if not image_advisory:
                    self._logger.warning(
                        "No need to send an advisory image list because this release doesn't have an image advisory."
                    )
                elif assembly_type in (AssemblyTypes.CANDIDATE, AssemblyTypes.PREVIEW):
                    self._logger.warning("No need to send an advisory image list for a candidate release.")
                elif self.skip_image_list:
                    self._logger.warning("Skip sending advisory image list")
                else:
                    self._logger.info("Gathering and sending advisory image list...")
                    mail_dir = self._working_dir / "email"
                    await self.send_image_list_email(release_name, image_advisory, mail_dir)
                    self._logger.info("Advisory image list sent.")

                # update jira promote task status
                self._logger.info("Updating promote release subtask")
                if release_jira and not self.runtime.dry_run:
                    parent_jira = self._jira_client.get_issue(release_jira)
                    title = "Promote the tested nightly"
                    subtask = next((s for s in parent_jira.fields.subtasks if title in s.fields.summary), None)
                    if not subtask:
                        self._logger.warning("Promote release subtask not found in release_jira: %s", release_jira)
                    elif subtask.fields.status.name != "Closed":
                        self._jira_client.add_comment(
                            subtask,
                            "promote release job : {}".format(os.environ.get("BUILD_URL")),
                        )
                        self._jira_client.assign_to_me(subtask)
                        self._jira_client.close_task(subtask)

                # extract client binaries
                client_type = "ocp"
                if (
                    assembly_type == AssemblyTypes.CANDIDATE and not self.assembly.startswith('rc.')
                ) or assembly_type in [AssemblyTypes.CUSTOM, AssemblyTypes.PREVIEW]:
                    client_type = "ocp-dev-preview"
                message_digests = []
                if not self.skip_mirror_binaries:
                    message_digests = await self.extract_and_publish_clients(client_type, release_infos)
                if not self.skip_signing:
                    if not self.runtime.dry_run:
                        lock = Lock.SIGNING
                        lock_identifier = jenkins.get_build_path()
                        if not lock_identifier:
                            self._logger.warning(
                                'Env var BUILD_URL has not been defined: a random identifier will be used for the locks'
                            )

                        await locks.run_with_lock(
                            coro=self.sign_artifacts(release_name, client_type, release_infos, message_digests),
                            lock=lock,
                            lock_name=lock.value.format(signing_env=self.signing_env),
                            lock_id=lock_identifier,
                        )
                    else:
                        self._logger.warning("[DRY RUN] will sign artifacts without locking")
                        await self.sign_artifacts(release_name, client_type, release_infos, message_digests)

                # publish rhcos on mirror via rhcos_sync job
                # only if release is EC or a GA release (.0)
                # job will not mirror & overwrite if destination already exists (sync already happened)
                # if that is desired, run rhcos_sync with FORCE=true
                is_ga = assembly_type == AssemblyTypes.STANDARD and self.assembly.endswith(".0")
                if assembly_type == AssemblyTypes.PREVIEW or is_ga:
                    for arch, pullspec in pullspecs.items():
                        if arch == "multi":
                            continue

                        # '4.19.0-ec.0-aarch64' from 'quay.io/openshift-release-dev/ocp-release:4.19.0-ec.0-aarch64'
                        # Since rhocs_sync does not take the quay URL as prefix
                        short_name = pullspec.split(":")[-1]
                        jenkins.start_rhcos_sync(short_name, dry_run=self.runtime.dry_run)

        except Exception as err:
            self._logger.exception(err)
            message = f"Promoting release {release_name} failed"
            await self._slack_client.say_in_thread(message)
            raise

        # Print release infos to console
        data = {
            "group": self.group,
            "assembly": self.assembly,
            "type": assembly_type.value,
            "name": release_name,
            "content": {},
            "justifications": justifications,
        }
        if image_advisory > 0:
            data["advisory"] = image_advisory
        if errata_url:
            data["live_url"] = errata_url
        for arch, release_info in release_infos.items():
            data["content"][arch] = {
                "pullspec": release_info["image"],
                "digest": release_info["digest"],
                "metadata": {
                    k: release_info["metadata"][k] for k in release_info["metadata"].keys() & {'version', 'previous'}
                },
            }
            # if this payload is a manifest list, iterate through each manifest
            manifests = release_info.get("manifests", [])
            if manifests:
                manifests_ent = data["content"][arch]["manifests"] = {}
                for manifest in manifests:
                    if manifest["platform"]["os"] != "linux":
                        logger.warning(
                            "Unsupported OS %s in manifest list %s", manifest["platform"]["os"], release_info["image"]
                        )
                        continue
                    manifest_arch = brew_arch_for_go_arch(manifest["platform"]["architecture"])
                    manifests_ent[manifest_arch] = {
                        "digest": manifest["digest"],
                    }

            from_release = (
                release_info.get("references", {})
                .get("metadata", {})
                .get("annotations", {})
                .get("release.openshift.io/from-release")
            )
            if from_release:
                data["content"][arch]["from_release"] = from_release
            rhcos_version = release_info.get("displayVersions", {}).get("machine-os", {}).get("Version", "")
            if rhcos_version:
                data["content"][arch]["rhcos_version"] = rhcos_version

        if assembly_type == AssemblyTypes.CUSTOM:
            self._logger.info("Skipping PR creation for custom assembly")
        elif self.skip_cincinnati_prs or self.runtime.dry_run:
            self._logger.info("Skipping Cincinnati PRs creation since skip param is set")
        else:
            await self.create_cincinnati_prs(assembly_type, data)

        try:
            # send promote complete email
            self.send_promote_complete_email(data["name"], release_infos)
        except Exception as e:
            self._logger.error("Failed to send promote complete email: %s", str(e))
            await self._slack_client.say_in_thread("Failed to send promote complete email")

        # Backup to ocp-doomsday-registry on AWS
        if "rc" in self.assembly or "ec" in self.assembly or "art" in self.assembly:
            # Skip for ECs, RCs and hotfixes
            self._logger.info("Skipping AWS backup for this payload")
        else:
            await self.ocp_doomsday_backup()

        # Print payload SHAs for each architecture
        self._logger.info("=== PAYLOAD SHAS ===")
        for arch, content in data["content"].items():
            digest = content["digest"]
            pullspec = content["pullspec"]
            self._logger.info("Arch %s: %s (%s)", arch, digest, pullspec)
        self._logger.info("===================")

        json.dump(data, sys.stdout)

        await self._slack_client.say_in_thread(f":white_check_mark: promote completed for {release_name}.")

    @staticmethod
    def _get_release_stream_name(assembly_type: AssemblyTypes, arch: str):
        go_arch_suffix = go_suffix_for_arch(arch)
        return (
            f'4-dev-preview{go_arch_suffix}' if assembly_type == AssemblyTypes.PREVIEW else f'4-stable{go_arch_suffix}'
        )

    @staticmethod
    def _get_image_stream_name(assembly_type: AssemblyTypes, arch: str):
        go_arch_suffix = go_suffix_for_arch(arch)
        return (
            f'4-dev-preview{go_arch_suffix}' if assembly_type == AssemblyTypes.PREVIEW else f'release{go_arch_suffix}'
        )

    def send_promote_complete_email(self, name, release_infos):
        content = "PullSpecs: \n"
        for arch, release_info in release_infos.items():
            content += f"{arch}: {release_info['image']}\n"
        content += f"\nJenkins Job: {os.environ.get('BUILD_URL')}\n"
        content += "NOTE: These job links are only available to ART. Please contact us if you need to see something specific from the logs.\n"
        mail = MailService.from_config(self.runtime.config)
        mail.send_mail(
            self.runtime.config["email"]["promote_complete_recipients"],
            f"Success building release payload: {name}",
            content,
            dry_run=self.runtime.dry_run,
        )

    def _reraise_if_not_permitted(self, err: VerificationError, code: str, permits: Iterable[Dict]):
        permit = next(filter(lambda waiver: waiver["code"] == code, permits), None)
        if not permit:
            raise err
        justification: Optional[str] = permit.get("why")
        if not justification:
            raise ValueError("A justification is required to permit issue %s.", code)
        self._logger.warn("Issue %s is permitted with justification: %s", err, justification)
        return justification

    async def extract_and_publish_clients(self, client_type: str, release_infos: Dict):
        logger = self._logger
        # make sure login to quay
        if "QUAY_PASSWORD" in os.environ:
            cmd = [
                "docker",
                "login",
                "-u",
                "openshift-release-dev+art_quay_dev",
                "-p",
                f"{os.environ['QUAY_PASSWORD']}",
                "quay.io",
            ]
            await exectools.cmd_assert_async(cmd, env=os.environ.copy(), stdout=sys.stderr)
        base_to_mirror_dir = f"{self._working_dir}/to_mirror/openshift-v4"
        message_digests = []
        for arch, release_info in release_infos.items():
            logger.info("Extracting client binaries for %s", arch)
            pullspec = release_info["image"]
            release_name = release_info["metadata"]["version"]
            if arch == "multi":
                manifest_arches = [
                    brew_arch_for_go_arch(manifest["platform"]["architecture"])
                    for manifest in release_info.get("manifests", [])
                ]
                message_digest = await self.publish_multi_client(
                    base_to_mirror_dir, pullspec, release_name, manifest_arches, client_type
                )
            else:
                message_digest = await self.publish_client(
                    base_to_mirror_dir, pullspec, release_name, arch, client_type
                )
            message_digests.append(message_digest)
        return message_digests

    async def sign_artifacts(
        self, release_name: str, client_type: str, release_infos: Dict, message_digests: List[str]
    ):
        """Signs artifacts and publishes signature files to mirror"""
        if not self.signing_env:
            raise ValueError("--signing-env is missing")
        cert_file = os.environ["SIGNING_CERT"]
        key_file = os.environ["SIGNING_KEY"]
        uri = constants.UMB_BROKERS[self.signing_env]
        sig_keyname = "redhatrelease2" if client_type == 'ocp' else "beta2"
        self._logger.info("About to sign artifacts with key %s", sig_keyname)
        json_digest_sig_dir = self._working_dir / "json_digests"
        message_digest_sig_dir = self._working_dir / "message_digests"
        base_to_mirror_dir = self._working_dir / "to_mirror/openshift-v4"

        async with AsyncSignatory(uri, cert_file, key_file, sig_keyname=sig_keyname) as signatory:
            tasks = []
            json_digests = []
            for release_info in release_infos.values():
                version = release_info["metadata"]["version"]
                pullspec = release_info["image"]
                digest = release_info["digest"]
                json_digests.append((version, pullspec, digest))
                # if this payload is a manifest list, iterate through each manifest
                manifests = release_info.get("manifests", [])
                for manifest in manifests:
                    if manifest["platform"]["os"] != "linux":
                        raise ValueError(
                            "Unsupported OS %s in manifest list %s", manifest["platform"]["os"], release_info["image"]
                        )
                    json_digests.append((version, pullspec, manifest["digest"]))

            for version, pullspec, digest in json_digests:
                sig_file = json_digest_sig_dir / f"{digest.replace(':', '=')}" / "signature-1"
                tasks.append(self._sign_json_digest(signatory, version, pullspec, digest, sig_file))

            for message_digest in message_digests:
                input_path = base_to_mirror_dir / message_digest
                if not input_path.is_file():
                    raise IOError(f"Message digest file {input_path} doesn't exist or is not a regular file")
                sig_file = message_digest_sig_dir / f"{message_digest}.gpg"
                tasks.append(self._sign_message_digest(signatory, release_name, input_path, sig_file))
            await asyncio.gather(*tasks)

        self._logger.info("All artifacts have been successfully signed.")
        if not self.runtime.dry_run:
            self._logger.info("Publishing signatures...")
            tasks = []
            if json_digests:
                tasks.append(self._publish_json_digest_signatures(json_digest_sig_dir))
            if message_digests:
                tasks.append(self._publish_message_digest_signatures(message_digest_sig_dir))
            await asyncio.gather(*tasks)
            self._logger.info("All signatures have been published.")
        else:
            self._logger.warning("[DRY RUN] Would have published signatures.")

    async def _sign_json_digest(
        self, signatory: AsyncSignatory, release_name: str, pullspec: str, digest: str, sig_path: Path
    ):
        """Sign a JSON digest claim
        :param signatory: Signatory
        :param pullspec: Pullspec of the payload
        :param digest: SHA256 digest of the payload
        :param sig_path: Where to save the signature file
        """
        self._logger.info("Signing json digest for payload %s with digest %s...", pullspec, digest)
        sig_path.parent.mkdir(parents=True, exist_ok=True)
        with open(sig_path, "wb") as sig_file:
            await signatory.sign_json_digest(
                product="openshift", release_name=release_name, pullspec=pullspec, digest=digest, sig_file=sig_file
            )

    async def _sign_message_digest(self, signatory: AsyncSignatory, release_name, input_path: Path, sig_path: Path):
        """Sign a message digest
        :param signatory: Signatory
        :param input_path: Path to the message digest file
        :param sig_path: Where to save the signature file
        """
        self._logger.info("Signing message digest file %s...", input_path.absolute())
        sig_path.parent.mkdir(parents=True, exist_ok=True)
        with open(input_path, "rb") as in_file, open(sig_path, "wb") as sig_file:
            await signatory.sign_message_digest(
                product="openshift", release_name=release_name, artifact=in_file, sig_file=sig_file
            )

    async def _publish_json_digest_signatures(self, local_dir: Union[str, Path], env: str = "prod"):
        tasks = []
        # mirror to S3
        mirror_release_path = "release" if env == "prod" else "test"
        tasks.append(
            util.mirror_to_s3(
                local_dir,
                f"s3://art-srv-enterprise/pub/openshift-v4/signatures/openshift/{mirror_release_path}/",
                exclude="*",
                include="sha256=*",
                dry_run=self.runtime.dry_run,
            )
        )
        if mirror_release_path == "release":
            tasks.append(
                util.mirror_to_s3(
                    local_dir,
                    "s3://art-srv-enterprise/pub/openshift-v4/signatures/openshift-release-dev/ocp-release/",
                    exclude="*",
                    include="sha256=*",
                    dry_run=self.runtime.dry_run,
                )
            )
            tasks.append(
                util.mirror_to_s3(
                    local_dir,
                    "s3://art-srv-enterprise/pub/openshift-v4/signatures/openshift-release-dev/ocp-release-nightly/",
                    exclude="*",
                    include="sha256=*",
                    dry_run=self.runtime.dry_run,
                )
            )

        # mirror to google storage
        google_storage_path = "official" if env == "prod" else "test-1"
        tasks.append(
            util.mirror_to_google_cloud(
                f"{local_dir}/*",
                f"gs://openshift-release/{google_storage_path}/signatures/openshift/release",
                dry_run=self.runtime.dry_run,
            )
        )
        tasks.append(
            util.mirror_to_google_cloud(
                f"{local_dir}/*",
                f"gs://openshift-release/{google_storage_path}/signatures/openshift-release-dev/ocp-release",
                dry_run=self.runtime.dry_run,
            )
        )
        tasks.append(
            util.mirror_to_google_cloud(
                f"{local_dir}/*",
                f"gs://openshift-release/{google_storage_path}/signatures/openshift-release-dev/ocp-release-nightly",
                dry_run=self.runtime.dry_run,
            )
        )

        await asyncio.gather(*tasks)

    async def _publish_message_digest_signatures(self, local_dir: Union[str, Path]):
        # mirror to S3
        await util.mirror_to_s3(
            local_dir,
            "s3://art-srv-enterprise/pub/openshift-v4/",
            exclude="*",
            include="*/sha256sum.txt.gpg",
            dry_run=self.runtime.dry_run,
        )

    async def publish_client(self, base_to_mirror_dir: str, pullspec, release_name, build_arch, client_type):
        # Anything under this directory will be sync'd to the mirror
        shutil.rmtree(f"{base_to_mirror_dir}/{build_arch}", ignore_errors=True)

        # From the newly built release, extract the client tools into the workspace following the directory structure
        # we expect to publish to mirror
        client_mirror_dir = f"{base_to_mirror_dir}/{build_arch}/clients/{client_type}/{release_name}"
        os.makedirs(client_mirror_dir)

        # extract release clients tools
        extract_release_client_tools(pullspec, f"--to={client_mirror_dir}", None)

        # Get cli installer operator-registry pull-spec from the release
        for release_component_tag_name, source_name in constants.MIRROR_CLIENTS.items():
            image_stat, cli_pull_spec = get_release_image_pullspec(pullspec, release_component_tag_name)
            if image_stat == 0:  # image exists
                _, image_info = get_release_image_info_from_pullspec(cli_pull_spec)
                # Retrieve the commit from image info
                commit = image_info["config"]["config"]["Labels"]["io.openshift.build.commit.id"]
                source_url = image_info["config"]["config"]["Labels"]["io.openshift.build.source-location"]
                # URL to download the tarball a specific commit
                response = requests.get(f"{source_url}/archive/{commit}.tar.gz", stream=True)
                if response.ok:
                    with open(f"{client_mirror_dir}/{source_name}-src-{release_name}-{build_arch}.tar.gz", "wb") as f:
                        f.write(response.raw.read())
                    # calc shasum
                    with open(f"{client_mirror_dir}/{source_name}-src-{release_name}-{build_arch}.tar.gz", 'rb') as f:
                        shasum = hashlib.sha256(f.read()).hexdigest()
                    # write shasum to sha256sum.txt
                    with open(f"{client_mirror_dir}/sha256sum.txt", 'a') as f:
                        f.write(f"{shasum}  {source_name}-src-{release_name}-{build_arch}.tar.gz\n")
                else:
                    response.raise_for_status()
            else:
                self._logger.error(f"Error get {release_component_tag_name} image from release pullspec")

        # Upload baremetal installer binary to mirror
        self.publish_baremetal_installer_binary(pullspec, client_mirror_dir, build_arch)

        # Starting from 4.14, oc-mirror will be synced for all arches. See ART-6820 and ART-6863
        # oc-mirror was introduced in 4.10, so skip for <= 4.9.
        major, minor = isolate_major_minor_in_group(self.group)
        if (major > 4 or minor >= 14) or (major == 4 and minor >= 10 and build_arch == 'x86_64'):
            # oc image  extract requires an empty destination directory. So do this before extracting tools.
            # oc adm release extract --tools does not require an empty directory.
            image_stat, oc_mirror_pullspec = get_release_image_pullspec(pullspec, "oc-mirror")
            if image_stat == 0:  # image exist
                # extract image to workdir, if failed it will raise error in function
                multi_rhel_path = [f"--path=/usr/bin/oc-mirror*:{client_mirror_dir}"]
                extract_release_binary(
                    oc_mirror_pullspec, multi_rhel_path
                )  # will exit with 0 even if no files are exacted

                # if oc-mirror.rhel8 exists, rename it to oc-mirror
                if Path(client_mirror_dir, 'oc-mirror.rhel8').exists():
                    Path(client_mirror_dir, 'oc-mirror.rhel8').replace(f'{client_mirror_dir}/oc-mirror')

                files_extracted = 0
                for file in Path(client_mirror_dir).glob('oc-mirror*'):
                    # archive file
                    tarball = Path(f'{file}.tar.gz')
                    with tarfile.open(tarball, "w:gz") as tar:
                        tar.add(f"{file}", arcname="oc-mirror")
                    # calc shasum
                    with open(tarball, 'rb') as f:
                        shasum = hashlib.sha256(f.read()).hexdigest()
                    # write shasum to sha256sum.txt
                    with open(f"{client_mirror_dir}/sha256sum.txt", 'a') as f:
                        f.write(f"{shasum}  {tarball.name}\n")
                    # remove extracted file
                    file.unlink()
                    files_extracted += 1
                if files_extracted == 0:
                    self._logger.error("No binaries extracted from the oc-mirror image")
            else:
                self._logger.error("Error get oc-mirror image from release pullspec")

        # create symlink for clients
        self.create_symlink(client_mirror_dir, False, False)

        # extract opm binaries
        self.extract_opm(client_mirror_dir, release_name, pullspec, build_arch)

        util.log_dir_tree(client_mirror_dir)  # print dir tree
        util.log_file_content(f"{client_mirror_dir}/sha256sum.txt")  # print sha256sum.txt

        with open(f"{client_mirror_dir}/sha256sum.txt", "r") as shas:
            archives = [line.split()[-1] for line in shas.readlines()]
            seen = set()
            dupes = [x for x in archives if x in seen or seen.add(x)]
            if dupes:
                raise ValueError(f'Duplicate archive entries in {client_mirror_dir}/sha256sum.txt: {dupes}')

        # Publish the clients to our S3 bucket.
        await util.mirror_to_s3(
            f"{base_to_mirror_dir}/{build_arch}",
            f"s3://art-srv-enterprise/pub/openshift-v4/{build_arch}",
            dry_run=self.runtime.dry_run,
        )

        await util.invalidate_cloudfront_cache("/pub/openshift-v4/clients/ocp-dev-preview/latest/*")

        return f"{build_arch}/clients/{client_type}/{release_name}/sha256sum.txt"

    async def sigstore_sign(self, release_name: str, release_infos: Dict):
        """Signs release and component images with sigstore/cosign which publishes to quay"""
        CONCURRENCY_LIMIT = 100  # we run out of processes without a limit
        signatory = SigstoreSignatory(
            logger=self._logger,
            dry_run=self.runtime.dry_run,
            signing_creds=os.environ.get("KMS_CRED_FILE", "dummy-file"),
            # Allow AWS_KEY_ID to be a comma delimited list
            signing_key_ids=os.environ.get("KMS_KEY_ID", "dummy-key").strip().split(','),
            rekor_url=os.environ.get("REKOR_URL", ""),
            concurrency_limit=CONCURRENCY_LIMIT,
            sign_release=True,
            sign_components=True,
            verify_release=False,  # not needed when we're supplying the shasum pullspecs
        )
        # recursively discover all the pullspecs that need to be signed.
        images: List[str] = [
            # for paranoia, refer to release image pullspecs with shasums
            signatory.redigest_pullspec(info["image"], info["digest"])
            for info in release_infos.values()
        ]
        need_signing: Set[str] = set()
        errors: Dict[str, str] = {}  # pullspec -> exception
        need_signing, errors = await signatory.discover_pullspecs(images, release_name)

        if errors:
            # this should be impossible given we just created the pullspecs
            raise IOError(f"Not all pullspecs examined were viable: {errors}")
        if errors := await signatory.sign_pullspecs(need_signing):
            raise IOError(f"Not all signings succeeded, check errors: {errors}")

    def publish_baremetal_installer_binary(self, release_pullspec: str, client_mirror_dir: str, build_arch: str):
        _, baremetal_installer_pullspec = get_release_image_pullspec(release_pullspec, 'baremetal-installer')
        self._logger.info('baremetal-installer pullspec: %s', baremetal_installer_pullspec)

        # Check rhel version (used for archive naming)
        major, minor = isolate_major_minor_in_group(self.group)
        if major == 4 and minor < 16:
            rhel_version = 'rhel8'
            binary_name = 'openshift-baremetal-install'
        else:
            rhel_version = 'rhel9'
            binary_name = 'openshift-install-fips'

        # oc adm release extract --command=openshift-baremetal-install -n=ocp <release-pullspec>
        self._logger.info('Extracting baremetal-install')
        go_arch = go_arch_for_brew_arch(build_arch)
        extract_baremetal_installer(release_pullspec, client_mirror_dir, go_arch, binary_name)

        # Create tarball
        archive_name = f'openshift-install-{rhel_version}-{go_arch}.tar.gz'
        with tarfile.open(f'{client_mirror_dir}/{archive_name}', 'w:gz') as tar:
            tar.add(f'{client_mirror_dir}/{binary_name}', f'{binary_name}')
        self._logger.info('Created tarball %s at %s', archive_name, client_mirror_dir)

        # Write shasum to sha256sum.txt
        with open(f'{client_mirror_dir}/{archive_name}', 'rb') as f:
            shasum = hashlib.sha256(f.read()).hexdigest()
        with open(f"{client_mirror_dir}/sha256sum.txt", 'a') as f:
            f.write(f"{shasum}  {archive_name}\n")

        # Remove baremetal-installer binary
        os.remove(f'{client_mirror_dir}/{binary_name}')

    def extract_opm(self, client_mirror_dir, release_name, release_pullspec, arch):
        major, minor = isolate_major_minor_in_group(self.group)
        path_args = []
        if (major, minor) >= (4, 16):
            # from 4.16 opm has multi rhel binaries, will use operator-framework-tools
            base_path = '/tools/'
            _, operator_pullspec = get_release_image_pullspec(release_pullspec, "operator-framework-tools")
            binaries = ['opm-rhel8', 'opm-rhel9']
            platforms = ['linux', 'linux-rhel9']
        else:
            base_path = '/usr/bin/registry/'
            _, operator_pullspec = get_release_image_pullspec(release_pullspec, "operator-registry")
            binaries = ['opm']
            platforms = ['linux']
        # For x86_64, we have binaries for macOS and Windows
        if arch == 'x86_64':
            binaries += ['darwin-amd64-opm', 'windows-amd64-opm']
            platforms += ['mac', 'windows']
        for binary in binaries:
            path_args.append(f'--path={base_path}{binary}:{client_mirror_dir}')
        extract_release_binary(operator_pullspec, path_args)
        # Compress binaries into tar.gz files and calculate sha256 digests
        for idx, binary in enumerate(binaries):
            platform = platforms[idx]
            os.chmod(f"{client_mirror_dir}/{binary}", 0o755)
            with tarfile.open(
                f"{client_mirror_dir}/opm-{platform}-{release_name}.tar.gz", "w:gz"
            ) as tar:  # archive file
                tar.add(f"{client_mirror_dir}/{binary}", arcname=binary)
            os.remove(f"{client_mirror_dir}/{binary}")  # remove opm binary
            os.symlink(
                f"opm-{platform}-{release_name}.tar.gz", f"{client_mirror_dir}/opm-{platform}.tar.gz"
            )  # create symlink
            with open(f"{client_mirror_dir}/opm-{platform}-{release_name}.tar.gz", 'rb') as f:  # calc shasum
                shasum = hashlib.sha256(f.read()).hexdigest()
            with open(f"{client_mirror_dir}/sha256sum.txt", 'a') as f:  # write shasum to sha256sum.txt
                f.write(f"{shasum}  opm-{platform}-{release_name}.tar.gz\n")

    async def publish_multi_client(self, base_to_mirror_dir: str, pullspec: str, release_name, arch_list, client_type):
        # Anything under this directory will be sync'd to the mirror
        shutil.rmtree(f"{base_to_mirror_dir}/multi", ignore_errors=True)
        release_mirror_dir = f"{base_to_mirror_dir}/multi/clients/{client_type}/{release_name}"
        for arch in arch_list:
            go_arch = go_arch_for_brew_arch(arch)
            # From the newly built release, extract the client tools into the workspace following the directory structure
            # we expect to publish to mirror
            client_mirror_dir = f"{release_mirror_dir}/{go_arch}"
            os.makedirs(client_mirror_dir)
            # extract release clients tools
            extract_release_client_tools(pullspec, f"--to={client_mirror_dir}", go_arch)
            # extract baremetal installer binary
            self.publish_baremetal_installer_binary(pullspec, client_mirror_dir, arch)
            # create symlink for clients
            self.create_symlink(path_to_dir=client_mirror_dir, log_tree=True, log_shasum=True)

        # Create a master sha256sum.txt including the sha256sum.txt files from all subarches
        # This is the file we will sign -- trust is transitive to the subarches
        for dir in os.listdir(release_mirror_dir):
            if not os.path.isdir(f"{release_mirror_dir}/{dir}"):
                continue
            for root, dirs, files in os.walk(f"{release_mirror_dir}/{dir}"):
                if "sha256sum.txt" not in files:
                    continue
                with open(f"{root}/sha256sum.txt", "rb") as f:
                    shasum = hashlib.sha256(f.read()).hexdigest()
                with open(f"{release_mirror_dir}/sha256sum.txt", 'a') as f:  # write shasum to sha256sum.txt
                    f.write(f"{shasum}  {dir}/sha256sum.txt\n")
        util.log_dir_tree(release_mirror_dir)

        # Publish the clients to our S3 bucket.
        await util.mirror_to_s3(
            f"{base_to_mirror_dir}/multi",
            "s3://art-srv-enterprise/pub/openshift-v4/multi",
            dry_run=self.runtime.dry_run,
        )
        return f"multi/clients/{client_type}/{release_name}/sha256sum.txt"

    def create_symlink(self, path_to_dir, log_tree, log_shasum):
        # External consumers want a link they can rely on.. e.g. .../latest/openshift-client-linux.tgz .
        # So whatever we extract, remove the version specific info and make a symlink with that name.
        # path_to_dir is relative path artcd_working/to_mirror/openshift-v4/aarch64/clients/ocp/4.13.0-rc.6
        for f in os.listdir(path_to_dir):
            if f.endswith(('.tar.gz', '.bz', '.zip', '.tgz')):
                # Is this already a link?
                if os.path.islink(f"{path_to_dir}/{f}"):
                    continue
                # example file names:
                #  - openshift-client-linux-4.3.0-0.nightly-2019-12-06-161135.tar.gz
                #  - openshift-client-mac-4.3.0-0.nightly-2019-12-06-161135.tar.gz
                #  - openshift-install-mac-4.3.0-0.nightly-2019-12-06-161135.tar.gz
                #  - openshift-client-linux-4.1.9.tar.gz
                #  - openshift-install-mac-4.3.0-0.nightly-s390x-2020-01-06-081137.tar.gz
                #  ...
                # So, match, and store in a group, any character up to the point we find -DIGIT. Ignore everything else
                # until we match (and store in a group) one of the valid file extensions.
                match = re.match(r'^([^-]+)((-[^0-9][^-]+)+)-[0-9].*(tar.gz|tgz|bz|zip)$', f)
                if match:
                    new_name = match.group(1) + match.group(2) + '.' + match.group(4)
                    # Create a symlink like openshift-client-linux.tgz => openshift-client-linux-4.3.0-0.nightly-2019-12-06-161135.tar.gz
                    os.symlink(f, f"{path_to_dir}/{new_name}")

        if log_tree:
            util.log_dir_tree(path_to_dir)  # print dir tree
        if log_shasum:
            util.log_file_content(f"{path_to_dir}/sha256sum.txt")  # print sha256sum.txt

    async def change_advisory_state_qe(self, advisory: int):
        cmd = [
            "elliott",
            "change-state",
            "-s",
            "QE",
            "--from",
            "NEW_FILES",
            "-a",
            str(advisory),
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        async with self._elliott_lock:
            await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, stdout=sys.stderr)

    async def check_blocker_bugs(self):
        # Note: --assembly option should always be "stream". We are checking blocker bugs for this release branch regardless of the sweep cutoff timestamp.
        cmd = [
            "elliott",
            f"--group={self.group}",
            "--assembly=stream",
            "find-bugs:blocker",
        ]
        async with self._elliott_lock:
            _, stdout, _ = await exectools.cmd_gather_async(cmd, env=self._elliott_env_vars, stderr=None)
        match = re.search(r"Found ([0-9]+) bugs", stdout)
        if not match:
            raise IOError(
                f"Could determine whether this release has blocker bugs. Elliott printed unexpected message: {stdout}"
            )
        if int(match[1]) != 0:
            raise VerificationError(
                f"{int(match[1])} blocker Bug(s) found for release; do not proceed without resolving. See https://art-docs.engineering.redhat.com/release/4.y.z-stream/#handling-blocker-bugs. To permit this validation error, see https://art-docs.engineering.redhat.com/jenkins/build-promote-assembly-readme/#permit-certain-validation-failures. Elliott output: {stdout}"
            )

    async def get_advisory_info(self, advisory: int) -> Dict:
        cmd = [
            "elliott",
            f"--group={self.group}",
            "get",
            "--json",
            "-",
            "--",
            f"{advisory}",
        ]
        async with self._elliott_lock:
            _, stdout, _ = await exectools.cmd_gather_async(cmd, env=self._elliott_env_vars, stderr=None)
        advisory_info = json.loads(stdout)
        if not isinstance(advisory_info, dict):
            raise ValueError(f"Got invalid advisory info for advisory {advisory}: {advisory_info}.")
        return advisory_info

    async def _build_microshift(self, releases_config):
        if self.skip_build_microshift:
            self._logger.info("Skipping microshift build because SKIP_BUILD_MICROSHIFT is set.")
            return

        major, minor = isolate_major_minor_in_group(self.group)
        if major == 4 and minor < 14:
            self._logger.info("Skip microshift build for version < 4.14")
            return

        jenkins.start_build_microshift(f'{major}.{minor}', self.assembly, self.runtime.dry_run)

    @staticmethod
    def get_live_id(advisory_info: Dict):
        # Extract live ID from advisory info
        # Examples:
        # - advisory with a live ID
        #     "errata_id": 2681,
        #     "fulladvisory": "RHBA-2019:2681-02",
        #     "id": 46049,
        #     "old_advisory": "RHBA-2019:46049-02",
        # - advisory without a live ID
        #     "errata_id": 46143,
        #     "fulladvisory": "RHBA-2019:46143-01",
        #     "id": 46143,
        #     "old_advisory": null,
        if advisory_info["errata_id"] == advisory_info["id"]:
            # advisory doesn't have a live ID
            return None
        live_id = advisory_info["fulladvisory"].rsplit("-", 1)[0]  # RHBA-2019:2681-02 => RHBA-2019:2681
        return live_id

    def verify_advisory_status(self, advisory_info: Dict):
        if advisory_info["status"] not in {"QE", "REL_PREP", "PUSH_READY", "IN_PUSH", "SHIPPED_LIVE"}:
            raise VerificationError(f"Advisory {advisory_info['id']} should not be in {advisory_info['status']} state.")

    async def verify_attached_bugs(
        self, advisories: Iterable[int], no_verify_blocking_bugs: bool, verify_flaws: bool = True
    ):
        advisories = list(advisories)
        if not advisories:
            self._logger.warning("No advisories to verify.")
            return
        cmd = [
            "elliott",
            f"--assembly={self.assembly}",
            f"--group={self.group}",
            "verify-attached-bugs",
        ]
        if verify_flaws:
            cmd.append("--verify-flaws")
        if no_verify_blocking_bugs:
            cmd.append("--no-verify-blocking-bugs")
        async with self._elliott_lock:
            await exectools.cmd_assert_async(cmd, env=self._elliott_env_vars, stdout=sys.stderr)

    async def promote(
        self,
        assembly_type: AssemblyTypes,
        release_name: str,
        arches: List[str],
        previous_list: List[str],
        next_list: List[str],
        metadata: Optional[Dict],
        tag_stable: bool,
    ):
        """Promote all release payloads
        :param assembly_type: Assembly type
        :param release_name: Release name. e.g. 4.11.0-rc.6
        :param arches: List of architecture names. e.g. ["x86_64", "s390x"]. Don't use "multi" in this parameter.
        :param previous_list: upgrade edges that are used in `oc adm release new --previous`
        :param next_list: upgrade edges that are used in `oc adm release new --next`
        :param metadata: Payload metadata
        :param tag_stable: Whether to tag the promoted payload to "4-stable[-$arch]" release stream.
        :return: A dict. Keys are architecture name or "multi", values are release_info dicts.
        """
        tasks = OrderedDict()
        if not self.no_multi and self._multi_enabled:
            tasks["heterogeneous"] = self._promote_heterogeneous_payload(
                assembly_type, release_name, arches, previous_list, next_list, metadata, tag_stable=tag_stable
            )
        else:
            self._logger.warning("Multi/heterogeneous payload is disabled.")
        if not self.multi_only:
            tasks["homogeneous"] = self._promote_homogeneous_payloads(
                assembly_type, release_name, arches, previous_list, next_list, metadata, tag_stable=tag_stable
            )
        else:
            self._logger.warning(
                "Arch-specific homogeneous release payloads will not be promoted because --multi-only is set."
            )
        try:
            results = dict(zip(tasks.keys(), await asyncio.gather(*tasks.values())))
        except ChildProcessError as err:
            self._logger.error("Error promoting release images: %s\n%s", str(err), traceback.format_exc())
            raise
        return_value = {}
        if "homogeneous" in results:
            return_value.update(results["homogeneous"])
        if "heterogeneous" in results:
            return_value["multi"] = results["heterogeneous"]
        return return_value

    async def _promote_homogeneous_payloads(
        self,
        assembly_type: AssemblyTypes,
        release_name: str,
        arches: List[str],
        previous_list: List[str],
        next_list: List[str],
        metadata: Optional[Dict],
        tag_stable: bool,
    ):
        """Promote homogeneous payloads for specified architectures
        :param assembly_type: Assembly type
        :param release_name: Release name. e.g. 4.11.0-rc.6
        :param arches: List of architecture names. e.g. ["x86_64", "s390x"].
        :param previous_list: upgrade edges that are used in `oc adm release new --previous`
        :param next_list: upgrade edges that are used in `oc adm release new --next`
        :param metadata: Payload metadata
        :param reference_releases: A dict of reference release payloads to promote. Keys are architecture names, values are payload pullspecs
        :param tag_stable: Whether to tag the promoted payload to "4-stable[-$arch]" release stream.
        :return: A dict. Keys are architecture name, values are release_info dicts.
        """
        release_infos = []
        for arch in arches:
            result = await self._promote_arch(
                assembly_type, release_name, arch, previous_list, next_list, metadata, tag_stable=tag_stable
            )
            release_infos.append(result)
        return dict(zip(arches, release_infos))

    async def _promote_arch(
        self,
        assembly_type: AssemblyTypes,
        release_name: str,
        arch: str,
        previous_list: List[str],
        next_list: List[str],
        metadata: Optional[Dict],
        tag_stable: bool,
    ):
        """Promote an arch-specific homogeneous payload
        :param assembly_type: Assembly type
        :param release_name: Release name. e.g. 4.11.0-rc.6
        :param arch: Architecture name.
        :param previous_list: upgrade edges that are used in `oc adm release new --previous`
        :param next_list: upgrade edges that are used in `oc adm release new --next`
        :param metadata: Payload metadata
        :param reference_releases: A dict of reference release payloads to promote. Keys are architecture names, values are payload pullspecs
        :param tag_stable: Whether to tag the promoted payload to "4-stable[-$arch]" release stream.
        :return: A dict. Keys are architecture name, values are release_info dicts.
        """
        # This suffix will be used to construct imagestream name
        # We always want to promote from public imagestream and never private,
        # therefore is_private should always be set to False
        go_arch_suffix = go_suffix_for_arch(arch, is_private=False)

        brew_arch = brew_arch_for_go_arch(
            arch
        )  # ensure we are using Brew arches (e.g. aarch64) instead of golang arches (e.g. arm64).
        dest_image_tag = f"{release_name}-{brew_arch}"
        dest_image_pullspec = f"{self.DEST_RELEASE_IMAGE_REPO}:{dest_image_tag}"
        self._logger.info(
            "Checking if release image %s for %s (%s) already exists...", release_name, arch, dest_image_pullspec
        )
        dest_image_info = await get_release_image_info(dest_image_pullspec)
        if dest_image_info:  # this arch-specific release image is already promoted
            self._logger.warning(
                "Release image %s for %s (%s) already exists", release_name, arch, dest_image_info["image"]
            )
            # TODO: Check if the existing release image matches the assembly definition.

        if not dest_image_info or self.permit_overwrite:
            if dest_image_info:
                self._logger.warning("The existing release image %s will be overwritten!", dest_image_pullspec)
            major, minor = isolate_major_minor_in_group(self.group)
            # Ensure build-sync has been run for this assembly
            is_name = f"{major}.{minor}-art-assembly-{self.assembly}{go_arch_suffix}"
            imagestream = await self.get_image_stream(f"ocp{go_arch_suffix}", is_name)
            if not imagestream:
                raise ValueError(f"Image stream {is_name} is not found. Did you run build-sync?")
            self._logger.info(
                "Building arch-specific release image %s for %s (%s)...", release_name, arch, dest_image_pullspec
            )
            reference_pullspec = None
            source_image_stream = is_name
            await self.build_release_image(
                release_name,
                brew_arch,
                previous_list,
                next_list,
                metadata,
                dest_image_pullspec,
                reference_pullspec,
                source_image_stream,
                keep_manifest_list=False,
            )
            self._logger.info(
                "Release image for %s %s has been built and pushed to %s", release_name, arch, dest_image_pullspec
            )
            self._logger.info("Getting release image information for %s...", dest_image_pullspec)
            if not self.runtime.dry_run:
                dest_image_info = await get_release_image_info(dest_image_pullspec, raise_if_not_found=True)
            else:
                # populate fake data for dry run
                dest_image_info = {
                    "image": f"example.com/fake-release:{release_name}{brew_suffix_for_arch(arch)}",
                    "digest": f"fake:deadbeef{brew_suffix_for_arch(arch)}",
                    "metadata": {
                        "version": release_name,
                        "previous": previous_list,
                    },
                    "references": {
                        "spec": {
                            "tags": [
                                {
                                    "name": get_primary_container_name(self.group_runtime),
                                    "annotations": {"io.openshift.build.versions": "machine-os=00.00.212301010000-0"},
                                },
                            ],
                        },
                    },
                }
                major, minor = isolate_major_minor_in_group(self.group)
                go_arch_suffix = go_suffix_for_arch(arch, is_private=False)
                dest_image_info["references"]["metadata"] = {
                    "annotations": {
                        "release.openshift.io/from-image-stream": f"fake{go_arch_suffix}/{major}.{minor}-art-assembly-{self.assembly}{go_arch_suffix}"
                    }
                }

        if not tag_stable:
            self._logger.info("Release image %s will not appear on the release controller.", dest_image_pullspec)
            return dest_image_info

        namespace = f"ocp{go_arch_suffix}"
        image_stream_name = self._get_image_stream_name(assembly_type, arch)
        image_stream_tag = f"{image_stream_name}:{release_name}"
        namespace_image_stream_tag = f"{namespace}/{image_stream_tag}"
        self._logger.info("Checking if ImageStreamTag %s exists...", namespace_image_stream_tag)
        ist = await self.get_image_stream_tag(namespace, image_stream_tag)
        if ist:
            ist_digest = ist["image"]["dockerImageReference"].split("@")[-1]
            if ist_digest == dest_image_info["digest"]:
                self._logger.info(
                    "ImageStreamTag %s already exists with digest %s matching release image %s.",
                    namespace_image_stream_tag,
                    ist_digest,
                    dest_image_pullspec,
                )
                return dest_image_info
            message = f"ImageStreamTag {namespace_image_stream_tag} already exists, but it has a different digest ({ist_digest}) from the expected release image {dest_image_pullspec} ({dest_image_info['digest']})."
            if not self.permit_overwrite:
                raise ValueError(message)
            self._logger.warning(message)
        else:
            self._logger.info("ImageStreamTag %s doesn't exist.", namespace_image_stream_tag)

        self._logger.info("Tagging release image %s into %s...", dest_image_pullspec, namespace_image_stream_tag)
        await self.tag_release(dest_image_pullspec, namespace_image_stream_tag)
        self._logger.info("Release image %s has been tagged into %s.", dest_image_pullspec, namespace_image_stream_tag)
        return dest_image_info

    async def _promote_heterogeneous_payload(
        self,
        assembly_type: AssemblyTypes,
        release_name: str,
        include_arches: List[str],
        previous_list: List[str],
        next_list: List[str],
        metadata: Optional[Dict],
        tag_stable: bool,
    ):
        """Promote heterogeneous payload.
        The heterogeneous payload itself is a manifest list, which include references to arch-specific heterogeneous payloads.
        :param assembly_type: Assembly type
        :param release_name: Release name. e.g. 4.11.0-rc.6
        :param include_arches: List of architecture names.
        :param previous_list: upgrade edges that are used in `oc adm release new --previous`
        :param next_list: upgrade edges that are used in `oc adm release new --next`
        :param metadata: Payload metadata
        :param tag_stable: Whether to tag the promoted payload to "4-stable[-$arch]" release stream.
        :return: A dict. Keys are architecture name, values are release_info dicts.
        """
        dest_image_tag = f"{release_name}-multi"
        dest_image_pullspec = f"{self.DEST_RELEASE_IMAGE_REPO}:{dest_image_tag}"
        self._logger.info("Checking if multi/heterogeneous payload %s exists...", dest_image_pullspec)
        dest_image_digest = await self.get_multi_image_digest(dest_image_pullspec)
        if dest_image_digest:  # already promoted
            self._logger.warning(
                "Multi/heterogeneous payload %s already exists; digest: %s", dest_image_pullspec, dest_image_digest
            )
            dest_manifest_list = await self.get_image_info(dest_image_pullspec, raise_if_not_found=True)

        if self.use_multi_hack:
            # Add '-multi' to heterogeneous payload name.
            # This is to workaround a cincinnati issue discussed in #incident-cincinnati-sha-mismatch-for-multi-images
            # and prevent the heterogeneous payload from getting into Cincinnati channels.
            # e.g.
            #   "4.11.0-rc.6" => "4.11.0-multi-rc.6"
            #   "4.11.0" => "4.11.0-multi"
            parsed_version = VersionInfo.parse(release_name)
            parsed_version = parsed_version.replace(
                prerelease=f"multi-{parsed_version.prerelease}" if parsed_version.prerelease else "multi"
            )
            release_name = str(parsed_version)
            # No previous list is required until we get rid of the "having `-multi` string in the release name" workaround
            previous_list = []

        if not dest_image_digest or self.permit_overwrite:
            if dest_image_digest:
                self._logger.warning("The existing payload %s will be overwritten!", dest_image_pullspec)
            major, minor = isolate_major_minor_in_group(self.group)
            # The imagestream for the assembly in ocp-multi contains a single tag.
            # That single istag points to a top-level manifest-list on quay.io.
            # Each entry in the manifest-list is an arch-specific heterogeneous payload.
            # We need to fetch that manifest-list and recreate all arch-specific heterogeneous payloads first,
            # then recreate the top-level manifest-list.
            multi_is_name = f"{major}.{minor}-art-assembly-{self.assembly}-multi"
            multi_is = await self.get_image_stream("ocp-multi", multi_is_name)
            if not multi_is:
                raise ValueError(f"Image stream {multi_is_name} is not found. Did you run build-sync?")
            if len(multi_is["spec"]["tags"]) != 1:
                raise ValueError(
                    f"Image stream {multi_is_name} should only contain a single tag; Found {len(multi_is['spec']['tags'])} tags"
                )
            multi_ist = multi_is["spec"]["tags"][0]
            source_manifest_list = await self.get_image_info(multi_ist["from"]["name"], raise_if_not_found=True)
            if source_manifest_list["mediaType"] != "application/vnd.docker.distribution.manifest.list.v2+json":
                raise ValueError(f'Pullspec {multi_ist["from"]["name"]} doesn\'t point to a valid manifest list.')
            source_repo = (
                multi_ist["from"]["name"].rsplit(':', 1)[0].rsplit('@', 1)[0]
            )  # quay.io/openshift-release-dev/ocp-release@sha256:deadbeef -> quay.io/openshift-release-dev/ocp-release
            # dest_manifest_list is the final top-level manifest-list
            dest_manifest_list = {
                "image": dest_image_pullspec,
                "manifests": [],
            }
            build_tasks = []
            for manifest in source_manifest_list["manifests"]:
                os = manifest["platform"]["os"]
                arch = manifest["platform"]["architecture"]
                brew_arch = brew_arch_for_go_arch(arch)
                if os != "linux" or brew_arch not in include_arches:
                    self._logger.warning(f"Skipping {os}/{arch} in manifest_list {source_manifest_list}")
                    continue
                arch_payload_source = f"{source_repo}@{manifest['digest']}"
                arch_payload_dest = f"{dest_image_pullspec}-{brew_arch}"
                # Add an entry to the top-level manifest list
                dest_manifest_list["manifests"].append(
                    {
                        'image': arch_payload_dest,
                        'platform': {
                            'os': 'linux',
                            'architecture': arch,
                        },
                    }
                )
                # Add task to build arch-specific heterogeneous payload
                metadata = metadata.copy() if metadata else {}
                metadata['release.openshift.io/architecture'] = 'multi'
                build_tasks.append(
                    self.build_release_image(
                        release_name,
                        brew_arch,
                        previous_list,
                        next_list,
                        metadata,
                        arch_payload_dest,
                        arch_payload_source,
                        None,
                        keep_manifest_list=True,
                    )
                )

            # Build and push all arch-specific heterogeneous payloads
            self._logger.info("Building arch-specific heterogeneous payloads for %s...", include_arches)
            await asyncio.gather(*build_tasks)

            # Push the top level manifest list
            self._logger.info("Pushing manifest list...")
            await self.push_manifest_list(release_name, dest_manifest_list)
            self._logger.info(
                "Heterogeneous release payload for %s has been built. Manifest list pullspec is %s",
                release_name,
                dest_image_pullspec,
            )

            # Get info of the pushed manifest list
            self._logger.info("Getting release image information for %s...", dest_image_pullspec)
            if self.runtime.dry_run:
                dest_image_digest = "fake:deadbeef-multi"
                dest_manifest_list = dest_manifest_list.copy()
            else:
                dest_image_digest = await self.get_multi_image_digest(dest_image_pullspec, raise_if_not_found=True)
                dest_manifest_list = await self.get_image_info(dest_image_pullspec, raise_if_not_found=True)

        dest_image_info = dest_manifest_list.copy()
        dest_image_info["image"] = dest_image_pullspec
        dest_image_info["digest"] = dest_image_digest
        dest_image_info["metadata"] = {
            "version": release_name,
        }
        if not tag_stable:
            self._logger.info("Release image %s will not appear on the release controller.", dest_image_pullspec)
            return dest_image_info

        # Check if the heterogeneous release payload is already tagged into the image stream.
        namespace = "ocp-multi"
        image_stream_name = self._get_image_stream_name(assembly_type, "multi")
        image_stream_tag = f"{image_stream_name}:{release_name}"
        namespace_image_stream_tag = f"{namespace}/{image_stream_tag}"
        self._logger.info("Checking if ImageStreamTag %s exists...", namespace_image_stream_tag)
        ist = await self.get_image_stream_tag(namespace, image_stream_tag)
        if ist:
            ist_pullspec = ist["tag"]["from"]["name"]
            if ist_pullspec == dest_image_pullspec:
                self._logger.info(
                    "ImageStreamTag %s already exists and points to %s.",
                    namespace_image_stream_tag,
                    dest_image_pullspec,
                )
                return dest_image_info
            message = f"ImageStreamTag {namespace_image_stream_tag} already exists, but it points to {ist_pullspec} instead of {dest_image_pullspec}"
            if not self.permit_overwrite:
                raise ValueError(message)
            self._logger.warning(message)
        else:
            self._logger.info("ImageStreamTag %s doesn't exist.", namespace_image_stream_tag)

        self._logger.info("Tagging release image %s into %s...", dest_image_pullspec, namespace_image_stream_tag)
        await self.tag_release(dest_image_pullspec, namespace_image_stream_tag)
        self._logger.info("Release image %s has been tagged into %s.", dest_image_pullspec, namespace_image_stream_tag)
        return dest_image_info

    async def push_manifest_list(self, release_name: str, dest_manifest_list: Dict):
        dest_manifest_list_path = self._working_dir / f"{release_name}.manifest-list.yaml"
        with dest_manifest_list_path.open("w") as ml:
            yaml.dump(dest_manifest_list, ml)

        await manifest_tool(["push", "from-spec", "--", f"{dest_manifest_list_path}"], self.runtime.dry_run)
        auth_opt = ""
        if os.environ.get("XDG_RUNTIME_DIR"):
            auth_file = os.path.expandvars("${XDG_RUNTIME_DIR}/containers/auth.json")
            if Path(auth_file).is_file():
                auth_opt = f"--docker-cfg={auth_file}"

        cmd = [
            "manifest-tool",
            auth_opt,
            "push",
            "from-spec",
            "--",
            f"{dest_manifest_list_path}",
        ]

        if self.runtime.dry_run:
            self._logger.warning("[DRY RUN] Would have run %s", cmd)
            return
        env = os.environ.copy()
        await exectools.cmd_assert_async(cmd, env=env, stdout=sys.stderr)

    async def build_release_image(
        self,
        release_name: str,
        arch: str,
        previous_list: List[str],
        next_list: List[str],
        metadata: Optional[Dict],
        dest_image_pullspec: str,
        source_image_pullspec: Optional[str],
        source_image_stream: Optional[str],
        keep_manifest_list: bool,
    ):
        if bool(source_image_pullspec) + bool(source_image_stream) != 1:
            raise ValueError("Specify one of source_image_pullspec or source_image_stream")
        go_arch_suffix = go_suffix_for_arch(arch, is_private=False)
        cmd = [
            "oc",
            "adm",
            "release",
            "new",
            "-n",
            f"ocp{go_arch_suffix}",
            f"--name={release_name}",
            f"--to-image={dest_image_pullspec}",
        ]
        if self.runtime.dry_run:
            cmd.append("--dry-run")
        if source_image_pullspec:
            cmd.append(f"--from-release={source_image_pullspec}")
        if source_image_stream:
            cmd.extend(["--reference-mode=source", f"--from-image-stream={source_image_stream}"])
        if keep_manifest_list:
            cmd.append("--keep-manifest-list")

        if previous_list:
            cmd.append(f"--previous={','.join(previous_list)}")
        if next_list:
            cmd.append(f"--next={','.join(next_list)}")
        if metadata:
            cmd.append("--metadata")
            cmd.append(json.dumps(metadata))
        env = os.environ.copy()
        env["GOTRACEBACK"] = "all"
        self._logger.info("Running %s", " ".join(cmd))
        return await retry(
            reraise=True,
            stop=stop_after_attempt(10),  # retry 10 times
            wait=wait_fixed(30),  # wait for 30 seconds between retries
        )(exectools.cmd_gather_async)(cmd, env=env)

    @staticmethod
    async def get_image_stream(namespace: str, image_stream: str):
        cmd = [
            "oc",
            "-n",
            namespace,
            "get",
            "imagestream",
            "-o",
            "json",
            "--ignore-not-found",
            "--",
            image_stream,
        ]
        env = os.environ.copy()
        env["GOTRACEBACK"] = "all"
        _, stdout, _ = await exectools.cmd_gather_async(cmd, env=env)
        stdout = stdout.strip()
        if not stdout:  # Not found
            return None
        return json.loads(stdout)

    @staticmethod
    async def get_image_info(pullspec: str, raise_if_not_found: bool = False):
        # Get image manifest/manifest-list.
        cmd = f'oc image info --show-multiarch -o json {pullspec}'
        env = os.environ.copy()
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False, env=env)
        if rc != 0:
            if "not found: manifest unknown" in stderr or "was deleted or has expired" in stderr:
                # image doesn't exist
                if raise_if_not_found:
                    raise IOError(f"Image {pullspec} is not found.")
                return None
            raise ChildProcessError(f"Error running {cmd}: exit_code={rc}, stdout={stdout}, stderr={stderr}")

        # Info provided by oc need to be converted back into Skopeo-looking format
        info = json.loads(stdout)
        if not isinstance(info, list):
            raise ValueError(f"Invalid image info: {info}")

        media_types = set([manifest['mediaType'] for manifest in info])
        if len(media_types) > 1:
            raise ValueError(f'Inconsistent media types across manifests: {media_types}')

        manifests = {
            'mediaType': "application/vnd.docker.distribution.manifest.list.v2+json",
            'manifests': [
                {
                    'digest': manifest['digest'],
                    'platform': {
                        'architecture': manifest['config']['architecture'],
                        'os': manifest['config']['os'],
                    },
                }
                for manifest in info
            ],
        }

        return manifests

    @staticmethod
    async def get_multi_image_digest(pullspec: str, raise_if_not_found: bool = False):
        # Get image digest
        cmd = f'oc image info {pullspec} --filter-by-os linux/amd64 -o json'
        env = os.environ.copy()
        rc, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False, env=env)

        if rc != 0:
            if "manifest unknown" in stderr or "was deleted or has expired" in stderr:
                # image doesn't exist
                if raise_if_not_found:
                    raise IOError(f"Image {pullspec} is not found.")
                return None
            raise ChildProcessError(f"Error running {cmd}: exit_code={rc}, stdout={stdout}, stderr={stderr}")

        return json.loads(stdout)['listDigest']

    @staticmethod
    async def get_image_stream_tag(namespace: str, image_stream_tag: str):
        cmd = [
            "oc",
            "-n",
            namespace,
            "get",
            "imagestreamtag",
            "-o",
            "json",
            "--ignore-not-found",
            "--",
            image_stream_tag,
        ]
        env = os.environ.copy()
        env["GOTRACEBACK"] = "all"
        _, stdout, _ = await exectools.cmd_gather_async(cmd, env=env, stderr=None)
        stdout = stdout.strip()
        if not stdout:  # Not found
            return None
        return json.loads(stdout)

    async def tag_release(self, image_pullspec: str, image_stream_tag: str):
        cmd = [
            "oc",
            "tag",
            "--import-mode=PreserveOriginal",
            "--",
            image_pullspec,
            image_stream_tag,
        ]
        if self.runtime.dry_run:
            self._logger.warning("[DRY RUN] Would have run %s", cmd)
            return
        env = os.environ.copy()
        env["GOTRACEBACK"] = "all"
        await exectools.cmd_assert_async(cmd, env=env, stdout=sys.stderr)

    async def is_accepted(self, release_name: str, arch: str, release_stream: str):
        go_arch = go_arch_for_brew_arch(arch)
        release_controller_url = f"https://{go_arch}.ocp.releases.ci.openshift.org"
        phase = await self.get_release_phase(release_controller_url, release_stream, release_name)
        return phase == "Accepted"

    async def wait_for_stable(self, release_name: str, arch: str, release_stream: str):
        go_arch = go_arch_for_brew_arch(arch)
        release_controller_url = f"https://{go_arch}.ocp.releases.ci.openshift.org"
        if self.runtime.dry_run:
            actual_phase = await self.get_release_phase(release_controller_url, release_stream, release_name)
            self._logger.warning(
                "[DRY RUN] Release %s for %s has phase %s. Assume accepted.", release_name, arch, actual_phase
            )
            return

        def _my_before_sleep(retry_state: RetryCallState):
            if retry_state.outcome.failed:
                err = retry_state.outcome.exception()
                self._logger.warning(
                    'Error communicating with %s release controller. Will check again in %s seconds. %s: %s',
                    arch,
                    retry_state.next_action.sleep,
                    type(err).__name__,
                    err,
                )
            else:
                self._logger.log(
                    logging.INFO if retry_state.attempt_number < 1 else logging.WARNING,
                    'Release payload for "%s" arch is in the "%s" phase. Will check again in %s seconds.',
                    arch,
                    retry_state.outcome.result(),
                    retry_state.next_action.sleep,
                )

        return await retry(
            stop=(stop_after_attempt(144)),  # wait for 10m * 144 = 1440m = 24 hours
            wait=wait_fixed(600),  # wait for 10 minutes between retries
            retry=(retry_if_result(lambda phase: phase != "Accepted") | retry_if_exception_type()),
            before_sleep=_my_before_sleep,
        )(self.get_release_phase)(release_controller_url, release_stream, release_name)

    @staticmethod
    async def get_release_phase(release_controller_url: str, release_stream: str, release_name: str):
        api_path = f"/api/v1/releasestream/{quote(release_stream)}/release/{quote(release_name)}"
        full_url = f"{release_controller_url}{api_path}"
        async with aiohttp.ClientSession() as session:
            async with session.get(full_url) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                release_info = await response.json()
                return release_info.get("phase")

    async def get_advisory_image_list(self, image_advisory: int):
        cmd = [
            "elliott",
            "advisory-images",
            "-a",
            f"{image_advisory}",
        ]
        async with self._elliott_lock:
            _, stdout, _ = await exectools.cmd_gather_async(cmd, env=self._elliott_env_vars, stderr=None)
        return stdout

    async def send_image_list_email(self, release_name: str, advisory: int, archive_dir: Path):
        content = await self.get_advisory_image_list(advisory)
        subject = f"OCP {release_name} Image List"
        return await to_thread(
            self._mail.send_mail,
            self.runtime.config["email"]["promote_image_list_recipients"],
            subject,
            content,
            archive_dir=archive_dir,
            dry_run=self.runtime.dry_run,
        )

    def handle_qe_notification(self, release_jira: str, release_name: str, impetus_advisories: Dict[str, int]):
        """
        Send a notification email to QEs if it hasn't been done yet
        Update QE release repo with release info if hasn't been done yet
        """

        if not release_jira:
            return

        parent_jira = self._jira_client.get_issue(release_jira)
        self._logger.info("Sending a notification to QE and multi-arch QE...")
        jira_issue_link = parent_jira.permalink()
        self._send_release_email(release_name, impetus_advisories, jira_issue_link)
        self._logger.info("Update QE's release tests repo...")
        self._update_qe_repo(release_name, release_jira, impetus_advisories)

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    def _update_qe_repo(self, release_name: str, release_jira: str, advisories: Dict[str, int]):
        github_client = Github(os.environ.get("GITHUB_TOKEN"))
        upstream_repo = github_client.get_repo("openshift/release-tests")
        fork_repo = github_client.get_repo("openshift-bot/release-tests")
        update_message = f"Add release {release_name}"
        major, minor = isolate_major_minor_in_group(self.group)
        file_path = f"_releases/{major}.{minor}/{major}.{minor}.z.yaml"
        self._logger.info("Updating QE release repo")
        # create branch
        for branch in fork_repo.get_branches():
            if branch.name == release_name:
                fork_repo.get_git_ref(f"heads/{release_name}").delete()
        fork_branch = fork_repo.create_git_ref(
            f"refs/heads/{release_name}", upstream_repo.get_branch("z-stream").commit.sha
        )
        self._logger.info("Created fork branch ref %s", fork_branch.ref)
        # get release file content
        try:
            release_content = upstream_repo.get_contents(file_path, ref="z-stream")
            file_content = yaml.load(release_content.decoded_content)
            file_content['releases'][release_name] = {'advisories': advisories, 'release_jira': release_jira}
        except ParserError:
            self._logger.warning("release file not in valid yaml format, overwrite with new value")
            file_content = {'releases': {}}
            file_content['releases'][release_name] = {'advisories': advisories, 'release_jira': release_jira}
        except GithubException:
            self._logger.warning("release file not found in upstream repo, skip update qe repo")
            return
        # update release file
        output = io.BytesIO()
        yaml.dump(file_content, output)
        output.seek(0)
        fork_file = fork_repo.get_contents(file_path, ref=release_name)
        fork_repo.update_file(file_path, update_message, output.read(), fork_file.sha, branch=release_name)
        # create pr
        try:
            pr = upstream_repo.create_pull(
                title=update_message, body=update_message, base="z-stream", head=f"openshift-bot:{release_name}"
            )
            pr.add_to_labels("lgtm", "approved")
            pr.merge()
            self._logger.info(f"PR {pr.html_url} merged into qe repo")
        except GithubException as e:
            self._logger.warning(f"Failed to update upstream repo: {e}")

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
    def _send_release_email(self, release_name: str, advisories: Dict[str, int], jira_link: str):
        subject = f"OCP {release_name} advisories and nightlies"
        content = f"This is the current set of advisories for {release_name}:\n"
        for impetus, advisory in advisories.items():
            content += f"- {impetus}: https://errata.devel.redhat.com/advisory/{advisory}\n"
        if 'microshift' in advisories.keys():
            content += (
                "\n Note: Microshift advisory gets populated with build and bugs after the release payload has "
                "been promoted on Release Controller. It will take a few hours for it to be ready and on QE."
            )

        content += (
            "\nThe nightlies used as reference for this release can be found in openshift-eng/ocp-build-data "
            "releases.yml file (in the corresponding release branch)\n"
        )
        content += (
            f"Its definition is provided by the assembly found under key '{self.assembly}' in "
            f"{constants.OCP_BUILD_DATA_URL}/blob/{self.group}/releases.yml\n"
        )
        content += f"\nJIRA ticket: {jira_link}\n"
        content += f"\nAdvisory dashboard: https://art-dash.engineering.redhat.com/dashboard/release/{self.group} \n"
        content += "\nThanks.\n"
        release_version = tuple(map(int, release_name.split(".", 2)))
        email_dir = self._working_dir.absolute() / "email"
        mail = MailService.from_config(self.runtime.config)
        mail.send_mail(
            self.runtime.config["email"][f"qe_notification_recipients_ocp{release_version[0]}"],
            subject,
            content,
            archive_dir=email_dir,
            dry_run=self.runtime.dry_run,
        )

    async def create_cincinnati_prs(self, assembly_type, release_info):
        """Create Cincinnati PRs for the release."""
        candidate_pr_note = ""
        justifications = release_info["justifications"]
        if justifications:
            candidate_pr_note = "\n".join(justifications)

        from_releases = [
            arch_info["from_release"].split(":")[-1]
            for arch_info in release_info["content"].values()
            if "from_release" in arch_info
        ]
        advisory_id = 0
        if "advisory" in release_info and release_info["advisory"]:
            advisory_id = release_info["advisory"]

        release_name = release_info["name"]
        branchName = f"pr_candidate_{release_name}"
        pr_title = f"Enable {release_name} in candidate channel"
        extraSlackComment = ""
        if advisory_id != 0:
            internal_errata_url = f"https://errata.devel.redhat.com/advisory/{advisory_id}"
            pr_messages = (
                f"Please merge immediately. This PR does not need to wait for an advisory to ship, but the associated advisory is {internal_errata_url} ."
                + candidate_pr_note
            )
            extraSlackComment = "automatically approved"
        elif assembly_type in [AssemblyTypes.PREVIEW, AssemblyTypes.CANDIDATE]:
            pr_messages = (
                "This is a release candidate. There is no advisory associated. \nPlease merge immediately."
                + candidate_pr_note
            )
        else:
            pr_messages = "Promoting a hotfix release (e.g. for a single customer). There is no advisory associated. \nPlease merge immediately."

        # create a forked branch
        github_client = Github(os.environ.get("GITHUB_TOKEN"))
        upstream_repo = github_client.get_repo("openshift/cincinnati-graph-data")
        for branch in upstream_repo.get_branches():
            if branch.name == branchName:
                upstream_repo.get_git_ref(f"heads/{branchName}").delete()
        fork_branch = upstream_repo.create_git_ref(
            f"refs/heads/{branchName}", upstream_repo.get_branch("master").commit.sha
        )
        self._logger.info("Created fork branch ref %s", fork_branch.ref)
        # edit channel file
        candidate_content = upstream_repo.get_contents("internal-channels/candidate.yaml", ref=branchName)
        file_content = candidate_content.decoded_content.decode("utf-8") + f"\n- {release_name}"
        upstream_repo.update_file(
            "internal-channels/candidate.yaml", pr_title, file_content, candidate_content.sha, branch=branchName
        )
        try:
            pr = upstream_repo.create_pull(title=pr_title, body=pr_messages, base="master", head=branchName)
            pr.add_to_labels("lgtm", "approved")
            self._logger.info(f"Cincinnati PR {pr.html_url} created")
        except GithubException as e:
            self._logger.warning(f"Failed to update upstream repo: {e}")
            raise ValueError(f"Failed to update upstream repo: {e}")

        if not self.skip_ota_notification:
            new_slackclient = self.runtime.new_slack_client()
            new_slackclient.bind_channel("#forum-ocp-release")
            slack_msg = f"ART has opened Cincinnati PRs for {release_name}:\n"
            if from_releases:
                slack_msg += "This release was promoted using nightly\n"
                for nightly in from_releases:
                    slack_msg += f"registry.ci.openshift.org/ocp/release:{nightly}\n"
            slack_msg += f"{pr.html_url}\n" + extraSlackComment
            await new_slackclient.say_in_thread(slack_msg)

    async def ocp_doomsday_backup(self):
        """
        Backup the payload to ocp-doomsday-registry s3 bucket on AWS. This function will trigger a pipeline on
        ART cluster

        :param major: Eg. 4.15
        :param version: Eg. 4.15.10
        """
        pipeline_name = "doomsday-pipeline"
        cmd = (
            f"tkn pipeline start {pipeline_name} "
            f"--kubeconfig {os.environ['ART_CLUSTER_ART_CD_PIPELINE_KUBECONFIG']} "
            f"--param major={self.group.split('-')[-1]} "
            f"--param version={self.assembly} "
            "--pipeline-timeout 4h"
        )

        env = os.environ.copy()
        rc, _, _ = await exectools.cmd_gather_async(cmd, env=env)

        if rc == 0:
            self._logger.info("Successfully triggered ocp-doomsday-registry pipline on cluster")
        else:
            self._logger.error("Error while triggering ocp-doomsday-registry pipline on cluster")

    async def update_shipment_with_payload_shas(self, shipment_url: str, payload_shas: Dict[str, str]):
        """Update shipment MR with payload SHA digests from successful promote job.

        :param shipment_url: The URL of the existing shipment MR to update
        :param payload_shas: Dict mapping architecture names to their SHA256 digests
        """
        self._logger.info("Updating shipment MR with payload SHAs: %s", shipment_url)

        gitlab_token = os.getenv("GITLAB_TOKEN")
        if not gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required for updating shipment MR")

        # Parse the shipment URL to extract project and MR details
        parsed_url = urlparse(shipment_url)
        target_project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
        mr_id = parsed_url.path.split('/')[-1]
        gitlab_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        # Connect to GitLab
        gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token)
        gl.auth()

        # Load the existing MR
        project = gl.projects.get(target_project_path)
        mr = project.mergerequests.get(mr_id)
        source_project = gl.projects.get(mr.source_project_id)

        # Load shipment configs from MR
        shipments_by_kind = get_shipment_configs_from_mr(shipment_url)

        # Collect all file changes before creating a single MR
        files_to_update = {}  # file_path -> updated_content
        templates_replaced_total = 0

        # Process all shipment types (except FBC) for template replacement
        for shipment_kind, shipment_config in shipments_by_kind.items():
            if shipment_kind == "fbc":
                continue  # Skip FBC shipments

            self._logger.info("Processing %s shipment for template replacement", shipment_kind)

            # Check for template updates for this shipment
            file_path, updated_content, templates_replaced = await self._prepare_shipment_templates(
                shipment_kind, shipment_config, payload_shas, mr, source_project, shipments_by_kind
            )

            if file_path and updated_content and templates_replaced > 0:
                files_to_update[file_path] = updated_content
                templates_replaced_total += templates_replaced
                self._logger.info(
                    "Prepared %d template replacements for %s shipment", templates_replaced, shipment_kind
                )
            else:
                self._logger.info("No template replacements needed for %s shipment", shipment_kind)

        # Create a single MR with all the file updates
        if files_to_update:
            await self._create_consolidated_shipment_update_mr(
                mr, source_project, files_to_update, templates_replaced_total
            )
        else:
            self._logger.info("No template replacements needed for any shipment files")

    async def _prepare_shipment_templates(
        self, shipment_kind: str, shipment_config, payload_shas: Dict[str, str], mr, source_project, shipments_by_kind
    ):
        """Prepare template replacements for a single shipment and return file path, content, and replacement count"""
        # Build format dictionary for template replacement
        format_dict = {}

        # Add SHA digests for image shipments
        if shipment_kind == "image":
            for arch, sha in payload_shas.items():
                if arch == "multi":
                    continue  # Skip multi-arch as it's not a specific architecture

                # Map architecture names to format variables
                if arch == "x86_64":
                    format_dict["x864_DIGEST"] = sha
                elif arch == "s390x":
                    format_dict["s390x_DIGEST"] = sha
                elif arch == "ppc64le":
                    format_dict["ppc64le_DIGEST"] = sha
                elif arch == "aarch64":
                    format_dict["aarch64_DIGEST"] = sha
                else:
                    self._logger.warning("Unknown architecture %s, skipping template replacement", arch)
                    continue

                self._logger.info("Prepared format variable: %s -> %s", arch, sha)

        # Generate IMAGE_ADVISORY template key for all shipment types (get from image shipment data)
        try:
            image_shipment = shipments_by_kind.get("image")

            if (
                image_shipment
                and hasattr(image_shipment.shipment.data.releaseNotes, 'type')
                and hasattr(image_shipment.shipment.data.releaseNotes, 'live_id')
                and image_shipment.shipment.data.releaseNotes.type
                and image_shipment.shipment.data.releaseNotes.live_id
            ):
                advisory_type = image_shipment.shipment.data.releaseNotes.type
                live_id = image_shipment.shipment.data.releaseNotes.live_id
                year = datetime.now().strftime("%Y")
                image_advisory = f"{advisory_type}-{year}:{live_id}"
                format_dict["IMAGE_ADVISORY"] = image_advisory
                self._logger.info("Generated IMAGE_ADVISORY: %s", image_advisory)
            else:
                self._logger.warning("Cannot generate IMAGE_ADVISORY: missing image shipment or type/live_id data")
        except Exception as ex:
            self._logger.warning("Failed to generate IMAGE_ADVISORY: %s", ex)

        # Generate RPM_ADVISORY template key for all shipment types
        try:
            rpm_advisory = await self._get_rpm_advisory()
            if rpm_advisory:
                format_dict["RPM_ADVISORY"] = rpm_advisory
                self._logger.info("Generated RPM_ADVISORY: %s", rpm_advisory)
            else:
                # Don't replace RPM_ADVISORY placeholder if we can't generate it
                self._logger.warning("Could not generate RPM_ADVISORY: no RPM advisory found in releases.yml")
        except Exception as ex:
            # Don't replace RPM_ADVISORY placeholder on error
            self._logger.warning("Failed to generate RPM_ADVISORY: %s", ex)

        # If no replacements are needed, skip this shipment
        if not format_dict:
            self._logger.info("No template replacements needed for %s shipment", shipment_kind)
            return None, None, 0

        # Check if the shipment file actually contains any of our placeholders
        # Find the shipment file first to check its content
        diff_info = mr.diffs.list(all=True)[0]
        diff = mr.diffs.get(diff_info.id)
        shipment_file_path = None
        for file_diff in diff.diffs:
            file_path = file_diff.get('new_path') or file_diff.get('old_path')
            if file_path and file_path.endswith(('.yaml', '.yml')) and shipment_kind in file_path:
                shipment_file_path = file_path
                break

        if not shipment_file_path:
            self._logger.warning("Could not find %s shipment file in MR", shipment_kind)
            return None, None, 0

        # Get original file content to check for placeholders
        original_file = source_project.files.get(shipment_file_path, mr.source_branch)
        original_content = original_file.decode().decode('utf-8')

        # Check if any of our placeholders are actually present in the file
        placeholders_found = []
        for var_name in format_dict.keys():
            placeholder = f"{{{var_name}}}"
            if placeholder in original_content:
                placeholders_found.append(placeholder)

        if not placeholders_found:
            self._logger.info("No relevant template placeholders found in %s shipment file", shipment_kind)
            return None, None, 0

        self._logger.info("Found template placeholders in %s shipment: %s", shipment_kind, placeholders_found)

        # Validate template placeholders in both description and solution fields (for image shipments)
        if shipment_kind == "image":
            # Check solution field for SHA digest placeholders
            if hasattr(shipment_config.shipment.data.releaseNotes, 'solution'):
                solution_text = shipment_config.shipment.data.releaseNotes.solution
                if solution_text:
                    # Check for SHA digest placeholders that have no corresponding replacement
                    placeholder_pattern = r'\{([^}]+)\}'
                    placeholders = re.findall(placeholder_pattern, solution_text)
                    for placeholder in placeholders:
                        if placeholder.endswith('_DIGEST') and placeholder not in format_dict:
                            raise ValueError(
                                f"Solution contains placeholder {{{placeholder}}} but no corresponding value was found"
                            )

            # Check description field for advisory placeholders
            if hasattr(shipment_config.shipment.data.releaseNotes, 'description'):
                description_text = shipment_config.shipment.data.releaseNotes.description
                if description_text:
                    # Check for advisory placeholders that have no corresponding replacement
                    placeholder_pattern = r'\{([^}]+)\}'
                    placeholders = re.findall(placeholder_pattern, description_text)
                    for placeholder in placeholders:
                        if placeholder in ["IMAGE_ADVISORY", "RPM_ADVISORY"] and placeholder not in format_dict:
                            # Allow advisory placeholders to remain if we can't generate them
                            pass

        # File content was already loaded above for placeholder checking

        # Replace template placeholders directly in the original content to preserve formatting
        # Advisory placeholders (IMAGE_ADVISORY, RPM_ADVISORY) go in description field
        # SHA digest placeholders (x864_DIGEST, etc.) go in solution field
        updated_content = original_content
        templates_replaced = 0

        for var_name, replacement_value in format_dict.items():
            placeholder = f"{{{var_name}}}"
            if placeholder in updated_content:
                # Log appropriate context based on placeholder type
                if var_name.endswith('_DIGEST'):
                    context = "solution"
                elif var_name in ["IMAGE_ADVISORY", "RPM_ADVISORY"]:
                    context = "description"
                else:
                    context = "content"

                updated_content = updated_content.replace(placeholder, replacement_value)
                templates_replaced += 1
                self._logger.info("Replaced %s with %s in %s field", placeholder, replacement_value, context)

        if templates_replaced > 0:
            self._logger.info(
                "Successfully replaced %d template placeholders in %s shipment", templates_replaced, shipment_kind
            )
            return shipment_file_path, updated_content, templates_replaced
        else:
            self._logger.info("No template placeholders found in %s shipment file", shipment_kind)
            return None, None, 0

    async def _create_consolidated_shipment_update_mr(
        self, mr, source_project, files_to_update: Dict[str, str], templates_replaced_total: int
    ):
        """Create a new MR to update all shipments with template replacements"""
        try:
            if self.runtime.dry_run:
                self._logger.info(
                    "[DRY RUN] Would have created MR to update shipment files with template replacements: %s",
                    list(files_to_update.keys()),
                )
                return None

            # Check if an MR with template updates already exists (reentrant check)
            sha_mr_title_pattern = f"Update {self.assembly} Docs release notes"
            existing_mrs = source_project.mergerequests.list(
                target_branch=mr.source_branch, state='opened', search=sha_mr_title_pattern
            )

            for existing_mr in existing_mrs:
                if existing_mr.title == sha_mr_title_pattern:
                    self._logger.info(
                        "Payload SHA update MR already exists: %s. Skipping creation.", existing_mr.web_url
                    )
                    await self._slack_client.say_in_thread(
                        f"Payload SHA update MR already exists: {existing_mr.web_url}"
                    )

                    # Check if comment about this MR already exists on main shipment MR
                    main_mr_comment = (
                        f"Docs team, please review the existing MR to update release notes: {existing_mr.web_url}"
                    )
                    existing_notes = mr.notes.list(all=True)
                    comment_exists = any(note.body == main_mr_comment for note in existing_notes)

                    if not comment_exists:
                        try:
                            mr.notes.create({'body': main_mr_comment})
                            self._logger.info("Added comment to main shipment MR")
                        except Exception as comment_ex:
                            self._logger.warning("Failed to comment on main shipment MR: %s", comment_ex)
                    else:
                        self._logger.info("Comment about payload SHA update MR already exists on main shipment MR")

                    return None

            # Create a new branch for the template updates
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            update_branch = f"update-shas-{self.assembly}-{timestamp}"

            # Create branch from the shipment MR's source branch
            source_project.branches.create({'branch': update_branch, 'ref': mr.source_branch})
            self._logger.info("Created template update branch: %s", update_branch)

            # Update all files in the new branch
            for file_path, updated_content in files_to_update.items():
                file_to_update = source_project.files.get(file_path, update_branch)
                file_to_update.content = updated_content
                file_to_update.save(
                    branch=update_branch,
                    commit_message=f"Update {file_path.split('/')[-1]} with payload SHAs for {self.assembly}",
                )
                self._logger.info("Updated file %s in branch %s", file_path, update_branch)

            # Create MR to merge template updates into the shipment MR branch
            update_mr_title = f"Update {self.assembly} Docs release notes"

            shipment_files = [f"- {file_path.split('/')[-1]}" for file_path in files_to_update.keys()]
            shipment_files_str = "\n".join(shipment_files)

            update_mr_description = f"""This MR updates shipment configurations with payload SHAs from the successful promote job.

**Release**: {self.assembly}
**Promote Job**: {os.environ.get('BUILD_URL', 'N/A')}
**Total Templates Replaced**: {templates_replaced_total}

**Updated Files:**
{shipment_files_str}

**Updated Templates:**
- Replaced SHA digest placeholders
- Updated advisory references

"""

            update_mr = source_project.mergerequests.create(
                {
                    'source_branch': update_branch,
                    'target_branch': mr.source_branch,
                    'title': update_mr_title,
                    'description': update_mr_description,
                    'remove_source_branch': True,
                }
            )

            update_mr_url = update_mr.web_url
            self._logger.info("Created consolidated template update MR: %s", update_mr_url)

            # Auto-merge Docs rel notes updates since the Docs team will review and approve the main Shipment MR
            try:
                # Wait a moment for GitLab to process the MR
                await asyncio.sleep(2)

                # Set auto-merge to merge when pipeline passes
                update_mr = source_project.mergerequests.get(update_mr.id, lazy=False)
                update_mr.merge(merge_when_pipeline_succeeds=True, should_remove_source_branch=True)
                self._logger.info("Successfully enabled auto-merge for template update MR: %s", update_mr_url)
                await self._slack_client.say_in_thread(
                    f"Enabled auto-merge for MR with payload SHAs (will merge when pipeline passes): {update_mr_url}"
                )
            except Exception as merge_ex:
                self._logger.warning("Failed to enable auto-merge for template update MR: %s", merge_ex)
                await self._slack_client.say_in_thread(
                    f"Created MR to update shipment with payload SHAs (auto-merge setup failed): {update_mr_url}"
                )

            # Comment on the main shipment MR to notify about the template update MR
            # Since pipeline may take time, always notify docs team about the auto-merge MR
            main_mr_comment = f"@hybrid-platforms/art/team-docs: Auto-merge has been enabled for payload SHA updates. The MR will automatically merge when the pipeline passes: {update_mr_url}"

            # Check if comment already exists to avoid duplicates
            existing_notes = mr.notes.list(all=True)
            comment_exists = any(main_mr_comment in note.body for note in existing_notes)

            if not comment_exists:
                try:
                    mr.notes.create({'body': main_mr_comment})
                    self._logger.info("Added comment to main shipment MR")
                except Exception as comment_ex:
                    self._logger.warning("Failed to comment on main shipment MR: %s", comment_ex)
            else:
                self._logger.info("Comment about template update MR already exists on main shipment MR")

        except Exception as ex:
            self._logger.error("Failed to create consolidated template update MR: %s", ex)
            raise

    async def _get_rpm_advisory(self) -> Optional[str]:
        """Get RPM advisory ID from releases.yml and query errata endpoint for advisory type.

        :return: Formatted RPM advisory string like "RHBA-2025:12345" or None if not found
        """
        try:
            # Load releases.yml config to get RPM advisory ID
            releases_config = await util.load_releases_config(
                group=self.group,
                data_path=self._doozer_env_vars.get("DOOZER_DATA_PATH", None) or constants.OCP_BUILD_DATA_URL,
            )

            # Debug: Show available releases in the config
            available_releases = list(releases_config.get("releases", {}).keys())
            self._logger.info("Available releases in releases.yml: %s", available_releases)

            # Get the assembly config from releases.yml
            assembly_config = releases_config.get("releases", {}).get(self.assembly)
            if not assembly_config:
                self._logger.warning("Assembly %s not found in releases.yml", self.assembly)
                return None

            self._logger.info("Assembly config keys for %s: %s", self.assembly, list(assembly_config.keys()))

            # Get RPM advisory ID from assembly.group.advisories path
            assembly_group_advisories = assembly_config.get("assembly", {}).get("group", {}).get("advisories", {})
            rpm_advisory_id = assembly_group_advisories.get("rpm")

            self._logger.info("Assembly group advisories for %s: %s", self.assembly, assembly_group_advisories)

            if not rpm_advisory_id:
                self._logger.info("No RPM advisory ID found for assembly %s", self.assembly)
                return None

            self._logger.info("Found RPM advisory ID %s for assembly %s", rpm_advisory_id, self.assembly)

            # Query errata endpoint to get advisory type
            errata_endpoint = "https://errata.devel.redhat.com/api/v1/erratum/{}"
            errata_url = urlparse(errata_endpoint.format(rpm_advisory_id)).geturl()

            self._logger.info("Querying errata API endpoint: %s", errata_url)

            # Make request to errata endpoint
            response = requests.get(
                errata_url,
                verify=ssl.get_default_verify_paths().openssl_cafile,
                auth=HTTPSPNEGOAuth(),
            )
            response.raise_for_status()

            advisory_data = response.json()

            # Extract advisory type and errata_id from response using similar logic to format_advisory_data
            advisory_type = None
            errata_id = None
            if "errata" in advisory_data:
                errata_data = advisory_data["errata"]
                # Get the first (and typically only) advisory type key
                for key in errata_data:
                    advisory_type = key
                    errata_id = errata_data[key].get("errata_id")
                    break

            if not advisory_type or not errata_id:
                self._logger.warning(
                    "Could not extract advisory type or errata_id from errata response for RPM advisory %s",
                    rpm_advisory_id,
                )
                return None

            # Generate the formatted advisory string: {advisory_type}-{year}:{errata_id}
            year = datetime.now().strftime("%Y")
            rpm_advisory = f"{advisory_type.upper()}-{year}:{errata_id}"

            return rpm_advisory

        except Exception as ex:
            self._logger.error("Error getting RPM advisory: %s", ex)
            return None


@cli.command("promote")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group of components on which to operate. e.g. openshift-4.9",
)
@click.option("--assembly", metavar="ASSEMBLY_NAME", required=True, help="The name of an assembly. e.g. 4.9.1")
@click.option(
    "--skip-blocker-bug-check",
    is_flag=True,
    help="Skip blocker bug check. Note block bugs are never checked for CUSTOM and CANDIDATE releases.",
)
@click.option(
    "--skip-attached-bug-check",
    is_flag=True,
    help="Skip attached bug check. Note attached bugs are never checked for CUSTOM and CANDIDATE releases.",
)
@click.option("--skip-image-list", is_flag=True, help="Do not gather an advisory image list for docs.")
@click.option("--skip-build-microshift", is_flag=True, help="Do not build microshift rpm")
@click.option("--skip-signing", is_flag=True, help="Do not sign artifacts (legacy signing)")
@click.option("--skip-sigstore", is_flag=True, help="Do not sign using the newer sigstore method.")
@click.option("--skip-cincinnati-prs", is_flag=True, help="Do not create Cincinnati PRs")
@click.option("--skip-ota-notification", is_flag=True, help="Do not send OTA notification on slack")
@click.option("--permit-overwrite", is_flag=True, help="DANGER! Allows the pipeline to overwrite an existing payload.")
@click.option("--no-multi", is_flag=True, help="Do not promote a multi-arch/heterogeneous payload.")
@click.option("--multi-only", is_flag=True, help="Do not promote arch-specific homogenous payloads.")
@click.option("--skip-mirror-binaries", is_flag=True, help="Do not mirror client binaries to mirror")
@click.option(
    "--use-multi-hack", is_flag=True, help="Add '-multi' to heterogeneous payload name to workaround a Cincinnati issue"
)
@click.option("--signing-env", type=click.Choice(("prod", "stage")), help="Signing server environment: prod or stage")
@pass_runtime
@click_coroutine
async def promote(
    runtime: Runtime,
    group: str,
    assembly: str,
    skip_blocker_bug_check: bool,
    skip_attached_bug_check: bool,
    skip_image_list: bool,
    skip_build_microshift: bool,
    skip_signing: bool,
    skip_sigstore: bool,
    skip_cincinnati_prs: bool,
    skip_ota_notification: bool,
    permit_overwrite: bool,
    no_multi: bool,
    multi_only: bool,
    skip_mirror_binaries: bool,
    use_multi_hack: bool,
    signing_env: Optional[str],
):
    pipeline = await PromotePipeline.create(
        runtime,
        group,
        assembly,
        skip_blocker_bug_check,
        skip_attached_bug_check,
        skip_image_list,
        skip_build_microshift,
        skip_signing,
        skip_sigstore,
        skip_cincinnati_prs,
        skip_ota_notification,
        permit_overwrite,
        no_multi,
        multi_only,
        skip_mirror_binaries,
        use_multi_hack,
        signing_env,
    )
    await pipeline.run()
