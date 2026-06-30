import asyncio
import json
import logging
import os
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import click
import yaml as stdlib_yaml
from artcommonlib import exectools
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.gitdata import SafeFormatter
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.gitlab import GitLabClient
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import (
    Data,
    Environments,
    Metadata,
    ReleaseNotes,
    Shipment,
    ShipmentConfig,
    ShipmentEnv,
    Snapshot,
    SnapshotSpec,
)
from elliottlib.util import extract_nvrs_from_fbc, get_advisory_boilerplate
from github import GithubException
from tenacity import retry, stop_after_attempt

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()


def _normalize_release_date(date_str: str) -> str:
    """Normalize a release date string to YYYY-Mon-DD format (e.g. 2026-Mar-31).

    Accepts both abbreviated month names (2026-Mar-31) and numeric months (2026-03-31).
    """
    for fmt in ("%Y-%b-%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%b-%d")
        except ValueError:
            continue
    raise click.ClickException(
        f"Invalid date format '{date_str}'. Use YYYY-Mon-DD (e.g. 2026-Mar-31) or YYYY-MM-DD (e.g. 2026-03-31)"
    )


class ReleaseFromFbcPipeline:
    """
    Pipeline for creating simplified shipment files from FBC images.

    This pipeline is designed for non-OpenShift products (e.g., OADP, MTA, MTC, logging)
    that need to create shipment files from FBC images. For OpenShift releases,
    use the PrepareReleaseKonfluxPipeline instead.

    When --ocp-optional is set, the pipeline operates in OCP optional-operator mode:
    it categorizes NVRs into extras/fbc kinds (matching the OCP shipment
    structure) instead of the default image/fbc split. All FBC related images are
    included by default, supporting the use case of shipping OCP optional operators
    that were excluded from the main release due to dependency version conflicts.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        fbc_pullspecs: List[str],
        create_mr: bool = False,
        shipment_data_repo_url: Optional[str] = None,
        shipment_path: Optional[str] = None,
        jira_bugs: Optional[List[str]] = None,
        target_release_date: Optional[str] = None,
        extra_image_nvrs: Optional[List[str]] = None,
        ocp_optional: bool = False,
        exclude_nvr_components: Optional[List[str]] = None,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.fbc_pullspecs = fbc_pullspecs
        self.extra_image_nvrs = extra_image_nvrs or []
        self.create_mr = create_mr
        self.dry_run = self.runtime.dry_run
        self.ocp_optional = ocp_optional
        self.excluded_components = set(exclude_nvr_components) if exclude_nvr_components else set()

        # Setup working directories
        self.working_dir = self.runtime.working_dir.absolute()
        self.elliott_working_dir = self.working_dir / "elliott-working"
        self.doozer_working_dir = self.working_dir / "doozer-working"
        self._shipment_data_repo_dir = self.working_dir / "shipment-data-push"

        # Shipment repository configuration
        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.gitlab_token = None
        self.shipment_mr_url = None
        self.job_url = None

        # Product configuration - initialized to None, will be loaded from group config in run()
        self.product = None

        # FBC operator doozer keys - populated during validate_fbc_related_images()
        self._fbc_operator_keys: list[str] = []

        # Set default shipment_path if not provided, using same logic as elliott
        self.shipment_path_was_defaulted = not shipment_path
        if not shipment_path:
            # For non-OpenShift products, use default shipment data repository
            shipment_path = SHIPMENT_DATA_URL_TEMPLATE

        # Setup shipment repo configuration
        self.shipment_data_repo_pull_url, self.shipment_data_repo_push_url = self._shipment_data_repo_vars(
            shipment_data_repo_url
        )
        # GitRepository expects a local filesystem path, not a URL
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, self.dry_run)

        self.jira_bugs = jira_bugs
        self.target_release_date = target_release_date

        # Base elliott command template
        self._elliott_base_command = [
            'elliott',
            f'--group={group}',
            f'--assembly={assembly}',
            '--build-system=konflux',
            f'--working-dir={self.elliott_working_dir}',
        ]

    def _shipment_data_repo_vars(self, shipment_data_repo_url: Optional[str]):
        """
        Determine shipment data repository URLs for pull and push operations.
        """
        shipment_data_repo_pull_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        shipment_data_repo_push_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_push_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        return shipment_data_repo_pull_url, shipment_data_repo_push_url

    def get_file_from_branch(self, branch: str, filename: str, data_path: str | None = None) -> bytes:
        """
        Fetch a file from ocp-build-data using PyGithub.
        Matches the interface expected by get_advisory_boilerplate().
        """
        if data_path is None:
            data_path = constants.OCP_BUILD_DATA_URL
        try:
            data_path = data_path.rstrip(".git")
            parts = data_path.rstrip("/").split("/")
            if len(parts) < 2:
                raise ValueError(f"Invalid git repo URL: {data_path}")
            user, repo_name = parts[-2], parts[-1]
            github_client = get_github_client_for_org(user)
            repo = github_client.get_repo(f"{user}/{repo_name}")
            file_content = repo.get_contents(filename, ref=branch)
            return file_content.decoded_content
        except GithubException as e:
            raise ValueError(f"Failed to fetch {filename} from {data_path} branch {branch}: {e}")

    def _load_release_notes_template(self, kind: str | None = None) -> dict | None:
        """
        Load and populate release notes template from ocp-build-data advisory_templates.yml.

        Two modes:
        - OCP optional (kind provided): Uses kind (e.g., 'extras') as template key,
          populates {MAJOR}/{MINOR}/{PATCH} from group.yml vars section.
        - Layered product (kind=None): Uses self.product as template key,
          populates {PRODUCT_MAJOR}/{PRODUCT_MINOR}/{PRODUCT_PATCH} from assembly
          and {OCP_RELEASE_NOTES_VERSION} from group.yml.

        Args:
            kind: When provided, used as the boilerplate lookup key (OCP optional mode).
                  When None, self.product is used (layered product mode).

        Returns:
            dict | None: Template dict with synopsis, topic, description, solution, or None if not found.
        """
        art_advisory_key = kind if kind else self.product

        try:
            boilerplate = get_advisory_boilerplate(
                runtime=self,
                et_data={},
                art_advisory_key=art_advisory_key,
                errata_type="RHBA",
            )
        except (ValueError, KeyError):
            self.logger.debug("No release notes template found for key '%s'", art_advisory_key)
            return None
        except Exception as e:
            self.logger.warning("Failed to load release notes template: %s", e)
            return None

        try:
            group_content = self.get_file_from_branch(self.group, "group.yml")
            group_config = stdlib_yaml.safe_load(group_content)

            if kind:
                # OCP optional mode: use {MAJOR}, {MINOR}, {PATCH} from vars section
                vars_section = group_config.get("vars", {})
                major = str(vars_section.get("MAJOR", ""))
                minor = str(vars_section.get("MINOR", ""))
                assembly_parts = self.assembly.split(".")
                patch = assembly_parts[2] if len(assembly_parts) > 2 else "0"
                if not major or not minor:
                    self.logger.warning("MAJOR/MINOR not found in group.yml vars for group '%s'", self.group)
                    return None
                replace_vars = {"MAJOR": major, "MINOR": minor, "PATCH": patch}
            else:
                # Layered product mode: use {PRODUCT_*} and {OCP_RELEASE_NOTES_VERSION}
                ocp_version = str(group_config.get("OCP_RELEASE_NOTES_VERSION", ""))
                if not ocp_version:
                    self.logger.warning("OCP_RELEASE_NOTES_VERSION not found in group.yml for group '%s'", self.group)
                    return None
                assembly_parts = self.assembly.split(".")
                replace_vars = {
                    "OCP_RELEASE_NOTES_VERSION": ocp_version,
                    "OCP_RELEASE_NOTES_VERSION_DASHED": ocp_version.replace(".", "-"),
                    "PRODUCT_MAJOR": assembly_parts[0] if len(assembly_parts) > 0 else "",
                    "PRODUCT_MINOR": assembly_parts[1] if len(assembly_parts) > 1 else "",
                    "PRODUCT_PATCH": assembly_parts[2] if len(assembly_parts) > 2 else "",
                }

            formatter = SafeFormatter()
            result = {}
            for field in ("synopsis", "topic", "description", "solution"):
                value = boilerplate.get(field, "")
                result[field] = formatter.format(value, **replace_vars)

            self.logger.info("Loaded release notes template for key '%s'", art_advisory_key)
            return result

        except Exception as e:
            self.logger.warning("Failed to process release notes template: %s", e)
            return None

    @staticmethod
    def basic_auth_url(url: str, token: str) -> str:
        """
        Create a basic auth URL with the given token.
        """
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme or "https"
        netloc = parsed_url.netloc
        path = parsed_url.path
        params = parsed_url.params
        query = parsed_url.query
        fragment = parsed_url.fragment

        # Construct the URL parts
        url_parts = [path]
        if params:
            url_parts.append(f";{params}")
        if query:
            url_parts.append(f"?{query}")
        if fragment:
            url_parts.append(f"#{fragment}")

        rest_of_url = "".join(url_parts)
        # Use oauth2 as placeholder username and token as password
        return f'{scheme}://oauth2:{token}@{netloc}{rest_of_url}'

    @cached_property
    def _gitlab(self) -> GitLabClient:
        """
        Get authenticated GitLab instance.
        """
        return GitLabClient(self.gitlab_url, self.gitlab_token, self.dry_run)

    def _get_gitlab_project(self, url: str):
        """
        Get GitLab project from URL.
        """
        parsed_url = urlparse(url)
        project_path = parsed_url.path.strip('/').removesuffix('.git')
        return self._gitlab.get_project(project_path)

    def _get_main_ocp_shipment_url(self) -> str | None:
        """Read releases.yml from ocp-build-data and return the main OCP shipment MR URL
        for the current assembly.

        Path: releases.<assembly>.assembly.group.shipment.url
              (key may also be 'shipment!' or other merge-operator variant)
        """
        try:
            content = self.get_file_from_branch(self.group, "releases.yml")
            releases_config = stdlib_yaml.safe_load(content)
            assembly_def = releases_config.get("releases", {}).get(self.assembly, {})
            group = assembly_def.get("assembly", {}).get("group", {})
            shipment = group.get("shipment")
            if shipment is None:
                # Handle assembly merge-operator suffixes (e.g. 'shipment!' override marker
                # written by prepare-release-konflux for child assemblies with basis.assembly)
                for key in group:
                    if isinstance(key, str) and key.startswith("shipment") and len(key) == len("shipment") + 1:
                        shipment = group[key]
                        break
            if not isinstance(shipment, dict):
                shipment = {}
            url = shipment.get("url")
            if url:
                self.logger.info("Found main OCP shipment MR: %s", url)
            else:
                self.logger.warning("No shipment URL in releases.yml for assembly '%s'", self.assembly)
            return url
        except Exception as e:
            self.logger.warning("Failed to read main OCP shipment URL from releases.yml: %s", e)
            return None

    def check_env_vars(self):
        """
        Check required environment variables for MR creation.
        """
        if not self.create_mr:
            return

        gitlab_token = os.getenv("GITLAB_TOKEN")
        if not gitlab_token:
            raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")
        self.gitlab_token = gitlab_token

        self.job_url = os.getenv('BUILD_URL')

    def setup_working_dir(self):
        """
        Setup working directories, cleaning up any existing ones.
        """
        self.working_dir.mkdir(parents=True, exist_ok=True)
        if self.elliott_working_dir.exists():
            shutil.rmtree(self.elliott_working_dir, ignore_errors=True)
        if self.doozer_working_dir.exists():
            shutil.rmtree(self.doozer_working_dir, ignore_errors=True)
        if self.create_mr and self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

    async def setup_shipment_repo(self):
        """
        Setup shipment data repository for MR creation.
        """
        if not self.create_mr:
            return

        # Setup shipment-data repo with GitLab authentication
        await self.shipment_data_repo.setup(
            remote_url=self.basic_auth_url(self.shipment_data_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

    async def _load_product_from_group_config(self) -> str:
        """
        Load the product field from group configuration using doozer command.
        Falls back to extracting from group name if not found.
        """
        try:
            # Use doozer command to read group config product field
            doozer_cmd = ['doozer', f'--group={self.group}', 'config:read-group', 'product']

            _, product_output, _ = await exectools.cmd_gather_async(doozer_cmd)
            # Clean up the output - remove all whitespace (including newlines)
            product = product_output.strip()

            if product and product != 'None' and product != 'null':
                self.logger.info(f"Loaded product from group config: {product}")
                return product
            else:
                self.logger.debug("No product field found in group config, falling back to group name extraction")

        except Exception as e:
            self.logger.warning(f"Failed to load product from group config: {e}")

        # Fallback: extract product from group name (e.g., "oadp-1.3" -> "oadp")
        product = self.group.split('-')[0]
        # Special case: openshift groups do not define product in group config,
        # use 'ocp' as the product name when openshift is detected
        if product == 'openshift':
            product = 'ocp'
        self.logger.info(f"Using product extracted from group name: {product}")
        return product

    async def _load_mr_approvers_from_group_config(self) -> dict[str, list[str]]:
        """
        Load the mr_approvers field from group configuration using doozer command.
        Returns a dict mapping approval group names to lists of GitLab usernames,
        e.g. {"QE": ["user1", "user2"]}. Returns empty dict if not configured.
        """
        try:
            cmd = [
                'doozer',
                f'--group={self.group}',
                f'--working-dir={self.doozer_working_dir}',
                'config:read-group',
                'mr_approvers',
            ]
            _, output, _ = await exectools.cmd_gather_async(cmd)
            output = output.strip()
            if output and output not in ('None', 'null'):
                parsed = stdlib_yaml.safe_load(output)
                if not isinstance(parsed, dict):
                    self.logger.warning("mr_approvers is not a dict (got %s), ignoring", type(parsed).__name__)
                    return {}
                return parsed
        except Exception as e:
            self.logger.warning(f"Failed to load mr_approvers from group config: {e}")
        return {}

    async def _resolve_doozer_key_from_component(self, component: str) -> Optional[str]:
        """Resolve a Brew/Konflux component name to its doozer image key.

        Uses `doozer images:print` with --short to query the authoritative
        component-to-key mapping from ocp-build-data.
        """
        try:
            cmd = [
                'doozer',
                f'--group={self.group}',
                f'--working-dir={self.doozer_working_dir}',
                '--load-disabled',
                'images:print',
                '--show-non-release',
                '--short',
                '{component}: {name}',
            ]
            _, output, _ = await exectools.cmd_gather_async(cmd)
            for line in output.strip().splitlines():
                parts = line.split(': ', 1)
                if len(parts) == 2 and parts[0].strip() == component:
                    return parts[1].strip()
        except Exception as e:
            self.logger.warning("Failed to resolve doozer key for component '%s': %s", component, e)
        return None

    async def _resolve_single_image_key(self) -> Optional[str]:
        """
        Determine the single unambiguous image config doozer key to query for mr_approvers.

        Returns the doozer key if exactly one image/operator can be identified, or None
        if the situation is ambiguous. Ambiguous cases are logged as warnings but do not
        block the pipeline.
        """
        has_fbc = bool(self._fbc_operator_keys)
        has_extras = bool(self.extra_image_nvrs)

        if has_fbc and has_extras:
            self.logger.warning(
                "Cannot resolve single image config for approvers: both FBC_PULLSPECS and "
                "EXTRA_IMAGE_NVRS are provided. Falling back to group config."
            )
            return None

        if has_fbc:
            # Filter out UNKNOWN-* keys (fallback-generated when __doozer_key label was missing)
            valid_keys = [k for k in self._fbc_operator_keys if not k.startswith("UNKNOWN-")]
            if len(valid_keys) == 1:
                return valid_keys[0]
            elif len(valid_keys) == 0:
                self.logger.warning(
                    "Cannot resolve single image config for approvers: no valid operator doozer keys "
                    "found from FBC images. Falling back to group config."
                )
            else:
                self.logger.warning(
                    "Cannot resolve single image config for approvers: multiple FBC operators found "
                    f"({valid_keys}). Falling back to group config."
                )
            return None

        if has_extras:
            if len(self.extra_image_nvrs) == 1:
                component = parse_nvr(self.extra_image_nvrs[0])['name']
                key = await self._resolve_doozer_key_from_component(component)
                if key:
                    self.logger.info("Resolved doozer key '%s' from component '%s'", key, component)
                    return key
                self.logger.warning(
                    "Could not resolve doozer key for component '%s', falling back to group config.", component
                )
                return None
            else:
                self.logger.warning(
                    "Cannot resolve single image config for approvers: multiple EXTRA_IMAGE_NVRS "
                    f"provided ({len(self.extra_image_nvrs)}). Falling back to group config."
                )
                return None

        return None

    async def _load_mr_approvers_from_image_config(self, doozer_key: str) -> dict[str, list[str]]:
        """
        Load the mr_approvers field from a specific image configuration using doozer.
        Returns a dict mapping approval group names to lists of GitLab usernames,
        e.g. {"QE": ["user1", "user2"]}. Returns empty dict if not configured.
        """
        try:
            cmd = [
                'doozer',
                f'--group={self.group}',
                f'--working-dir={self.doozer_working_dir}',
                '-i',
                doozer_key,
                'config:print',
                '--key',
                'mr_approvers',
                '--yaml',
            ]
            _, output, _ = await exectools.cmd_gather_async(cmd)
            output = output.strip()
            if not output:
                return {}
            parsed = stdlib_yaml.safe_load(output)
            if not isinstance(parsed, dict):
                return {}
            images = parsed.get('images')
            if not isinstance(images, dict):
                self.logger.debug(
                    "Unexpected doozer config:print --yaml structure for '%s': missing 'images' key in envelope",
                    doozer_key,
                )
                return {}
            mr_approvers = images.get(doozer_key)
            if not isinstance(mr_approvers, dict) or not mr_approvers:
                return {}
            self.logger.info(f"Loaded mr_approvers from image config '{doozer_key}': {mr_approvers}")
            return mr_approvers
        except Exception as e:
            self.logger.warning(f"Failed to load mr_approvers from image config '{doozer_key}': {e}")
        return {}

    async def extract_fbc_labels(self, fbc_pullspec: str) -> Dict[str, Optional[str]]:
        """
        Extract both the NVR and __doozer_key labels from the FBC image.
        Returns a dict with 'nvr' and 'doozer_key' keys.
        """
        self.logger.info(f"Extracting FBC labels from FBC pullspec: {fbc_pullspec}")

        @retry(reraise=True, stop=stop_after_attempt(3))
        async def _get_image_info():
            oc_cmd = ['oc', 'image', 'info', fbc_pullspec, '--filter-by-os', 'amd64', '-o', 'json']

            # Add registry config for authentication if available
            quay_auth_file = os.getenv("QUAY_AUTH_FILE")
            if quay_auth_file:
                oc_cmd.extend(['--registry-config', quay_auth_file])

            rc, stdout, stderr = await exectools.cmd_gather_async(oc_cmd, check=False)
            if rc != 0:
                raise RuntimeError(f"Failed to get image info for FBC {fbc_pullspec}: {stderr}")
            return stdout

        try:
            image_info_output = await _get_image_info()
            image_info = json.loads(image_info_output)

            # Extract labels
            labels = image_info.get('config', {}).get('config', {}).get('Labels', {})
            nvr = labels.get('com.redhat.art.nvr')
            doozer_key = labels.get('__doozer_key')

            self.logger.info(f"✓ Extracted FBC labels - nvr: {nvr}, doozer_key: {doozer_key}")
            return {'nvr': nvr, 'doozer_key': doozer_key}

        except Exception as e:
            self.logger.exception(f"✗ Failed to extract labels from FBC {fbc_pullspec}: {e}")
            return {'nvr': None, 'doozer_key': None}

    def extract_fbc_nvr(self, fbc_pullspec: str) -> Optional[str]:
        """
        Extract the NVR from the FBC image itself using oc image info and com.redhat.art.nvr label.
        """
        self.logger.info(f"Extracting FBC NVR from FBC pullspec: {fbc_pullspec}")

        @retry(reraise=True, stop=stop_after_attempt(3))
        def _get_image_info():
            oc_cmd = ['oc', 'image', 'info', fbc_pullspec, '--filter-by-os', 'amd64', '-o', 'json']

            # Add registry config for authentication if available
            quay_auth_file = os.getenv("QUAY_AUTH_FILE")
            if quay_auth_file:
                oc_cmd.extend(['--registry-config', quay_auth_file])

            rc, stdout, stderr = exectools.cmd_gather(oc_cmd)
            if rc != 0:
                raise RuntimeError(f"Failed to get image info for FBC {fbc_pullspec}: {stderr}")
            return stdout

        try:
            image_info_output = _get_image_info()
            image_info = json.loads(image_info_output)

            # Extract labels
            labels = image_info.get('config', {}).get('config', {}).get('Labels', {})
            nvr = labels.get('com.redhat.art.nvr')

            if nvr:
                self.logger.info(f"✓ Extracted FBC NVR: {nvr}")
                return nvr
            else:
                self.logger.warning(f"✗ Missing com.redhat.art.nvr label for FBC image {fbc_pullspec}")
                return None

        except Exception as e:
            self.logger.exception(f"✗ Failed to get image info for FBC {fbc_pullspec}: {e}")
            return None

    def is_nvr_from_current_group(self, nvr: str) -> bool:
        """
        Determine if an NVR belongs to the current group based on version matching.

        This filters out external dependencies like kube-rbac-proxy from openshift-4.x
        when processing logging-6.x FBCs.

        Example:
            Assembly "6.3.3" matches NVR version "6.3.3-202602032201..." -> True
            Assembly "6.3.3" does NOT match NVR version "v4.20.0-202512150315..." -> False
            Assembly "6.3" does NOT match NVR version "6.30.0-..." -> False (prevents false positives)
        """
        try:
            nvr_dict = parse_nvr(nvr)
            nvr_version = nvr_dict.get('version', '').lstrip('v')
            assembly_version = self.assembly.lstrip('v')

            # Split into version components
            nvr_parts = nvr_version.split('.')
            assembly_parts = assembly_version.split('.')

            # Require at least major.minor for matching
            if len(assembly_parts) < 2 or len(nvr_parts) < 2:
                # If assembly has < 2 parts, fall back to startswith for compatibility
                is_from_group = nvr_version.startswith(assembly_version)
            else:
                # Compare major.minor segments exactly (prevents "6.3" matching "6.30")
                is_from_group = nvr_parts[0] == assembly_parts[0] and nvr_parts[1] == assembly_parts[1]

            if not is_from_group:
                self.logger.info(
                    f"NVR {nvr} (version: {nvr_version}) does not match assembly {self.assembly} "
                    f"- marking as external dependency"
                )

            return is_from_group

        except ValueError as e:
            # Fail-open: unparsable NVRs are treated as from current group
            # to avoid accidentally excluding legitimate builds
            self.logger.error(
                f"Failed to parse NVR {nvr} for group checking: {e}. "
                f"Treating as from current group to avoid excluding builds. "
                f"INVESTIGATE THIS IF SEEN IN PRODUCTION."
            )
            return True

    def categorize_nvrs(self, nvrs: List[str]) -> Dict[str, List[str]]:
        """
        Categorize NVRs into build kinds.

        In default mode: image / fbc / external (based on group version matching).
        In OCP optional mode (--ocp-optional): extras / fbc / external.
        All non-FBC images (including bundles) go into extras by default;
        only explicitly listed --exclude-nvr-components are filtered out.
        """
        self.logger.info(f"Categorizing {len(nvrs)} extracted NVRs...")

        non_fbc_key = "extras" if self.ocp_optional else "image"
        categorized: Dict[str, List[str]] = {non_fbc_key: [], "fbc": [], "external": []}

        for nvr in nvrs:
            component_name = parse_nvr(nvr)['name']

            if component_name.endswith('-fbc'):
                categorized["fbc"].append(nvr)
            elif self.ocp_optional:
                if self.excluded_components and component_name in self.excluded_components:
                    categorized["external"].append(nvr)
                else:
                    categorized[non_fbc_key].append(nvr)
            elif self.is_nvr_from_current_group(nvr):
                categorized[non_fbc_key].append(nvr)
            else:
                categorized["external"].append(nvr)

        self.logger.info(f"Categorization results{' (OCP optional mode)' if self.ocp_optional else ''}:")
        self.logger.info(f"  • {non_fbc_key.title()} builds: {len(categorized[non_fbc_key])}")
        self.logger.info(f"  • FBC builds: {len(categorized['fbc'])}")
        if categorized['external']:
            label = "Excluded" if self.ocp_optional else "External dependencies (filtered)"
            self.logger.info(f"  • {label}: {len(categorized['external'])}")
            for ext_nvr in categorized['external']:
                self.logger.info(f"    - {ext_nvr}")

        return categorized

    async def create_snapshot(self, builds: List[str]) -> Optional[Snapshot]:
        """
        Create a snapshot from a list of build NVRs using elliott.
        """
        if not builds:
            self.logger.debug("No builds provided, skipping snapshot creation")
            return None

        self.logger.info(f"Creating Konflux snapshot for {len(builds)} builds...")

        # store builds in a temporary file, each nvr string in a new line
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
            for nvr in builds:
                temp_file.write(nvr + '\n')
            temp_file.flush()
            temp_file_path = temp_file.name

        # now call elliott snapshot new -f <temp_file_path>
        snapshot_cmd = self._elliott_base_command + [
            "snapshot",
            "new",
            f"--builds-file={temp_file_path}",
        ]

        quay_auth_file = os.getenv("QUAY_AUTH_FILE")
        if quay_auth_file:
            snapshot_cmd.append(f"--pull-secret={quay_auth_file}")

        try:
            self.logger.info(f"Running elliott snapshot command: {' '.join(snapshot_cmd)}")
            stdout, _ = exectools.cmd_assert(snapshot_cmd)
        except Exception as e:
            self.logger.exception(f"Failed to create snapshot: {e}")
            raise
        finally:
            # remove the temporary file
            os.unlink(temp_file_path)

        # parse the output of the snapshot new command, it should be valid yaml
        try:
            new_snapshot_obj = yaml.load(stdout)
            self.logger.info("✓ Successfully created Konflux snapshot")
            return Snapshot(spec=SnapshotSpec(**new_snapshot_obj.get("spec")), nvrs=sorted(builds))
        except Exception as e:
            self.logger.exception(f"Failed to parse elliott snapshot output: {e}")
            self.logger.debug(f"Raw output was: {stdout}")
            raise

    def generate_release_notes(self) -> Optional[ReleaseNotes]:
        """
        Call elliott process-release-from-fbc-bugs to process JIRAs and return ReleaseNotes.
        """
        if not self.jira_bugs:
            return None

        jira_bugs_str = ",".join(self.jira_bugs)
        self.logger.info("Processing JIRA bugs for release notes: %s", jira_bugs_str)

        cmd = self._elliott_base_command + [
            "process-release-from-fbc-bugs",
            f"--jira-bugs={jira_bugs_str}",
        ]

        self.logger.info("Running: %s", " ".join(cmd))
        stdout, stderr = exectools.cmd_assert(cmd)
        if stderr:
            for line in stderr.strip().splitlines():
                self.logger.info("  elliott: %s", line)

        release_notes_data = stdlib_yaml.safe_load(stdout)
        release_notes = ReleaseNotes(**release_notes_data)
        self.logger.info(
            "Generated release notes: type=%s, issues=%d, cves=%d",
            release_notes.type,
            len(release_notes.issues.fixed) if release_notes.issues and release_notes.issues.fixed else 0,
            len(release_notes.cves) if release_notes.cves else 0,
        )
        return release_notes

    def create_shipment_config(
        self, kind: str, snapshot: Optional[Snapshot], release_notes: Optional[ReleaseNotes] = None
    ) -> ShipmentConfig:
        """
        Create a shipment configuration for the given kind and snapshot.
        """
        # Guard: ensure product is initialized
        if self.product is None:
            raise RuntimeError(
                "Product is not initialized. Please call run() first to load the product from group configuration, "
                "or ensure self.product is set before calling create_shipment_config()."
            )

        self.logger.info(f"Creating shipment config for {kind}...")

        metadata = Metadata(
            product=self.product,
            application=snapshot.spec.application if snapshot else "default-app",
            group=self.group,
            assembly=self.assembly,
            fbc=kind == 'fbc',
        )

        # Create environments - read from shipment repo config if available
        stage_rpa = "n/a"
        prod_rpa = "n/a"

        if hasattr(self, 'shipment_data_repo') and self.shipment_data_repo:
            try:
                config_path = self.shipment_data_repo._directory / "config.yaml"
                if config_path.exists():
                    with open(config_path, 'r') as f:
                        # Use yaml.safe_load from stdlib for reading config
                        shipment_config = stdlib_yaml.safe_load(f) or {}

                    application = snapshot.spec.application if snapshot else "default-app"
                    app_env_config = (
                        shipment_config.get("applications", {}).get(application, {}).get("environments", {})
                    )
                    stage_rpa = app_env_config.get("stage", {}).get("releasePlan", "n/a")
                    prod_rpa = app_env_config.get("prod", {}).get("releasePlan", "n/a")
            except Exception as e:
                self.logger.warning(f"Failed to read shipment config, using defaults: {e}")

        environments = Environments(stage=ShipmentEnv(releasePlan=stage_rpa), prod=ShipmentEnv(releasePlan=prod_rpa))

        # Determine which kinds carry release notes.
        # - 'fbc' shipments never carry release notes.
        # - For openshift-* groups, ALL non-FBC shipments (image, extras)
        #   require data.releaseNotes per shipment model validation.
        # - For non-openshift groups, release notes are included only if provided.
        data = None
        if kind != 'fbc' and release_notes:
            self.logger.info("Using provided release notes (type=%s) for %s shipment", release_notes.type, kind)
            data = Data(releaseNotes=release_notes)
        elif kind != 'fbc' and self.group.startswith('openshift-'):
            self.logger.info(
                f"Group '{self.group}' starts with 'openshift-' - generating minimal release notes "
                f"for {kind} shipment to satisfy shipment validation requirements."
            )
            rn = ReleaseNotes(type='RHBA', synopsis=f'FBC-derived {kind} shipment for {self.product} {self.assembly}')
            data = Data(releaseNotes=rn)

        # Create shipment
        shipment = Shipment(metadata=metadata, environments=environments, snapshot=snapshot, data=data)

        return ShipmentConfig(shipment=shipment)

    async def create_shipment_mr(self, shipments_by_kind: Dict[str, ShipmentConfig], env: str = "prod") -> str:
        """
        Create a new shipment MR with the given shipment config files.
        """
        if not self.create_mr:
            return ""

        self.logger.info("Creating shipment MR...")

        # Create branch name
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"prepare-shipment-{self.assembly}-{timestamp}"
        target_branch = "main"

        # Create and checkout branch
        await self.shipment_data_repo.create_branch(source_branch)

        # Update shipment data repo with shipment configs
        commit_message = f"Add shipment configurations for {self.product} {self.assembly}"
        updated = await self.update_shipment_data(shipments_by_kind, env, commit_message, source_branch)
        if not updated:
            raise ValueError("Failed to update shipment data repo. Please investigate.")

        source_project = self._get_gitlab_project(self.shipment_data_repo_push_url)
        target_project = self._get_gitlab_project(self.shipment_data_repo_pull_url)

        # Create MR title and description
        if self.target_release_date:
            mr_title = f"Draft: Shipment for {self.product} {self.assembly} (ship date: {self.target_release_date})"
        else:
            mr_title = f"Draft: Shipment for {self.product} {self.assembly}"
        mr_description = f"Created by job: {self.job_url}\n\n" if self.job_url else ""
        mr_description += f"Shipment files created for {self.assembly} using release-from-fbc command"

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
            self.logger.info("Created Merge Request: %s", mr_url)

        # Configure approval rules: try image config first, fall back to group.yml
        approvers_config: dict[str, list[str]] = {}
        image_key = await self._resolve_single_image_key()
        if image_key:
            approvers_config = await self._load_mr_approvers_from_image_config(image_key)
        if not approvers_config:
            approvers_config = await self._load_mr_approvers_from_group_config()

        if approvers_config:
            if self.dry_run:
                self.logger.info("[DRY-RUN] Would set MR approval rules: %s", approvers_config)
            else:
                try:
                    await self._gitlab.set_mr_approval_rules(mr_url, approvers_config)
                except Exception as e:
                    self.logger.warning(f"Failed to set MR approval rules: {e}")

        # Store the MR URL for later use
        self.shipment_mr_url = mr_url
        return mr_url

    async def set_shipment_mr_ready(self):
        """
        Mark the shipment MR as ready by removing the Draft prefix from the title.
        This should be called at the end of the pipeline when all work is complete.
        """
        mr = await self._gitlab.set_mr_ready(self.shipment_mr_url)

        if mr and not self.dry_run:
            self.logger.info("Waiting for 30 seconds to ensure MR is updated...")
            await asyncio.sleep(30)

            try:
                pipeline_url = await self._gitlab.trigger_ci_pipeline(mr)
                if pipeline_url:
                    self.logger.info(f"CI pipeline triggered: {pipeline_url}")
                else:
                    self.logger.warning(f"Failed to trigger CI pipeline for MR branch {mr.source_branch}")
            except Exception as e:
                self.logger.warning(f"Failed to trigger CI MR pipeline for branch {mr.source_branch}: {e}")

    async def _set_shipment_mr_dependency(self, blocking_mr_url: str):
        """Set a native GitLab MR dependency so the shipment MR cannot merge
        before *blocking_mr_url* is merged.

        Failures are logged as warnings and do not block the pipeline.
        """
        if not self.shipment_mr_url or not blocking_mr_url:
            return
        try:
            self._gitlab.add_mr_dependency(self.shipment_mr_url, blocking_mr_url)
        except Exception as e:
            self.logger.warning(
                "Failed to set MR dependency (%s depends on %s): %s",
                self.shipment_mr_url,
                blocking_mr_url,
                e,
            )

    async def update_shipment_data(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, commit_message: str, branch: str
    ) -> bool:
        """
        Update shipment data repo with the given shipment config files.
        """
        if not self.create_mr:
            return False

        # Get the timestamp from the branch name
        timestamp = branch.split("-")[-1]

        for advisory_kind, shipment_config in shipments_by_kind.items():
            filepath = await self._write_shipment_file(advisory_kind, shipment_config, env, timestamp)
            self.logger.info("Updating shipment file: %s", filepath)

        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        return await self.shipment_data_repo.commit_push(commit_message, safe=True)

    async def _write_shipment_file(
        self, advisory_kind: str, shipment_config: ShipmentConfig, env: str, timestamp: str
    ) -> str:
        """
        Common logic for writing a single shipment file.
        Returns the filepath where the file was written.
        """
        # Use improved naming: for FBC shipments use counter format, for image use simple format
        if advisory_kind.startswith('fbc'):
            # Extract counter from advisory_kind (e.g., 'fbc01' -> '01')
            counter = advisory_kind[3:]  # Remove 'fbc' prefix
            filename = f"{self.assembly}.fbc.{timestamp}{counter}.yaml"
        else:
            filename = f"{self.assembly}.{advisory_kind}.{timestamp}.yaml"

        product = shipment_config.shipment.metadata.product
        group = shipment_config.shipment.metadata.group
        application = shipment_config.shipment.metadata.application
        relative_target_dir = Path("shipment") / product / group / application / env
        target_dir = self.shipment_data_repo._directory / relative_target_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        filepath = relative_target_dir / filename

        shipment_dump = shipment_config.model_dump(exclude_unset=True, exclude_none=True)
        out = StringIO()
        yaml.dump(shipment_dump, out)
        await self.shipment_data_repo.write_file(filepath, out.getvalue())

        return str(filepath)

    async def write_shipment_files_locally(
        self, shipments_by_kind: Dict[str, ShipmentConfig], env: str, timestamp: str
    ):
        """
        Write shipment files to the local repository without creating MR.
        """
        self.logger.info(f"Writing {len(shipments_by_kind)} shipment files to local repository...")

        for advisory_kind, shipment_config in shipments_by_kind.items():
            filepath = await self._write_shipment_file(advisory_kind, shipment_config, env, timestamp)
            self.logger.info(f"Created shipment file: {filepath}")

    async def validate_fbc_related_images(self, fbc_pullspecs: List[str]) -> List[str]:
        """
        Validate that FBC pullspecs from the same operator have the same related images.
        Different operators are allowed to have different related images.
        Returns the combined list of all unique related images across all operators.
        """
        self.logger.info(f"Validating related images for {len(fbc_pullspecs)} FBC builds (grouped by operator)...")

        # Step 1: Extract operator identifiers from all FBCs in parallel
        self.logger.info("Extracting operator identifiers from FBC images...")
        label_tasks = [self.extract_fbc_labels(fbc) for fbc in fbc_pullspecs]
        fbc_labels_list = await asyncio.gather(*label_tasks)

        # Step 2: Map FBCs to operators
        fbc_to_operator = {}
        for fbc_pullspec, labels in zip(fbc_pullspecs, fbc_labels_list):
            operator = labels.get('doozer_key')

            # Fallback: If __doozer_key is missing, extract from tag
            if not operator:
                # Try to extract operator name from pullspec tag
                # e.g., "art-fbc:cluster-logging-operator-fbc-6.3.3-..." -> "cluster-logging-operator"
                try:
                    tag = fbc_pullspec.split(':')[-1]
                    if '-fbc-' in tag:
                        operator = tag.split('-fbc-')[0]
                        self.logger.error(
                            f"Missing __doozer_key label for {fbc_pullspec}, using extracted operator name: {operator}"
                        )
                    else:
                        # Use unique key per FBC to prevent unrelated FBCs from being grouped
                        operator = f"UNKNOWN-{hash(fbc_pullspec) & 0xFFFFFFFF:08x}"
                        self.logger.error(
                            f"Missing __doozer_key label for {fbc_pullspec}, using generated operator identifier: {operator}"
                        )
                except Exception as e:
                    self.logger.error(f"Failed to extract operator from {fbc_pullspec}: {e}")
                    # Use unique key per FBC to prevent unrelated FBCs from being grouped
                    operator = f"UNKNOWN-{hash(fbc_pullspec) & 0xFFFFFFFF:08x}"

            fbc_to_operator[fbc_pullspec] = operator

        # Step 3: Group FBCs by operator
        operator_groups = defaultdict(list)
        for fbc, operator in fbc_to_operator.items():
            operator_groups[operator].append(fbc)

        # Store unique operator keys for approver resolution
        self._fbc_operator_keys = sorted(operator_groups.keys())

        self.logger.info(f"Found {len(operator_groups)} operator group(s):")
        for operator, fbcs in operator_groups.items():
            self.logger.info(f"  • {operator}: {len(fbcs)} FBC(s)")

        # Step 4: Extract related images from each FBC sequentially to avoid temp directory conflicts
        self.logger.info("Extracting related images from all FBCs...")
        fbc_related_images = {}
        for fbc_pullspec in fbc_pullspecs:
            related_nvrs = await extract_nvrs_from_fbc(fbc_pullspec, self.product)
            fbc_related_images[fbc_pullspec] = sorted(related_nvrs)

        # Step 5: Validate related images within each operator group
        all_related_images = set()
        validation_errors = []

        for operator, fbcs_in_group in operator_groups.items():
            if len(fbcs_in_group) == 1:
                # Single FBC for this operator, no comparison needed
                self.logger.info(f"✓ Operator '{operator}': single FBC, no validation needed")
                all_related_images.update(fbc_related_images[fbcs_in_group[0]])
                continue

            # Multiple FBCs for this operator - validate they match
            self.logger.info(f"Validating {len(fbcs_in_group)} FBCs for operator '{operator}'...")
            reference_fbc = fbcs_in_group[0]
            reference_images = fbc_related_images[reference_fbc]

            mismatches = []
            for fbc_pullspec in fbcs_in_group[1:]:
                current_images = fbc_related_images[fbc_pullspec]
                if current_images != reference_images:
                    # Find specific differences
                    only_in_reference = set(reference_images) - set(current_images)
                    only_in_current = set(current_images) - set(reference_images)
                    mismatches.append(
                        {
                            'fbc': fbc_pullspec,
                            'only_in_reference': sorted(only_in_reference),
                            'only_in_current': sorted(only_in_current),
                        }
                    )

            if mismatches:
                error_msg = f"Operator '{operator}': FBC builds have mismatched related images.\n"
                error_msg += f"Reference FBC: {reference_fbc}\n"
                for mismatch in mismatches:
                    error_msg += f"\nMismatch with: {mismatch['fbc']}\n"
                    if mismatch['only_in_reference']:
                        error_msg += f"  Only in reference: {mismatch['only_in_reference']}\n"
                    if mismatch['only_in_current']:
                        error_msg += f"  Only in current: {mismatch['only_in_current']}\n"
                validation_errors.append(error_msg)
            else:
                self.logger.info(
                    f"✓ Operator '{operator}': all {len(fbcs_in_group)} FBCs have matching related images "
                    f"({len(reference_images)} images)"
                )
                all_related_images.update(reference_images)

        # Step 6: Report validation results
        if validation_errors:
            full_error = "\n".join(validation_errors)
            self.logger.error(f"Validation failed:\n{full_error}")
            raise RuntimeError(full_error)

        unique_images = sorted(all_related_images)
        self.logger.info(
            f"✓ Validation passed: {len(unique_images)} total unique related images across "
            f"{len(operator_groups)} operator(s)"
        )
        return unique_images

    async def run(self) -> None:
        """
        Execute the FBC release workflow.

        In default mode, produces image + fbc shipments for layered products.
        In OCP optional mode (--ocp-optional), produces extras + fbc
        shipments for OCP optional operators that were excluded from the main release.
        """
        self.logger.info(f"Starting FBC-based release workflow for {self.assembly}")
        if self.ocp_optional:
            self.logger.info("OCP optional-operator mode enabled (extras/fbc)")
            if self.excluded_components:
                self.logger.info(f"Excluding NVR components: {sorted(self.excluded_components)}")
            if not self.group.startswith('openshift-'):
                raise ValueError(
                    f"--ocp-optional requires an openshift-* group, but got '{self.group}'. "
                    "This flag is only for OCP optional operators excluded from the main OCP release. "
                    "Layered products (OADP, MTA, MTC, Logging) should use the default mode."
                )
        else:
            if self.group.startswith('openshift-'):
                raise ValueError(
                    f"Group '{self.group}' is an openshift-* group but --ocp-optional was not set. "
                    "OCP FBCs must use --ocp-optional to produce extras/fbc shipments. "
                    "Without it, cross-group filtering may discard images and produce only FBC yaml."
                )
        self.logger.info(f"Processing {len(self.fbc_pullspecs)} FBC pullspecs")
        if self.extra_image_nvrs:
            self.logger.info(f"Including {len(self.extra_image_nvrs)} extra image NVRs")

        # Initialize environment and repositories
        self.check_env_vars()
        self.setup_working_dir()
        if self.create_mr:
            await self.setup_shipment_repo()

        # Load product from group configuration
        self.product = await self._load_product_from_group_config()
        self.logger.info(f"Loaded product '{self.product}' - continuing workflow for {self.product} {self.assembly}")

        related_nvrs = []
        fbc_nvrs = []

        if self.fbc_pullspecs:
            # Validate that all FBC builds have the same related images
            related_nvrs = await self.validate_fbc_related_images(self.fbc_pullspecs)

            # Extract FBC NVRs from each FBC pullspec
            fbc_nvrs = [
                nvr for fbc_pullspec in self.fbc_pullspecs if (nvr := self.extract_fbc_nvr(fbc_pullspec)) is not None
            ]

            if fbc_nvrs:
                self.logger.info(f"Extracted {len(fbc_nvrs)} FBC NVRs: {fbc_nvrs}")
            else:
                self.logger.warning("Could not extract any FBC NVRs - FBC shipments will not be created")

        # Combine related images with FBC NVRs
        all_nvrs = related_nvrs[:] + fbc_nvrs

        if not all_nvrs and not self.extra_image_nvrs:
            raise RuntimeError("No NVRs extracted from FBC images and no extra image NVRs provided")

        # Categorize the extracted NVRs
        empty_categorized: Dict[str, List[str]]
        if self.ocp_optional:
            empty_categorized = {"extras": [], "fbc": [], "external": []}
        else:
            empty_categorized = {"image": [], "fbc": [], "external": []}
        categorized_nvrs = self.categorize_nvrs(all_nvrs) if all_nvrs else empty_categorized

        # The key for the primary non-FBC image shipment depends on mode
        image_key = "extras" if self.ocp_optional else "image"

        # Merge extra image NVRs into the primary image category
        if self.extra_image_nvrs:
            fbc_extras = [nvr for nvr in self.extra_image_nvrs if parse_nvr(nvr)['name'].endswith('-fbc')]
            if fbc_extras:
                raise RuntimeError(
                    f"--extra-image-nvrs contains FBC builds which belong in --fbc-pullspecs instead: {fbc_extras}"
                )
            self.logger.info(f"Adding {len(self.extra_image_nvrs)} extra image NVRs to {image_key} shipment")
            categorized_nvrs[image_key] = list(dict.fromkeys([*categorized_nvrs[image_key], *self.extra_image_nvrs]))

        # Create snapshots for the primary image/extras builds
        image_snapshot = None
        if categorized_nvrs.get(image_key):
            image_snapshot = await self.create_snapshot(categorized_nvrs[image_key])

        # Create separate FBC snapshots for each FBC build
        fbc_snapshots = {}
        for fbc_nvr in fbc_nvrs:
            fbc_snapshots[fbc_nvr] = await self.create_snapshot([fbc_nvr])

        # Generate release notes from JIRA bugs if provided
        release_notes = None
        if self.jira_bugs:
            release_notes = self.generate_release_notes()

        # Load release notes template from ocp-build-data
        if self.ocp_optional:
            template = self._load_release_notes_template(kind=image_key)
        else:
            template = self._load_release_notes_template()

        if template:
            if release_notes is None:
                release_notes = ReleaseNotes(type="RHBA")
            release_notes.synopsis = template["synopsis"]
            release_notes.topic = template["topic"]
            release_notes.description = template["description"]
            release_notes.solution = template["solution"]

        # Create shipment configurations
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        shipments_by_kind: Dict[str, ShipmentConfig] = {}

        # Create the primary image/extras shipment
        if image_snapshot:
            shipment_config = self.create_shipment_config(image_key, image_snapshot, release_notes=release_notes)
            shipments_by_kind[image_key] = shipment_config

        # Create separate FBC shipment for each FBC build
        fbc_counter = 1
        for _, fbc_snapshot in fbc_snapshots.items():
            if fbc_snapshot:
                shipment_config = self.create_shipment_config('fbc', fbc_snapshot)
                shipment_kind = f"fbc{fbc_counter:02d}"
                shipments_by_kind[shipment_kind] = shipment_config
                fbc_counter += 1

        # Write shipment files to local repository (only when not creating MR)
        if shipments_by_kind and not self.create_mr:
            await self.write_shipment_files_locally(shipments_by_kind, "prod", timestamp)

        # Create MR if requested
        if self.create_mr and shipments_by_kind:
            try:
                mr_url = await self.create_shipment_mr(shipments_by_kind, env="prod")
                if mr_url:
                    self.logger.info(f"Created shipment MR: {mr_url}")

                    if self.ocp_optional:
                        main_ocp_mr_url = self._get_main_ocp_shipment_url()
                        if main_ocp_mr_url:
                            await self._set_shipment_mr_dependency(main_ocp_mr_url)

                    await self.set_shipment_mr_ready()
            except Exception as e:
                self.logger.exception(f"Failed to create MR: {e}")
                if not self.dry_run:
                    self.logger.info("Continuing with local files only")

        # Generate completion message
        completion_msg = (
            f"FBC-based release workflow completed for {self.product} {self.assembly}. "
            f"Created {len(shipments_by_kind)} shipment file(s) "
            f"({', '.join(shipments_by_kind.keys())}) "
            f"in repository: {self.shipment_data_repo._directory}"
        )

        if self.shipment_mr_url:
            completion_msg += f". MR: {self.shipment_mr_url}"
        self.logger.info(completion_msg)


@cli.command("release-from-fbc")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group to operate on e.g. oadp-1.5, mta-7.0, mtc-1.8, logging-5.9, or openshift-4.22 (with --ocp-optional)",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly to operate on e.g. 1.5.3, 4.18.x",
)
@click.option(
    "--fbc-pullspecs",
    metavar="FBC_PULLSPECS",
    required=False,
    default="",
    help="Comma-separated list of FBC image pullspecs to extract NVRs from. At least one of --fbc-pullspecs or --extra-image-nvrs must be provided.",
)
@click.option(
    "--extra-image-nvrs",
    metavar="EXTRA_IMAGE_NVRS",
    required=False,
    default="",
    help="Comma-separated list of extra image NVRs to include in the image shipment (not part of the FBC). At least one of --fbc-pullspecs or --extra-image-nvrs must be provided.",
)
@click.option(
    "--create-mr",
    is_flag=True,
    help="Create a merge request in the shipment data repository (requires GITLAB_TOKEN environment variable)",
)
@click.option(
    '--shipment-data-repo-url',
    help='Shipment data repository URL for MR creation. If not provided, will use default based on configuration.',
)
@click.option(
    '--shipment-path',
    help='Path to shipment data repository for elliott commands. If not provided, defaults to the OCP shipment data repository URL.',
)
@click.option(
    '--jira-bugs',
    default=None,
    help='Comma-separated list of JIRA issue IDs to include in the advisory (e.g., OADP-7223,OADP-7222). '
    'When provided, release notes with bug/CVE information are generated for the image shipment.',
)
@click.option(
    '--target-release-date',
    default=None,
    help='Target ship date for the release (e.g., 2026-Mar-31 or 2026-03-31). '
    'When provided, the date is included in the shipment MR title.',
)
@click.option(
    '--ocp-optional',
    is_flag=True,
    default=False,
    help='Enable OCP optional-operator mode. Categorizes NVRs into extras/fbc '
    'kinds (matching the OCP shipment structure) instead of the default image/fbc split. '
    'All FBC related images are included by default. Use with --group openshift-4.x.',
)
@click.option(
    '--exclude-nvr-components',
    default=None,
    help='Comma-separated NVR component names to explicitly exclude from the shipment '
    '(e.g., kube-rbac-proxy-container). Only needed in edge cases with --ocp-optional.',
)
@pass_runtime
@click_coroutine
async def release_from_fbc(
    runtime: Runtime,
    group: str,
    assembly: str,
    fbc_pullspecs: str,
    extra_image_nvrs: str,
    create_mr: bool,
    shipment_data_repo_url: Optional[str],
    shipment_path: Optional[str],
    jira_bugs: Optional[str],
    target_release_date: Optional[str],
    ocp_optional: bool,
    exclude_nvr_components: Optional[str],
):
    """
    Create shipment files from an FBC image.

    This command extracts NVRs from an FBC image and creates separate shipment files.
    It supports two modes:

    \b
    DEFAULT MODE (layered products):
      For non-OpenShift products (OADP, MTA, MTC, logging). Creates image + fbc
      shipment files. External dependencies from other groups are filtered out.

    \b
    OCP OPTIONAL MODE (--ocp-optional):
      For OCP optional operators that were excluded from the main OCP release
      (e.g., due to a dependency version conflict like kube-rbac-proxy). Creates
      extras + fbc shipment files. ALL related images from the FBC are
      included by default.

    At least one of --fbc-pullspecs or --extra-image-nvrs must be provided.

    \b
    # Layered product (default mode):
    $ artcd release-from-fbc \\
        --group oadp-1.5 \\
        --assembly 1.5.3 \\
        --fbc-pullspecs quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:oadp-operator-fbc-1.5.3-20251028153444

    \b
    # OCP optional operator (all related images included):
    $ artcd release-from-fbc \\
        --group openshift-4.22 \\
        --assembly 4.22.0 \\
        --fbc-pullspecs quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:ose-metallb-operator-fbc-4.22.0-20260528170921 \\
        --ocp-optional \\
        --create-mr

    \b
    # OCP optional operator with explicit exclusion (edge case):
    $ artcd release-from-fbc \\
        --group openshift-4.22 \\
        --assembly 4.22.0 \\
        --fbc-pullspecs quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc:ose-metallb-operator-fbc-4.22.0-20260528170921 \\
        --ocp-optional \\
        --exclude-nvr-components kube-rbac-proxy-container \\
        --create-mr
    """
    fbc_pullspecs_list = [spec.strip() for spec in fbc_pullspecs.split(',') if spec.strip()]
    extra_image_nvrs_list = [nvr.strip() for nvr in extra_image_nvrs.split(',') if nvr.strip()]

    if not fbc_pullspecs_list and not extra_image_nvrs_list:
        raise click.ClickException("At least one of --fbc-pullspecs or --extra-image-nvrs must be provided")

    # Parse comma-separated JIRA bugs
    jira_bugs_list = None
    if jira_bugs:
        jira_bugs_list = [j.strip() for j in jira_bugs.split(',') if j.strip()]
        if not jira_bugs_list:
            raise click.ClickException("--jira-bugs was provided but contains no valid JIRA IDs")

    # Normalize target release date if provided
    normalized_date = None
    if target_release_date:
        normalized_date = _normalize_release_date(target_release_date)

    # Parse comma-separated exclude NVR components
    exclude_nvr_components_list = None
    if exclude_nvr_components:
        if not ocp_optional:
            raise click.ClickException("--exclude-nvr-components requires --ocp-optional to be set")
        exclude_nvr_components_list = [c.strip() for c in exclude_nvr_components.split(',') if c.strip()]

    pipeline = ReleaseFromFbcPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        fbc_pullspecs=fbc_pullspecs_list,
        create_mr=create_mr,
        shipment_data_repo_url=shipment_data_repo_url,
        shipment_path=shipment_path,
        jira_bugs=jira_bugs_list,
        target_release_date=normalized_date,
        extra_image_nvrs=extra_image_nvrs_list,
        ocp_optional=ocp_optional,
        exclude_nvr_components=exclude_nvr_components_list,
    )

    await pipeline.run()
