import asyncio
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from functools import cached_property
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import click
import yaml as stdlib_yaml
from artcommonlib import exectools
from artcommonlib.constants import SHIPMENT_DATA_URL_TEMPLATE
from artcommonlib.gitdata import SafeFormatter
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.gitlab import GitLabClient
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import (
    new_roundtrip_yaml_handler,
    resolve_konflux_kubeconfig_by_product,
    resolve_konflux_namespace_by_product,
)
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
from elliottlib.util import get_advisory_boilerplate
from github import GithubException

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.fbc_util import validate_fbc_related_images
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config

yaml = new_roundtrip_yaml_handler()


def _normalize_release_date(date_str: str) -> str:
    for fmt in ("%Y-%b-%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%b-%d")
        except ValueError:
            continue
    raise click.ClickException(f"Invalid date format '{date_str}'. Use YYYY-Mon-DD or YYYY-MM-DD")


class PrepareReleaseLPPipeline:
    """
    Pipeline for preparing Layered Product releases from assembly definitions.

    This is the LP equivalent of prepare-release-konflux for OCP. It reads
    a named assembly from releases.yml, triggers bundle and FBC builds
    from the pinned operand NVRs, and creates a shipment MR.

    Key differences from OCP prepare-release:
    - No ET advisories (LP is Konflux-only)
    - No RHCOS / payload verification
    - Actively builds bundles and FBCs (OCP finds existing builds)
    - Uses the named assembly for all doozer/elliott commands
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        build_data_repo_url: Optional[str] = None,
        shipment_data_repo_url: Optional[str] = None,
        create_mr: bool = False,
        jira_bugs: Optional[List[str]] = None,
        target_release_date: Optional[str] = None,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.create_mr = create_mr
        self.dry_run = self.runtime.dry_run
        self.jira_bugs = jira_bugs
        self.target_release_date = target_release_date

        self._working_dir = self.runtime.working_dir.absolute()
        self._elliott_working_dir = self._working_dir / "elliott-working"
        self._shipment_data_repo_dir = self._working_dir / "shipment-data-push"

        self.build_data_repo_url = build_data_repo_url or constants.OCP_BUILD_DATA_URL

        self.gitlab_url = self.runtime.config.get("gitlab_url", "https://gitlab.cee.redhat.com")
        self.gitlab_token: Optional[str] = None
        self.shipment_mr_url: Optional[str] = None
        self.job_url: Optional[str] = None

        self.product: Optional[str] = None

        shipment_data_pull_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        shipment_data_push_url = (
            shipment_data_repo_url
            or self.runtime.config.get("shipment_config", {}).get("shipment_data_push_url")
            or SHIPMENT_DATA_URL_TEMPLATE
        )
        self.shipment_data_repo_pull_url = shipment_data_pull_url
        self.shipment_data_repo_push_url = shipment_data_push_url
        self.shipment_data_repo = GitRepository(self._shipment_data_repo_dir, self.dry_run)

        group_param = f'--group={group}'
        self._doozer_base_command = [
            'doozer',
            group_param,
            f'--assembly={assembly}',
            '--build-system=konflux',
        ]
        self._elliott_base_command = [
            'elliott',
            group_param,
            f'--assembly={assembly}',
            '--build-system=konflux',
            f'--working-dir={self._elliott_working_dir}',
        ]

    def _check_env_vars(self):
        if self.create_mr:
            gitlab_token = os.getenv("GITLAB_TOKEN")
            if not gitlab_token:
                raise ValueError("GITLAB_TOKEN environment variable is required to create a merge request")
            self.gitlab_token = gitlab_token
        self.job_url = os.getenv('BUILD_URL')

    def _setup_working_dir(self):
        self._working_dir.mkdir(parents=True, exist_ok=True)
        if self._elliott_working_dir.exists():
            shutil.rmtree(self._elliott_working_dir, ignore_errors=True)
        if self.create_mr and self._shipment_data_repo_dir.exists():
            shutil.rmtree(self._shipment_data_repo_dir, ignore_errors=True)

    @staticmethod
    def _basic_auth_url(url: str, token: str) -> str:
        parsed = urlparse(url)
        scheme = parsed.scheme or "https"
        return f'{scheme}://oauth2:{token}@{parsed.netloc}{parsed.path}'

    @cached_property
    def _gitlab(self) -> GitLabClient:
        return GitLabClient(self.gitlab_url, self.gitlab_token, self.dry_run)

    def _get_gitlab_project(self, url: str):
        parsed = urlparse(url)
        project_path = parsed.path.strip('/').removesuffix('.git')
        return self._gitlab.get_project(project_path)

    async def _load_product_from_group_config(self) -> str:
        try:
            cmd = ['doozer', f'--group={self.group}', 'config:read-group', 'product']
            _, product_output, _ = await exectools.cmd_gather_async(cmd)
            product = product_output.strip()
            if product and product not in ('None', 'null'):
                return product
        except Exception as e:
            self._logger.warning("Failed to load product from group config: %s", e)
        product = self.group.split('-')[0]
        if product == 'openshift':
            product = 'ocp'
        return product

    async def _load_mr_approvers_from_group_config(self) -> dict[str, list[str]]:
        """
        Load the mr_approvers field from group configuration using doozer command.
        Returns a dict mapping approval group names to lists of GitLab usernames,
        e.g. {"QE": ["user1", "user2"]}. Returns empty dict if not configured.
        """
        try:
            cmd = ['doozer', f'--group={self.group}', 'config:read-group', 'mr_approvers']
            _, output, _ = await exectools.cmd_gather_async(cmd)
            output = output.strip()
            if output and output not in ('None', 'null'):
                parsed = stdlib_yaml.safe_load(output)
                if not isinstance(parsed, dict):
                    self._logger.warning("mr_approvers is not a dict (got %s), ignoring", type(parsed).__name__)
                    return {}
                return parsed
        except Exception as e:
            self._logger.warning(f"Failed to load mr_approvers from group config: {e}")
        return {}

    async def _load_assembly(self) -> Dict:
        """Read assembly definition from ocp-build-data releases.yml."""
        self._logger.info("Reading assembly '%s' from releases.yml...", self.assembly)

        build_data_path = self._working_dir / "ocp-build-data-read"
        build_data = GitRepository(build_data_path, dry_run=self.dry_run)
        await build_data.setup(self.build_data_repo_url)
        await build_data.fetch_switch_branch(self.group)

        releases_yaml_path = build_data_path / "releases.yml"
        if not releases_yaml_path.exists():
            raise click.ClickException(f"releases.yml not found in {self.build_data_repo_url} on branch {self.group}")

        releases_config = yaml.load(releases_yaml_path)
        assembly = releases_config.get('releases', {}).get(self.assembly)
        if assembly is None:
            raise click.ClickException(f"Assembly '{self.assembly}' not found in releases.yml")

        return assembly

    def _extract_operand_nvrs(self, assembly_config: Dict) -> List[str]:
        """Extract pinned operand NVRs from the assembly definition.

        Walks the members.images list and collects NVRs from is.nvr or is.nvr! entries.

        NOTE: For inherited (delta) assemblies, only NVRs explicitly listed
        in this assembly's members.images are returned. Parent chain
        resolution is not performed -- doozer/elliott commands handle
        inheritance internally via --assembly.
        """
        members_images = assembly_config.get('assembly', {}).get('members', {}).get('images', [])
        nvrs = []
        for entry in members_images:
            is_spec = entry.get('metadata', {}).get('is', {})
            nvr = is_spec.get('nvr') or is_spec.get('nvr!')
            if nvr:
                nvrs.append(nvr)
        return nvrs

    async def _find_existing_bundles(self) -> Dict[str, List[str]]:
        """Check for existing bundle builds that match the assembly's pinned operands.

        Uses ``elliott find-builds --all-image-types --json=-`` to discover
        bundles that already exist with the correct operand pins.

        Returns a dict with keys ``metadata`` (found bundle NVRs) and
        ``olm_builds_not_found`` (operator NVRs that still need bundles built).
        """
        self._logger.info("Checking for existing bundle builds for assembly '%s'...", self.assembly)

        cmd = self._elliott_base_command + [
            'find-builds',
            '--kind=image',
            '--all-image-types',
            '--json=-',
        ]

        if self.dry_run:
            self._logger.info("[DRY-RUN] Would run: %s", ' '.join(cmd))
            return {"metadata": [], "olm_builds_not_found": []}

        try:
            stdout, _ = exectools.cmd_assert(cmd)
        except Exception as e:
            self._logger.warning("find-builds failed, will rebuild all bundles: %s", e)
            return {"metadata": [], "olm_builds_not_found": []}

        if not stdout.strip():
            return {"metadata": [], "olm_builds_not_found": []}

        out = json.loads(stdout)
        result = {
            "metadata": out.get("olm_builds", []),
            "olm_builds_not_found": out.get("olm_builds_not_found", []),
            "olm_operator_nvrs": out.get("olm_operator_nvrs", []),
        }
        self._logger.info(
            "Found %d existing bundle NVRs; %d operators still need bundles",
            len(result["metadata"]),
            len(result["olm_builds_not_found"]),
        )
        return result

    def _konflux_build_args(self) -> List[str]:
        """Return --konflux-kubeconfig and --konflux-namespace args for doozer build commands."""
        product = self.product or "ocp"
        args: List[str] = []
        kubeconfig = resolve_konflux_kubeconfig_by_product(product)
        if kubeconfig:
            args.append(f"--konflux-kubeconfig={kubeconfig}")
        namespace = resolve_konflux_namespace_by_product(product)
        args.append(f"--konflux-namespace={namespace}")
        return args

    async def _trigger_bundle_build(self, operand_nvrs: List[str]) -> Tuple[List[str], List[str]]:
        """Build OLM bundles, reusing existing ones when possible.

        First calls ``_find_existing_bundles`` to see which bundles already
        exist with the correct operand pins.  Only operators listed in
        ``olm_builds_not_found`` whose NVR is pinned in the assembly are rebuilt.

        Returns ``(bundle_nvrs, assembly_operator_nvrs)`` where
        ``bundle_nvrs`` is the combined list of bundle NVRs (existing + newly
        built) and ``assembly_operator_nvrs`` is the list of OLM operator
        NVRs that are pinned in this assembly (used to scope FBC builds).
        """
        existing = await self._find_existing_bundles()
        existing_bundle_nvrs = existing["metadata"]
        missing_operator_nvrs = existing["olm_builds_not_found"]
        all_operator_nvrs = existing.get("olm_operator_nvrs", [])

        # Compute the assembly-scoped operator NVRs for downstream FBC builds.
        # elliott find-builds returns ALL OLM operators in the group, but LP
        # assemblies typically only pin a subset.  Compare by NVR name
        # component only — the release timestamp may differ between the
        # assembly-pinned NVR and the latest build returned by elliott.
        assembly_operator_nvrs: List[str] = []
        if operand_nvrs:
            pinned_names = {parse_nvr(nvr)['name'] for nvr in operand_nvrs}
            assembly_operator_nvrs = [nvr for nvr in all_operator_nvrs if parse_nvr(nvr)['name'] in pinned_names]

            # Also filter missing operators to assembly scope for bundle builds.
            scoped = [nvr for nvr in missing_operator_nvrs if parse_nvr(nvr)['name'] in pinned_names]
            if len(scoped) < len(missing_operator_nvrs):
                skipped = sorted(set(missing_operator_nvrs) - set(scoped))
                self._logger.info("Skipping %d operator(s) not pinned in assembly: %s", len(skipped), skipped)
            missing_operator_nvrs = scoped

        if existing_bundle_nvrs:
            self._logger.info(
                "Reusing %d existing bundle NVRs: %s",
                len(existing_bundle_nvrs),
                existing_bundle_nvrs,
            )

        if not missing_operator_nvrs:
            self._logger.info("All bundles already exist -- skipping bundle build")
            return existing_bundle_nvrs, assembly_operator_nvrs

        self._logger.info(
            "Building bundles for %d operators without existing bundles...",
            len(missing_operator_nvrs),
        )

        cmd = self._doozer_base_command + [
            'beta:images:konflux:bundle',
            *self._konflux_build_args(),
            '--output=json',
            '--',
            *missing_operator_nvrs,
        ]

        if self.dry_run:
            self._logger.info("[DRY-RUN] Would run: %s", ' '.join(cmd))
            return existing_bundle_nvrs, assembly_operator_nvrs

        rc, stdout, _ = await exectools.cmd_gather_async(cmd, check=False, stderr=None)
        if rc != 0:
            self._logger.error("Bundle build command exited with rc=%s", rc)

        try:
            output_data = json.loads(stdout)
        except json.JSONDecodeError:
            raise RuntimeError(f"Bundle build command failed (rc={rc}) and did not produce valid JSON output.")

        new_bundle_nvrs = output_data.get("nvrs", [])
        errors = output_data.get("errors", [])

        if errors:
            self._logger.warning(
                "Bundle build completed with %d failure(s) and %d success(es)",
                output_data.get("failed_count", 0),
                output_data.get("success_count", 0),
            )
            for error in errors:
                self._logger.error("Bundle build error: %s", error)

        all_bundle_nvrs = existing_bundle_nvrs + new_bundle_nvrs
        self._logger.info(
            "Bundle builds complete: %d existing + %d new = %d total",
            len(existing_bundle_nvrs),
            len(new_bundle_nvrs),
            len(all_bundle_nvrs),
        )
        return all_bundle_nvrs, assembly_operator_nvrs

    async def _load_ocp_target_versions(self) -> List[str]:
        """Read OCP_TARGET_VERSIONS from group config.

        Falls back to reading group.yml directly from ocp-build-data
        if doozer config:read-group fails (e.g. LP groups that doozer
        doesn't fully support yet).
        """
        try:
            group_config = await load_group_config(
                group=self.group,
                assembly=self.assembly,
                doozer_data_path=self.build_data_repo_url,
            )
            return group_config.get("OCP_TARGET_VERSIONS", [])
        except Exception as e:
            self._logger.warning(
                "doozer config:read-group failed (%s), reading group.yml directly",
                e,
            )

        build_data_path = self._working_dir / "ocp-build-data-group"
        build_data = GitRepository(build_data_path, dry_run=self.dry_run)
        await build_data.setup(self.build_data_repo_url)
        await build_data.fetch_switch_branch(self.group)

        group_yml = build_data_path / "group.yml"
        if group_yml.exists():
            group_config = stdlib_yaml.safe_load(group_yml.read_text())
            if isinstance(group_config, dict):
                return group_config.get("OCP_TARGET_VERSIONS", [])

        return []

    async def _trigger_fbc_build(self, operator_nvrs: Optional[List[str]] = None) -> Tuple[List[str], List[str]]:
        """Trigger FBC builds for each OCP target version.

        For non-OpenShift groups, reads ``OCP_TARGET_VERSIONS`` from the
        group config and builds one FBC per version using ``--major-minor``.
        Falls back to a single build if no target versions are configured.

        When *operator_nvrs* is provided, only the given operators are
        processed (passed as positional args to ``doozer beta:fbc:rebase-and-build``).

        Returns a tuple of (NVR list, pullspec list) produced across all versions.
        """
        self._logger.info("Triggering FBC builds for assembly '%s'...", self.assembly)

        version = self.assembly
        release_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        message = f"Rebase FBC segment with release {release_str}"

        ocp_target_versions = await self._load_ocp_target_versions()

        if ocp_target_versions:
            self._logger.info(
                "Building FBCs for %d OCP target versions: %s",
                len(ocp_target_versions),
                ocp_target_versions,
            )
            all_fbc_nvrs: List[str] = []
            all_fbc_pullspecs: List[str] = []
            for target_version in ocp_target_versions:
                self._logger.info("Building FBC for OCP %s...", target_version)
                cmd = self._doozer_base_command + [
                    'beta:fbc:rebase-and-build',
                    *self._konflux_build_args(),
                    f'--version={version}',
                    f'--release={release_str}',
                    f'--message={message}',
                    f'--major-minor={target_version}',
                    '--output=json',
                ]
                if operator_nvrs:
                    cmd.extend(['--', *operator_nvrs])

                if self.dry_run:
                    self._logger.info("[DRY-RUN] Would run: %s", ' '.join(cmd))
                    continue

                rc, stdout, _ = await exectools.cmd_gather_async(cmd, check=False, stderr=None)
                if rc != 0:
                    self._logger.error("FBC build for OCP %s exited with rc=%s", target_version, rc)
                try:
                    output_data = json.loads(stdout)
                except json.JSONDecodeError:
                    raise RuntimeError(
                        f"FBC build for OCP {target_version} failed (rc={rc}) and did not produce valid JSON output."
                    )
                nvrs = output_data.get("nvrs", [])
                pullspecs = output_data.get("pullspecs", [])
                errors = output_data.get("errors", [])
                if errors:
                    self._logger.warning(
                        "FBC build for OCP %s had %d error(s): %s",
                        target_version,
                        len(errors),
                        errors,
                    )
                all_fbc_nvrs.extend(nvrs)
                all_fbc_pullspecs.extend(pullspecs)

            self._logger.info(
                "FBC builds produced %d total NVRs across %d OCP versions", len(all_fbc_nvrs), len(ocp_target_versions)
            )
            return all_fbc_nvrs, all_fbc_pullspecs

        self._logger.info("No OCP_TARGET_VERSIONS configured; building single FBC")
        cmd = self._doozer_base_command + [
            'beta:fbc:rebase-and-build',
            *self._konflux_build_args(),
            f'--version={version}',
            f'--release={release_str}',
            f'--message={message}',
            '--output=json',
        ]
        if operator_nvrs:
            cmd.extend(['--', *operator_nvrs])

        if self.dry_run:
            self._logger.info("[DRY-RUN] Would run: %s", ' '.join(cmd))
            return [], []

        rc, stdout, _ = await exectools.cmd_gather_async(cmd, check=False, stderr=None)
        if rc != 0:
            self._logger.error("FBC build exited with rc=%s", rc)
        try:
            output_data = json.loads(stdout)
        except json.JSONDecodeError:
            raise RuntimeError(f"FBC build failed (rc={rc}) and did not produce valid JSON output.")
        fbc_nvrs = output_data.get("nvrs", [])
        fbc_pullspecs = output_data.get("pullspecs", [])
        errors = output_data.get("errors", [])
        if errors:
            self._logger.warning("FBC build had %d error(s): %s", len(errors), errors)
        self._logger.info("FBC builds produced %d NVRs", len(fbc_nvrs))
        return fbc_nvrs, fbc_pullspecs

    async def _create_snapshot(self, builds: List[str]) -> List[Snapshot]:
        """Create Konflux snapshot(s) from a list of build NVRs using elliott.

        ``elliott snapshot new`` groups builds by Konflux application and
        outputs one YAML document per application.  Returns one
        :class:`Snapshot` per document.
        """
        if not builds:
            return []

        self._logger.info("Creating Konflux snapshot for %d builds...", len(builds))

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            for nvr in builds:
                f.write(nvr + '\n')
            f.flush()
            temp_path = f.name

        cmd = self._elliott_base_command + [
            'snapshot',
            'new',
            f'--builds-file={temp_path}',
        ]
        quay_auth = os.getenv("QUAY_AUTH_FILE")
        if quay_auth:
            cmd.append(f'--pull-secret={quay_auth}')

        try:
            stdout, _ = exectools.cmd_assert(cmd)
        finally:
            os.unlink(temp_path)

        snapshots = []
        for doc in yaml.load_all(stdout):
            snapshots.append(Snapshot(spec=SnapshotSpec(**doc.get("spec")), nvrs=sorted(builds)))
        return snapshots

    def _create_shipment_config(
        self,
        kind: str,
        snapshot: Optional[Snapshot],
        release_notes: Optional[ReleaseNotes] = None,
    ) -> ShipmentConfig:
        """Create a shipment configuration for the given kind."""
        metadata = Metadata(
            product=self.product,
            application=snapshot.spec.application if snapshot else "default-app",
            group=self.group,
            assembly=self.assembly,
            fbc=kind == 'fbc',
        )

        stage_rpa = "n/a"
        prod_rpa = "n/a"
        if self.create_mr:
            try:
                config_path = self.shipment_data_repo._directory / "config.yaml"
                if config_path.exists():
                    with open(config_path, 'r') as f:
                        shipment_config = stdlib_yaml.safe_load(f) or {}
                    application = snapshot.spec.application if snapshot else "default-app"
                    app_env = shipment_config.get("applications", {}).get(application, {}).get("environments", {})
                    stage_rpa = app_env.get("stage", {}).get("releasePlan", "n/a")
                    prod_rpa = app_env.get("prod", {}).get("releasePlan", "n/a")
            except Exception as e:
                self._logger.warning("Failed to read shipment config: %s", e)

        environments = Environments(
            stage=ShipmentEnv(releasePlan=stage_rpa),
            prod=ShipmentEnv(releasePlan=prod_rpa),
        )

        data = None
        if kind == 'image' and release_notes:
            data = Data(releaseNotes=release_notes)

        shipment = Shipment(metadata=metadata, environments=environments, snapshot=snapshot, data=data)
        return ShipmentConfig(shipment=shipment)

    def _generate_release_notes(self) -> Optional[ReleaseNotes]:
        """Process JIRA bugs into release notes."""
        if not self.jira_bugs:
            return None

        jira_bugs_str = ",".join(self.jira_bugs)
        self._logger.info("Processing JIRA bugs for release notes: %s", jira_bugs_str)

        cmd = self._elliott_base_command + [
            'process-release-from-fbc-bugs',
            f'--jira-bugs={jira_bugs_str}',
        ]

        stdout, _ = exectools.cmd_assert(cmd)
        release_notes_data = stdlib_yaml.safe_load(stdout)
        return ReleaseNotes(**release_notes_data)

    def _load_release_notes_template(self) -> Optional[Dict]:
        """Load release notes template from ocp-build-data."""
        try:
            boilerplate = get_advisory_boilerplate(
                runtime=self,
                et_data={},
                art_advisory_key=self.product,
                errata_type="RHBA",
            )
        except Exception:
            return None

        try:
            group_content = self.get_file_from_branch(self.group, "group.yml")
            group_config = stdlib_yaml.safe_load(group_content)
            ocp_version = str(group_config.get("OCP_RELEASE_NOTES_VERSION", ""))
            if not ocp_version:
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
            return {
                field: formatter.format(boilerplate.get(field, ""), **replace_vars)
                for field in ("synopsis", "topic", "description", "solution")
            }
        except Exception:
            return None

    def get_file_from_branch(self, branch: str, filename: str, data_path: str | None = None) -> bytes:
        if data_path is None:
            data_path = self.build_data_repo_url
        try:
            data_path = data_path.removesuffix(".git")
            parts = data_path.rstrip("/").split("/")
            user, repo_name = parts[-2], parts[-1]
            github_client = get_github_client_for_org(user)
            repo = github_client.get_repo(f"{user}/{repo_name}")
            file_content = repo.get_contents(filename, ref=branch)
            return file_content.decoded_content
        except GithubException as e:
            raise ValueError(f"Failed to fetch {filename} from {data_path} branch {branch}: {e}")

    async def _setup_shipment_repo(self):
        if not self.create_mr:
            return
        await self.shipment_data_repo.setup(
            remote_url=self._basic_auth_url(self.shipment_data_repo_push_url, self.gitlab_token),
            upstream_remote_url=self.shipment_data_repo_pull_url,
        )
        await self.shipment_data_repo.fetch_switch_branch("main")

    async def _write_shipment_file(
        self,
        advisory_kind: str,
        shipment_config: ShipmentConfig,
        env: str,
        timestamp: str,
    ) -> str:
        if advisory_kind.startswith('fbc'):
            counter = advisory_kind[3:]
            filename = f"{self.assembly}.fbc.{timestamp}{counter}.yaml"
        else:
            filename = f"{self.assembly}.{advisory_kind}.{timestamp}.yaml"

        product = shipment_config.shipment.metadata.product
        group = shipment_config.shipment.metadata.group
        application = shipment_config.shipment.metadata.application
        relative_dir = Path("shipment") / product / group / application / env
        target_dir = self.shipment_data_repo._directory / relative_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        filepath = relative_dir / filename

        out = StringIO()
        yaml.dump(shipment_config.model_dump(exclude_unset=True, exclude_none=True), out)
        await self.shipment_data_repo.write_file(filepath, out.getvalue())
        return str(filepath)

    async def _create_shipment_mr(self, shipments_by_kind: Dict[str, ShipmentConfig]) -> Optional[str]:
        if not self.create_mr:
            return None

        self._logger.info("Creating shipment MR...")
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        source_branch = f"prepare-shipment-lp-{self.assembly}-{timestamp}"

        await self.shipment_data_repo.create_branch(source_branch)

        for kind, config in shipments_by_kind.items():
            filepath = await self._write_shipment_file(kind, config, "prod", timestamp)
            self._logger.info("Wrote shipment file: %s", filepath)

        await self.shipment_data_repo.add_all()
        await self.shipment_data_repo.log_diff()
        commit_msg = f"Add shipment configs for {self.product} {self.assembly}"
        pushed = await self.shipment_data_repo.commit_push(commit_msg, safe=True)
        if not pushed:
            raise ValueError("Failed to push shipment data. Nothing to commit.")

        source_project = self._get_gitlab_project(self.shipment_data_repo_push_url)
        target_project = self._get_gitlab_project(self.shipment_data_repo_pull_url)

        if self.target_release_date:
            mr_title = f"Draft: Shipment for {self.product} {self.assembly} (ship date: {self.target_release_date})"
        else:
            mr_title = f"Draft: Shipment for {self.product} {self.assembly}"

        mr_description = ""
        if self.job_url:
            mr_description += f"Created by job: {self.job_url}\n\n"
        mr_description += f"Shipment files for {self.product} {self.assembly} (prepare-release-lp)"

        if self.dry_run:
            self._logger.info("[DRY-RUN] Would create MR: %s", mr_title)
            return f"{self.gitlab_url}/placeholder/-/merge_requests/placeholder"

        mr = source_project.mergerequests.create(
            {
                'source_branch': source_branch,
                'target_project_id': target_project.id,
                'target_branch': 'main',
                'title': mr_title,
                'description': mr_description,
                'remove_source_branch': True,
            }
        )
        mr_url = mr.web_url
        self._logger.info("Created MR: %s", mr_url)

        approvers_config = await self._load_mr_approvers_from_group_config()
        if approvers_config:
            try:
                await self._gitlab.set_mr_approval_rules(mr_url, approvers_config)
            except Exception as e:
                self._logger.warning(f"Failed to set MR approval rules: {e}")

        self.shipment_mr_url = mr_url
        return mr_url

    async def _set_shipment_mr_ready(self):
        if not self.shipment_mr_url:
            return
        mr = await self._gitlab.set_mr_ready(self.shipment_mr_url)
        if mr and not self.dry_run:
            self._logger.info("Waiting 30s for MR update...")
            await asyncio.sleep(30)
            try:
                pipeline_url = await self._gitlab.trigger_ci_pipeline(mr)
                if pipeline_url:
                    self._logger.info("CI pipeline triggered: %s", pipeline_url)
            except Exception as e:
                self._logger.warning("Failed to trigger CI pipeline: %s", e)

    async def _update_assembly_with_shipment_url(self, shipment_mr_url: str) -> None:
        """Update the assembly in releases.yml with the shipment MR URL.

        Pushes a commit to ocp-build-data to record the shipment MR URL
        in the assembly's ``group.shipment.mr`` field.
        """
        ocp_build_data_repo_push_url = self.runtime.config["build_config"]["ocp_build_data_repo_push_url"]

        if self.dry_run:
            self._logger.warning(
                "[DRY RUN] Would update assembly %s with shipment MR URL: %s",
                self.assembly,
                shipment_mr_url,
            )
            return

        build_data_path = self._working_dir / "ocp-build-data-push"
        build_data = GitRepository(build_data_path, dry_run=self.dry_run)
        await build_data.setup(ocp_build_data_repo_push_url)
        await build_data.fetch_switch_branch(self.group)

        releases_yaml_path = build_data_path / "releases.yml"
        if not releases_yaml_path.exists():
            self._logger.warning("releases.yml not found; skipping shipment URL update")
            return

        releases_yaml = yaml.load(releases_yaml_path)
        assembly_entry = releases_yaml.get('releases', {}).get(self.assembly, {})
        assembly_def = assembly_entry.get('assembly', {})
        group_info = assembly_def.setdefault('group', {})
        shipment_info = group_info.setdefault('shipment', {})
        shipment_info['mr'] = shipment_mr_url

        yaml.dump(releases_yaml, releases_yaml_path)

        pushed = await build_data.commit_push(f"Update assembly {self.assembly}: add shipment MR URL")
        if pushed:
            self._logger.info("Updated releases.yml with shipment MR URL: %s", shipment_mr_url)
        else:
            self._logger.warning("No changes to commit when updating shipment MR URL")

    async def run(self) -> None:
        self._logger.info(
            "Starting prepare-release-lp for group=%s assembly=%s",
            self.group,
            self.assembly,
        )

        self._check_env_vars()
        self._setup_working_dir()

        self.product = await self._load_product_from_group_config()
        self._logger.info("Product: %s", self.product)

        if self.create_mr:
            await self._setup_shipment_repo()

        assembly_config = await self._load_assembly()
        operand_nvrs = self._extract_operand_nvrs(assembly_config)
        self._logger.info("Assembly contains %d pinned operand NVRs", len(operand_nvrs))

        bundle_nvrs, assembly_operator_nvrs = await self._trigger_bundle_build(operand_nvrs)
        self._logger.info("Bundle builds: %d NVRs", len(bundle_nvrs))

        fbc_nvrs, fbc_pullspecs = await self._trigger_fbc_build(assembly_operator_nvrs)
        self._logger.info("FBC builds: %d NVRs", len(fbc_nvrs))

        if len(fbc_pullspecs) > 1:
            self._logger.info("Validating FBC consistency across OCP target versions...")
            await validate_fbc_related_images(fbc_pullspecs, self.product)
        elif len(fbc_nvrs) > 1 and not fbc_pullspecs:
            self._logger.warning("FBC pullspecs not available; skipping FBC consistency validation")

        release_notes = self._generate_release_notes()

        template = self._load_release_notes_template()
        if template:
            if release_notes is None:
                release_notes = ReleaseNotes(type="RHBA")
            release_notes.synopsis = template["synopsis"]
            release_notes.topic = template["topic"]
            release_notes.description = template["description"]
            release_notes.solution = template["solution"]

        shipments_by_kind: Dict[str, ShipmentConfig] = {}

        if operand_nvrs:
            image_snapshots = await self._create_snapshot(operand_nvrs)
            for i, snapshot in enumerate(image_snapshots, start=1):
                key = 'image' if len(image_snapshots) == 1 else f'image{i:02d}'
                shipments_by_kind[key] = self._create_shipment_config(
                    'image',
                    snapshot,
                    release_notes=release_notes,
                )

        fbc_counter = 1
        for fbc_nvr in fbc_nvrs:
            fbc_snapshots = await self._create_snapshot([fbc_nvr])
            for snapshot in fbc_snapshots:
                shipments_by_kind[f'fbc{fbc_counter:02d}'] = self._create_shipment_config(
                    'fbc',
                    snapshot,
                )
                fbc_counter += 1

        if shipments_by_kind:
            if self.create_mr:
                mr_url = await self._create_shipment_mr(shipments_by_kind)
                if mr_url:
                    self._logger.info("Shipment MR: %s", mr_url)
                    await self._set_shipment_mr_ready()
                    await self._update_assembly_with_shipment_url(mr_url)
            else:
                timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
                for kind, config in shipments_by_kind.items():
                    filepath = await self._write_shipment_file(kind, config, "prod", timestamp)
                    self._logger.info("Wrote local shipment file: %s", filepath)

        self._logger.info(
            "prepare-release-lp completed for %s %s. %d shipment configs created.",
            self.product,
            self.assembly,
            len(shipments_by_kind),
        )


@cli.command("prepare-release-lp")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group to operate on (e.g. acm-2.17, mce-2.17)",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The named assembly to prepare (e.g. 2.17.3). Must exist in releases.yml.",
)
@click.option(
    "--build-data-repo-url",
    metavar="URL",
    default=None,
    help="ocp-build-data pull URL. Defaults to openshift-eng/ocp-build-data.",
)
@click.option(
    "--shipment-data-repo-url",
    metavar="URL",
    default=None,
    help="GitLab shipment data repo URL override.",
)
@click.option(
    "--create-mr",
    is_flag=True,
    help="Create a merge request in the shipment data repository (requires GITLAB_TOKEN).",
)
@click.option(
    "--jira-bugs",
    default=None,
    help="Comma-separated JIRA issue IDs for release notes.",
)
@click.option(
    "--target-release-date",
    default=None,
    help="Target ship date (e.g. 2026-Mar-31 or 2026-03-31).",
)
@pass_runtime
@click_coroutine
async def prepare_release_lp(
    runtime: Runtime,
    group: str,
    assembly: str,
    build_data_repo_url: Optional[str],
    shipment_data_repo_url: Optional[str],
    create_mr: bool,
    jira_bugs: Optional[str],
    target_release_date: Optional[str],
):
    """
    Prepare a Layered Product release from a named assembly.

    Reads the assembly from releases.yml, triggers OLM bundle and FBC builds
    using the pinned operand NVRs, then creates shipment configurations and
    optionally a GitLab MR.

    \b
    # Prepare release from assembly:
    $ artcd prepare-release-lp -g acm-2.17 --assembly 2.17.3 --create-mr

    \b
    # With JIRA bugs for release notes:
    $ artcd prepare-release-lp -g acm-2.17 --assembly 2.17.3 \\
        --create-mr --jira-bugs ACM-1234,ACM-5678
    """
    jira_bugs_list = None
    if jira_bugs:
        jira_bugs_list = [j.strip() for j in jira_bugs.split(',') if j.strip()]
        if not jira_bugs_list:
            raise click.ClickException("--jira-bugs was provided but contains no valid JIRA IDs")

    normalized_date = None
    if target_release_date:
        normalized_date = _normalize_release_date(target_release_date)

    pipeline = PrepareReleaseLPPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        build_data_repo_url=build_data_repo_url,
        shipment_data_repo_url=shipment_data_repo_url,
        create_mr=create_mr,
        jira_bugs=jira_bugs_list,
        target_release_date=normalized_date,
    )

    await pipeline.run()
