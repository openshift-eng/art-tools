import logging
import os
import re
from typing import Dict, List, Optional, Tuple

import click
from artcommonlib import exectools
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import merge_objects, new_roundtrip_yaml_handler, split_git_url

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.fbc_util import (
    extract_fbc_labels,
    extract_ocp_version_from_nvr,
    validate_fbc_related_images,
)
from pyartcd.git import GitRepository
from pyartcd.runtime import Runtime

yaml = new_roundtrip_yaml_handler()
logger = logging.getLogger(__name__)


class GenAssemblyLPPipeline:
    """
    Pipeline for generating assembly definitions for Layered Products (ACM, MCE, etc.)
    from FBC images.

    This is the LP equivalent of the OCP gen-assembly pipeline. Instead of introspecting
    nightlies and computing a basis brew event, it extracts operand NVRs from FBC images
    and pins them explicitly in the assembly definition.

    Supports assembly inheritance via --basis-assembly for lightweight z-stream releases.
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        fbc_pullspecs: Optional[List[str]] = None,
        basis_assembly: Optional[str] = None,
        include: Optional[List[str]] = None,
        extra_image_nvrs: Optional[List[str]] = None,
        data_path: Optional[str] = None,
    ) -> None:
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.fbc_pullspecs = fbc_pullspecs or []
        self.basis_assembly = basis_assembly
        self.include = include or []
        self.extra_image_nvrs = extra_image_nvrs or []
        self.data_path = data_path or constants.OCP_BUILD_DATA_URL

        self._working_dir = self.runtime.working_dir.absolute()
        self._logger = logging.getLogger(__name__)

        self.product: Optional[str] = None

        if not self.fbc_pullspecs and not self.basis_assembly:
            raise ValueError("At least one of --fbc-pullspecs or --basis-assembly must be provided")

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

    def _is_nvr_from_current_group(self, nvr: str) -> bool:
        """Filter out NVRs from other groups based on version matching."""
        try:
            nvr_dict = parse_nvr(nvr)
            nvr_version = nvr_dict.get('version', '').lstrip('v')
            assembly_version = self.assembly.lstrip('v')

            nvr_parts = nvr_version.split('.')
            assembly_parts = assembly_version.split('.')

            if len(assembly_parts) < 2 or len(nvr_parts) < 2:
                return nvr_version.startswith(assembly_version)

            return nvr_parts[0] == assembly_parts[0] and nvr_parts[1] == assembly_parts[1]
        except ValueError:
            return True

    def _categorize_nvrs(self, nvrs: List[str]) -> Dict[str, List[str]]:
        """Split NVRs into image, bundle, fbc, and external categories.

        Bundles (metadata containers) and FBCs are excluded from the assembly
        because ``prepare-release-lp`` rebuilds them from the pinned operands.
        """
        categorized: Dict[str, List[str]] = {"image": [], "bundle": [], "fbc": [], "external": []}
        for nvr in nvrs:
            nvr_dict = parse_nvr(nvr)
            component_name = nvr_dict['name']
            if component_name.endswith('-fbc'):
                categorized["fbc"].append(nvr)
            elif '-metadata-container' in component_name:
                categorized["bundle"].append(nvr)
            elif self._is_nvr_from_current_group(nvr):
                categorized["image"].append(nvr)
            else:
                categorized["external"].append(nvr)
        return categorized

    async def _extract_nvrs_from_fbc(self) -> Dict[str, str]:
        """
        Extract operand NVRs from FBC pullspecs and return a dict of distgit_key -> NVR.

        When multiple FBC pullspecs are provided (one per OCP target version),
        validates that all FBCs for the same operator contain identical related images.
        """
        if len(self.fbc_pullspecs) > 1:
            all_related_nvrs = await validate_fbc_related_images(self.fbc_pullspecs, self.product)
        else:
            from elliottlib.util import extract_nvrs_from_fbc as _extract

            all_related_nvrs = []
            for fbc_pullspec in self.fbc_pullspecs:
                related_nvrs = await _extract(fbc_pullspec, self.product)
                all_related_nvrs.extend(related_nvrs)

        all_related_nvrs = sorted(set(all_related_nvrs))
        categorized = self._categorize_nvrs(all_related_nvrs)

        image_nvrs = categorized["image"]
        if categorized["bundle"]:
            self._logger.info(
                "Filtered %d bundle/metadata NVRs (rebuilt by prepare-release-lp): %s",
                len(categorized["bundle"]),
                categorized["bundle"],
            )
        if categorized["fbc"]:
            self._logger.info(
                "Filtered %d FBC NVRs (rebuilt by prepare-release-lp): %s",
                len(categorized["fbc"]),
                categorized["fbc"],
            )
        if categorized["external"]:
            self._logger.info(
                "Filtered %d external dependencies: %s",
                len(categorized["external"]),
                categorized["external"],
            )

        return self._nvrs_to_distgit_map(image_nvrs)

    def _nvrs_to_distgit_map(self, nvrs: List[str]) -> Dict[str, str]:
        """Convert a list of NVRs to a dict of distgit_key -> NVR.

        Uses the component name (the 'name' field from parsing the NVR) as
        the distgit_key.
        """
        result: Dict[str, str] = {}
        for nvr in nvrs:
            nvr_dict = parse_nvr(nvr)
            distgit_key = nvr_dict['name']
            if distgit_key in result:
                self._logger.warning(
                    "Duplicate distgit_key '%s': keeping %s, discarding %s",
                    distgit_key,
                    result[distgit_key],
                    nvr,
                )
                continue
            result[distgit_key] = nvr
        return result

    def _apply_includes(self, operand_map: Dict[str, str]) -> Dict[str, str]:
        """Apply --include overrides (swap builds) to the operand map."""
        for include_spec in self.include:
            if '=' not in include_spec:
                raise click.ClickException(f"Invalid --include format: '{include_spec}'. Expected 'distgit_key=nvr'")
            distgit_key, nvr = include_spec.split('=', 1)
            if distgit_key not in operand_map:
                raise click.ClickException(
                    f"Cannot swap '{distgit_key}': not found in the extracted operand set. "
                    f"Available keys: {sorted(operand_map.keys())}. "
                    f"To add a new operand, use --extra-image-nvrs instead."
                )
            self._logger.info("Swapping %s: %s -> %s", distgit_key, operand_map[distgit_key], nvr)
            operand_map[distgit_key] = nvr
        return operand_map

    def _apply_extra_image_nvrs(self, operand_map: Dict[str, str]) -> Dict[str, str]:
        """Add extra image NVRs to the operand map."""
        for nvr in self.extra_image_nvrs:
            if not self._is_nvr_from_current_group(nvr):
                nvr_dict = parse_nvr(nvr)
                raise click.ClickException(
                    f"Extra image NVR '{nvr}' has version '{nvr_dict.get('version', '?')}' "
                    f"which does not match assembly '{self.assembly}'. "
                    f"Only NVRs matching the group's major.minor version are allowed."
                )
            nvr_dict = parse_nvr(nvr)
            distgit_key = nvr_dict['name']
            if distgit_key in operand_map:
                self._logger.info(
                    "Extra NVR overrides existing entry for %s: %s -> %s",
                    distgit_key,
                    operand_map[distgit_key],
                    nvr,
                )
            operand_map[distgit_key] = nvr
        return operand_map

    async def _resolve_parent_operands(self) -> Dict[str, str]:
        """Resolve the full operand set from the parent (basis) assembly.

        NOTE: Only resolves one level of inheritance. Multi-level chains
        (A->B->C) are not supported; the basis assembly must list all
        operands explicitly.
        """
        self._logger.info("Resolving operand set from basis assembly '%s'...", self.basis_assembly)

        build_data_path = self._working_dir / "ocp-build-data-read"
        build_data = GitRepository(build_data_path, dry_run=self.runtime.dry_run)
        await build_data.setup(self.data_path)
        await build_data.fetch_switch_branch(self.group)

        releases_yaml_path = build_data_path / "releases.yml"
        if not releases_yaml_path.exists():
            raise click.ClickException(f"releases.yml not found in {self.data_path} on branch {self.group}")

        releases_config = yaml.load(releases_yaml_path)

        parent_assembly = releases_config.get('releases', {}).get(self.basis_assembly)
        if parent_assembly is None:
            raise click.ClickException(f"Basis assembly '{self.basis_assembly}' not found in releases.yml")

        members_images = parent_assembly.get('assembly', {}).get('members', {}).get('images', [])
        operand_map: Dict[str, str] = {}
        for entry in members_images:
            distgit_key = entry.get('distgit_key')
            nvr = entry.get('metadata', {}).get('is', {}).get('nvr')
            if distgit_key and nvr:
                operand_map[distgit_key] = nvr

        self._logger.info(
            "Resolved %d operands from basis assembly '%s'",
            len(operand_map),
            self.basis_assembly,
        )
        return operand_map

    async def _build_fbc_pullspecs_map(self) -> Dict[str, List[str]]:
        """Map FBC pullspecs to their OCP target versions.

        Extracts the OCP version from the NVR embedded in each FBC image's labels.
        Returns a dict of ``{"4.18": ["quay.io/...", ...], "4.19": [...], ...}``.
        Multiple operators targeting the same OCP version are grouped together.
        Falls back to an empty dict (caller stores a flat list) if OCP versions
        cannot be determined.
        """
        fbc_map: Dict[str, List[str]] = {}
        for pullspec in self.fbc_pullspecs:
            labels = await extract_fbc_labels(pullspec)
            nvr = labels.get('nvr')
            ocp_version = extract_ocp_version_from_nvr(nvr) if nvr else None
            if ocp_version:
                fbc_map.setdefault(ocp_version, []).append(pullspec)
            else:
                self._logger.warning(
                    "Could not determine OCP target version from FBC %s (nvr=%s); falling back to flat list",
                    pullspec,
                    nvr,
                )
                return {}
        return fbc_map

    def _generate_assembly_definition(
        self,
        operand_map: Dict[str, str],
        parent_operand_map: Optional[Dict[str, str]] = None,
        fbc_pullspecs_map: Optional[Dict[str, List[str]]] = None,
    ) -> dict:
        """Generate the assembly YAML definition."""

        basis: Dict = {}
        if self.basis_assembly:
            basis['assembly'] = self.basis_assembly
        if self.fbc_pullspecs:
            if fbc_pullspecs_map:
                basis['fbc_pullspecs'] = {
                    ver: specs if len(specs) > 1 else specs[0] for ver, specs in fbc_pullspecs_map.items()
                }
            else:
                basis['fbc_pullspecs'] = list(self.fbc_pullspecs)

        group_info: Dict = {
            'shipment': {
                'advisories': [
                    {'kind': 'image'},
                    {'kind': 'fbc'},
                ],
            },
        }

        if self.basis_assembly and parent_operand_map is not None:
            members_images = []
            for distgit_key, nvr in sorted(operand_map.items()):
                if parent_operand_map.get(distgit_key) != nvr:
                    members_images.append(
                        {
                            'distgit_key': distgit_key,
                            'metadata': {'is': {'nvr!': nvr}},
                        }
                    )

            if not members_images and not self.fbc_pullspecs:
                group_info = {}
        else:
            members_images = [
                {
                    'distgit_key': distgit_key,
                    'metadata': {'is': {'nvr': nvr}},
                }
                for distgit_key, nvr in sorted(operand_map.items())
            ]

        assembly_def: Dict = {
            'type': 'standard',
            'basis': basis,
        }
        if group_info:
            assembly_def['group'] = group_info
        assembly_def['members'] = {
            'images': members_images,
        }

        return {
            'releases': {
                self.assembly: {
                    'assembly': assembly_def,
                },
            },
        }

    async def _create_or_update_pull_request(self, assembly_definition: dict):
        """Create or update a pull request on ocp-build-data with the assembly definition."""
        self._logger.info("Creating ocp-build-data PR...")

        ocp_build_data_repo_push_url = self.runtime.config["build_config"]["ocp_build_data_repo_push_url"]
        match = re.search(r"github\.com[:/](.+)/(.+?)(?:\.git)?$", ocp_build_data_repo_push_url)
        if not match:
            raise ValueError(
                f"Couldn't create a pull request: {ocp_build_data_repo_push_url} is not a valid github repo"
            )

        title = f"Add LP assembly {self.assembly} for {self.group}"
        body = f"Generated by `gen-assembly-lp` for group=`{self.group}` assembly=`{self.assembly}`."
        if self.basis_assembly:
            body += f"\nInherits from basis assembly: `{self.basis_assembly}`"

        build_url = os.environ.get("BUILD_URL")
        if build_url:
            body += f"\n\n**Jenkins job:** [{build_url}]({build_url})"

        if self.fbc_pullspecs:
            body += f"\n\n**FBC pullspecs:** {len(self.fbc_pullspecs)} provided"

        branch = f"auto-gen-assembly-lp-{self.group}-{self.assembly}"
        head = f"{match[1]}:{branch}"
        base = self.group

        if self.runtime.dry_run:
            self._logger.warning(
                "[DRY RUN] Would have created PR with head '%s', base '%s', title '%s'",
                head,
                base,
                title,
            )
            return None

        build_data_path = self._working_dir / "ocp-build-data-push"
        build_data = GitRepository(build_data_path, dry_run=self.runtime.dry_run)
        await build_data.setup(ocp_build_data_repo_push_url)
        await build_data.fetch_switch_branch(branch, base)

        releases_yaml_path = build_data_path / "releases.yml"
        releases_yaml = yaml.load(releases_yaml_path) if releases_yaml_path.exists() else {}
        releases_yaml = merge_objects(assembly_definition, releases_yaml)
        yaml.dump(releases_yaml, releases_yaml_path)

        pushed = await build_data.commit_push(f"{title}\n{body}")
        if not pushed:
            self._logger.warning("PR is not created: Nothing to commit.")
            return None

        _, owner, repo_name = split_git_url(ocp_build_data_repo_push_url)
        gh_repo = get_github_client_for_org(owner).get_repo(f"{owner}/{repo_name}")
        existing_prs = list(gh_repo.get_pulls(state="open", base=base, head=head))
        if not existing_prs:
            result = gh_repo.create_pull(
                head=head,
                base=base,
                title=title,
                body=body,
                maintainer_can_modify=True,
            )
        else:
            pr = existing_prs[0]
            pr.edit(title=title, body=body)
            result = pr

        self._logger.info("Pull request: %s", result.html_url)
        return result

    async def run(self) -> dict:
        """Execute the gen-assembly-lp pipeline.

        Returns the generated assembly definition.
        """
        self._logger.info(
            "Starting gen-assembly-lp for group=%s assembly=%s",
            self.group,
            self.assembly,
        )

        self.product = await self._load_product_from_group_config()
        self._logger.info("Product: %s", self.product)

        parent_operand_map: Optional[Dict[str, str]] = None

        if self.basis_assembly:
            parent_operand_map = await self._resolve_parent_operands()
            if self.fbc_pullspecs:
                operand_map = await self._extract_nvrs_from_fbc()
            else:
                operand_map = dict(parent_operand_map)
        else:
            operand_map = await self._extract_nvrs_from_fbc()

        operand_map = self._apply_includes(operand_map)
        operand_map = self._apply_extra_image_nvrs(operand_map)

        self._logger.info("Final operand set: %d images", len(operand_map))
        for key, nvr in sorted(operand_map.items()):
            self._logger.info("  %s = %s", key, nvr)

        fbc_pullspecs_map: Optional[Dict[str, str]] = None
        if self.fbc_pullspecs:
            fbc_pullspecs_map = await self._build_fbc_pullspecs_map()

        assembly_definition = self._generate_assembly_definition(
            operand_map,
            parent_operand_map,
            fbc_pullspecs_map,
        )

        if self.runtime.dry_run:
            from io import StringIO

            out = StringIO()
            yaml.dump(dict(assembly_definition), out)
            self._logger.info("Generated assembly definition:\n%s", out.getvalue())

        result = await self._create_or_update_pull_request(assembly_definition)
        if result:
            self._logger.info("PR created/updated: %s", result.html_url)

        return assembly_definition


@cli.command("gen-assembly-lp")
@click.option(
    "-g",
    "--group",
    metavar='NAME',
    required=True,
    help="The group to operate on (e.g. acm-2.17, mce-2.17, oadp-1.5)",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly name to generate (e.g. 2.17.3)",
)
@click.option(
    "--fbc-pullspecs",
    metavar="FBC_PULLSPECS",
    default="",
    help="Comma-separated FBC image pullspecs to extract operand NVRs from. "
    "Provide one pullspec per OCP target version for multi-OCP products. "
    "When multiple are provided, related images are validated for consistency "
    "across OCP versions. Required unless --basis-assembly is provided.",
)
@click.option(
    "--basis-assembly",
    metavar="ASSEMBLY_NAME",
    default=None,
    help="Inherit from an existing assembly. When set, the new assembly inherits all "
    "operand pins from the parent. Use --include to swap specific builds.",
)
@click.option(
    "--include",
    metavar="DISTGIT_KEY=NVR",
    multiple=True,
    help="Swap a specific operand build. Format: distgit_key=nvr. Can be specified multiple times.",
)
@click.option(
    "--extra-image-nvrs",
    metavar="EXTRA_IMAGE_NVRS",
    default="",
    help="Comma-separated extra operand NVRs not in FBC catalog.",
)
@click.option(
    "--data-path",
    metavar="PATH",
    default=None,
    help="ocp-build-data URL or local path.",
)
@pass_runtime
@click_coroutine
async def gen_assembly_lp(
    runtime: Runtime,
    group: str,
    assembly: str,
    fbc_pullspecs: str,
    basis_assembly: Optional[str],
    include: Tuple[str, ...],
    extra_image_nvrs: str,
    data_path: Optional[str],
):
    """
    Generate an assembly definition for Layered Products from FBC images.

    This command extracts operand NVRs from FBC images (excluding bundle and FBC
    artifacts themselves), applies any --include overrides, and creates a PR to
    ocp-build-data with the assembly definition in releases.yml.

    Supports assembly inheritance: use --basis-assembly to create lightweight
    z-stream assemblies that only record the delta from a parent.

    \b
    # Fresh assembly from FBC:
    $ artcd gen-assembly-lp -g acm-2.17 --assembly 2.17.3 \\
        --fbc-pullspecs quay.io/.../acm-fbc@sha256:abc...

    \b
    # Inherit from previous release, swap one build:
    $ artcd gen-assembly-lp -g acm-2.17 --assembly 2.17.4 \\
        --basis-assembly 2.17.3 \\
        --include search-v2-api=search-v2-api-container-2.17.4-1

    \b
    # Re-extract from new FBC but inherit group config:
    $ artcd gen-assembly-lp -g acm-2.17 --assembly 2.17.4 \\
        --basis-assembly 2.17.3 \\
        --fbc-pullspecs quay.io/.../acm-fbc@sha256:def...
    """
    fbc_pullspecs_list = [s.strip() for s in fbc_pullspecs.split(',') if s.strip()]
    extra_nvrs_list = [s.strip() for s in extra_image_nvrs.split(',') if s.strip()]

    if not fbc_pullspecs_list and not basis_assembly:
        raise click.ClickException("At least one of --fbc-pullspecs or --basis-assembly must be provided")

    pipeline = GenAssemblyLPPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        fbc_pullspecs=fbc_pullspecs_list,
        basis_assembly=basis_assembly,
        include=list(include),
        extra_image_nvrs=extra_nvrs_list,
        data_path=data_path,
    )

    await pipeline.run()
