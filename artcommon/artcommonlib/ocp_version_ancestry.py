"""
OpenShift Container Platform (OCP) version ancestry and upgrade path calculations.

This module handles non-standard OCP version transitions (e.g., 4.22→5.0, bridge releases)
and determines which previous major.minor versions a target OCP version can upgrade from.

Key components:
- SuggestionsSpec: Pydantic model for version constraints
- BuildSuggestions: Pydantic model for complete build-suggestions structure
- get_build_suggestions_async(): Fetch and validate Cincinnati build-suggestions YAML files
- get_previous_minor_version(): Determine previous version using three-tier strategy
- get_cincinnati_channels(): Get Cincinnati channel names for a version
- get_release_calc_previous(): Calculate which versions can upgrade to a target version
"""

import functools
from typing import Any, Optional

import httpx
import semver
import yaml
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.util import extract_version_fields
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class SuggestionsSpec(BaseModel):
    """
    Version constraints specification from Cincinnati build-suggestions.

    These constraints define which versions can upgrade to a target version:
    - minor_* fields control upgrades from the PREVIOUS minor version (e.g., 4.22 → 5.0)
    - z_* fields control upgrades within the CURRENT minor version (e.g., 5.0.0 → 5.0.1)

    All version strings must be valid semver (e.g., '4.22.0-rc.0', '5.0.9999').
    minor_max and z_max are optional; if omitted, all versions >= the corresponding
    min with the same major.minor are included.
    """

    minor_min: str = Field(
        ...,
        description="Minimum version from previous minor to include (e.g., '4.22.0-rc.0')",
    )
    minor_max: Optional[str] = Field(
        None,
        description="Maximum version from previous minor, typically 'X.Y.9999' as placeholder. "
        "If omitted, all versions >= minor_min with the same major.minor are included.",
    )
    minor_block_list: list[str] = Field(
        ...,
        description="Explicitly blocked versions from previous minor (empty list if none)",
    )
    z_min: str = Field(
        ...,
        description="Minimum version from current minor to include (e.g., '5.0.0-ec.0')",
    )
    z_max: Optional[str] = Field(
        None,
        description="Maximum version from current minor, typically 'X.Y.9999' as placeholder. "
        "If omitted, all versions >= z_min with the same major.minor are included.",
    )
    z_block_list: list[str] = Field(
        ...,
        description="Explicitly blocked versions from current minor (empty list if none)",
    )

    @field_validator('minor_min', 'minor_max', 'z_min', 'z_max')
    @classmethod
    def validate_semver(cls, v: Optional[str]) -> Optional[str]:
        """Validate that version strings are valid semver."""
        if v is None:
            return v
        try:
            semver.VersionInfo.parse(v)
        except ValueError as e:
            raise ValueError(f"Invalid semver format '{v}': {e}") from e
        return v

    @field_validator('minor_block_list', 'z_block_list')
    @classmethod
    def validate_semver_list(cls, versions: list[str]) -> list[str]:
        """Validate that all versions in block lists are valid semver."""
        for v in versions:
            try:
                semver.VersionInfo.parse(v)
            except ValueError as e:
                raise ValueError(f"Invalid semver format in block list '{v}': {e}") from e
        return versions


class BuildSuggestions(BaseModel):
    """
    Complete build-suggestions structure for a specific OCP minor version.

    The 'default' section applies to all architectures unless overridden by
    architecture-specific sections (e.g., 's390x', 'aarch64').

    Example YAML structure:
        default:
          minor_min: "4.22.0-rc.0"
          minor_max: "4.22.9999"
          minor_block_list: []
          z_min: "5.0.0-ec.0"
          z_max: "5.0.9999"
          z_block_list: []
        s390x:
          minor_min: "4.22.1"
          minor_max: "4.22.9999"
          minor_block_list: []
          z_min: "5.0.0-ec.0"
          z_max: "5.0.9999"
          z_block_list: []
    """

    default: SuggestionsSpec = Field(
        ...,
        description="Default constraints applied to all architectures",
    )

    # Architecture-specific overrides as additional fields
    model_config = {"extra": "allow"}

    @model_validator(mode="before")
    @classmethod
    def parse_architecture_overrides(cls, data: Any) -> Any:
        """
        Parse architecture-specific overrides as SuggestionsSpec objects.

        Pydantic's extra="allow" keeps extra fields as-is (dicts). This validator
        converts architecture override dicts into SuggestionsSpec objects.
        """
        if not isinstance(data, dict):
            return data

        # Parse all non-default fields as SuggestionsSpec
        parsed = {}
        for key, value in data.items():
            if key == "default":
                parsed[key] = value  # Let Pydantic handle default field normally
            elif isinstance(value, dict):
                # Parse architecture override as SuggestionsSpec
                parsed[key] = SuggestionsSpec.model_validate(value)
            else:
                raise ValueError(
                    f"Architecture override '{key}' must be a mapping of version constraints, "
                    f"got {type(value).__name__}"
                )

        return parsed

    def get_for_arch(self, arch: str) -> SuggestionsSpec:
        """
        Get constraints for a specific architecture, falling back to default.

        :param arch: Architecture name (e.g., 'amd64', 's390x', 'aarch64')
        :return: SuggestionsSpec for the architecture
        """
        # Check if architecture-specific override exists
        if hasattr(self, arch) and arch != 'default':
            return getattr(self, arch)
        return self.default


async def get_build_suggestions_async(
    major: int,
    minor: int,
    suggestions_url: str = 'https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/',
    timeout: float = 30.0,
) -> BuildSuggestions:
    """
    Asynchronously fetch and parse build suggestions from Cincinnati graph-data repository.

    Loads suggestions_url/{major}.{minor}.yaml and returns a validated BuildSuggestions object.
    All version strings are validated as proper semver during parsing.

    :param major: Major version (e.g., 5)
    :param minor: Minor version (e.g., 0)
    :param suggestions_url: Base URL to Cincinnati build-suggestions directory
    :param timeout: HTTP request timeout in seconds
    :return: BuildSuggestions object with default and architecture-specific constraints
    :raises httpx.HTTPError: If the HTTP request fails (404, network errors, etc.)
    :raises ValueError: If YAML parsing or validation fails
    """
    # Ensure URL doesn't have double slashes
    base_url = suggestions_url.rstrip('/')
    url = f'{base_url}/{major}.{minor}.yaml'

    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=timeout)
        response.raise_for_status()

        # Parse YAML
        try:
            data = yaml.safe_load(response.text)
        except yaml.YAMLError as e:
            raise ValueError(
                f"Failed to parse YAML from build-suggestions file {major}.{minor}.yaml at {url}. "
                f"The file contains invalid YAML syntax. "
                f"Please contact the OTA (OpenShift Update Architecture) team to fix the build-suggestions file. "
                f"YAML error: {e}"
            ) from e

        # Validate against Pydantic model
        try:
            return BuildSuggestions.model_validate(data)
        except ValidationError as e:
            raise ValueError(
                f"Failed to validate build-suggestions for {major}.{minor} from {url}. "
                f"The YAML structure is invalid or contains incorrect version strings. "
                f"Please contact the OTA (OpenShift Update Architecture) team to fix the build-suggestions file. "
                f"Validation error: {e}"
            ) from e


def get_previous_minor_version_from_suggestions(
    suggestions: BuildSuggestions,
    go_arch: str = 'default',
) -> tuple[int, int]:
    """
    Extract the previous major.minor version from build-suggestions by parsing minor_min.

    This is the PRIMARY strategy for determining the previous version, as the
    build-suggestions YAML explicitly defines which previous versions can upgrade.

    Example:
        If 5.0.yaml contains minor_min: "4.22.0-rc.0", this returns (4, 22).

    Validation:
        - minor_min and minor_max must have the same major.minor version
        - Example: minor_min="4.22.0" and minor_max="4.22.9999" is valid
        - Example: minor_min="4.22.0" and minor_max="4.23.0" is INVALID

    :param suggestions: BuildSuggestions object (already fetched and validated)
    :param go_arch: Go architecture name (e.g., 'amd64', 's390x'), defaults to 'default'
    :return: Tuple of (prev_major, prev_minor)
    :raises ValueError: If minor_min/minor_max cannot be parsed or have different minor versions
    """
    spec = suggestions.get_for_arch(go_arch)

    try:
        min_info = semver.VersionInfo.parse(spec.minor_min)
    except (ValueError, AttributeError) as e:
        raise ValueError(f"Cannot parse minor_min '{spec.minor_min}' as valid semver. Error: {e}") from e

    # If minor_max is provided, validate it has the same major.minor as minor_min
    if spec.minor_max is not None:
        try:
            max_info = semver.VersionInfo.parse(spec.minor_max)
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Cannot parse minor_max '{spec.minor_max}' as valid semver. Error: {e}") from e
        if (min_info.major, min_info.minor) != (max_info.major, max_info.minor):
            raise ValueError(
                f"Build-suggestions constraint error: minor_min and minor_max must have the same major.minor version. "
                f"Found minor_min='{spec.minor_min}' ({min_info.major}.{min_info.minor}) "
                f"and minor_max='{spec.minor_max}' ({max_info.major}.{max_info.minor}). "
                f"Please contact the OTA team to fix the build-suggestions file."
            )

    return (min_info.major, min_info.minor)


def get_cincinnati_channels(major: int, minor: int) -> list[str]:
    """
    Returns Cincinnati graph channels for a release in promotion order.

    :param major: Major version for release
    :param minor: Minor version for release
    :return: List of channel names (e.g., ['candidate-4.16', 'fast-4.16', 'stable-4.16'])
    :raises ValueError: If major version is less than 4 (Cincinnati channels only exist for OCP 4+)
    """
    if major < 4:
        raise ValueError(f'Cincinnati channels are only available for OCP 4.x and later (requested: {major}.{minor})')

    # Special case: OCP 4.1 used different channel names
    if major == 4 and minor == 1:
        prefixes = ['prerelease', 'stable']
    else:
        # Standard channel names for all other versions (4.2+, 5.x+)
        prefixes = ['candidate', 'fast', 'stable']

    return [f'{prefix}-{major}.{minor}' for prefix in prefixes]


def sort_semver(versions: list[str]) -> list[str]:
    """
    Sort a list of semantic version strings in descending order.

    :param versions: List of version strings (e.g., ['4.22.0', '4.22.1', '5.0.0'])
    :return: Sorted list in descending order (newest first)
    """
    return sorted(versions, key=functools.cmp_to_key(semver.compare), reverse=True)


async def get_channel_versions_async(
    channel: str,
    go_arch: str,
    graph_url: str = 'https://api.openshift.com/api/upgrades_info/v1/graph',
    timeout: float = 30.0,
) -> tuple[list[str], dict[str, list[str]]]:
    """
    Query Cincinnati and return versions and edges for a channel.

    :param channel: The name of the channel to inspect (e.g., 'candidate-4.22')
    :param go_arch: Go architecture name (e.g., 'amd64', 's390x', 'aarch64')
    :param graph_url: Cincinnati graph URL to query
    :param timeout: HTTP request timeout in seconds
    :return: Tuple of (versions_descending, edge_map) where edge_map maps version -> list of versions it upgrades TO
    """
    url = f'{graph_url}?arch={go_arch}&channel={channel}'
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers={'Accept': 'application/json'}, timeout=timeout)
        response.raise_for_status()

    graph = response.json()
    versions = [node['version'] for node in graph['nodes']]
    descending_versions = sort_semver(versions)

    edges: dict[str, list[str]] = {v: [] for v in versions}
    for edge_def in graph['edges']:
        from_ver = versions[edge_def[0]]
        to_ver = versions[edge_def[1]]
        edges[from_ver].append(to_ver)

    return descending_versions, edges


def _version_in_range(version: str, v_min: str, v_max: Optional[str]) -> bool:
    """Check if version is within [v_min, v_max] range (inclusive).

    If v_max is None, includes all versions >= v_min with the same major.minor.
    """
    v_info = semver.VersionInfo.parse(version)
    min_info = semver.VersionInfo.parse(v_min)
    if v_info < min_info:
        return False
    if v_max is not None:
        return v_info <= semver.VersionInfo.parse(v_max)
    # No upper bound: include if same major.minor as v_min
    return v_info.major == min_info.major and v_info.minor == min_info.minor


async def calc_upgrade_sources_async(
    version: str,
    arch: str,
    graph_url: str = 'https://api.openshift.com/api/upgrades_info/v1/graph',
    suggestions_url: str = 'https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/',
) -> list[str]:
    """
    Calculate which previous release versions can upgrade to the specified version.

    This function determines upgrade sources by:
    1. Fetching build-suggestions to determine the previous minor version
    2. Using get_previous_minor_version_from_suggestions to extract previous version
    3. Querying Cincinnati for versions from both previous and current minor releases
    4. Filtering based on build-suggestions constraints (minor_min/max, z_min/max, block lists)
    5. Including eligible hotfix releases

    :param version: Version string (e.g., "5.0.0-rc.0")
    :param arch: Architecture (brew arch name, e.g., "x86_64")
    :param graph_url: Cincinnati API endpoint
    :param suggestions_url: Base URL to Cincinnati build-suggestions directory
    :return: Sorted list of version strings that can upgrade to the target version
    :raises ValueError: If build-suggestions are invalid or previous version cannot be determined
    :raises httpx.HTTPError: If Cincinnati or build-suggestions fetch fails
    """
    # Parse version to extract major.minor
    major, minor = extract_version_fields(version, at_least=2)[:2]

    # Convert brew arch to Go arch (Cincinnati uses Go arch names)
    go_arch = go_arch_for_brew_arch(arch)

    # STEP 1: Fetch build-suggestions FIRST (before querying channels)
    # This is the KEY FIX for ART-14348 - we fetch suggestions before determining channels
    suggestions = await get_build_suggestions_async(major, minor, suggestions_url)
    spec = suggestions.get_for_arch(go_arch)

    # STEP 2: Determine previous minor version using build-suggestions
    # This replaces the broken "minor - 1" logic that fails for 5.0 → 4.22
    prev_major, prev_minor = get_previous_minor_version_from_suggestions(suggestions, go_arch)

    # STEP 3: Get Cincinnati channel names
    candidate_channel = get_cincinnati_channels(major, minor)[0]
    prev_candidate_channel = get_cincinnati_channels(prev_major, prev_minor)[0]

    # STEP 4: Query Cincinnati for version lists
    prev_versions, _ = await get_channel_versions_async(prev_candidate_channel, go_arch, graph_url)
    curr_versions, current_edges = await get_channel_versions_async(candidate_channel, go_arch, graph_url)

    upgrade_from = set()

    # STEP 5: Filter previous minor versions based on minor_min/minor_max constraints
    for v in prev_versions:
        if _version_in_range(v, spec.minor_min, spec.minor_max) and v not in spec.minor_block_list:
            upgrade_from.add(v)

    # STEP 6: Filter current minor versions based on z_min/z_max constraints
    for v in curr_versions:
        if _version_in_range(v, spec.z_min, spec.z_max) and v not in spec.z_block_list:
            upgrade_from.add(v)

    # STEP 7: Include eligible hotfix releases (only for standard releases)
    # If we are calculating previous list for a standard release (not a nightly/hotfix),
    # include hotfixes that don't already have 2 outgoing edges to standard releases.
    # Ref: https://docs.google.com/document/d/16eGVikCYARd6nUUtAIHFRKXa7R_rU5Exc9jUPcQoG8A/edit
    if 'nightly' not in version and 'hotfix' not in version:
        previous_hotfixes = [release for release in curr_versions if 'nightly' in release or 'hotfix' in release]
        for hotfix_version in previous_hotfixes:
            if hotfix_version in spec.z_block_list:
                continue
            standard_edges = [
                edge for edge in current_edges.get(hotfix_version, []) if 'nightly' not in edge and 'hotfix' not in edge
            ]
            if len(standard_edges) < 2:
                upgrade_from.add(hotfix_version)

    return sort_semver(list(upgrade_from))
