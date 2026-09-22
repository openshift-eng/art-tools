"""
OpenShift Container Platform (OCP) version ancestry and upgrade path calculations.

This module handles non-standard OCP version transitions (e.g., 4.22→5.0, bridge releases)
and determines which previous major.minor versions a target OCP version can upgrade from.

Key components:
- BuildSuggestions: Pydantic model for multi-stream build-suggestions
- get_build_suggestions_async(): Fetch and validate Cincinnati build-suggestions YAML files
- get_cincinnati_channels(): Get Cincinnati channel names for a version
- get_release_controller_versions_async(): Fetch promoted versions from the release controller
- calc_upgrade_sources_async(): Calculate which versions can upgrade to a target version
"""

import functools
import logging
import re

import httpx
import semver
import yaml
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.util import extract_version_fields
from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)

BUILD_SUGGESTIONS_OWNERS_URL = 'https://github.com/openshift/cincinnati-graph-data/blob/master/build-suggestions/OWNERS'


class _SourceConstraint(BaseModel):
    """Inclusive minimum for one source major.minor release line."""

    min_version: str

    @property
    def major_minor(self) -> tuple[int, int]:
        version = semver.VersionInfo.parse(self.min_version)
        return version.major, version.minor


class BuildSuggestions(BaseModel):
    """
    Build-suggestions for a specific OCP minor version.

    The schema lists the inclusive minimum version for every source release line::

        min_versions:
        - 4.23.0-rc.0
        - 5.0.0-rc.0
        - 5.1.0-ec.0

    """

    min_versions: list[str] = Field(
        ...,
        min_length=1,
        description="Inclusive minimum version for every source major.minor release line",
    )
    model_config = {"extra": "forbid"}

    @field_validator('min_versions')
    @classmethod
    def validate_min_versions(cls, versions: list[str]) -> list[str]:
        """Validate minima and ensure each release line occurs once."""
        release_lines: dict[tuple[int, int], str] = {}
        for version in versions:
            try:
                parsed = semver.VersionInfo.parse(version)
            except ValueError as e:
                raise ValueError(f"Invalid semver format '{version}': {e}") from e
            release_line = (parsed.major, parsed.minor)
            if release_line in release_lines:
                raise ValueError(
                    f"min_versions contains duplicate release line {parsed.major}.{parsed.minor}: "
                    f"'{release_lines[release_line]}' and '{version}'"
                )
            release_lines[release_line] = version
        return versions

    def get_source_constraints(self) -> list[_SourceConstraint]:
        """Return the inclusive minimum for each source release line."""
        return [_SourceConstraint(min_version=version) for version in self.min_versions]


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
    :return: Validated multi-stream BuildSuggestions object
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
                f"Please contact the build-suggestions owners listed in {BUILD_SUGGESTIONS_OWNERS_URL} "
                f"to fix the file. "
                f"YAML error: {e}"
            ) from e

        # Validate against Pydantic model
        try:
            return BuildSuggestions.model_validate(data)
        except ValidationError as e:
            raise ValueError(
                f"Failed to validate build-suggestions for {major}.{minor} from {url}. "
                f"The YAML structure is invalid or contains incorrect version strings. "
                f"Please contact the build-suggestions owners listed in {BUILD_SUGGESTIONS_OWNERS_URL} "
                f"to fix the file. "
                f"Validation error: {e}"
            ) from e


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


def _version_meets_minimum(version: str, minimum: str) -> bool:
    """Check if a version is in the minimum's release line and at or above it."""
    v_info = semver.VersionInfo.parse(version)
    min_info = semver.VersionInfo.parse(minimum)
    if v_info < min_info:
        return False
    return v_info.major == min_info.major and v_info.minor == min_info.minor


async def _fetch_release_controller_tags(
    url: str,
    major: int,
    minor: int,
    timeout: float,
) -> list[str]:
    """
    Fetch tags from a single release controller stream URL and filter to major.minor.

    :param url: Full URL to the release controller tags endpoint
    :param major: Major version to filter for
    :param minor: Minor version to filter for
    :param timeout: HTTP request timeout in seconds
    :return: List of matching version strings (unsorted)
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=timeout)
            response.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning('Failed to query release controller at %s: %s', url, e)
        return []

    data = response.json()
    tags = data.get('tags') or []

    version_pattern = re.compile(rf'^{major}\.{minor}\.')
    versions = []
    for tag in tags:
        name = tag.get('name', '')
        if version_pattern.match(name):
            try:
                semver.VersionInfo.parse(name)
                versions.append(name)
            except ValueError:
                continue

    return versions


async def get_release_controller_versions_async(
    major: int,
    minor: int,
    go_arch: str,
    release_controller_url: str = '',
    timeout: float = 30.0,
) -> list[str]:
    """
    Query the release controller's stable and dev-preview streams to get all promoted versions
    for a major.minor.

    The release controller tracks all versions that have been promoted, regardless of whether
    their cincinnati-graph-data PR has merged. This supplements Cincinnati data to avoid missing
    recently-promoted z-streams.

    Both {major}-stable and {major}-dev-preview streams are queried because EC releases
    (e.g., 5.0.0-ec.5) are promoted to dev-preview, not stable.

    :param major: Major version (e.g., 4 or 5)
    :param minor: Minor version (e.g., 18)
    :param go_arch: Go architecture name (e.g., 'amd64', 's390x', 'arm64', 'ppc64le', 'multi')
    :param release_controller_url: Base URL for the release controller. If empty, defaults to
        'https://{go_arch}.ocp.releases.ci.openshift.org'.
    :param timeout: HTTP request timeout in seconds
    :return: List of version strings matching major.minor, sorted descending by semver
    """
    if not release_controller_url:
        release_controller_url = f'https://{go_arch}.ocp.releases.ci.openshift.org'

    base_url = release_controller_url.rstrip('/')

    streams = [f'{major}-stable', f'{major}-dev-preview']
    all_versions: set[str] = set()
    for stream in streams:
        url = f'{base_url}/api/v1/releasestream/{stream}/tags'
        versions = await _fetch_release_controller_tags(url, major, minor, timeout)
        all_versions.update(versions)

    return sort_semver(list(all_versions))


async def calc_upgrade_sources_async(
    version: str,
    arch: str,
    graph_url: str = 'https://api.openshift.com/api/upgrades_info/v1/graph',
    suggestions_url: str = 'https://raw.githubusercontent.com/openshift/cincinnati-graph-data/master/build-suggestions/',
    release_controller_url: str = '',
) -> list[str]:
    """
    Calculate which previous release versions can upgrade to the specified version.

    This function determines upgrade sources by:
    1. Fetching multi-stream build-suggestions
    2. Querying Cincinnati for every configured source release line
    3. Supplementing Cincinnati data with release controller versions (catches recently-promoted
       z-streams whose cincinnati-graph-data PR hasn't merged yet)
    4. Filtering each release line based on its inclusive minimum
    5. Including eligible hotfix releases from the target release line

    :param version: Version string (e.g., "5.0.0-rc.0")
    :param arch: Architecture (brew arch name, e.g., "x86_64")
    :param graph_url: Cincinnati API endpoint
    :param suggestions_url: Base URL to Cincinnati build-suggestions directory
    :param release_controller_url: Base URL for the release controller. If empty, defaults to
        'https://{go_arch}.ocp.releases.ci.openshift.org'.
    :return: Sorted list of version strings that can upgrade to the target version
    :raises IOError: If the version string cannot be parsed into major.minor fields
    :raises ValueError: If build-suggestions are invalid
    :raises httpx.HTTPError: If Cincinnati or build-suggestions fetch fails
    """
    # Parse version to extract major.minor
    major, minor = extract_version_fields(version, at_least=2)[:2]

    # Convert brew arch to Go arch (Cincinnati uses Go arch names)
    go_arch = go_arch_for_brew_arch(arch)

    # Fetch build-suggestions before querying channels because they define every source release line.
    suggestions = await get_build_suggestions_async(major, minor, suggestions_url)
    constraints = suggestions.get_source_constraints()

    target_release_major_minor = (major, minor)
    source_release_lines = [constraint.major_minor for constraint in constraints]
    target_constraint_count = source_release_lines.count(target_release_major_minor)
    if target_constraint_count != 1:
        raise ValueError(
            f"Build-suggestions must include exactly one constraint for target release line {major}.{minor}; "
            f"found {target_constraint_count}."
        )

    newer_release_lines = [
        release_line for release_line in source_release_lines if release_line > target_release_major_minor
    ]
    if newer_release_lines:
        formatted_lines = ', '.join(
            f'{source_major}.{source_minor}' for source_major, source_minor in newer_release_lines
        )
        raise ValueError(
            f"Build-suggestions cannot include source release lines newer than target {major}.{minor}: "
            f'{formatted_lines}.'
        )

    upgrade_from: set[str] = set()
    current_versions: list[str] = []
    current_edges: dict[str, list[str]] = {}

    for constraint in constraints:
        source_major, source_minor = constraint.major_minor
        candidate_channel = get_cincinnati_channels(source_major, source_minor)[0]
        channel_versions, channel_edges = await get_channel_versions_async(candidate_channel, go_arch, graph_url)
        rc_versions = await get_release_controller_versions_async(
            source_major, source_minor, go_arch, release_controller_url
        )
        source_versions = sort_semver(list(set(channel_versions) | set(rc_versions)))

        for source_version in source_versions:
            if _version_meets_minimum(source_version, constraint.min_version):
                upgrade_from.add(source_version)

        if (source_major, source_minor) == target_release_major_minor:
            current_versions = source_versions
            current_edges = channel_edges

    # Include eligible hotfix releases from the target release line (only for standard releases).
    # If we are calculating previous list for a standard release (not a nightly/hotfix),
    # include hotfixes that don't already have 2 outgoing edges to standard releases.
    # Ref: https://docs.google.com/document/d/16eGVikCYARd6nUUtAIHFRKXa7R_rU5Exc9jUPcQoG8A/edit
    if 'nightly' not in version and 'hotfix' not in version:
        previous_hotfixes = [release for release in current_versions if 'nightly' in release or 'hotfix' in release]
        for hotfix_version in previous_hotfixes:
            standard_edges = [
                edge for edge in current_edges.get(hotfix_version, []) if 'nightly' not in edge and 'hotfix' not in edge
            ]
            if len(standard_edges) < 2:
                upgrade_from.add(hotfix_version)

    return sort_semver(list(upgrade_from))
