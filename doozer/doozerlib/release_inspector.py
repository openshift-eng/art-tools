"""
Shared utilities for introspecting nightly/release payloads.
Used by both release:gen-payload and release:gen-assembly commands.
"""

import asyncio
import json
import logging
from typing import Tuple

from artcommonlib import exectools
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.model import Model
from artcommonlib.oc_image_info import oc_image_info__cached_async

from doozerlib.build_info import BrewBuildRecordInspector, BuildRecordInspector, KonfluxBuildRecordInspector
from doozerlib.runtime import Runtime

logger = logging.getLogger(__name__)


async def fetch_release_pullspec_from_stream_api(release_name: str, major_minor: str) -> str:
    """
    Query the release stream API to get the pullspec for a specific nightly.

    :param release_name: Nightly name (e.g., '4.21.0-0.nightly-2025-11-12-194750')
    :param major_minor: Version (e.g., '4.21')
    :return: Pullspec for the nightly
    """
    release_stream_name = f"{major_minor}.0-0.nightly"
    api_url = (
        f"https://openshift-release.apps.ci.l2s4.p1.openshiftapps.com/api/v1/releasestream/{release_stream_name}/tags"
    )

    rc, api_json_str, err = await exectools.cmd_gather_async(["curl", "-s", api_url], check=False)

    if rc != 0:
        raise IOError(f"Unable to fetch release stream tags from {api_url}: {err}")

    release_stream_data = json.loads(api_json_str)

    # Find the specific nightly in the tags list
    for tag in release_stream_data.get("tags", []):
        if tag.get("name") == release_name:
            pullspec = tag.get("pullSpec")
            if pullspec:
                return pullspec

    raise IOError(f"Nightly {release_name} not found in release stream {release_stream_name}")


async def introspect_release(pullspec: str, registry_config: str = None, retries: int = 5,
                             retry_delay: float = 30.0) -> Model:
    """
    Run 'oc adm release info' and return parsed release information.
    Retries on transient "manifest unknown" errors, which can occur when
    the release controller advertises a nightly before the registry has
    finished propagating the manifest.

    :param pullspec: Release payload pullspec
    :param registry_config: Optional path to registry auth config
    :param retries: Number of retry attempts for transient errors
    :param retry_delay: Seconds to wait between retries
    :return: Model object containing release info
    """
    cmd = ["oc", "adm", "release", "info", pullspec, "-o=json"]
    if registry_config:
        cmd.insert(1, f"--registry-config={registry_config}")

    for attempt in range(1, retries + 1):
        rc, release_json_str, err = await exectools.cmd_gather_async(cmd, check=False)

        if rc == 0:
            break

        if "manifest unknown" in err and attempt < retries:
            logger.warning("Manifest not yet available for %s (attempt %d/%d); retrying in %ds...",
                           pullspec, attempt, retries, int(retry_delay))
            await asyncio.sleep(retry_delay)
            continue

        raise IOError(f"Unable to gather release info for {pullspec}: {err}")

    release_info = Model(dict_to_model=json.loads(release_json_str))

    if not release_info.references.spec.tags:
        raise IOError(f"Could not find tags in release {pullspec}")

    return release_info


async def extract_nvr_from_pullspec(
    pullspec: str, arch: str = None, registry_config: str = None
) -> Tuple[str, str, str]:
    """
    Extract NVR (name, version, release) from an image pullspec by reading labels.

    :param pullspec: Image pullspec
    :param arch: Optional go architecture to filter by
    :param registry_config: Optional path to registry auth config file
    :return: Tuple of (name, version, release)
    """
    options = []
    if arch:
        options.append(f"--filter-by-os={arch}")
    try:
        stdout = await oc_image_info__cached_async(pullspec, *options, registry_config=registry_config)
    except ChildProcessError as e:
        raise IOError(f"Failed to get image info for {pullspec}: {e}") from e

    image_info = json.loads(stdout)
    labels = image_info['config']['config']['Labels']

    name = labels.get('com.redhat.component')
    version = labels.get('version')
    release = labels.get('release')

    if not (name and version and release):
        raise IOError(f"Missing NVR labels in {pullspec}")

    return (name, version, release)


async def get_build_inspector_from_nvr(runtime: Runtime, nvr: str, pullspec: str = None) -> BuildRecordInspector:
    """
    Query build system for a build by NVR and create a build inspector.
    For Brew builds, pullspec is required. For Konflux builds, it queries the DB.

    :param runtime: Doozer runtime
    :param nvr: Build NVR
    :param pullspec: Image pullspec (required for Brew builds)
    :return: BuildRecordInspector for the build
    """
    if runtime.build_system == 'brew':
        if not pullspec:
            raise ValueError("pullspec is required for Brew builds")
        return BrewBuildRecordInspector(runtime, pullspec)
    else:
        build_record = await runtime.konflux_db.get_build_record_by_nvr(
            nvr=nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            exclude_large_columns=True,
        )
        return KonfluxBuildRecordInspector(runtime, build_record)


async def get_build_inspector_from_pullspec(
    runtime: Runtime, pullspec: str, arch: str = None, registry_config: str = None
) -> BuildRecordInspector:
    """
    Extract NVR from a pullspec and query build system to get build inspector.

    :param runtime: Doozer runtime
    :param pullspec: Image pullspec
    :param arch: Optional go architecture to filter by
    :param registry_config: Optional path to registry auth config file
    :return: BuildRecordInspector for the build
    """
    name, version, release = await extract_nvr_from_pullspec(pullspec, arch, registry_config=registry_config)
    nvr = f"{name}-{version}-{release}"
    return await get_build_inspector_from_nvr(runtime, nvr, pullspec=pullspec)
