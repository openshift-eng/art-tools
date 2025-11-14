"""
Shared utilities for introspecting nightly/release payloads.
Used by both release:gen-payload and release:gen-assembly commands.
"""

import json
from typing import Dict, Optional, Tuple

from artcommonlib import exectools
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.model import Model

from doozerlib.build_info import BuildRecordInspector, KonfluxBuildRecordInspector
from doozerlib.runtime import Runtime
from doozerlib.util import isolate_nightly_name_components


async def fetch_release_pullspec_from_stream_api(release_name: str, major_minor: str) -> str:
    """
    Query the release stream API to get the pullspec for a specific nightly.
    
    :param release_name: Nightly name (e.g., '4.21.0-0.nightly-2025-11-12-194750')
    :param major_minor: Version (e.g., '4.21')
    :return: Pullspec for the nightly
    """
    release_stream_name = f"{major_minor}.0-0.nightly"
    api_url = f"https://openshift-release.apps.ci.l2s4.p1.openshiftapps.com/api/v1/releasestream/{release_stream_name}/tags"
    
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


async def introspect_release(pullspec: str) -> Model:
    """
    Run 'oc adm release info' and return parsed release information.
    
    :param pullspec: Release payload pullspec
    :return: Model object containing release info
    """
    rc, release_json_str, err = await exectools.cmd_gather_async(
        f"oc adm release info {pullspec} -o=json", check=False
    )
    
    if rc != 0:
        raise IOError(f"Unable to gather release info for {pullspec}: {err}")
    
    release_info = Model(dict_to_model=json.loads(release_json_str))
    
    if not release_info.references.spec.tags:
        raise IOError(f"Could not find tags in release {pullspec}")
    
    return release_info


async def extract_nvr_from_pullspec(pullspec: str, arch: str = None) -> Tuple[str, str, str]:
    """
    Extract NVR (name, version, release) from an image pullspec by reading labels.
    
    :param pullspec: Image pullspec
    :param arch: Optional go architecture to filter by
    :return: Tuple of (name, version, release)
    """
    filter_arg = f"--filter-by-os={arch}" if arch else ""
    rc, stdout, stderr = await exectools.cmd_gather_async(
        f'oc image info -o json {filter_arg} {pullspec}',
        check=False
    )
    
    if rc != 0:
        raise IOError(f"Failed to get image info for {pullspec}: {stderr}")
    
    image_info = json.loads(stdout)
    labels = image_info['config']['config']['Labels']
    
    name = labels.get('com.redhat.component')
    version = labels.get('version')
    release = labels.get('release')
    
    if not (name and version and release):
        raise IOError(f"Missing NVR labels in {pullspec}")
    
    return (name, version, release)


async def get_build_inspector_from_nvr(runtime: Runtime, nvr: str) -> BuildRecordInspector:
    """
    Query Konflux DB for a build by NVR and create a build inspector.
    
    :param runtime: Doozer runtime
    :param nvr: Build NVR
    :return: BuildRecordInspector for the build
    """
    build_record = await runtime.konflux_db.get_build_record_by_nvr(
        nvr=nvr,
        outcome=KonfluxBuildOutcome.SUCCESS,
        strict=True,
    )
    return KonfluxBuildRecordInspector(runtime, build_record)


async def get_build_inspector_from_pullspec(
    runtime: Runtime, pullspec: str, arch: str = None
) -> BuildRecordInspector:
    """
    Extract NVR from a pullspec and query Konflux DB to get build inspector.
    
    :param runtime: Doozer runtime  
    :param pullspec: Image pullspec
    :param arch: Optional go architecture to filter by
    :return: BuildRecordInspector for the build
    """
    name, version, release = await extract_nvr_from_pullspec(pullspec, arch)
    nvr = f"{name}-{version}-{release}"
    return await get_build_inspector_from_nvr(runtime, nvr)

