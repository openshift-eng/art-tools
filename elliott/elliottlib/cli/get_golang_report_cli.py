import json
import re

import click
from artcommonlib import logutil
from artcommonlib.format_util import green_print
from artcommonlib.release_util import split_el_suffix_in_release
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import oc_image_info

from elliottlib.cli.common import cli
from elliottlib.runtime import Runtime
from elliottlib.util import get_golang_container_nvrs

_LOGGER = logutil.get_logger(__name__)

# Matches floating golang-builder tags such as:
#   openshift-golang-builder-container-v1.22-rhel9
#   openshift-golang-builder-container-v1.22-rhel8
# These lack the X.Y.Z patch version present in full NVR tags.
_FLOATING_TAG_RE = re.compile(r'v(\d+\.\d+)-rhel(\d+)$')


def is_floating_golang_builder_tag(nvr_like: str) -> bool:
    """Return True when *nvr_like* is a floating tag (vX.Y-rhelN) rather than a full NVR string."""
    return bool(_FLOATING_TAG_RE.search(nvr_like))


def go_version_from_floating_tag(nvr_like: str, ignore_rhel: bool) -> str:
    """Extract a go-version string from a floating tag like
    ``openshift-golang-builder-container-v1.22-rhel9``.

    Returns ``X.Y.elN`` normally, or just ``X.Y`` when *ignore_rhel* is True.
    """
    m = _FLOATING_TAG_RE.search(nvr_like)
    if not m:
        raise ValueError(f"Not a floating golang-builder tag: {nvr_like!r}")
    major_minor = m.group(1)
    rhel_version = m.group(2)
    if ignore_rhel:
        return major_minor
    return f"{major_minor}.el{rhel_version}"


def go_version_from_floating_tag_exact(image_pullspec: str) -> str:
    """Resolve a floating-tag pullspec to the exact golang package NVR.

    Calls ``oc image info`` to read the OCI labels from the resolved image,
    constructs the builder NVR, then delegates to ``get_golang_container_nvrs``
    (exact mode) to return the golang package NVR string (e.g.
    ``golang-1.22.5-1.el9``).
    """
    _LOGGER.info(f"Resolving floating tag via oc image info: {image_pullspec}")
    image_data = oc_image_info(image_pullspec, '--filter-by-os=amd64')
    labels = image_data.get('config', {}).get('config', {}).get('Labels', {})
    component = labels.get('com.redhat.component')
    version = labels.get('version')
    release = labels.get('release')
    if not all([component, version, release]):
        raise ValueError(
            f"Cannot determine NVR from image labels for {image_pullspec}: "
            f"component={component!r} version={version!r} release={release!r}"
        )
    _LOGGER.info(f"Resolved floating tag to builder NVR: {component}-{version}-{release}")
    go_builder_nvr_map = get_golang_container_nvrs([(component, version, release)], _LOGGER, exact=True)
    if not go_builder_nvr_map:
        raise ValueError(f"Could not determine golang package NVR for builder {component}-{version}-{release}")
    if len(go_builder_nvr_map) != 1:
        raise ValueError(
            f"Expected exactly one golang version for builder {component}-{version}-{release}, "
            f"got {list(go_builder_nvr_map.keys())}"
        )
    return list(go_builder_nvr_map.keys())[0]


@cli.command("go:report", short_help="Report about golang streams configured in streams.yml")
@click.option('--ocp-versions', help="OCP versions to show report for. e.g. `4.14`. Comma separated")
@click.option("--ignore-rhel", is_flag=True, help="Ignore rhel version and instead only show go version")
@click.option('--exact', is_flag=True, help="Show exact golang package instead of just major.minor")
@click.option('-o', '--output', type=click.Choice(['json', 'text']), default='text', help='Output format')
@click.pass_obj
def get_golang_report_cli(runtime: Runtime, ocp_versions: str, ignore_rhel: bool, exact: bool, output: str):
    """
    Show currently configured builders in streams.yml and compilers in buildroot

    Usage:

    $ elliott go:report --ocp-versions 4.11,4.12,4.13,4.14,4.15,4.16

    """
    results = {}

    for ocp_version in ocp_versions.split(","):
        _LOGGER.info(f"Generating report for OCP {ocp_version}...")
        runtime.group = f"openshift-{ocp_version}"
        runtime.group_commitish = None
        runtime.image_map = {}
        runtime.rpm_map = {}
        runtime._group_config = None
        runtime.branch = None
        runtime.initialized = False
        runtime.initialize(mode="both")

        out = golang_report_for_version(runtime, ocp_version, ignore_rhel, exact)
        results[ocp_version] = out

    if output == 'json':
        print(json.dumps(results, indent=4))
    else:
        for ocp_version, result in results.items():
            green_print(f'{ocp_version}: {result}')


def golang_report_for_version(runtime, ocp_version: str, ignore_rhel: bool = False, exact: bool = False):
    if exact and ignore_rhel:
        raise ValueError("Cannot use exact and ignore_rhel together")

    if not runtime.image_metas() or not runtime.rpm_metas():
        raise ValueError("runtime is not initialized properly. use mode=both")

    streams_dict = runtime.get_streams_config()

    # Build a reverse map from alias -> actual stream name
    stream_alias_map = {}
    for stream_name, info in streams_dict.items():
        for alias in info.get('aliases', []):
            stream_alias_map[alias] = stream_name

    golang_streams = {}
    golang_streams_images = {}
    _LOGGER.info(f"Analyzing golang streams for OCP {ocp_version}...")
    for stream_name, info in streams_dict.items():
        if 'golang' not in stream_name:
            continue
        image_nvr_like = info['image']
        if 'golang-builder' not in image_nvr_like:
            continue

        if image_nvr_like.startswith('openshift/golang-builder:'):
            # handle legacy format openshift/golang-builder:v1.23.9-202506111225.g6c23478.el9
            nvr = image_nvr_like.replace('openshift/golang-builder:', 'openshift-golang-builder-container-')
        else:
            tag = image_nvr_like.split(':')[-1]
            if tag.startswith('openshift-golang-builder-container-'):
                # registry.redhat.io/openshift/golang-builder:openshift-golang-builder-container-v1.25.8-...
                # (formerly art-images-base before ART moved published golang builders)
                # Tag is already in NVR name format
                nvr = tag
            else:
                # quay.io/redhat-user-workloads/ocp-art-tenant/art-images:golang-builder-v1.23.10-...
                nvr = tag.replace('golang-builder', 'openshift-golang-builder-container')

        _LOGGER.info(f"Detected stream {stream_name} with builder nvr: {nvr}")

        if is_floating_golang_builder_tag(nvr):
            # Floating tag (e.g. openshift-golang-builder-container-v1.22-rhel9): no full NVR available.
            # Non-exact mode: extract major.minor + RHEL suffix from the tag string directly.
            # Exact mode: resolve to actual image via oc image info to obtain the real golang package NVR.
            _LOGGER.info(f"Stream {stream_name} uses a floating tag; extracting version from tag")
            if exact:
                version = go_version_from_floating_tag_exact(image_nvr_like)
            else:
                version = go_version_from_floating_tag(nvr, ignore_rhel)
        elif exact:
            parsed_nvr = parse_nvr(nvr)
            go_builder_nvr_map = get_golang_container_nvrs(
                [(parsed_nvr['name'], parsed_nvr['version'], parsed_nvr['release'])], _LOGGER, exact=exact
            )
            if len(go_builder_nvr_map) != 1:
                raise ValueError(
                    f"Expected exactly one golang version for builder {nvr}, got {list(go_builder_nvr_map.keys())}"
                )
            exact_pkg = list(go_builder_nvr_map.keys())[0]
            version = exact_pkg
        else:
            version = go_version_from_nvr_string(nvr, ignore_rhel)

        golang_streams[stream_name] = version
        golang_streams_images[version] = 0

    _LOGGER.info(f"golang streams mapped to versions: {golang_streams}")

    for meta in runtime.image_metas():
        image_name = meta.config_filename.replace('.yml', '')
        if not meta.enabled:
            _LOGGER.debug(f"Skipping image {image_name}")
            continue

        builders = {list(b.values())[0] for b in meta.config.get("from", {}).get("builder", [])}
        for b in builders:
            if 'golang' not in b:
                continue
            stream_key = stream_alias_map.get(b, b)
            if stream_key not in golang_streams:
                _LOGGER.warning(f"Image {image_name} references unknown golang stream: {b}")
                continue
            v = golang_streams[stream_key]
            golang_streams_images[v] += 1

    _LOGGER.info(f"image count by builders: {golang_streams_images}")

    # Analyze defined rpms
    rpm_rhel_target_map = {}
    for rpm_meta in runtime.rpm_metas():
        rpm_name = rpm_meta.config_filename.replace('.yml', '')
        golang_rpms = {
            'microshift',
            'openshift-clients',
            'openshift',
            'ose-aws-ecr-image-credential-provider',
            'ose-azure-acr-image-credential-provider',
            'ose-gcp-gcr-image-credential-provider',
            'ose-crio-credential-provider',
        }
        if rpm_name not in golang_rpms:
            _LOGGER.debug(f"Skipping rpm {rpm_name} since it is not a golang rpm")
            continue

        for el_v in rpm_meta.determine_rhel_targets():
            if el_v not in rpm_rhel_target_map:
                rpm_rhel_target_map[el_v] = 0
            rpm_rhel_target_map[el_v] += 1

    _LOGGER.info(f"golang rpms mapped to rhel targets: {rpm_rhel_target_map}")

    golang_streams_rpms = {}
    with runtime.shared_koji_client_session() as koji_session:
        for el_v in rpm_rhel_target_map.keys():
            nvr = latest_go_build_in_buildroot(ocp_version, el_v, koji_session)
            version = nvr if exact else go_version_from_nvr_string(nvr, ignore_rhel)
            golang_streams_rpms[version] = rpm_rhel_target_map[el_v]

    _LOGGER.info(f"rpm count by builders: {golang_streams_rpms}")

    # Add result
    out = []
    for golang_version, len_images in golang_streams_images.items():
        if len_images == 0:
            continue
        info = {"go_version": golang_version, "building_image_count": len_images}
        if golang_version in golang_streams_rpms:
            info["building_rpm_count"] = golang_streams_rpms[golang_version]
        out.append(info)

    for golang_version, len_rpms in golang_streams_rpms.items():
        if len_rpms == 0 or golang_version in golang_streams_images:
            continue
        out.append({"go_version": golang_version, "building_rpm_count": len_rpms})

    out = sorted(
        out,
        key=lambda x: x['building_image_count'] if 'building_image_count' in x else x['building_rpm_count'],
        reverse=True,
    )
    return out


def go_version_from_nvr_string(nvr_string: str, ignore_rhel: bool) -> str:
    nvr = parse_nvr(nvr_string)
    match = re.search(r'(\d+\.\d+\.\d+)', nvr['version'])
    version = match.group(1)
    _, el_version = split_el_suffix_in_release(nvr['release'])
    if not ignore_rhel:
        version = f"{version}.{el_version}"
    return version


def latest_go_build_in_buildroot(ocp_version: str, el_v: int, koji_session) -> str:
    if el_v == 7:
        # rhel7 golang packages are differently named e.g. `go-toolset-1.18-golang`
        raise NotImplementedError

    go_pkg_name = "golang"
    build_tag = f'rhaos-{ocp_version}-rhel-{el_v}-build'
    latest_build = koji_session.getLatestBuilds(build_tag, package=go_pkg_name)
    if not latest_build:  # if this happens, investigate
        raise ValueError(f'Cannot find latest {go_pkg_name} build in {build_tag}. Please investigate.')
    return latest_build[0]['nvr']
