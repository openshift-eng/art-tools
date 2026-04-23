import json
import re

import click
from artcommonlib import logutil
from artcommonlib.format_util import green_print
from artcommonlib.release_util import split_el_suffix_in_release
from artcommonlib.rpm_utils import parse_nvr

from elliottlib.cli.common import cli
from elliottlib.runtime import Runtime
from elliottlib.util import get_golang_container_nvrs

_LOGGER = logutil.get_logger(__name__)


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

    $ elliott go:report --versions 4.11,4.12,4.13,4.14,4.15,4.16

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

    streams_dict = runtime.gitdata.load_data(key='streams').data
    golang_streams = {}
    golang_streams_images = {}
    _LOGGER.info(f"Analyzing golang streams for OCP {ocp_version}...")
    for stream_name, info in streams_dict.items():
        # FIXME: This doesn't handle stream aliases

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
                # registry.redhat.io/openshift/art-images-base:openshift-golang-builder-container-v1.25.8-...
                # Tag is already in NVR name format
                nvr = tag
            else:
                # quay.io/redhat-user-workloads/ocp-art-tenant/art-images:golang-builder-v1.23.10-...
                nvr = tag.replace('golang-builder', 'openshift-golang-builder-container')

        _LOGGER.info(f"Detected stream {stream_name} with builder nvr: {nvr}")

        if exact:
            parsed_nvr = parse_nvr(nvr)
            go_builder_nvr_map = get_golang_container_nvrs(
                [(parsed_nvr['name'], parsed_nvr['version'], parsed_nvr['release'])], _LOGGER, exact=exact
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
            v = golang_streams[b]
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
