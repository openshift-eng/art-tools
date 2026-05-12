import json
import logging
from urllib import request

import click
from artcommonlib import exectools
from artcommonlib.arch_util import brew_arch_for_go_arch
from artcommonlib.constants import RHCOS_RELEASES_STREAM_URL
from artcommonlib.format_util import green_print
from artcommonlib.rhcos import get_container_configs
from artcommonlib.util import get_art_prod_image_repo_for_version, oc_image_info, oc_image_info_show_multiarch
from doozerlib.util import get_nightly_pullspec

from elliottlib.cli.common import cli
from elliottlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)


@cli.command("rhcos", short_help="Get RHCOS manifest list tags from an OCP release payload")
@click.option(
    '--release',
    '-r',
    'release',
    help='OCP release: nightly name, named release (e.g. 4.16.5), or full pullspec',
)
@click.pass_obj
def rhcos_cli(runtime: Runtime, release):
    """
        Extract RHCOS manifest list tags from an OCP release payload.

        Inspects the RHCOS images in a release payload and outputs their
        manifest list tags (e.g. quay.io/openshift-release-dev/ocp-v4.0-art-dev:418.94.202605101521-0-coreos).

        For layered RHCOS (4.19+), reads the coreos.build.manifest-list-tag label
        from each RHCOS image in the payload.

        For non-layered RHCOS, fetches RHCOS build metadata (meta.json) and
        extracts the build-specific tags from each container entry.

        Each tag is verified to be live in the registry before being returned.

        Usage:

    \b Nightly
        $ elliott -g openshift-4.19 rhcos -r 4.19.0-0.nightly-2025-05-01-010101

    \b Named Release
        $ elliott -g openshift-4.16 rhcos -r 4.16.5

    \b Any Pullspec
        $ elliott -g openshift-4.X rhcos -r <pullspec>

    \b Assembly
        $ elliott -g openshift-4.19 --assembly 4.19.3 rhcos
    """
    named_assembly = runtime.assembly not in ['stream', 'test']
    if not release and not named_assembly:
        raise click.BadParameter("Use one of --release or --assembly")
    if release and named_assembly:
        raise click.BadParameter("Use only one of --release or --assembly")

    runtime.initialize()

    if named_assembly:
        release = runtime.assembly
        LOGGER.info("Using assembly name as release: %s", release)

    nightly = 'nightly' in release
    pullspec = '/' in release

    if nightly:
        payload_pullspec = get_nightly_pullspec(release, runtime.build_system)
    elif pullspec:
        payload_pullspec = release
    else:
        payload_pullspec = f'quay.io/openshift-release-dev/ocp-release:{release}-x86_64'

    tags = get_rhcos_tags(runtime, payload_pullspec)
    for tag_name, full_tag in tags.items():
        green_print(f"{tag_name}: {full_tag}")


def _get_rhcos_pullspec_from_payload(payload_pullspec, tag_name):
    out, _ = exectools.cmd_assert(["oc", "adm", "release", "info", "--image-for", tag_name, "--", payload_pullspec])
    return out.strip()


def _verify_tag(tag_pullspec):
    result = oc_image_info_show_multiarch(tag_pullspec, strict=False)
    if result is None:
        raise click.ClickException(f"Tag is not live in registry: {tag_pullspec}")


def get_rhcos_tags(runtime, payload_pullspec):
    art_repo = get_art_prod_image_repo_for_version(4, "dev")
    container_configs = get_container_configs(runtime)
    primary_conf = next(c for c in container_configs if c.primary)

    primary_rhcos_pullspec = _get_rhcos_pullspec_from_payload(payload_pullspec, primary_conf.name)
    image_info = oc_image_info(primary_rhcos_pullspec)
    labels = image_info["config"]["config"]["Labels"]
    arch = brew_arch_for_go_arch(image_info["config"]["architecture"])

    manifest_list_tag = labels.get("coreos.build.manifest-list-tag")
    if manifest_list_tag:
        return _get_layered_tags(runtime, payload_pullspec, container_configs, art_repo)
    else:
        build_id = labels.get("org.opencontainers.image.version") or labels.get("version")
        if not build_id:
            raise click.ClickException(f"Cannot determine RHCOS build ID from {primary_rhcos_pullspec}")
        return _get_non_layered_tags(runtime, build_id, arch, container_configs, art_repo)


def _get_layered_tags(runtime, payload_pullspec, container_configs, art_repo):
    tags = {}
    for container_conf in container_configs:
        tag_name = container_conf.name
        rhcos_pullspec = _get_rhcos_pullspec_from_payload(payload_pullspec, tag_name)
        image_info = oc_image_info(rhcos_pullspec)
        mlt = image_info["config"]["config"]["Labels"].get("coreos.build.manifest-list-tag")
        if not mlt:
            raise click.ClickException(f"No coreos.build.manifest-list-tag label found for {tag_name}")
        tag_pullspec = f"{art_repo}:{mlt}"
        _verify_tag(tag_pullspec)
        tags[tag_name] = tag_pullspec
    return tags


def _get_non_layered_tags(runtime, build_id, arch, container_configs, art_repo):
    major, minor = runtime.get_major_minor()
    major_minor = f"{major}.{minor}"
    rhcos_el_major = runtime.group_config.vars.RHCOS_EL_MAJOR
    rhcos_el_minor = runtime.group_config.vars.RHCOS_EL_MINOR
    url_key = f"{major_minor}-{rhcos_el_major}.{rhcos_el_minor}" if rhcos_el_major > 8 else major_minor
    meta_url = f"{RHCOS_RELEASES_STREAM_URL}/{url_key}/builds/{build_id}/{arch}/meta.json"

    LOGGER.info("Fetching RHCOS metadata: %s", meta_url)
    with request.urlopen(meta_url) as resp:
        meta = json.loads(resp.read().decode())

    tags = {}
    for container_conf in container_configs:
        key = container_conf.build_metadata_key
        if key not in meta:
            raise click.ClickException(
                f"Key '{key}' not found in RHCOS metadata for {container_conf.name}. URL: {meta_url}"
            )
        container = meta[key]
        image = container["image"]
        container_tags = container.get("tags", [])
        build_tag = next((t for t in container_tags if build_id in t), None)
        if not build_tag:
            raise click.ClickException(
                f"No build-specific tag (containing {build_id}) found for {container_conf.name} in RHCOS metadata"
            )
        tag_pullspec = f"{image}:{build_tag}"
        _verify_tag(tag_pullspec)
        tags[container_conf.name] = tag_pullspec
    return tags
