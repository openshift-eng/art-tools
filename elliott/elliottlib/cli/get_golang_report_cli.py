import click
import json
import re

from artcommonlib.release_util import split_el_suffix_in_release
from artcommonlib.format_util import green_print
from elliottlib import logutil
from elliottlib.cli.common import cli
from elliottlib.rpm_utils import parse_nvr

_LOGGER = logutil.getLogger(__name__)


@cli.command("go:report", short_help="Report about golang streams configured in streams.yml")
@click.option('--ocp-versions',
              help="OCP versions to show report for. e.g. `4.14`. Comma separated")
@click.option("--ignore-rhel", is_flag=True, help="Ignore rhel version and instead only show go version")
@click.option('-o', '--output',
              type=click.Choice(['json', 'text']),
              default='text', help='Output format')
@click.pass_obj
def get_golang_report_cli(runtime, ocp_versions, ignore_rhel, output):
    """
    Show currently configured builders in streams.yml and compilers in buildroot

    Usage:

    $ elliott go:report --versions 4.11,4.12,4.13,4.14,4.15,4.16

"""
    results = {}

    for ocp_version in ocp_versions.split(","):
        runtime.group = f"openshift-{ocp_version}"
        runtime.image_map = {}
        runtime.rpm_map = {}
        runtime._group_config = None
        runtime.branch = None
        runtime.initialized = False
        runtime.initialize(mode="both")

        streams_dict = runtime.gitdata.load_data(key='streams').data
        golang_streams = {}
        golang_streams_images = {}
        for stream_name, info in streams_dict.items():
            if 'golang' not in stream_name:
                continue
            image_nvr_like = info['image']
            if 'golang-builder' not in image_nvr_like:
                continue

            name, vr = image_nvr_like.split(':')
            nvr = f"{name.replace('/', '-')}-container-{vr}"
            parsed = parse_nvr(nvr)
            match = re.search(r'(\d+\.\d+\.\d+)', parsed['version'])
            version = match.group(1)
            _, el_version = split_el_suffix_in_release(parsed['release'])
            if not ignore_rhel:
                version = f"{version}.{el_version}"

            golang_streams[stream_name] = version
            golang_streams_images[version] = []

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
                golang_streams_images[v].append(image_name)

        # Add result
        out = []
        for golang_version, images in golang_streams_images.items():
            if not images:
                continue
            out.append({"go_version": golang_version, "building_count": len(images)})

        out = sorted(out, key=lambda x: x['building_count'], reverse=True)
        results[ocp_version] = out

    if output == 'json':
        print(json.dumps(results, indent=4))
    else:
        for ocp_version, result in results.items():
            green_print(f'{ocp_version}: {result}')
