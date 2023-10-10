import click
import yaml

from doozerlib import util
from doozerlib.cli import cli
from doozerlib.rhcos import RHCOSBuildFinder, RHCOSBuildInspector
from artcommon.rhcos import get_container_configs, get_container_pullspec


@cli.command("gen-assembly:rhcos", short_help="Generate assembly config for rhcos by given build_id eg. 414.92.202309222337-0")
@click.argument("build_id", required=True)
@click.pass_obj
def gen_assembly_rhcos(runtime, build_id):
    """
    Generate assembly config for rhcos by given build_id

    $ doozer -g openshift-4.14 gen-assembly:rhcos 414.92.202309222337-0
    """
    runtime.initialize(clone_distgits=False)
    rhcos_info = {}
    for arch in runtime.group_config.arches:
        brew_arch = util.brew_arch_for_go_arch(arch)
        runtime.logger.info(f"Getting RHCOS pullspecs for build {build_id}-{brew_arch}...")
        for container_conf in get_container_configs(runtime):
            version = runtime.get_minor_version()
            finder = RHCOSBuildFinder(runtime, version, brew_arch, False)
            if container_conf.name not in rhcos_info:
                rhcos_info[container_conf.name] = {"images": {}}
            rhcos_info[container_conf.name]["images"][arch] = get_container_pullspec(
                finder.rhcos_build_meta(build_id),
                container_conf or finder.get_primary_container_conf()
            )

    print(yaml.dump({'rhcos': rhcos_info}))
