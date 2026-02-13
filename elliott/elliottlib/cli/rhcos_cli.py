import logging

import click
from artcommonlib.arch_util import BREW_ARCHES, GO_ARCHES, brew_arch_for_go_arch, go_suffix_for_arch
from artcommonlib.format_util import green_print
from artcommonlib.rhcos import get_build_id_from_rhcos_pullspec, get_primary_container_name

from elliottlib import rhcos, util
from elliottlib.cli.common import cli

LOGGER = logging.getLogger(__name__)


@cli.command("rhcos", short_help="Show details of packages contained in OCP RHCOS builds")
@click.option(
    "--release",
    "-r",
    "release",
    help="Show details for this OCP release. Can be a full pullspec or a named release ex: 4.8.4",
)
@click.option(
    "--arch",
    "arch",
    default="x86_64",
    type=click.Choice(BREW_ARCHES + ["all"]),
    help='Specify architecture. Default is x86_64. "all" to get all arches. aarch64 only works for 4.8+',
)
@click.option("--packages", "-p", "packages", help="Show details for only these package names (comma-separated)")
@click.option("--go", "-g", "go", is_flag=True, help="Show go version for packages that are go binaries")
@click.pass_obj
def rhcos_cli(runtime, release, packages, arch, go):
    """
        Show packages in an RHCOS build in a payload image.
        There are several ways to specify the location of the RHCOS build.

        Usage:

    \b Nightly
        $ elliott -g openshift-4.8 rhcos -r 4.8.0-0.nightly-s390x-2021-07-31-070046

    \b Named Release
        $ elliott -g openshift-4.6 rhcos -r 4.6.31

    \b Any Pullspec
        $ elliott -g openshift-4.X rhcos -r <pullspec>

    \b Assembly Definition
        $ elliott --group openshift-4.8 --assembly 4.8.21 rhcos

    \b Only lookup specified package(s)
        $ elliott -g openshift-4.6 rhcos -r 4.6.31 -p "openshift,runc,cri-o,selinux-policy"

    \b Also lookup go build version (if available)
        $ elliott -g openshift-4.6 rhcos -r 4.6.31 -p openshift --go

    \b Specify arch (default being x64)
        $ elliott -g openshift-4.6 rhcos -r 4.6.31 --arch s390x -p openshift

    \b Get all arches (supported only for named release and assembly)
        $ elliott -g openshift-4.6 rhcos -r 4.6.31 --arch all -p openshift
    """
    named_assembly = runtime.assembly not in ["stream", "test"]
    count_options = sum(map(bool, [named_assembly, release]))
    if count_options != 1:
        raise click.BadParameter("Use one of --assembly or --release")

    nightly = release and "nightly" in release
    pullspec = release and "/" in release
    named_release = not (nightly or pullspec or named_assembly)
    if arch == "all" and (pullspec or nightly):
        raise click.BadParameter("--arch=all cannot be used with --release <pullspec> or <*nightly*>")

    runtime.initialize()
    major, minor = runtime.get_major_minor()
    if nightly:
        for a in GO_ARCHES:
            if a in release:
                arch = a
                break

    version = f"{major}.{minor}"
    logger = LOGGER

    if arch == "all":
        target_arches = BREW_ARCHES
        if (major, minor) < (4, 9):
            target_arches.remove("aarch64")
    else:
        target_arches = [arch]

    payload_pullspecs = []
    if release:
        if pullspec:
            payload_pullspecs.append(release)
        elif nightly:
            payload_pullspecs.append(get_nightly_pullspec(release, arch))
        elif named_release:
            for local_arch in target_arches:
                p = get_pullspec(release, local_arch)
                payload_pullspecs.append(p)
        build_ids = [get_build_id_from_image_pullspec(runtime, p) for p in payload_pullspecs]
    elif named_assembly:
        rhcos_pullspecs = get_rhcos_pullspecs_from_assembly(runtime)
        build_ids = [
            (get_build_id_from_rhcos_pullspec(p), arch) for arch, p in rhcos_pullspecs.items() if arch in target_arches
        ]

    for build, local_arch in build_ids:
        _via_build_id(runtime, build, local_arch, version, packages, go, logger)


def get_pullspec(release, arch):
    return f"quay.io/openshift-release-dev/ocp-release:{release}-{arch}"


def get_nightly_pullspec(release, arch):
    suffix = go_suffix_for_arch(arch, "priv" in release)
    return f"registry.ci.openshift.org/ocp{suffix}/release{suffix}:{release}"


def get_rhcos_pullspecs_from_assembly(runtime):
    rhcos_def = runtime.releases_config.releases[runtime.assembly].assembly.rhcos
    if not rhcos_def:
        raise click.BadParameter(
            "only named assemblies with valid rhcos values are supported. If an assembly is "
            "based on another, try using the original assembly"
        )

    images = rhcos_def[get_primary_container_name(runtime)]["images"]
    return images


def get_build_id_from_image_pullspec(runtime, pullspec):
    green_print(f"Image pullspec: {pullspec}")
    build_id, arch = rhcos.get_build_from_payload(runtime, pullspec)
    return build_id, arch


def _via_build_id(runtime, build_id, arch, version, packages, go, logger):
    if not build_id:
        Exception("Cannot find build_id")

    arch = brew_arch_for_go_arch(arch)
    green_print(f"Build: {build_id} Arch: {arch}")
    nvrs = rhcos.get_rpm_nvrs(runtime, build_id, version, arch)
    if not nvrs:
        return
    if packages:
        packages = [p.strip() for p in packages.split(",")]
        if "openshift" in packages:
            packages.remove("openshift")
            packages.append("openshift-hyperkube")
        nvrs = [p for p in nvrs if p[0] in packages]
    if go:
        go_rpm_nvrs = util.get_golang_rpm_nvrs(nvrs, logger)
        util.pretty_print_nvrs_go(go_rpm_nvrs, ignore_na=True)
        return
    for nvr in sorted(nvrs):
        print("-".join(nvr))
