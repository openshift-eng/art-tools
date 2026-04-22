import logging
import os
from pprint import pprint

import click
from artcommonlib.assembly import AssemblyIssue, AssemblyIssueCode
from artcommonlib.konflux.package_rpm_finder import PackageRpmFinder

from doozerlib.assembly_inspector import AssemblyInspector
from doozerlib.cli import cli, click_coroutine
from doozerlib.cli.release_gen_payload import PayloadGenerator
from doozerlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)


@cli.command("inspect:stream", short_help="Inspect stream assembly for assembly issues")
@click.argument(
    "code", type=click.Choice([code.name for code in AssemblyIssueCode], case_sensitive=False), required=True
)
@click.option("--strict", default=False, type=bool, is_flag=True, help='Fail even if permitted')
@click_coroutine
@click.pass_obj
async def inspect_stream(runtime: Runtime, code: AssemblyIssueCode, strict: bool):
    if runtime.build_system != "konflux" or runtime.assembly != "stream":
        raise ValueError("This command is only intended to be used with --build-system=konflux and assembly=stream")

    code = AssemblyIssueCode[code]
    runtime.initialize(config_only=True)

    if code == AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS:
        assembly_inspector = AssemblyInspector(runtime)
        await assembly_inspector.initialize(lookup_mode=None)
        package_rpm_finder = PackageRpmFinder(runtime)
        payload_generator = PayloadGenerator(runtime, package_rpm_finder)
        registry_config = os.getenv("QUAY_AUTH_FILE")
        rhcos_builds, rhcos_inconsistencies = _check_inconsistent_rhcos_rpms(
            runtime,
            assembly_inspector,
            payload_generator,
            registry_config=registry_config,
        )
        if rhcos_inconsistencies:
            msg = f'Found RHCOS inconsistencies in builds {rhcos_builds}'
            LOGGER.info(msg)
            pprint(rhcos_inconsistencies)
            assembly_issue = AssemblyIssue(msg, component='rhcos', code=code)
            if assembly_inspector.does_permit(assembly_issue):
                LOGGER.info(f'Assembly permits code {code}.')
                if not strict:
                    exit(0)
                LOGGER.info('Running in strict mode')
            exit(1)
        LOGGER.info(f'RHCOS builds consistent {rhcos_builds}')
    elif code == AssemblyIssueCode.FAILED_CONSISTENCY_REQUIREMENT:
        requirements = runtime.group_config.rhcos.require_consistency
        if not requirements:
            LOGGER.info("No cross-payload consistency requirements defined in group.yml")
            exit(0)

        runtime.logger.info("Checking cross-payload consistency requirements defined in group.yml")
        for img in requirements.keys():
            runtime.late_resolve_image(img, add=True)
        assembly_inspector = AssemblyInspector(runtime)
        await assembly_inspector.initialize(lookup_mode="images")
        package_rpm_finder = PackageRpmFinder(runtime)
        payload_generator = PayloadGenerator(runtime, package_rpm_finder)
        registry_config = os.getenv("QUAY_AUTH_FILE")
        issues = _check_cross_payload_consistency_requirements(
            runtime,
            assembly_inspector,
            payload_generator,
            requirements,
            registry_config=registry_config,
        )
        if issues:
            LOGGER.info('Payload contents consistency requirements not satisfied')
            pprint(issues)
            not_permitted = [issue for issue in issues if not assembly_inspector.does_permit(issue)]
            if not_permitted:
                raise ValueError(f'Assembly does not permit: {not_permitted}')
            elif strict:
                raise ValueError(f'Running in strict mode; saw: {issues}')
        LOGGER.info('Payload contents consistency requirements satisfied')
    else:
        raise ValueError(f'AssemblyIssueCode {code} not supported at this time :(')


def _check_inconsistent_rhcos_rpms(
    runtime: Runtime,
    assembly_inspector: AssemblyInspector,
    payload_generator: PayloadGenerator,
    registry_config: str = None,
):
    rhcos_builds = []
    for arch in runtime.group_config.arches:
        build_inspector = assembly_inspector.get_rhcos_build(arch, registry_config=registry_config)
        rhcos_builds.append(build_inspector)
    runtime.logger.info(f"Checking following builds for inconsistency: {rhcos_builds}")
    rhcos_inconsistencies = payload_generator.find_rhcos_build_rpm_inconsistencies(rhcos_builds)
    return rhcos_builds, rhcos_inconsistencies


def _check_cross_payload_consistency_requirements(
    runtime: Runtime,
    assembly_inspector: AssemblyInspector,
    payload_generator: PayloadGenerator,
    requirements: dict,
    registry_config: str = None,
):
    issues = []
    for arch in runtime.group_config.arches:
        issues.extend(
            payload_generator.find_rhcos_payload_rpm_inconsistencies(
                assembly_inspector.get_rhcos_build(arch, registry_config=registry_config),
                assembly_inspector.get_group_release_images(),
                requirements,
            )
        )
    return issues
