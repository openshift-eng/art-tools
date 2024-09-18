import asyncio
import os
from datetime import datetime

import click
import io
import traceback
from typing import List

from artcommonlib import exectools
from artcommonlib.exectools import RetryException
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord, ArtifactType, Engine, KonfluxBuildOutcome
from artcommonlib.util import convert_remote_git_to_https
from doozerlib.brew import get_build_objects
from doozerlib.cli import cli, click_coroutine, pass_runtime, validate_rpm_version
from doozerlib.exceptions import DoozerFatalError
from doozerlib.rpm_builder import RPMBuilder
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.runtime import Runtime


@cli.command("rpms:print", short_help="Print data for each rpm metadata")
@click.option("--short", default=False, is_flag=True, help="Suppress all output other than the data itself")
@click.option("--output", "-o", default=None, help="Write data to FILE instead of STDOUT")
@click.argument("pattern", default="{name}", nargs=1)
@pass_runtime
def rpms_print(runtime, short, output, pattern):
    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    runtime.initialize(mode="rpms", clone_distgits=False, clone_source=False)
    rpms = list(runtime.rpm_metas())

    if short:
        echo_verbose = lambda _: None
    else:
        echo_verbose = click.echo

    echo_verbose("")
    echo_verbose("------------------------------------------")

    for rpm in rpms:
        s = pattern
        s = s.replace("{name}", rpm.name)
        s = s.replace("{component}", rpm.get_component_name())

        if output is None:
            # Print to stdout
            click.echo(s)
        else:
            # Write to a file
            with io.open(output, 'a', encoding="utf-8") as out_file:
                out_file.write("{}\n".format(s))


@cli.command("rpms:clone", help="Clone a group's rpm distgit repos locally.")
@pass_runtime
def rpms_clone(runtime):
    runtime.initialize(mode='rpms', clone_distgits=True)
    # Never delete after clone; defeats the purpose of cloning
    runtime.remove_tmp_working_dir = False


@cli.command("rpms:clone-sources", help="Clone a group's rpm source repos locally and add to sources yaml.")
@click.option("--output-yml", metavar="YAML_PATH",
              help="Output yml file to write sources dict to. Can be same as --sources option but must be explicitly specified.")
@pass_runtime
def rpms_clone_sources(runtime, output_yml):
    runtime.initialize(mode='rpms')
    # Never delete after clone; defeats the purpose of cloning
    runtime.remove_tmp_working_dir = False
    [r for r in runtime.rpm_metas()]
    if output_yml:
        runtime.export_sources(output_yml)


@cli.command("rpms:rebase-and-build", help="Rebase and build rpms in the group or given by --rpms.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_rpm_version,
              help="Version string to populate in specfile.", required=True)
@click.option("--release", metavar='RELEASE', default=None,
              help="Release label to populate in specfile.", required=True)
@click.option("--embargoed", default=False, is_flag=True,
              help="Add .p1 to the release string for all rpms, which indicates those rpms have embargoed fixes")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
@click_coroutine
async def rpms_rebase_and_build(runtime: Runtime, version: str, release: str, embargoed: bool, scratch: bool,
                                dry_run: bool):
    """
    Attempts to rebase and build rpms for all of the defined rpms
    in a group.
    """
    exit_code = await _rpms_rebase_and_build(runtime, version=version, release=release, embargoed=embargoed,
                                             scratch=scratch, dry_run=dry_run)
    exit(exit_code)


async def _rpms_rebase_and_build(runtime: Runtime, version: str, release: str, embargoed: bool, scratch: bool,
                                 dry_run: bool):
    if version.startswith('v'):
        version = version[1:]

    runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)  # We will clone distgits later.
    if runtime.local:
        raise DoozerFatalError("Local RPM build is not currently supported.")
    if runtime.group_config.public_upstreams and (release is None or not release.endswith(".p?")):
        raise click.BadParameter(
            "You must explicitly specify a `release` ending with `.p?` when there is a public upstream mapping in ocp-build-data.")

    runtime.assert_mutation_is_permitted()

    rpms: List[RPMMetadata] = runtime.rpm_metas()
    if not rpms:
        runtime.logger.error("No RPMs found. Check the arguments.")
        exit(0)

    if embargoed:
        for rpm in rpms:
            rpm.private_fix = True

    with runtime.shared_koji_client_session() as koji_api:
        if not koji_api.logged_in:
            koji_api.gssapi_login()

    builder = RPMBuilder(runtime, dry_run=dry_run, scratch=scratch)

    async def _rebase_and_build(rpm: RPMMetadata):
        status = await _rebase_rpm(runtime, builder, rpm, version, release)
        if status != 0:
            return status
        status = await _build_rpm(runtime, builder, rpm)
        return status

    tasks = [asyncio.ensure_future(_rebase_and_build(rpm)) for rpm in rpms]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed = [rpms[i].distgit_key for i, r in enumerate(results) if r != 0]
    if failed:
        runtime.logger.error("\n".join(["Build failures:"] + sorted(failed)))
        return 1
    return 0


@cli.command("rpms:rebase", help="Rebase rpms in the group or given by --rpms.")
@click.option("--version", metavar='VERSION', default=None, callback=validate_rpm_version,
              help="Version string to populate in specfile.", required=True)
@click.option("--release", metavar='RELEASE', default=None,
              help="Release label to populate in specfile.", required=True)
@click.option("--embargoed", default=False, is_flag=True,
              help="Add .p1 to the release string for all rpms, which indicates those rpms have embargoed fixes")
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option('--push/--no-push', default=False, is_flag=True,
              help='Push changes back to config repo. --no-push is default')
@pass_runtime
@click_coroutine
async def rpms_rebase(runtime: Runtime, version: str, release: str, embargoed: bool, push: bool, dry_run: bool):
    """
    Attempts to rebase rpms for all of the defined rpms in a group.

    For each rpm, uploads the source tarball to distgit lookaside cache and pulls the current source rpm spec file (and potentially other supporting
    files) into distgit with transformations defined in the config yaml applied.

    This operation will also set the version and release in the file according to the
    command line arguments provided.
    """
    exit_code = await _rpms_rebase(runtime, version=version, release=release, embargoed=embargoed, push=push,
                                   dry_run=dry_run)
    exit(exit_code)


async def _rpms_rebase(runtime: Runtime, version: str, release: str, embargoed: bool, push: bool, dry_run: bool):
    if version.startswith('v'):
        version = version[1:]

    runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)  # We will clone distgits later.
    if runtime.local:
        raise DoozerFatalError("Local RPM build is not currently supported.")
    if runtime.group_config.public_upstreams and (release is None or not release.endswith(".p?")):
        raise click.BadParameter(
            "You must explicitly specify a `release` ending with `.p?` when there is a public upstream mapping in ocp-build-data.")

    runtime.assert_mutation_is_permitted()

    rpms: List[RPMMetadata] = runtime.rpm_metas()
    if not rpms:
        runtime.logger.error("No RPMs found. Check the arguments.")
        exit(0)

    if embargoed:
        for rpm in rpms:
            rpm.private_fix = True

    builder = RPMBuilder(runtime, push=push, dry_run=dry_run)
    tasks = [asyncio.ensure_future(_rebase_rpm(runtime, builder, rpm, version, release)) for rpm in rpms]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed = [rpms[i].distgit_key for i, r in enumerate(results) if r != 0]
    if failed:
        runtime.logger.error("\n".join(["Build failures:"] + sorted(failed)))
        return 1
    return 0


async def _rebase_rpm(runtime: Runtime, builder: RPMBuilder, rpm: RPMMetadata, version, release):
    logger = rpm.logger
    action = "rebase_rpm"
    record = {
        "distgit_key": rpm.distgit_key,
        "rpm": rpm.rpm_name,
        "version": version,
        "release": release,
        "message": "Unknown failure",
        "status": -1,
        # Status defaults to failure until explicitly set by success. This handles raised exceptions.
    }
    try:
        await builder.rebase(rpm, version, release)
        record["version"] = rpm.version
        record["release"] = rpm.release
        record["specfile"] = rpm.specfile
        record["private_fix"] = rpm.private_fix
        record["source_commit"] = rpm.pre_init_sha or ""
        record["dg_branch"] = rpm.distgit_repo().branch
        record["status"] = 0
        record["message"] = "Success"
        logger.info("Successfully rebased rpm: %s", rpm.distgit_key)
    except Exception:
        tb = traceback.format_exc()
        record["message"] = "Exception occurred:\n{}".format(tb)
        logger.error("Exception occurred when rebasing %s:\n%s", rpm.distgit_key, tb)
    finally:
        runtime.record_logger.add_record(action, **record)
    return record["status"]


@cli.command("rpms:build", help="Build rpms in the group or given by --rpms.")
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
@click_coroutine
async def rpms_build(runtime: Runtime, scratch: bool, dry_run: bool):
    """
    Attempts to build rpms for all of the defined rpms
    in a group.
    """
    exit_code = await _rpms_build(runtime, scratch=scratch, dry_run=dry_run)
    exit(exit_code)


async def _rpms_build(runtime: Runtime, scratch: bool, dry_run: bool):
    runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)  # We will clone distgits later.
    if runtime.local:
        raise DoozerFatalError("Local RPM build is not currently supported.")

    runtime.assert_mutation_is_permitted()

    rpms: List[RPMMetadata] = runtime.rpm_metas()
    if not rpms:
        runtime.logger.error("No RPMs found. Check the arguments.")
        exit(0)

    with runtime.shared_koji_client_session() as koji_api:
        if not koji_api.logged_in:
            koji_api.gssapi_login()

    builder = RPMBuilder(runtime, dry_run=dry_run, scratch=scratch)
    tasks = [asyncio.ensure_future(_build_rpm(runtime, builder, rpm)) for rpm in rpms]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    failed = [rpms[i].distgit_key for i, r in enumerate(results) if r != 0]
    if failed:
        runtime.logger.error("\n".join(["Build failures:"] + sorted(failed)))
        return 1
    return 0


async def _build_rpm(runtime: Runtime, builder: RPMBuilder, rpm: RPMMetadata):
    logger = rpm.logger
    action = "build_rpm"
    record = {
        "distgit_key": rpm.distgit_key,
        "rpm": rpm.rpm_name,
        "message": "Unknown failure",
        "targets": rpm.targets,
        "status": -1,
        # Status defaults to failure until explicitly set by success. This handles raised exceptions.
    }
    task_ids = []
    task_urls = []
    try:
        task_ids, task_urls, nvrs = await builder.build(rpm)
        record["nvrs"] = ",".join(nvrs)
        record["specfile"] = rpm.specfile
        record["status"] = 0
        record["message"] = "Success"
        logger.info("Successfully built rpm: %s ; Task URLs: %s", rpm.distgit_key, [url for url in task_urls])
        await update_konflux_db(runtime, rpm, record)

    except (Exception, KeyboardInterrupt) as e:
        tb = traceback.format_exc()
        record["message"] = "Exception occurred:\n{}".format(tb)
        logger.error("Exception occurred when building %s:\n%s", rpm.distgit_key, tb)
        if isinstance(e, RetryException):
            task_ids, task_urls = e.args[1]
    finally:
        if task_ids:
            record["task_ids"] = task_ids
            record["task_urls"] = task_urls
            record["task_id"] = task_ids[0]
            record["task_url"] = task_urls[0]
        runtime.record_logger.add_record(action, **record)
    return record["status"]


async def update_konflux_db(runtime, rpm: RPMMetadata, record: dict):
    nvrs = record["nvrs"].split(",")

    with runtime.shared_koji_client_session() as koji_api:
        builds = get_build_objects(nvrs, koji_api)

    _, version, _ = await exectools.cmd_gather_async(
        ["rpmspec", "-q", "--qf", "%{version}", "--srpm", "--undefine", "dist", "--",
         str(rpm.specfile)])

    for build in builds:
        rebase_url = build["extra"]["source"]["original_url"].split('+')[-1]
        rebase_repo_url, rebase_commitish = rebase_url.split('#')

        with open(rpm.specfile) as specfile:
            rpmspec = specfile.read().splitlines()
            try:
                commitish = [line for line in rpmspec if "Update to source commit" in line][0].split(" ")[-1].split("/")[-1]
            except IndexError:
                rpm.logger.warning('Could not determine commitish for rpm %s', rpm.rpm_name)
                commitish = ''

        build_record = KonfluxBuildRecord(
            name=rpm.rpm_name,
            group=runtime.group,
            version=version,
            release=rpm.release,
            assembly=runtime.assembly,
            el_target=build["nvr"].split(".")[-1],
            arches=rpm.get_arches(),
            installed_packages=[],
            parent_images=[],
            source_repo=convert_remote_git_to_https(rpm.source.git.url),
            commitish=commitish,
            rebase_repo_url=rebase_repo_url,
            rebase_commitish=rebase_commitish,
            embargoed="p1" in rpm.release.split("."),
            start_time=datetime.strptime(build["creation_time"], '%Y-%m-%d %H:%M:%S.%f'),
            end_time=datetime.strptime(build["completion_time"], '%Y-%m-%d %H:%M:%S.%f'),
            artifact_type=ArtifactType.RPM,
            engine=Engine.BREW,
            image_pullspec="n/a",
            image_tag="n/a",
            outcome=KonfluxBuildOutcome.SUCCESS,
            art_job_url=os.getenv("BUILD_URL", "n/a"),
            build_pipeline_url=str(build["task_id"]),
            pipeline_commit='n/a',
            nvr=build["nvr"],
            build_id=str(build["build_id"])
        )

        runtime.konflux_db.add_build(build_record)
        rpm.logger.info('Brew build info for %s stored successfully', build["nvr"])
