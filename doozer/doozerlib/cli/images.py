import io
import json
import pathlib
import subprocess
import sys
import traceback
import urllib
import urllib.error
import urllib.parse
import urllib.request
from numbers import Number
from typing import Optional, cast

import click
import koji
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import color_print, green_print, yellow_print
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from dockerfile_parse import DockerfileParser
from future import standard_library

from doozerlib import Runtime, coverity, state
from doozerlib.brew import get_watch_task_info_copy
from doozerlib.cli import cli, option_commit_message, option_push, pass_runtime, validate_semver_major_minor_patch
from doozerlib.distgit import ImageDistGitRepo
from doozerlib.exceptions import DoozerFatalError
from doozerlib.source_resolver import SourceResolver


@cli.command("images:clone", help="Clone a group's image distgit repos locally.")
@click.option('--clone-upstreams', default=False, is_flag=True, help='Also clone upstream sources.')
@pass_runtime
def images_clone(runtime, clone_upstreams):
    runtime.initialize(clone_distgits=True, clone_source=clone_upstreams)
    # Never delete after clone; defeats the purpose of cloning
    runtime.remove_tmp_working_dir = False


@cli.command("images:list", help="List of distgits being selected.")
@click.option("--json", "is_json", is_flag=True, default=False, help="Output the list in JSON format.")
@pass_runtime
def images_list(runtime, is_json):
    runtime.initialize(clone_distgits=False, prevent_cloning=True)
    if is_json:
        click.echo(json.dumps({"images": [image.distgit_key for image in runtime.image_metas()]}))
        return

    click.echo("------------------------------------------")
    for image in runtime.image_metas():
        click.echo(image.qualified_name)
    click.echo("------------------------------------------")
    click.echo("%s images" % len(runtime.image_metas()))


@cli.command("images:push-distgit", short_help="Push all distgist repos in working-dir.")
@pass_runtime
def images_push_distgit(runtime):
    """
    Run to execute an rhpkg push on all locally cloned distgit
    repositories. This is useful following a series of modifications
    in the local clones.
    """
    runtime.initialize(clone_distgits=True)
    runtime.push_distgits()


@cli.command("images:covscan", short_help="Run a coverity scan for the specified images")
@click.option(
    "--result-archive",
    metavar="ARCHIVE_DIR",
    required=True,
    help="The covscan process computes diffs between different runs. Location where all runs can store data.",
)
@click.option(
    "--local-repo-rhel-7",
    metavar="REPO_DIR",
    default=[],
    multiple=True,
    help="Absolute path of yum repo to make available for rhel-7 scanner images",
)
@click.option(
    "--local-repo-rhel-8",
    metavar="REPO_DIR",
    default=[],
    multiple=True,
    help="Absolute path of yum repo to make available for rhel-8 scanner images",
)
@click.option(
    "--local-repo-rhel-9",
    metavar="REPO_DIR",
    default=[],
    multiple=True,
    help="Absolute path of yum repo to make available for rhel-9 scanner images",
)
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default="unsigned",
    help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).",
)
@click.option('--preserve-builder-images', default=False, is_flag=True, help='Reuse any previous builder images')
@click.option(
    '--force-analysis',
    default=False,
    is_flag=True,
    help='Even if an existing analysis is present for a given hash, re-run',
)
@click.option(
    '--ignore-waived', default=False, is_flag=True, help='Ignore any previously detected waived results (all=diff)'
)
@click.option('--https-proxy', default='', help='HTTPS proxy to be used during image builds')
@click.option('--podman-sudo', is_flag=True, help="Run podman with sudo")
@click.option("--podman-tmpdir", help='Set the temporary storage location of downloaded container images for podman')
@pass_runtime
def images_covscan(
    runtime: Runtime,
    result_archive,
    local_repo_rhel_7,
    local_repo_rhel_8,
    local_repo_rhel_9,
    repo_type,
    preserve_builder_images,
    force_analysis,
    ignore_waived,
    https_proxy,
    podman_sudo: bool,
    podman_tmpdir: Optional[str],
):
    """
    Runs a coverity scan against the specified images.

    \b
    Workflow:
    1. Doozer is invoked against a set of images.

    2. Each dockerfile in disgit is analyzed, instruction by instruction.

    3. Two or more Dockerfiles will be generated for each image:
       - A Dockerfile for each parent image specified in a FROM instruction. These will generate
         parent image derivatives. These derivatives are FROM the parent, but layer on coverity
         scanning tools. When you specify --local-repo-rhel-7 / --local-repo-rhel-8, these
         repos are mounted into the image and are used as the source for installing coverity tools.
         See the section below for information on creating these repos.
         If two stages are FROM the exact same image, they will use the same derivative image
         without having to rebuild it. To save time during development, you can run with
         --preserve-builder-images to keep these images around between executions.
       - A Dockerfile like the one in distgit, but with:
         1. Parent images replaced by parent image derivatives.
         2. RUN commands wrapped into scripts that will be executed under coverity's build
            monitoring tools.
         3. The execution of coverity tools that will capture information about the source code
            as well as run a coverity analysis against information emitted by the coverity tools.
        Each stage in the Dockerfile is treated as an independent coverity scan/build/analyze.
        This is because we may be moving between RHEL versions / libcs / etc between stages,
        and I believe a coverity analysis run in the same stage that emitted the output is
        fundamentally less risky.
        The build of this Dockerfile is performed by podman and volumes are mounted in which
        will be used for the coverity output of each stage. Specifically,
        <workdir>/<distgits>/<image>/<cov> is mounted into /cov within the build.
        Each stage will output its coverity information to /cov/<stage_number> (1 based).
        e.g. /cov/1  would contain coverity information for the first FROM in the distgit
        Dockerfile, /cov/2 for the second FROM, and so on.
        The desired output of each stage's analysis is /cov/<stage_number>/all_results.js.

    6. Once all_results.js exists for each stage, we want to deposit them in the results archive. In
       addition to all_results.js, we want to translate the results into HTML for easy consumption.

    7. To help prodsec focus on new problems, we also try to compute diff_results.js for each stage.
       To do this, doozer searches the result archive for any preceding commits that have been computed for this
       distgit's commit history. If one is found, and a 'waived.flag' file exists in the directory, then
       a tool called 'csdiff' is used to compare the difference between the current run and the most
       recently waived commit's 'all_results.js'.

    12. The computed difference is written to the current run's archive as 'diff_results.js' and transformed
       into html (diff_results.html). If no previous commit was waived, all_results.js/html and
       diff_results.js/html will match.

    13. Non-empty diff_results will be written into the doozer record. This record should be parsed by the
        pipeline.

    14. The pipeline (not doozer) should mirror the results out to a file server (e.g. rcm-guest)
        and then slack/email owners about the difference. The pipeline should wait until results are waived.

    15. If the results are waived, the pipeline should write 'waived.flag' to the appropriate result
        archive directory.

    16. Rinse and repeat for the next commit. Note that if doozer finds results computed already for a
        given commit hash on in the results archive, it will not recompute them with coverity.

    \b
    Caching coverity repos for rhel-7 locally:
    Use --local-repos to supply coverity repositories for the scanner image.
    $ reposync  -c covscan.repo -a x86_64 -d -p covscan_repos -n -e covscan_cache -r covscan -r covscan-testing
    $ createrepo_c covscan_repos/covscan
    $ createrepo_c covscan_repos/covscan-testing
    Where covscan.repo is:
        [covscan]
        name=Copr repo for covscan
        baseurl=http://coprbe.devel.redhat.com/repos/kdudka/covscan/epel-7-$basearch/
        skip_if_unavailable=True
        gpgcheck=0
        gpgkey=http://coprbe.devel.redhat.com/repos/kdudka/covscan/pubkey.gpg
        enabled=1
        enabled_metadata=1

        [covscan-testing]
        name=Copr repo for covscan-testing
        baseurl=http://coprbe.devel.redhat.com/repos/kdudka/covscan-testing/epel-7-$basearch/
        skip_if_unavailable=True
        gpgcheck=0
        gpgkey=http://coprbe.devel.redhat.com/repos/kdudka/covscan-testing/pubkey.gpg
        enabled=0
        enabled_metadata=1

    """

    def absolutize(path):
        return str(pathlib.Path(path).absolute())

    result_archive = absolutize(result_archive)
    local_repo_rhel_7 = [absolutize(repo) for repo in local_repo_rhel_7]
    local_repo_rhel_8 = [absolutize(repo) for repo in local_repo_rhel_8]
    local_repo_rhel_9 = [absolutize(repo) for repo in local_repo_rhel_9]

    runtime.initialize()

    def delete_images(key, value):
        """
        Deletes images with a particular label value
        """
        podman_cmd = "sudo podman" if podman_sudo else "podman"
        rc, image_list, stderr = exectools.cmd_gather(
            f'{podman_cmd} images -q --filter label={key}={value}', strip=True
        )
        if not image_list:
            runtime.logger.info(f'No images found with {key}={value} label exist: {stderr}')
            return
        targets = " ".join(image_list.split())
        rc, _, _ = exectools.cmd_gather(f'{podman_cmd} rmi {targets}')
        if rc != 0:
            runtime.logger.warning(f'Unable to remove all images with {key}={value}')

    # Clean up any old runner images to keep image storage under control.
    delete_images('DOOZER_COVSCAN_RUNNER', runtime.group_config.name)

    if not preserve_builder_images:
        delete_images('DOOZER_COVSCAN_PARENT', runtime.group_config.name)

    successes = []
    failures = []
    for image in runtime.image_metas():
        dg = cast(ImageDistGitRepo, image.distgit_repo())
        with Dir(dg.distgit_dir):
            dg.pull_sources()
            dg_commit_hash = dg.sha
            if not dg_commit_hash:
                raise ValueError(f"Distgit commit hash for {dg.name} is unknown.")
            cc = coverity.CoverityContext(
                image,
                dg_commit_hash,
                result_archive,
                repo_type=repo_type,
                local_repo_rhel_7=local_repo_rhel_7,
                local_repo_rhel_8=local_repo_rhel_8,
                local_repo_rhel_9=local_repo_rhel_9,
                force_analysis=force_analysis,
                ignore_waived=ignore_waived,
                https_proxy=https_proxy,
                podman_sudo=podman_sudo,
                podman_tmpdir=podman_tmpdir,
            )

            if image.covscan(cc):
                successes.append(image.distgit_key)
            else:
                failures.append(image.distgit_key)

    # Clean up runner images
    delete_images('DOOZER_COVSCAN_RUNNER', runtime.group_config.name)

    print(yaml.safe_dump({'success': successes, 'failure': failures}))


@cli.command("images:rebase", short_help="Refresh a group's distgit content from source content.")
@click.option(
    "--version",
    metavar='VERSION',
    default=None,
    callback=validate_semver_major_minor_patch,
    help="Version string to populate in Dockerfiles. \"auto\" gets version from atomic-openshift RPM",
)
@click.option("--release", metavar='RELEASE', default=None, help="Release string to populate in Dockerfiles.")
@click.option(
    "--embargoed",
    is_flag=True,
    help="Add .p1 to the release string for all images, which indicates those images have embargoed fixes",
)
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default="unsigned",
    help="Repo group type to use for version autodetection scan (e.g. signed, unsigned).",
)
@click.option(
    "--force-yum-updates",
    is_flag=True,
    default=False,
    help="Inject \"yum update -y\" in the final stage of an image build. This ensures the component image will be able to override RPMs it is inheriting from its parent image using RPMs in the rebuild plashet.",
)
@option_commit_message
@option_push
@pass_runtime
def images_rebase(
    runtime: Runtime,
    version: Optional[str],
    release: Optional[str],
    embargoed: bool,
    repo_type: str,
    force_yum_updates: bool,
    message: str,
    push: bool,
):
    """
    Many of the Dockerfiles stored in distgit are based off of content managed in GitHub.
    For example, openshift-enterprise-node should always closely reflect the changes
    being made upstream in github.com/openshift/ose/images/node. This operation
    goes out and pulls the current source Dockerfile (and potentially other supporting
    files) into distgit and applies any transformations defined in the config yaml associated
    with the distgit repo.

    This operation will also set the version and release in the file according to the
    command line arguments provided.

    If a distgit repo does not have associated source (i.e. it is managed directly in
    distgit), the Dockerfile in distgit will not be rebased, but other aspects of the
    metadata may be applied (base image, tags, etc) along with the version and release.
    """

    runtime.initialize(validate_content_sets=True)

    if runtime.group_config.public_upstreams and (release is None or release != "+" and not release.endswith(".p?")):
        raise click.BadParameter(
            "You must explicitly specify a `release` ending with `.p?` (or '+') when there is a public upstream mapping in ocp-build-data."
        )

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    cmd = runtime.command
    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    # Get the version from the atomic-openshift package in the RPM repo
    if version == "auto":
        version = runtime.auto_version(repo_type)

    if version and not runtime.valid_version(version):
        raise ValueError(
            "invalid version string: {}, expecting like v3.4 or v1.2.3".format(version),
        )

    runtime.clone_distgits()
    metas = runtime.ordered_image_metas()
    lstate['total'] = len(metas)

    def dgr_rebase(image_meta, terminate_event):
        try:
            dgr = image_meta.distgit_repo()
            if embargoed:
                dgr.private_fix = True
            (real_version, real_release) = dgr.rebase_dir(version, release, terminate_event, force_yum_updates)
            sha = dgr.commit(message, log_diff=True)
            dgr.tag(real_version, real_release)
            runtime.record_logger.add_record(
                "distgit_commit",
                distgit=image_meta.qualified_name,
                image=image_meta.config.name,
                sha=sha,
            )
            state.record_image_success(lstate, image_meta)

            if push:
                (meta, success) = dgr.push()
                if success is not True:
                    state.record_image_fail(lstate, meta, success)
                dgr.wait_on_cgit_file()

        except Exception as ex:
            # Only the message will recorded in the state. Make sure we print out a stacktrace in the logs.
            traceback.print_exc()

            owners = image_meta.config.owners
            owners = ",".join(list(owners) if owners is not Missing else [])
            runtime.record_logger.add_record(
                "distgit_commit_failure",
                distgit=image_meta.qualified_name,
                image=image_meta.config.name,
                owners=owners,
                message=str(ex).replace("|", ""),
            )
            msg = str(ex)
            state.record_image_fail(lstate, image_meta, msg, runtime.logger)
            return False
        return True

    jobs = exectools.parallel_exec(
        lambda image_meta, terminate_event: dgr_rebase(image_meta, terminate_event),
        metas,
    )
    jobs.get()

    state.record_image_finish(lstate)

    failed = []
    for img, status in lstate['images'].items():
        if status is not True:  # anything other than true is fail
            failed.append(img)

    if failed:
        msg = "The following non-critical images failed to rebase:\n{}".format('\n'.join(failed))
        yellow_print(msg)

    if lstate['status'] == state.STATE_FAIL or lstate['success'] == 0:
        raise DoozerFatalError('One or more images failed. See state.yaml')


@cli.command("images:foreach", short_help="Run a command relative to each distgit dir.")
@click.argument("cmd", nargs=-1)
@click.option("--message", "-m", metavar='MSG', help="Commit message for dist-git.", required=False)
@option_push
@pass_runtime
def images_foreach(runtime, cmd, message, push):
    """
    Clones all distgit repos found in the specified group and runs an arbitrary
    command once for each local distgit directory. If the command runs without
    error for all directories, a commit will be made. If not a dry_run,
    the repo will be pushed.

    \b
    The following environment variables will be available in each invocation:
    doozer_repo_name : The name of the distgit repository
    doozer_repo_namespace : The distgit repository namespaces (e.g. containers, rpms))
    doozer_config_filename : The config yaml (basename, no path) associated with an image
    doozer_distgit_key : The name of the distgit_key used with -i, -x for this image
    doozer_image_name : The name of the image from Dockerfile
    doozer_image_version : The current version found in the Dockerfile
    doozer_group: The group for this invocation
    doozer_data_path: The directory containing the doozer metadata
    doozer_working_dir: The current working directory
    """
    runtime.initialize(clone_distgits=True)

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    cmd_str = " ".join(cmd)

    for image in runtime.image_metas():
        dgr = image.distgit_repo()
        with Dir(dgr.distgit_dir):
            runtime.logger.info("Executing in %s: [%s]" % (dgr.distgit_dir, cmd_str))

            dfp = DockerfileParser()
            dfp.content = image.fetch_cgit_file("Dockerfile")

            if (
                subprocess.call(
                    cmd_str,
                    shell=True,
                    env={
                        "doozer_repo_name": image.name,
                        "doozer_repo_namespace": image.namespace,
                        "doozer_image_name": dfp.labels["name"],
                        "doozer_image_version": dfp.labels["version"],
                        "doozer_group": runtime.group,
                        "doozer_data_path": runtime.data_dir,
                        "doozer_working_dir": runtime.working_dir,
                        "doozer_config_filename": image.config_filename,
                        "doozer_distgit_key": image.distgit_key,
                    },
                )
                != 0
            ):
                raise IOError("Command return non-zero status")
            runtime.logger.info("\n")

        if message is not None:
            dgr.commit(message)

    if push:
        runtime.push_distgits()


@cli.command("images:revert", help="Revert a fixed number of commits in each distgit.")
@click.argument("count", nargs=1)
@click.option("--message", "-m", metavar='MSG', help="Commit message for dist-git.", default=None, required=False)
@option_push
@pass_runtime
def images_revert(runtime, count, message, push):
    """
    Revert a particular number of commits in each distgit repository. If
    a message is specified, a new commit will be made.
    """
    runtime.initialize()

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    count = int(count) - 1
    if count < 0:
        runtime.logger.info("Revert count must be >= 1")

    if count == 0:
        commit_range = "HEAD"
    else:
        commit_range = "HEAD~%s..HEAD" % count

    cmd = ["git", "revert", "--no-commit", commit_range]

    cmd_str = " ".join(cmd)
    runtime.clone_distgits()
    dgrs = [image.distgit_repo() for image in runtime.image_metas()]
    for dgr in dgrs:
        with Dir(dgr.distgit_dir):
            runtime.logger.info("Running revert in %s: [%s]" % (dgr.distgit_dir, cmd_str))
            if subprocess.call(cmd_str, shell=True) != 0:
                raise IOError("Command return non-zero status")
            runtime.logger.info("\n")

        if message is not None:
            dgr.commit(message)

    if push:
        runtime.push_distgits()


@cli.command("images:merge-branch", help="Copy content of source branch to target.")
@click.option("--target", metavar="TARGET_BRANCH", help="Branch to populate from source branch.")
@click.option(
    '--allow-overwrite',
    default=False,
    is_flag=True,
    help='Merge in source branch even if Dockerfile already exists in distgit',
)
@option_push
@pass_runtime
def images_merge(runtime, target, push, allow_overwrite):
    """
    For each distgit repo, copies the content of the group's branch to a new
    branch.
    """
    runtime.initialize()

    # This is ok to run if automation is frozen as long as you are not pushing
    if push:
        runtime.assert_mutation_is_permitted()

    # If not pushing, do not clean up our work
    runtime.remove_tmp_working_dir = push

    runtime.clone_distgits()
    dgrs = [image.distgit_repo() for image in runtime.image_metas()]
    for dgr in dgrs:
        with Dir(dgr.distgit_dir):
            dgr.logger.info("Merging from branch {} to {}".format(dgr.branch, target))
            dgr.merge_branch(target, allow_overwrite)
            runtime.logger.info("\n")

    if push:
        runtime.push_distgits()


def _taskinfo_has_timestamp(task_info, key_name):
    """
    Tests to see if a named timestamp exists in a koji taskinfo
    dict.
    :param task_info: The taskinfo dict to check
    :param key_name: The name of the timestamp key
    :return: Returns True if the timestamp is found and is a Number
    """
    return isinstance(task_info.get(key_name, None), Number)


def print_build_metrics(runtime):
    watch_task_info = get_watch_task_info_copy()
    runtime.logger.info("\n\n\nImage build metrics:")
    runtime.logger.info("Number of brew tasks attempted: {}".format(len(watch_task_info)))

    # Make sure all the tasks have the expected timestamps:
    # https://github.com/openshift/enterprise-images/pull/178#discussion_r173812940
    for task_id in list(watch_task_info.keys()):
        info = watch_task_info[task_id]
        runtime.logger.debug("Watch task info:\n {}\n\n".format(info))
        # Error unless all true
        if not (
            'id' in info
            and koji.TASK_STATES[info['state']] == 'CLOSED'
            and _taskinfo_has_timestamp(info, 'create_ts')
            and _taskinfo_has_timestamp(info, 'start_ts')
            and _taskinfo_has_timestamp(info, 'completion_ts')
        ):
            runtime.logger.error("Discarding incomplete/error task info: {}".format(info))
            del watch_task_info[task_id]

    runtime.logger.info("Number of brew tasks successful: {}".format(len(watch_task_info)))

    # An estimate of how long the build time was extended due to FREE state (i.e. waiting for capacity)
    elapsed_wait_minutes = 0

    # If two builds each take one minute of actual active CPU time to complete, this value will be 2.
    aggregate_build_secs = 0

    # If two jobs wait 1m for in FREE state, this value will be '2' even if
    # the respective wait periods overlap. This is different from elapsed_wait_minutes
    # which is harder to calculate.
    aggregate_wait_secs = 0

    # Will be populated with earliest creation timestamp found in all the koji tasks; initialize with
    # infinity to support min() logic.
    min_create_ts = float('inf')

    # Will be populated with the latest completion timestamp found in all the koji tasks
    max_completion_ts = 0

    # Loop through all koji task infos and calculate min
    for task_id, info in watch_task_info.items():
        create_ts = info['create_ts']
        completion_ts = info['completion_ts']
        start_ts = info['start_ts']
        min_create_ts = min(create_ts, min_create_ts)
        max_completion_ts = max(completion_ts, max_completion_ts)
        build_secs = completion_ts - start_ts
        aggregate_build_secs += build_secs
        wait_secs = start_ts - create_ts
        aggregate_wait_secs += wait_secs

        runtime.logger.info(
            'Task {} took {:.1f}m of active build and was waiting to start for {:.1f}m'.format(
                task_id, build_secs / 60.0, wait_secs / 60.0
            )
        )
    runtime.logger.info('Aggregate time all builds spent building {:.1f}m'.format(aggregate_build_secs / 60.0))
    runtime.logger.info('Aggregate time all builds spent waiting {:.1f}m'.format(aggregate_wait_secs / 60.0))

    # If we successfully found timestamps in completed builds
    if watch_task_info:
        # For each minute which elapsed between the first build created (min_create_ts) to the
        # last build to complete (max_completion_ts), check whether there was any build that
        # was created but still waiting to start (i.e. in FREE state). If there is a build
        # waiting, include that minute in the elapsed wait time.

        for ts in range(int(min_create_ts), int(max_completion_ts), 60):
            # See if any of the tasks were created but not started during this minute
            for info in watch_task_info.values():
                create_ts = int(info['create_ts'])
                start_ts = int(info['start_ts'])
                # Was the build waiting to start during this minute?
                if create_ts <= ts <= start_ts:
                    # Increment and exit; we don't want to count overlapping wait periods
                    # since it would not accurately reflect the overall time savings we could
                    # expect with more capacity.
                    elapsed_wait_minutes += 1
                    break

        runtime.logger.info("Approximate elapsed time (wasted) waiting: {}m".format(elapsed_wait_minutes))
        elapsed_total_minutes = (max_completion_ts - min_create_ts) / 60.0
        runtime.logger.info(
            "Elapsed time (from first submit to last completion) for all builds: {:.1f}m".format(elapsed_total_minutes)
        )

        runtime.record_logger.add_record(
            "image_build_metrics",
            elapsed_wait_minutes=int(elapsed_wait_minutes),
            elapsed_total_minutes=int(elapsed_total_minutes),
            task_count=len(watch_task_info),
        )
    else:
        runtime.logger.info('Unable to determine timestamps from collected info: {}'.format(watch_task_info))


@cli.command("images:build", short_help="Build images for the group.")
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default='',
    help="Repo type (e.g. signed, unsigned).",
)
@click.option(
    "--repo",
    default=[],
    metavar="REPO_URL",
    multiple=True,
    help="Custom repo URL to supply to brew build. If specified, defaults from --repo-type will be ignored.",
)
@click.option(
    '--push-to-defaults', default=False, is_flag=True, help='Push to default registries when build completes.'
)
@click.option(
    "--push-to",
    default=[],
    metavar="REGISTRY",
    multiple=True,
    help="Specific registries to push to when image build completes.  [multiple]",
)
@click.option('--scratch', default=False, is_flag=True, help='Perform a scratch build.')
@click.option(
    "--threads",
    default=1,
    metavar="NUM_THREADS",
    help="Number of concurrent builds to execute. Only valid for --local builds.",
)
@click.option(
    "--filter-by-os", default=None, metavar="ARCH", help="Specify an exact arch to push (golang name e.g. 'amd64')."
)
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@click.option('--build-retries', type=int, default=1, help='Number of build attempts for an osbs build')
@click.option('--comment-on-pr', default=False, is_flag=True, help='Comment on PR after a build, if flag is enabled')
@pass_runtime
def images_build_image(
    runtime,
    repo_type,
    repo,
    push_to_defaults,
    push_to,
    scratch,
    threads,
    filter_by_os,
    dry_run,
    build_retries,
    comment_on_pr,
):
    """
    Attempts to build container images for all of the distgit repositories
    in a group. If an image has already been built, it will be treated as
    a successful operation.

    If docker registries as specified, this action will push resultant
    images to those mirrors as they become available. Note that this should
    be more performant than running images:push since pushes can
    be performed in parallel with other images building.

    Tips on using custom --repo.
    1. Upload a .repo file (it must end in .repo) with your desired yum repos enabled
       into an internal location OSBS can reach like gerrit.
    2. Specify the raw URL to this file for the build.
    3. You will probably want to use --scratch since it is unlikely you want your
        custom build tagged.
    """
    # Initialize all distgit directories before trying to build. This is to
    # ensure all build locks are acquired before the builds start and for
    # clarity in the logs.
    runtime.initialize(clone_distgits=True)

    if runtime.assembly not in ['stream', 'test'] and comment_on_pr:
        runtime.logger.warning("Commenting on PRs is only supported for stream/test assemblies")
        comment_on_pr = False

    runtime.assert_mutation_is_permitted()

    cmd = runtime.command

    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    pre_step = None
    if 'images:rebase' in runtime.state:
        pre_step = runtime.state['images:rebase']

    required = []
    failed = []

    if pre_step:
        for img, status in pre_step['images'].items():
            if status is not True:  # anything other than true is fail
                img_obj = runtime.image_map.get(img, Model())
                failed.append(img)
                if img_obj.required:
                    required.append(img)

    items = [m.distgit_repo() for m in runtime.ordered_image_metas()]

    lstate['total'] = len(items)

    if not items:
        runtime.logger.info("No images found. Check the arguments.")
        exit(1)

    if required:
        msg = 'The following images failed during the previous step and are required:\n{}'.format('\n'.join(required))
        lstate['status'] = state.STATE_FAIL
        lstate['msg'] = msg
        raise DoozerFatalError(msg)
    elif failed:
        # filter out failed images and their children
        failed = runtime.filter_failed_image_trees(failed)
        yellow_print(
            'The following images failed the last step (or are children of failed images) and will be skipped:\n{}'.format(
                '\n'.join(failed)
            )
        )
        # reload after fail filtered
        items = [m.distgit_repo() for m in runtime.ordered_image_metas()]

        if not items:
            runtime.logger.info("No images left to build after failures and children filtered out.")
            exit(1)

    if not runtime.local:
        threads = None
        with runtime.shared_koji_client_session() as koji_api:
            if not koji_api.logged_in:
                koji_api.gssapi_login()

    # load active build profile
    profiles = runtime.group_config.build_profiles
    active_profile = {}
    profile_name = runtime.profile or runtime.group_config.default_image_build_profile
    if profile_name:
        active_profile = profiles.primitive()["image"][profile_name]
    # provide build profile defaults
    active_profile.setdefault("targets", [])
    active_profile.setdefault("repo_type", "unsigned")
    active_profile.setdefault("repo_list", [])
    active_profile.setdefault("signing_intent", "unsigned")

    if repo_type:  # backward compatible with --repo-type option
        active_profile["repo_type"] = repo_type
        active_profile["signing_intent"] = "release" if repo_type == "signed" else repo_type
    if repo:
        active_profile["repo_list"] = list(repo)
    results = exectools.parallel_exec(
        lambda dgr, terminate_event: dgr.build_container(
            active_profile,
            push_to_defaults,
            additional_registries=push_to,
            retries=build_retries,
            terminate_event=terminate_event,
            scratch=scratch,
            realtime=(threads == 1),
            dry_run=dry_run,
            registry_config_dir=runtime.registry_config_dir,
            filter_by_os=filter_by_os,
            comment_on_pr=comment_on_pr,
        ),
        items,
        n_threads=threads,
    )
    results = results.get()

    if not runtime.local:  # not needed for local builds
        try:
            print_build_metrics(runtime)
        except:
            # Never kill a build because of bad logic in metrics
            traceback.print_exc()
            runtime.logger.error("Error trying to show build metrics")

    failed = [name for name, r in results if not r]
    if failed:
        runtime.logger.error("\n".join(["Build/push failures:"] + sorted(failed)))
        if len(runtime.missing_pkgs):
            runtime.logger.error("Missing packages: \n{}".format("\n".join(runtime.missing_pkgs)))
        exit(1)

    # Push all late images
    for image in runtime.image_metas():
        image.distgit_repo().push_image(
            [],
            push_to_defaults,
            additional_registries=push_to,
            push_late=True,
            registry_config_dir=runtime.registry_config_dir,
            filter_by_os=filter_by_os,
        )

    state.record_image_finish(lstate)


@cli.command("images:push", short_help="Push the most recently built images to mirrors.")
@click.option(
    '--tag',
    default=[],
    metavar="PUSH_TAG",
    multiple=True,
    help='Push to registry using these tags instead of default set.',
)
@click.option(
    "--version-release",
    default=None,
    metavar="VERSION-RELEASE",
    help="Specify an exact version to pull/push (e.g. 'v3.9.31-1' ; default is latest built).",
)
@click.option('--to-defaults', default=False, is_flag=True, help='Push to default registries.')
@click.option('--late-only', default=False, is_flag=True, help='Push only "late" images.')
@click.option(
    "--to",
    default=[],
    metavar="REGISTRY",
    multiple=True,
    help="Registry to push to when image build completes.  [multiple]",
)
@click.option(
    "--filter-by-os", default=None, metavar="ARCH", help="Specify an exact arch to push (golang name e.g. 'amd64')."
)
@click.option(
    '--dry-run', default=False, is_flag=True, help='Only print tag/push operations which would have occurred.'
)
@pass_runtime
def images_push(runtime, tag, version_release, to_defaults, late_only, to, filter_by_os, dry_run):
    """
    Each distgit repository will be cloned and the version and release information
    will be extracted. That information will be used to determine the most recently
    built image associated with the distgit repository.

    An attempt will be made to pull that image and push it to one or more
    docker registries specified on the command line.
    """

    additional_registries = list(to)  # In case we get a tuple

    if to_defaults is False and len(additional_registries) == 0:
        click.echo("You need specify at least one destination registry.")
        exit(1)

    runtime.initialize()

    # This might introduce unwanted mutation, so prevent if automation is frozen
    runtime.assert_mutation_is_permitted()

    cmd = runtime.command
    runtime.state[cmd] = dict(state.TEMPLATE_IMAGE)
    lstate = runtime.state[cmd]  # get local convenience copy

    pre_step = runtime.state.get('images:build', None)

    required = []
    pre_failed = []

    if pre_step:
        for img, status in pre_step['images'].items():
            if status is not True:  # anything other than true is fail
                img_obj = runtime.image_map[img]
                pre_failed.append(img)
                if img_obj.required:
                    required.append(img)

    version_release_tuple = None

    if version_release:
        version_release_tuple = version_release.split('-')
        click.echo(
            'Setting up to push: version={} release={}'.format(version_release_tuple[0], version_release_tuple[1])
        )

    items = runtime.image_metas()

    if required:
        msg = 'The following images failed during the previous step and are required:\n{}'.format('\n'.join(required))
        lstate['status'] = state.STATE_FAIL
        lstate['msg'] = msg
        raise DoozerFatalError(msg)
    elif pre_failed:
        # filter out failed images and their children
        failed = runtime.filter_failed_image_trees(pre_failed)
        yellow_print(
            'The following images failed the last step (or are children of failed images) and will be skipped:\n{}'.format(
                '\n'.join(failed)
            )
        )
        # reload after fail filtered
        items = runtime.ordered_image_metas()

        if not items:
            runtime.logger.info("No images left to build after failures and children filtered out.")
            exit(1)

    # late-only is useful if we are resuming a partial build in which not all images
    # can be built/pushed. Calling images:push can end up hitting the same
    # push error, so, without late-only, there is no way to push "late" images and
    # deliver the partial build's last images.
    if not late_only:
        # Allow all non-late push operations to be attempted and track failures
        # with this list. Since "late" images are used as a marker for success,
        # don't push them if there are any preceding errors.
        # This error tolerance is useful primarily in synching images that our
        # team does not build but which should be kept up to date in the
        # operations registry.
        failed = []
        # Push early images

        results = exectools.parallel_exec(
            lambda img, terminate_event: img.distgit_repo().push_image(
                tag,
                to_defaults,
                additional_registries,
                version_release_tuple=version_release_tuple,
                dry_run=dry_run,
                registry_config_dir=runtime.registry_config_dir,
                filter_by_os=filter_by_os,
            ),
            items,
            n_threads=4,
        )
        results = results.get()

        failed = [name for name, r in results if not r]
        if failed:
            runtime.logger.error("\n".join(["Push failures:"] + sorted(failed)))
            exit(1)

    # Push all late images
    for image in items:
        # Check if actually a late image to prevent cloning all distgit on --late-only
        if image.config.push.late is True:
            image.distgit_repo().push_image(
                tag,
                to_defaults,
                additional_registries,
                version_release_tuple=version_release_tuple,
                push_late=True,
                dry_run=dry_run,
                registry_config_dir=runtime.registry_config_dir,
                filter_by_os=filter_by_os,
            )

    state.record_image_finish(lstate)


@cli.command("images:pull", short_help="Pull latest images from pulp")
@pass_runtime
def images_pull_image(runtime):
    """
    Pulls latest images from pull, fetching the dockerfiles from cgit to
    determine the version/release.
    """
    runtime.initialize(clone_distgits=True)
    for image in runtime.image_metas():
        image.pull_image()


@cli.command("images:show-tree", short_help="Display the image relationship tree")
@click.option("--imagename", default=False, is_flag=True, help="Use the image name instead of the dist-git name")
@click.option("--yml", default=False, is_flag=True, help="Ouput to yaml formatted text, otherwise generate a tree view")
@pass_runtime
def images_show_tree(runtime, imagename, yml):
    """
    Displays the parent/child relationship of all images or just those given.
    This can be helpful to determine build order and dependencies.
    """
    runtime.initialize(clone_distgits=False, prevent_cloning=True)

    images = list(runtime.image_metas())

    print_colors = ['green', 'cyan', 'blue', 'yellow', 'magenta']

    def name(image):
        return image.image_name if imagename else image.distgit_key

    if yml:
        color_print(yaml.safe_dump(runtime.image_tree, indent=2, default_flow_style=False), 'cyan')
    else:

        def print_branch(image, indent=0):
            num_child = len(image.children)
            for i in range(num_child):
                child = image.children[i]
                if i == (num_child - 1):
                    tree_char = '└─'
                else:
                    tree_char = '├─'
                tree_char += '┐' if len(child.children) else ' '

                line = '{} {} {}'.format(('  ' * indent), tree_char, name(child))
                color_print(line, print_colors[(indent + 1) % len(print_colors)])
                print_branch(child, indent + 1)

        for image in images:
            if not image.parent:
                line = name(image)
                color_print(line, print_colors[0])
                print_branch(image)


@cli.command("images:show-ancestors", short_help="Display the image ancestors")
@click.option(
    "--image-names",
    required=True,
    help="A comma-separated list of component names of the images (file name in ocp-build-data).",
)
@pass_runtime
def images_show_ancestors(runtime, image_names):
    """
    Displays the ancestors of a given image or images.
    """
    runtime.initialize(clone_distgits=False, prevent_cloning=True)

    all_ancestors = set()
    image_name_list = [name.strip() for name in image_names.split(',')]

    # Build a map of stream name -> image metadata
    stream_provided_by = {}
    for image_meta in runtime.image_metas():
        if image_meta.config.provides.stream is not Missing:
            stream_provided_by[image_meta.config.provides.stream] = image_meta

    # Build a map of image name (from config.name) -> image metadata
    image_name_map = {meta.image_name: meta for meta in runtime.image_metas()}

    images_to_process = []
    for image_name in image_name_list:
        image = runtime.image_map.get(image_name)
        if not image:
            raise DoozerFatalError(f"Image '{image_name}' not found in group.")
        images_to_process.append(image)

    visited = set()
    while images_to_process:
        current_image = images_to_process.pop(0)

        if current_image.distgit_key in visited:
            continue
        visited.add(current_image.distgit_key)

        # 1. Get parent from `from.member` or `from.stream` (this is what `image.parent` is set to during init)
        parent = current_image.parent
        if parent:
            all_ancestors.add(parent.distgit_key)
            images_to_process.append(parent)

        # 2. Get parent from `from.image`
        if current_image.config['from'].image is not Missing:
            parent_image_name = current_image.config['from'].image
            parent_meta = image_name_map.get(parent_image_name)
            if parent_meta:
                all_ancestors.add(parent_meta.distgit_key)
                images_to_process.append(parent_meta)

        # 3. Get parents from `from.builder`
        if current_image.config['from'].builder is not Missing:
            for builder in current_image.config['from'].builder:
                builder_meta = None
                if builder.member:
                    builder_meta = runtime.image_map.get(builder.member)
                elif builder.stream:
                    builder_meta = stream_provided_by.get(builder.stream)
                elif builder.image:
                    builder_image_name = builder.image
                    builder_meta = image_name_map.get(builder_image_name)

                if builder_meta:
                    all_ancestors.add(builder_meta.distgit_key)
                    images_to_process.append(builder_meta)

    if all_ancestors:
        click.echo(",".join(sorted(list(all_ancestors))))


@cli.command("images:print", short_help="Print data for each image metadata")
@click.option("--short", default=False, is_flag=True, help="Suppress all output other than the data itself")
@click.option(
    '--show-non-release', default=False, is_flag=True, help='Include images which have been marked as non-release.'
)
@click.option('--show-base', default=False, is_flag=True, help='Include images which have been marked as base images.')
@click.option(
    '--only-for-payload', default=False, is_flag=True, help='Filter out images that do not have "for_payload: true".'
)
@click.option("--output", "-o", default=None, help="Write data to FILE instead of STDOUT")
@click.option("--label", "-l", default=None, help="The label you want to print if it exists. Empty string if n/a")
@click.argument("pattern", default="{build}", nargs=1)
@pass_runtime
def images_print(runtime, short, show_non_release, only_for_payload, show_base, output, label, pattern):
    """
    Prints data from each distgit. The pattern specified should be a string
    with replacement fields:

    \b
    {type} - The type of the distgit (e.g. rpms)
    {name} - The name of the distgit repository (e.g. openshift-enterprise)
    {image_name} - The container registry image name (e.g. openshift3/ose-ansible)
    {image_name_short} - The container image name without the registry (e.g. ose-ansible)
    {component} - The component identified in the Dockerfile
    {image} - The image name according to distgit Dockerfile (image_name / image_name_short are faster)
    {version} - The version of the latest brew build
    {release} - The release of the latest brew build
    {build} - Shorthand for {component}-{version}-{release} (e.g. container-engine-v3.6.173.0.25-1)
    {repository} - Shorthand for {image}:{version}-{release}
    {label} - The label you want to print from the Dockerfile (Empty string if n/a)
    {jira_info} - All known BZ component information for the component
    {jira_project} - The associated Jira project for bugs, if known
    {jira_component} - The associated Jira project component, if known
    {upstream} - The upstream repository for the image
    {upstream_public} - The public upstream repository (if different) for the image
    {owners} - comma delimited list of owners
    {lf} - Line feed

    If pattern contains no braces, it will be wrapped with them automatically. For example:
    "build" will be treated as "{build}"
    """

    runtime.initialize(clone_distgits=False, prevent_cloning=True)

    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    count = 0
    if short:

        def echo_verbose(_):
            return None
    else:
        echo_verbose = click.echo

    if output is None:
        echo_verbose("")
        echo_verbose("------------------------------------------")
    else:
        green_print("Writing image list to {}".format(output))

    non_release_images = []
    images = []
    if show_non_release:
        images = list(runtime.image_metas())
    else:
        for i in runtime.image_metas():
            if i.for_release:
                images.append(i)
            else:
                non_release_images.append(i.distgit_key)

    for image in images:
        # skip base images unless requested
        if image.base_only and not show_base:
            continue
        # skip disabled images unless requested
        if not (image.enabled or runtime.load_disabled):
            continue

        if only_for_payload and image.config.for_payload is not True:
            continue

        dfp = None

        # Method to lazily load the remote dockerfile content.
        # Avoiding it when the content is not necessary speeds up most print operations.
        def get_dfp():
            nonlocal dfp
            if dfp:
                return dfp
            dfp = DockerfileParser(path=runtime.working_dir)
            try:
                dfp.content = image.fetch_cgit_file("Dockerfile")
                return dfp
            except Exception:
                raise DoozerFatalError("Error reading Dockerfile from distgit: {}".format(image.distgit_key))

        s = pattern
        s = s.replace("{build}", "{component}-{version}-{release}")
        s = s.replace("{repository}", "{image}:{version}-{release}")
        s = s.replace("{namespace}", image.namespace)
        s = s.replace("{name}", image.name)
        s = s.replace("{image_name}", image.image_name)
        s = s.replace("{image_name_short}", image.image_name_short)
        s = s.replace("{component}", image.get_component_name())

        source_url = image.config.content.source.git.url
        s = s.replace("{upstream}", source_url or 'None')
        if source_url:
            public_url = SourceResolver.get_public_upstream(source_url, runtime.group_config.public_upstreams)[0]
        else:
            public_url = None
        s = s.replace("{upstream_public}", public_url or 'None')

        if '{jira_' in s:
            jira_project, jira_component = image.get_jira_info()
            s = s.replace('{jira_info}', 'jira[project={jira_project} component={jira_component}]')
            s = s.replace('{jira_project}', jira_project)
            s = s.replace('{jira_component}', jira_component)

        if '{owners}' in s:
            owners = []
            if image.config.owners is not Missing:
                owners = image.config.owners
            s = s.replace("{owners}", 'owners[' + ' '.join(owners) + ']')

        if '{image}' in s:
            s = s.replace("{image}", get_dfp().labels["name"])

        if label is not None:
            s = s.replace("{label}", get_dfp().labels.get(label, ''))
        s = s.replace("{lf}", "\n")

        release_query_needed = '{release}' in s or '{pushes}' in s
        version_query_needed = '{version}' in s or '{pushes}' in s

        # Since querying nvr takes time, check before executing replace
        version = ''
        release = ''
        if release_query_needed or version_query_needed:
            try:
                _, version, release = image.get_latest_build_info()
            except IOError as err:
                err_msg = str(err)
                if err_msg.find("No builds detected") >= 0:
                    # ignore "No builds detected" error
                    runtime.logger.warning("No builds delected for {}: {}".format(image.name, err_msg))
                else:
                    raise err

        s = s.replace("{version}", version)
        s = s.replace("{release}", release)

        pushes_formatted = ''
        for push_name in image.get_default_push_names():
            pushes_formatted += '\t{} : [{}]\n'.format(
                push_name, ', '.join(image.get_default_push_tags(version, release))
            )

        if not pushes_formatted:
            pushes_formatted = "(None)"

        s = s.replace("{pushes}", '{}\n'.format(pushes_formatted))

        if "{" in s:
            raise IOError("Unrecognized fields remaining in pattern: %s" % s)

        if output is None:
            # Print to stdout
            click.echo(s)
        else:
            # Write to a file
            with io.open(output, 'a', encoding="utf-8") as out_file:
                out_file.write("{}\n".format(s))

        count += 1

    echo_verbose("------------------------------------------")
    echo_verbose("{} images".format(count))

    # If non-release images are being suppressed, let the user know
    if not show_non_release and non_release_images:
        echo_verbose(
            "\nThe following {} non-release images were excluded; use --show-non-release to include them:".format(
                len(non_release_images)
            )
        )
        for image in non_release_images:
            echo_verbose("    {}".format(image))


@cli.command("images:print-config-template", short_help="Create template package yaml from distgit Dockerfile.")
@click.argument("url", nargs=1)
def distgit_config_template(url):
    """
    Pulls the specified URL (to a Dockerfile in distgit) and prints the boilerplate
    for a config yaml for the image.
    """

    f = urllib.request.urlopen(url)
    if f.code != 200:
        click.echo("Error fetching {}: {}".format(url, f.code), err=True)
        exit(1)

    dfp = DockerfileParser()
    dfp.content = f.read()

    if "cgit/rpms/" in url:
        type = "rpms"
    elif "cgit/containers/" in url:
        type = "containers"
    elif "cgit/apbs/" in url:
        type = "apbs"
    else:
        raise IOError("doozer does not yet support that distgit repo type")

    config = {
        "repo": {
            "type": type,
        },
        "name": dfp.labels['name'],
        "from": {
            "image": dfp.baseimage,
        },
        "labels": {},
        "owners": [],
    }

    branch = url[url.index("?h=") + 3 :]

    if "Architecture" in dfp.labels:
        dfp.labels["architecture"] = dfp.labels["Architecture"]

    component = dfp.labels.get("com.redhat.component", dfp.labels.get("BZComponent", None))

    if component is not None:
        config["repo"]["component"] = component

    managed_labels = [
        'vendor',
        'License',
        'architecture',
        'io.k8s.display-name',
        'io.k8s.description',
        'io.openshift.tags',
    ]

    for ml in managed_labels:
        if ml in dfp.labels:
            config["labels"][ml] = dfp.labels[ml]

    click.echo("---")
    click.echo("# populated from branch: {}".format(branch))
    yaml.safe_dump(config, sys.stdout, indent=2, default_flow_style=False)


@cli.command("images:query-rpm-version", short_help="Find the OCP version from the atomic-openshift RPM")
@click.option(
    "--repo-type",
    metavar="REPO_TYPE",
    envvar="OIT_IMAGES_REPO_TYPE",
    default="unsigned",
    help="Repo group to scan for the RPM (e.g. signed, unsigned). env: OIT_IMAGES_REPO_TYPE",
)
@pass_runtime
def query_rpm_version(runtime, repo_type):
    """
    Retrieve the version number of the atomic-openshift RPM in the indicated
    repository. This is the version number that will be applied to new images
    created from this build.
    """
    runtime.initialize(clone_distgits=False, prevent_cloning=True)

    version = runtime.auto_version(repo_type)
    click.echo("version: {}".format(version))
