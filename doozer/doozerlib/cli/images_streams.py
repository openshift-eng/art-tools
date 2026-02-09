import hashlib
import io
import json
import os
import random
import re
import time
from datetime import datetime
from typing import Dict, Optional, Set, Tuple

import click
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import green_print, yellow_print
from artcommonlib.git_helper import git_clone
from artcommonlib.jira_config import get_jira_browse_url
from artcommonlib.model import Missing, Model
from artcommonlib.pushd import Dir
from artcommonlib.util import convert_remote_git_to_https, convert_remote_git_to_ssh, remove_prefix, split_git_url
from dockerfile_parse import DockerfileParser
from github import Github, GithubException, PullRequest, UnknownObjectException
from jira import JIRA, Issue
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import constants, util
from doozerlib.cli import cli, option_registry_auth, pass_runtime
from doozerlib.image import ImageMetadata
from doozerlib.source_resolver import SourceResolver
from doozerlib.util import (
    extract_version_fields,
    get_docker_config_json,
    oc_image_info,
    oc_image_info_for_arch,
    what_is_in_master,
)
from pyartcd import jenkins

transform_rhel_7_base_repos = 'rhel-7/base-repos'
transform_rhel_8_base_repos = 'rhel-8/base-repos'
transform_rhel_9_base_repos = 'rhel-9/base-repos'
transform_rhel_7_golang = 'rhel-7/golang'
transform_rhel_8_golang = 'rhel-8/golang'
transform_rhel_9_golang = 'rhel-9/golang'
transform_rhel_7_ci_build_root = 'rhel-7/ci-build-root'
transform_rhel_8_ci_build_root = 'rhel-8/ci-build-root'
transform_rhel_9_ci_build_root = 'rhel-9/ci-build-root'

# The set of valid transforms
transforms = set(
    [
        transform_rhel_7_base_repos,
        transform_rhel_8_base_repos,
        transform_rhel_9_base_repos,
        transform_rhel_7_golang,
        transform_rhel_8_golang,
        transform_rhel_9_golang,
        transform_rhel_7_ci_build_root,
        transform_rhel_8_ci_build_root,
        transform_rhel_9_ci_build_root,
    ]
)


@cli.group("images:streams", short_help="Manage ART equivalent images in upstream CI.")
def images_streams():
    """
    When changing streams.yml, the following sequence of operations is required.

    \b
    1. Set KUBECONFIG to the art-publish service account.
    2. Run gen-buildconfigs with --apply or 'oc apply' it after the fact.
    3. Run mirror verb to push the images to the api.ci cluster (consider if --only-if-missing is appropriate).
    4. Run start-builds to trigger buildconfigs if they have not run already.
    5. Run 'check-upstream' after about 10 minutes to make sure those builds have succeeded. Investigate any failures.
    6. Run 'prs open' when check-upstream indicates all builds are complete.

    To test changes before affecting CI:

    \b
    - You can run, 'gen-buildconfigs', 'start-builds', and 'check-upstream' with the --live-test-mode flag.
      This will create and run real buildconfigs on api.ci, but they will not promote into locations used by CI.
    - You can run 'prs open' with --moist-run. This will create forks for the target repos, but will only
      print out PRs that would be opened instead of creating them.

    """
    pass


@images_streams.command('mirror', short_help='Reads streams.yml and mirrors out ART equivalent images to app.ci.')
@click.option(
    '--stream',
    'streams',
    metavar='STREAM_NAME',
    default=[],
    multiple=True,
    help='If specified, only these stream names will be mirrored.',
)
@click.option(
    '--only-if-missing',
    default=False,
    is_flag=True,
    help='Only mirror the image if there is presently no equivalent image upstream.',
)
@click.option(
    '--live-test-mode',
    default=False,
    is_flag=True,
    help='Append "test" to destination images to exercise live-test-mode buildconfigs',
)
@click.option(
    '--force', default=False, is_flag=True, help='Force the mirror operation even if not defined in config upstream.'
)
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@option_registry_auth
@pass_runtime
def images_streams_mirror(
    runtime,
    streams: Tuple[str, ...],
    only_if_missing: bool,
    live_test_mode: bool,
    force: bool,
    dry_run: bool,
    registry_auth: Optional[str],
):
    runtime.initialize(clone_distgits=False, clone_source=False)
    runtime.assert_mutation_is_permitted()

    # Determine which registry config to use:
    # 1. If --registry-auth is provided, use it directly (it's already a file path)
    # 2. Otherwise, fall back to --registry-config-dir (global option, needs conversion to file path)
    registry_config_file = None
    if registry_auth:
        registry_config_file = registry_auth
    elif runtime.registry_config_dir is not None:
        registry_config_file = get_docker_config_json(runtime.registry_config_dir)

    def mirror_image(cmd_start: str, upstream_dest: str):
        full_cmd_1 = f'{cmd_start} {upstream_dest}'
        if dry_run:
            print(f'For {upstream_entry_name}, would have run: {full_cmd_1}')
        else:
            exectools.cmd_assert(full_cmd_1, retries=3, realtime=True)

        if upstream_dest.startswith('registry.ci.openshift.org/'):
            # If the image is being mirrored the CI imagestreams, we must also mirror it
            # to the quay.io/openshift/ci (aka QCI) repository. QCI is the location from which
            # imagestream references will be resolved for CI operations. Eventually, the
            # internal registry on the app.ci cluster will not be used at all.
            # upstream_dest might look something like: registry.ci.openshift.org/ocp/{MAJOR}.{MINOR}:base-rhel9
            # We want to transform this into: quay.io/openshift/ci:<datetime>_prune_art__ocp_{MAJOR}.{MINOR}:base-rhel9
            # This tag convention allows images to be pruned over time if they are no longer being
            # used. See https://github.com/openshift/release/blob/244558fe310225fa8d9895c0c70a279cc104c612/hack/qci_registry_pruner.py#L3-L25
            # We also mirror to a floating tag that will be overwritten as new images are
            # mirrored: quay.io/openshift/ci:art__ocp_{MAJOR}.{MINOR}:base-rhel9 . This ensures that
            # at least one copy of the image will stay around until a new version is mirrored
            # to the same tag, regardless of whether the prune tag is removed.

            _, org_repo_tag = upstream_dest.split('/', 1)  # isolate "ocp/{MAJOR}.{MINOR}:base-rhel9"

            # Use re.sub() to replace any character in the set [:/] with '_'
            org_repo_tag = re.sub(r"[:/]", "_", org_repo_tag)  # => ocp_{MAJOR}.{MINOR}_base-rhel9
            prune_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            prunable_qci_upstream_dest = f"quay.io/openshift/ci:{prune_timestamp}_prune_art__{org_repo_tag}"
            full_cmd_2 = f'{cmd_start} {prunable_qci_upstream_dest}'
            if dry_run:
                print(f'For {upstream_entry_name}, would have run: {full_cmd_2}')
            else:
                exectools.cmd_assert(full_cmd_2, retries=3, realtime=True)

            floating_qci_upstream_dest = f"quay.io/openshift/ci:art__{org_repo_tag}"
            full_cmd_3 = f'{cmd_start} {floating_qci_upstream_dest}'
            if dry_run:
                print(f'For {upstream_entry_name}, would have run: {full_cmd_3}')
            else:
                exectools.cmd_assert(full_cmd_3, retries=3, realtime=True)

    upstreaming_entries = _get_upstreaming_entries(runtime, streams)

    for upstream_entry_name, config in upstreaming_entries.items():
        if config.mirror is True or force:
            upstream_dest = config.upstream_image
            mirror_arm = False
            if upstream_dest is Missing:
                raise IOError(f'Unable to mirror {upstream_entry_name} since upstream_image is not defined')

            # If the configuration specifies an upstream_image_base, then ART is responsible for mirroring
            # that location and NOT the upstream_image. A buildconfig from gen-buildconfig is responsible
            # for transforming upstream_image_base to upstream_image.
            if config.upstream_image_base is not Missing:
                upstream_dest = config.upstream_image_base
                if "aarch64" in runtime.arches:
                    mirror_arm = True

            src_image = config.image
            if len(src_image.split('/')) > 2:
                # appears to be a pullspec already; like: quay.io/centos/centos:stream8
                src_image_pullspec = src_image
            else:
                # appears to be short image name like: openshift/golang-builder:v1.19.13-202404160932.g3172d57.el9
                src_image_pullspec = runtime.resolve_brew_image_url(src_image)

            # Check all destinations: maps destination -> exists boolean
            destinations_to_check = {}

            if only_if_missing:
                # Check main destination (AMD64)
                amd64_info = oc_image_info_for_arch(
                    upstream_dest, go_arch='amd64', registry_config=registry_config_file, strict=False
                )
                destinations_to_check[upstream_dest] = amd64_info is not None

                # Check ARM64 if needed
                if mirror_arm:
                    arm64_info = oc_image_info(
                        f'{upstream_dest}-arm64', registry_config=registry_config_file, strict=False
                    )
                    destinations_to_check[f'{upstream_dest}-arm64'] = arm64_info is not None

                # Check upstream_image_mirror destinations
                if config.upstream_image_mirror is not Missing:
                    for mirror_dest in config.upstream_image_mirror:
                        mirror_info = oc_image_info(mirror_dest, registry_config=registry_config_file, strict=False)
                        destinations_to_check[mirror_dest] = mirror_info is not None

                # Check if ALL destinations exist
                all_exist = all(destinations_to_check.values())

                if all_exist:
                    runtime.logger.info(
                        f'All destinations for {upstream_entry_name} exist; skipping because of --only-if-missing'
                    )
                    continue

                else:
                    runtime.logger.info(
                        f'Some destinations for {upstream_entry_name} are missing; will proceed with mirroring:'
                    )
                    for dest, exists in destinations_to_check.items():
                        status = '✓ exists' if exists else '✗ missing'
                        runtime.logger.info(f'  - {dest} ({status})')

            if live_test_mode:
                upstream_dest += '.test'

            as_manifest_list = ''
            if config.mirror_manifest_list is True:
                as_manifest_list = '--keep-manifest-list'

            cmd = f'oc image mirror {as_manifest_list} {src_image_pullspec}'

            if registry_config_file is not None:
                cmd += f" --registry-config={registry_config_file}"

            # Mirror to main destination only if not in only-if-missing mode OR destination doesn't exist
            if not only_if_missing or not destinations_to_check.get(upstream_dest, False):
                mirror_image(cmd, upstream_dest)

            # mirror arm64 builder and base images for CI
            if mirror_arm:
                # oc image mirror will filter out missing arches (as long as the manifest is there) regardless of specifying --skip-missing
                arm_cmd = f'oc image mirror --filter-by-os linux/arm64 {src_image_pullspec}'
                if registry_config_file is not None:
                    arm_cmd += f" --registry-config={registry_config_file}"
                # Mirror ARM64 only if not in only-if-missing mode OR destination doesn't exist
                if not only_if_missing or not destinations_to_check.get(f'{upstream_dest}-arm64', False):
                    mirror_image(arm_cmd, f'{upstream_dest}-arm64')

            # If upstream_image_mirror is set, mirror the upstream_image (which we just updated above)
            # to the locations specified. Note that this is NOT upstream_image_base.
            # This should be the fully transformed result. Usually this is used
            # to mirror public builder images to the private image builders.
            # Now that we've mirrored the source to upstream_image, we mirror
            # upstream_image to all the upstream_image_mirror destinations so they all get the same version.
            if config.upstream_image_mirror is not Missing:
                for upstream_image_mirror_dest in config.upstream_image_mirror:
                    # Mirror to each destination only if not in only-if-missing mode OR destination doesn't exist
                    if not only_if_missing or not destinations_to_check.get(upstream_image_mirror_dest, False):
                        priv_cmd = f'oc image mirror {config.upstream_image}'
                        if registry_config_file is not None:
                            priv_cmd += f" --registry-config={registry_config_file}"
                        mirror_image(priv_cmd, upstream_image_mirror_dest)


@images_streams.command(
    'check-upstream', short_help='Dumps information about CI buildconfigs/mirrored images associated with this group.'
)
@click.option(
    '--stream',
    'streams',
    metavar='STREAM_NAME',
    default=[],
    multiple=True,
    help='If specified, only check these streams',
)
@click.option('--live-test-mode', default=False, is_flag=True, help='Scan for live-test mode buildconfigs')
@pass_runtime
def images_streams_check_upstream(runtime, streams, live_test_mode):
    runtime.initialize(clone_distgits=False, clone_source=False)

    istags_status = []
    if not streams:
        streams = runtime.get_stream_names()
    upstreaming_entries = _get_upstreaming_entries(runtime, streams)

    for upstream_entry_name, config in upstreaming_entries.items():
        upstream_dest = config.upstream_image
        _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)

        if live_test_mode:
            dest_istag += '.test'

        rc, stdout, stderr = exectools.cmd_gather(f'oc get -n {dest_ns} istag {dest_istag} --no-headers')
        if rc:
            istags_status.append(
                f'ERROR: {upstream_entry_name}\nIs not yet represented upstream in {dest_ns} istag/{dest_istag}'
            )
        else:
            istags_status.append(
                f'OK: {upstream_entry_name} exists, but check whether it is recently updated\n{stdout}'
            )

    group_label = runtime.group_config.name
    if live_test_mode:
        group_label += '.test'

    bc_stdout, bc_stderr = exectools.cmd_assert(f'oc -n ci get -o=wide buildconfigs -l art-builder-group={group_label}')
    builds_stdout, builds_stderr = exectools.cmd_assert(
        f'oc -n ci get -o=wide builds -l art-builder-group={group_label}'
    )
    print('Build configs:')
    print(bc_stdout or bc_stderr)
    print('Recent builds:')
    print(builds_stdout or builds_stderr)

    print('Upstream imagestream tag status')
    for istag_status in istags_status:
        print(istag_status)
        print()


def get_eligible_buildconfigs(runtime, streams, live_test_mode):
    upstreaming_entries = _get_upstreaming_entries(runtime, streams)
    buildconfig_names = []
    for upstream_entry_name, config in upstreaming_entries.items():
        transform = config.transform
        if transform is Missing or transform not in transforms:
            continue

        upstream_dest = config.upstream_image
        upstream_intermediate_image = config.upstream_image_base
        if upstream_dest is Missing or upstream_intermediate_image is Missing:
            continue

        _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)
        if live_test_mode:
            dest_istag += '.test'
        dest_imagestream, dest_tag = dest_istag.split(':')
        name = f'{dest_imagestream}-{dest_tag}--art-builder'
        buildconfig_names.append(name)
    return buildconfig_names


@images_streams.command('start-builds', short_help='Triggers a build for each buildconfig associated with this group.')
@click.option(
    '--stream',
    'streams',
    metavar='STREAM_NAME',
    default=[],
    multiple=True,
    help='If specified, only these stream names will be processed.',
)
@click.option(
    '--as',
    'as_user',
    metavar='CLUSTER_USERNAME',
    required=False,
    default=None,
    help='Specify --as during oc start-build',
)
@click.option('--live-test-mode', default=False, is_flag=True, help='Act on live-test mode buildconfigs')
@click.option('--dry-run', default=False, is_flag=True, help='Do not build anything, but only print build operations.')
@pass_runtime
def images_streams_start_buildconfigs(runtime, streams, as_user, live_test_mode, dry_run):
    runtime.initialize(clone_distgits=False, clone_source=False)

    group_label = runtime.group_config.name
    if live_test_mode:
        group_label += '.test'

    cmd = f'oc -n ci get -o=name buildconfigs -l art-builder-group={group_label}'
    if as_user:
        cmd += f' --as {as_user}'
    bc_stdout, bc_stderr = exectools.cmd_assert(cmd, retries=3)
    bc_stdout = bc_stdout.strip()

    eligible_bcs = get_eligible_buildconfigs(runtime, streams, live_test_mode)

    if bc_stdout:
        for name in bc_stdout.splitlines():
            #  buildconfig.build.openshift.io/release-rhel-8-release-golang-1.20-openshift-4.16--art-builder
            if name.split('/')[-1] not in eligible_bcs:
                print(f'Skipping outdated buildconfig: {name}')
                continue
            print(f'Triggering: {name}')
            cmd = f'oc -n ci start-build {name}'
            if as_user:
                cmd += f' --as {as_user}'
            if dry_run:
                print(f'Would have run: {cmd}')
                continue
            stdout, stderr = exectools.cmd_assert(cmd, retries=3)
            print('   ' + stdout or stderr)
    else:
        print(f'No buildconfigs associated with this group: {group_label}')


def _get_upstreaming_entries(runtime, stream_names=None):
    """
    Looks through streams.yml entries and each image metadata for upstream
    transform information.
    :param runtime: doozer runtime
    :param stream_names: A list of streams to look for in streams.yml. If None or empty, all will be searched.
    :return: Returns a map[name] => { transform: '...', upstream_image.... } where name is a stream name or distgit key.
    """

    fetch_image_metas = False
    if not stream_names:
        # If not specified, use all.
        stream_names = runtime.get_stream_names()
        fetch_image_metas = True

    upstreaming_entries = {}
    streams_config = runtime.streams
    for stream in stream_names:
        config = streams_config[stream]
        if config is Missing:
            raise IOError(f'Did not find stream {stream} in streams.yml for this group')
        if config.upstream_image is not Missing:
            upstreaming_entries[stream] = streams_config[stream]

    if fetch_image_metas:
        # Some images also have their own upstream information. This allows them to
        # be mirrored out into upstream, optionally transformed, and made available as builder images for
        # other images without being in streams.yml.

        for image_meta in runtime.ordered_image_metas():
            if image_meta.config.content.source.ci_alignment.upstream_image is not Missing:
                upstream_entry = Model(
                    dict_to_model=image_meta.config.content.source.ci_alignment.primitive()
                )  # Make a copy

                # Use pull_url() which handles both Brew and Konflux build systems
                upstream_entry['image'] = image_meta.pull_url()

                if upstream_entry.final_user is Missing:
                    upstream_entry.final_user = image_meta.config.final_stage_user
                upstreaming_entries[image_meta.distgit_key] = upstream_entry

    return upstreaming_entries


@images_streams.command(
    'gen-buildconfigs', short_help='Generates buildconfigs necessary to assemble ART equivalent images upstream.'
)
@click.option(
    '--stream',
    'streams',
    metavar='STREAM_NAME',
    default=[],
    multiple=True,
    help='If specified, only these stream names will be processed.',
)
@click.option(
    '-o',
    '--output',
    metavar='FILENAME',
    required=True,
    help='The filename into which to write the YAML. It should be oc applied against api.ci as art-publish. The file may be empty if there are no buildconfigs.',
)
@click.option(
    '--as', 'as_user', metavar='CLUSTER_USERNAME', required=False, default=None, help='Specify --as during oc apply'
)
@click.option('--apply', default=False, is_flag=True, help='Apply the output if any buildconfigs are generated')
@click.option('--live-test-mode', default=False, is_flag=True, help='Generate live-test mode buildconfigs')
@pass_runtime
def images_streams_gen_buildconfigs(runtime, streams, output, as_user, apply, live_test_mode):
    """
    ART has a mandate to make "ART equivalent" images available usptream for CI workloads. This enables
    CI to compile with the same golang version ART is using and use identical UBI8 images, etc. To accomplish
    this, streams.yml contains metadata which is extraneous to the product build, but critical to enable
    a high fidelity CI signal.
    It may seem at first that all we would need to do was mirror the internal build images (Brew or Konflux)
    somewhere accessible by CI, but it is not that simple:
    1. When a CI build yum installs, it needs to pull RPMs from an RPM mirroring service that runs in
       CI. That mirroring service subsequently pulls and caches files ART syncs using reposync.
    2. There is a variation of simple golang builders that CI uses to compile test cases. These
       images are configured in ci-operator config's 'build_root' and they are used to build
       and run test cases. Sometimes called 'CI release' image, these images contain tools that
       are not part of the typical golang builder (e.g. tito).
    Both of these differences require us to 'transform' the ART build images into images compatible
    for use in CI. A challenge to this transformation is that they must be performed in the CI context
    as they depend on the services only available in ci (e.g. base-4-6-rhel8.ocp.svc is used to
    find the current yum repo configuration which should be used).
    To accomplish that, we don't build the images ourselves. We establish OpenShift buildconfigs on the CI
    cluster which process intermediate images we push into the final, CI consumable image.
    These buildconfigs are generated dynamically by this sub-verb.
    The verb will also create a daemonset for the group on the CI cluster. This daemonset overcomes
    a bug in OpenShift 3.11 where the kubelet can garbage collection an image that the build process
    is about to use (because the kubelet & build do not communicate). The daemonset forces the kubelet
    to know the image is in use. These daemonsets can like be eliminated when CI infra moves fully to
    4.x.
    """
    runtime.initialize(clone_distgits=False, clone_source=False)
    runtime.assert_mutation_is_permitted()

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']

    # Use runtime.repos which handles both old-style and new-style repo configs
    # Convert to Model objects to maintain compatibility with existing dot notation code
    rpm_repos_conf = {name: Model(repo.to_dict()) for name, repo in runtime.repos.items()}

    group_label = runtime.group_config.name
    if live_test_mode:
        group_label += '.test'

    buildconfig_definitions = []

    upstreaming_entries = _get_upstreaming_entries(runtime, streams)

    for upstream_entry_name, config in upstreaming_entries.items():
        transform = config.transform
        if transform is Missing:
            # No buildconfig is necessary
            continue

        if transform not in transforms:
            raise IOError(
                f'Unable to render buildconfig for upstream config {upstream_entry_name} - transform {transform} not found within {transforms}'
            )

        upstream_dest = config.upstream_image
        upstream_intermediate_image = config.upstream_image_base
        if upstream_dest is Missing or upstream_intermediate_image is Missing:
            raise IOError(
                f'Unable to render buildconfig for upstream config {upstream_entry_name} - you must define upstream_image_base AND upstream_image'
            )

        # split a pullspec like registry.svc.ci.openshift.org/ocp/builder:rhel-8-golang-openshift-{MAJOR}.{MINOR}.art
        # into  OpenShift namespace, imagestream, and tag
        _, intermediate_ns, intermediate_istag = upstream_intermediate_image.rsplit('/', maxsplit=2)
        if live_test_mode:
            intermediate_istag += '.test'
        intermediate_imagestream, intermediate_tag = intermediate_istag.split(':')

        _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)
        if live_test_mode:
            dest_istag += '.test'
        dest_imagestream, dest_tag = dest_istag.split(':')

        python_file_dir = os.path.dirname(__file__)

        # should align with files like: doozerlib/cli/ci_transforms/rhel-7/base-repos
        # OR ocp-build-data branch ci_transforms/rhel-7/base-repos . The latter is given
        # priority.
        ocp_build_data_transform = os.path.join(runtime.data_dir, 'ci_transforms', transform, 'Dockerfile')
        if os.path.exists(ocp_build_data_transform):
            transform_template = os.path.join(runtime.data_dir, 'ci_transforms', transform, 'Dockerfile')
        else:
            # fall back to the doozerlib versions
            transform_template = os.path.join(python_file_dir, 'ci_transforms', transform, 'Dockerfile')

        with open(transform_template, mode='r', encoding='utf-8') as tt:
            transform_template_content = tt.read()

        dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO())
        dfp.content = transform_template_content

        # Make sure that upstream images can discern they are building in CI with ART equivalent images
        dfp.envs['OPENSHIFT_CI'] = 'true'

        dfp.labels['io.k8s.display-name'] = f'{dest_imagestream}-{dest_tag}'
        dfp.labels['io.k8s.description'] = f'ART equivalent image {group_label}-{upstream_entry_name} - {transform}'
        dfp.add_lines('USER 0')  # Make sure we are root so that repos can be modified

        def add_localdev_repo_profile(profile):
            """
            The images we push to CI are used in two contexts:
            1. In CI builds, running on the CI clusters.
            2. In local development (e.g. docker build).
            This method is enabling the latter. If a developer is connected to the VPN,
            they will not be able to resolve RPMs through the RPM mirroring service running
            on CI, but they will be able to pull RPMs directly from the sources ART does.
            Since skip_if_unavailable is True globally, it doesn't matter if they can't be
            accessed via CI.
            """
            for repo_name in rpm_repos_conf.keys():
                repo_desc = rpm_repos_conf[repo_name]
                localdev_repo_name = f'localdev-{repo_name}'
                repo_conf = repo_desc.conf
                ci_alignment = repo_conf.ci_alignment
                if ci_alignment.localdev.enabled and profile in ci_alignment.profiles:
                    # CI only really deals with x86_64 at this time.
                    if repo_conf.baseurl.unsigned:
                        x86_64_url = repo_conf.baseurl.unsigned.x86_64
                    else:
                        x86_64_url = repo_conf.baseurl.x86_64
                    if not x86_64_url:
                        raise IOError(f'Expected x86_64 baseurl for repo {repo_name}')
                    dfp.add_lines(
                        f"RUN echo -e '[{localdev_repo_name}]\\nname = {localdev_repo_name}\\nid = {localdev_repo_name}\\nbaseurl = {x86_64_url}\\nenabled = 1\\ngpgcheck = 0\\nsslverify=0\\n' > /etc/yum.repos.d/{localdev_repo_name}.repo"
                    )

        if transform == transform_rhel_9_base_repos or config.transform == transform_rhel_9_golang:
            # The repos transform create a build config that will layer the base image with CI appropriate yum
            # repository definitions.
            dfp.add_lines(
                f'RUN rm -rf /etc/yum.repos.d/*.repo && curl http://base-{major}-{minor}-rhel9.ocp.svc > /etc/yum.repos.d/ci-rpm-mirrors.repo'
            )

            # Allow the base repos to be used BEFORE art begins mirroring 4.x to openshift mirrors.
            # This allows us to establish this locations later -- only disrupting CI for those
            # components that actually need reposync'd RPMs from the mirrors.
            dfp.add_lines('RUN yum config-manager --setopt=skip_if_unavailable=True --save')
            add_localdev_repo_profile('el9')

        if transform == transform_rhel_8_base_repos or config.transform == transform_rhel_8_golang:
            # The repos transform create a build config that will layer the base image with CI appropriate yum
            # repository definitions.
            dfp.add_lines(
                f'RUN rm -rf /etc/yum.repos.d/*.repo && curl http://base-{major}-{minor}-rhel8.ocp.svc > /etc/yum.repos.d/ci-rpm-mirrors.repo'
            )

            # Allow the base repos to be used BEFORE art begins mirroring 4.x to openshift mirrors.
            # This allows us to establish this locations later -- only disrupting CI for those
            # components that actually need reposync'd RPMs from the mirrors.
            dfp.add_lines('RUN yum config-manager --setopt=skip_if_unavailable=True --save')
            add_localdev_repo_profile('el8')

        if transform == transform_rhel_7_base_repos or config.transform == transform_rhel_7_golang:
            # The repos transform create a build config that will layer the base image with CI appropriate yum
            # repository definitions.
            dfp.add_lines(
                f'RUN rm -rf /etc/yum.repos.d/*.repo && curl http://base-{major}-{minor}.ocp.svc > /etc/yum.repos.d/ci-rpm-mirrors.repo'
            )
            # Allow the base repos to be used BEFORE art begins mirroring 4.x to openshift mirrors.
            # This allows us to establish this locations later -- only disrupting CI for those
            # components that actually need reposync'd RPMs from the mirrors.
            add_localdev_repo_profile('el7')
            dfp.add_lines("RUN yum-config-manager --save '--setopt=skip_if_unavailable=True'")
            dfp.add_lines("RUN yum-config-manager --save '--setopt=*.skip_if_unavailable=True'")

        if config.final_user:
            # If the image should not run as root/0, then allow metadata to specify a
            # true final user.
            dfp.add_lines(f'USER {config.final_user}')

        # We've arrived at a Dockerfile.
        dockerfile_content = dfp.content

        # Now to create a buildconfig for it.
        buildconfig = {
            'apiVersion': 'build.openshift.io/v1',
            'kind': 'BuildConfig',
            'metadata': {
                'name': f'{dest_imagestream}-{dest_tag}--art-builder',
                'namespace': 'ci',
                'labels': {
                    'art-builder-group': group_label,
                    'art-builder-stream': upstream_entry_name,
                },
                'annotations': {
                    'description': 'Generated by the ART pipeline by doozer. Processes raw ART images into ART equivalent images for CI.',
                },
            },
            'spec': {
                'failedBuildsHistoryLimit': 2,
                'output': {
                    'to': {
                        'kind': 'ImageStreamTag',
                        'namespace': dest_ns,
                        'name': dest_istag,
                    },
                },
                'source': {
                    'dockerfile': dockerfile_content,
                    'type': 'Dockerfile',
                },
                'strategy': {
                    'dockerStrategy': {
                        'from': {
                            'kind': 'ImageStreamTag',
                            'name': intermediate_istag,
                            'namespace': intermediate_ns,
                        },
                        'imageOptimizationPolicy': 'SkipLayers',
                    },
                },
                'successfulBuildsHistoryLimit': 2,
                'triggers': [
                    {
                        'imageChange': {},
                        'type': 'ImageChange',
                    }
                ],
            },
        }

        buildconfig_definitions.append(buildconfig)

    with open(output, mode='w+', encoding='utf-8') as f:
        objects = list()
        objects.extend(buildconfig_definitions)
        yaml.dump_all(objects, f, default_flow_style=False)

    if apply:
        if buildconfig_definitions:
            print('Applying buildconfigs...')
            cmd = f'oc apply -f {output}'
            if as_user:
                cmd += f' --as {as_user}'
            exectools.cmd_assert(cmd, retries=3)
        else:
            print('No buildconfigs were generated; skipping apply.')


def calc_parent_digest(parent_images):
    m = hashlib.md5()
    m.update(';'.join(parent_images).encode('utf-8'))
    return m.hexdigest()


def extract_parent_digest(dockerfile_path):
    with dockerfile_path.open(mode='r') as handle:
        dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO())
        dfp.content = handle.read()
    return calc_parent_digest(dfp.parent_images), dfp.parent_images


def compute_dockerfile_digest(dockerfile_path):
    with dockerfile_path.open(mode='r') as handle:
        # Read in and standardize linefeed
        content = '\n'.join(handle.read().splitlines())
        m = hashlib.md5()
        m.update(content.encode('utf-8'))
        return m.hexdigest()


def resolve_upstream_from(runtime, image_entry):
    """
    :param runtime: The runtime object
    :param image_entry: A builder or from entry. e.g. { 'member': 'openshift-enterprise-base' }  or { 'stream': 'golang; }
    :return: The upstream CI pullspec to which this entry should resolve.
    """
    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']

    if image_entry.member:
        target_meta = runtime.resolve_image(image_entry.member, True)

        if target_meta.config.content.source.ci_alignment.upstream_image is not Missing:
            # If the upstream is specified in the metadata, use this information
            # directly instead of a heuristic.
            return target_meta.config.content.source.ci_alignment.upstream_image
        else:
            # If payload_name is specified, this is what we need
            # Otherwise, fallback to the legacy "name" field
            name = target_meta.config.payload_name or target_meta.config.name
            image_name = name.split('/')[-1]
            # In release payloads, images are promoted into an imagestream
            # tag name without the ose- prefix.
            image_name = remove_prefix(image_name, 'ose-')
            # e.g. registry.ci.openshift.org/ocp/4.6:base
            return f'registry.ci.openshift.org/ocp/{major}.{minor}:{image_name}'

    if image_entry.image:
        # CI is on its own. We can't give them an image that isn't available outside the firewall.
        return None
    elif image_entry.stream:
        return runtime.resolve_stream(image_entry.stream).upstream_image


def _get_upstream_source(runtime, image_meta, skip_branch_check=False):
    """
    Analyzes an image metadata to find the upstream URL and branch associated with its content.
    :param runtime: The runtime object
    :param image_meta: The metadata to inspect
    :param skip_branch_check: Skip check branch exist every time
    :return: A tuple containing (url, branch) for the upstream source OR (None, None) if there
            is no upstream source.
    """
    if "git" in image_meta.config.content.source:
        source_repo_url = image_meta.config.content.source.git.url
        source_repo_branch = image_meta.config.content.source.git.branch.target
        if not skip_branch_check:
            branch_check, err = exectools.cmd_assert(
                f'git ls-remote --heads {source_repo_url} {source_repo_branch}', strip=True, retries=3
            )
            if not branch_check:
                # Output is empty if branch does not exist
                source_repo_branch = image_meta.config.content.source.git.branch.fallback
                if source_repo_branch is Missing:
                    raise IOError(f'Unable to detect source repository branch for {image_meta.distgit_key}')
    elif "alias" in image_meta.config.content.source:
        alias = image_meta.config.content.source.alias
        if alias not in runtime.group_config.sources:
            raise IOError(f'Unable to find source alias {alias} for {image_meta.distgit_key}')
        source_repo_url = runtime.group_config.sources[alias].url
        source_repo_branch = runtime.group_config.sources[alias].branch.target
    else:
        # No upstream source, no PR to open
        return None, None

    return source_repo_url, source_repo_branch


@images_streams.group("prs", short_help="Manage ART equivalent PRs in upstream.")
def prs():
    pass


@prs.command(
    'list', short_help='List all open reconciliation prs for upstream repos (requires GITHUB_TOKEN env var to be set.'
)
@click.option(
    '--as',
    'as_user',
    metavar='GITHUB_USERNAME',
    required=False,
    default="openshift-bot",
    help='Github username to check PRs against. Defaults to openshift-bot.',
)
@click.option(
    '--include-master',
    default=False,
    is_flag=True,
    help='Also include master branch as base ref when checking for PRs, '
    'useful when master is fast forwarded to the release branch.',
)
@pass_runtime
def prs_list(runtime, as_user, include_master):
    runtime.initialize(clone_distgits=False, clone_source=False)
    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']
    retdata = {}
    github_client = Github(os.getenv(constants.GITHUB_TOKEN))
    pr_query = f'org:openshift author:{as_user} type:pr state:open ART in:title'
    query = f'{pr_query} base:release-{major}.{minor}'
    if include_master:
        query = f'{query} OR {pr_query} base:master OR {pr_query} base:main'
    runtime.logger.info(f'Search with query: {query}')
    prs = github_client.search_issues(query=query)
    runtime.logger.info(f'Total PRs: {prs.totalCount}')
    for pr in prs:
        current_pr = pr.repository.get_pull(pr.number)
        branch = current_pr.head.ref  # forked branch name art-consistency-{runtime.group_config.name}-{dgk}g
        if runtime.group_config.name in branch:
            owners_email = next(
                (
                    image_meta.config['owners'][0]
                    for image_meta in runtime.ordered_image_metas()
                    if image_meta.distgit_key in branch
                ),
                None,
            )
            retdata.setdefault(owners_email, {}).setdefault(current_pr.base.repo.html_url, []).append(
                dict(pr_url=pr.html_url, created_at=pr.created_at)
            )
    print(yaml.dump(retdata, default_flow_style=False, width=10000))


def connect_issue_with_pr(pr: PullRequest.PullRequest, issue: str):
    """
    Aligns an existing Jira issue with a PR. Put the issue number in the title of the github pr.
    Args:
        pr: The PR to align with
        issue: JIRA issue key
    """
    exist_issues = re.findall(r'OCPBUGS-[0-9]+', pr.title)
    if issue in pr.title:  # the issue already in pr title
        return
    elif exist_issues:  # another issue is in pr title, add comment
        for comment in pr.get_issue_comments():
            if issue in comment.body:
                return  # an exist comment already have the issue
        pr.create_issue_comment(
            f"ART wants to connect issue [{issue}]({get_jira_browse_url(issue)}) to this PR, \
                                but found it is currently hooked up to {exist_issues}. Please consult with #forum-ocp-art if it is not clear what there is to do."
        )
    else:  # update pr title
        pr.edit(title=f"{issue}: {pr.title}")


def reconcile_jira_issues(runtime, pr_map: Dict[str, Tuple[PullRequest.PullRequest, Optional[str]]], dry_run: bool):
    """
    Ensures there is a Jira issue open for reconciliation PRs.
    Args:
        runtime: The doozer runtime
        pr_map: a map of distgit_keys -> (pr_object, jenkins_build_url) to open reconciliation PRs
        dry_run: If true, new desired jira issues would only be printed to the console.
    """
    major, minor = runtime.get_major_minor_fields()
    if (major, minor) < (4, 16):
        # Only enabled for 4.16 and beyond
        return

    new_issues: Dict[str, Issue] = dict()
    existing_issues: Dict[str, Issue] = dict()

    release_version = f'{major}.{minor}'
    jira_client: JIRA = runtime.build_jira_client()

    jira_projects = jira_client.projects()
    jira_project_names = set([project.name for project in jira_projects])
    jira_project_components: Dict[str, Set[str]] = dict()  # Maps project names to the components they expose

    for distgit_key, value in pr_map.items():
        pr, jenkins_build_url = value
        image_meta: ImageMetadata = runtime.image_map[distgit_key]
        potential_project, potential_component = image_meta.get_jira_info()
        summary = f"ART requests updates to {release_version} image {image_meta.get_component_name()}"
        old_summary_format = (
            f"Update {release_version} {image_meta.get_component_name()} image to be consistent with ART"
        )

        project = potential_project
        if potential_project not in jira_project_names:
            # If the project in the prodsec data does not exist in Jira, default to OCPBUGS
            project = 'OCPBUGS'

        if project in jira_project_components:
            available_components = jira_project_components[project]
        else:
            available_components = set([component.name for component in jira_client.project_components(project)])
            jira_project_components[project] = available_components

        component = potential_component
        if potential_component not in available_components:
            # If the component in prodsec data does not exist in the Jira project, use Unknown.
            component = 'Unknown'

        query = f'project={project} AND ( summary ~ "{summary}" OR summary ~ "{old_summary_format}" ) AND issueFunction in linkedIssuesOfRemote("url", "{pr.html_url}")'

        @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
        def search_issues(query):
            return jira_client.search_issues(query)

        # jira title search is fuzzy, so we need to check if an issue is really the one we want
        open_issues = [
            i for i in search_issues(query) if i.fields.summary == summary or i.fields.summary == old_summary_format
        ]

        if open_issues:
            print(f'A JIRA issue is already open for {pr.html_url}: {open_issues[0].key}')
            existing_issues[distgit_key] = open_issues[0]
            connect_issue_with_pr(pr, open_issues[0].key)
            continue

        description = f'''
Please review the following PR: {pr.html_url}

The PR has been automatically opened by ART (#forum-ocp-art) team automation and indicates
that the image(s) being used downstream for production builds are not consistent
with the images referenced in this component's github repository.

Differences in upstream and downstream builds impact the fidelity of your CI signal.

If you disagree with the content of this PR, please contact @release-artists
in #forum-ocp-art to discuss the discrepancy.

Closing this issue without addressing the difference will cause the issue to
be reopened automatically.
'''
        if potential_project != project or potential_component != component:
            description += f'''

Important: ART has recorded in their product data that bugs for
this component should be opened against Jira project "{potential_project}" and
component "{potential_component}". This project or component does not exist. Jira
should either be updated to include this component or @release-artists should be
notified of the proper mapping in the #forum-ocp-art Slack channel.

Component name: {image_meta.get_component_name()} .
Jira mapping: https://github.com/openshift-eng/ocp-build-data/blob/main/product.yml
'''
        elif potential_project == 'Unknown':
            description += f'''

Important: ART maintains a mapping of OpenShift software components to
Jira components. This component does not currently have an entry defined
within that data.
Contact @release-artists in the #forum-ocp-art Slack channel to inform them of
the Jira project and component to use for this image.

Until this is done, ART issues against this component will be opened
against OCPBUGS/Unknown -- creating unnecessary processing work and delay.

Component name: {image_meta.get_component_name()} .
Jira mapping: https://github.com/openshift-eng/ocp-build-data/blob/main/product.yml
'''
        if jenkins_build_url:
            description += f'''

This ticket was created by ART pipline run [sync-ci-images|{jenkins_build_url}]
'''

        fields = {
            'project': {'key': project},
            'issuetype': {'name': 'Bug'},
            'labels': ['art:reconciliation', f'art:package:{image_meta.get_component_name()}'],
            'versions': [{'name': release_version}],  # Affects Version/s
            'components': [{'name': component}],
            'summary': summary,
            'description': description,
            "security": {"id": "11697"},  # Restrict to Red Hat Employee
        }

        if not dry_run:
            issue = jira_client.create_issue(
                fields,
            )
            try:
                # retrieve the target version string (e.g., 'z' or '4.21.0')
                target_version_segment = Model(runtime.gitdata.load_data(key='bug').data).target_release[-1]

                # Build the update payload using the retrieved string
                issue_update = {
                    'customfield_12319940': [{'name': target_version_segment}],
                }
                runtime.logger.info(
                    f"Attempting to update issue {issue.key} Target Version to: {target_version_segment}"
                )
                issue.update(fields=issue_update)
                runtime.logger.info(f"Successfully updated Target Version for issue {issue.key}.")

            except Exception as e:
                runtime.logger.error(f"An error occurred while updating the Target Version on issue {issue.key}: {e}")

            # check depend issues and set depend to a higher version issue if true
            look_for_summary = f'Update {major}.{minor + 1} {image_meta.name} image to be consistent with ART'
            depend_issues = search_issues(f"project={project} AND summary ~ '{look_for_summary}'")
            # jira title search is fuzzy, so we need to check if an issue is really the one we want
            depend_issues = [i for i in depend_issues if i.fields.summary == look_for_summary]
            if depend_issues:
                jira_client.create_issue_link("Depend", issue.key, depend_issues[0].key)
            new_issues[distgit_key] = issue
            print(f'A JIRA issue has been opened for {pr.html_url}: {issue.key}')
            connect_issue_with_pr(pr, issue.key)
            try:
                # Retrieve the value of the custom field
                release_notes_text_cf_value = getattr(issue.fields, 'customfield_12317313', None)
                release_notes_type_cf_value = getattr(issue.fields, 'customfield_12320850', None)

                if release_notes_type_cf_value is None and release_notes_text_cf_value is None:
                    # Data to update (e.g., changing the Release Notes Type and Release Notes Text)
                    issue_update = {
                        'customfield_12317313': 'N/A',  # customfield_12317313 is Release Notes Text in JIRA
                        'customfield_12320850': {
                            'value': 'Release Note Not Required'
                        },  # customfield_12320850 is Release Notes Type in JIRA
                    }
                    # Now update the issue using the retrieved issue object
                    issue.update(fields=issue_update)
            except Exception as e:
                print(f"An error occurred while updating the issue {issue.key}: {e}")
        else:
            new_issues[distgit_key] = 'NEW!'
            print(f'Would have created JIRA issue for {distgit_key} / {pr.html_url}:\n{fields}\n')

    if new_issues:
        print('Newly opened JIRA issues:')
        for key, issue in new_issues.items():
            print(f'{key}: {issue}')

    if existing_issues:
        print('Previously existing JIRA issues:')
        for key, issue in existing_issues.items():
            print(f'{key}: {issue}')


@prs.command(
    'open', short_help='Open PRs against upstream component repos that have a FROM that differs from ART metadata.'
)
@click.option('--github-access-token', metavar='TOKEN', required=True, help='Github access token for user.')
@click.option('--bug', metavar='BZ#', required=False, default=None, help='Title with Bug #: prefix')
@click.option(
    '--interstitial',
    metavar='SECONDS',
    default=120,
    required=False,
    help='Delay after each new PR to prevent flooding CI',
)
@click.option(
    '--ignore-ci-master',
    default=False,
    is_flag=True,
    help='Do not consider what is in master branch when determining what branch to target',
)
@click.option(
    '--force-merge',
    default=False,
    is_flag=True,
    help='DANGER! Use only with approval. Do not wait for standard CI PR merge. Call merge API directly.',
)
@click.option(
    '--ignore-missing-images', default=False, is_flag=True, help='Do not exit if an image is missing upstream.'
)
@click.option('--draft-prs', default=False, is_flag=True, help='Open PRs as draft PRs')
@click.option('--moist-run', default=False, is_flag=True, help='Do everything except opening the final PRs')
@click.option(
    '--add-auto-labels',
    default=False,
    is_flag=True,
    help='Add auto_labels to PRs; unless running as openshift-bot, you probably lack the privilege to do so',
)
@click.option(
    '--add-label',
    default=[],
    multiple=True,
    help='Add a label to all open PRs (new and existing) - Requires being openshift-bot',
)
@pass_runtime
def images_streams_prs(
    runtime,
    github_access_token,
    bug,
    interstitial,
    ignore_ci_master,
    force_merge,
    ignore_missing_images,
    draft_prs,
    moist_run,
    add_auto_labels,
    add_label,
):
    runtime.initialize(clone_distgits=False, clone_source=False)
    g = Github(login_or_token=github_access_token)
    github_user = g.get_user()

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']
    interstitial = int(interstitial)

    to_reconcile = runtime.group_config.reconciliation_prs.enabled
    if to_reconcile is not Missing and not to_reconcile:
        runtime.logger.warning(f'streams_prs_disabled is set in group.yml of {major}.{minor}; skipping PRs')
        exit(0)

    master_major, master_minor = extract_version_fields(what_is_in_master(), at_least=2)
    if not ignore_ci_master and (major > master_major or minor > master_minor):
        # ART building a release before it is in master. Too early to open PRs.
        runtime.logger.warning(
            f'Target {major}.{minor} has not been in master yet (it is tracking {master_major}.{master_minor}); skipping PRs'
        )
        exit(0)

    prs_in_master = (major == master_major and minor == master_minor) and not ignore_ci_master

    # map of distgit_key to (PR url, jenkins_build_url) associated with updates
    pr_dgk_map: Dict[str, Tuple[PullRequest.PullRequest, Optional[str]]] = {}
    new_pr_links = {}
    skipping_dgks = set()  # If a distgit key is skipped, it children will see it in this list and skip themselves.
    checked_upstream_images = (
        set()
    )  # A PR will not be opened unless the upstream image exists; keep track of ones we have checked.
    errors_raised = False  # If failures are found when opening a PR, won't break the loop but will return an exit code to signal this event

    for image_meta in runtime.ordered_image_metas():
        dgk = image_meta.distgit_key
        logger = image_meta.logger
        logger.info('Analyzing image')

        streams_pr_config = image_meta.config.content.source.ci_alignment.streams_prs

        if streams_pr_config and streams_pr_config.enabled is not Missing and not streams_pr_config.enabled:
            # Make sure this is an explicit False. Missing means the default or True.
            logger.info('The image has alignment PRs disabled; ignoring')
            continue

        from_config = image_meta.config['from']
        if not from_config:
            logger.info('Skipping PR check since there is no configured .from')
            continue

        def check_if_upstream_image_exists(upstream_image):
            if upstream_image not in checked_upstream_images:
                # We don't know yet whether this image exists; perhaps a buildconfig is
                # failing. Don't open PRs for images that don't yet exist.
                try:
                    util.oc_image_info_for_arch__caching(upstream_image)
                except:
                    yellow_print(
                        f'Unable to access upstream image {upstream_image} for {dgk}-- check whether buildconfigs are running successfully.'
                    )
                    if not ignore_missing_images:
                        raise
                checked_upstream_images.add(
                    upstream_image
                )  # Don't check this image again since it is a little slow to do so.

        desired_parents = []

        # There are two methods to find the desired parents for upstream Dockerfiles.
        # 1. We can analyze the image_metadata "from:" stanza and determine the upstream
        #    equivalents for streams and members. This is the most common flow of the code.
        # 2. In rare circumstances, we need to circumvent this analysis and specify the
        #    desired upstreams equivalents directly in the image metadata. This is only
        #    required when the upstream Dockerfile uses something wholly different
        #    than the downstream build. Use this method by specifying:
        #      source.ci_alignment.streams_prs.from: [ list of full pullspecs ]
        # We check for option 2 first
        if streams_pr_config['from'] is not Missing:
            desired_parents = streams_pr_config['from'].primitive()  # This should be list; so we're done.
        else:
            builders = from_config.builder or []
            for builder in builders:
                upstream_image = resolve_upstream_from(runtime, builder)
                if not upstream_image:
                    logger.warning(f'Unable to resolve upstream image for: {builder}')
                    break
                check_if_upstream_image_exists(upstream_image)
                desired_parents.append(upstream_image)

            parent_upstream_image = resolve_upstream_from(runtime, from_config)
            if len(desired_parents) != len(builders) or not parent_upstream_image:
                logger.warning('Unable to find all ART equivalent upstream images for this image')
                continue

            desired_parents.append(parent_upstream_image)

        desired_parent_digest = calc_parent_digest(desired_parents)
        logger.info(f'Found desired FROM state of: {desired_parents} with digest: {desired_parent_digest}')

        desired_ci_build_root_coordinate = None
        desired_ci_build_root_image = ''
        if streams_pr_config.ci_build_root is not Missing:
            desired_ci_build_root_image = resolve_upstream_from(runtime, streams_pr_config.ci_build_root)
            check_if_upstream_image_exists(desired_ci_build_root_image)

            # Split the pullspec into an openshift namespace, imagestream, and tag.
            # e.g. registry.openshift.org:999/ocp/release:golang-1.16 => tag=golang-1.16, namespace=ocp, imagestream=release
            pre_tag, tag = desired_ci_build_root_image.rsplit(':', 1)
            _, namespace, imagestream = pre_tag.rsplit('/', 2)
            # https://docs.ci.openshift.org/docs/architecture/ci-operator/#build-root-image
            desired_ci_build_root_coordinate = {
                'namespace': namespace,
                'name': imagestream,
                'tag': tag,
            }
            logger.info(f'Found desired build_root state of: {desired_ci_build_root_coordinate}')

        source_repo_url, source_repo_branch = _get_upstream_source(runtime, image_meta)

        if not source_repo_url or 'github.com' not in source_repo_url:
            # No upstream to clone; no PRs to open
            logger.info('Skipping PR check since there is no configured github source URL')
            continue

        public_repo_url, public_branch, _ = SourceResolver.get_public_upstream(
            source_repo_url, runtime.group_config.public_upstreams
        )
        if not public_branch:
            public_branch = source_repo_branch

        if (public_branch == 'master' or public_branch == 'main') and not prs_in_master:
            # If a component is not using 'release-4.x' / 'openshift-4.x' branching mechanics,
            # ART will be falling back to use master/main branch for the content of ALL
            # releases. In this case, only open a reconciliation PR for when the current
            # group matches what release CI is tracking in master.
            logger.info(
                f'Skipping PR for {runtime.group} : {dgk} / {public_repo_url} since associated public branch is {public_branch} but CI is tracking {master_major}.{master_minor} in that branch.'
            )
            continue

        # There are two standard upstream branching styles:
        # release-4.x   : CI fast-forwards from default branch (master or main) when appropriate
        # openshift-4.x : Upstream team manages completely.
        # For the former style, we may need to open the PRs against the default branch (master or main).
        # For the latter style, always open directly against named branch
        if public_branch.startswith('release-') and prs_in_master:
            public_branches, _ = exectools.cmd_assert(f'git ls-remote --heads {public_repo_url}', strip=True)
            public_branches = public_branches.splitlines()
            priv_branches, _ = exectools.cmd_assert(f'git ls-remote --heads {source_repo_url}', strip=True)
            priv_branches = priv_branches.splitlines()

            if [bl for bl in public_branches if bl.endswith('/main')] and [
                bl for bl in priv_branches if bl.endswith('/main')
            ]:
                public_branch = 'main'
            elif [bl for bl in public_branches if bl.endswith('/master')] and [
                bl for bl in priv_branches if bl.endswith('/master')
            ]:
                public_branch = 'master'
            else:
                # There are ways of determining default branch without using naming conventions, but as of today, we don't need it.
                raise IOError(f'Did not find master or main branch; unable to detect default branch: {public_branches}')

        _, org, repo_name = split_git_url(public_repo_url)

        public_source_repo = g.get_repo(f'{org}/{repo_name}')

        try:
            fork_repo_name = f'{github_user.login}/{repo_name}'
            fork_repo = g.get_repo(fork_repo_name)
        except UnknownObjectException:
            # Repo doesn't exist; fork it
            fork_repo = github_user.create_fork(public_source_repo)

        fork_branch_name = f'art-consistency-{runtime.group_config.name}-{dgk}'
        fork_branch_head = f'{github_user.login}:{fork_branch_name}'

        fork_branch = None
        try:
            fork_branch = fork_repo.get_branch(fork_branch_name)
        except UnknownObjectException:
            # Doesn't presently exist and will need to be created
            pass
        except GithubException as ge:
            # This API seems to return 404 instead of UnknownObjectException.
            # So allow 404 to pass through as well.
            if ge.status != 404:
                raise

        public_repo_url = convert_remote_git_to_ssh(public_repo_url)
        clone_dir = os.path.join(runtime.working_dir, 'clones', dgk)
        # Clone the private url to make the best possible use of our doozer_cache
        git_clone(source_repo_url, clone_dir, git_cache_dir=runtime.git_cache_dir)

        with Dir(clone_dir):
            exectools.cmd_assert(f'git remote add public {public_repo_url}')
            exectools.cmd_assert(f'git remote add fork {convert_remote_git_to_ssh(fork_repo.git_url)}')
            exectools.cmd_assert('git fetch --all', retries=3)

            # The path to the Dockerfile in the target branch
            if image_meta.config.content.source.dockerfile is not Missing:
                # Be aware that this attribute sometimes contains path elements too.
                dockerfile_name = image_meta.config.content.source.dockerfile
            else:
                dockerfile_name = "Dockerfile"

            df_path = Dir.getpath()
            if image_meta.config.content.source.path:
                dockerfile_name = os.path.join(image_meta.config.content.source.path, dockerfile_name)

            df_path = df_path.joinpath(dockerfile_name).resolve()
            ci_operator_config_path = (
                Dir.getpath().joinpath('.ci-operator.yaml').resolve()
            )  # https://docs.ci.openshift.org/docs/architecture/ci-operator/#build-root-image

            assignee = None
            try:
                root_owners_path = Dir.getpath().joinpath('OWNERS')
                if root_owners_path.exists():
                    parsed_owners = yaml.safe_load(root_owners_path.read_text())
                    if 'approvers' in parsed_owners and len(parsed_owners['approvers']) > 0:
                        assignee = random.choice(parsed_owners['approvers'])
            except Exception as owners_e:
                yellow_print(f'Error finding assignee in OWNERS for {public_repo_url}: {owners_e}')

            fork_ci_build_root_coordinate = None
            fork_branch_df_digest = None  # digest of dockerfile image names
            if fork_branch:
                # There is already an ART reconciliation branch. Our fork branch might be up-to-date
                # OR behind the times (e.g. if someone committed changes to the upstream Dockerfile).
                # Let's check.

                # If there is already an art reconciliation branch, get an MD5
                # of the FROM images in the Dockerfile in that branch.
                exectools.cmd_assert(f'git checkout fork/{fork_branch_name}')
                if df_path.exists():
                    fork_branch_df_digest = compute_dockerfile_digest(df_path)
                else:
                    # It is possible someone has moved the Dockerfile around since we
                    # made the fork.
                    fork_branch_df_digest = 'DOCKERFILE_NOT_FOUND'

                if ci_operator_config_path.exists():
                    fork_ci_operator_config = yaml.safe_load(
                        ci_operator_config_path.read_text(encoding='utf-8')
                    )  # Read in content from fork
                    fork_ci_build_root_coordinate = fork_ci_operator_config.get('build_root_image', None)

            # Now change over to the target branch in the actual public repo
            exectools.cmd_assert(f'git checkout public/{public_branch}')

            try:
                source_branch_parent_digest, source_branch_parents = extract_parent_digest(df_path)
            except FileNotFoundError:
                logger.error('%s not found in branch public/%s', df_path, public_branch)
                errors_raised = True
                continue

            source_branch_ci_build_root_coordinate = None
            if ci_operator_config_path.exists():
                source_branch_ci_operator_config = yaml.safe_load(
                    ci_operator_config_path.read_text(encoding='utf-8')
                )  # Read in content from public source
                source_branch_ci_build_root_coordinate = source_branch_ci_operator_config.get('build_root_image', None)

            public_branch_commit, _ = exectools.cmd_assert('git rev-parse HEAD', strip=True)

            logger.info(f'''
Desired parents: {desired_parents} ({desired_parent_digest})
Desired build_root (in .ci-operator.yaml): {desired_ci_build_root_coordinate}

Source parents: {source_branch_parents} ({source_branch_parent_digest})
Source build_root (in .ci-operator.yaml): {source_branch_ci_build_root_coordinate}

Fork build_root (in .ci-operator.yaml): {fork_ci_build_root_coordinate}
''')
            if desired_parent_digest == source_branch_parent_digest and (
                desired_ci_build_root_coordinate is None
                or desired_ci_build_root_coordinate == source_branch_ci_build_root_coordinate
            ):
                green_print(
                    'Desired digest and source digest match; desired build_root unset OR coordinates match; Upstream is in a good state'
                )
                if fork_branch:
                    for pr in list(public_source_repo.get_pulls(state='open', head=fork_branch_head)):
                        if moist_run:
                            yellow_print(f'Would have closed existing PR: {pr.html_url}')
                        else:
                            yellow_print(f'Closing unnecessary PR: {pr.html_url}')
                            pr.edit(state='closed')
                continue

            cardinality_mismatch = False
            if len(desired_parents) != len(source_branch_parents):
                # The number of FROM statements in the ART metadata does not match the number
                # of FROM statements in the upstream Dockerfile.
                cardinality_mismatch = True

            yellow_print(
                f'Upstream dockerfile does not match desired state in {public_repo_url}/blob/{public_branch}/{dockerfile_name}'
            )

            first_commit_line = (
                f"Updating {image_meta.get_component_name()} image to be consistent with ART for {major}.{minor}"
            )
            reconcile_url = f'{convert_remote_git_to_https(runtime.gitdata.origin_url)}/tree/{runtime.gitdata.commit_hash}/images/{os.path.basename(image_meta.config_filename)}'
            reconcile_info = f"Reconciling with {reconcile_url}"

            diff_text = None
            # Three possible states at this point:
            # 1. No fork branch exists
            # 2. It does exist, but is out of date (e.g. metadata has changed or upstream Dockerfile has changed)
            # 3. It does exist, and is exactly how we want it.
            # Let's create a local branch that will contain the Dockerfile/.ci-operator.yaml in the state we desire.
            work_branch_name = '__mod'
            exectools.cmd_assert(f'git checkout public/{public_branch}')
            exectools.cmd_assert(f'git checkout -b {work_branch_name}')
            with df_path.open(mode='r+') as handle:
                dfp = DockerfileParser(cache_content=True, fileobj=io.BytesIO())
                dfp.content = handle.read()
                handle.truncate(0)
                handle.seek(0)
                if not cardinality_mismatch:
                    dfp.parent_images = desired_parents
                else:
                    handle.writelines(
                        [
                            '# URGENT! ART metadata configuration has a different number of FROMs\n',
                            '# than this Dockerfile. ART will be unable to build your component or\n',
                            '# reconcile this Dockerfile until that disparity is addressed.\n',
                        ]
                    )
                handle.write(dfp.content)

            exectools.cmd_assert(f'git add {str(df_path)}')

            if desired_ci_build_root_coordinate:
                if ci_operator_config_path.exists():
                    ci_operator_config = yaml.safe_load(ci_operator_config_path.read_text(encoding='utf-8'))
                else:
                    ci_operator_config = {}

                # Overwrite the existing build_root_image if it exists. Preserve any other settings.
                ci_operator_config['build_root_image'] = desired_ci_build_root_coordinate
                with ci_operator_config_path.open(mode='w+', encoding='utf-8') as config_file:
                    yaml.safe_dump(ci_operator_config, config_file, default_flow_style=False)

                exectools.cmd_assert(f'git add -f {str(ci_operator_config_path)}')

            desired_df_digest = compute_dockerfile_digest(df_path)

            # Check for any existing open PR
            open_prs = list(public_source_repo.get_pulls(state='open', head=fork_branch_head))

            logger.info(f'''
Desired Dockerfile digest: {desired_df_digest}
Fork Dockerfile digest: {fork_branch_df_digest}
open_prs: {open_prs}
''')

            # A note on why the following `if` cares about whether there is a PR open.
            # We need to check on an edge case.. if the upstream merged our reconciliation PR and THEN proceeded to change
            # their Dockerfile FROMs again, our desired_df_digest will always equal fork_branch_df_digest and
            # we might conclude that we just need to open a PR with our fork branch. However, in this scenario,
            # GitHub will throw an exception like:
            # "No commits between openshift:master and openshift-bot:art-consistency-openshift-4.8-ptp-operator-must-gather"
            # So... to prevent this, if there is no open PR, we should force push to the fork branch to make its
            # commit something different than what is in the public branch.

            diff_text, _ = exectools.cmd_assert('git diff HEAD', strip=True)
            if (
                desired_df_digest != fork_branch_df_digest
                or (
                    desired_ci_build_root_coordinate
                    and desired_ci_build_root_coordinate != fork_ci_build_root_coordinate
                )
                or not open_prs
            ):
                yellow_print('Found that fork branch is not in sync with public Dockerfile/.ci-operator.yaml changes')
                yellow_print(diff_text)

                if not moist_run:
                    commit_prefix = image_meta.config.content.source.ci_alignment.streams_prs.commit_prefix or ''
                    if repo_name.startswith('kubernetes') and not commit_prefix:
                        # Repos starting with 'kubernetes' don't have this in metadata atm. Preserving
                        # historical behavior until they do.
                        commit_prefix = 'UPSTREAM: <carry>: '
                    commit_msg = f"""{commit_prefix}{first_commit_line}
{reconcile_info}
"""
                    exectools.cmd_assert(
                        f'git commit -m "{commit_msg}"'
                    )  # Add a commit atop the public branch's current state
                    # Create or update the remote fork branch
                    exectools.cmd_assert(f'git push --force fork {work_branch_name}:{fork_branch_name}', retries=3)

            # At this point, we have a fork branch in the proper state
            pr_body = f"""{first_commit_line}
__TLDR__:
Product builds by ART can be configured for different base and builder images than corresponding CI
builds. This automated PR requests a change to CI configuration to align with ART's configuration;
please take steps to merge it quickly or contact ART to coordinate changes.

The configuration in the following ART component metadata is driving this alignment request:
[{os.path.basename(image_meta.config_filename)}]({reconcile_url}).

__Detail__:

This repository is out of sync with the downstream product builds for this component. The CI
configuration for at least one image differs from ART's expected product configuration. This should
be addressed to ensure that the component's CI testing accurate reflects what customers will
experience.

Most of these PRs are opened as an ART-driven proposal to migrate base image or builder(s) to a
different version, usually prior to GA. The intent is to effect changes in both configurations
simultaneously without breaking either CI or ART builds, so usually ART builds are configured to
consider CI as canonical and attempt to match CI config until the PR merges to align both. ART may
also configure changes in GA releases with CI remaining canonical for a brief grace period to enable
CI to succeed and the alignment PR to merge. In either case, ART configuration will be made
canonical at some point (typically at branch-cut before GA or release dev-cut after GA), so it is
important to align CI configuration as soon as possible.

PRs are also triggered when CI configuration changes without ART coordination, for instance to
change the number of builder images or to use a different golang version. These changes should be
coordinated with ART; whether ART configuration is canonical or not, preferably it would be updated
first to enable the changes to occur simultaneously in both CI and ART at the same time. This also
gives ART a chance to validate the intended changes first. For instance, ART compiles most
components with the Golang version being used by the control plane for a given OpenShift release.
Exceptions to this convention (i.e. you believe your component must be compiled with a Golang
version independent from the control plane) must be granted by the OpenShift staff engineers and
communicated to the ART team.

__Roles & Responsibilities__:
- Component owners are responsible for ensuring these alignment PRs merge with passing
  tests OR that necessary metadata changes are reported to the ART team: `@release-artists`
  in `#forum-ocp-art` on Slack. If necessary, the changes required by this pull request can be
  introduced with a separate PR opened by the component team. Once the repository is aligned,
  this PR will be closed automatically.
- In particular, it could be that a job like `verify-deps` is complaining. In that case, please open
  a new PR with the dependency issues addressed (and base images bumped). ART-9595 for reference.
- Patch-manager or those with sufficient privileges within this repository may add
  any required labels to ensure the PR merges once tests are passing. In cases where ART config is
  canonical, downstream builds are *already* being built with these changes, and merging this PR
  only improves the fidelity of our CI. In cases where ART config is not canonical, this provides
  a grace period for the component team to align their CI with ART's configuration before it becomes
  canonical in product builds.
"""
            if desired_ci_build_root_coordinate:
                pr_body += """
ART has been configured to reconcile your CI build root image (see https://docs.ci.openshift.org/docs/architecture/ci-operator/#build-root-image).
In order for your upstream .ci-operator.yaml configuration to be honored, you must set the following in your openshift/release ci-operator configuration file:
```
build_root:
  from_repository: true
```
"""

            pr_body += """
__Change behavior of future PRs__:
* In case you just want to follow the base images that ART suggests, you can configure additional labels to be
  set up automatically. This means that such a PR would *merge without human intervention* (and awareness!) in the future.
  To do so, open a PR to set the `auto_label` attribute in the image configuration. [Example](https://github.com/openshift-eng/ocp-build-data/pull/1778)
* You can set a commit prefix, like `UPSTREAM: <carry>: `. [An example](https://github.com/openshift-eng/ocp-build-data/blob/6831b59dddc5b63282076d3abe04593ad1945148/images/ose-cluster-api.yml#L11).
"""
            pr_body += """
If you have any questions about this pull request, please reach out to `@release-artists` in the `#forum-ocp-art` coreos slack channel.
"""

            parent_pr_urls = None
            parent_meta = image_meta.resolve_parent()
            if parent_meta:
                if parent_meta.distgit_key in skipping_dgks:
                    skipping_dgks.add(image_meta.distgit_key)
                    yellow_print(
                        f'Image has parent {parent_meta.distgit_key} which was skipped; skipping self: {image_meta.distgit_key}'
                    )
                    continue

                parent_pr_urls = pr_dgk_map.get(parent_meta.distgit_key, None)
                if parent_pr_urls:
                    if parent_meta.config.content.source.ci_alignment.streams_prs.merge_first:
                        skipping_dgks.add(image_meta.distgit_key)
                        yellow_print(
                            f'Image has parent {parent_meta.distgit_key} open PR ({parent_pr_urls[0]}) and streams_prs.merge_first==True; skipping PR opening for this image {image_meta.distgit_key}'
                        )
                        continue

                    # If the parent has an open PR associated with it, make sure the
                    # child PR notes that the parent PR should merge first.
                    pr_body += (
                        f'\nDepends on {parent_pr_urls[0]} . Allow it to merge and then run `/test all` on this PR.'
                    )

            jenkins_build_url = None
            if open_prs:
                existing_pr = open_prs[0]
                # Update body, but never title; The upstream team may need set something like a Bug XXXX: there.
                # Don't muck with it.

                try:
                    if streams_pr_config.auto_label and add_auto_labels:
                        # If we are to automatically add labels to this upstream PR, do so.
                        existing_pr.add_to_labels(*streams_pr_config.auto_label)

                    if add_label:
                        existing_pr.add_to_labels(*add_label)
                except GithubException as pr_e:
                    # We are not admin on all repos
                    yellow_print(f'Unable to add labels to {existing_pr.html_url}: {str(pr_e)}')

                pr_dgk_map[dgk] = (existing_pr, jenkins_build_url)

                # The pr_body may change and the base branch may change (i.e. at branch cut,
                # a version 4.6 in master starts being tracked in release-4.6 and master tracks
                # 4.7.
                if moist_run:
                    if existing_pr.base.sha != public_branch_commit:
                        yellow_print(
                            f'Would have changed PR {existing_pr.html_url} to use base {public_branch} ({public_branch_commit}) vs existing {existing_pr.base.sha}'
                        )
                    if existing_pr.body != pr_body:
                        yellow_print(
                            f'Would have changed PR {existing_pr.html_url} to use body "{pr_body}" vs existing "{existing_pr.body}"'
                        )
                    if not existing_pr.assignees and assignee:
                        yellow_print(f'Would have changed PR assignee to {assignee}')
                else:
                    if not existing_pr.assignees and assignee:
                        try:
                            existing_pr.add_to_assignees(assignee)
                        except Exception as access_issue:
                            # openshift-bot does not have admin on *all* repositories; permit this error.
                            yellow_print(f'Unable to set assignee for {existing_pr.html_url}: {access_issue}')
                    existing_pr.edit(body=pr_body, base=public_branch)
                    yellow_print(
                        f'A PR is already open requesting desired reconciliation with ART: {existing_pr.html_url}'
                    )
                    if force_merge:
                        existing_pr.merge()
                        yellow_print(f'Force merge is enabled. Triggering merge for: {existing_pr.html_url}')
                continue

            # Otherwise, we need to create a pull request
            if moist_run:
                pr_dgk_map[dgk] = (f'MOIST-RUN-PR:{dgk}', jenkins_build_url)
                green_print(
                    f'Would have opened PR against: {public_source_repo.html_url}/blob/{public_branch}/{dockerfile_name}.'
                )
                yellow_print('PR body would have been:')
                yellow_print(pr_body)

                if parent_pr_urls:
                    green_print(f'Would have identified dependency on PR: {parent_pr_urls[0]}.')
                if diff_text:
                    yellow_print('PR diff would have been:')
                    yellow_print(diff_text)
                else:
                    yellow_print(
                        f'Fork from which PR would be created ({fork_branch_head}) is populated with desired state.'
                    )
            else:
                pr_title = first_commit_line
                if bug:
                    pr_title = f'Bug {bug}: {pr_title}'
                try:
                    new_pr = public_source_repo.create_pull(
                        title=pr_title, body=pr_body, base=public_branch, head=fork_branch_head, draft=draft_prs
                    )
                    jenkins_build_url = jenkins.get_build_url()
                    if jenkins_build_url:
                        new_pr.create_issue_comment(f"Created by ART pipeline job run {jenkins_build_url}")

                except GithubException as ge:
                    if 'already exists' in json.dumps(ge.data):
                        # In one execution to date, get_pulls did not find the open PR and the code repeatedly hit this
                        # branch -- attempting to recreate the PR. Everything seems right, but the github api is not
                        # returning it. So catch the error and try to move on.
                        yellow_print(
                            'Issue attempting to find it, but a PR is already open requesting desired reconciliation with ART'
                        )
                        continue
                    raise

                try:
                    if streams_pr_config.auto_label and add_auto_labels:
                        # If we are to automatically add labels to this upstream PR, do so.
                        new_pr.add_to_labels(*streams_pr_config.auto_label)

                    if add_label:
                        new_pr.add_to_labels(*add_label)
                except GithubException as pr_e:
                    # We are not admin on all repos
                    yellow_print(f'Unable to add labels to {existing_pr.html_url}: {str(pr_e)}')

                pr_msg = f'A new PR has been opened: {new_pr.html_url}'
                pr_dgk_map[dgk] = (new_pr, jenkins_build_url)
                new_pr_links[dgk] = new_pr.html_url
                reconcile_jira_issues(runtime, {dgk: (new_pr, jenkins_build_url)}, moist_run)
                logger.info(pr_msg)
                yellow_print(pr_msg)
                print(f'Sleeping {interstitial} seconds before opening another PR to prevent flooding prow...')
                time.sleep(interstitial)

    if new_pr_links:
        print('Newly opened PRs:')
        print(yaml.safe_dump(new_pr_links))

    if pr_dgk_map:
        print('Currently open PRs:')
        print(yaml.safe_dump({key: pr_dgk_map[key][0].html_url for key in pr_dgk_map}))
        reconcile_jira_issues(runtime, pr_dgk_map, moist_run)

    if skipping_dgks:
        print('Some PRs were skipped; Exiting with return code 25 to indicate this')
        exit(25)

    if errors_raised:
        print('Some errors were raised: see the log for details')
        exit(50)
