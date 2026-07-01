import hashlib
import io
import json
import os
import random
import re
import time
from typing import Dict, Optional, Set, Tuple

import click
import yaml
from artcommonlib import exectools
from artcommonlib.format_util import green_print, yellow_print
from artcommonlib.git_helper import git_clone
from artcommonlib.github_auth import build_git_auth_env, get_github_client_for_org, get_github_git_auth_env
from artcommonlib.jira_config import get_jira_browse_url
from artcommonlib.model import Missing, Model
from artcommonlib.ocp_version_lineage import get_reconciliation_depend_version
from artcommonlib.pushd import Dir
from artcommonlib.util import (
    convert_remote_git_to_https,
    ensure_github_https_url,
    remove_prefix,
    split_git_url,
)
from dockerfile_parse import DockerfileParser
from elliottlib.bzutil import JIRABugTracker
from github import Auth, Github, GithubException, PullRequest, UnknownObjectException
from jira import JIRA, Issue
from jira.exceptions import JIRAError
from tenacity import retry, stop_after_attempt, wait_fixed

from doozerlib import util
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


def get_image_digest(pullspec: str, registry_config: Optional[str] = None) -> Optional[str]:
    """
    Get the digest of an image, handling both single-arch and manifest lists.
    Returns the digest string (e.g., 'sha256:...') or None if not found.
    """
    # Use text output (not -o json) because -o json fails for manifest lists
    cmd = f'oc image info {pullspec}'
    if registry_config:
        cmd += f' --registry-config={registry_config}'

    rc, stdout, stderr = exectools.cmd_gather(cmd)
    if rc != 0:
        return None

    # Parse digest from text output: "Digest: sha256:..."
    for line in stdout.splitlines():
        if line.startswith('Digest:'):
            return line.split(':', 1)[1].strip()
    return None


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
    '--image',
    'images',
    metavar='IMAGE_NAME',
    default=[],
    multiple=True,
    help='If specified, only these image distgit keys will be mirrored (must have ci_alignment.upstream_image).',
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
    images: Tuple[str, ...],
    only_if_missing: bool,
    live_test_mode: bool,
    force: bool,
    dry_run: bool,
    registry_auth: Optional[str],
):
    runtime.initialize(clone_distgits=False, clone_source=False, prevent_cloning=True)
    runtime.assert_mutation_is_permitted()

    # Determine which registry config to use:
    # 1. If --registry-auth is provided, use it directly (it's already a file path)
    # 2. Otherwise, fall back to --registry-config-dir (global option, needs conversion to file path)
    registry_config_file = None
    if registry_auth:
        registry_config_file = registry_auth
    elif runtime.registry_config_dir is not None:
        registry_config_file = get_docker_config_json(runtime.registry_config_dir)

    def mirror_image(cmd_start: str, upstream_dest: str, source_digest: Optional[str] = None):
        if upstream_dest.startswith('registry.ci.openshift.org/'):
            # Images targeting CI imagestreams must be mirrored to quay.io/openshift/ci (QCI) first,
            # then imagestreams updated to reference the QCI image by digest.
            # upstream_dest looks like: registry.ci.openshift.org/ocp/{MAJOR}.{MINOR}:base-rhel9
            # Extract namespace and imagestreamtag
            _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)
            dest_imagestream, dest_tag = dest_istag.split(':')

            # Build QCI floating tag: art__ocp_{MAJOR}.{MINOR}_base-rhel9
            org_repo_tag = re.sub(r"[:/]", "_", f"{dest_ns}/{dest_istag}")
            floating_qci_tag = f"art__{org_repo_tag}"
            floating_qci_dest = f"quay.io/openshift/ci:{floating_qci_tag}"

            # Mirror to QCI floating tag
            full_cmd_floating = f'{cmd_start} {floating_qci_dest}'
            if dry_run:
                print(f'For {upstream_entry_name}, would have run: {full_cmd_floating}')
            else:
                # Try to mirror - if source doesn't exist, log warning and skip
                rc, stdout, stderr = exectools.cmd_gather(full_cmd_floating, realtime=True)
                if rc != 0:
                    if 'manifest unknown' in stderr or 'not found' in stderr:
                        runtime.logger.warning(
                            f'Source image does not exist for {upstream_entry_name}, skipping: {stderr.strip()}'
                        )
                        print(f'For {upstream_entry_name}, source image not found - skipping')
                        return
                    else:
                        # Other error - raise it
                        raise ChildProcessError(
                            f'Failed to mirror {upstream_entry_name}: {stderr}', (rc, stdout, stderr)
                        )

                # Use source digest when available (oc image mirror preserves manifest digests).
                # Falling back to get_image_digest for tag-based sources where digest isn't known.
                if source_digest:
                    qci_digest = source_digest
                else:
                    qci_digest = get_image_digest(floating_qci_dest, registry_config_file)

                if qci_digest:
                    # Mirror to GC-prevention tag: art__<digest>
                    # Preserve --keep-manifest-list flag if it was in the original command
                    gc_prevention_tag = f"art__{qci_digest[7:23]}"
                    gc_prevention_dest = f"quay.io/openshift/ci:{gc_prevention_tag}"
                    keep_manifest_list = '--keep-manifest-list' if '--keep-manifest-list' in cmd_start else ''
                    gc_mirror_cmd = (
                        f'oc image mirror {keep_manifest_list} {floating_qci_dest}@{qci_digest} {gc_prevention_dest}'
                    )
                    if registry_config_file:
                        gc_mirror_cmd += f' --registry-config={registry_config_file}'
                    exectools.cmd_assert(gc_mirror_cmd, retries=3, realtime=True)

                    # Update imagestream to reference QCI by digest via quay-proxy
                    istag_patch = {
                        'tag': {
                            'name': dest_tag,
                            'from': {
                                'kind': 'DockerImage',
                                'name': f'quay-proxy.ci.openshift.org/openshift/ci@{qci_digest}',
                            },
                            'reference': True,
                            'referencePolicy': {'type': 'Source'},
                            'importPolicy': {'importMode': 'PreserveOriginal'},
                        }
                    }

                    # Get current imagestream to find tag index
                    get_istag_cmd = f'oc get imagestream {dest_imagestream} -n {dest_ns} -o json'
                    istag_stdout, _ = exectools.cmd_assert(get_istag_cmd, retries=3)
                    imagestream_data = json.loads(istag_stdout)
                    existing_tags = imagestream_data.get('spec', {}).get('tags', [])
                    tag_index = next((i for i, t in enumerate(existing_tags) if t.get('name') == dest_tag), None)

                    if tag_index is not None:
                        patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "replace", "path": "/spec/tags/{tag_index}", "value": {json.dumps(istag_patch["tag"])}}}]\''
                    else:
                        patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "add", "path": "/spec/tags/-", "value": {json.dumps(istag_patch["tag"])}}}]\''

                    exectools.cmd_assert(patch_cmd, retries=3)
                    print(
                        f'For {upstream_entry_name}, updated imagestream {dest_ns}/{dest_istag} to reference QCI@{qci_digest}'
                    )
                else:
                    runtime.logger.warning(f'Failed to get digest for {floating_qci_dest}')

        else:
            # Not a CI imagestream - mirror directly to destination
            full_cmd_1 = f'{cmd_start} {upstream_dest}'
            if dry_run:
                print(f'For {upstream_entry_name}, would have run: {full_cmd_1}')
            else:
                # Try to mirror - if source doesn't exist, log warning and skip
                rc, stdout, stderr = exectools.cmd_gather(full_cmd_1, realtime=True)
                if rc != 0:
                    if 'manifest unknown' in stderr or 'not found' in stderr:
                        runtime.logger.warning(
                            f'Source image does not exist for {upstream_entry_name}, skipping: {stderr.strip()}'
                        )
                        print(f'For {upstream_entry_name}, source image not found - skipping')
                        return
                    else:
                        # Other error - raise it
                        raise ChildProcessError(
                            f'Failed to mirror {upstream_entry_name}: {stderr}', (rc, stdout, stderr)
                        )

    upstreaming_entries = _get_upstreaming_entries(runtime, streams, image_names=images)

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

            # Extract digest from source pullspec when available (e.g. Konflux @sha256: refs).
            # oc image mirror preserves manifest bytes, so source digest == destination digest.
            src_digest = src_image_pullspec.split('@')[1] if '@' in src_image_pullspec else None

            # Mirror to main destination only if not in only-if-missing mode OR destination doesn't exist
            if not only_if_missing or not destinations_to_check.get(upstream_dest, False):
                mirror_image(cmd, upstream_dest, source_digest=src_digest)

            # mirror arm64 builder and base images for CI
            if mirror_arm:
                # oc image mirror will filter out missing arches (as long as the manifest is there) regardless of specifying --skip-missing
                arm_cmd = f'oc image mirror --filter-by-os linux/arm64 {src_image_pullspec}'
                if registry_config_file is not None:
                    arm_cmd += f" --registry-config={registry_config_file}"
                # Mirror ARM64 only if not in only-if-missing mode OR destination doesn't exist
                # ARM64 filter produces a single-arch image, not the original manifest list, so don't pass source_digest
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
    'check-upstream',
    short_help='Check and sync QCI images to imagestreams (mirrors to GC-prevention tags and updates references)',
)
@click.option(
    '--stream',
    'streams',
    metavar='STREAM_NAME',
    default=[],
    multiple=True,
    help='If specified, only check these streams',
)
@click.option(
    '--image',
    'images',
    metavar='IMAGE_NAME',
    default=[],
    multiple=True,
    help='If specified, only check these image distgit keys (must have ci_alignment.upstream_image).',
)
@click.option('--live-test-mode', default=False, is_flag=True, help='Scan for live-test mode buildconfigs')
@click.option('--dry-run', default=False, is_flag=True, help='Do not actually mirror or update imagestreams')
@option_registry_auth
@pass_runtime
def images_streams_check_upstream(runtime, streams, images, live_test_mode, dry_run, registry_auth: Optional[str]):
    runtime.initialize(clone_distgits=False, clone_source=False, prevent_cloning=True)

    # Determine which registry config to use
    registry_config_file = None
    if registry_auth:
        registry_config_file = registry_auth
    elif runtime.registry_config_dir is not None:
        registry_config_file = get_docker_config_json(runtime.registry_config_dir)

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']

    istags_status = []
    qci_status = []

    upstreaming_entries = _get_upstreaming_entries(runtime, streams, image_names=images)

    for upstream_entry_name, config in upstreaming_entries.items():
        upstream_dest = config.upstream_image
        openshift_imagestream_prefixes = (
            'registry.ci.openshift.org/',
            'registry.svc.ci.openshift.org/',
        )
        if not upstream_dest.startswith(openshift_imagestream_prefixes):
            istags_status.append(f'SKIP: {upstream_entry_name}\nNot an OpenShift imagestream target: {upstream_dest}')
            continue

        _, dest_ns, dest_istag = upstream_dest.rsplit('/', maxsplit=2)

        if live_test_mode:
            dest_istag += '.test'

        dest_imagestream, dest_tag = dest_istag.split(':')

        # Check imagestream tag exists and inspect its configuration
        rc, stdout, stderr = exectools.cmd_gather(f'oc get -n {dest_ns} istag {dest_istag} -o json')
        if rc:
            istags_status.append(
                f'ERROR: {upstream_entry_name}\nImagestream tag does not exist: {dest_ns}/istag/{dest_istag}'
            )
        else:
            try:
                istag_data = json.loads(stdout)
                # Check if it's referencing QCI by sha256
                image_ref = istag_data.get('tag', {}).get('from', {}).get('name', '')
                ref_policy = istag_data.get('tag', {}).get('referencePolicy', {}).get('type', '')

                if image_ref.startswith('quay.io/openshift/ci@sha256:'):
                    if ref_policy == 'Source':
                        istags_status.append(
                            f'OK: {upstream_entry_name}\n  Imagestream: {dest_ns}/{dest_istag}\n  Reference: {image_ref}\n  Policy: {ref_policy}'
                        )
                    else:
                        istags_status.append(
                            f'WARNING: {upstream_entry_name}\n  Imagestream: {dest_ns}/{dest_istag}\n  Reference: {image_ref}\n  Policy: {ref_policy} (expected "Source")'
                        )
                else:
                    istags_status.append(
                        f'WARNING: {upstream_entry_name}\n  Imagestream: {dest_ns}/{dest_istag}\n  Reference: {image_ref} (expected quay.io/openshift/ci@sha256:...)\n  Policy: {ref_policy}'
                    )
            except Exception as e:
                istags_status.append(f'ERROR: {upstream_entry_name}\n  Failed to parse imagestream tag: {e}')

        # Check QCI tag exists and sync if needed (only for entries with transforms, which have buildconfigs)
        transform = config.transform
        if transform is not Missing and transform in transforms:
            qci_tag = f'art-builder-{major}.{minor}-{dest_tag}'
            qci_pullspec = f'quay.io/openshift/ci:{qci_tag}'

            # Get digest (handles manifest lists)
            current_digest = get_image_digest(qci_pullspec, registry_config_file)

            if not current_digest:
                qci_status.append(f'ERROR: {upstream_entry_name}\n  QCI image does not exist: {qci_pullspec}')
            else:
                # Check if imagestream already points to this digest
                needs_sync = False
                try:
                    rc_istag, istag_stdout, istag_stderr = exectools.cmd_gather(
                        f'oc get -n {dest_ns} istag {dest_istag} -o json'
                    )
                    if rc_istag == 0:
                        try:
                            istag_data = json.loads(istag_stdout)
                            istag_ref = istag_data.get('tag', {}).get('from', {}).get('name', '')
                            # Extract digest from imagestream reference (quay-proxy.ci.openshift.org/openshift/ci@sha256:...)
                            if '@' in istag_ref:
                                istag_digest = istag_ref.split('@')[1]
                                if istag_digest != current_digest:
                                    needs_sync = True
                            else:
                                needs_sync = True  # Not using digest reference
                        except json.JSONDecodeError:
                            needs_sync = True  # Can't parse, assume needs sync
                    else:
                        needs_sync = True  # Imagestream tag doesn't exist

                    if needs_sync:
                        qci_status.append(
                            f'SYNCING: {upstream_entry_name}\n  QCI image: {qci_pullspec}\n  Digest: {current_digest}'
                        )

                        # Mirror to GC-prevention tag
                        gc_prevention_tag = f'art-builder-{current_digest[7:23]}'  # sha256:abc... -> first 16 chars
                        gc_prevention_pullspec = f'quay.io/openshift/ci:{gc_prevention_tag}'

                        if dry_run:
                            qci_status.append(
                                f'  DRY RUN: Would mirror to {gc_prevention_pullspec} and update imagestream'
                            )
                        else:
                            # Perform mirror
                            mirror_cmd = f'oc image mirror {qci_pullspec}@{current_digest} {gc_prevention_pullspec}'
                            if registry_config_file:
                                mirror_cmd += f' --registry-config={registry_config_file}'

                            try:
                                exectools.cmd_assert(mirror_cmd, retries=3)
                                qci_status.append(f'  Mirrored to {gc_prevention_pullspec}')

                                # Update imagestream
                                istag_patch = {
                                    'tag': {
                                        'name': dest_tag,
                                        'from': {
                                            'kind': 'DockerImage',
                                            'name': f'quay-proxy.ci.openshift.org/openshift/ci@{current_digest}',
                                        },
                                        'reference': True,
                                        'referencePolicy': {'type': 'Source'},
                                        'importPolicy': {'importMode': 'PreserveOriginal'},
                                    }
                                }

                                # Get current imagestream to find tag index
                                get_istag_cmd = f'oc get imagestream {dest_imagestream} -n {dest_ns} -o json'
                                istag_get_stdout, _ = exectools.cmd_assert(get_istag_cmd, retries=3)
                                imagestream_data = json.loads(istag_get_stdout)
                                existing_tags = imagestream_data.get('spec', {}).get('tags', [])
                                tag_index = next(
                                    (i for i, t in enumerate(existing_tags) if t.get('name') == dest_tag), None
                                )

                                if tag_index is not None:
                                    patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "replace", "path": "/spec/tags/{tag_index}", "value": {json.dumps(istag_patch["tag"])}}}]\''
                                else:
                                    patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "add", "path": "/spec/tags/-", "value": {json.dumps(istag_patch["tag"])}}}]\''

                                exectools.cmd_assert(patch_cmd, retries=3)
                                qci_status.append(f'  Updated imagestream {dest_ns}/{dest_istag}')

                            except Exception as e:
                                qci_status.append(f'  ERROR during sync: {e}')
                    else:
                        qci_status.append(
                            f'OK: {upstream_entry_name}\n  QCI image: {qci_pullspec}\n  Digest: {current_digest}\n  Imagestream is up-to-date'
                        )

                except json.JSONDecodeError:
                    qci_status.append(
                        f'WARNING: {upstream_entry_name}\n  QCI image exists but could not parse info: {qci_pullspec}'
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
    print()
    print('Recent builds:')
    print(builds_stdout or builds_stderr)
    print()

    print('QCI image status:')
    for status in qci_status:
        print(status)
        print()

    print('Imagestream tag status:')
    for istag_status in istags_status:
        print(istag_status)
        print()


def get_eligible_buildconfigs(runtime, streams, live_test_mode, images=()):
    upstreaming_entries = _get_upstreaming_entries(runtime, streams, image_names=images)
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
    '--image',
    'images',
    metavar='IMAGE_NAME',
    default=[],
    multiple=True,
    help='If specified, only these image distgit keys will be processed (must have ci_alignment.upstream_image).',
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
@option_registry_auth
@pass_runtime
def images_streams_start_buildconfigs(
    runtime, streams, images, as_user, live_test_mode, dry_run, registry_auth: Optional[str]
):
    runtime.initialize(clone_distgits=False, clone_source=False, prevent_cloning=True)

    # Determine which registry config to use
    registry_config_file = None
    if registry_auth:
        registry_config_file = registry_auth
    elif runtime.registry_config_dir is not None:
        registry_config_file = get_docker_config_json(runtime.registry_config_dir)

    group_label = runtime.group_config.name
    if live_test_mode:
        group_label += '.test'

    cmd = f'oc -n ci get -o=json buildconfigs -l art-builder-group={group_label}'
    if as_user:
        cmd += f' --as {as_user}'
    bc_stdout, bc_stderr = exectools.cmd_assert(cmd, retries=3)
    bc_stdout = bc_stdout.strip()

    if not bc_stdout:
        print(f'No buildconfigs associated with this group: {group_label}')
        return

    buildconfigs = json.loads(bc_stdout).get('items', [])
    eligible_bc_names = get_eligible_buildconfigs(runtime, streams, live_test_mode, images=images)

    # Track triggered builds for post-build preservation
    triggered_builds = []

    for bc in buildconfigs:
        bc_name = bc['metadata']['name']

        if bc_name not in eligible_bc_names:
            print(f'Skipping outdated buildconfig: {bc_name}')
            continue

        # Extract metadata for pre-build operations
        labels = bc['metadata'].get('labels', {})
        annotations = bc['metadata'].get('annotations', {})
        qci_tag = labels.get('art-builder-qci-tag')
        dest_ns = annotations.get('art-builder-imagestream-namespace')
        dest_imagestream = annotations.get('art-builder-imagestream-name')
        dest_tag = annotations.get('art-builder-imagestream-tag')

        if not qci_tag:
            runtime.logger.warning(f'BuildConfig {bc_name} missing art-builder-qci-tag label; skipping pre-build steps')
        else:
            qci_pullspec = f'quay.io/openshift/ci:{qci_tag}'

            # Step 0: FIRST, preserve the current registry.ci.openshift.org imagestream content
            # This ensures we don't lose the old image if something goes wrong during migration
            if dest_ns and dest_imagestream and dest_tag:
                old_istag_pullspec = f'registry.ci.openshift.org/{dest_ns}/{dest_imagestream}:{dest_tag}'
                print(f'Preserving current imagestream content from {old_istag_pullspec}')

                if dry_run:
                    print('  DRY RUN: Would check and preserve old imagestream content')
                else:
                    # Get the current digest from the old imagestream tag (handles manifest lists)
                    old_digest = get_image_digest(old_istag_pullspec, registry_config_file)
                    if old_digest:
                        print(f'  Current imagestream digest: {old_digest}')

                        # Mirror to a preservation tag to prevent garbage collection
                        old_gc_prevention_tag = f'art-builder-{old_digest.replace("sha256:", "")[:16]}'
                        old_gc_prevention_pullspec = f'quay.io/openshift/ci:{old_gc_prevention_tag}'

                        print(f'  Mirroring old content to GC-prevention tag: {old_gc_prevention_pullspec}')
                        old_mirror_cmd = (
                            f'oc image mirror {old_istag_pullspec}@{old_digest} {old_gc_prevention_pullspec}'
                        )
                        if registry_config_file:
                            old_mirror_cmd += f' --registry-config={registry_config_file}'

                        # CRITICAL: This MUST succeed before we proceed
                        exectools.cmd_assert(old_mirror_cmd, retries=3)
                        print(f'  Successfully preserved old imagestream content to {old_gc_prevention_pullspec}')
                    else:
                        print(f'  Old imagestream tag does not exist at {old_istag_pullspec} (initial setup)')

            # Step 1: Get current digest of the QCI tag (if it exists)
            print(f'Checking current digest of {qci_pullspec}')
            current_digest = None

            if dry_run:
                print('  DRY RUN: Simulating existing image for testing')
                current_digest = 'sha256:0000000000000000000000000000000000000000000000000000000000000000'
            else:
                # Try to get image info (handles manifest lists)
                current_digest = get_image_digest(qci_pullspec, registry_config_file)
                if not current_digest:
                    # Image does not exist yet - this is expected during initial migration
                    print(f'  Image does not exist yet at {qci_pullspec} (first build for this buildconfig)')

            if current_digest:
                print(f'  Current digest: {current_digest}')

                # Step 2: Mirror current image to GC-prevention tag
                # CRITICAL: If image exists, this step MUST succeed before proceeding
                gc_prevention_tag = f'art-builder-{current_digest.replace("sha256:", "")[:16]}'
                gc_prevention_pullspec = f'quay.io/openshift/ci:{gc_prevention_tag}'

                print(f'Mirroring to GC-prevention tag: {gc_prevention_pullspec}')
                mirror_cmd = f'oc image mirror {qci_pullspec}@{current_digest} {gc_prevention_pullspec}'
                if registry_config_file:
                    mirror_cmd += f' --registry-config={registry_config_file}'

                if dry_run:
                    print(f'  Would have run: {mirror_cmd}')
                else:
                    # Use cmd_assert to fail fast if mirroring fails
                    exectools.cmd_assert(mirror_cmd, retries=3)
                    print(f'  Successfully mirrored to {gc_prevention_pullspec}')

                # Step 3: Update imagestream tag to reference QCI by sha256
                # CRITICAL: If image exists, this step MUST succeed before proceeding
                if dest_ns and dest_imagestream and dest_tag:
                    istag_name = f'{dest_imagestream}:{dest_tag}'
                    print(
                        f'Updating imagestream tag {dest_ns}/{istag_name} to reference quay-proxy.ci.openshift.org/openshift/ci@{current_digest}'
                    )

                    # Build imagestream tag patch
                    # Use quay-proxy for better performance/caching in CI cluster
                    istag_patch = {
                        'tag': {
                            'name': dest_tag,
                            'from': {
                                'kind': 'DockerImage',
                                'name': f'quay-proxy.ci.openshift.org/openshift/ci@{current_digest}',
                            },
                            'reference': True,
                            'referencePolicy': {'type': 'Source'},
                            'importPolicy': {'importMode': 'PreserveOriginal'},
                        }
                    }

                    if dry_run:
                        print(f'  Would have updated imagestream tag {dest_ns}/{istag_name}')
                    else:
                        # First check if tag exists, if so use replace instead of add
                        get_istag_cmd = f'oc get imagestream {dest_imagestream} -n {dest_ns} -o json'
                        if as_user:
                            get_istag_cmd += f' --as {as_user}'

                        istag_stdout, _ = exectools.cmd_assert(get_istag_cmd, retries=3)
                        imagestream_data = json.loads(istag_stdout)
                        existing_tags = imagestream_data.get('spec', {}).get('tags', [])
                        tag_index = next((i for i, t in enumerate(existing_tags) if t.get('name') == dest_tag), None)

                        if tag_index is not None:
                            # Tag exists, use replace
                            patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "replace", "path": "/spec/tags/{tag_index}", "value": {json.dumps(istag_patch["tag"])}}}]\''
                        else:
                            # Tag doesn't exist, use add
                            patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "add", "path": "/spec/tags/-", "value": {json.dumps(istag_patch["tag"])}}}]\''

                        if as_user:
                            patch_cmd += f' --as {as_user}'

                        # Use cmd_assert to fail fast if imagestream update fails
                        exectools.cmd_assert(patch_cmd, retries=3)
                        print('  Successfully updated imagestream tag')
            else:
                # No existing QCI image - bootstrap from old imagestream if available
                print('  QCI floating tag does not exist yet (initial migration)')

                if dest_ns and dest_imagestream and dest_tag:
                    old_istag_pullspec = f'registry.ci.openshift.org/{dest_ns}/{dest_imagestream}:{dest_tag}'
                    print(f'  Attempting to bootstrap QCI tag from old imagestream: {old_istag_pullspec}')

                    if dry_run:
                        print('  DRY RUN: Would bootstrap QCI tag from old imagestream')
                    else:
                        # Try to get the old imagestream tag
                        old_image_info_cmd = f'oc image info {old_istag_pullspec} -o json'
                        if registry_config_file:
                            old_image_info_cmd += f' --registry-config={registry_config_file}'

                        rc, old_info_stdout, old_info_stderr = exectools.cmd_gather(old_image_info_cmd)
                        if rc == 0:
                            try:
                                old_image_info = json.loads(old_info_stdout)
                                bootstrap_digest = old_image_info.get('digest', '')
                                if bootstrap_digest:
                                    print(f'  Found old imagestream with digest: {bootstrap_digest}')

                                    # Bootstrap the QCI floating tag from the old imagestream
                                    print(f'  Bootstrapping QCI tag: {qci_pullspec}')
                                    bootstrap_mirror_cmd = (
                                        f'oc image mirror {old_istag_pullspec}@{bootstrap_digest} {qci_pullspec}'
                                    )
                                    if registry_config_file:
                                        bootstrap_mirror_cmd += f' --registry-config={registry_config_file}'

                                    # CRITICAL: Bootstrap must succeed to have a starting point
                                    exectools.cmd_assert(bootstrap_mirror_cmd, retries=3)
                                    print(f'  Successfully bootstrapped {qci_pullspec}')

                                    # Also create GC-prevention tag for the bootstrapped image
                                    bootstrap_gc_tag = f'art-builder-{bootstrap_digest.replace("sha256:", "")[:16]}'
                                    bootstrap_gc_pullspec = f'quay.io/openshift/ci:{bootstrap_gc_tag}'
                                    print(f'  Creating GC-prevention tag: {bootstrap_gc_pullspec}')
                                    gc_mirror_cmd = (
                                        f'oc image mirror {qci_pullspec}@{bootstrap_digest} {bootstrap_gc_pullspec}'
                                    )
                                    if registry_config_file:
                                        gc_mirror_cmd += f' --registry-config={registry_config_file}'
                                    exectools.cmd_assert(gc_mirror_cmd, retries=3)
                                    print('  Successfully created GC-prevention tag')

                                    # Update imagestream to reference the bootstrapped QCI image
                                    istag_name = f'{dest_imagestream}:{dest_tag}'
                                    print(
                                        f'  Updating imagestream tag {dest_ns}/{istag_name} to reference quay-proxy.ci.openshift.org/openshift/ci@{bootstrap_digest}'
                                    )

                                    istag_patch = {
                                        'tag': {
                                            'name': dest_tag,
                                            'from': {
                                                'kind': 'DockerImage',
                                                'name': f'quay-proxy.ci.openshift.org/openshift/ci@{bootstrap_digest}',
                                            },
                                            'reference': True,
                                            'referencePolicy': {'type': 'Source'},
                                            'importPolicy': {'importMode': 'PreserveOriginal'},
                                        }
                                    }

                                    # Check if tag exists, use replace or add
                                    get_istag_cmd = f'oc get imagestream {dest_imagestream} -n {dest_ns} -o json'
                                    if as_user:
                                        get_istag_cmd += f' --as {as_user}'

                                    istag_stdout, _ = exectools.cmd_assert(get_istag_cmd, retries=3)
                                    imagestream_data = json.loads(istag_stdout)
                                    existing_tags = imagestream_data.get('spec', {}).get('tags', [])
                                    tag_index = next(
                                        (i for i, t in enumerate(existing_tags) if t.get('name') == dest_tag), None
                                    )

                                    if tag_index is not None:
                                        patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "replace", "path": "/spec/tags/{tag_index}", "value": {json.dumps(istag_patch["tag"])}}}]\''
                                    else:
                                        patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "add", "path": "/spec/tags/-", "value": {json.dumps(istag_patch["tag"])}}}]\''

                                    if as_user:
                                        patch_cmd += f' --as {as_user}'

                                    exectools.cmd_assert(patch_cmd, retries=3)
                                    print('  Successfully updated imagestream tag to bootstrapped QCI image')
                                else:
                                    runtime.logger.warning(f'No digest found in old imagestream {old_istag_pullspec}')
                                    print('  WARNING: Could not extract digest from old imagestream')
                                    print(f'  The buildconfig will create the initial image at {qci_pullspec}')
                            except json.JSONDecodeError as e:
                                runtime.logger.warning(f'Failed to parse old imagestream info: {e}')
                                print('  WARNING: Could not parse old imagestream info')
                                print(f'  The buildconfig will create the initial image at {qci_pullspec}')
                        else:
                            # Old imagestream doesn't exist either - truly initial setup
                            print(f'  Old imagestream tag does not exist at {old_istag_pullspec}')
                            print(f'  The buildconfig will create the initial image at {qci_pullspec}')
                else:
                    print(f'  The buildconfig will create the initial image at {qci_pullspec}')

        # Finally, trigger the build
        # NOTE: We only reach here if:
        # 1. No image existed (initial migration), OR
        # 2. Image existed AND mirroring AND imagestream update both succeeded
        print(f'Triggering build: {bc_name}')
        cmd = f'oc -n ci start-build {bc_name}'
        if as_user:
            cmd += f' --as {as_user}'
        if dry_run:
            print(f'Would have run: {cmd}')
            triggered_builds.append((bc_name, qci_pullspec, dest_ns, dest_imagestream, dest_tag))
        else:
            stdout, stderr = exectools.cmd_assert(cmd, retries=3)
            print('   ' + stdout or stderr)
            # Track build for post-build preservation
            triggered_builds.append((bc_name, qci_pullspec, dest_ns, dest_imagestream, dest_tag))

    # Post-build preservation: wait for builds and preserve newly built images
    if triggered_builds and not dry_run:
        print('\n=== Post-Build Preservation ===')
        print(f'Waiting for {len(triggered_builds)} builds to complete...')

        import time

        overall_timeout = 3600  # 1 hour total for all builds
        overall_start_time = time.time()

        for bc_name, qci_pullspec, dest_ns, dest_imagestream, dest_tag in triggered_builds:
            # Check if we've exceeded overall timeout
            elapsed = time.time() - overall_start_time
            remaining = overall_timeout - elapsed
            if remaining <= 0:
                runtime.logger.warning(f'Overall timeout ({overall_timeout}s) exceeded, skipping remaining builds')
                print(
                    f'\nOverall wait timeout exceeded, skipping remaining {len(triggered_builds) - triggered_builds.index((bc_name, qci_pullspec, dest_ns, dest_imagestream, dest_tag))} builds'
                )
                break

            print(f'\nWaiting for build: {bc_name} (timeout remaining: {int(remaining)}s)')

            # Get the latest build for this buildconfig and wait for it
            # First, get the latest build name
            get_build_cmd = f'oc -n ci get builds -l buildconfig={bc_name} --sort-by={{.metadata.creationTimestamp}} -o jsonpath={{.items[-1].metadata.name}}'
            if as_user:
                get_build_cmd += f' --as {as_user}'

            try:
                build_name, _ = exectools.cmd_assert(get_build_cmd, retries=1)
                build_name = build_name.strip()
                if not build_name:
                    runtime.logger.warning(f'No build found for buildconfig {bc_name}')
                    continue

                print(f'  Waiting for build {build_name}...')

                # Wait for the specific build to complete or fail
                # Use remaining time from overall timeout
                wait_cmd = f"oc -n ci wait --for=jsonpath='{{.status.phase}}'=Complete --for=jsonpath='{{.status.phase}}'=Failed --timeout={int(remaining)}s build/{build_name}"
                if as_user:
                    wait_cmd += f' --as {as_user}'

                rc, stdout, stderr = exectools.cmd_gather(wait_cmd)
                if rc != 0:
                    runtime.logger.warning(f'Build {build_name} did not complete: {stderr}')
                    continue

                # Check if build succeeded or failed
                check_phase_cmd = f'oc -n ci get build/{build_name} -o jsonpath={{.status.phase}}'
                if as_user:
                    check_phase_cmd += f' --as {as_user}'

                phase_stdout, _ = exectools.cmd_assert(check_phase_cmd, retries=1)
                phase = phase_stdout.strip()

                if phase != 'Complete':
                    runtime.logger.warning(
                        f'Build {build_name} did not complete successfully (phase: {phase}), skipping preservation'
                    )
                    continue

                print(f'  Build {bc_name} completed successfully')

                # Now preserve the newly built image
                print(f'  Preserving newly built image at {qci_pullspec}')

                # Get the new digest (handles manifest lists)
                new_digest = get_image_digest(qci_pullspec, registry_config_file)

                if new_digest:
                    # Mirror to GC-prevention tag
                    gc_prevention_tag = f'art-builder-{new_digest[7:23]}'  # sha256:abc... -> abc (first 16 chars)
                    gc_prevention_pullspec = f'quay.io/openshift/ci:{gc_prevention_tag}'

                    print(f'  Mirroring to GC-prevention tag: {gc_prevention_pullspec}')
                    mirror_cmd = f'oc image mirror {qci_pullspec}@{new_digest} {gc_prevention_pullspec}'
                    if registry_config_file:
                        mirror_cmd += f' --registry-config={registry_config_file}'

                    exectools.cmd_assert(mirror_cmd, retries=3)
                    print(f'  Successfully mirrored to {gc_prevention_pullspec}')

                    # Update imagestream to reference the new image
                    if dest_ns and dest_imagestream and dest_tag:
                        istag_name = f'{dest_imagestream}:{dest_tag}'
                        print(f'  Updating imagestream tag {dest_ns}/{istag_name} to reference new build')

                        istag_patch = {
                            'tag': {
                                'name': dest_tag,
                                'from': {
                                    'kind': 'DockerImage',
                                    'name': f'quay-proxy.ci.openshift.org/openshift/ci@{new_digest}',
                                },
                                'reference': True,
                                'referencePolicy': {'type': 'Source'},
                                'importPolicy': {'importMode': 'PreserveOriginal'},
                            }
                        }

                        # Get current imagestream to find tag index
                        get_istag_cmd = f'oc get imagestream {dest_imagestream} -n {dest_ns} -o json'
                        if as_user:
                            get_istag_cmd += f' --as {as_user}'

                        istag_stdout, _ = exectools.cmd_assert(get_istag_cmd, retries=3)
                        imagestream_data = json.loads(istag_stdout)
                        existing_tags = imagestream_data.get('spec', {}).get('tags', [])
                        tag_index = next((i for i, t in enumerate(existing_tags) if t.get('name') == dest_tag), None)

                        if tag_index is not None:
                            patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "replace", "path": "/spec/tags/{tag_index}", "value": {json.dumps(istag_patch["tag"])}}}]\''
                        else:
                            patch_cmd = f'oc patch imagestream {dest_imagestream} -n {dest_ns} --type=json -p \'[{{"op": "add", "path": "/spec/tags/-", "value": {json.dumps(istag_patch["tag"])}}}]\''

                        if as_user:
                            patch_cmd += f' --as {as_user}'

                        exectools.cmd_assert(patch_cmd, retries=3)
                        print('  Successfully updated imagestream tag')
                else:
                    runtime.logger.warning(f'No digest found for {qci_pullspec}')

            except Exception as e:
                runtime.logger.error(f'Failed to wait for or preserve build {bc_name}: {e}')
                # Continue with other builds even if one fails
                continue

        print('\n=== Post-Build Preservation Complete ===')


def _get_upstreaming_entry_for_image(runtime, image_meta):
    """
    Build an upstreaming entry from an image meta's ci_alignment config.
    Returns the entry Model, or None if the image should be skipped (e.g. no build yet).
    """
    upstream_entry = Model(dict_to_model=image_meta.config.content.source.ci_alignment.primitive())

    try:
        upstream_entry['image'] = image_meta.pull_url()
    except IOError as e:
        runtime.logger.warning(f'Skipping {image_meta.distgit_key} for CI alignment: {e}')
        return None

    if upstream_entry.final_user is Missing:
        upstream_entry.final_user = image_meta.config.final_stage_user
    return upstream_entry


def _get_upstreaming_entries(runtime, stream_names=None, image_names=None):
    """
    Looks through streams.yml entries and each image metadata for upstream
    transform information.
    :param runtime: doozer runtime
    :param stream_names: A list of streams to look for in streams.yml. If None or empty, all will be searched.
    :param image_names: A list of image distgit keys to include. Each must have ci_alignment.upstream_image set.
    :return: Returns a map[name] => { transform: '...', upstream_image.... } where name is a stream name or distgit key.
    """

    fetch_all_image_metas = False
    if not stream_names and not image_names:
        stream_names = runtime.get_stream_names()
        fetch_all_image_metas = True

    upstreaming_entries = {}

    if stream_names:
        streams_config = runtime.streams
        for stream in stream_names:
            config = streams_config[stream]
            if config is Missing:
                raise IOError(f'Did not find stream {stream} in streams.yml for this group')
            if config.upstream_image is not Missing:
                upstreaming_entries[stream] = streams_config[stream]

    if fetch_all_image_metas:
        for image_meta in runtime.ordered_image_metas():
            if image_meta.config.content.source.ci_alignment.upstream_image is not Missing:
                entry = _get_upstreaming_entry_for_image(runtime, image_meta)
                if entry is not None:
                    upstreaming_entries[image_meta.distgit_key] = entry

    elif image_names:
        for name in image_names:
            meta = runtime.image_map.get(name)
            if not meta:
                raise IOError(f'Image {name} not found in group metadata')
            if meta.config.content.source.ci_alignment.upstream_image is Missing:
                raise IOError(f'Image {name} does not have ci_alignment.upstream_image set; not eligible for CI sync')
            entry = _get_upstreaming_entry_for_image(runtime, meta)
            if entry is not None:
                upstreaming_entries[name] = entry

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
    '--image',
    'images',
    metavar='IMAGE_NAME',
    default=[],
    multiple=True,
    help='If specified, only these image distgit keys will be processed (must have ci_alignment.upstream_image).',
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
def images_streams_gen_buildconfigs(runtime, streams, images, output, as_user, apply, live_test_mode):
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
    runtime.initialize(clone_distgits=False, clone_source=False, prevent_cloning=True)
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

    upstreaming_entries = _get_upstreaming_entries(runtime, streams, image_names=images)

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

        # Construct QCI destination tag: quay.io/openshift/ci:art-builder-{MAJOR}.{MINOR}-{tag}
        # Example: quay.io/openshift/ci:art-builder-4.19-base-rhel9
        qci_tag = f'art-builder-{major}.{minor}-{dest_tag}'
        qci_destination = f'quay.io/openshift/ci:{qci_tag}'

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
                    'art-builder-qci-tag': qci_tag,  # Store QCI tag for use by start-builds
                },
                'annotations': {
                    'description': 'Generated by the ART pipeline by doozer. Processes raw ART images into ART equivalent images for CI.',
                    'art-builder-imagestream-namespace': dest_ns,  # Store for imagestream updates
                    'art-builder-imagestream-name': dest_imagestream,  # Store for imagestream updates
                    'art-builder-imagestream-tag': dest_tag,  # Store for imagestream updates
                },
            },
            'spec': {
                'failedBuildsHistoryLimit': 2,
                'output': {
                    'to': {
                        'kind': 'DockerImage',
                        'name': qci_destination,
                    },
                    'pushSecret': {
                        'name': 'registry-push-credentials-quay-qci-art-builder',
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
    'list',
    short_help='List all open reconciliation prs for upstream repos (requires GitHub App credentials or GITHUB_TOKEN).',
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
    runtime.initialize(clone_distgits=False, clone_source=False, prevent_cloning=True)
    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']
    retdata = {}
    github_client = get_github_client_for_org("openshift")
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

        @retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
        def search_issues(query):
            return jira_client.search_issues(query)

        # Tier A: check if the PR title already references a Jira issue for this project.
        # connect_issue_with_pr prepends the issue key to the PR title, so this is the
        # fastest and most reliable way to find existing issues.
        found_issue = None
        title_issue_keys = re.findall(r'([A-Z][A-Z0-9]+-\d+)', pr.title)
        for candidate_key in title_issue_keys:
            if not candidate_key.startswith(f'{project}-'):
                continue
            try:
                candidate_issue = jira_client.issue(candidate_key)
                if candidate_issue.fields.summary in (summary, old_summary_format):
                    found_issue = candidate_issue
                    break
            except JIRAError as err:
                if getattr(err, 'status_code', None) == 404:
                    continue
                runtime.logger.warning('Failed to fetch Jira issue %s: %s', candidate_key, err)
                raise

        # Tier B: fall back to JQL search by summary and labels, without requiring
        # remote links (which depend on external systems and may be broken).
        if not found_issue:
            query = (
                f'project={project} AND '
                f'( summary ~ "{summary}" OR summary ~ "{old_summary_format}" ) AND '
                f'labels = "art:reconciliation" AND '
                f'labels = "art:package:{image_meta.get_component_name()}"'
            )
            candidates = [i for i in search_issues(query) if i.fields.summary in (summary, old_summary_format)]
            for candidate in candidates:
                if candidate.fields.description and pr.html_url in candidate.fields.description:
                    found_issue = candidate
                    break

        if found_issue:
            print(f'A JIRA issue is already open for {pr.html_url}: {found_issue.key}')
            existing_issues[distgit_key] = found_issue
            if not dry_run:
                connect_issue_with_pr(pr, found_issue.key)
            continue

        description = f'''
Please review the following PR: {pr.html_url}

The PR has been automatically opened by ART (#forum-ocp-art) team automation and indicates
that the image(s) being used downstream for production builds are not consistent
with the images referenced in this component's github repository.

Differences in upstream and downstream builds impact the fidelity of your CI signal.

If you disagree with the content of this PR, please reach out
in #forum-ocp-art to discuss the discrepancy.

Closing this issue without addressing the difference will cause the issue to
be reopened automatically.

Once the differences have been resolved (either by merging this pull or updating the
downstream production build configuration), ART will automatically move this issue
from ON_QA to Closed (at some point during the release's Release Candidate phase).
There is no need for QE verification beyond the existing CI signal this change helps
improve.
'''
        if potential_project != project or potential_component != component:
            description += f'''

Important: ART has recorded in their product data that bugs for
this component should be opened against Jira project "{potential_project}" and
component "{potential_component}". This project or component does not exist. Jira
should either be updated to include this component or the ART team should be
notified of the proper mapping in the #forum-ocp-art Slack channel.

Component name: {image_meta.get_component_name()} .
Jira mapping: https://github.com/openshift-eng/ocp-build-data/blob/main/product.yml
'''
        elif potential_project == 'Unknown':
            description += f'''

Important: ART maintains a mapping of OpenShift software components to
Jira components. This component does not currently have an entry defined
within that data.
Contact the ART team in the #forum-ocp-art Slack channel to inform them of
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
            "security": {"name": "Red Hat Employee"},
        }

        if not dry_run:
            issue = jira_client.create_issue(
                fields,
            )
            try:
                # retrieve the target version string (e.g., 'z' or '4.21.0')
                target_version_segment = Model(
                    runtime.gitdata.load_data(key='bug', replace_vars=runtime.group_config.vars).data
                ).target_release[-1]

                # Build the update payload using the retrieved string
                issue_update = {
                    JIRABugTracker.field_target_version: [{'name': target_version_segment}],
                }
                runtime.logger.info(
                    f"Attempting to update issue {issue.key} Target Version to: {target_version_segment}"
                )
                issue.update(fields=issue_update)
                runtime.logger.info(f"Successfully updated Target Version for issue {issue.key}.")

            except Exception as e:
                runtime.logger.error(f"An error occurred while updating the Target Version on issue {issue.key}: {e}")

            # check depend issues and set depend to a higher version issue if true
            next_major, next_minor = get_reconciliation_depend_version(major, minor)
            look_for_summary = f'Update {next_major}.{next_minor} {image_meta.name} image to be consistent with ART'
            depend_issues = search_issues(f"project={project} AND summary ~ '{look_for_summary}'")
            # jira title search is fuzzy, so we need to check if an issue is really the one we want
            depend_issues = [i for i in depend_issues if i.fields.summary == look_for_summary]
            if depend_issues:
                jira_client.create_issue_link("Depend", issue.key, depend_issues[0].key)
            new_issues[distgit_key] = issue
            print(f'A JIRA issue has been opened for {pr.html_url}: {issue.key}')
            connect_issue_with_pr(pr, issue.key)
            try:
                jira_client.add_remote_link(issue.key, {'url': pr.html_url, 'title': pr.title})
            except Exception as e:
                runtime.logger.error(f"Failed to add remote link to {issue.key}: {e}")
            try:
                # Retrieve the value of the custom field
                release_notes_text_cf_value = getattr(issue.fields, JIRABugTracker.field_release_notes_text, None)
                release_notes_type_cf_value = getattr(issue.fields, JIRABugTracker.field_release_notes_type, None)

                if release_notes_type_cf_value is None and release_notes_text_cf_value is None:
                    # Data to update (e.g., changing the Release Notes Type and Release Notes Text)
                    issue_update = {
                        JIRABugTracker.field_release_notes_text: 'N/A',
                        JIRABugTracker.field_release_notes_type: {'value': 'Release Note Not Required'},
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
@click.option(
    '--github-access-token',
    metavar='TOKEN',
    required=False,
    envvar='GITHUB_TOKEN',
    help='Github access token for user.',
)
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
@click.option('--dry-run', default=False, is_flag=True, help='Do everything except any remote writes/pushes')
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
    dry_run,
    moist_run,
    add_auto_labels,
    add_label,
):
    runtime.initialize(clone_distgits=False, clone_source=False, prevent_cloning=True)
    if not github_access_token:
        raise click.BadParameter(
            "This command requires a personal access token (--github-access-token) "
            "because it forks repos and opens PRs as a user. GitHub App tokens cannot be used.",
            param_hint="'--github-access-token'",
        )
    g = Github(auth=Auth.Token(github_access_token))
    github_user = g.get_user()

    major = runtime.group_config.vars['MAJOR']
    minor = runtime.group_config.vars['MINOR']
    interstitial = int(interstitial)

    to_reconcile = runtime.group_config.reconciliation_prs.enabled
    if to_reconcile is not Missing and not to_reconcile:
        runtime.logger.warning(f'streams_prs_disabled is set in group.yml of {major}.{minor}; skipping PRs')
        exit(0)

    master_major, master_minor = extract_version_fields(what_is_in_master(), at_least=2)
    if not ignore_ci_master and (major, minor) > (master_major, master_minor):
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
    missing_upstream_images = set()  # Track images that failed the check so we don't retry slow oc calls.
    errors_raised = False  # If failures are found when opening a PR, won't break the loop but will return an exit code to signal this event
    images_skipped = False  # If images are skipped due to missing upstream manifests, exit 25 (unstable)

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

        def check_if_upstream_image_exists(upstream_image) -> bool:
            """Check if an upstream image exists. Returns False if missing.

            We don't know yet whether this image exists; perhaps a buildconfig is
            failing. Don't open PRs for images that don't yet exist.
            """
            if upstream_image in checked_upstream_images:
                return upstream_image not in missing_upstream_images
            try:
                util.oc_image_info_for_arch(upstream_image)
                checked_upstream_images.add(upstream_image)
                return True
            except Exception:
                yellow_print(
                    f'Unable to access upstream image {upstream_image} for {dgk}'
                    ' -- check whether buildconfigs are running successfully.'
                )
                checked_upstream_images.add(upstream_image)
                if ignore_missing_images:
                    return True
                missing_upstream_images.add(upstream_image)
                return False

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
                try:
                    upstream_image = resolve_upstream_from(runtime, builder)
                except Exception as e:
                    yellow_print(f'Unable to resolve upstream image for {builder} in {dgk} for {major}.{minor}: {e}')
                    images_skipped = True
                    break
                if not upstream_image:
                    logger.warning(f'Unable to resolve upstream image for: {builder}')
                    break
                if not check_if_upstream_image_exists(upstream_image):
                    images_skipped = True
                    break
                desired_parents.append(upstream_image)

            try:
                parent_upstream_image = resolve_upstream_from(runtime, from_config)
            except Exception as e:
                yellow_print(f'Unable to resolve upstream image for {from_config} in {dgk} for {major}.{minor}: {e}')
                images_skipped = True
                continue
            if len(desired_parents) != len(builders) or not parent_upstream_image:
                logger.warning('Unable to find all ART equivalent upstream images for this image')
                continue
            if not check_if_upstream_image_exists(parent_upstream_image):
                images_skipped = True
                continue

            desired_parents.append(parent_upstream_image)

        desired_parent_digest = calc_parent_digest(desired_parents)
        logger.info(f'Found desired FROM state of: {desired_parents} with digest: {desired_parent_digest}')

        desired_ci_build_root_coordinate = None
        desired_ci_build_root_image = ''
        if streams_pr_config.ci_build_root is not Missing:
            try:
                desired_ci_build_root_image = resolve_upstream_from(runtime, streams_pr_config.ci_build_root)
            except Exception as e:
                yellow_print(
                    f'Unable to resolve ci_build_root {streams_pr_config.ci_build_root} in {dgk} for {major}.{minor}: {e}'
                )
                images_skipped = True
                continue
            if not check_if_upstream_image_exists(desired_ci_build_root_image):
                images_skipped = True
                continue

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
            if dry_run:
                logger.info(
                    f'DRY RUN: Would have forked {public_source_repo.full_name} to {fork_repo_name}. Moving to next image since fork is required to continue.'
                )
                continue
            else:
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

        public_repo_url = ensure_github_https_url(public_repo_url)
        fork_url = ensure_github_https_url(fork_repo.git_url)
        clone_dir = os.path.join(runtime.working_dir, 'clones', dgk)
        # Clone the private url to make the best possible use of our doozer_cache
        git_clone(source_repo_url, clone_dir, git_cache_dir=runtime.git_cache_dir)

        with Dir(clone_dir):
            # Generate per-org auth credentials for each remote.
            # GIT_ASKPASS/GIT_PASSWORD can only hold a single token, so
            # `git fetch --all` fails when remotes span different GitHub
            # orgs (e.g. openshift-priv vs openshift vs openshift-bot).
            # Fetching each remote individually with the correct per-org
            # token avoids "Repository not found" auth errors.
            priv_auth = get_github_git_auth_env(url=source_repo_url)
            pub_auth = get_github_git_auth_env(url=public_repo_url)
            fork_auth = build_git_auth_env(github_access_token)

            exectools.cmd_assert(f'git remote add public {public_repo_url}')
            exectools.cmd_assert(f'git remote add fork {fork_url}')

            for remote_name, auth_env in [('origin', priv_auth), ('public', pub_auth), ('fork', fork_auth)]:
                exectools.cmd_assert(f'git fetch {remote_name}', retries=3, set_env=auth_env)

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
                        if moist_run or dry_run:
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

                if not moist_run and not dry_run:
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
                    exectools.cmd_assert(
                        f'git push --force fork {work_branch_name}:{fork_branch_name}', retries=3, set_env=fork_auth
                    )

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
  tests OR that necessary metadata changes are reported to the ART team
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
If you have any questions about this pull request, please reach out in the `#forum-ocp-art` Slack channel.
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

                if not dry_run:
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
                if moist_run or dry_run:
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
            if moist_run or dry_run:
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
        reconcile_jira_issues(runtime, pr_dgk_map, moist_run or dry_run)

    if skipping_dgks:
        print('Some PRs were skipped; Exiting with return code 25 to indicate this')
        exit(25)

    if errors_raised:
        print('Some errors were raised: see the log for details')
        exit(50)

    if images_skipped:
        print('Some images were skipped due to missing upstream manifests: see the log for details')
        exit(25)
