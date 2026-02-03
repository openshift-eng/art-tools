import json
import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, Optional, Union, cast

import artcommonlib
import yaml
from artcommonlib import exectools, redis
from artcommonlib.arch_util import go_suffix_for_arch
from artcommonlib.assembly import assembly_type
from artcommonlib.exectools import limit_concurrency
from artcommonlib.model import Missing, Model
from artcommonlib.release_util import SoftwareLifecyclePhase, isolate_assembly_in_release
from doozerlib import util as doozerutil
from errata_tool import ErrataConnector

from pyartcd import constants, jenkins, record
from pyartcd.mail import MailService

logger = logging.getLogger(__name__)


def isolate_el_version_in_release(release: str) -> Optional[int]:
    """
    Given a release field, determines whether is contains
    a RHEL version. If it does, it returns the version value as int.
    If it is not found, None is returned.
    """
    match = re.match(r'.*\.el(\d+)(?:\.+|$)', release)
    if match:
        return int(match.group(1))

    return None


def isolate_el_version_in_branch(branch_name: str) -> Optional[int]:
    """
    Given a distgit branch name, determines whether is contains
    a RHEL version. If it does, it returns the version value as int.
    If it is not found, None is returned.
    """
    match = re.fullmatch(r'.*rhel-(\d+).*', branch_name)
    if match:
        return int(match.group(1))

    return None


def is_greenwave_all_pass_on_advisory(advisory_id: int) -> bool:
    """
    Use /api/v1/external_tests API to check if builds on advisory have failed greenwave_cvp test
    If the tests all pass then the data field of the return value will be empty
    Return True, If all greenwave test passed on advisory
    Return False, If there are failed test on advisory
    """
    logger.info(f"Check failed greenwave tests on {advisory_id}")
    result = ErrataConnector()._get(
        f'/api/v1/external_tests?filter[test_type]=greenwave_cvp&filter[status]=FAILED&filter[active]=true&page[size]=1000&filter[errata_id]={advisory_id}'
    )
    if result.get('data', []):
        logger.warning(f"Some greenwave tests on {advisory_id} failed with {result}")
        return False
    return True


async def load_group_config(
    group: str,
    assembly: str,
    env=None,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
) -> Dict:
    if doozer_data_gitref:
        group += f'@{doozer_data_gitref}'
    cmd = [
        "doozer",
        f"--data-path={doozer_data_path}",
        "--group",
        group,
        "--assembly",
        assembly,
        "config:read-group",
        "--yaml",
    ]
    if env is None:
        env = os.environ.copy()
    temp_workdir = None
    if not env.get("DOOZER_WORKING_DIR"):
        temp_workdir = tempfile.mkdtemp(prefix="doozer-working-", dir=".")
        env["DOOZER_WORKING_DIR"] = temp_workdir
    try:
        _, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None, env=env)
    finally:
        if temp_workdir:
            shutil.rmtree(temp_workdir)
    group_config = yaml.safe_load(stdout)
    if not isinstance(group_config, dict):
        raise ValueError("ocp-build-data contains invalid group config.")
    return group_config


async def load_releases_config(group: str, data_path: str = constants.OCP_BUILD_DATA_URL) -> Optional[Dict]:
    cmd = [
        'doozer',
        f'--data-path={data_path}',
        f'--group={group}',
        'config:read-releases',
        '--yaml',
    ]

    try:
        _, out, _ = await exectools.cmd_gather_async(cmd)
        return yaml.safe_load(out.strip())

    except ChildProcessError as e:
        logger.error('Command "%s" failed: %s', ' '.join(cmd), e)
        return None


async def load_assembly(
    group: str, assembly: str, key: str = '', data_path: str = constants.OCP_BUILD_DATA_URL
) -> Optional[Dict]:
    cmd = [
        'doozer',
        f'--data-path={data_path}',
        f'--group={group}',
        f'--assembly={assembly}',
        'config:read-assembly',
        '--yaml',
        key,
    ]

    try:
        _, out, _ = await exectools.cmd_gather_async(cmd)
        return yaml.safe_load(out.strip())

    except ChildProcessError as e:
        logger.error('Command "%s" failed: %s', ' '.join(cmd), e)
        return None


def get_assembly_type(releases_config: Dict, assembly_name: str):
    return assembly_type(Model(releases_config), assembly_name)


def get_assembly_basis(releases_config: Dict, assembly_name: str):
    return artcommonlib.assembly.assembly_basis(Model(releases_config), assembly_name)


def get_assembly_promotion_permits(releases_config: Dict, assembly_name: str):
    return artcommonlib.assembly.assembly_config_struct(Model(releases_config), assembly_name, 'promotion_permits', [])


def get_release_name_for_assembly(group_name: str, releases_config: Dict, assembly_name: str):
    return doozerutil.get_release_name_for_assembly(group_name, Model(releases_config), assembly_name)


def get_rpm_if_pinned_directly(releases_config: Dict, assembly_name: str, rpm_name: str) -> dict:
    # this does not consider inherited assemblies
    # use with caution
    pinned_rpms = Model(releases_config).releases[assembly_name].assembly.members.rpms
    return next((rpm['metadata']['is'] for rpm in pinned_rpms if rpm['distgit_key'] == rpm_name), dict())


def get_image_if_pinned_directly(releases_config: Dict, assembly_name: str, image_name: str) -> str:
    # this does not consider inherited assemblies
    # use with caution
    try:
        pinned_images = Model(releases_config).releases[assembly_name].assembly.members.images
    except (KeyError, AttributeError):
        # Assembly structure doesn't exist
        return ""

    image_metadata = next(
        (image['metadata']['is'] for image in pinned_images if image['distgit_key'] == image_name), None
    )
    if image_metadata:
        return image_metadata['nvr']  # Let it fail if 'nvr' key isn't found
    return ""


async def kinit():
    logger.info('Initializing ocp-build kerberos credentials')

    keytab_file = os.getenv('DISTGIT_KEYTAB_FILE', None)
    keytab_user = os.getenv('DISTGIT_KEYTAB_USER', 'exd-ocp-buildvm-bot-prod@IPA.REDHAT.COM')
    if keytab_file:
        # The '-f' ensures that the ticket is forwarded to remote hosts
        # when using SSH. This is required for when we build signed
        # puddles.
        cmd = [
            'kinit',
            '-f',
            '-k',
            '-t',
            keytab_file,
            keytab_user,
        ]
        await exectools.cmd_assert_async(cmd)
    else:
        logger.warning('DISTGIT_KEYTAB_FILE is not set. Using any existing kerberos credential.')


async def branch_arches(
    group: str,
    assembly: str,
    ga_only: bool = False,
    build_system: str = 'brew',
    data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
) -> list:
    """
    Find the supported arches for a specific release
    :param str group: The name of the branch to get configs for. For example: 'openshift-4.12
    :param str assembly: The name of the assembly. For example: 'stream'
    :param bool ga_only: If you only want group arches and do not care about arches_override.
    :param str build_system: 'brew' | 'konflux'
    :param str data_path: Path to ocp-build-data repository
    :param str doozer_data_gitref: Git ref for ocp-build-data
    :return: A list of the arches built for this branch
    """

    logger.info('Fetching group config for %s', group)
    group_config = Model(
        await load_group_config(
            group=group, assembly=assembly, doozer_data_path=data_path, doozer_data_gitref=doozer_data_gitref
        )
    )

    # Check if arches_override has been specified. This is used in group.yaml
    # when we temporarily want to build for CPU architectures that are not yet GA.
    arches_override = group_config.get('arches_override', None)
    if arches_override and ga_only:
        return arches_override

    # Otherwise, read supported arches from group config
    if build_system == 'brew':
        return group_config.arches
    elif build_system == 'konflux':
        if group_config.konflux.arches is not Missing:
            return group_config.konflux.arches
        return group_config.arches
    else:
        raise ValueError(f'Invalid build system: {build_system}')


def get_changes(yaml_data: dict) -> dict:
    """
    Scans data outputted by config:scan-sources yaml and records changed
    elements in the object it returns.
    The return dict has optional .rpms, .images and .rhcos fields,
    that are omitted if no change was detected.
    """

    changes = {}

    rpms = [rpm['name'] for rpm in yaml_data.get('rpms', []) if rpm['changed']]
    if rpms:
        changes['rpms'] = rpms

    images = [image['name'] for image in yaml_data.get('images', []) if image['changed']]
    if images:
        changes['images'] = images

    rhcos = [{'name': rhcos['name'], 'reason': rhcos} for rhcos in yaml_data.get('rhcos', []) if rhcos['changed']]
    if rhcos:
        changes['rhcos'] = rhcos

    return changes


async def get_freeze_automation(
    group: str,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_working: str = '',
    doozer_data_gitref: str = '',
) -> str:
    """
    Returns freeze_automation flag for a specific group
    """

    group_param = f'--group={group}'
    if doozer_data_gitref:
        group_param += f'@{doozer_data_gitref}'

    cmd = [
        'doozer',
        f'--working-dir={doozer_working}' if doozer_working else '',
        '--assembly=stream',
        f'--data-path={doozer_data_path}',
        group_param,
        'config:read-group',
        '--default=no',
        'freeze_automation',
    ]
    _, out, _ = await exectools.cmd_gather_async(cmd)
    return out.strip()


async def has_layered_rhcos(doozer_base_command: list) -> bool:
    """
    Check if the current version uses layered RHCOS
    """

    cmd = doozer_base_command + [
        'config:read-group',
        'rhcos.layered_rhcos',
        '--default=False',
    ]
    _, out, _ = await exectools.cmd_gather_async(cmd)
    layered_rhcos = out.strip() == 'True'
    logger.info('Layered RHCOS %s enabled', 'NOT' if not layered_rhcos else '')
    return layered_rhcos


def is_manual_build() -> bool:
    """
    Builds that are triggered manually by a Jenkins user carry a BUILD_USER_EMAIL environment variable.
    If this var is not defined, we can infer that the build was triggered by a timer.

    Be aware that Jenkins pipeline need to pass this var by enclosing the code in a wrap([$class: 'BuildUser']) {} block
    """

    build_user_email = os.getenv('BUILD_USER_EMAIL')
    logger.info('Found BUILD_USER_EMAIL=%s', build_user_email)

    if build_user_email is not None:
        logger.info('Considering this a manual build')
        return True

    logger.info('Considering this a scheduled build')
    return False


def get_weekday() -> str:
    """
    Returns the current day of the week as a string
    """

    return datetime.today().strftime("%A")


async def is_build_permitted(
    version: str = '',
    group: str = '',
    data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_working: str = '',
    doozer_data_gitref: str = '',
) -> bool:
    """
    Check whether the group should be built right now.
    This depends on:
        - group config 'freeze_automation'
        - manual/scheduled run
        - current day of the week

    Args:
        version: OCP version (e.g., '4.17'). If provided, group is constructed as 'openshift-{version}'.
        group: Full group name (e.g., 'openshift-4.17'). One of version or group must be provided.
        data_path: Path to ocp-build-data
        doozer_working: Doozer working directory
        doozer_data_gitref: Git ref for ocp-build-data
    """
    if not version and not group:
        raise ValueError("Either version or group must be provided")
    if not group:
        group = f'openshift-{version}'
    # Get 'freeze_automation' flag
    # get_freeze_automation now expects a full group name like 'openshift-4.15'
    freeze_automation = await get_freeze_automation(
        group=group,
        doozer_data_path=data_path,
        doozer_working=doozer_working,
        doozer_data_gitref=doozer_data_gitref,
    )
    logger.info('Group freeze automation flag is set to: "%s"', freeze_automation)

    # Check for frozen automation
    # yaml parses unquoted "yes" as a boolean... accept either
    if freeze_automation in ['yes', 'True']:
        logger.info('All automation is currently disabled by freeze_automation in group.yml.')
        return False

    # Check for frozen scheduled automation
    if freeze_automation == "scheduled" and not is_manual_build():
        logger.info(
            'Only manual runs are permitted according to freeze_automation in group.yml '
            'and this run appears to be non-manual.'
        )
        return False

    # Check if group can run on weekends
    if freeze_automation == 'weekdays':
        # Manual builds are always permitted
        if is_manual_build():
            logger.info('Current build is permitted as it has been triggered manually')
            return True

        # Check current day of the week
        weekday = get_weekday()
        if weekday in ['Saturday', 'Sunday']:
            logger.info(f'Automation is permitted during weekends, and today is {weekday}')
            return True

        if weekday in ['Monday'] and os.environ.get('JOB_BASE_NAME', '') in ['images-health']:
            logger.info(f"Permitting automated run of {os.environ['JOB_BASE_NAME']} on Monday")
            return True

        logger.info('Scheduled builds for %s are permitted only on weekends, and today is %s', version, weekday)
        return False

    # Fallback to default
    return True


def log_dir_tree(path_to_dir):
    logger.info(f"Printing dir tree of {path_to_dir}")
    for child in os.listdir(path_to_dir):
        child_path = os.path.join(path_to_dir, child)
        logger.info(child_path)


def log_file_content(path_to_file):
    logger.info(f"Printing file content of {path_to_file}")
    with open(path_to_file, 'r') as f:
        logger.info(f.read())


def default_release_suffix():
    """
    Returns a release suffix based on current timestamp
    E.g. "202312311112.p?"
    """

    return f'{datetime.strftime(datetime.now(tz=timezone.utc), "%Y%m%d%H%M")}.p?'


def dockerfile_url_for(url, branch, sub_path) -> str:
    if not url or not branch:
        return ''

    # if it looks like an ssh GitHub remote, transform it to https
    url = url.replace('git@', 'https://')
    url = url.replace(':', '/')
    url = url.replace('.git', '')

    return f"{url}/blob/{branch}/{sub_path if sub_path else ''}"


def notify_dockerfile_reconciliations(version: str, doozer_working: str, mail_client: MailService):
    """
    Loop through all new commits that affect dockerfiles and notify their owners
    """

    with open(Path(doozer_working) / "record.log", "r") as file:
        record_log: dict = record.parse_record_log(file)

    distgit_notify = record.get_distgit_notify(record_log)

    # Convert the dict to a list of tuples
    distgit_notify = [(key, value) for key, value in distgit_notify.items()]

    for i in range(len(distgit_notify)):
        distgit = distgit_notify[i][0]

        val = distgit_notify[i][1]
        if not val.get('owners'):
            continue

        alias = val['source_alias']
        url = dockerfile_url_for(alias['origin_url'], alias['branch'], val['source_dockerfile_subpath'])
        dockerfile_url = f'Upstream source file: {url}' if url else ''

        # Populate the introduction for all emails to owners
        explanation_body = """Why am I receiving this?
------------------------
You are receiving this message because you are listed as an owner for an
OpenShift related image - or you recently made a modification to the definition
of such an image in github. Upstream (github) OpenShift Dockerfiles are
regularly pulled from their upstream source and used as an input to build our
productized images - RHEL-based OpenShift Container Platform (OCP) images.

To serve as an input to RHEL/OCP images, upstream Dockerfiles are
programmatically modified before they are checked into a downstream git
repository which houses all Red Hat images:
 - https://pkgs.devel.redhat.com/cgit/containers/

We call this programmatic modification "reconciliation" and you will receive an
email when the upstream Dockerfile changes so that you can review the
differences between the upstream & downstream Dockerfiles.\n"""

        if val.get('failure', None):
            email_subject = f'FAILURE: Error reconciling Dockerfile for {val["image"]} in OCP v{version}'
            explanation_body += f"""
What do I need to do?
---------------------
An error occurred during your reconciliation. Until this issue is addressed,
your upstream changes may not be reflected in the product build.

Please review the error message reported below to see if the issue is due to upstream
content. If it is not, the Automated Release Tooling (ART) team will engage to address
the issue. Please direct any questions to the ART team (#forum-ocp-art on slack).

Error Reported
--------------
{val['failure']}\n"""

        elif val.get('sha', None):
            email_subject = f'SUCCESS: Changed Dockerfile reconciled for {val["image"]} in OCP v{version}'
            explanation_body += f"""
What do I need to do?
---------------------
You may want to look at the result of the reconciliation. Usually,
reconciliation is transparent and safe. However, you may be interested in any
changes being performed by the OCP build system.


What changed this time?
-----------------------
Reconciliation has just been performed for the image: {val["image"]}
{dockerfile_url}
The reconciled (downstream OCP) Dockerfile can be viewed here:
 - https://pkgs.devel.redhat.com/cgit/{distgit}/tree/Dockerfile?id={val["sha"]}

Please direct any questions to the Automated Release Tooling team (#forum-ocp-art on slack).\n"""

        else:
            raise RuntimeError('Unable to determine notification reason; something is broken')

        mail_client.send_mail(
            to=val['owners'],
            subject=email_subject,
            content=explanation_body,
        )


def notify_bz_info_missing(version: str, doozer_working: str, mail_client: MailService):
    with open(Path(doozer_working) / "record.log", "r") as file:
        record_log: dict = record.parse_record_log(file)

    bz_notify_entries = record_log.get('bz_maintainer_notify', [])
    for bz_notify in bz_notify_entries:
        owners = bz_notify.get('owners', '')
        if not owners:
            continue

        public_upstream_url = bz_notify['public_upstream_url']
        distgit = bz_notify['distgit']
        email_subject = (
            f'[ACTION REQUIRED] Bugzilla component information missing for image {distgit} in OCP v{version}'
        )
        explanation_body = f"""
Why am I receiving this?
------------------------
You are receiving this message because you are listed as an owner for an
OpenShift related image - or you recently made a modification to the definition
of such an image in github.

To comply with prodsec requirements, all images in the OpenShift product
should identify their Bugzilla component. To accomplish this, ART
expects to find Bugzilla component information in the default branch of
the image's upstream repository or requires it in ART image metadata.

What should I do?
------------------------
There are two options to supply Bugzilla component information.
1) The OWNERS file in the default branch (e.g. main / master) of {public_upstream_url}
   can be updated to include the bugzilla component information.

2) The component information can be specified directly in the
   ART metadata for the image {distgit}.

Details for either approach can be found here:
https://docs.google.com/document/d/1V_DGuVqbo6CUro0RC86THQWZPrQMwvtDr0YQ0A75QbQ/edit?usp=sharing

Thanks for your help!\n"""

        mail_client.send_mail(
            to=owners,
            subject=email_subject,
            content=explanation_body,
        )


def mail_build_failure_owners(failed_builds: dict, doozer_working: str, mail_client: MailService, default_owner: str):
    """
     Send email to owners of failed image builds.


     :param failed_builds: map of records as below (all values strings)

     presto:
         status: -1
         push_status: 0
         distgit: presto
         image: openshift/ose-presto
         owners: sd-operator-metering@redhat.com,czibolsk@redhat.com
         version: v4.0.6
         release: 1
         dir: doozer_working/distgits/containers/presto
         dockerfile: doozer_working/distgits/containers/presto/Dockerfile
         task_id: 20415814
         task_url: https://brewweb.engineering.redhat.com/brew/taskinfo?taskID=20415814
         message: "Exception occurred: ;;; Traceback (most recent call last): [...]"

    :param doozer_working: path to Doozer working directory

    :param mail_client: MailService instance

    :param return_address: replies to the email will go to this

    :param default_owner: if no owner is listed, send build failure email to this
    """

    for failure in failed_builds.values():
        if failure['status'] == '0':
            continue

        container_log = """
--------------------------------------------------------------------------
The following logs are just the container build portion of the OSBS build:
--------------------------------------------------------------------------\n"""
        container_log_file = (
            f'{doozer_working}/brew-logs/{failure["distgit"]}/noarch-{failure["task_id"]}/container-build-x86_64.log'
        )

        try:
            with open(container_log_file) as f:
                container_log += f.read()

        except:
            container_log = "Unfortunately there were no container build logs; something else about the build failed."
            logger.warning(
                'No container build log for failed %s build\n(task url %s)\nat path %s',
                failure['distgit'],
                failure['task_url'],
                container_log,
            )

        explanation_body = f"ART's brew/OSBS build of OCP image {failure['image']}:{failure['version']} has failed.\n\n"
        if failure['owners']:
            explanation_body += "This email is addressed to the owner(s) of this image per ART's build configuration."
        else:
            explanation_body += 'There is no owner listed for this build (you may want to add one).'
        explanation_body += '\n\n'
        explanation_body += (
            "Builds may fail for many reasons, some under owner control, some under ART's control, "
            "and some in the domain of other groups. This message is only sent when the build fails "
            "consistently, so it is unlikely this failure will resolve itself without intervention.\n\n"
        )
        explanation_body += (
            f'The brew build task {failure["task_url"]} failed with error message:\n'
            f'{failure["message"]}\n'
            f'{container_log}'
        )

        # Send email to owners of failed image builds
        # If art is the only owner of image (example for our ci golang builder images) send instead to our default automation email
        owner = (
            failure['owners']
            if (failure['owners'] and failure['owners'] != ["aos-team-art@redhat.com"])
            else default_owner
        )
        mail_client.send_mail(
            to=['aos-art-automation+failed-ocp-build@redhat.com', owner],
            subject=f'Failed OCP build of {failure["image"]}:{failure["version"]}',
            content=explanation_body,
        )


async def invalidate_cloudfront_cache(invalidation_path):
    """
    Invalidate s3 Cloudfront cache
    """
    cmd = f"aws cloudfront create-invalidation --distribution-id E3RAW1IMLSZJW3 --paths {invalidation_path}"

    await exectools.cmd_assert_async(cmd, env=os.environ.copy(), stdout=sys.stderr)


async def mirror_to_s3(
    source: Union[str, Path],
    dest: str,
    exclude: Optional[str] = None,
    include: Optional[str] = None,
    dry_run: bool = False,
    delete: bool = False,
):
    """
    Copy to AWS S3
    """
    cmd = ["aws", "s3", "sync", "--no-progress", "--exact-timestamps"]
    if delete:
        cmd.append("--delete")
    paths = ['--', f'{source}', f'{dest}']
    if exclude is not None:
        cmd.append(f"--exclude={exclude}")
    if include is not None:
        cmd.append(f"--include={include}")
    if dry_run:
        cmd.append("--dryrun")
    await exectools.cmd_assert_async(cmd + paths, env=os.environ.copy(), stdout=sys.stderr)

    # Mirror to Cloudflare as well
    await exectools.cmd_assert_async(
        cmd + ["--profile", "cloudflare", "--endpoint-url", os.environ["CLOUDFLARE_ENDPOINT"]] + paths,
        env=os.environ.copy(),
        stdout=sys.stderr,
    )


async def mirror_to_google_cloud(source: Union[str, Path], dest: str, dry_run=False):
    """
    Copy to Google Cloud
    """
    # -n - no clobber/overwrite; -v - print url of item; -L - write to log for auto re-processing; -r - recursive
    cmd = ["gsutil", "cp", "-n", "-v", "-r", "--", f"{source}", f"{dest}"]
    if dry_run:
        logger.warning("[DRY RUN] Would have run %s", cmd)
        return
    await exectools.cmd_assert_async(cmd, env=os.environ.copy(), stdout=sys.stderr)


async def get_signing_mode(
    group: str = None,
    assembly: str = None,
    group_config: dict = None,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
) -> str:
    """
    If any arch is GA, use signed mode for everything
    This also includes EOL ones, that might be triggered manually
    Versions that are still in pre-release state will not be signed
    """

    if not group_config:
        assert group, 'Group must be specified in order to load group config'
        assert assembly, 'Assembly must be specified in order to load group config'
        group_config = await load_group_config(
            group=group, assembly=assembly, doozer_data_path=doozer_data_path, doozer_data_gitref=doozer_data_gitref
        )

    # For non-OpenShift groups, always use signed repos regardless of phase
    if group and not group.startswith('openshift-'):
        return 'signed'

    phase = SoftwareLifecyclePhase.from_name(group_config['software_lifecycle']['phase'])
    return 'signed' if phase >= SoftwareLifecyclePhase.SIGNING else 'unsigned'


def nightlies_with_pullspecs(nightly_tags: Iterable[str]) -> Dict[str, str]:
    """
    Parse nightly tags and return a dict of arch:nightly_pullspecs
    """
    arch_nightlies = {}
    for nightly in nightly_tags:
        if "s390x" in nightly:
            arch = "s390x"
        elif "ppc64le" in nightly:
            arch = "ppc64le"
        elif "arm64" in nightly:
            arch = "aarch64"
        else:
            arch = "x86_64"
        if ":" not in nightly:
            # prepend pullspec URL to nightly name
            arch_suffix = go_suffix_for_arch(arch, "priv" in nightly)
            nightly = f"registry.ci.openshift.org/ocp{arch_suffix}/release{arch_suffix}:{nightly}"
        arch_nightlies[arch] = nightly
    return arch_nightlies


async def get_microshift_builds(group, assembly, env):
    cmd = [
        "elliott",
        "--group",
        group,
        "--assembly",
        assembly,
        "-r",
        "microshift",
        "find-builds",
        "-k",
        "rpm",
        "--member-only",
        "--include-shipped",
    ]
    with TemporaryDirectory() as tmpdir:
        path = f"{tmpdir}/out.json"
        cmd.append(f"--json={path}")
        await exectools.cmd_assert_async(cmd, env=env)
        with open(path) as f:
            result = json.load(f)

    nvrs = cast(List[str], result)

    # microshift builds are special in that they build for each assembly after payload is promoted
    # and they include the assembly name in its build name
    # so make sure found nvrs are related to assembly
    return [n for n in nvrs if isolate_assembly_in_release(n) == assembly]


def mass_rebuild_score(version: str, okd: bool = False) -> int:
    """
    For the ocp_version (e.g. `4.16`) return an integer score value
    Higher the score, higher the priority.
    For OKD, the score is half that of OCP for the same version.
    """

    if okd:
        return round(float(version) * 50)  # '4.16' -> 208

    return round(float(version) * 100)  # '4.16' -> 416


async def get_group_images(
    group: str,
    assembly: str,
    build_system: str,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
    load_okd_only: bool = False,
) -> List[str]:
    """
    Get the list of images for a given group and assembly.

    :param group: The group name (e.g. 'openshift-4.21')
    :param assembly: The assembly name (e.g. 'stream', 'rc.1')
    :param build_system: Build system to use ('brew' or 'konflux'). If empty string, doozer will use its default.
    :param doozer_data_path: Path to ocp-build-data repository
    :param doozer_data_gitref: Git reference to use in ocp-build-data
    :param load_okd_only: Whether to load OKD-only images (mode: disabled, okd.mode: enabled)
    :return: List of image distgit keys
    """

    with TemporaryDirectory() as doozer_working:
        group_param = f'--group={group}'
        if doozer_data_gitref:
            group_param += f'@{doozer_data_gitref}'
        command = [
            'doozer',
            f'--working-dir={doozer_working}',
            f'--data-path={doozer_data_path}',
        ]
        if load_okd_only:
            command.append('--load-okd-only')
        if build_system:
            command.append(f'--build-system={build_system}')
        command.extend(
            [
                group_param,
                '--assembly',
                assembly,
                'images:list',
                '--json',
            ]
        )
        _, out, _ = await exectools.cmd_gather_async(command)
        return json.loads(out)['images']


async def get_group_rpms(
    group: str,
    assembly: str,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
) -> List[str]:
    """
    Get the list of RPMs for a given group and assembly.
    """

    with TemporaryDirectory() as doozer_working:
        group_param = f'--group={group}'
        if doozer_data_gitref:
            group_param += f'@{doozer_data_gitref}'
        command = [
            'doozer',
            f'--working-dir={doozer_working}',
            f'--data-path={doozer_data_path}',
            group_param,
            f'--assembly={assembly}',
            'rpms:print',
            '--output=rpms.txt',
        ]
        await exectools.cmd_assert_async(command)
        with open('rpms.txt', 'r') as f:
            out = f.read()
        return out.splitlines()


async def increment_rebase_fail_counter(image, version, build_system, branch='rebase-failure', job_url=None):
    """
    Increment the fail counter for a given image in Redis.
    Optionally store the job URL where the failure occurred.

    Arg(s):
        image (str): Image name
        version (str): OCP version (e.g., '4.17')
        build_system (str): Build system ('brew', 'konflux')
        branch (str): Branch identifier for the counter (default: 'rebase-failure')
        job_url (str): Optional job URL where the failure occurred
    """

    redis_branch = f'count:{branch}:{build_system}:{version}:{image}'
    failure_key = f'{redis_branch}:failure'
    fail_count = await redis.get_value(failure_key)
    fail_count = int(fail_count) if fail_count else 0
    await redis.set_value(key=failure_key, value=fail_count + 1)

    # Store the job URL if provided
    if job_url:
        await redis.set_value(key=f'{redis_branch}:url', value=job_url)


@limit_concurrency(50)
async def reset_rebase_fail_counter(image, version, build_system, branch='rebase-failure'):
    """
    Reset the fail counter for a given image in Redis.
    Limit concurrency as we might have a lot of images to reset.

    Arg(s):
        image (str): Image name
        version (str): OCP version (e.g., '4.17')
        build_system (str): Build system ('brew', 'konflux')
        branch (str): Branch identifier for the counter (default: 'rebase-failure')
    """

    redis_branch = f'count:{branch}:{build_system}:{version}:{image}:*'
    await redis.delete_keys_by_pattern(redis_branch)
