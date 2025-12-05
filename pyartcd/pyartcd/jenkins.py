import functools
import logging
import os
import time
from enum import Enum
from typing import Optional
from urllib.parse import unquote, urlparse

import requests
from jenkinsapi.build import Build
from jenkinsapi.custom_exceptions import NotFound
from jenkinsapi.jenkins import Jenkins
from jenkinsapi.job import Job
from jenkinsapi.queue import QueueItem
from jenkinsapi.utils.crumb_requester import CrumbRequester
from tenacity import retry, stop_after_attempt, wait_fixed

from pyartcd import constants

logger = logging.getLogger(__name__)

current_build_url: Optional[str] = None
current_job_name: Optional[str] = None
jenkins_client: Optional[Jenkins] = None


class Jobs(Enum):
    BUILD_SYNC = 'aos-cd-builds/build%2Fbuild-sync'
    BUILD_SYNC_KONFLUX = 'aos-cd-builds/build%2Fbuild-sync-konflux'
    BUILD_MICROSHIFT = 'aos-cd-builds/build%2Fbuild-microshift'
    BUILD_MICROSHIFT_BOOTC = 'aos-cd-builds/build%2Fbuild-microshift-bootc'
    OCP4 = 'aos-cd-builds/build%2Focp4'
    OCP4_KONFLUX = 'aos-cd-builds/build%2Focp4-konflux'
    OCP4_SCAN = 'aos-cd-builds/build%2Focp4_scan'
    OCP4_SCAN_KONFLUX = 'aos-cd-builds/build%2Focp4-scan-konflux'
    RHCOS = 'aos-cd-builds/build%2Frhcos'
    OLM_BUNDLE = 'aos-cd-builds/build%2Folm_bundle'
    OLM_BUNDLE_KONFLUX = 'aos-cd-builds/build%2Folm_bundle_konflux'
    SYNC_FOR_CI = 'scheduled-builds/sync-for-ci'
    MICROSHIFT_SYNC = 'aos-cd-builds/build%2Fmicroshift_sync'
    CINCINNATI_PRS = 'aos-cd-builds/build%2Fcincinnati-prs'
    RHCOS_SYNC = 'aos-cd-builds/build%2Frhcos_sync'
    BUILD_PLASHETS = 'aos-cd-builds/build%2Fbuild-plashets'
    BUILD_FBC = 'aos-cd-builds/build%2Fbuild-fbc'
    OADP = 'aos-cd-builds/build%2Foadp'
    OADP_SCAN = 'aos-cd-builds/build%2Foadp-scan'


def get_jenkins_url():
    url = os.environ.get("JENKINS_URL", constants.JENKINS_SERVER_URL)
    return url.rstrip('/')


def init_jenkins():
    global jenkins_client
    if jenkins_client:
        return

    jenkins_url = get_jenkins_url()

    logger.info('Initializing Jenkins client..')
    requester = CrumbRequester(
        username=os.environ['JENKINS_SERVICE_ACCOUNT'],
        password=os.environ['JENKINS_SERVICE_ACCOUNT_TOKEN'],
        baseurl=jenkins_url,
    )

    jenkins_client = Jenkins(
        jenkins_url,
        username=os.environ['JENKINS_SERVICE_ACCOUNT'],
        password=os.environ['JENKINS_SERVICE_ACCOUNT_TOKEN'],
        requester=requester,
        lazy=True,
        timeout=60,
    )
    logger.info('Connected to Jenkins server %s', jenkins_client.base_server_url())


def get_build_url():
    url = os.environ.get("BUILD_URL")
    if not url:
        return None
    return f"{url.rstrip('/')}"


def get_build_path():
    """
    Examples:
    - https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/job/aos-cd-builds/job/build%252Focp4/46870/ =>
        job/aos-cd-builds/job/build%252Focp4/46870
    - https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/job/aos-cd-builds/job/build%252Focp4/46870 =>
        job/aos-cd-builds/job/build%252Focp4/46870
    """

    url = get_build_url()
    return '/'.join(url.split('/')[3:]) if url else None


def get_build_id() -> str:
    return os.environ.get("BUILD_ID")


def get_build_id_from_url(build_url: str) -> int:
    """
    Examples:
    - https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com:8443/job/aos-cd-builds/job/build%252Focp4/46870/ => 46870
    - https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com:8443/job/aos-cd-builds/job/build%252Focp4/46870 => 46870
    """

    return int(list(filter(None, build_url.split('/')))[-1])


def get_job_name():
    return os.environ.get("JOB_NAME")


def get_job_name_and_build_number_from_path(build_path: str) -> tuple[Optional[str], Optional[str]]:
    if not build_path:
        logger.warning('Empty build path received')
        return None, None

    path = build_path
    if build_path.startswith('http://') or build_path.startswith('https://'):
        path = urlparse(build_path).path

    # path is now something like '/job/aos-cd-builds/job/build%252Focp4-konflux/16686/'
    path = path.strip('/')

    try:
        job_path_str, build_number = path.rsplit("/", 1)
    except ValueError:
        logger.warning('Invalid build path: %s', build_path)
        return None, None

    # If job_path_str is like 'job/aos-cd-builds/job/build%2Focp4-konflux'
    # We need to convert it to a job name like 'aos-cd-builds/build%2Focp4-konflux'
    job_name = job_path_str
    if job_name.startswith('job/'):
        job_name = job_name[4:]
    job_name = job_name.replace('/job/', '/')

    # The job name from URL path is double-encoded. We need to decode it once.
    job_name = unquote(job_name)

    return job_name, build_number


def get_build_parameters(build_path: str) -> Optional[dict]:
    """
    Fetches build data using API endpoint {JENKINS_SERVER_URL}/{BUILD_PATH}/api/json
    and returns the build parameters
    """

    init_jenkins()

    job_name, build_number = get_job_name_and_build_number_from_path(build_path)
    if not job_name:
        return None

    try:
        job = jenkins_client.get_job(job_name)
    except (requests.exceptions.HTTPError, NotFound) as err:
        # Check for 404
        if isinstance(err, requests.exceptions.HTTPError):
            if err.response.status_code == 404:
                logger.warning('Job %s not found', job_name)
                return None
        else:  # issubclass(type(err), NotFound)
            logger.warning('Job %s not found', job_name)
            return None
        # Reraise other errors
        raise

    try:
        build = job.get_build(int(build_number))
    except (NotFound, ValueError):
        return None

    params = {}
    build_data = build._data
    for action in build_data.get('actions', []):
        if action.get('_class') == 'hudson.model.ParametersAction':
            for param in action.get('parameters', []):
                if 'value' in param:
                    params[param['name']] = param['value']
            break  # Found parameters, no need to check other actions
    return params


def check_env_vars(func):
    """
    Enforces that BUILD_URL and JOB_NAME are set
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        global current_build_url, current_job_name
        current_build_url = current_build_url or get_build_url()
        current_job_name = current_job_name or get_job_name()

        if not current_build_url or not current_job_name:
            logger.error('Env vars BUILD_URL and JOB_NAME must be defined!')
            raise RuntimeError

        return func(*args, **kwargs)

    return wrapped


@check_env_vars
def wait_until_building(queue_item: QueueItem, job: Job, delay: int = 5) -> Build:
    """
    Watches a queue item and blocks until the scheduled build starts.
    Updates the description of the new build with the details of the caller job
    Returns a jenkinsapi.build.Build object representing the new build.
    """

    while True:
        try:
            data: dict = queue_item.poll()
            build_number = data['executable']['number']
            break
        except (KeyError, TypeError):
            logger.info('Build not started yet, sleeping for %s seconds...', delay)
            time.sleep(delay)

    triggered_build_url = f"{data['task']['url']}{build_number}"
    logger.info('Started new build at %s', triggered_build_url)

    # Update the description of the new build with the details of the caller job
    jenkins_url = get_jenkins_url()
    triggered_build_url = triggered_build_url.replace(constants.JENKINS_UI_URL, jenkins_url)
    triggered_build = Build(url=triggered_build_url, buildno=get_build_id_from_url(triggered_build_url), job=job)
    description = (
        f'Started by upstream project <b>{current_job_name}</b> '
        f'build number <a href="{current_build_url}">{get_build_id_from_url(current_build_url)}</a><br><br>'
    )
    set_build_description(triggered_build, description)

    return triggered_build


def set_build_description(build: Build, description: str):
    build.job.jenkins.requester.post_and_confirm_status(
        f'{build.baseurl}/submitDescription',
        params={
            'Submit': 'submit',
            'description': description,
        },
        data="",
        valid=[200],
    )


def is_build_running(build_path: str) -> bool:
    """
    Fetches build data using API endpoint {JENKINS_SERVER_URL}/{BUILD_PATH}/api/json

    The resulting JSON has a field called "inProgress" that is true if the build is still ongoing

    Build paths examples are:
    - job/aos-cd-builds/job/build%252Focp4/46902
    - job/hack/job/dpaolell/job/ocp4/238/
    """

    init_jenkins()

    job_name, build_number = get_job_name_and_build_number_from_path(build_path)
    if not job_name:
        return False

    try:
        job = jenkins_client.get_job(job_name)
    except (requests.exceptions.HTTPError, NotFound) as err:
        if isinstance(err, requests.exceptions.HTTPError):
            if err.response.status_code == 404:
                return False
        else:
            return False
        raise

    try:
        build = job.get_build(int(build_number))
    except (NotFound, ValueError):
        return False
    return build.is_running()


@check_env_vars
def start_build(
    job: Jobs,
    params: dict,
    block_until_building: bool = True,
    block_until_complete: bool = False,
    watch_building_delay: int = 5,
) -> Optional[str]:
    """
    Starts a new Jenkins build

    :param job: one of Jobs enum
    :param params: a key-value collection to be passed to the build
    :param block_until_building: True by default. Will block until the new build starts. This ensures
        triggered jobs are properly backlinked to parent jobs.
    :param block_until_complete: False by default. Will block until the new build completes
    :param watch_building_delay: Poll rate for building state

    Returns the build result if block_until_complete is True, None otherwise
    """

    init_jenkins()
    job_name = job.value
    logger.info('Starting new build for job: %s', job_name)
    job = jenkins_client.get_job(job_name)
    queue_item = job.invoke(build_params=params)

    if not (block_until_building or block_until_complete):
        logger.info('Queued new build for job: %s', job_name)
        return

    # Wait for the build to start
    triggered_build = wait_until_building(queue_item, job, watch_building_delay)

    if not block_until_complete:
        return None

    # Wait for the build to complete; get its status and return it
    logger.info('Waiting for build to complete...')
    triggered_build.block_until_complete()
    result = triggered_build.poll()['result']
    logger.info('Build completed with result: %s', result)
    return result


def start_ocp4(
    build_version: str, assembly: str, rpm_list: list, image_list: list, comment_on_pr: bool, **kwargs
) -> Optional[str]:
    params = {
        'BUILD_VERSION': build_version,
        'ASSEMBLY': assembly,
    }

    # If any rpm/image changed, force a build with only changed sources
    if rpm_list or image_list:
        params['PIN_BUILDS'] = True

    # Build only changed RPMs or none
    if rpm_list:
        params['BUILD_RPMS'] = 'only'
        params['RPM_LIST'] = ','.join(rpm_list)
    else:
        params['BUILD_RPMS'] = 'none'

    # Build only changed images or none
    if image_list:
        params['BUILD_IMAGES'] = 'only'
        params['IMAGE_LIST'] = ','.join(image_list)
    else:
        params['BUILD_IMAGES'] = 'none'

    if comment_on_pr:
        params['COMMENT_ON_PR'] = True

    # SKIP_PLASHETS defaults to True for manual builds, setting to False for scheduled
    params['SKIP_PLASHETS'] = False

    return start_build(
        job=Jobs.OCP4,
        params=params,
        **kwargs,
    )


def start_ocp4_konflux(
    build_version: str,
    assembly: str,
    image_list: list,
    rpm_list: list = None,
    limit_arches: list = None,
    **kwargs,
) -> Optional[str]:
    params = {
        'BUILD_VERSION': build_version,
        'ASSEMBLY': assembly,
    }

    # Build only changed images or none
    if image_list:
        params['IMAGE_LIST'] = ','.join(image_list)

    # Build changed RPMs if any
    if rpm_list:
        params['RPM_BUILD_STRATEGY'] = 'only'
        params['RPM_LIST'] = ','.join(rpm_list)

    # Limit arches when requested
    if limit_arches:
        params['LIMIT_ARCHES'] = ','.join(limit_arches)

    # SKIP_PLASHETS defaults to True for manual builds, setting to False for scheduled
    params['SKIP_PLASHETS'] = False

    return start_build(
        job=Jobs.OCP4_KONFLUX,
        params=params,
        **kwargs,
    )


def start_ocp4_scan(version: str, **kwargs) -> Optional[str]:
    params = {
        'VERSION': version,
    }
    return start_build(
        job=Jobs.OCP4_SCAN,
        params=params,
        **kwargs,
    )


def start_ocp4_scan_konflux(version: str, **kwargs) -> Optional[str]:
    params = {
        'VERSION': version,
    }
    return start_build(
        job=Jobs.OCP4_SCAN_KONFLUX,
        params=params,
        **kwargs,
    )


def start_rhcos(build_version: str, new_build: bool, job_name: str = 'build', **kwargs) -> Optional[str]:
    return start_build(
        job=Jobs.RHCOS,
        params={'BUILD_VERSION': build_version, 'NEW_BUILD': new_build, 'JOB_NAME': job_name},
        **kwargs,
    )


def start_build_sync(
    build_version: str,
    assembly: str,
    doozer_data_path: Optional[str] = None,
    doozer_data_gitref: Optional[str] = None,
    build_system: Optional[str] = 'brew',
    exclude_arches: list = None,
    **kwargs,
) -> Optional[str]:
    params = {
        'BUILD_VERSION': build_version,
        'ASSEMBLY': assembly,
    }
    if doozer_data_path:
        params['DOOZER_DATA_PATH'] = doozer_data_path
    if doozer_data_gitref:
        params['DOOZER_DATA_GITREF'] = doozer_data_gitref
    if exclude_arches:
        params['EXCLUDE_ARCHES'] = ','.join(exclude_arches)

    if build_system == 'brew':
        return start_build(
            job=Jobs.BUILD_SYNC,
            params=params | kwargs,
        )
    elif build_system == 'konflux':
        return start_build(
            job=Jobs.BUILD_SYNC_KONFLUX,
            params=params | kwargs,
        )


def start_cincinnati_prs(
    from_releases: list, release_name: str, advisory_id: int, candidate_pr_note: str, skip_ota_notification, **kwargs
) -> Optional[str]:
    return start_build(
        job=Jobs.CINCINNATI_PRS,
        params={
            'FROM_RELEASE_TAG': ','.join(from_releases),
            'RELEASE_NAME': release_name,
            'ADVISORY_NUM': advisory_id,
            'CANDIDATE_PR_NOTE': candidate_pr_note,
            'SKIP_OTA_SLACK_NOTIFICATION': skip_ota_notification,
            'GITHUB_ORG': 'openshift',
        },
        **kwargs,
    )


def start_build_microshift(build_version: str, assembly: str, dry_run: bool, **kwargs) -> Optional[str]:
    return start_build(
        job=Jobs.BUILD_MICROSHIFT,
        params={
            'BUILD_VERSION': build_version,
            'ASSEMBLY': assembly,
            'DRY_RUN': dry_run,
        },
        **kwargs,
    )


def start_olm_bundle(
    build_version: str,
    assembly: str,
    operator_nvrs: list,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
    **kwargs,
) -> Optional[str]:
    if not operator_nvrs:
        logger.warning('Empty operator NVR received: skipping olm-bundle')
        return

    return start_build(
        job=Jobs.OLM_BUNDLE,
        params={
            'BUILD_VERSION': build_version,
            'ASSEMBLY': assembly,
            'DOOZER_DATA_PATH': doozer_data_path,
            'DOOZER_DATA_GITREF': doozer_data_gitref,
            'OPERATOR_NVRS': ','.join(operator_nvrs),
        },
        **kwargs,
    )


def start_olm_bundle_konflux(
    build_version: str,
    assembly: str,
    operator_nvrs: list,
    doozer_data_path: str = constants.OCP_BUILD_DATA_URL,
    doozer_data_gitref: str = '',
    group: Optional[str] = None,
    **kwargs,
) -> Optional[str]:
    if not operator_nvrs:
        logger.warning('Empty operator NVR received: skipping olm-bundle')
        return

    params = {
        'BUILD_VERSION': build_version,
        'ASSEMBLY': assembly,
        'DOOZER_DATA_PATH': doozer_data_path,
        'DOOZER_DATA_GITREF': doozer_data_gitref,
        'OPERATOR_NVRS': ','.join(operator_nvrs),
    }

    if group:
        params['GROUP'] = group

    return start_build(
        job=Jobs.OLM_BUNDLE_KONFLUX,
        params=params,
        **kwargs,
    )


def start_sync_for_ci(version: str, **kwargs):
    return start_build(
        job=Jobs.SYNC_FOR_CI,
        params={
            'ONLY_FOR_VERSION': version,
        },
        **kwargs,
    )


def start_microshift_sync(version: str, assembly: str, dry_run: bool, **kwargs):
    return start_build(
        job=Jobs.MICROSHIFT_SYNC,
        params={
            'BUILD_VERSION': version,
            'ASSEMBLY': assembly,
            'DRY_RUN': dry_run,
        },
        **kwargs,
    )


def start_build_microshift_bootc(version: str, assembly: str, dry_run: bool, prepare_shipment: bool = True, **kwargs):
    return start_build(
        job=Jobs.BUILD_MICROSHIFT_BOOTC,
        params={
            'BUILD_VERSION': version,
            'ASSEMBLY': assembly,
            'DRY_RUN': dry_run,
            'PREPARE_SHIPMENT': prepare_shipment,
        },
        **kwargs,
    )


def start_rhcos_sync(release_tag_or_pullspec: str, dry_run: bool, **kwargs) -> Optional[str]:
    return start_build(
        job=Jobs.RHCOS_SYNC,
        params={
            'RELEASE_TAG': release_tag_or_pullspec,
            'DRY_RUN': dry_run,
        },
        **kwargs,
    )


def start_build_plashets(
    group, release, assembly, repos=None, data_path='', data_gitref='', copy_links=False, dry_run=False, **kwargs
) -> Optional[str]:
    params = {
        'GROUP': group,
        'RELEASE': release,
        'ASSEMBLY': assembly,
        'REPOS': ','.join(repos) if repos else '',
        'DATA_PATH': data_path,
        'DATA_GITREF': data_gitref,
        'COPY_LINKS': copy_links,
        'DRY_RUN': dry_run,
    }

    return start_build(
        job=Jobs.BUILD_PLASHETS,
        params=params,
        **kwargs,
    )


def start_build_fbc(
    version: str,
    assembly: str,
    operator_nvrs: list,
    dry_run: bool,
    force_build: Optional[bool] = None,
    group: Optional[str] = None,
    ocp_target_version: Optional[str] = None,
    **kwargs,
) -> Optional[str]:
    params = {
        'BUILD_VERSION': version,
        'ASSEMBLY': assembly,
        'OPERATOR_NVRS': ','.join(operator_nvrs),
        'DRY_RUN': dry_run,
    }
    if group:
        params['GROUP'] = group
    if ocp_target_version:
        params['OCP_TARGET_VERSION'] = ocp_target_version
    if force_build:
        params["FORCE_BUILD"] = force_build

    return start_build(
        job=Jobs.BUILD_FBC,
        params=params,
        **kwargs,
    )


def start_oadp(
    group: str,
    assembly: str,
    image_list: list = None,
    **kwargs,
) -> Optional[str]:
    params = {
        'GROUP': group,
        'ASSEMBLY': assembly,
    }

    # Build only changed images or none
    if image_list:
        params['IMAGE_LIST'] = ','.join(image_list)

    return start_build(
        job=Jobs.OADP,
        params=params,
        **kwargs,
    )


def start_oadp_scan_konflux(group: str, assembly: str = "stream", **kwargs) -> Optional[str]:
    params = {
        'GROUP': group,
        'ASSEMBLY': assembly,
    }

    return start_build(
        job=Jobs.OADP_SCAN,
        params=params,
        **kwargs,
    )


@check_env_vars
@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
def update_title(title: str, append: bool = True):
    """
    Set build title to <title>. If append is True, retrieve current title,
    append <title> and update. Otherwise, replace current title
    """

    job = jenkins_client.get_job(current_job_name)
    jenkins_url = get_jenkins_url()
    build = Build(
        url=current_build_url.replace(constants.JENKINS_UI_URL, jenkins_url),
        buildno=int(list(filter(None, current_build_url.split('/')))[-1]),
        job=job,
    )

    if append:
        title = build._data['displayName'] + title

    data = {'json': f'{{"displayName":"{title}"}}'}
    headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Referer': f"{build.baseurl}/configure"}
    build.job.jenkins.requester.post_url(f'{build.baseurl}/configSubmit', params=data, data='', headers=headers)


@check_env_vars
@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(10))
def update_description(description: str, append: bool = True):
    """
    Set build description to <description>. If append is True, retrieve current description,
    append <description> and update. Otherwise, replace current description
    """

    job = jenkins_client.get_job(current_job_name)
    jenkins_url = get_jenkins_url()
    build = Build(
        url=current_build_url.replace(constants.JENKINS_UI_URL, jenkins_url),
        buildno=int(list(filter(None, current_build_url.split('/')))[-1]),
        job=job,
    )

    if append:
        current_description = build.get_description()
        current_description = current_description if current_description else ''
        description = current_description + description

    set_build_description(build, description)


def is_api_reachable() -> bool:
    """
    Returns True if Jenkins API is reachable, False otherwise
    """
    if jenkins_client is None:
        return False
    try:
        jenkins_client.version
    except Exception:
        return False
    return True
