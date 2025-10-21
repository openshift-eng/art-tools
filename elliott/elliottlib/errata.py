"""Utility functions for general interactions with the errata SERVICE.

As a human, you will be working with "ADVISORIES". Advisories are one
or more errata (an errata is one or more erratum) as well as
associated metadata.

Classes representing an ERRATUM (a single errata)

"""

import datetime
import json
import re
import ssl
from functools import lru_cache
from typing import Dict, List

import click
import requests
from artcommonlib import logutil
from artcommonlib.format_util import green_print
from errata_tool import ErrataConnector, ErrataException, Erratum
from requests_gssapi import HTTPSPNEGOAuth
from tenacity import retry, stop_after_attempt, wait_fixed

from elliottlib import brew, bzutil, constants, exceptions
from elliottlib.util import chunk

logger = logutil.get_logger(__name__)

ErrataConnector._url = constants.errata_url


class Advisory(Erratum):
    """
    Wrapper class of errata_tool.Erratum
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def ensure_state(self, target_state):
        """
        Ensures that an Advisory is in a given state
        :param target_state: Desired target state
        :raises ValueError: ValueError if target_state is not a recognized state
        :raises ErrataException:
        """
        if target_state not in constants.errata_states:
            raise ValueError(f'Desired state {target_state} is not a valid Errata state {constants.errata_states}')
        if self.errata_state != target_state:
            self.setState(target_state)
            self.commit()

    def attach_builds(self, builds, kind):
        """
        Attach a list of builds to Advisory
        :param builds: List of brew builds
        :param kind: rpm or image
        :raises ValueError: When wrong kind
        :raises ErrataException:
        """
        click.echo(f"Attaching to advisory {self.errata_id}...")
        if kind not in {"rpm", "image"}:
            raise ValueError(f"{kind} should be one of 'rpm' or 'image'")

        file_type = 'tar' if kind == 'image' else 'rpm'
        product_version_set = {build.product_version for build in builds}
        for pv in product_version_set:
            self.addBuilds(
                buildlist=[build.nvr for build in builds if build.product_version == pv],
                release=pv,
                file_types={build.nvr: [file_type] for build in builds if build.product_version == pv},
            )

        build_nvrs = sorted(build.nvr for build in builds)
        green_print('Attached build(s) successfully:')
        click.echo(' '.join(build_nvrs))

    def set_cdn_repos(self, cdn_repos):
        """
        Configures CDN repos for Advisory
        :param cdn_repos: List of cdn repositories
        """
        click.echo(f"Configuring CDN repos {' '.join(cdn_repos)}")
        self.metadataCdnRepos(enable=cdn_repos)
        click.echo("Configured CDN repos successfully")

    def remove_builds(self, to_remove):
        """
        Remove list of builds from Advisory
        :param to_remove: List of NVRs to remove
        """
        click.echo(f"Removing build(s) from advisory {self.errata_id}: {' '.join(to_remove)}")
        self.removeBuilds(to_remove)
        green_print('Removed build(s) successfully')


def get_raw_erratum(advisory_id):
    """
    Retrieve the raw dictionary object that we get for an erratum,
    without wasting time processing it, loading builds, etc.
    """
    return ErrataConnector()._get(f"/api/v1/erratum/{advisory_id}")


def update_erratum(advisory_id, data):
    """
    Update an erratum
    """
    return ErrataConnector()._put(f"/api/v1/erratum/{advisory_id}", json=data)


def add_jira_issue(advisory_id, jira_issue_id):
    """
    Attach a jira issue to advisory
    Response code will return
    """
    return ErrataConnector()._post(f"/api/v1/erratum/{advisory_id}/add_jira_issue", json={'jira_issue': jira_issue_id})


def sync_jira_issue(jira_issue_id):
    """
    Sync a jira issue to advisory
    Response code will return
    """
    return ErrataConnector()._post("/api/v1/jira/refresh", json=[jira_issue_id])


def remove_jira_issue(advisory_id, jira_issue_id):
    """
    Remove a jira issue from advisory
    Response code will return
    """
    return ErrataConnector()._post(
        f"/api/v1/erratum/{advisory_id}/remove_jira_issue", json={'jira_issue': jira_issue_id}
    )


def remove_multi_jira_issues(advisory_id, jira_list: List):
    """
    Remove multi jira issues from advisory
    Return a list of response code
    """
    ec = ErrataConnector()
    res = []
    for jira_id in jira_list:
        res.append(ec._post(f"/api/v1/erratum/{advisory_id}/remove_jira_issue", json={'jira_issue': jira_id}))
    return res


def remove_bug(advisory_id, bug_id):
    """
    Remove a bug from advisory
    Response code will return
    """
    return ErrataConnector()._post(f"/api/v1/erratum/{advisory_id}/remove_bug", json={"bug": f"{bug_id}"})


def remove_multi_bugs(advisory_id, bug_list: List):
    """
    Remove multi bugs from advisory
    Return a list of response code
    """
    ec = ErrataConnector()
    res = []
    for bug_id in bug_list:
        res.append(ec._post(f"/api/v1/erratum/{advisory_id}/remove_bug", json={"bug": f"{bug_id}"}))
    return res


def add_multi_jira_issues(advisory_id, jira_list: List):
    """
    Add multi jira issues to advisory
    Return a list of response code
    """
    ec = ErrataConnector()
    res = []
    for jira_id in jira_list:
        res.append(ec._post(f"/api/v1/erratum/{advisory_id}/add_jira_issue", json={'jira_issue': jira_id}))
    return res


def get_jira_issue_from_advisory(advisory_id):
    """
    Get a list of jira issues from a advisory
    Will return a list of dict contains jira issue data
    """
    return ErrataConnector()._get(f"/advisory/{advisory_id}/jira_issues.json")


def get_jira_issue(jira_issue_id):
    """
    Get a jira issue by errata
    Will return a dict contains jira issue data
    """
    return ErrataConnector()._get(f"/jira_issues/{jira_issue_id}.json")


def get_bug_ids(advisory_id) -> dict:
    """
    Retrieve just the bug IDs from an advisory without wasting time processing it, loading builds, etc.
    :param advisory_id: The advisory ID
    :return: A dict with keys 'bugzilla' and 'jira' containing lists of bug IDs
    """
    raw_erratum = get_raw_erratum(advisory_id)
    bugzilla_ids = [bug['bug']['id'] for bug in raw_erratum['bugs']['bugs']]
    jira_ids = raw_erratum['jira_issues']['idsfixed']
    return {'bugzilla': bugzilla_ids, 'jira': jira_ids}


def get_erratum_content_type(advisory_id: str):
    raw_erratum = get_raw_erratum(advisory_id)
    erratum = raw_erratum.get('errata')
    for t in constants.ADVISORY_TYPES:
        data = erratum.get(t)
        if data is not None:
            return data.get('content_types')[0]
    return None


def new_erratum(
    et_data,
    errata_type,
    boilerplate_name,
    release_date=None,
    create=False,
    assigned_to=None,
    manager=None,
    package_owner=None,
):
    """5.2.1.1. POST /api/v1/erratum

    Create a new advisory.
    Takes an unrealized advisory object and related attributes using the following format:

    https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#advisories

    :param et_data: The ET data dump we got from our erratatool.yml file
    :param errata_type: The type of advisory to create (RHBA or RHEA)
    :param string boilerplate_name: The name of boilerplate template to use
        It is looked up from the erratatool.yml file
    :param string release_date: A date in the form YYYY-Mon-DD
    :param bool create: If true, create the erratum in the Errata
        tool, by default just the DATA we would have POSTed is
        returned
    :param string assigned_to: The email address of the group responsible for
        examining and approving the advisory entries
    :param string manager: The email address of the manager responsible for
        managing the contents and status of this advisory
    :param string package_owner: The email address of the person who is handling
        the details and status of this advisory

    :return: An Advisory object
    :raises: exceptions.ErrataToolUnauthenticatedException if the user is not authenticated to make the request
    """
    if errata_type not in ['RHBA', 'RHEA']:
        raise ValueError("errata_type must be one of 'RHBA' or 'RHEA'")

    if not release_date:
        release_date = datetime.datetime.now() + datetime.timedelta(days=21)

    if "boilerplates" not in et_data:
        raise ValueError("`boilerplates` is required in erratatool.yml")

    if boilerplate_name not in et_data["boilerplates"]:
        raise ValueError(f"Boilerplate {boilerplate_name} not found in erratatool.yml")

    boilerplate = et_data['boilerplates'][boilerplate_name]

    if 'release' in boilerplate:
        release = boilerplate['release']
    else:
        release = et_data['release']

    e = Advisory(
        product=et_data['product'],
        release=release,
        errata_type=errata_type,
        synopsis=boilerplate['synopsis'],
        topic=boilerplate['topic'],
        description=boilerplate['description'],
        solution=boilerplate['solution'],
        qe_email=assigned_to,
        qe_group=et_data['quality_responsibility_name'],
        owner_email=package_owner,
        manager_email=manager,
        date=release_date,
    )

    if create:
        # THIS IS NOT A DRILL
        e.commit()
        return e
    else:
        return e


def build_signed(build):
    """return boolean: is the build signed or not

    :param string build: The build nvr or id
    """
    filter_endpoint = constants.errata_get_build_url.format(id=build)
    res = requests.get(filter_endpoint, verify=ssl.get_default_verify_paths().openssl_cafile, auth=HTTPSPNEGOAuth())
    if res.status_code == 200:
        return res.json()['rpms_signed']
    elif res.status_code == 401:
        raise exceptions.ErrataToolUnauthenticatedException(res.text)
    else:
        raise exceptions.ErrataToolError(
            "Other error (status_code={code}): {msg}".format(code=res.status_code, msg=res.text)
        )


def get_art_release_from_erratum(advisory_id):
    erratum = get_raw_erratum(advisory_id)['errata']
    advisory_type_key = list(erratum.keys())[0]
    synopsis = erratum[advisory_type_key]['synopsis']
    # OpenShift Container Platform 4.14.z bug fix update
    match = re.search(r'OpenShift Container Platform (\d\.\d+)', synopsis)
    if match:
        return match.group(1)
    return None


def add_comment(advisory_id, comment):
    """5.2.1.8. POST /api/v1/erratum/{id}/add_comment

    Add a comment to an advisory.
    Example request body:

        {"comment": "This is my comment"}

    The response body is the updated or unmodified advisory, in the same format as GET /api/v1/erratum/{id}.

    https://errata.devel.redhat.com/developer-guide/api-http-api.html#api-post-apiv1erratumidadd_comment

    :param dict comment: The metadata object to add as a comment
    """
    data = {"comment": json.dumps(comment)}
    return requests.post(
        constants.errata_add_comment_url.format(id=advisory_id),
        verify=ssl.get_default_verify_paths().openssl_cafile,
        auth=HTTPSPNEGOAuth(),
        json=data,
    )


@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(60))
def get_builds(advisory_id, session=None):
    """5.2.2.6. GET /api/v1/erratum/{id}/builds
     Fetch the Brew builds associated with an advisory.
     Returned builds are organized by product version, variant, arch
    and include all the build files from the advisory.
     Returned attributes for the product version include:
    * name: name of the product version.
    * description: description of the product version.
     Returned attributes for each build include:
    * id: build's ID from Brew, Errata Tool also uses this as an internal ID
    * nvr: nvr of the build.
    * variant_arch: the list of files grouped by variant and arch.
     https://errata.devel.redhat.com/developer-guide/api-http-api.html#api-get-apiv1erratumidbuilds
    """
    if not session:
        session = requests.session()
    res = session.get(
        constants.errata_get_builds_url.format(id=advisory_id),
        verify=ssl.get_default_verify_paths().openssl_cafile,
        auth=HTTPSPNEGOAuth(),
    )
    if res.status_code == 200:
        return res.json()
    else:
        raise exceptions.ErrataToolUnauthorizedException(res.text)


# https://errata.devel.redhat.com/bugs/1743872/advisories.json


def get_brew_builds(errata_id, session=None):
    """5.2.2.1. GET /api/v1/erratum/{id}/builds

    Get Errata list of builds.

    https://errata.devel.redhat.com/developer-guide/api-http-api.html#api-get-apiv1erratumidbuilds
    :param str errata_id: the errata id
    :param requests.Session session: A python-requests Session object,
    used for for connection pooling. Providing `session` object can
    yield a significant reduction in total query time when looking up
    many builds.

    https://docs.python-requests.org/en/master/user/advanced/#session-objects

    :return: A List of initialized Build object with the build details
    :raises exceptions.BrewBuildException: When erratum return errors

    """
    if session is None:
        session = requests.session()

    res = session.get(
        constants.errata_get_builds_url.format(id=errata_id),
        verify=ssl.get_default_verify_paths().openssl_cafile,
        auth=HTTPSPNEGOAuth(),
    )
    brew_list = []
    if res.status_code == 200:
        jlist = res.json()
        for key in jlist.keys():
            for obj in jlist[key]['builds']:
                brew_list.append(brew.Build(nvr=list(obj.keys())[0], product_version=key))
        return brew_list
    else:
        raise exceptions.BrewBuildException("fetch builds from {id}: {msg}".format(id=errata_id, msg=res.text))


@retry(reraise=True, stop=stop_after_attempt(10), wait=wait_fixed(3))
def get_brew_build(nvr, product_version='', session=None) -> brew.Build:
    """5.2.2.1. GET /api/v1/build/{id_or_nvr}

    Get Brew build details.

    https://errata.devel.redhat.com/developer-guide/api-http-api.html#api-get-apiv1buildid_or_nvr

    :param str nvr: A name-version-release string of a brew rpm/image build
    :param str product_version: The product version tag as given to ET
    when attaching a build
    :param requests.Session session: A python-requests Session object,
    used for for connection pooling. Providing `session` object can
    yield a significant reduction in total query time when looking up
    many builds.

    https://docs.python-requests.org/en/master/user/advanced/#session-objects

    :return: An initialized Build object with the build details
    :raises exceptions.BrewBuildException: When build not found

    """
    if session is None:
        session = requests.Session()

    res = session.get(
        constants.errata_get_build_url.format(id=nvr),
        verify=ssl.get_default_verify_paths().openssl_cafile,
        auth=HTTPSPNEGOAuth(),
    )

    if res.status_code == 200:
        return brew.Build(nvr=nvr, body=res.json(), product_version=product_version)
    else:
        raise exceptions.BrewBuildException("{build}: {msg}".format(build=nvr, msg=res.text))


def get_advisories_for_bug(bug_id, session=None):
    """Fetch the list of advisories which a specified bug is attached to.

    5.2.26.7 /bugs/{id}/advisories.json

    :param bug_id: Bug ID
    :param session: Optional requests.Session
    """
    if not session:
        session = requests.session()
    r = session.get(
        constants.errata_get_advisories_for_bug_url.format(id=int(bug_id)),
        verify=ssl.get_default_verify_paths().openssl_cafile,
        auth=HTTPSPNEGOAuth(),
    )
    r.raise_for_status()
    return r.json()


def parse_exception_error_message(e):
    """
    :param e: exception messages (format is like 'Bug #1685399 The bug is filed already in RHBA-2019:1589.
        # Bug #1685398 The bug is filed already in RHBA-2019:1589.' )

    :return: [1685399, 1685398]
    """
    return [int(b.split('#')[1]) for b in re.findall(r'Bug #[0-9]*', str(e))]


def remove_bugzilla_bugs(advisory_obj, bugids: List):
    advisory_obj.removeBugs([bug for bug in bugids])
    advisory_obj.commit()


def add_bugzilla_bugs_with_retry(
    advisory: Erratum, bugids: List, noop: bool = False, batch_size: int = constants.BUG_ATTACH_CHUNK_SIZE
):
    """
    adding specified bugs into advisory, retry 2 times: first time
    parse the exception message to get failed bug id list, remove from original
    list then add bug to advisory again, if still has failures raise exceptions

    :param advisory: advisory object
    :param bugids: iterable of bugzilla bug ids to attach to advisory
    :param noop: do not modify anything
    :param batch_size: perform operation in batches of given size
    :return:
    """
    logger.info(f'Request to attach {len(bugids)} bugs to the advisory {advisory.errata_id}')
    if not advisory:
        raise exceptions.ElliottFatalError("Error: advisory object cannot be empty")

    existing_bugs = bzutil.BugzillaBugTracker.advisory_bug_ids(advisory)
    new_bugs = set(bugids) - set(existing_bugs)
    logger.info(f'New bugs (not already attached to advisory): {len(new_bugs)}')
    logger.debug(f'New bugs: {sorted(new_bugs)}')
    logger.debug(f'Bugs already attached: {sorted(set(bugids) & set(existing_bugs))}')
    if not new_bugs:
        return

    for chunk_of_bugs in chunk(list(new_bugs), batch_size):
        if noop:
            logger.info('Dry run: Would have attached bugs')
            continue
        try:
            advisory.addBugs(chunk_of_bugs)
            advisory.commit()
        except ErrataException as e:
            logger.info(f"ErrataException Message: {e}\nRetrying...")
            block_list = parse_exception_error_message(e)
            retry_list = [x for x in chunk_of_bugs if x not in block_list]
            if len(retry_list) == 0:
                continue
            try:
                advisory = Erratum(errata_id=advisory.errata_id)
                advisory.addBugs(retry_list)
                advisory.commit()
            except ErrataException as e:
                raise exceptions.ElliottFatalError(getattr(e, 'message', repr(e)))
            logger.info("remaining bugs attached")
        logger.info("All bugzilla bugs attached")


def add_jira_bugs_with_retry(
    advisory: Erratum, bugids: List[str], noop: bool = False, batch_size: int = constants.BUG_ATTACH_CHUNK_SIZE
):
    """
    :param advisory: advisory object
    :param bugids: iterable of jira bug ids to attach to advisory
    :param noop: do not modify anything
    :param batch_size: perform operation in batches of given size
    """
    logger.info(f'Request to attach {len(bugids)} bugs to the advisory {advisory.errata_id}')
    if not advisory:
        raise exceptions.ElliottFatalError("Error: advisory object cannot be empty")

    existing_bugs = bzutil.JIRABugTracker.advisory_bug_ids(advisory)
    new_bugs = set(bugids) - set(existing_bugs)
    logger.info(f'New bugs (not already attached to advisory): {len(new_bugs)}')
    logger.debug(f'New bugs: {sorted(new_bugs)}')
    logger.debug(f'Bugs already attached: {sorted(set(bugids) & set(existing_bugs))}')
    if not new_bugs:
        return
    for chunk_of_bugs in chunk(bugids, batch_size):
        if noop:
            logger.info('Dry run: Would have attached bugs')
            continue
        try:
            advisory.addJiraIssues(chunk_of_bugs)
            advisory.commit()
        except ErrataException as e:
            attached_bugs = re.findall("Issue (.*) The issue is filed already in", str(e))
            if attached_bugs:
                chunk_of_bugs = [b for b in chunk_of_bugs if b not in [b.upper() for b in attached_bugs]]
                advisory = Erratum(errata_id=advisory.errata_id)
                advisory.addJiraIssues(chunk_of_bugs)
                advisory.commit()
            else:
                raise e


def get_image_cdns(advisory_id):
    return ErrataConnector()._get(f'/api/v1/push_metadata/cdn_docker_file_list/{advisory_id}.json')


@lru_cache()  # advisories slow to look up, and not expected to change during a run
def get_cached_image_cdns(advisory_id):
    return get_image_cdns(advisory_id)


def get_advisory_images(image_advisory_id, raw=False):
    """List images of a given advisory, raw, or in the format we usually send to CCS (docs team)

    :param int image_advisory_id: ID of the main image advisory
    :param bool raw: Print undoctored artifact list

    :return: str with a list of images
    """
    cdn_docker_file_list = get_image_cdns(image_advisory_id)

    if raw:
        return '\n'.join(cdn_docker_file_list.keys())

    def _get_image_name(nvr, repo):
        name = next(iter(repo['docker']['target']['external_repos'].keys()), None)
        if not name:
            raise ValueError(
                f"Couldn't get repo name for {nvr}. Please open a ticket for CLOUDWF to set up the CDN repo."
            )
        return name

    def _get_vr(component):
        parts = component.split('-')
        return '{}-{}'.format(parts[-2], parts[-1])

    image_list = [
        '{}:{}'.format(_get_image_name(nvr, repo), _get_vr(nvr)) for nvr, repo in sorted(cdn_docker_file_list.items())
    ]

    return '#########\n{}\n#########'.format('\n'.join(image_list))


def get_advisory_builds(advisory, session=None):
    """
    :return: list of build dicts
    """
    try:
        builds = get_builds(advisory, session=session)
    except exceptions.ErrataToolError as ex:
        raise exceptions.ElliottFatalError(getattr(ex, 'message', repr(ex)))

    advisory_builds = []
    for tag in builds.keys():
        for build in builds[tag]['builds']:
            for name in build.keys():
                advisory_builds.append(build[name])

    return advisory_builds


def get_advisory_nvrs(advisory):
    """
    :return: dict, with keys as package names and values as strs in the form: '{version}-{release}'
    """
    try:
        builds = get_builds(advisory)
    except exceptions.ErrataToolError as ex:
        raise exceptions.ElliottFatalError(getattr(ex, 'message', repr(ex)))

    all_advisory_nvrs: Dict[str, str] = {}
    # Results come back with top level keys which are brew tags
    for tag in builds.keys():
        # Each top level has a key 'builds' which is a list of dicts
        for build in builds[tag]['builds']:
            # Each dict has a top level key which might be the actual
            # 'nvr' but I don't have enough data to know for sure
            # yet. Also I don't know when there might be more than one
            # key in the build dict. We'll loop over it to be sure.
            for name in build.keys():
                n, v, r = name.rsplit('-', 2)
                version_release = "{}-{}".format(v, r)
                all_advisory_nvrs[n] = version_release

    return all_advisory_nvrs


def get_all_advisory_nvrs(advisory):
    """
    :return: list of tuples (name, version, release)
    """
    try:
        builds = get_builds(advisory)
    except exceptions.ErrataToolError as ex:
        raise exceptions.ElliottFatalError(getattr(ex, 'message', repr(ex)))

    all_advisory_nvrs = []
    # Results come back with top level keys which are brew tags
    for tag in builds.keys():
        # Each top level has a key 'builds' which is a list of dicts
        for build in builds[tag]['builds']:
            for name in build.keys():
                n, v, r = name.rsplit('-', 2)
                all_advisory_nvrs.append((n, v, r))

    return all_advisory_nvrs


def get_advisory_nvrs_flattened(advisory: str | int) -> List[str]:
    """
    :return: get a flattened list of string nvrs for a given advisory
    """
    try:
        builds = get_builds(advisory)
    except exceptions.ErrataToolError as ex:
        raise exceptions.ElliottFatalError(getattr(ex, 'message', repr(ex)))

    all_advisory_nvrs = []
    # Results come back with top level keys which are brew tags
    for tag in builds.keys():
        # Each top level has a key 'builds' which is a list of dicts
        for build in builds[tag]['builds']:
            for name in build.keys():
                n, v, r = name.rsplit('-', 2)
                all_advisory_nvrs.append(f"{n}-{v}-{r}")

    return all_advisory_nvrs


def get_advisory(advisory_id):
    return ErrataConnector()._get(f'/api/v1/erratum/{advisory_id}')


def is_security_advisory(advisory):
    return advisory.errata_type == 'RHSA'


def is_advisory_impact_smaller_than(advisory, impact):
    i = [None] + constants.SECURITY_IMPACT
    return i.index(advisory.security_impact) < i.index(impact)


def set_blocking_advisory(target_advisory_id, blocking_advisory_id, blocking_state="SHIPPED_LIVE") -> dict:
    """Set a blocker advisory (at blocking state) for given target advisory

    :param target_advisory_id: advisory number of the target
    :param blocking_advisory_id: advisory number of the blocker
    :param blocking_state: a valid advisory state like "SHIPPED_LIVE" (default to "SHIPPED_LIVE")
    """
    response = ErrataConnector()._post(
        f'/api/v1/erratum/{target_advisory_id}/add_blocking_errata', json={"blocking_errata": blocking_advisory_id}
    )
    if response.status_code != requests.codes.created:
        # The endpoint 404s if the advisory is already in the list
        # with the error text "Advisory already listed"
        # so only warn if the error is something else
        if "Advisory already listed" not in response.text:
            logger.warning(
                f'Failed to set blocking advisory {blocking_advisory_id} for advisory {target_advisory_id}'
                f' with error: {response.text} status code: {response.status_code}'
            )
    data = {"blocking_errata": blocking_advisory_id, "blocker_state": blocking_state}
    response = ErrataConnector()._post(
        f'/api/v1/erratum/{target_advisory_id}/set_blocker_state_for_blocking_errata', json=data
    )
    if response.status_code != requests.codes.created:
        raise IOError(
            f'Failed to set blocking advisory {blocking_advisory_id} for advisory {target_advisory_id} '
            f'with error: {response.text} status code: {response.status_code}'
        )
    return response.json()


def get_blocking_advisories(advisory_id) -> List[int]:
    """Get a list of blocking advisory ids for a given advisory
    Raises IOError if the advisory or blocking_advisories not found

    :param advisory_id: advisory number
    :return: a list of advisory ids
    """
    advisory_id = int(advisory_id)
    errata = get_advisory(advisory_id)['errata']
    # This response is unnecessarily nested, so we need to dig into it
    """
    "errata": {
        "rhba": {
            "id": 110351,
            "blocking_advisories": [100, 200]
            ...
    """
    for k in errata:
        if errata[k]["id"] == advisory_id:
            return errata[k]['blocking_advisories']
    raise IOError(f'Failed to find blocking advisories for {advisory_id} in ET response: {errata}')


def get_dependent_advisories(advisory_id) -> List[int]:
    """Get a list of dependent advisory ids for a given advisory
    Raises IOError if the advisory or dependent_advisories not found

    :param advisory_id: advisory number
    :return: a list of advisory ids
    """
    advisory_id = int(advisory_id)
    errata = get_advisory(advisory_id)['errata']
    # This response is unnecessarily nested, so we need to dig into it
    """
    "errata": {
        "rhba": {
            "id": 110351,
            "dependent_advisories": [100, 200]
            ...
    """
    for k in errata:
        if errata[k]["id"] == advisory_id:
            return errata[k]['dependent_advisories']
    raise IOError(f'Failed to find dependent advisories for {advisory_id} in ET response: {errata}')


def remove_dependent_advisories(advisory_id):
    endpoint = f'/api/v1/erratum/{advisory_id}/remove_dependent_errata'
    for dependent in get_dependent_advisories(advisory_id):
        data = {"dependent_errata": int(dependent)}
        response = ErrataConnector()._post(endpoint, json=data)
        if response.status_code != requests.codes.created:
            raise IOError(
                f'Failed to remove dependent {dependent} from {advisory_id}'
                f'with code {response.status_code} and error: {response.text}'
            )


def remove_blocking_advisories_depends(advisory_id):
    # Remove advisory from blocking advisory's depends_on list
    for blocking_advisory in get_blocking_advisories(advisory_id):
        endpoint = f'/api/v1/erratum/{blocking_advisory}/remove_blocking_errata'
        data = {"blocking_errata": int(advisory_id)}
        response = ErrataConnector()._post(endpoint, json=data)
        if response.status_code != requests.codes.created:
            raise IOError(
                f'Failed to remove blocking {advisory_id} from {blocking_advisory}'
                f'with code {response.status_code} and error: {response.text}'
            )


def get_file_meta(advisory_id) -> List[dict]:
    """Get the metadata for all applicable files in this advisory (does not include builds)
    https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#api-get-apiv1erratumidfilemeta
    [{
    "file": {
      "id": 8820909,
      "path": "/mnt/redhat/brewroot/packages/rhcos-s390x/413.92.202307311416/0/images/coreos-assembler-git.tar.gz",
      "type": "tar",
      "arch": {
        "id": 8,
        "name": "noarch"
      }
    },
    "title": "RHCOS Image metadata (s390x)",
    "rank": 1
    },..]
    """
    return ErrataConnector()._get(f'/api/v1/erratum/{advisory_id}/filemeta')


def create_batch(release_version, release_date):
    """Create batch for a release
    https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#batches
    :param release_version: release name, e.g. 4.14.12
    :param release_date: release date, e.g. 2024-02-08
    :return: return the batch id which used as parameter when creating advisory

    """
    data = {
        "name": f"OCP {release_version}",
        "release_name": "RHOSE ASYNC - AUTO",
        "release_date": release_date,
        "description": f"OCP {release_version}",
        "is_active": True,
    }
    response = ErrataConnector()._post("/api/v1/batches", json=data)
    if response.status_code != requests.codes.created:
        raise IOError(f'Failed to create batch with code {response.status_code} and error: {response.text}')
    else:
        return response.json()['data']['id']


def lock_batch(batch_id):
    """Update lock status of a batch.
    PUT /api/v1/batches/{id}
    """
    response = ErrataConnector()._put(f'/api/v1/batches/{batch_id}', json={"is_locked": True})
    if response.status_code != requests.codes.ok:
        raise IOError(f'Failed to lock batch with code {response.status_code} and error: {response.text}')


def unlock_batch(batch_id):
    """Update lock status of a batch.
    PUT /api/v1/batches/{id}
    """
    response = ErrataConnector()._put(f'/api/v1/batches/{batch_id}', json={"is_locked": False})
    if response.status_code != requests.codes.ok:
        raise IOError(f'Failed to lock batch with code {response.status_code} and error: {response.text}')


def get_advisory_batch(advisory_id):
    """Get the batch id for an advisory."""
    erratum = get_raw_erratum(advisory_id)['errata']
    advisory_type_key = list(erratum.keys())[0]
    return erratum[advisory_type_key]['batch_id']


def set_advisory_batch(advisory_id, batch_id):
    """Set the batch for an advisory.
    POST /api/v1/erratum/{id}/change_batch
    Request body may contain:
        batch_id - id of the batch (integer)
        batch_name - name of the batch (string) - alternative to batch_id
        clear_batch - removes erratum from batch (boolean)
        is_batch_blocker - indicates if advisory blocks batch (boolean)
    """
    # Make sure the batch is unlocked
    unlock_batch(batch_id)

    # Set the batch
    response = ErrataConnector()._post(f'/api/v1/erratum/{advisory_id}/change_batch', json={"batch_id": batch_id})
    if response.status_code != requests.codes.created:
        raise IOError(f'Failed to set advisory batch with code {response.status_code} and error: {response.text}')

    # Lock the batch
    lock_batch(batch_id)


def unset_advisory_batch(advisory_id):
    """Unset the batch for an advisory.
    POST /api/v1/erratum/{id}/change_batch
    """
    batch_id = get_advisory_batch(advisory_id)
    if not batch_id:
        logger.info(f'No batch found for {advisory_id}')
        return

    # Make sure the batch is unlocked
    logger.info(f'Unlocking batch {batch_id} for advisory {advisory_id}')
    unlock_batch(batch_id)

    # Clear batch
    response = ErrataConnector()._post(f'/api/v1/erratum/{advisory_id}/change_batch', json={"clear_batch": True})
    logger.info(
        f'Attempted to remove advisory {advisory_id} from batch {batch_id}, got http status {response.status_code}'
    )
    if response.status_code != requests.codes.created:
        raise IOError(f'Failed to unset advisory batch with code {response.status_code} and error: {response.text}')

    # Lock the batch
    logger.info(f'Locking batch {batch_id} for advisory {advisory_id}')
    lock_batch(batch_id)


def put_file_meta(advisory_id, file_meta: dict) -> List[dict]:
    """Update the metadata for some or all files in this advisory.
    https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#api-put-apiv1erratumidfilemeta
    """
    return ErrataConnector()._put(f'/api/v1/erratum/{advisory_id}/filemeta?put_rank=true', json=file_meta)


def push_cdn_stage(advisory_id):
    """Trigger stage push for an advisory
    https://errata.devel.redhat.com/documentation/developer-guide/api-http-api.html#pushing-advisories
    """
    response = ErrataConnector()._post(
        f'/api/v1/erratum/{advisory_id}/push', json=[{"target": "cdn_stage"}, {"target": "cdn_docker_stage"}]
    )
    if response.status_code == 400 and "dependencies" in response.text:
        # if advisory has push dependencies then it will return 400, this is expected
        return None
    return response.json()


def is_advisory_editable(advisory_id: int) -> bool:
    erratum = get_raw_erratum(advisory_id)['errata']
    advisory_type_key = list(erratum.keys())[0]
    status = erratum[advisory_type_key]['status']
    return status in {"NEW_FILES", "QE"}
