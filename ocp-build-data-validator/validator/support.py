import os

import requests
from ruamel.yaml import YAML
from tenacity import retry, retry_if_result, stop_after_attempt, wait_exponential

from . import exceptions, global_session


def fail_validation(msg, parsed):
    if not parsed:
        raise exceptions.ValidationFailed(msg)
    if 'mode' in parsed and parsed['mode'] == 'wip':
        raise exceptions.ValidationFailedWIP(msg)
    raise exceptions.ValidationFailed(msg)


def is_disabled(parsed):
    return 'mode' in parsed and parsed['mode'] == 'disabled'


def load_group_config_for(file):
    group_yaml = os.path.join(get_ocp_build_data_dir(file), 'group.yml')
    return YAML(typ='safe').load(open(group_yaml).read())


def get_ocp_build_data_dir(file):
    file_path = os.path.dirname(file)
    if os.path.exists(os.path.join(file_path, 'group.yml')):
        # File like releases.yml is already co-resident with group.yml
        obd_dir = file_path
    else:
        # image and rpm metas
        obd_dir = os.path.join(file_path, '..')
    return os.path.normpath(obd_dir)


def get_artifact_type(file):
    if os.path.basename(file) == 'build-priority.yml':
        return 'build-priority'

    if 'releases.yml' in file:
        return 'releases'

    if 'streams.yml' in file:
        return 'streams'

    if 'binaries/' in file:
        return 'binary'

    if 'images/' in file:
        return 'image'

    if 'rpms/' in file:
        return 'rpm'

    if 'shipment' in file:
        return 'shipment'

    if 'group.yml' in file:
        return 'group'

    if any([x in file for x in ['erratatool.yml', 'bug.yml', 'bugzilla.yml']]):
        return 'ignore'

    return '???'


def get_valid_streams_for(file):
    streams_yaml = os.path.join(get_ocp_build_data_dir(file), 'streams.yml')
    return set(YAML(typ='safe').load(open(streams_yaml).read()).keys())


def get_valid_member_references_for(file):
    images_dir = os.path.join(get_ocp_build_data_dir(file), 'images')
    return set([os.path.splitext(img)[0] for img in os.listdir(images_dir)])


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    retry=retry_if_result(lambda response: response.status_code == 429),
)
def _head(url):
    session = global_session.request_session or requests
    return session.head(url)


def resource_exists(url):
    if url.startswith('https://github.com/openshift/ose-ovn-kubernetes'):
        # This is a private repository, and only used for 3.11. This will not change.
        return True
    return 200 <= _head(url).status_code < 400


def get_repository_name(file):
    return os.path.basename(file).split('.')[0]


def replace_vars(text, vars_map):
    return text.replace('{MAJOR}', str(vars_map['MAJOR'])).replace('{MINOR}', str(vars_map['MINOR']))
