import re
from typing import OrderedDict, Optional


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    else:
        return s[:]


def remove_prefixes(s: str, *args) -> str:
    for prefix in args:
        s = remove_prefix(s, prefix)
    return s


def remove_suffix(s: str, suffix: str) -> str:
    # suffix='' should not call self[:-0].
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    else:
        return s[:]


def convert_remote_git_to_https(source_url: str):
    """
    Accepts a source git URL in ssh or https format and return it in a normalized
    https format (:port on servers is not supported):
        - https protocol
        - no trailing /
    :param source_url: Git remote
    :return: Normalized https git URL
    """
    url = source_url.strip().rstrip('/')
    url = remove_prefixes(url, 'http://', 'https://', 'git://', 'git@', 'ssh://')
    url = remove_suffix(url, '.git')
    url = url.split('@', 1)[-1]  # Strip username@

    if url.find(':') > -1:
        server, org_repo = url.rsplit(':', 1)
    elif url.rfind('/') > -1:
        server, org_repo = url.rsplit('/', 1)
    else:
        return f'https://{url}'  # weird..

    return f'https://{server}/{org_repo}'


def convert_remote_git_to_ssh(url):
    """
    Accepts a remote git URL and turns it into a git@
    ssh form.
    :param url: The initial URL
    :return: A url in git@server:repo.git
    """
    server, org, repo_name = split_git_url(url)
    return f'git@{server}:{org}/{repo_name}.git'


def split_git_url(url) -> (str, str, str):
    """
    :param url: A remote ssh or https github url
    :return: Splits a github url into the server name, org, and repo name
    """
    https_normalized = convert_remote_git_to_https(url)
    url = https_normalized[8:]  # strip https://
    server, repo = url.split('/', 1)  # e.g. 'github.com', 'openshift/origin'
    org, repo_name = repo.split('/', 1)
    return server, org, repo_name


def merge_objects(a, b):
    """ Merges two, potentially deep, objects into a new one and returns the result.
    'a' is layered over 'b' and is dominant when necessary. The output is 'c'.
    """
    if not isinstance(a, dict) or not isinstance(b, dict):
        return a
    c: OrderedDict = b.copy()
    for k, v in a.items():
        c[k] = merge_objects(v, b.get(k))
        if k not in b:
            # move new entry to the beginning
            c.move_to_end(k, last=False)
    return c


def isolate_rhel_major_from_version(version: str) -> Optional[int]:
    """
    E.g. '9.2' => 9
    """

    match = re.fullmatch(r"(\d+).(\d+)", version)
    if match:
        return int(match[1])
    return None
