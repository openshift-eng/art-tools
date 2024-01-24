from . import support


def validate(file, data, group_cfg):
    endpoint = get_cgit_endpoint(group_cfg)
    namespace = support.get_namespace(data, file)
    repository = support.get_repository_name(file)

    url = '{}/{}/{}'.format(endpoint, namespace, repository)

    if not support.resource_is_reachable(endpoint):
        return (url, ('This validation must run from a network '
                      'with access to {}'.format(endpoint)))

    if not support.resource_exists(url):
        return (url, ('Repo was not found in CGit cache.\n'
                      "If you didn't request a DistGit repo yet, "
                      'please check https://mojo.redhat.com/docs/DOC-1168290\n'
                      'But if you already obtained one, make sure its name '
                      'matches the YAML filename'))

    branch = support.get_distgit_branch(data, group_cfg)
    if not branch_exists(branch, url):
        return (url, ('Branch {} not found in CGit cache'.format(branch)))

    return (url, None)


def get_cgit_endpoint(group_cfg):
    return group_cfg['urls']['cgit']


def branch_exists(branch, url):
    return support.resource_exists('{}/log?h={}'.format(url, branch))
