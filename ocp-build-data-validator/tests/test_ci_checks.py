"""Contracts for the shared LP/OCP CI checks, independent of remote services."""

import subprocess
from unittest.mock import Mock

import pytest
import requests
from validator import checks


def test_fbc_catalogs_check_each_unique_target_and_report_missing(mocker, tmp_path):
    inspect = mocker.patch('validator.checks.subprocess.run')
    inspect.side_effect = [
        subprocess.CompletedProcess([], 0, stdout='{}', stderr=''),
        subprocess.CompletedProcess([], 1, stdout='', stderr='reading manifest v5.2: manifest unknown'),
    ]
    auth = tmp_path / 'auth.json'
    errors = checks.validate_fbc_catalogs({'OCP_TARGET_VERSIONS': ['4.22', '5.2', '4.22']}, auth)
    assert errors == [
        'OCP_TARGET_VERSIONS: FBC catalog does not exist: registry.redhat.io/redhat/redhat-operator-index:v5.2'
    ]
    assert inspect.call_count == 2
    command = inspect.call_args_list[0].args[0]
    assert command[-1] == 'docker://registry.redhat.io/redhat/redhat-operator-index:v4.22'
    assert command[command.index('--authfile') + 1] == str(auth)
    assert inspect.call_args.kwargs['timeout'] == 60


@pytest.mark.parametrize('versions', ['4.22', [4.22], ['../bad'], [None], ['4.22.1']])
def test_fbc_catalogs_reject_invalid_target_versions_without_remote_calls(mocker, versions):
    inspect = mocker.patch('validator.checks.subprocess.run')
    assert checks.validate_fbc_catalogs({'OCP_TARGET_VERSIONS': versions})
    inspect.assert_not_called()


@pytest.mark.parametrize('group', [{}, {'OCP_TARGET_VERSIONS': []}])
def test_fbc_catalogs_skip_groups_without_targets(mocker, group):
    inspect = mocker.patch('validator.checks.subprocess.run')
    assert checks.validate_fbc_catalogs(group) == []
    inspect.assert_not_called()


@pytest.mark.parametrize('message', ['unauthorized: authentication required', 'connection timeout', 'HTTP 403'])
def test_fbc_catalogs_do_not_report_access_failures_as_missing(mocker, message):
    mocker.patch(
        'validator.checks.subprocess.run', return_value=subprocess.CompletedProcess([], 1, stdout='', stderr=message)
    )
    errors = checks.validate_fbc_catalogs({'OCP_TARGET_VERSIONS': ['4.22']})
    assert len(errors) == 1
    assert 'could not verify' in errors[0]
    assert 'does not exist' not in errors[0]


@pytest.mark.parametrize('error', [FileNotFoundError(), subprocess.TimeoutExpired('skopeo', 60)])
def test_fbc_catalogs_report_missing_tool_and_timeout(mocker, error):
    mocker.patch('validator.checks.subprocess.run', side_effect=error)
    assert checks.validate_fbc_catalogs({'OCP_TARGET_VERSIONS': ['4.22']})


def image():
    """Create an enabled image with private source, delivery and ownership metadata."""
    return {
        'name': 'product/operator',
        'owners': ['owner@redhat.com'],
        'delivery': {'delivery_repo_names': ['product/operator']},
        'content': {
            'source': {
                'git': {
                    'url': 'git@github.com:openshift-priv/operator.git',
                    'branch': {'target': 'release-{MAJOR}.{MINOR}', 'fallback': 'main'},
                }
            }
        },
    }


def test_metadata_rejects_empty_owners_and_wrong_delivery_name():
    """Metadata rejects empty owners and wrong delivery name."""
    data = image()
    data.update(owners=[' '], delivery={'delivery_repo_names': ['other/operator']})
    errors = checks.validate_image(data)
    assert any('owners' in error for error in errors)
    assert any('name' in error for error in errors)


def test_release_exclusions_do_not_disable_source_or_owners_checks():
    """Release exclusions do not disable source or owners checks."""
    data = image()
    data.update(for_release=False, owners=[])
    data.pop('delivery')
    assert checks.validate_image(data) == ['owners must contain at least one nonblank email address']


@pytest.mark.parametrize('path', ['/etc/passwd', '../Dockerfile', 'a/../../Dockerfile'])
def test_source_paths_cannot_escape_repository(path):
    """Source paths cannot escape repository."""
    data = image()
    data['content']['source']['dockerfile'] = path
    assert any('relative' in error for error in checks.validate_image(data))


def test_empty_source_path_means_repository_root():
    """An explicitly empty source path is equivalent to the repository root."""
    data = image()
    data['content']['source']['path'] = ''
    assert checks.validate_image(data) == []


def test_empty_dockerfile_is_not_a_valid_source_file():
    """Only the source directory may be empty, not a file path."""
    data = image()
    data['content']['source']['dockerfile'] = ''
    assert any('dockerfile' in error for error in checks.validate_image(data))


def test_reference_validation_reports_unknown_streams_and_missing_members():
    """Basic OCP member/stream validation remains available."""
    images = {'a': {'from': {'member': 'b'}}, 'b': {'from': {'member': 'a', 'builder': [{'stream': 'missing'}]}}}
    errors = checks.validate_references(images, {})
    assert any('missing' in error and 'stream' in error for error in errors)
    assert any('absent' in error for error in checks.validate_references({'a': {'from': {'member': 'absent'}}}, {}))


def test_sources_check_both_repositories_with_independent_fallback_and_default_dockerfile():
    """A public fallback must not replace the private target branch or its Dockerfile check."""
    client = Mock()
    client.exists.side_effect = lambda repo, path, ref, **kwargs: (
        not (repo == 'openshift/operator' and ref == 'release-4.22')
    )
    data = image()
    data['content']['source']['path'] = 'build'
    group = {
        'vars': {'MAJOR': 4, 'MINOR': 22},
        'public_upstreams': [
            {'private': 'https://github.com/openshift-priv', 'public': 'https://github.com/openshift'}
        ],
    }
    assert checks.validate_sources(data, group, client) == []
    calls = [call.args for call in client.exists.call_args_list]
    assert ('openshift-priv/operator', None, 'release-4.22') in calls
    assert ('openshift-priv/operator', 'build/Dockerfile', 'release-4.22') in calls
    assert ('openshift/operator', 'build/Dockerfile', 'main') in calls


def test_public_mapping_honors_longest_prefix_and_public_branch():
    """Public mapping honors longest prefix and public branch."""
    group = {
        'public_upstreams': [
            {'private': 'https://github.com/openshift-priv', 'public': 'https://github.com/openshift'},
            {
                'private': 'https://github.com/openshift-priv/operator',
                'public': 'https://github.com/team/upstream',
                'public_branch': 'stable',
            },
        ]
    }
    assert checks.source_repositories(image(), group) == [
        ('openshift-priv/operator', None),
        ('team/upstream', 'stable'),
    ]


def test_missing_dockerfile_is_reported_for_public_source():
    """Missing dockerfile is reported for public source."""
    client = Mock()
    client.exists.side_effect = lambda repo, path, ref, **kwargs: path is None
    assert any('Dockerfile' in e for e in checks.validate_sources(image(), {'vars': {'MAJOR': 4, 'MINOR': 22}}, client))


def test_rpm_sources_do_not_require_dockerfile():
    """Rpm sources do not require dockerfile."""
    client = Mock()
    client.exists.return_value = True
    assert checks.validate_sources(image(), {'vars': {'MAJOR': 4, 'MINOR': 22}}, client, image=False) == []
    assert all(call.args[1] is None for call in client.exists.call_args_list)


def admission(environment, components):
    """Build a release admission scoped to the test application and environment."""
    return {
        'kind': 'ReleasePlanAdmission',
        'spec': {
            'applications': ['product-1-0'],
            'data': {'intention': environment, 'mapping': {'components': components}},
        },
    }


def test_rpa_requires_both_environments_and_scopes_by_application(tmp_path):
    """Rpa requires both environments and scopes by application."""
    from ruamel.yaml import YAML

    yaml = YAML()
    component = {'name': 'product-1-0-operator', 'repositories': [{'url': 'registry.redhat.io/product/operator'}]}
    for environment in ('staging', 'production'):
        yaml.dump(admission(environment, [component]), tmp_path / f'{environment}.yaml')
    assert checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0') == []
    assert any('stage' in e for e in checks.validate_admissions({'operator': image()}, tmp_path, 'other-1-0'))
    (tmp_path / 'production.yaml').unlink()
    assert any('prod' in e for e in checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0'))


def test_rpa_detects_component_and_repository_mismatches(tmp_path):
    """Rpa detects component and repository mismatches."""
    from ruamel.yaml import YAML

    yaml = YAML()
    for environment in ('staging', 'production'):
        yaml.dump(
            admission(environment, [{'name': 'wrong', 'repositories': [{'url': 'registry.redhat.io/other/repo'}]}]),
            tmp_path / f'{environment}.yaml',
        )
    errors = checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0')
    assert any('product/operator' in error for error in errors)
    assert any('product-1-0-operator' in error for error in errors)


def test_github_api_encodes_refs_and_uses_timeout():
    """Github api encodes refs and uses timeout."""
    session = Mock()
    session.get.return_value.status_code = 200
    client = checks.GitHubClient(session, token='test-read-only')
    assert client.exists('team/operator', 'Dockerfile', 'release/1.0')
    args, kwargs = session.get.call_args
    assert args[0] == 'https://api.github.com/repos/team/operator/contents/Dockerfile'
    assert kwargs['params'] == {'ref': 'release/1.0'}
    assert kwargs['timeout'] == (10, 30)
    assert kwargs['allow_redirects'] is False


def test_api_access_errors_are_not_misreported_as_missing_branches():
    """Api access errors are not misreported as missing branches."""
    session = Mock()
    session.get.return_value.status_code = 403
    client = checks.GitHubClient(session, token='test-read-only')
    with pytest.raises(checks.RemoteError, match='403'):
        client.exists('team/operator', None, 'main')
    session.get.side_effect = requests.Timeout('secret URL must not appear')
    with pytest.raises(checks.RemoteError, match='unavailable') as error:
        client.exists('team/operator', None, 'main')
    assert 'secret' not in str(error.value)


def test_wip_images_do_not_block_reference_or_rpa_checks(tmp_path):
    """Wip images do not block reference or rpa checks."""
    data = image()
    data.update(mode='wip', owners=[], **{'from': {'member': 'missing'}})
    assert checks.validate_references({'operator': data}, {}) == []
    assert checks.validate_admissions({'operator': data}, tmp_path, 'product-1-0') == []


def test_malformed_references_report_errors_instead_of_crashing():
    """Malformed references report errors instead of crashing."""
    assert any('from' in error for error in checks.validate_references({'bad': {'from': ['not-a-mapping']}}, {}))


def test_dockerfile_cannot_be_the_source_directory():
    """Dockerfile cannot be the source directory."""
    data = image()
    data['content']['source']['dockerfile'] = '.'
    assert any('dockerfile' in error for error in checks.validate_image(data))


def test_stream_aliases_are_valid_build_references():
    """Stream aliases are valid build references."""
    images = {'operator': {'from': {'stream': 'golang-1.26'}}}
    assert checks.validate_references(images, {'golang': {'aliases': ['golang-1.26']}}) == []


def test_rpa_rejects_delivery_repository_under_another_component(tmp_path):
    """Presence elsewhere in an RPA must not satisfy the expected component mapping."""
    from ruamel.yaml import YAML

    components = [
        {'name': 'product-1-0-operator', 'repositories': [{'url': 'registry.redhat.io/wrong/operator'}]},
        {'name': 'product-1-0-other', 'repositories': [{'url': 'registry.redhat.io/product/operator'}]},
    ]
    for environment in ('staging', 'production'):
        YAML().dump(admission(environment, components), tmp_path / f'{environment}.yaml')
    errors = checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0')
    assert len(errors) == 2
    assert all('component product-1-0-operator missing delivery repository product/operator' in e for e in errors)


def test_malformed_admission_components_are_reported(tmp_path):
    """Malformed admission components are reported."""
    from ruamel.yaml import YAML

    YAML().dump(admission('staging', ['bad']), tmp_path / 'stage.yaml')
    assert any(
        'invalid' in error for error in checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0')
    )


def test_github_contents_type_must_match_dockerfile_or_manifests():
    """Github contents type must match dockerfile or manifests."""
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.return_value = [{'name': 'some-file'}]
    client = checks.GitHubClient(session, token='test-read-only')
    assert not client.exists('team/operator', 'Dockerfile', 'main', kind='file')
    assert client.exists('team/operator', 'bundle', 'main', kind='dir')
    session.get.return_value.json.return_value = {'type': 'file'}
    assert client.exists('team/operator', 'Containerfile', 'main', kind='file')
    assert not client.exists('team/operator', 'manifests', 'main', kind='dir')


def test_invalid_github_json_is_an_unavailable_result():
    """Invalid github json is an unavailable result."""
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.side_effect = requests.exceptions.JSONDecodeError('bad JSON', '', 0)
    with pytest.raises(checks.RemoteError, match='invalid response'):
        checks.GitHubClient(session, token='test-read-only').exists('team/operator', 'Dockerfile', 'main', kind='file')


def test_deep_dependency_graph_does_not_overflow_python_stack():
    """Deep dependency graph does not overflow python stack."""
    images = {f'image-{n}': {'from': {'member': f'image-{n + 1}'}} for n in range(1500)}
    images['image-1500'] = {'from': {}}
    assert checks.validate_references(images, {}) == []


def test_stream_aliases_expand_group_variables():
    """Stream aliases expand group variables."""
    images = {'operator': {'from': {'stream': 'golang-1.26'}}}
    assert (
        checks.validate_references(
            images, {'golang': {'aliases': ['golang-{GO_LATEST}']}}, variables={'GO_LATEST': '1.26'}
        )
        == []
    )


def test_bundle_repository_requires_its_own_bundle_component(tmp_path):
    """A bundle repository under the operator component must not satisfy the bundle mapping."""
    from ruamel.yaml import YAML

    data = image()
    data['delivery']['bundle_delivery_repo_name'] = 'product/operator-bundle'
    components = [
        {
            'name': 'product-1-0-operator',
            'repositories': [
                {'url': 'registry.redhat.io/product/operator'},
                {'url': 'registry.redhat.io/product/operator-bundle'},
            ],
        },
    ]
    for environment in ('staging', 'production'):
        YAML().dump(admission(environment, components), tmp_path / f'{environment}.yaml')
    errors = checks.validate_admissions({'operator': data}, tmp_path, 'product-1-0')
    assert any('stage RPA missing component product-1-0-operator-bundle' in e for e in errors)
    assert any('prod RPA missing component product-1-0-operator-bundle' in e for e in errors)


@pytest.mark.parametrize('exception', [{'mode': 'disabled'}, {'for_release': False}])
def test_rpa_preserves_original_release_exceptions(tmp_path, exception):
    """Disabled and non-release images require neither RPAs nor components."""
    data = image()
    data.update(exception)
    assert checks.validate_admissions({'operator': data}, tmp_path, 'openshift-4-22') == []


@pytest.mark.parametrize('missing', [None, 'stage-repo', 'prod-repo', 'bundle', 'component'])
def test_ocp_rpa_presence_contract(tmp_path, missing):
    """Missing delivery/bundle repos or components in either environment must fail."""
    from ruamel.yaml import YAML

    data = image()
    data['delivery']['bundle_delivery_repo_name'] = 'product/operator-bundle'
    for environment in ('stage', 'prod'):
        repos = [{'url': 'registry.redhat.io/product/operator'}]
        bundle_repos = [{'url': 'quay.io/product/operator-bundle'}]
        if missing == f'{environment}-repo':
            repos.pop(0)
        if missing == 'bundle':
            bundle_repos.clear()
        components = [
            {'name': 'wrong' if missing == 'component' else 'ose-4-22-my-operator', 'repositories': repos},
            {'name': 'ose-4-22-my-operator-bundle', 'repositories': bundle_repos},
        ]
        rpa = admission(environment, components)
        rpa['spec']['applications'] = ['openshift-4-22']
        rpa['spec']['data'].pop('intention')
        rpa['metadata'] = {'name': f'ocp-art-advisory-{environment}-4-22'}
        YAML().dump(rpa, tmp_path / f'{environment}.yaml')
    errors = checks.validate_admissions({'my_operator': data}, tmp_path, 'openshift-4-22')
    if missing is None:
        assert errors == []
    elif missing == 'component':
        assert any('missing component ose-4-22-my-operator' in e for e in errors)
    else:
        expected_repo = 'product/operator-bundle' if missing == 'bundle' else 'product/operator'
        assert any(f'missing delivery repository {expected_repo}' in e for e in errors)


def test_bundle_component_uses_top_level_short_name_override():
    """Bundle component uses top level short name override."""
    assert (
        checks.component_name('product-1-0', 'operator', {'bundle_name_override': 'custom'}, bundle=True)
        == 'product-1-0-custom'
    )


@pytest.mark.parametrize(
    'url',
    [
        'https://github.com/org/.',
        'https://github.com/org/..',
        'https://github.com/./repo',
        'https://github.com/../repo',
        'git@github.com:org/...git',
    ],
)
def test_repository_dot_segments_are_rejected(url):
    """Repository URLs must not change the target API route through dot segments."""
    with pytest.raises(ValueError, match='GitHub URL'):
        checks.repository_name(url)


@pytest.mark.parametrize('path', ['/other-repo', '../../other-repo', 'a/../../../other-repo'])
def test_manifests_paths_are_rejected_before_network_access(path):
    """Unsafe bundle paths must fail locally without making an authenticated request."""
    data = image()
    data['update-csv'] = {'manifests-dir': path}
    assert any('manifests-dir' in error for error in checks.validate_image(data))
    client = Mock()
    assert any('manifests-dir' in error for error in checks.validate_sources(data, {}, client))
    client.exists.assert_not_called()


@pytest.mark.parametrize('repository,path', [('org/..', 'Dockerfile'), ('org/repo', '../../other')])
def test_github_client_rejects_unsafe_api_routes(repository, path):
    """The authenticated client independently guards its repository and contents route."""
    session = Mock()
    with pytest.raises(ValueError):
        checks.GitHubClient(session, token='test-read-only').exists(repository, path, 'main')
    session.get.assert_not_called()


def test_relative_manifests_directory_is_valid():
    """Source subdirectories and bundle directory checks continue to work together."""
    data = image()
    data['content']['source']['path'] = 'src'
    data['update-csv'] = {'manifests-dir': 'bundle/manifests'}
    data['content']['source']['git']['branch'] = {'target': 'main'}
    client = Mock()
    client.exists.return_value = True
    assert checks.validate_image(data) == []
    assert checks.validate_sources(data, {}, client) == []
    client.exists.assert_any_call('openshift-priv/operator', 'src/bundle/manifests', 'main', kind='dir')


def test_pull_url_override_also_checks_the_actual_fork_source():
    """A fork does not exempt either declared source repository from validation."""
    data = image()
    data['content']['source']['git']['url_pull'] = 'https://github.com/team/operator-fork.git'
    group = {
        'public_upstreams': [{'private': 'https://github.com/openshift-priv', 'public': 'https://github.com/openshift'}]
    }
    assert checks.source_repositories(data, group) == [
        ('openshift-priv/operator', None),
        ('openshift/operator', None),
        ('team/operator-fork', None),
    ]


def test_unsafe_pull_url_is_rejected_before_network_access():
    """A pull override cannot introduce a non-GitHub host or an ambiguous API route."""
    for url in ('https://other-host/repo', 'https://github.com/team/..'):
        data = image()
        data['content']['source']['git']['url_pull'] = url
        client = Mock()
        errors = checks.validate_sources(data, {'vars': {'MAJOR': 4, 'MINOR': 18}}, client)
        assert any('GitHub URL' in error for error in errors)
        client.exists.assert_not_called()


@pytest.mark.parametrize(
    'images,variables,cycle',
    [
        ({'a': {'from': {'member': 'a'}}}, {}, 'a -> a'),
        ({'a': {'from': {'member': 'b'}}, 'b': {'from': {'member': 'a'}}}, {}, 'a -> b -> a'),
        (
            {'a': {'from': {'member': 'base-rhel{RHEL}'}}, 'base-rhel9': {'from': {'member': 'a'}}},
            {'RHEL': 9},
            'a -> base-rhel9 -> a',
        ),
    ],
)
def test_parent_cycles_are_rejected(images, variables, cycle):
    """Parent cycles already rejected by Doozer should fail metadata validation too."""
    assert checks.validate_references(images, {}, variables) == [f'Parent member cycle: {cycle}']


def test_shared_parent_is_not_a_cycle():
    images = {'a': {'from': {'member': 'base'}}, 'b': {'from': {'member': 'base'}}, 'base': {}}
    assert checks.validate_references(images, {}) == []


def test_wip_parent_cycles_remain_nonblocking():
    images = {'a': {'from': {'member': 'b'}}, 'b': {'mode': 'wip', 'from': {'member': 'a'}}}
    assert checks.validate_references(images, {}) == []


@pytest.mark.parametrize(
    'path,kind,url',
    [
        (None, None, 'https://github.com/team/operator/tree/release%2F1.0'),
        ('src/Dockerfile', 'file', 'https://github.com/team/operator/blob/release%2F1.0/src/Dockerfile'),
        ('bundle', 'dir', 'https://github.com/team/operator/tree/release%2F1.0/bundle'),
    ],
)
def test_anonymous_checks_use_existing_ocp_web_url_pattern(path, kind, url):
    """A tokenless CI job checks web URLs rather than consuming the anonymous REST quota."""
    session = Mock()
    session.head.return_value.status_code = 200
    client = checks.GitHubClient(session)
    assert client.exists('team/operator', path, 'release/1.0', kind=kind)
    assert client.exists('team/operator', path, 'release/1.0', kind=kind)
    session.head.assert_called_once_with(url, timeout=(10, 30), allow_redirects=False)
    session.get.assert_not_called()


def test_anonymous_missing_or_inaccessible_source_is_not_silently_skipped():
    """No credentials must not turn an inaccessible private source into a successful check."""
    session = Mock()
    session.head.return_value.status_code = 404
    assert not checks.GitHubClient(session).exists('team/operator', None, 'main')
    session.head.return_value.status_code = 403
    with pytest.raises(checks.RemoteError, match='403'):
        checks.GitHubClient(session).exists('team/operator', None, 'main')


@pytest.mark.parametrize(
    'original,public,expected',
    [
        (
            'https://github.com/openshift-priv/stolostron-acm-cli.git',
            'https://github.com/stolostron',
            'stolostron/acm-cli',
        ),
        ('https://github.com/openshift-priv/operator.git', 'https://github.com/openshift', 'openshift/operator'),
    ],
)
def test_public_upstream_is_checked_alongside_private_source(original, public, expected):
    """Public mappings add a source check without replacing the private midstream."""
    data = image()
    data['content']['source']['git']['url'] = original
    group = {'public_upstreams': [{'private': 'https://github.com/openshift-priv', 'public': public}]}
    assert checks.source_repositories(data, group) == [(checks.repository_name(original), None), (expected, None)]


def test_unmapped_source_is_still_checked():
    """No mapping must not silently exempt a source from branch/Dockerfile validation."""
    assert checks.source_repositories(image(), {}) == [('openshift-priv/operator', None)]


def test_mapping_branch_override_applies_even_when_repository_name_is_unchanged():
    """A mapping can select the public branch of the same repository."""
    group = {
        'public_upstreams': [
            {
                'private': 'https://github.com/openshift-priv/operator',
                'public': 'https://github.com/openshift-priv/operator',
                'public_branch': 'public-release',
            }
        ]
    }
    assert checks.source_repositories(image(), group) == [
        ('openshift-priv/operator', None),
        ('openshift-priv/operator', 'public-release'),
    ]


@pytest.mark.parametrize(
    'parent',
    [
        {'member': 'base-{EL}'},
        {'stream': 'rhel-{EL}-golang-{GO_LATEST}'},
        {'builder': [{'member': 'base-{EL}'}, {'stream': 'rhel-{EL}-golang-{GO_LATEST}'}]},
    ],
)
def test_image_references_expand_group_variables_before_lookup(parent):
    """Valid templated parent and builder references must not be reported missing."""
    images = {'operator': {'from': parent}, 'base-9': {}}
    streams = {'rhel-9-golang-1.26': {}}
    assert checks.validate_references(images, streams, {'EL': 9, 'GO_LATEST': '1.26'}) == []


@pytest.mark.parametrize('field', ['member', 'stream'])
def test_image_reference_undefined_variable_is_a_validation_error(field):
    """Undefined image-reference variables must be reported locally without crashing."""
    errors = checks.validate_references({'operator': {'from': {field: 'base-{MISSING}'}}}, {}, {})
    assert any('operator' in error and 'undefined group variable MISSING' in error for error in errors)


@pytest.mark.parametrize('mapping', [False, True])
def test_explicit_web_selects_public_source_instead_of_inferred_repository(mapping):
    data = image()
    data['content']['source']['git']['web'] = 'https://github.com/team/actual-upstream'
    group = (
        {
            'public_upstreams': [
                {
                    'private': 'https://github.com/openshift-priv',
                    'public': 'https://github.com/openshift',
                    'public_branch': 'stable',
                }
            ]
        }
        if mapping
        else {}
    )
    assert checks.source_repositories(data, group) == [
        ('openshift-priv/operator', None),
        ('team/actual-upstream', 'stable' if mapping else None),
    ]


@pytest.mark.parametrize('missing_repository', ['openshift-priv/operator', 'team/upstream'])
@pytest.mark.parametrize('missing_path', [None, 'src/Containerfile', 'src/manifests'])
def test_either_declared_source_can_fail_branch_or_file_validation(missing_repository, missing_path):
    data = image()
    data['content']['source'].update(path='src', dockerfile='Containerfile')
    data['update-csv'] = {'manifests-dir': 'manifests'}
    data['content']['source']['git'].update(web='https://github.com/team/upstream', branch={'target': 'main'})
    client = Mock()
    client.exists.side_effect = lambda repo, path, ref, **kwargs: (repo, path) != (missing_repository, missing_path)
    errors = checks.validate_sources(data, {}, client)
    assert len(errors) == 1
    assert missing_repository in errors[0]
    if missing_path:
        assert missing_path in errors[0]
    assert {call.args[0] for call in client.exists.call_args_list} == {'openshift-priv/operator', 'team/upstream'}


@pytest.mark.parametrize('web', ['https://other-host/team/repo', 'https://github.com/team/..'])
def test_unsafe_web_url_is_rejected_before_network_access(web):
    data = image()
    data['content']['source']['git']['web'] = web
    client = Mock()
    errors = checks.validate_sources(data, {'vars': {'MAJOR': 4, 'MINOR': 22}}, client)
    assert any('GitHub URL' in error for error in errors)
    client.exists.assert_not_called()


def test_identical_url_and_web_do_not_duplicate_source_checks():
    data = image()
    data['content']['source']['git']['web'] = 'https://github.com/openshift-priv/operator'
    assert checks.source_repositories(data, {}) == [('openshift-priv/operator', None)]


def test_private_access_failure_does_not_skip_public_source_checks():
    data = image()
    data['content']['source']['git'].update(web='https://github.com/team/upstream', branch={'target': 'main'})
    client = Mock()
    client.exists.side_effect = [checks.RemoteError('private source access denied'), True, True]
    assert checks.validate_sources(data, {}, client) == ['private source access denied']
    client.exists.assert_any_call('team/upstream', 'Dockerfile', 'main', kind='file')


@pytest.mark.parametrize('overrides', [False, True])
@pytest.mark.parametrize('swapped', [False, True])
def test_rpa_binds_image_and_bundle_repositories_to_their_components(tmp_path, overrides, swapped):
    from ruamel.yaml import YAML

    data = image()
    data['delivery']['bundle_delivery_repo_name'] = 'product/operator-bundle'
    image_component, bundle_component = 'product-1-0-operator', 'product-1-0-operator-bundle'
    if overrides:
        image_component, bundle_component = 'custom-image', 'custom-bundle'
        data['konflux'] = {'component': {'name_override': image_component, 'bundle_name_override': bundle_component}}
    repositories = ['product/operator', 'product/operator-bundle']
    if swapped:
        repositories.reverse()
    components = [
        {'name': name, 'repositories': [{'url': f'registry.redhat.io/{repo}'}]}
        for name, repo in zip((image_component, bundle_component), repositories)
    ]
    for environment in ('staging', 'production'):
        YAML().dump(admission(environment, components), tmp_path / f'{environment}.yaml')
    errors = checks.validate_admissions({'operator': data}, tmp_path, 'product-1-0')
    if swapped:
        assert len(errors) == 4
        assert all('missing delivery repository' in error for error in errors)
    else:
        assert errors == []
