"""Contracts for the shared LP/OCP CI checks, independent of remote services."""

from unittest.mock import Mock

import pytest
import requests
from validator import checks


def image():
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
    data = image()
    data.update(owners=[' '], delivery={'delivery_repo_names': ['other/operator']})
    errors = checks.validate_image(data)
    assert any('owners' in error for error in errors)
    assert any('name' in error for error in errors)


def test_release_exclusions_do_not_disable_source_or_owners_checks():
    data = image()
    data.update(for_release=False, owners=[])
    data.pop('delivery')
    assert checks.validate_image(data) == ['owners must contain at least one nonblank email address']


@pytest.mark.parametrize('path', ['/etc/passwd', '../Dockerfile', 'a/../../Dockerfile'])
def test_source_paths_cannot_escape_repository(path):
    data = image()
    data['content']['source']['dockerfile'] = path
    assert any('relative' in error for error in checks.validate_image(data))


def test_dependency_validation_reports_unknown_streams_missing_members_and_cycles():
    images = {'a': {'from': {'member': 'b'}}, 'b': {'from': {'member': 'a', 'builder': [{'stream': 'missing'}]}}}
    errors = checks.validate_references(images, {})
    assert any('cycle' in error for error in errors)
    assert any('missing' in error and 'stream' in error for error in errors)
    assert any('absent' in error for error in checks.validate_references({'a': {'from': {'member': 'absent'}}}, {}))


def test_sources_check_private_and_public_with_independent_fallback_and_default_dockerfile():
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
    assert ('openshift-priv/operator', 'build/Dockerfile', 'release-4.22') in calls
    assert ('openshift/operator', 'build/Dockerfile', 'main') in calls


def test_public_mapping_honors_longest_prefix_and_public_branch():
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
    client = Mock()
    client.exists.side_effect = lambda repo, path, ref, **kwargs: path is None
    assert any('Dockerfile' in e for e in checks.validate_sources(image(), {'vars': {'MAJOR': 4, 'MINOR': 22}}, client))


def test_rpm_sources_do_not_require_dockerfile():
    client = Mock()
    client.exists.return_value = True
    assert checks.validate_sources(image(), {'vars': {'MAJOR': 4, 'MINOR': 22}}, client, image=False) == []
    assert all(call.args[1] is None for call in client.exists.call_args_list)


def admission(environment, components):
    return {
        'kind': 'ReleasePlanAdmission',
        'spec': {
            'applications': ['product-1-0'],
            'data': {'intention': environment, 'mapping': {'components': components}},
        },
    }


def test_rpa_requires_both_environments_and_scopes_by_application(tmp_path):
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
    session = Mock()
    session.get.return_value.status_code = 200
    client = checks.GitHubClient(session)
    assert client.exists('team/operator', 'Dockerfile', 'release/1.0')
    args, kwargs = session.get.call_args
    assert args[0] == 'https://api.github.com/repos/team/operator/contents/Dockerfile'
    assert kwargs['params'] == {'ref': 'release/1.0'}
    assert kwargs['timeout'] == (10, 30)
    assert kwargs['allow_redirects'] is False


def test_api_access_errors_are_not_misreported_as_missing_branches():
    session = Mock()
    session.get.return_value.status_code = 403
    client = checks.GitHubClient(session)
    with pytest.raises(checks.RemoteError, match='403'):
        client.exists('team/operator', None, 'main')
    session.get.side_effect = requests.Timeout('secret URL must not appear')
    with pytest.raises(checks.RemoteError, match='unavailable') as error:
        client.exists('team/operator', None, 'main')
    assert 'secret' not in str(error.value)


def test_wip_images_do_not_block_reference_or_rpa_checks(tmp_path):
    data = image()
    data.update(mode='wip', owners=[], **{'from': {'member': 'missing'}})
    assert checks.validate_references({'operator': data}, {}) == []
    assert checks.validate_admissions({'operator': data}, tmp_path, 'product-1-0') == []


def test_malformed_references_report_errors_instead_of_crashing():
    assert any('from' in error for error in checks.validate_references({'bad': {'from': ['not-a-mapping']}}, {}))


def test_dockerfile_cannot_be_the_source_directory():
    data = image()
    data['content']['source']['dockerfile'] = '.'
    assert any('dockerfile' in error for error in checks.validate_image(data))


def test_stream_aliases_are_valid_build_references():
    images = {'operator': {'from': {'stream': 'golang-1.26'}}}
    assert checks.validate_references(images, {'golang': {'aliases': ['golang-1.26']}}) == []


def test_rpa_does_not_accept_delivery_repository_assigned_to_another_component(tmp_path):
    from ruamel.yaml import YAML

    components = [
        {'name': 'product-1-0-operator', 'repositories': [{'url': 'registry.redhat.io/wrong/operator'}]},
        {'name': 'product-1-0-other', 'repositories': [{'url': 'registry.redhat.io/product/operator'}]},
    ]
    for environment in ('staging', 'production'):
        YAML().dump(admission(environment, components), tmp_path / f'{environment}.yaml')
    assert any(
        'product/operator' in error
        for error in checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0')
    )


def test_malformed_admission_components_are_reported(tmp_path):
    from ruamel.yaml import YAML

    YAML().dump(admission('staging', ['bad']), tmp_path / 'stage.yaml')
    assert any(
        'invalid' in error for error in checks.validate_admissions({'operator': image()}, tmp_path, 'product-1-0')
    )


def test_github_contents_type_must_match_dockerfile_or_manifests():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.return_value = [{'name': 'some-file'}]
    client = checks.GitHubClient(session)
    assert not client.exists('team/operator', 'Dockerfile', 'main', kind='file')
    assert client.exists('team/operator', 'bundle', 'main', kind='dir')
    session.get.return_value.json.return_value = {'type': 'file'}
    assert client.exists('team/operator', 'Containerfile', 'main', kind='file')
    assert not client.exists('team/operator', 'manifests', 'main', kind='dir')


def test_invalid_github_json_is_an_unavailable_result():
    session = Mock()
    session.get.return_value.status_code = 200
    session.get.return_value.json.side_effect = requests.exceptions.JSONDecodeError('bad JSON', '', 0)
    with pytest.raises(checks.RemoteError, match='invalid response'):
        checks.GitHubClient(session).exists('team/operator', 'Dockerfile', 'main', kind='file')


def test_deep_dependency_graph_does_not_overflow_python_stack():
    images = {f'image-{n}': {'from': {'member': f'image-{n + 1}'}} for n in range(1500)}
    images['image-1500'] = {'from': {}}
    assert checks.validate_references(images, {}) == []


def test_stream_aliases_expand_group_variables():
    images = {'operator': {'from': {'stream': 'golang-1.26'}}}
    assert (
        checks.validate_references(
            images, {'golang': {'aliases': ['golang-{GO_LATEST}']}}, variables={'GO_LATEST': '1.26'}
        )
        == []
    )


def test_bundle_mapping_must_belong_to_expected_bundle_component(tmp_path):
    from ruamel.yaml import YAML

    data = image()
    data['delivery']['bundle_delivery_repo_name'] = 'product/operator-bundle'
    data['konflux'] = {'component': {'bundle_name_override': 'custom-bundle'}}
    components = [
        {'name': 'product-1-0-operator', 'repositories': [{'url': 'registry.redhat.io/product/operator'}]},
        {'name': 'unrelated', 'repositories': [{'url': 'registry.redhat.io/product/operator-bundle'}]},
    ]
    for environment in ('staging', 'production'):
        YAML().dump(admission(environment, components), tmp_path / f'{environment}.yaml')
    errors = checks.validate_admissions({'operator': data}, tmp_path, 'product-1-0')
    assert any('custom-bundle' in error for error in errors)
    components[1]['name'] = 'custom-bundle'
    for environment in ('staging', 'production'):
        YAML().dump(admission(environment, components), tmp_path / f'{environment}.yaml')
    assert checks.validate_admissions({'operator': data}, tmp_path, 'product-1-0') == []


def test_bundle_component_uses_top_level_short_name_override():
    assert (
        checks.component_name('product-1-0', 'operator', {'bundle_name_override': 'custom'}, bundle=True)
        == 'product-1-0-custom'
    )
