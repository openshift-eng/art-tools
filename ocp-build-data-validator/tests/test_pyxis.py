"""Content-stream regressions from pyxis-repo-configs MR !2027."""

import json

import pytest
from validator import ci


@pytest.mark.parametrize(
    'group,repository,filename,tags,passes',
    [
        ('quay-3.18', 'quay/quay-rhel9', 'quay/quay.yaml', ['latest'], False),
        ('quay-3.18', 'quay/quay-rhel9', 'quay/quay.yaml', ['v3.16', 'v3.17', 'v3.18'], True),
        ('openshift-4.22', 'openshift4/aws-node-termination-handler-rhel9', 'ocp/ocp-managed.yaml', ['latest'], False),
        ('openshift-4.22', 'openshift4/aws-node-termination-handler-rhel9', 'ocp/ocp-managed.yaml', ['v4.22'], True),
    ],
)
def test_cli_checks_pyxis_streams(tmp_path, group, repository, filename, tags, passes):
    """Exercise schema, delivery metadata, Pyxis loading and JSON diagnostics together."""
    root = tmp_path / 'build-data'
    (root / 'images').mkdir(parents=True)
    (root / 'group.yml').write_text(json.dumps({'name': group}))
    (root / 'images/operator.yml').write_text(
        json.dumps(
            {
                'name': repository,
                'from': {},
                'owners': ['owner@redhat.com'],
                'delivery': {'delivery_repo_names': [repository]},
            }
        )
    )
    pyxis = tmp_path / 'pyxis'
    path = pyxis / 'products' / filename
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({'repositories': [{'repository': {'repository': repository, 'content_stream_tags': tags}}]})
    )
    report = tmp_path / 'result.json'
    result = ci.main(['--repo', str(root), '--schema-only', '--pyxis-root', str(pyxis), '--report', str(report)])
    errors = json.loads(report.read_text())['errors']
    assert result == int(not passes), errors
    if passes:
        assert errors == []
    else:
        assert any(
            repository in error and f'v{group.rsplit("-", 1)[1]}' in error and filename in error for error in errors
        )


def write_config(root, repository, tags, filename='product.yaml'):
    path = root / 'products' / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({'repositories': [{'repository': {'repository': repository, 'content_stream_tags': tags}}]})
    )


def test_bundles_and_rhel_repositories_are_checked_independently(tmp_path):
    from validator.pyxis import validate_content_streams

    images = {
        'operator': {
            'delivery': {
                'delivery_repo_names': ['quay/operator-rhel9'],
                'bundle_delivery_repo_name': 'quay/operator-bundle',
            }
        }
    }
    write_config(tmp_path, 'quay/operator-rhel8', ['v3.18'], 'rhel8.yaml')
    write_config(tmp_path, 'quay/operator-rhel9', ['latest'], 'rhel9.yaml')
    write_config(tmp_path, 'quay/operator-bundle', ['v3.17', 'v3.18'], 'bundle.yaml')
    errors = validate_content_streams(images, {'name': 'quay-3.18'}, tmp_path)
    assert len(errors) == 1
    assert 'quay/operator-rhel9' in errors[0] and 'rhel9.yaml' in errors[0]
    write_config(tmp_path, 'quay/operator-rhel9', ['v3.18'], 'rhel9.yaml')
    write_config(tmp_path, 'quay/operator-bundle', ['latest'], 'bundle.yaml')
    errors = validate_content_streams(images, {'name': 'quay-3.18'}, tmp_path)
    assert len(errors) == 1 and 'quay/operator-bundle' in errors[0]


def test_product_stream_uses_resolved_group_name_not_upstream_version(tmp_path):
    from validator.pyxis import validate_content_streams

    write_config(tmp_path, 'rhosdt/operator', ['v3.11'])
    group = {'name': 'rhosdt-{MAJOR}.{MINOR}', 'vars': {'MAJOR': 3, 'MINOR': 11}, 'version': '0.158.1'}
    images = {'operator': {'delivery': {'delivery_repo_names': ['rhosdt/operator']}}}
    assert validate_content_streams(images, group, tmp_path) == []


@pytest.mark.parametrize('mode,for_release', [('disabled', True), ('wip', True), ('auto', False)])
def test_unreleased_images_need_no_pyxis_entry(tmp_path, mode, for_release):
    from validator.pyxis import validate_content_streams

    images = {
        'operator': {
            'mode': mode,
            'for_release': for_release,
            'delivery': {'delivery_repo_names': ['product/operator']},
        }
    }
    assert validate_content_streams(images, {'name': 'product-1.0'}, tmp_path) == []


def test_missing_repository_and_stream_metadata_fail(tmp_path):
    from validator.pyxis import validate_content_streams

    images = {'operator': {'delivery': {'delivery_repo_names': ['product/operator']}}}
    (tmp_path / 'products').mkdir()
    errors = validate_content_streams(images, {'name': 'product-1.0'}, tmp_path)
    assert len(errors) == 1 and 'product/operator' in errors[0] and 'v1.0' in errors[0] and 'products' in errors[0]
    for tags in (None, [], 'v1.0', [None], [1.0]):
        write_config(tmp_path, 'product/operator', tags)
        assert validate_content_streams(images, {'name': 'product-1.0'}, tmp_path)


def test_invalid_product_stream_cannot_silently_skip_validation(tmp_path):
    from validator.pyxis import validate_content_streams

    images = {'operator': {'delivery': {'delivery_repo_names': ['product/operator']}}}
    write_config(tmp_path, 'product/operator', ['v1.0'])
    with pytest.raises(ValueError, match='group.yml.name'):
        validate_content_streams(images, {'name': 'product'}, tmp_path)


def test_duplicate_config_cannot_mask_missing_stream(tmp_path):
    from validator.pyxis import validate_content_streams

    write_config(tmp_path, 'product/operator', ['v1.0'], 'good.yaml')
    write_config(tmp_path, 'product/operator', ['latest'], 'bad.yaml')
    images = {'operator': {'delivery': {'delivery_repo_names': ['product/operator']}}}
    errors = validate_content_streams(images, {'name': 'product-1.0'}, tmp_path)
    assert len(errors) == 1 and 'bad.yaml' in errors[0]


def test_absent_pyxis_checkout_fails_for_delivery_repositories(tmp_path):
    from validator.pyxis import validate_content_streams

    images = {'operator': {'delivery': {'delivery_repo_names': ['product/operator']}}}
    with pytest.raises(ValueError, match='products directory does not exist'):
        validate_content_streams(images, {'name': 'product-1.0'}, tmp_path)


def test_pyxis_symlink_cannot_escape_checkout(tmp_path):
    from validator.pyxis import validate_content_streams

    root = tmp_path / 'pyxis'
    (root / 'products').mkdir(parents=True)
    outside = tmp_path / 'outside.yaml'
    outside.write_text('repositories: []')
    (root / 'products/escape.yaml').symlink_to(outside)
    images = {'operator': {'delivery': {'delivery_repo_names': ['product/operator']}}}
    with pytest.raises(ValueError, match='symlink escapes'):
        validate_content_streams(images, {'name': 'product-1.0'}, root)
