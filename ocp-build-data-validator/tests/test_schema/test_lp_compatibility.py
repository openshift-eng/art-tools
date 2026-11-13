"""Schema contracts for fields consumed by LP build and release pipelines."""

import copy

import pytest
from validator.schema import group_schema, image_schema


@pytest.mark.parametrize(
    'key,value',
    [
        ('image_repo', 'quay.io/art-builds'),
        ('ignore_patch_version', True),
        ('ignore_patch_version', False),
        ('OCP_RELEASE_NOTES_VERSION', '4.20'),
    ],
)
def test_consumed_group_fields_are_supported(key, value):
    """Accept LP settings at the locations used by their actual pipeline consumers."""
    data = {'konflux': {key: value}} if key == 'image_repo' else {key: value}
    assert group_schema.validate('group.yml', data) == ''


@pytest.mark.parametrize(
    'data',
    [
        {'konflux': {'image_repo': 7}},
        {'konflux': {'image_repo': ''}},
        {'ignore_patch_version': 'true'},
        {'ignore_patch_version': 1},
        {'OCP_RELEASE_NOTES_VERSION': 4.20},
        {'OCP_RELEASE_NOTES_VERSION': ''},
    ],
)
def test_consumed_group_fields_reject_malformed_values(data):
    """Compatibility support must preserve type and nonempty-string validation."""
    assert group_schema.validate('group.yml', data)


@pytest.mark.parametrize('field', ['no_shell', 'stage_release'])
@pytest.mark.parametrize('suffix', ['', '!', '?'])
def test_image_runtime_flags_are_boolean(field, suffix):
    """Accept real Konflux image flags and their assembly merge forms, without coercion."""
    for value in (True, False):
        data = {'from': {}, 'name': 'test-image', 'for_release': False, 'konflux': {field + suffix: value}}
        assert image_schema.validate('images/test-image.yml', data) is None
        malformed = copy.deepcopy(data)
        malformed['konflux'][field + suffix] = 'true'
        assert image_schema.validate('images/test-image.yml', malformed)


def test_channel_skip_flag_is_rejected_at_the_wrong_location():
    """The FBC code reads vars.FBC_DISABLE_CHANNEL_SKIPS, not konflux.FBC_DISABLE_CHANNEL_SKIPS."""
    assert group_schema.validate('group.yml', {'vars': {'FBC_DISABLE_CHANNEL_SKIPS': True}}) == ''
    assert group_schema.validate('group.yml', {'konflux': {'FBC_DISABLE_CHANNEL_SKIPS': True}})
