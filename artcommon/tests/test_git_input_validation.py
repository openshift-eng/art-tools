"""Check untrusted Git input boundaries without network access."""

from unittest.mock import patch

import pytest
from artcommonlib.git_helper import validate_git_branch, validate_git_group_name, validate_git_remote_url


@pytest.mark.parametrize('branch', [None, '', 'codex/topic', 'feature/a+b', 'release/3.18', 'feature/café'])
def test_literal_branch_names_and_empty_overrides_remain_supported(branch):
    validate_git_branch(branch)


@pytest.mark.parametrize(
    'branch',
    [
        '--upload-pack=command',
        '-bad',
        '.',
        '..',
        'foo/../bar',
        'foo.lock',
        'foo/a.lock/bar',
        'white space',
        'foo:bar',
        'HEAD',
        '@{-1}',
        'foo\nbar',
    ],
)
def test_invalid_or_option_like_branch_names_are_rejected(branch):
    with pytest.raises(ValueError, match='Invalid Git branch'):
        validate_git_branch(branch)


def test_empty_branch_overrides_do_not_invoke_git():
    with patch('artcommonlib.git_helper.subprocess.run') as run:
        validate_git_branch(None)
        validate_git_branch('')
    run.assert_not_called()


@pytest.mark.parametrize(
    'group', ['.', '..', '-bad', '--detach', 'quay/3.18', 'quay-3..18', '/tmp', 'quay.lock', 'quay.']
)
def test_source_groups_cannot_be_paths_or_git_options(group):
    with pytest.raises(ValueError):
        validate_git_group_name(group)


@pytest.mark.parametrize(
    'url',
    [
        'https://user:dummy-secret@example.invalid/repo.git',
        'https://dummy-secret@github.com/example/repo.git',
        'http://user:dummy-secret@example.invalid/repo.git',
        'ssh://git:dummy-secret@example.invalid/repo.git',
    ],
)
def test_embedded_credentials_are_rejected_without_echoing_the_url(url):
    with pytest.raises(ValueError, match='embedded credentials') as exc:
        validate_git_remote_url(url)
    assert 'dummy-secret' not in str(exc.value)
    assert url not in str(exc.value)


@pytest.mark.parametrize(
    'url',
    [
        'https://github.com/example/repo.git',
        'ssh://git@github.com/example/repo.git',
        'git@github.com:example/repo.git',
        '/tmp/local-repository',
    ],
)
def test_normal_git_authentication_and_local_repositories_remain_supported(url):
    validate_git_remote_url(url)
