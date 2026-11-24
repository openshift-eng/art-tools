"""Regression coverage for direct LP assembly generation and atomic persistence."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest
from artcommonlib.assembly import assembly_image_pins
from artcommonlib.model import Model
from artcommonlib.util import new_roundtrip_yaml_handler
from github import GithubException
from pyartcd.lp_assembly import direct_release_assembly, persist_direct_assembly
from pyartcd.lp_fbc import update_assembly_fbcs
from pyartcd.lp_shipment import update_shipment_mr_url

YAML = new_roundtrip_yaml_handler()
ASSEMBLY = '1.5.8'
PIN = 'oadp-operator-container-1.5.8-1'
REPO_URL = 'https://github.com/openshift-eng/ocp-build-data'


def releases(definition):
    """Wrap one test assembly in releases.yml structure."""
    return {'releases': {ASSEMBLY: {'assembly': definition}}}


def test_upgrade_and_remove_obsolete_standalone_pins():
    original = releases({'type': 'stream', 'group': {'shipment': {'mr': 'old'}, 'release_jira': 'OADP-1'}})
    first = direct_release_assembly(original, ASSEMBLY, {'operator': PIN, 'operand': 'operand-1.5.8-1'}, {})
    assert first['type'] == 'standard'
    second = direct_release_assembly(releases(first), ASSEMBLY, {'operator': PIN}, {})
    assert assembly_image_pins(Model(releases(second)), ASSEMBLY) == {'operator': PIN}
    assert second['group']['shipment']['mr'] == 'old'
    assert second['group']['release_jira'] == 'OADP-1'
    assert original['releases'][ASSEMBLY]['assembly']['type'] == 'stream'


def test_inheritance_keeps_absent_parent_members_and_exclusions():
    parent = {
        'members': {
            'images': [
                {'distgit_key': 'operator', 'metadata': {'is': {'nvr': 'oadp-operator-container-1.5.7-1'}}},
                {'distgit_key': 'retained', 'metadata': {'is': {'nvr': 'retained-1.5.7-1'}}},
                {'distgit_key': 'excluded', 'metadata': {'is': {'nvr': 'excluded-1.5.7-1'}}},
            ]
        }
    }
    original = releases(
        {'basis': {'assembly': '1.5.7'}, 'members': {'images': [{'distgit_key': 'excluded', 'exclude': True}]}}
    )
    original['releases']['1.5.7'] = {'assembly': parent}
    before = deepcopy(original)
    definition = direct_release_assembly(original, ASSEMBLY, {'operator': PIN, 'new': 'new-1.5.8-1'}, {})
    current = deepcopy(original)
    current['releases'][ASSEMBLY]['assembly'] = definition
    assert assembly_image_pins(Model(current), ASSEMBLY) == {
        'operator': PIN,
        'retained': 'retained-1.5.7-1',
        'new': 'new-1.5.8-1',
    }
    assert definition['members']['images'][1]['metadata']['is']['nvr!'] == PIN
    assert original == before


def mock_assembly_pr(path, monkeypatch, *, existing_pr=False):
    base_content = path.read_bytes()
    repo = MagicMock(
        _directory=path.parent,
        fetch_switch_branch=AsyncMock(),
        commit_push=AsyncMock(return_value=True),
        create_branch=AsyncMock(),
        does_branch_exist_on_remote=AsyncMock(return_value=existing_pr),
        read_file=AsyncMock(side_effect=lambda _: path.read_text()),
        write_file=AsyncMock(side_effect=lambda _, value: path.write_text(value)),
    )
    gh_repo = MagicMock()
    monkeypatch.setattr(
        'pyartcd.lp_assembly.get_github_client_for_org', MagicMock(return_value=MagicMock(get_repo=lambda _: gh_repo))
    )
    monkeypatch.setattr('pyartcd.lp_assembly._MERGE_POLL_INTERVAL', 0)
    pr = gh_repo.get_pull.return_value
    pr.head.sha = gh_repo.get_branch.return_value.commit.sha = 'head-sha'
    pr.base.ref = 'oadp-1.5'
    pr.html_url = 'https://github.com/openshift-eng/ocp-build-data/pull/42'
    pr.state = 'open'
    pr.merged = False
    pr.merge.side_effect = [
        GithubException(405, {'message': 'Required status check is expected'}),
        MagicMock(merged=True),
    ]
    gh_repo.get_pulls.return_value = [pr] if existing_pr else []
    gh_repo.create_pull.return_value = pr
    gh_repo.get_contents.side_effect = lambda _, ref: MagicMock(
        decoded_content=path.read_bytes() if ref == 'head-sha' else base_content
    )
    return repo, gh_repo, pr


@pytest.mark.parametrize('existing_pr', [False, True])
def test_persistence_is_atomic_preserves_comments_and_is_idempotent(tmp_path, monkeypatch, existing_pr):
    path = tmp_path / 'releases.yml'
    path.write_bytes(b'# release history\nreleases: {}\n')
    repo, gh_repo, pr = mock_assembly_pr(path, monkeypatch, existing_pr=existing_pr)
    definition = direct_release_assembly({}, ASSEMBLY, {'operator': PIN}, {})
    assert asyncio.run(persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, {}, 'new-mr', repo_url=REPO_URL))
    saved = YAML.load(path)
    assert saved['releases'][ASSEMBLY]['assembly']['group']['shipment']['mr'] == 'new-mr'
    assert path.read_text().startswith('# release history')
    assert pr.merge.call_count == 2
    pr.merge.assert_called_with(sha='head-sha')
    assert repo.commit_push.await_count == int(not existing_pr)
    assert gh_repo.create_pull.call_count == int(not existing_pr)
    if not existing_pr:
        branch = repo.create_branch.await_args.args[0]
        repo.create_branch.assert_awaited_once_with(branch, reset=True)
        assert gh_repo.create_pull.call_args.kwargs['head'] == f'openshift-eng:{branch}'
        assert gh_repo.create_pull.call_args.kwargs['base'] == 'oadp-1.5'
        assert branch != 'oadp-1.5'
    assert not asyncio.run(
        persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, saved, 'new-mr', repo_url=REPO_URL)
    )
    assert repo.commit_push.await_count == int(not existing_pr)


@pytest.mark.parametrize('write', ['shipment', 'fbcs'])
@pytest.mark.parametrize('outcome', ['merged', 'timeout', 'concurrent edit'])
def test_prepare_release_writes_use_the_same_pr_merge_flow(tmp_path, monkeypatch, write, outcome):
    path = tmp_path / 'releases.yml'
    previous = releases(direct_release_assembly({}, ASSEMBLY, {'operator': PIN}, {}))
    YAML.dump(previous, path)
    repo, gh_repo, pr = mock_assembly_pr(path, monkeypatch)
    previous_content = path.read_bytes()
    if write == 'shipment':
        publish = update_shipment_mr_url(
            repo, 'oadp-1.5', ASSEMBLY, 'new-mr', None, create_as_stream=False, repo_url=REPO_URL
        )
        changed_group = {'shipment': {'mr': 'competing-mr'}}
    else:
        records = {'4.22': {'operator': {'operand_nvrs': [PIN]}}}
        publish = update_assembly_fbcs(repo, 'oadp-1.5', ASSEMBLY, records, previous, repo_url=REPO_URL)
        changed_group = {'fbc_builds': {'competing': {}}}

    if outcome == 'timeout':
        monkeypatch.setattr('pyartcd.lp_assembly._MERGE_TIMEOUT', 0)
        with pytest.raises(TimeoutError, match=pr.html_url):
            asyncio.run(publish)
        assert pr.merge.call_count == 1
    elif outcome == 'concurrent edit':
        changed = deepcopy(previous)
        changed['releases'][ASSEMBLY]['assembly']['group'].update(changed_group)
        changed_path = tmp_path / 'concurrent.yml'
        YAML.dump(changed, changed_path)
        base_revisions = iter((previous_content, changed_path.read_bytes()))
        gh_repo.get_contents.side_effect = lambda _, ref: MagicMock(
            decoded_content=path.read_bytes() if ref == 'head-sha' else next(base_revisions)
        )
        with pytest.raises(RuntimeError, match='changed concurrently.*pull/42'):
            asyncio.run(publish)
        assert pr.merge.call_count == 1
    else:
        assert asyncio.run(publish)
        assert pr.merge.call_count == 2
    branch = repo.create_branch.await_args.args[0]
    assert branch.startswith('update-assembly-oadp-1.5-')
    assert gh_repo.create_pull.call_args.kwargs['base'] == 'oadp-1.5'
    assert gh_repo.create_pull.call_args.kwargs['head'] == f'openshift-eng:{branch}'
    pr.merge.assert_called_with(sha='head-sha')


def test_accepts_pr_merged_while_reading_the_base(tmp_path, monkeypatch):
    path = tmp_path / 'releases.yml'
    path.write_text('releases: {}\n')
    repo, gh_repo, pr = mock_assembly_pr(path, monkeypatch)
    merged_pr = MagicMock(head=pr.head, base=pr.base, merged=True, html_url=pr.html_url)
    gh_repo.get_pull.side_effect = [pr, merged_pr, merged_pr]
    gh_repo.get_contents.side_effect = lambda *_args, **_kwargs: MagicMock(decoded_content=path.read_bytes())
    definition = direct_release_assembly({}, ASSEMBLY, {'operator': PIN}, {})

    assert asyncio.run(persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, {}, 'new-mr', repo_url=REPO_URL))
    pr.merge.assert_not_called()


def test_reincluded_child_drops_stale_override_when_input_matches_parent():
    original = releases(
        {
            'basis': {'assembly': '1.5.7'},
            'members': {
                'images': [{'distgit_key': 'operator', 'exclude': False, 'metadata': {'is': {'nvr!': 'old-1.5.8-1'}}}]
            },
        }
    )
    original['releases']['1.5.7'] = {
        'assembly': {'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': PIN}}}]}}
    }
    definition = direct_release_assembly(original, ASSEMBLY, {'operator': PIN}, {})
    original['releases'][ASSEMBLY]['assembly'] = definition
    assert assembly_image_pins(Model(original), ASSEMBLY) == {'operator': PIN}


@pytest.mark.parametrize('inherited', [False, True])
def test_removed_or_redundant_pins_preserve_unrelated_member_settings(inherited):
    """Keep image overrides when dropping a standalone pin or inheriting the same build."""
    member = {
        'distgit_key': 'operator',
        'why': 'Restrict release architectures',
        'metadata': {'is': {'nvr!' if inherited else 'nvr': 'oadp-operator-container-1.5.8-2'}, 'arches': ['x86_64']},
    }
    original = releases({'members': {'images': [member]}})
    if inherited:
        original['releases'][ASSEMBLY]['assembly']['basis'] = {'assembly': '1.5.7'}
        original['releases']['1.5.7'] = {
            'assembly': {'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': PIN}}}]}}
        }
    before = deepcopy(original)
    definition = direct_release_assembly(original, ASSEMBLY, {'operator': PIN} if inherited else {}, {})
    assert definition['members']['images'] == [
        {'distgit_key': 'operator', 'why': 'Restrict release architectures', 'metadata': {'arches': ['x86_64']}}
    ]
    current = deepcopy(original)
    current['releases'][ASSEMBLY]['assembly'] = definition
    assert assembly_image_pins(Model(current), ASSEMBLY) == ({'operator': PIN} if inherited else {})
    assert original == before


@pytest.mark.parametrize('pin_field', ['is', 'is!', 'is?'])
@pytest.mark.parametrize('operation', ['replace', 'remove', 'inherit'])
def test_merge_operator_pin_forms_are_reconciled(pin_field, operation):
    """Replace or remove supported pin forms without reviving stale pins or dropping image settings."""
    original = releases(
        {
            'type': 'standard',
            'members': {
                'images': [
                    {
                        'distgit_key': 'operator',
                        'why': 'Keep architecture restrictions',
                        'metadata': {pin_field: {'nvr': 'oadp-operator-container-1.5.8-0'}, 'arches': ['x86_64']},
                    }
                ]
            },
        }
    )
    if operation == 'inherit':
        original['releases'][ASSEMBLY]['assembly']['basis'] = {'assembly': '1.5.7'}
        original['releases']['1.5.7'] = {
            'assembly': {'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': PIN}}}]}}
        }
    before = deepcopy(original)
    selected = {} if operation == 'remove' else {'operator': PIN}
    definition = direct_release_assembly(original, ASSEMBLY, selected, {})
    metadata = definition['members']['images'][0]['metadata']
    assert metadata == (
        {'arches': ['x86_64'], 'is': {'nvr': PIN}} if operation == 'replace' else {'arches': ['x86_64']}
    )
    current = deepcopy(original)
    current['releases'][ASSEMBLY]['assembly'] = definition
    assert assembly_image_pins(Model(current), ASSEMBLY) == selected
    assert original == before


@pytest.mark.parametrize('selected_pin', [None, PIN, 'oadp-operator-container-1.5.8-2'])
def test_explicit_pin_removal_is_preserved_unless_reselected(selected_pin):
    """Keep an inherited-pin suppression until the current release explicitly selects that image."""
    original = releases(
        {
            'basis': {'assembly': '1.5.7'},
            'members': {
                'images': [{'distgit_key': 'operator', 'metadata': {'is-': {'nvr': PIN}, 'arches': ['x86_64']}}]
            },
        }
    )
    original['releases']['1.5.7'] = {
        'assembly': {'members': {'images': [{'distgit_key': 'operator', 'metadata': {'is': {'nvr': PIN}}}]}}
    }
    selected = {'operator': selected_pin} if selected_pin else {}
    definition = direct_release_assembly(original, ASSEMBLY, selected, {})
    original['releases'][ASSEMBLY]['assembly'] = definition
    assert assembly_image_pins(Model(original), ASSEMBLY) == selected
    metadata = definition['members']['images'][0]['metadata']
    assert ('is-' in metadata) is (selected_pin is None)
    assert metadata['arches'] == ['x86_64']


@pytest.mark.parametrize('field', ['pin', 'pointer', 'fbc', 'parent'])
def test_concurrent_release_changes_fail_without_writes(tmp_path, field):
    original = releases(direct_release_assembly({}, ASSEMBLY, {'operator': PIN}, {}))
    original['releases'][ASSEMBLY]['assembly']['basis']['assembly'] = '1.5.7'
    original['releases']['1.5.7'] = {
        'assembly': {'members': {'images': [{'distgit_key': 'parent', 'metadata': {'is': {'nvr': 'parent-1.5.7-1'}}}]}}
    }
    current = deepcopy(original)
    definition = current['releases'][ASSEMBLY]['assembly']
    if field == 'pin':
        definition['members']['images'][0]['metadata']['is']['nvr'] = 'changed-1.5.8-2'
    elif field == 'pointer':
        definition['group']['shipment']['mr'] = 'competing-mr'
    elif field == 'fbc':
        definition['group']['fbc_builds'] = {'changed': {}}
    else:
        current['releases']['1.5.7']['assembly']['members']['images'][0]['metadata']['is']['nvr'] = 'parent-1.5.7-2'
    path = tmp_path / 'releases.yml'
    YAML.dump(current, path)
    before = path.read_text()
    repo = MagicMock(_directory=tmp_path, fetch_switch_branch=AsyncMock(), commit_push=AsyncMock())
    with pytest.raises(RuntimeError, match='changed concurrently'):
        asyncio.run(
            persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, original, 'new-mr', repo_url=REPO_URL)
        )
    assert path.read_text() == before
    repo.commit_push.assert_not_awaited()
