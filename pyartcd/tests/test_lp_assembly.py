"""Regression coverage for direct LP assembly generation and atomic persistence."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest
from artcommonlib.assembly import assembly_image_pins
from artcommonlib.model import Model
from artcommonlib.util import new_roundtrip_yaml_handler
from pyartcd.lp_assembly import direct_release_assembly, persist_direct_assembly

YAML = new_roundtrip_yaml_handler()
ASSEMBLY = '1.5.8'
PIN = 'oadp-operator-container-1.5.8-1'


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


def test_persistence_is_atomic_preserves_comments_and_is_idempotent(tmp_path):
    path = tmp_path / 'releases.yml'
    path.write_text('# release history\nreleases: {}\n')
    repo = MagicMock(_directory=tmp_path, fetch_switch_branch=AsyncMock(), commit_push=AsyncMock(return_value=True))
    definition = direct_release_assembly({}, ASSEMBLY, {'operator': PIN}, {})
    assert asyncio.run(persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, {}, 'new-mr'))
    saved = YAML.load(path)
    assert saved['releases'][ASSEMBLY]['assembly']['group']['shipment']['mr'] == 'new-mr'
    assert path.read_text().startswith('# release history')
    repo.commit_push.assert_awaited_once()
    assert not asyncio.run(persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, saved, 'new-mr'))
    assert repo.commit_push.await_count == 1


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
        asyncio.run(persist_direct_assembly(repo, 'oadp-1.5', ASSEMBLY, definition, original, 'new-mr'))
    assert path.read_text() == before
    repo.commit_push.assert_not_awaited()
