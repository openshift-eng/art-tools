import asyncio
from copy import deepcopy
from io import StringIO
from unittest.mock import AsyncMock, MagicMock

import pytest
from artcommonlib.util import new_roundtrip_yaml_handler
from pyartcd.lp_fbc import assembly_fbc_records, recorded_assembly_fbcs, update_assembly_fbcs

ASSEMBLY = '3.18.5'
NVR = 'quay-operator-fbc-3.18.5-20261106190000.ocp4.22'
PULLSPEC = 'quay.io/test/fbc@sha256:' + 'a' * 64
PIN = 'quay-operator-container-3.18.5-1'
YAML = new_roundtrip_yaml_handler()


def releases():
    return {
        'releases': {
            ASSEMBLY: {
                'assembly': {
                    'members': {
                        'images': [
                            {'distgit_key': 'quay-operator', 'metadata': {'is': {'nvr': PIN}}},
                        ]
                    },
                    'group': {'shipment': {'mr': 'existing-mr'}},
                }
            }
        }
    }


def records():
    return assembly_fbc_records(ASSEMBLY, [NVR], [PULLSPEC], [PIN])


def test_records_are_target_specific_and_reject_changed_inherited_pins():
    config = releases()
    config['releases'][ASSEMBLY]['assembly']['group']['fbc_builds'] = records()
    assert recorded_assembly_fbcs(config, ASSEMBLY, '4.22') == records()['4.22']
    with pytest.raises(ValueError, match='no recorded FBCs'):
        recorded_assembly_fbcs(config, ASSEMBLY, '4.21')
    config['releases'][ASSEMBLY]['assembly']['basis'] = {'assembly': '3.18.4'}
    config['releases']['3.18.4'] = {
        'assembly': {
            'members': {
                'images': [
                    {'distgit_key': 'quay-quay', 'metadata': {'is': {'nvr': 'quay-quay-container-3.18.4-2'}}},
                ]
            }
        }
    }
    with pytest.raises(ValueError, match='stale'):
        recorded_assembly_fbcs(config, ASSEMBLY, '4.22')


@pytest.mark.parametrize(
    'nvrs,pullspecs,error',
    [
        ([NVR], [], 'complete'),
        ([NVR], ['quay.io/test/fbc:latest'], 'immutable'),
        ([NVR, NVR], [PULLSPEC, PULLSPEC], 'Duplicate'),
        ([NVR.replace('3.18.5', '3.18.4')], [PULLSPEC], 'do not match'),
        (
            [NVR, NVR.replace('ocp4.22', 'ocp4.21'), NVR.replace('quay-operator', 'other')],
            [PULLSPEC] * 3,
            'different operator sets',
        ),
    ],
)
def test_invalid_or_partial_outputs_are_not_recorded(nvrs, pullspecs, error):
    with pytest.raises(ValueError, match=error):
        assembly_fbc_records(ASSEMBLY, nvrs, pullspecs, [PIN])


def test_cross_patch_records_require_group_policy_in_writer_and_reader():
    """Permit different patches only when both producer and consumer enable group policy."""
    nvr = NVR.replace('3.18.5', '3.18.4')
    with pytest.raises(ValueError, match='do not match'):
        assembly_fbc_records(ASSEMBLY, [nvr], [PULLSPEC], [PIN])
    config = releases()
    config['releases'][ASSEMBLY]['assembly']['group']['fbc_builds'] = assembly_fbc_records(
        ASSEMBLY, [nvr], [PULLSPEC], [PIN], ignore_patch_version=True
    )
    with pytest.raises(ValueError, match='do not match'):
        recorded_assembly_fbcs(config, ASSEMBLY, '4.22')
    assert recorded_assembly_fbcs(config, ASSEMBLY, '4.22', ignore_patch_version=True)['quay-operator']['nvr'] == nvr


def test_cross_patch_policy_still_rejects_another_release_train():
    """Ignoring patches must never permit a different major/minor version."""
    with pytest.raises(ValueError, match='do not match'):
        assembly_fbc_records(ASSEMBLY, [NVR.replace('3.18.5', '3.17.5')], [PULLSPEC], [PIN], ignore_patch_version=True)


def test_record_metadata_must_match_its_operator_and_target():
    config = releases()
    config['releases'][ASSEMBLY]['assembly']['group']['fbc_builds'] = records()
    config['releases'][ASSEMBLY]['assembly']['group']['fbc_builds']['4.22']['quay-operator']['nvr'] = NVR.replace(
        'ocp4.22', 'ocp4.21'
    )
    with pytest.raises(ValueError, match='wrong operator or target'):
        recorded_assembly_fbcs(config, ASSEMBLY, '4.22')


def test_update_refetches_preserves_unrelated_changes_and_is_idempotent():
    config = releases()
    repo = MagicMock()
    repo.fetch_switch_branch = AsyncMock()
    repo.read_file = AsyncMock()

    out = StringIO()
    current = deepcopy(config)
    current['releases'][ASSEMBLY]['assembly']['group']['shipment']['mr'] = 'newer-mr'
    YAML.dump(current, out)
    repo.read_file.return_value = out.getvalue()
    repo.write_file = AsyncMock()
    repo.commit_push = AsyncMock(return_value=True)
    assert asyncio.run(update_assembly_fbcs(repo, 'quay-3.18', ASSEMBLY, records(), config))
    saved = YAML.load(repo.write_file.call_args.args[1])
    assert saved['releases'][ASSEMBLY]['assembly']['group']['shipment']['mr'] == 'newer-mr'
    repo.fetch_switch_branch.assert_awaited_once_with('quay-3.18', remote='origin')
    repo.commit_push.assert_awaited_once_with('Record FBC builds for quay-3.18 assembly 3.18.5', safe=True)
    repo.read_file.return_value = repo.write_file.call_args.args[1]
    repo.write_file.reset_mock()
    assert not asyncio.run(update_assembly_fbcs(repo, 'quay-3.18', ASSEMBLY, records(), config))
    repo.write_file.assert_not_awaited()


@pytest.mark.parametrize('change', ['pins', 'records', 'deleted'])
def test_update_rejects_concurrent_release_changes(change):
    original = releases()
    current = deepcopy(original)
    if change == 'pins':
        current['releases'][ASSEMBLY]['assembly']['members']['images'][0]['metadata']['is']['nvr'] = PIN + '.changed'
    elif change == 'records':
        current['releases'][ASSEMBLY]['assembly']['group']['fbc_builds'] = {'other': {}}
    else:
        del current['releases'][ASSEMBLY]
    out = StringIO()
    YAML.dump(current, out)
    repo = MagicMock(
        fetch_switch_branch=AsyncMock(), read_file=AsyncMock(return_value=out.getvalue()), write_file=AsyncMock()
    )
    with pytest.raises((ValueError, RuntimeError), match='changed|already exist'):
        asyncio.run(update_assembly_fbcs(repo, 'quay-3.18', ASSEMBLY, records(), original))
    repo.write_file.assert_not_awaited()
