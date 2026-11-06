import asyncio
import json
from io import StringIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from artcommonlib.util import new_roundtrip_yaml_handler
from pyartcd.pipelines.merge_layered_fbc import MergeLayeredFbcPipeline

SHA = 'c' * 40
DIGEST = 'quay.io/test/fbc@sha256:' + 'a' * 64
NVR = 'quay-operator-fbc-3.18.5-1.ocp4.22'
PIN = 'quay-operator-container-3.18.5-1'


def definition():
    return {
        'releases': {
            '3.18.5': {
                'assembly': {
                    'members': {'images': [{'distgit_key': 'quay-operator', 'metadata': {'is': {'nvr': PIN}}}]},
                    'group': {
                        'fbc_builds': {
                            '4.22': {
                                'quay-operator': {
                                    'nvr': NVR,
                                    'pullspec': DIGEST,
                                    'operand_nvrs': [PIN],
                                }
                            }
                        }
                    },
                }
            }
        }
    }


def pipeline(tmp_path, **kwargs):
    runtime = Mock(working_dir=tmp_path, doozer_working=tmp_path / 'doozer', dry_run=True)
    return MergeLayeredFbcPipeline(runtime, '4.22', assemblies=('3.18.5',), **kwargs)


def test_reads_exact_named_assembly_from_derived_branch_and_forwards_provenance(tmp_path):
    p = pipeline(tmp_path)
    repo = Mock()
    repo.get_branch.return_value.commit.sha = SHA
    out = StringIO()
    new_roundtrip_yaml_handler().dump(definition(), out)
    repo.get_contents.return_value.decoded_content = out.getvalue().encode()
    with patch('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org') as client:
        client.return_value.get_repo.return_value = repo
        asyncio.run(p._resolve_assemblies())
    repo.get_branch.assert_called_once_with('quay-3.18')
    repo.get_contents.assert_called_once_with('releases.yml', ref=SHA)
    assert p.builds == (DIGEST,)
    assert p.assembly_inputs[NVR]['assembly'] == '3.18.5'
    assert p.assembly_inputs[NVR]['group_revision'] == SHA
    p.group = 'quay-3.18'
    p.output_file.write_text(json.dumps({'dry_run': True}))
    with patch('pyartcd.pipelines.merge_layered_fbc.exectools.cmd_assert_async', new=AsyncMock()) as cmd:
        asyncio.run(p._build())
    args = cmd.call_args.args[0]
    assert args[args.index('--fbc-build') + 1] == DIGEST
    inputs_file = args[args.index('--assembly-inputs-file') + 1]
    assert json.loads(open(inputs_file).read())[NVR]['group_revision'] == SHA


@pytest.mark.parametrize('assemblies', [('quay-3.18',), ('3.18',), ('3.18.5', '3.18.6')])
def test_rejects_invalid_or_ambiguous_versions_before_remote_reads(tmp_path, assemblies):
    p = pipeline(tmp_path)
    p.assemblies = assemblies
    with patch('pyartcd.pipelines.merge_layered_fbc.get_github_client_for_org') as client:
        with pytest.raises(ValueError, match='format|Multiple assemblies'):
            asyncio.run(p._resolve_assemblies())
    client.assert_not_called()


@pytest.mark.parametrize('options', [{'fbc_branch': 'custom'}, {'skip_rebase': True}, {'fbc_builds': (NVR,)}])
def test_rejects_assembly_inputs_with_existing_source_modes(tmp_path, options):
    with pytest.raises(ValueError, match='cannot be combined'):
        asyncio.run(pipeline(tmp_path, **options)._resolve_assemblies())


def test_rejects_record_resolved_to_another_group(tmp_path):
    p = pipeline(tmp_path)
    p.builds = (NVR,)
    p.assembly_inputs = {NVR: {'group': 'quay-3.18', 'operator': 'quay-operator'}}

    async def records(**kwargs):
        yield SimpleNamespace(nvr=NVR, group='quay-3.17', name='quay-operator-fbc')

    with patch('pyartcd.pipelines.merge_layered_fbc.KonfluxDb') as db:
        db.return_value.search_builds_by_fields = records
        with pytest.raises(ValueError, match='recorded assembly FBC identity'):
            asyncio.run(p._resolve_groups())
