import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pyartcd.cli import cli
from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline


@pytest.mark.parametrize(
    'unified,group_version,fbc_version,error',
    [
        pytest.param(False, '3.18.5', '3.18.5', None, id='separate-fbc-shipment'),
        pytest.param(True, '3.18.5', '3.18.5', None, id='unified-explicit'),
        pytest.param(True, 'auto', '3.18.0', None, id='unified-auto'),
        pytest.param(True, '3.18.5', '3.18.0', 'FBC NVRs do not match', id='explicit-mismatch'),
        pytest.param(True, 'auto', '3.18.0', 'requires an immutable pullspec', id='auto-invalid-pullspec'),
    ],
)
def test_prepare_release_records_unified_fbcs_with_group_version_policy(
    tmp_path, unified, group_version, fbc_version, error
):
    push_url = 'https://github.com/example/ocp-build-data'
    runtime = MagicMock(
        dry_run=False, working_dir=tmp_path, config={'build_config': {'ocp_build_data_repo_push_url': push_url}}
    )
    pipeline = PrepareReleaseLPPipeline(runtime, 'quay-3.18', '3.18.5', unified_fbc=unified)
    pins = ['quay-operator-container-3.18.5-1']
    bundles = ['quay-operator-metadata-container-3.18.5-1']
    fbc = f'quay-operator-fbc-{fbc_version}-1.ocp4.22'
    pullspec = 'quay.io/test/fbc@sha256:' + 'a' * 64
    if error == 'requires an immutable pullspec':
        pullspec = 'quay.io/test/fbc:latest'
    pipeline._check_env_vars = MagicMock()
    pipeline._setup_working_dir = MagicMock()
    pipeline._load_assembly = AsyncMock(return_value={})
    pipeline._extract_operand_nvrs = MagicMock(return_value=pins)
    pipeline._trigger_bundle_build = AsyncMock(return_value=(bundles, pins))
    pipeline._trigger_fbc_build = AsyncMock(return_value=([fbc], [pullspec]))
    pipeline._validate_fbc_pins = AsyncMock()
    pipeline._generate_release_notes = MagicMock(return_value=None)
    pipeline._load_release_notes_template = MagicMock(return_value=None)
    pipeline._create_snapshot = AsyncMock(return_value=[MagicMock()])
    pipeline._create_shipment_config = MagicMock(return_value=MagicMock())
    pipeline._write_shipment_file = AsyncMock()

    repo = MagicMock(setup=AsyncMock())
    with (
        patch(
            'pyartcd.pipelines.prepare_release_lp.load_group_config',
            new_callable=AsyncMock,
            return_value={'product': 'quay', 'version': group_version},
        ),
        patch('pyartcd.pipelines.prepare_release_lp.validate_fbc_related_images', new_callable=AsyncMock),
        patch('pyartcd.pipelines.prepare_release_lp.GitRepository', return_value=repo),
        patch('pyartcd.pipelines.prepare_release_lp.update_assembly_fbcs', new_callable=AsyncMock) as update_records,
    ):
        if error:
            with pytest.raises(ValueError, match=error):
                asyncio.run(pipeline.run())
            update_records.assert_not_awaited()
            pipeline._create_snapshot.assert_not_awaited()
            return
        asyncio.run(pipeline.run())

    pipeline._validate_fbc_pins.assert_awaited_once_with([fbc], bundles, pins)
    assert pipeline._create_snapshot.call_args_list[0].args[0] == pins + bundles
    if unified:
        records = {'4.22': {'quay-operator': {'nvr': fbc, 'pullspec': pullspec, 'operand_nvrs': pins}}}
        update_records.assert_awaited_once_with(repo, 'quay-3.18', '3.18.5', records, None, repo_url=push_url)
        assert pipeline._create_snapshot.await_count == 1
        assert all(c.args[0] == 'image' for c in pipeline._create_shipment_config.call_args_list)
    else:
        update_records.assert_not_awaited()
        assert pipeline._create_snapshot.call_args_list[1].args[0] == [fbc]


def test_cli_exposes_opt_in_flag():
    with cli.commands['prepare-release-lp'].make_context(
        'prepare-release-lp',
        [
            '--group=quay-3.18',
            '--assembly=3.18.5',
            '--unified-fbc',
        ],
    ) as context:
        assert context.params['unified_fbc'] is True


def test_dry_run_never_writes_fbc_metadata(tmp_path):
    runtime = MagicMock(dry_run=True, working_dir=Path(tmp_path), config={})
    pipeline = PrepareReleaseLPPipeline(runtime, 'quay-3.18', '3.18.5', unified_fbc=True)
    with patch('pyartcd.pipelines.prepare_release_lp.GitRepository') as repo:
        asyncio.run(pipeline._record_assembly_fbcs([], [], []))
    repo.assert_not_called()


def test_dry_run_never_publishes_the_shipment_pointer(tmp_path):
    runtime = MagicMock(dry_run=True, working_dir=tmp_path, config={})
    pipeline = PrepareReleaseLPPipeline(runtime, 'quay-3.18', '3.18.5', create_mr=True)
    with patch('pyartcd.pipelines.prepare_release_lp.GitRepository') as repo:
        asyncio.run(pipeline._update_assembly_with_shipment_url('https://gitlab.example/mr/42'))
    repo.assert_not_called()
