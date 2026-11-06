import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pyartcd.cli import cli
from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline


@pytest.mark.parametrize('unified', [False, True])
def test_unified_mode_records_fbcs_and_ships_only_operands_and_bundles(tmp_path, unified):
    runtime = MagicMock(dry_run=False, working_dir=tmp_path, config={})
    pipeline = PrepareReleaseLPPipeline(runtime, 'quay-3.18', '3.18.5', unified_fbc=unified)
    pins = ['quay-operator-container-3.18.5-1']
    bundles = ['quay-operator-metadata-container-3.18.5-1']
    fbcs = ['quay-operator-fbc-3.18.5-1.ocp4.22']
    pipeline._check_env_vars = MagicMock()
    pipeline._setup_working_dir = MagicMock()
    pipeline._load_product_from_group_config = AsyncMock(return_value='quay')
    pipeline._load_assembly = AsyncMock(return_value={})
    pipeline._extract_operand_nvrs = MagicMock(return_value=pins)
    pipeline._trigger_bundle_build = AsyncMock(return_value=(bundles, pins))
    pipeline._trigger_fbc_build = AsyncMock(return_value=(fbcs, []))
    pipeline._validate_fbc_pins = AsyncMock()
    pipeline._record_assembly_fbcs = AsyncMock()
    pipeline._generate_release_notes = MagicMock(return_value=None)
    pipeline._load_release_notes_template = MagicMock(return_value=None)
    pipeline._create_snapshot = AsyncMock(return_value=[MagicMock()])
    pipeline._create_shipment_config = MagicMock(return_value=MagicMock())
    pipeline._write_shipment_file = AsyncMock()
    asyncio.run(pipeline.run())
    assert pipeline._create_snapshot.call_args_list[0].args[0] == pins + bundles
    if unified:
        pipeline._record_assembly_fbcs.assert_awaited_once_with(fbcs, [], pins)
        assert pipeline._create_snapshot.await_count == 1
        assert all(c.args[0] == 'image' for c in pipeline._create_shipment_config.call_args_list)
    else:
        pipeline._record_assembly_fbcs.assert_not_awaited()
        assert pipeline._create_snapshot.call_args_list[1].args[0] == fbcs


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
