"""Exercise direct assembly recording through the release pipeline with mocked services."""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import ComponentSource, GitSource, Snapshot, SnapshotComponent, SnapshotSpec
from pyartcd.lp_fbc import recorded_assembly_fbcs
from pyartcd.lp_shipment import ShipmentMRProductionError
from pyartcd.pipelines.release_from_fbc import ReleaseFromFbcPipeline, release_from_fbc

PIN = 'oadp-operator-container-1.5.8-1.p0'
BUNDLE = 'oadp-operator-metadata-container-1.5.8-1'
FBC = 'oadp-operator-fbc-1.5.8-20261111120000.ocp4.22'
PULLSPEC = 'quay.io/test/fbc@sha256:' + 'a' * 64
YAML = new_roundtrip_yaml_handler()


def pipeline(tmp_path, **kwargs):
    """Build a pipeline using real assembly/shipment generation and mocked external services."""
    runtime = MagicMock(working_dir=tmp_path, dry_run=False, config={})
    result = ReleaseFromFbcPipeline(runtime, 'oadp-1.5', '1.5.8', [PULLSPEC], **kwargs)
    result.product = 'oadp'
    result._gitlab = MagicMock()
    result._releases_config = {}
    result._component_doozer_keys = {'oadp-operator-container': 'oadp-operator'}
    result._group_components = {'oadp-operator-container', 'oadp-operator-metadata-container'}
    result._load_component_doozer_keys = AsyncMock(return_value=result._component_doozer_keys)
    result._load_ignore_patch_version_from_group_config = AsyncMock(return_value=False)
    result.check_env_vars = MagicMock()
    result.setup_working_dir = MagicMock()
    result.setup_shipment_repo = AsyncMock()
    result._load_product_from_group_config = AsyncMock(return_value='oadp')
    result._load_layered_product_shipment_mr = MagicMock(return_value=None)
    result.extract_fbc_nvr = MagicMock(return_value=FBC)
    result.validate_fbc_related_images = AsyncMock(return_value=[PIN, BUNDLE])
    result._load_release_notes_template = MagicMock(return_value=None)
    result.write_shipment_files_locally = AsyncMock()
    result.create_shipment_mr = AsyncMock(return_value='https://gitlab.example/art/data/-/merge_requests/123')
    result._update_layered_product_shipment_mr = AsyncMock()
    result._verify_layered_product_shipment_mr = AsyncMock()
    result._update_jira_with_mr_link = MagicMock()
    result.set_shipment_mr_ready = AsyncMock()

    async def snapshot(nvrs):
        return Snapshot(
            nvrs=nvrs,
            spec=SnapshotSpec(
                application='oadp-1-5',
                components=[
                    SnapshotComponent(
                        name='oadp-operator',
                        containerImage=PULLSPEC,
                        source=ComponentSource(
                            git=GitSource(url='https://github.com/example/operator', revision='abc')
                        ),
                    )
                ],
            ),
        )

    result.create_snapshot = AsyncMock(side_effect=snapshot)
    return result


@pytest.fixture(autouse=True)
def external_services():
    """Keep all tests offline while exercising the real orchestration."""
    with (
        patch('pyartcd.pipelines.release_from_fbc.is_nvr_embargoed', return_value=False),
        patch('pyartcd.pipelines.release_from_fbc.resolve_shipment_release_plans', return_value=('stage', 'prod')),
        patch('pyartcd.pipelines.release_from_fbc.validate_snapshot_against_rpa', new_callable=AsyncMock),
    ):
        yield


@pytest.mark.parametrize('unified', [False, True])
def test_local_outputs_and_unified_consumer_contract(tmp_path, unified):
    release = pipeline(tmp_path, unified_fbc=unified)
    asyncio.run(release.run())
    config = YAML.load(tmp_path / 'assembly.yml')
    definition = config['releases']['1.5.8']['assembly']
    assert definition['type'] == 'standard'
    assert definition['members']['images'] == [
        {
            'distgit_key': 'oadp-operator',
            'why': 'Pin this image build for the assembly',
            'metadata': {'is': {'nvr': PIN}},
        }
    ]
    assert definition['basis']['fbc_pullspecs'] == {'4.22': PULLSPEC}
    shipments = release.write_shipment_files_locally.await_args.args[0]
    assert shipments['image'].shipment.snapshot.nvrs == [PIN, BUNDLE]
    assert ('fbc01' in shipments) is not unified
    if unified:
        records = recorded_assembly_fbcs(config, '1.5.8', '4.22')
        assert records['oadp-operator']['nvr'] == FBC
        assert records['oadp-operator']['operand_nvrs'] == [PIN]
    release._update_layered_product_shipment_mr.assert_not_awaited()
    release.create_shipment_mr.assert_not_awaited()


def test_unified_records_honor_the_loaded_patch_policy(tmp_path):
    """Generate usable unified records for a cross-patch FBC explicitly allowed by the group."""
    release = pipeline(tmp_path, unified_fbc=True)
    release.extract_fbc_nvr.return_value = FBC.replace('1.5.8', '1.5.7')
    release._load_ignore_patch_version_from_group_config.return_value = True
    asyncio.run(release.run())
    config = YAML.load(tmp_path / 'assembly.yml')
    records = recorded_assembly_fbcs(config, '1.5.8', '4.22', ignore_patch_version=True)
    assert records['oadp-operator']['nvr'] == FBC.replace('1.5.8', '1.5.7')
    assert records['oadp-operator']['operand_nvrs'] == [PIN]


@pytest.mark.parametrize(
    'data_path', ['git@github.com:my-team/build-data.git', 'https://github.com/my-team/build-data']
)
def test_assembly_lookup_normalizes_remote_build_data_and_preserves_gitref(tmp_path, data_path):
    """Read SSH and HTTPS build-data URLs from the same repository and requested revision."""
    release = ReleaseFromFbcPipeline(MagicMock(working_dir=tmp_path, dry_run=True, config={}), 'oadp-1.5', '1.5.8', [])
    with (
        patch.dict(os.environ, {'DOOZER_DATA_PATH': data_path, 'DOOZER_DATA_GITREF': 'release-policy'}),
        patch('pyartcd.pipelines.release_from_fbc.get_github_client_for_org') as client,
    ):
        repo = client.return_value.get_repo.return_value
        repo.get_contents.return_value.decoded_content = b'releases: {}\n'
        assert release._load_layered_product_shipment_mr() is None
    client.assert_called_once_with('my-team')
    client.return_value.get_repo.assert_called_once_with('my-team/build-data')
    repo.get_contents.assert_called_once_with('releases.yml', ref='release-policy')
    assert release._releases_config == {'releases': {}}


@pytest.mark.parametrize('test_mode,dry_run', [(False, False), (True, False), (False, True)])
def test_creation_persists_before_ready_unless_test_mode(tmp_path, test_mode, dry_run):
    release = pipeline(tmp_path, create_mr=True, test_mode=test_mode)
    release.dry_run = dry_run
    events = []
    release._update_layered_product_shipment_mr.side_effect = lambda *_: events.append('persist')
    release.set_shipment_mr_ready.side_effect = lambda: events.append('ready')
    asyncio.run(release.run())
    assert events == (['ready'] if test_mode else ['persist', 'ready'])


def test_reuse_updates_assembly_before_ready_and_failure_stops_ready(tmp_path):
    release = pipeline(tmp_path, create_mr=True)
    release._configured_shipment_mr_url = 'https://gitlab.example/art/data/-/merge_requests/123'
    release._load_layered_product_shipment_mr.return_value = release._configured_shipment_mr_url
    release._update_layered_product_shipment_mr.side_effect = RuntimeError('concurrent edit')
    with (
        patch(
            'pyartcd.pipelines.release_from_fbc.validate_shipment_mr_for_operation', new_callable=AsyncMock
        ) as validate,
        patch('pyartcd.pipelines.release_from_fbc.reconcile_shipment_mr', new_callable=AsyncMock),
    ):
        validate.return_value = (MagicMock(title='Shipment', description='Release'), MagicMock())
        with pytest.raises(RuntimeError, match='concurrent edit'):
            asyncio.run(release.run())
    release._update_layered_product_shipment_mr.assert_awaited_once_with(release._configured_shipment_mr_url)
    release.set_shipment_mr_ready.assert_not_awaited()
    release.create_shipment_mr.assert_not_awaited()


@pytest.mark.parametrize('force', [False, True])
def test_production_history_stops_assembly_and_shipment_mutation(tmp_path, force):
    release = pipeline(tmp_path, create_mr=True, force=force)
    release._load_layered_product_shipment_mr.return_value = 'https://gitlab.example/art/data/-/merge_requests/123'
    with patch(
        'pyartcd.pipelines.release_from_fbc.validate_shipment_mr_for_operation', new_callable=AsyncMock
    ) as validate:
        validate.side_effect = ShipmentMRProductionError('production was attempted')
        with pytest.raises(ShipmentMRProductionError):
            asyncio.run(release.run())
    assert not (tmp_path / 'assembly.yml').exists()
    release.create_snapshot.assert_not_awaited()
    release._update_layered_product_shipment_mr.assert_not_awaited()
    release.create_shipment_mr.assert_not_awaited()


def test_unified_rejects_ocp_optional_before_external_work():
    with pytest.raises(ValueError, match='only supported for layered-product'):
        ReleaseFromFbcPipeline(MagicMock(), 'openshift-4.22', '4.22.0', [], unified_fbc=True, ocp_optional=True)
    context = release_from_fbc.make_context(
        'release-from-fbc', ['--group', 'oadp-1.5', '--assembly', '1.5.8', '--unified-fbc', '--extra-image-nvrs', PIN]
    )
    assert context.params['unified_fbc'] is True


def test_ocp_optional_does_not_generate_or_persist_lp_assembly(tmp_path):
    release = pipeline(tmp_path, ocp_optional=True)
    release.group = 'openshift-4.22'
    release.assembly = '4.22.0'
    release._prepare_assembly = AsyncMock(side_effect=AssertionError('LP path invoked'))
    asyncio.run(release.run())
    assert not (tmp_path / 'assembly.yml').exists()
    release._prepare_assembly.assert_not_awaited()
    release._update_layered_product_shipment_mr.assert_not_awaited()


def test_inherited_images_are_included_in_shipment_and_records(tmp_path):
    release = pipeline(tmp_path, unified_fbc=True)
    inherited = 'oadp-velero-container-1.5.7-1.p0'
    release._component_doozer_keys['oadp-velero-container'] = 'oadp-velero'
    release._releases_config = {
        'releases': {
            '1.5.7': {
                'assembly': {
                    'members': {'images': [{'distgit_key': 'oadp-velero', 'metadata': {'is': {'nvr': inherited}}}]}
                }
            },
            '1.5.8': {'assembly': {'basis': {'assembly': '1.5.7'}}},
        }
    }
    asyncio.run(release.run())
    shipments = release.write_shipment_files_locally.await_args.args[0]
    assert inherited in shipments['image'].shipment.snapshot.nvrs
    assert release._assembly_definition['group']['fbc_builds']['4.22']['oadp-operator']['operand_nvrs'] == sorted(
        [PIN, inherited]
    )


def test_refreshed_catalog_is_the_recorded_source(tmp_path):
    release = pipeline(tmp_path, unified_fbc=True, refresh_fbcs=True)
    refreshed_nvr = FBC.replace('20261111120000', '20261111130000')
    refreshed_pullspec = PULLSPEC.replace('a' * 64, 'b' * 64)
    release._refresh_fbc_pullspecs = AsyncMock(return_value=[refreshed_pullspec])
    release.extract_fbc_nvr.side_effect = [FBC, refreshed_nvr]
    make_snapshot = release.create_snapshot.side_effect

    async def snapshot(nvrs):
        result = await make_snapshot(nvrs)
        if nvrs == [refreshed_nvr]:
            result.spec.components[0].containerImage = refreshed_pullspec
        return result

    release.create_snapshot.side_effect = snapshot
    asyncio.run(release.run())
    definition = release._assembly_definition
    assert definition['basis']['fbc_pullspecs']['4.22'] == refreshed_pullspec
    assert definition['group']['fbc_builds']['4.22']['oadp-operator']['nvr'] == refreshed_nvr


def test_external_extras_are_not_invented_assembly_members(tmp_path):
    external = 'postgresql-container-16.0-1.p0'
    release = pipeline(tmp_path, extra_image_nvrs=[external])
    asyncio.run(release.run())
    assert external in release.write_shipment_files_locally.await_args.args[0]['image'].shipment.snapshot.nvrs
    assert len(release._assembly_definition['members']['images']) == 1


@pytest.mark.parametrize('dry_run', [False, True])
def test_actual_persistence_honors_dry_run_and_passes_complete_definition(tmp_path, dry_run):
    release = pipeline(tmp_path)
    release.dry_run = dry_run
    with (
        patch('pyartcd.pipelines.release_from_fbc.persist_direct_assembly', new_callable=AsyncMock) as persist,
        patch('pyartcd.pipelines.release_from_fbc.GitRepository') as repo,
    ):
        repo.return_value.setup = AsyncMock()
        asyncio.run(ReleaseFromFbcPipeline._update_layered_product_shipment_mr(release, 'new-mr'))
    if dry_run:
        persist.assert_not_awaited()
        repo.assert_not_called()
    else:
        persist.assert_awaited_once_with(
            repo.return_value,
            release.group,
            release.assembly,
            release._assembly_definition,
            release._releases_config,
            'new-mr',
        )
