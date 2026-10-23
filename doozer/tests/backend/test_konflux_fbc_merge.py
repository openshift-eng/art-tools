import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.model import Model
from artcommonlib.util import KubeCondition
from dockerfile_parse import DockerfileParser
from doozerlib import opm
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.backend.konflux_fbc import get_target_fbc_arches
from doozerlib.backend.konflux_fbc_merge import LayeredFbcInput, LayeredFbcMerger, merge_layered_catalogs


def package(name='quay', default='stable-3.17'):
    return {'schema': 'olm.package', 'name': name, 'defaultChannel': default}


def channel(name, entries, pkg='quay'):
    return {'schema': 'olm.channel', 'package': pkg, 'name': name, 'entries': entries}


def bundle(name, pkg='quay'):
    return {'schema': 'olm.bundle', 'package': pkg, 'name': name, 'image': f'registry.example/{name}@sha256:abc'}


def source(version, catalog, channels=None, pkg='quay'):
    return LayeredFbcInput(
        record=KonfluxFbcBuildRecord(
            name=f'{pkg}-operator-fbc',
            group=f'quay-{version}',
            version=f'{version}.1',
            release='1.ocp4.20',
            nvr=f'{pkg}-operator-fbc-{version}.1-1.ocp4.20',
            image_pullspec='quay.io/test/fbc@sha256:' + 'a' * 64,
            bundle_nvrs=[f'quay-operator-bundle-container-{version}.1-1'],
            rebase_repo_url='https://github.com/openshift-priv/art-fbc',
            rebase_commitish='b' * 40,
        ),
        bundle=KonfluxBundleBuildRecord(
            nvr=f'quay-operator-bundle-container-{version}.1-1', bundle_csv_name=f'{pkg}.v{version}.1'
        ),
        package=pkg,
        channels=channels or (f'stable-{version}',),
        catalog=catalog,
        group_revision='c' * 40,
        csv_name=f'{pkg}.v{version}.1',
    )


class TestMergeLayeredCatalogs(unittest.TestCase):
    def setUp(self):
        self.baseline = [
            package(),
            channel('stable-3.17', [{'name': 'quay.v3.17.0'}]),
            bundle('quay.v3.17.0'),
            package('other', 'stable'),
            channel('stable', [{'name': 'other.v1'}], 'other'),
            bundle('other.v1', 'other'),
        ]
        self.first = source(
            '3.17',
            [
                package(),
                channel(
                    'stable-3.17', [{'name': 'quay.v3.17.0'}, {'name': 'quay.v3.17.1', 'replaces': 'quay.v3.17.0'}]
                ),
                bundle('quay.v3.17.0'),
                bundle('quay.v3.17.1'),
            ],
        )
        self.second = source(
            '3.18',
            [
                package(default='stable-3.18'),
                channel('stable-3.17', [{'name': 'quay.v3.17.0'}]),
                bundle('quay.v3.17.0'),
                channel('stable-3.18', [{'name': 'quay.v3.18.1'}]),
                bundle('quay.v3.18.1'),
            ],
        )

    def test_preserves_updates_and_shared_history_independent_of_order(self):
        original = copy.deepcopy(self.baseline)
        result = merge_layered_catalogs(self.baseline, [self.first, self.second])
        self.assertEqual(result, merge_layered_catalogs(self.baseline, [self.second, self.first]))
        self.assertEqual(self.baseline, original)
        self.assertEqual(len([blob for blob in result if blob['schema'] == 'olm.package']), 1)
        self.assertEqual(
            {blob['name'] for blob in result if blob['schema'] == 'olm.bundle'},
            {'quay.v3.17.0', 'quay.v3.17.1', 'quay.v3.18.1'},
        )
        self.assertEqual(
            next(blob for blob in result if blob['schema'] == 'olm.package')['defaultChannel'], 'stable-3.18'
        )

    def test_preserves_unsupplied_production_channel(self):
        result = merge_layered_catalogs(self.baseline, [self.second])
        self.assertIn(self.baseline[1], result)
        self.assertIn(self.baseline[2], result)

    def test_different_operators_fail(self):
        other = source(
            '3.18',
            [
                package('other', 'stable'),
                channel('stable', [{'name': 'other.v1'}], 'other'),
                bundle('other.v1', 'other'),
            ],
            channels=('stable',),
            pkg='other',
        )
        with self.assertRaisesRegex(ValueError, 'exactly one operator package'):
            merge_layered_catalogs(self.baseline, [self.first, other])

    def test_missing_selected_csv_name_fails(self):
        self.first.csv_name = ''
        with self.assertRaisesRegex(ValueError, 'no selected CSV name'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_stale_catalog_missing_requested_csv_fails(self):
        self.first.csv_name = 'quay.v3.17.2'
        with self.assertRaisesRegex(ValueError, 'missing bundle quay/quay.v3.17.2 for selected CSV'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_selected_csv_must_belong_to_expected_package(self):
        self.first.catalog[-1]['package'] = 'other'
        with self.assertRaisesRegex(ValueError, 'missing bundle quay/quay.v3.17.1 for selected CSV'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_selected_csv_must_appear_in_every_declared_channel(self):
        self.first.channels = ('stable-3.17', 'candidate')
        self.first.catalog.append(channel('candidate', [{'name': 'quay.v3.17.0'}]))
        with self.assertRaisesRegex(ValueError, 'selected CSV quay.v3.17.1 is missing from channel quay/candidate'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_selected_csv_can_appear_in_multiple_channels(self):
        self.first.channels = ('stable-3.17', 'candidate')
        self.first.catalog.append(channel('candidate', [{'name': 'quay.v3.17.1'}]))
        result = merge_layered_catalogs(self.baseline, [self.first])
        self.assertIn(self.first.catalog[-1], result)

    def test_overlapping_channel_owners_fail(self):
        with self.assertRaisesRegex(ValueError, 'updated by both'):
            merge_layered_catalogs(self.baseline, [self.first, self.first])

    def test_stale_input_cannot_remove_published_entries(self):
        self.baseline[1]['entries'].append({'name': 'quay.v3.17.2', 'replaces': 'quay.v3.17.0'})
        self.baseline.append(bundle('quay.v3.17.2'))
        with self.assertRaisesRegex(ValueError, 'stale channel'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_conflicting_bundle_fails(self):
        self.first.catalog[2]['image'] = 'registry.example/changed@sha256:def'
        with self.assertRaisesRegex(ValueError, 'conflicting bundle'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_missing_bundle_fails(self):
        self.first.catalog.pop()
        with self.assertRaisesRegex(ValueError, 'missing bundle'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_missing_owned_channel_fails(self):
        self.first.channels = ('nonexistent',)
        with self.assertRaisesRegex(ValueError, 'missing channel'):
            merge_layered_catalogs(self.baseline, [self.first])

    def test_ambiguous_default_fails(self):
        self.second.catalog[0]['defaultChannel'] = 'candidate'
        with self.assertRaisesRegex(ValueError, 'Ambiguous default'):
            merge_layered_catalogs(self.baseline, [self.second])

    def test_new_package_bootstrap(self):
        result = merge_layered_catalogs([], [self.first])
        self.assertEqual(len(result), 4)


class TestLayeredFbcMerger(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.runtime = SimpleNamespace(
            product='quay',
            group='quay-3.18',
            assembly='stream',
            working_dir=self.temp.name,
            data_path='https://github.com/openshift-eng/ocp-build-data',
            group_config=Model({'arches': ['x86_64'], 'konflux': {'build_attempts': 2}}),
        )
        with (
            patch('doozerlib.backend.konflux_fbc_merge.KonfluxDb', side_effect=[Mock(), Mock()]),
            patch('doozerlib.backend.konflux_fbc.KonfluxClient.from_kubeconfig'),
        ):
            self.merger = LayeredFbcMerger(
                runtime=self.runtime,
                target_version='4.20',
                registry_auth=None,
                konflux_namespace='art-quay-tenant',
                konflux_kubeconfig=None,
                dry_run=True,
            )
        self.merger.inputs = [source('3.18', [])]
        self.merger._set_operator_identity('quay')
        self.merger.provenance = {'production_index': 'registry.example/index@sha256:abc'}
        self.merger._build_arches = ['x86_64']
        lookup_patch = patch('doozerlib.backend.konflux_fbc_merge.get_target_fbc_arches', new_callable=AsyncMock)
        self.arch_lookup = lookup_patch.start()
        self.arch_lookup.return_value = ['x86_64']
        self.addCleanup(lookup_patch.stop)

    async def test_prepares_release_labels_and_provenance(self):
        directory = Path(self.temp.name)
        (directory / 'catalog').mkdir()
        (directory / 'catalog' / 'catalog.yaml').write_text('')
        (directory / 'catalog.Dockerfile').write_text('FROM scratch\n')
        repo = SimpleNamespace(local_dir=directory)
        await self.merger._prepare_source(repo)
        dfp = DockerfileParser(str(directory / 'catalog.Dockerfile'))
        self.assertEqual(dfp.labels['com.redhat.art.nvr'], self.merger.nvr)
        self.assertEqual(dfp.envs['__doozer_group'], 'quay-3.18')
        self.assertEqual(dfp.envs['__doozer_bundle_nvrs'], self.merger.inputs[0].bundle.nvr)
        self.assertEqual(dfp.labels['com.redhat.art.name'], 'quay-fbc-unified')
        self.assertEqual(json.loads((directory / 'unified-provenance.json').read_text()), self.merger.provenance)
        self.assertEqual(self.merger.get_application_name('quay-3.18'), 'quay-fbc-unified-4-20')
        self.assertTrue((directory / 'catalog' / 'quay' / 'catalog.yaml').is_file())
        self.assertFalse((directory / 'catalog' / 'catalog.yaml').exists())

    async def test_replaces_previous_package_catalog_without_stale_files(self):
        for operator in ('quay-operator', 'container-security-operator'):
            with self.subTest(operator=operator):
                self.merger._set_operator_identity(operator)
                directory = Path(self.temp.name) / operator
                catalog_dir = directory / 'catalog'
                package_dir = catalog_dir / operator
                package_dir.mkdir(parents=True)
                (package_dir / 'catalog.yaml').write_text('previous catalog')
                (package_dir / 'stale.yaml').write_text('stale package catalog')
                (catalog_dir / 'stale.yaml').write_text('stale root catalog')
                (catalog_dir / 'catalog.yaml').write_text('updated catalog')
                (directory / 'catalog.Dockerfile').write_text('FROM scratch\n')

                await self.merger._prepare_source(SimpleNamespace(local_dir=directory))

                self.assertEqual((package_dir / 'catalog.yaml').read_text(), 'updated catalog')
                self.assertEqual(
                    {path.relative_to(catalog_dir) for path in catalog_dir.rglob('*') if path.is_file()},
                    {Path(operator) / 'catalog.yaml'},
                )

    def test_names_come_from_operator_and_target(self):
        self.merger._set_operator_identity('container-security-operator')
        self.assertEqual(self.merger.get_component_name('quay-3.16'), 'container-security-operator-fbc-unified-4-20')
        self.assertEqual(self.merger.fbc_git_branch, 'art-container-security-operator-fbc-unified-4-20')
        self.assertEqual(self.merger.working_dir.name, 'container-security-operator-fbc-unified-4-20')
        self.assertEqual(self.merger.nvr, f'container-security-operator-fbc-unified-4.20-{self.merger.release}')

    async def test_submission_and_records_use_target_architectures(self):
        client = self.merger._konflux_client
        client.ensure_application = AsyncMock()
        client.get_component = AsyncMock(return_value=None)
        client.ensure_component = AsyncMock()
        client.start_pipeline_run_for_image_build = AsyncMock()
        repo = SimpleNamespace(
            https_url='https://github.com/openshift-priv/art-fbc',
            commit_hash='a' * 40,
            branch=self.merger.fbc_git_branch,
        )
        self.arch_lookup.side_effect = get_target_fbc_arches
        selected = self.merger.inputs[0]
        selected.catalog = [package(), channel('stable-3.17', [{'name': selected.csv_name}]), bundle(selected.csv_name)]
        self._fbc_query(selected.record)
        self.merger.bundle_db.get_build_records_by_nvrs = AsyncMock(return_value=[selected.bundle])
        self.merger._load_group = Mock(return_value=({}, 'a' * 40))
        self.merger._pin_image = AsyncMock(side_effect=lambda image: image)
        labels = {
            'operators.operatorframework.io.bundle.package.v1': 'quay',
            'operators.operatorframework.io.bundle.channels.v1': 'stable-3.17',
        }
        for target, source_arches, target_arches in (
            ('4.12', list(KonfluxClient.SUPPORTED_ARCHES), ['x86_64']),
            ('4.13', list(KonfluxClient.SUPPORTED_ARCHES), ['x86_64']),
            ('4.20', ['x86_64'], ['x86_64', 'aarch64', 's390x', 'ppc64le']),
            ('4.20', None, ['x86_64', 'aarch64']),
        ):
            with self.subTest(target=target, source_arches=source_arches):
                self.merger.target_version = target
                self.merger.major_minor_override = tuple(map(int, target.split('.')))
                selected.record.nvr = f'quay-operator-fbc-3.18.1-1.ocp{target}'
                self.merger.group_config['arches'] = source_arches
                if source_arches is None:
                    self.merger.group_config.pop('arches')
                with (
                    patch(
                        'doozerlib.backend.konflux_fbc.exectools.cmd_gather_async',
                        new=AsyncMock(return_value=(0, ''.join(f'- {arch}\n' for arch in target_arches), '')),
                    ) as read_group,
                    patch(
                        'doozerlib.backend.konflux_fbc_merge.util.oc_image_info_for_arch_async',
                        new=AsyncMock(return_value={'config': {'config': {'Labels': labels}}}),
                    ),
                    patch(
                        'doozerlib.backend.konflux_fbc_merge.opm.render',
                        new=AsyncMock(side_effect=[[bundle(selected.csv_name)], selected.catalog, []]),
                    ),
                ):
                    await self.merger.prepare((selected.record.nvr,))
                read_group.assert_awaited_once_with(
                    [
                        'doozer',
                        f'--data-path={self.runtime.data_path}',
                        f'--group=openshift-{target}',
                        'config:read-group',
                        'konflux.arches',
                        '--yaml',
                    ]
                )
                await self.merger._start_build(repo, f'quay.io/test/fbc:{self.merger.nvr}')
                submitted = client.start_pipeline_run_for_image_build.await_args.kwargs
                identity = f'quay-fbc-unified-{target.replace(".", "-")}'
                self.assertEqual(submitted['generate_name'], f'{identity}-')
                self.assertEqual(submitted['component_name'], identity)
                self.assertEqual(submitted['application_name'], identity)
                self.assertEqual(submitted['target_branch'], f'art-{identity}')
                self.assertEqual(submitted['building_arches'], target_arches)
                plr = Mock()
                plr.to_dict.return_value = {
                    'status': {
                        'results': [
                            {'name': 'IMAGE_URL', 'value': 'quay.io/test/fbc:tag'},
                            {'name': 'IMAGE_DIGEST', 'value': 'sha256:' + 'd' * 64},
                        ]
                    }
                }
                for outcome in (
                    KonfluxBuildOutcome.PENDING,
                    KonfluxBuildOutcome.BUILD_ERROR,
                    KonfluxBuildOutcome.SUCCESS,
                ):
                    await self.merger._record_build(repo, plr, outcome)
                    self.assertEqual(self.merger.db.add_build.call_args.args[0].arches, submitted['building_arches'])
                self.arch_lookup.assert_awaited_once_with(
                    self.runtime.data_path, self.merger.major_minor_override, self.merger._logger
                )
                self.arch_lookup.reset_mock()

    async def test_prepare_rejects_failed_or_invalid_target_architectures(self):
        self.arch_lookup.side_effect = get_target_fbc_arches
        self.merger.major_minor_override = (4, 13)
        with patch('doozerlib.backend.konflux_fbc.exectools.cmd_gather_async', new_callable=AsyncMock) as read_group:
            read_group.side_effect = ChildProcessError('config lookup failed')
            with self.assertRaisesRegex(RuntimeError, 'Failed to read konflux.arches from group openshift-4.13'):
                await self.merger.prepare(('source-fbc',))
            read_group.side_effect = None
            for output in ('', '[]', 'x86_64\n', '- x86_64\n- 42\n', '- ""\n', '[invalid yaml'):
                with self.subTest(output=output):
                    read_group.return_value = (0, output, '')
                    with self.assertRaises((ValueError, RuntimeError)):
                        await self.merger.prepare(('source-fbc',))
            self.runtime.data_path = None
            with self.assertRaisesRegex(ValueError, 'no data path configured'):
                await self.merger.prepare(('source-fbc',))
        with self.assertRaisesRegex(ValueError, 'Prepare the target architectures'):
            self.merger._get_build_arches()
        self.merger._konflux_client.start_pipeline_run_for_image_build.assert_not_called()
        self.merger.db.add_build.assert_not_called()

    async def test_dry_run_does_not_create_auth_or_build(self):
        client = self.merger._konflux_client
        await self.merger._build(Mock(), 'quay.io/test/fbc:tag', None)
        client.ensure_git_auth_secret.assert_not_called()
        client.start_pipeline_run_for_image_build.assert_not_called()
        self.merger.db.add_build.assert_not_called()

    async def test_records_pending_and_success(self):
        self.merger.db.add_build = Mock()
        plr = {
            'metadata': {'name': 'unified-test'},
            'status': {
                'results': [
                    {'name': 'IMAGE_URL', 'value': 'quay.io/test/fbc:tag'},
                    {'name': 'IMAGE_DIGEST', 'value': 'sha256:' + 'd' * 64},
                ]
            },
        }
        plr_info = Mock(name='plr')
        plr_info.name = 'unified-test'
        plr_info.to_dict.return_value = plr
        repo = SimpleNamespace(https_url='https://github.com/openshift-priv/art-fbc', commit_hash='a' * 40)
        await self.merger._record_build(repo, plr_info, KonfluxBuildOutcome.PENDING)
        await self.merger._record_build(repo, plr_info, KonfluxBuildOutcome.SUCCESS)
        pending, success = [call.args[0] for call in self.merger.db.add_build.call_args_list]
        self.assertEqual(pending.outcome, KonfluxBuildOutcome.PENDING)
        self.assertIsNone(pending.image_pullspec)
        self.assertEqual(success.image_pullspec, 'quay.io/test/fbc@sha256:' + 'd' * 64)
        self.assertEqual(success.bundle_nvrs, [self.merger.inputs[0].bundle.nvr])
        self.assertEqual(success.build_component, 'quay-fbc-unified-4-20')
        self.assertEqual(success.name, 'quay-fbc-unified')
        self.assertEqual(success.arches, ['x86_64'])

    async def test_records_all_outcomes_without_configured_arches(self):
        self.merger.group_config.pop('arches')
        self.merger._build_arches = ['x86_64', 'aarch64']
        plr_info = Mock()
        plr_info.name = 'unified-test'
        plr_info.to_dict.return_value = {
            'status': {
                'results': [
                    {'name': 'IMAGE_URL', 'value': 'quay.io/test/fbc:tag'},
                    {'name': 'IMAGE_DIGEST', 'value': 'sha256:' + 'd' * 64},
                ]
            }
        }
        repo = SimpleNamespace(https_url='https://github.com/openshift-priv/art-fbc', commit_hash='a' * 40)
        for outcome in (KonfluxBuildOutcome.PENDING, KonfluxBuildOutcome.SUCCESS, KonfluxBuildOutcome.BUILD_ERROR):
            with self.subTest(outcome=outcome):
                await self.merger._record_build(repo, plr_info, outcome)
                record = self.merger.db.add_build.call_args.args[0]
                self.assertEqual(record.arches, ['x86_64', 'aarch64'])

    async def test_success_without_digest_fails(self):
        plr = Mock()
        plr.to_dict.return_value = {'status': {'results': []}}
        with self.assertRaisesRegex(ValueError, 'no IMAGE_URL or IMAGE_DIGEST'):
            await self.merger._record_build(Mock(), plr, KonfluxBuildOutcome.SUCCESS)

    def _fbc_query(self, record):
        async def search(**kwargs):
            self.assertEqual(kwargs['where']['outcome'], str(KonfluxBuildOutcome.SUCCESS))
            self.assertNotIn('group', kwargs['where'])
            yield record

        self.merger.db.search_builds_by_fields = search

    async def test_prepare_rejects_wrong_target(self):
        self._fbc_query(self.merger.inputs[0].record)
        self.merger.inputs[0].record.nvr = 'quay-operator-fbc-3.18.1-1.ocp5.0'
        with self.assertRaisesRegex(ValueError, 'not a successful FBC'):
            await self.merger.prepare(('quay-operator-fbc-3.18.1-1.ocp5.0',))

    async def test_pullspec_must_match_database(self):
        self._fbc_query(self.merger.inputs[0].record)
        self.merger._pin_image = AsyncMock(
            side_effect=['quay.io/request@sha256:' + '1' * 64, 'quay.io/record@sha256:' + '2' * 64]
        )
        self.merger._load_group = Mock(return_value=({}, 'a' * 40))
        with patch(
            'doozerlib.backend.konflux_fbc_merge.util.oc_image_info_for_arch_async',
            new=AsyncMock(
                return_value={
                    'config': {'config': {'Labels': {'com.redhat.art.nvr': self.merger.inputs[0].record.nvr}}}
                }
            ),
        ):
            with self.assertRaisesRegex(ValueError, 'does not match'):
                await self.merger.prepare(('quay.io/request:tag',))

    async def test_pin_keeps_manifest_list_digest(self):
        with patch(
            'doozerlib.backend.konflux_fbc_merge.util.oc_image_info_for_arch_async',
            new=AsyncMock(
                return_value={
                    'digest': 'sha256:' + 'a' * 64,
                    'listDigest': 'sha256:' + 'b' * 64,
                }
            ),
        ):
            self.assertEqual(
                await self.merger._pin_image('quay.io/example/fbc:latest'), 'quay.io/example/fbc@sha256:' + 'b' * 64
            )

    async def test_prepare_uses_bundle_channels_and_pins_production(self):
        selected = self.merger.inputs[0]
        selected.bundle.bundle_csv_name = 'quay.3.18.1'
        selected.catalog = [
            package(default='stable-3.18'),
            channel('stable-3.18', [{'name': 'quay.v3.18.1'}]),
            bundle('quay.v3.18.1'),
        ]
        self._fbc_query(selected.record)
        self.merger.bundle_db.get_build_records_by_nvrs = AsyncMock(return_value=[selected.bundle])
        self.merger._load_group = Mock(return_value=({}, 'c' * 40))
        self.merger._pin_image = AsyncMock(
            side_effect=[
                selected.record.image_pullspec,
                'registry.redhat.io/redhat/redhat-operator-index@sha256:' + 'b' * 64,
            ]
        )
        labels = {
            'operators.operatorframework.io.bundle.package.v1': 'quay',
            'operators.operatorframework.io.bundle.channels.v1': 'stable-3.18',
        }
        with (
            patch(
                'doozerlib.backend.konflux_fbc_merge.util.oc_image_info_for_arch_async',
                new=AsyncMock(return_value={'config': {'config': {'Labels': labels}}}),
            ),
            patch(
                'doozerlib.backend.konflux_fbc_merge.opm.render',
                new=AsyncMock(side_effect=[[bundle(selected.csv_name)], selected.catalog, []]),
            ),
        ):
            await self.merger.prepare((selected.record.nvr,))
        self.assertEqual(self.merger.inputs[0].channels, ('stable-3.18',))
        self.assertEqual(self.merger.provenance['operator'], 'quay')
        self.assertEqual(self.merger.identity, 'quay-fbc-unified-4-20')
        self.assertEqual(self.merger.provenance['inputs'][0]['group_revision'], 'c' * 40)
        self.assertEqual(self.merger.provenance['inputs'][0]['bundle_csv_name'], 'quay.v3.18.1')
        self.assertEqual(selected.bundle.bundle_csv_name, 'quay.3.18.1')
        self.assertEqual(
            self.merger.provenance['production_index'],
            'registry.redhat.io/redhat/redhat-operator-index@sha256:' + 'b' * 64,
        )
        self.merger._pin_image.assert_any_await('registry.redhat.io/redhat/redhat-operator-index:v4.20')
        self.arch_lookup.assert_awaited_once_with(self.runtime.data_path, (4, 20), self.merger._logger)
        self.assertEqual(self.merger._get_build_arches(), ['x86_64'])

    async def test_dry_run_renders_and_validates_without_remote_writes(self):
        directory = self.merger.working_dir
        repo = Mock(local_dir=directory)
        repo.ensure_source = AsyncMock()
        repo.commit = AsyncMock()
        repo.push = AsyncMock()
        self.merger.catalog = [package(), channel('stable-3.17', [{'name': 'quay.v3.17.0'}]), bundle('quay.v3.17.0')]
        self.merger._merge_idms = AsyncMock(return_value={})

        async def generate(*args, **kwargs):
            (directory / 'catalog.Dockerfile').write_text('FROM scratch\n')

        async def validate_catalog(catalog_dir):
            self.assertEqual({path.name for path in catalog_dir.iterdir()}, {'quay'})
            with (catalog_dir / 'quay' / 'catalog.yaml').open() as catalog_file:
                self.assertEqual(list(opm.yaml.load_all(catalog_file)), self.merger.catalog)

        with (
            patch('doozerlib.backend.konflux_fbc.BuildRepo', return_value=repo),
            patch('doozerlib.backend.konflux_fbc.opm.generate_dockerfile', new=AsyncMock(side_effect=generate)),
            patch(
                'doozerlib.backend.konflux_fbc.opm.validate', new=AsyncMock(side_effect=validate_catalog)
            ) as validate,
        ):
            result = await self.merger.merge('quay.io/example/art-fbc')
        validate.assert_awaited_once_with(directory / 'catalog')
        repo.commit.assert_not_called()
        repo.push.assert_not_called()
        self.merger._konflux_client.ensure_git_auth_secret.assert_not_called()
        self.assertIsNone(result['pullspec'])
        self.assertIsNone(result['provenance_url'])
        self.assertTrue((directory / 'catalog' / 'quay' / 'catalog.yaml').is_file())

    async def test_prepare_accepts_same_operator_across_groups(self):
        selected = [
            source(
                version,
                [
                    package('container-security-operator', f'stable-{version}'),
                    channel(
                        f'stable-{version}',
                        [{'name': f'container-security-operator.v{version}.1'}],
                        'container-security-operator',
                    ),
                    bundle(f'container-security-operator.v{version}.1', 'container-security-operator'),
                ],
                pkg='container-security-operator',
            )
            for version in ('3.17', '3.18')
        ]

        async def search(**kwargs):
            yield next(item.record for item in selected if item.record.nvr == kwargs['where']['nvr'])

        self.merger.db.search_builds_by_fields = search
        self.merger.bundle_db.get_build_records_by_nvrs = AsyncMock(side_effect=[[item.bundle] for item in selected])
        self.merger._load_group = Mock(return_value=({}, 'c' * 40))
        self.merger._pin_image = AsyncMock(side_effect=lambda image: image)
        with (
            patch(
                'doozerlib.backend.konflux_fbc_merge.util.oc_image_info_for_arch_async',
                new=AsyncMock(
                    side_effect=[
                        {
                            'config': {
                                'config': {
                                    'Labels': {
                                        'operators.operatorframework.io.bundle.package.v1': item.package,
                                        'operators.operatorframework.io.bundle.channels.v1': item.channels[0],
                                    }
                                }
                            }
                        }
                        for item in selected
                    ]
                ),
            ),
            patch(
                'doozerlib.backend.konflux_fbc_merge.opm.render',
                new=AsyncMock(
                    side_effect=[
                        blobs for item in selected for blobs in ([bundle(item.csv_name, item.package)], item.catalog)
                    ]
                    + [[]]
                ),
            ),
        ):
            await self.merger.prepare(tuple(item.record.nvr for item in selected))
        self.assertEqual(self.merger.identity, 'container-security-operator-fbc-unified-4-20')
        self.assertEqual({item['group'] for item in self.merger.provenance['inputs']}, {'quay-3.17', 'quay-3.18'})

    async def test_retries_and_records_failed_attempt_before_success(self):
        self.merger.dry_run = False
        self.merger._konflux_client.ensure_git_auth_secret = AsyncMock(return_value='git-auth-merge')
        self.merger._konflux_client.delete_git_auth_secret = AsyncMock()
        self.merger._konflux_client.token_refresh_loop = AsyncMock()
        failed = Mock()
        failed.find_condition.return_value = KubeCondition({'type': 'Succeeded', 'status': 'False', 'reason': 'Failed'})
        succeeded = Mock()
        succeeded.find_condition.return_value = KubeCondition(
            {'type': 'Succeeded', 'status': 'True', 'reason': 'Succeeded'}
        )
        for plr in (failed, succeeded):
            plr.to_dict.return_value = {
                'status': {
                    'results': [
                        {'name': 'IMAGE_URL', 'value': 'quay.io/test/fbc:tag'},
                        {'name': 'IMAGE_DIGEST', 'value': 'sha256:' + 'd' * 64},
                    ]
                }
            }
        self.merger._start_build = AsyncMock(side_effect=[failed, succeeded])
        self.merger._record_build = AsyncMock(wraps=self.merger._record_build)
        self.merger._konflux_client.wait_for_pipelinerun = AsyncMock(side_effect=[failed, succeeded])
        repo = SimpleNamespace(https_url='https://github.com/openshift-priv/art-fbc', commit_hash='a' * 40)
        await self.merger._build(repo, 'quay.io/example/art-fbc:tag', None)
        self.assertEqual(self.merger._start_build.await_count, 2)
        self.assertEqual(
            [call.args[2] for call in self.merger._record_build.await_args_list],
            [
                KonfluxBuildOutcome.PENDING,
                KonfluxBuildOutcome.BUILD_ERROR,
                KonfluxBuildOutcome.PENDING,
                KonfluxBuildOutcome.SUCCESS,
            ],
        )
        self.merger._konflux_client.delete_git_auth_secret.assert_awaited_once()
        self.assertEqual([call.args[0].arches for call in self.merger.db.add_build.call_args_list], [['x86_64']] * 4)

    async def test_source_group_rejects_other_products_and_targets(self):
        for config in (
            {'product': 'logging', 'OCP_TARGET_VERSIONS': ['4.20']},
            {'product': 'quay', 'OCP_TARGET_VERSIONS': ['5.0']},
        ):
            with (
                patch('doozerlib.backend.konflux_fbc_merge.GitData'),
                patch('doozerlib.backend.konflux_fbc_merge.BuildDataLoader') as loader,
            ):
                loader.return_value.load_group_config.return_value = config
                with self.assertRaisesRegex(ValueError, 'does not support product'):
                    self.merger._load_group('quay-3.18')
