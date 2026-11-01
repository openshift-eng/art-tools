"""Exercise restricted repositories at lockfile and optional validation consumer boundaries."""

import logging
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

import yaml
from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import RepoList
from artcommonlib.model import Model
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli.images_konflux import KonfluxRebaseCli
from doozerlib.image import ImageMetadata
from doozerlib.lockfile import RpmInfoCollector, RPMLockfileGenerator
from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repos
from doozerlib.runtime import Runtime

ARCHES = ['x86_64', 'aarch64', 'ppc64le', 's390x']


def repo_configs():
    """Provide one restricted plashet and an unrelated repository on every architecture."""
    return [
        {
            'name': name,
            'type': 'plashet',
            'content_set': {arch: f'{name}-{arch}' for arch in ARCHES},
            'plashet': {
                'arches': ['x86_64', 'aarch64'] if name == 'restricted' else None,
                'source': {'type': 'brew', 'from_tags': []},
            },
        }
        for name in ['restricted', 'base']
    ]


def make_repos(arches=None, opt_in=True, optional=False):
    """Use real schema conversion and repository objects without external services."""
    configs = repo_configs()
    configs[0]['content_set']['optional'] = optional
    return Repos(
        RepoList.model_validate(configs),
        ARCHES if arches is None else arches,
        plashet_config=PlashetConfig(base_url='https://example.com', base_dir='repos', plashet_dir='latest'),
        respect_repo_arches=opt_in,
    )


class TestRepoArchConsumers(IsolatedAsyncioTestCase):
    def setUp(self):
        """Keep network IO mocked while exercising actual loader and resolution code."""
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.repos = make_repos()
        self.logger = Mock()
        self.generator = RPMLockfileGenerator(self.repos, logger=self.logger)
        self.load = self.enterContext(patch('doozerlib.repos.RepodataLoader.load', side_effect=self.load_repodata))

    async def load_repodata(self, name, url):
        """Return real RPM metadata for each supported fetch instead of contacting a repository."""
        arch = name.rsplit('-', 1)[1]
        rpm = Rpm('test-rpm', 0, '1', 'checksum', 1, 'test.rpm', 'test-1-1.src.rpm', '1', arch)
        return Repodata(name, primary_rpms=[rpm])

    def image(self, backend='art-internal'):
        """Use real backend/repo selection and a minimal image RPM request."""
        runtime = SimpleNamespace(repos=self.repos, group_config=Model({}))
        metadata = SimpleNamespace(
            runtime=runtime,
            logger=logging.getLogger(__name__),
            config=Model(
                {'enabled_repos': list(self.repos.names), 'konflux': {'cachi2': {'lockfile': {'backend': backend}}}}
            ),
            distgit_key='test-image',
            get_arches=lambda: list(self.repos._arches),
            is_lockfile_generation_enabled=lambda: True,
            get_lockfile_rpms_to_install=AsyncMock(return_value={'test-rpm'}),
            get_lockfile_modules_to_install=lambda: set(),
            is_cross_arch_enabled=lambda: False,
        )
        metadata.get_enabled_repos = lambda: ImageMetadata.get_enabled_repos(metadata)
        metadata.get_lockfile_backend = lambda: ImageMetadata.get_lockfile_backend(metadata)
        return metadata

    async def test_actual_cli_preloads_only_applicable_repos(self):
        """The art-internal CLI must reach rebase after loading six valid repo/arch pairs."""
        metadata = self.image()
        runtime = Mock()
        runtime.upcycle = False
        runtime.working_dir = self.directory.name
        runtime.ordered_image_metas.return_value = [metadata]
        runtime.state = {}
        rebaser = SimpleNamespace(rpm_lockfile_generator=self.generator, rebase_to=AsyncMock())
        with patch('doozerlib.cli.images_konflux.KonfluxRebaser', return_value=rebaser):
            cli = KonfluxRebaseCli(runtime, 'v5.1.0', '1', False, False, 'unsigned', 'test', 'test', False)
            await cli.run()
        runtime.initialize.assert_called_once_with(mode='images', clone_distgits=False, build_system='konflux')
        rebaser.rebase_to.assert_awaited_once()
        self.assertEqual(self.load.await_count, 6)
        self.assertNotIn('restricted-ppc64le', self.generator.builder.loaded_repos)
        self.assertIn('base-ppc64le', self.generator.builder.loaded_repos)
        await self.generator.ensure_repositories_loaded([metadata], Path(self.directory.name))
        self.assertEqual(self.load.await_count, 6)

    async def test_default_prototype_does_not_preload_legacy_repos(self):
        """Default backend selection must keep bypassing the art-internal loader."""
        metadata = self.image(backend=None)
        self.assertEqual(metadata.get_lockfile_backend(), 'rpm-lockfile-prototype')
        runtime = Mock(upcycle=False, working_dir=self.directory.name, state={})
        runtime.ordered_image_metas.return_value = [metadata]
        rebaser = SimpleNamespace(rpm_lockfile_generator=self.generator, rebase_to=AsyncMock())
        with (
            patch('doozerlib.cli.images_konflux.KonfluxRebaser', return_value=rebaser),
            patch.object(
                self.generator, 'ensure_repositories_loaded', wraps=self.generator.ensure_repositories_loaded
            ) as preload,
        ):
            await KonfluxRebaseCli(runtime, 'v5.1.0', '1', False, False, 'unsigned', 'test', 'test', False).run()
        preload.assert_not_called()
        self.load.assert_not_awaited()

    async def test_per_image_generation_resolves_supported_rpms(self):
        """Real generation must omit unsupported sources without losing unrelated RPM metadata."""
        metadata = self.image()
        dest = Path(self.directory.name)
        await self.generator.generate_lockfile(metadata, dest)
        lockfile = yaml.safe_load((dest / 'rpms.lock.yaml').read_text())
        self.assertEqual([entry['arch'] for entry in lockfile['arches']], ARCHES)
        for entry in lockfile['arches']:
            self.assertEqual(len(entry['packages']), 1)
            self.assertIn('test.rpm', entry['packages'][0]['url'])
            if entry['arch'] in ['ppc64le', 's390x']:
                self.assertEqual(entry['packages'][0]['repoid'], f"base-{entry['arch']}")
        self.logger.error.assert_not_called()

    async def test_per_image_prototype_dispatch_bypasses_legacy_generator(self):
        """Prototype lockfile writing must retain its separate generation path."""
        rebaser = SimpleNamespace(
            _logger=self.logger,
            rpm_lockfile_generator=self.generator,
            _write_rpms_lock_file_prototype=AsyncMock(),
        )
        await KonfluxRebaser._write_rpms_lock_file(rebaser, self.image(backend=None), Path(self.directory.name))
        rebaser._write_rpms_lock_file_prototype.assert_awaited_once()
        self.load.assert_not_awaited()

    async def test_zero_arch_repo_is_retained_but_not_loaded_or_resolved(self):
        """A completely inapplicable repo must not create fake cache entries or missing-cache errors."""
        self.repos = make_repos(['ppc64le'])
        self.generator = RPMLockfileGenerator(self.repos, logger=self.logger)
        self.assertEqual(self.repos['restricted'].arches, [])
        await self.generator.generate_lockfile(self.image(), Path(self.directory.name))
        self.assertEqual(set(self.generator.builder.loaded_repos), {'base-ppc64le'})
        self.assertIn('restricted', self.repos.names)
        self.logger.error.assert_not_called()

    def test_resolution_skips_even_stale_unsupported_cache(self):
        """Neither RPM nor module resolution may inspect an unsupported cached entry."""
        collector = self.generator.builder
        stale = Mock()
        type(stale).modules = property(lambda instance: self.fail('unsupported cache inspected'))
        collector.loaded_repos['restricted-ppc64le'] = stale
        self.assertEqual(collector._fetch_rpms_info_per_arch(set(), {'restricted'}, 'ppc64le'), [])
        self.assertEqual(collector._fetch_modules_info_per_arch(set(), {'restricted'}, 'ppc64le'), [])
        self.logger.error.assert_not_called()

    def test_missing_supported_cache_is_still_reported(self):
        """Filtering must not hide missing repodata for a supported repository."""
        collector = self.generator.builder
        collector._fetch_rpms_info_per_arch(set(), {'base'}, 'ppc64le')
        self.logger.error.assert_called_once_with(
            'repodata base-ppc64le not found while fetching rpms, it should be loaded by now'
        )
        self.logger.reset_mock()
        collector._fetch_modules_info_per_arch(set(), {'base'}, 'ppc64le')
        self.logger.error.assert_called_once_with('repodata base-ppc64le not found while fetching modules')

    async def test_legacy_opt_out_and_supported_url_errors_propagate(self):
        """Keep the old opt-out failure and do not suppress malformed supported URLs."""
        legacy = RpmInfoCollector(make_repos(opt_in=False))
        with self.assertRaisesRegex(ValueError, 'unsigned is not a valid repotype'):
            await legacy._load_repos({'restricted'}, 'ppc64le')
        self.repos['restricted']._data.conf.baseurl = Model({'unsigned': {'aarch64': 'https://example.com'}})
        with self.assertRaisesRegex(ValueError, 'No baseurl available for arch x86_64'):
            await self.generator.builder._load_repos({'restricted'}, 'x86_64')

    async def test_supported_fetch_failures_are_not_suppressed(self):
        """Unexpected failures fetching an applicable repository must still propagate."""
        self.load.side_effect = RuntimeError('mock repository unavailable')
        with self.assertRaisesRegex(RuntimeError, 'mock repository unavailable'):
            await self.generator.builder._load_repos({'base'}, 'ppc64le')

    def test_repo_filter_preserves_unknown_names_and_inputs(self):
        """Only known unsupported repos are omitted; unknown-name diagnostics remain possible."""
        names = {'restricted', 'base', 'unknown'}
        self.assertEqual(self.repos.repos_for_arch(names, 'ppc64le'), {'base', 'unknown'})
        self.assertEqual(names, {'restricted', 'base', 'unknown'})
        self.assertEqual(make_repos(opt_in=False).repos_for_arch(names, 'ppc64le'), names)

    def test_content_set_validation_skips_unsupported_but_preserves_base(self):
        """Availability checks cover supported content and all unrelated repository architectures."""
        for arches in [ARCHES, ['ppc64le']]:
            with self.subTest(arches=arches):
                repos = make_repos(arches)
                with patch.object(repos, '_validate_content_sets', side_effect=lambda arch, names: set(names)) as check:
                    repos.validate_content_sets()
                self.assertEqual(
                    {call.args[0]: set(call.args[1]) for call in check.call_args_list},
                    {
                        arch: {f'base-{arch}'} | ({f'restricted-{arch}'} if arch in ['x86_64', 'aarch64'] else set())
                        for arch in arches
                    },
                )

    def test_supported_content_set_configuration_errors_propagate(self):
        """A malformed supported content-set definition remains an error before availability IO."""
        self.repos['restricted']._data.content_set = Model({'aarch64': 'only-other-arch'})
        with patch.object(self.repos, '_validate_content_sets') as check:
            with self.assertRaisesRegex(ValueError, 'does not contain a content_set for x86_64'):
                self.repos.validate_content_sets()
        check.assert_not_called()

    def test_required_optional_and_legacy_validation_semantics(self):
        """Required-invalid sets still fail, optional-invalid sets are marked, and legacy lookup is unchanged."""
        for optional in [False, True]:
            with self.subTest(optional=optional):
                repos = make_repos(['x86_64'], optional=optional)
                with patch.object(repos, '_validate_content_sets', return_value={'base-x86_64'}):
                    if optional:
                        repos.validate_content_sets()
                        self.assertIsNone(repos['restricted'].content_set('x86_64'))
                    else:
                        with self.assertRaisesRegex(
                            ValueError, 'do not exist, and are not optional: x86_64/restricted-x86_64'
                        ):
                            repos.validate_content_sets()
        legacy = make_repos(opt_in=False)
        with patch.object(legacy, '_validate_content_sets', side_effect=lambda arch, names: set(names)):
            with self.assertRaisesRegex(ValueError, 'ppc64le is not a valid arch'):
                legacy.validate_content_sets()

    def test_runtime_optional_validation_uses_konflux_opt_in(self):
        """Exercise real Runtime validation with mock credential presence, never reading real credentials."""

        class Validated(Exception):
            """Stop after real successful validation, before unrelated runtime initialization."""

        runtime = Runtime(
            working_dir=self.directory.name,
            data_dir=self.directory.name,
            group='test',
            source=[],
            brew_event=1,
            state={},
        )
        runtime._logger = logging.getLogger(__name__)
        runtime._build_data_loader = Mock()
        runtime._build_data_loader.load_config.return_value = {}
        group = Model(
            {
                'name': 'test',
                'arches': ARCHES,
                'all_repos': repo_configs(),
                'plashet': {'base_dir': 'repos', 'plashet_dir': 'latest', 'base_url': 'https://example.com'},
            }
        )
        validate = Repos.validate_content_sets

        def validate_then_stop(collection):
            """Retain all actual validation logic before stopping runtime setup."""
            validate(collection)
            raise Validated()

        with ExitStack() as stack:
            for method in ['initialize_logging', 'initialize_konflux_db', 'init_state', 'resolve_metadata']:
                stack.enter_context(patch.object(runtime, method))
            stack.enter_context(patch.object(runtime, 'get_group_config', return_value=group))
            stack.enter_context(patch.object(runtime, 'get_releases_config', return_value=Model({})))
            stack.enter_context(patch('doozerlib.runtime.dblib.DB'))
            stack.enter_context(patch('doozerlib.runtime.SourceResolver'))
            stack.enter_context(
                patch('doozerlib.runtime.os.environ', {'RHSM_PULP_KEY': 'mock-key', 'RHSM_PULP_CERT': 'mock-cert'})
            )
            stack.enter_context(
                patch.object(Repos, '_validate_content_sets', side_effect=lambda arch, names: set(names))
            )
            stack.enter_context(patch.object(Repos, 'validate_content_sets', validate_then_stop))
            with self.assertRaises(Validated):
                runtime.initialize(build_system='konflux', validate_content_sets=True)
        self.assertEqual(runtime.repos.repos_for_arch({'restricted', 'base'}, 'ppc64le'), {'base'})
