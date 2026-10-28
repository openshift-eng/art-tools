"""Exercise Konflux repository limits without changing legacy Brew initialization."""

import logging
from contextlib import ExitStack
from itertools import product
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import RepoList
from artcommonlib.model import Model
from doozerlib.repos import Repo, Repos
from doozerlib.runtime import Runtime

ARCHES = ['x86_64', 'aarch64', 'ppc64le', 's390x']


def repo_config(arches, enabled=True):
    """Build a plashet fixture with per-architecture shipping metadata."""
    return {
        'name': 'restricted',
        'type': 'plashet',
        'conf': {'enabled': enabled},
        'content_set': {arch: f'restricted-{arch}' for arch in ARCHES},
        'plashet': {'arches': arches, 'source': {'type': 'brew', 'from_tags': []}},
    }


def plashet_config(arches=None):
    """Use public example URLs and deterministic directory names."""
    return PlashetConfig(base_url='https://example.com', base_dir='repos', plashet_dir='latest', arches=arches)


class TestRepoArchLimits(TestCase):
    def test_global_and_repo_limits_intersect_only_on_opt_in(self):
        """Reproduce cross-cut limits while preserving the legacy replacement rule."""
        config = RepoList.model_validate([repo_config(['x86_64', 's390x'])]).root[0]
        valid_arches = list(ARCHES)
        global_config = plashet_config(['x86_64', 'aarch64'])
        for opt_in, expected in [(False, ['x86_64', 's390x']), (True, ['x86_64'])]:
            with self.subTest(opt_in=opt_in):
                repo = Repo.from_repo_config(
                    config, valid_arches, plashet_config=global_config, respect_repo_arches=opt_in
                )
                self.assertEqual(repo.arches, expected)
                self.assertEqual(list(repo.to_dict()['conf']['baseurl']), expected)
        self.assertEqual(valid_arches, ARCHES)
        self.assertEqual(global_config.arches, ['x86_64', 'aarch64'])
        self.assertEqual(config.plashet.arches, ['x86_64', 's390x'])

    def test_fallback_and_request_order(self):
        """Omitted, None and empty lists mean no additional limit, never an expansion."""
        for override in ['omitted', None, []]:
            for global_arches in [None, [], ['x86_64', 'aarch64']]:
                with self.subTest(override=override, global_arches=global_arches):
                    config = repo_config(None if override == 'omitted' else override)
                    if override == 'omitted':
                        del config['plashet']['arches']
                    repo = Repo.from_repo_config(
                        RepoList.model_validate([config]).root[0],
                        ['aarch64', 'x86_64'],
                        plashet_config=plashet_config(global_arches),
                        respect_repo_arches=True,
                    )
                    self.assertEqual(repo.arches, ['aarch64', 'x86_64'])

    def test_disjoint_repos_retained_only_on_opt_in(self):
        """Unused and enabled repos remain addressable even with no applicable architecture."""
        for enabled in [True, False]:
            with self.subTest(enabled=enabled):
                configs = RepoList.model_validate([repo_config(['s390x'], enabled)])
                with self.assertRaises(IndexError):
                    Repos(configs, ['x86_64'], plashet_config=plashet_config())
                repos = Repos(configs, ['x86_64'], plashet_config=plashet_config(), respect_repo_arches=True)
                self.assertIn('restricted', repos.names)
                self.assertEqual(repos['restricted'].arches, [])
                self.assertEqual(repos['restricted'].to_dict()['conf']['baseurl'], {})
                self.assertEqual(repos.repo_file('unsigned', enabled_repos=['restricted'], konflux=True), '')

    def test_supported_arch_errors_and_custom_repotypes_preserved(self):
        """The opt-in must not suppress malformed URLs or normal repotype discovery."""
        for opt_in in [False, True]:
            with self.subTest(opt_in=opt_in):
                config = repo_config(['x86_64'])
                config['conf']['baseurl'] = {'unsigned': {'aarch64': 'https://example.com'}}
                with self.assertRaisesRegex(ValueError, 'No baseurl available for arch x86_64'):
                    Repo.from_repo_config(
                        RepoList.model_validate([config]).root[0],
                        ['x86_64'],
                        plashet_config=plashet_config(),
                        respect_repo_arches=opt_in,
                    )
                config['conf']['baseurl'] = {
                    repotype: {'x86_64': 'https://example.com'} for repotype in ['unsigned', 'custom']
                }
                repo = Repo.from_repo_config(
                    RepoList.model_validate([config]).root[0],
                    ['x86_64'],
                    plashet_config=plashet_config(),
                    respect_repo_arches=opt_in,
                )
                self.assertEqual(repo.repotypes, ['unsigned', 'custom'])

    def test_runtime_initialize_gates_real_repo_construction(self):
        """Run initialize through actual Repos construction, stopping before image/network work."""

        class ReposConstructed(Exception):
            """Stop after exercising the real runtime-to-factory call chain."""

        for backend in ['konflux', 'brew', None]:
            for old_style, disjoint, enabled in product([False, True], repeat=3):
                with (
                    self.subTest(backend=backend, old_style=old_style, disjoint=disjoint, enabled=enabled),
                    TemporaryDirectory() as directory,
                ):
                    group = {
                        'name': 'test',
                        'arches': ARCHES,
                        'plashet': plashet_config(['x86_64', 'aarch64']).model_dump(),
                    }
                    if old_style:
                        group['repos'] = {
                            'external': {'conf': {'baseurl': 'https://example.com'}, 'content_set': {'default': 'base'}}
                        }
                    else:
                        group['all_repos'] = [repo_config(['s390x'] if disjoint else ['x86_64', 's390x'], enabled)]
                    runtime = Runtime(
                        working_dir=directory, data_dir=directory, group='test', source=[], brew_event=1, state={}
                    )
                    runtime._logger = logging.getLogger(__name__)
                    if disjoint:
                        runtime.arches = ['x86_64']
                    runtime._build_data_loader = Mock()
                    runtime._build_data_loader.load_config.return_value = {}
                    captured = []

                    def construct(*args, **kwargs):
                        captured.append(Repos(*args, **kwargs))
                        raise ReposConstructed()

                    with ExitStack() as stack:
                        for method in ['initialize_logging', 'initialize_konflux_db', 'init_state', 'resolve_metadata']:
                            stack.enter_context(patch.object(runtime, method))
                        stack.enter_context(patch.object(runtime, 'get_group_config', return_value=Model(group)))
                        stack.enter_context(patch.object(runtime, 'get_releases_config', return_value=Model({})))
                        stack.enter_context(patch('doozerlib.runtime.dblib.DB'))
                        stack.enter_context(patch('doozerlib.runtime.SourceResolver'))
                        stack.enter_context(patch('doozerlib.runtime.Repos', side_effect=construct))
                        legacy_disjoint = disjoint and not old_style and backend != 'konflux'
                        with self.assertRaises(IndexError if legacy_disjoint else ReposConstructed):
                            runtime.initialize(build_system=backend)
                    if legacy_disjoint:
                        self.assertEqual(captured, [])
                        continue
                    if old_style:
                        self.assertEqual(captured[0]['external'].arches, ['x86_64'] if disjoint else ARCHES)
                    else:
                        expected = [] if disjoint else ['x86_64'] if backend == 'konflux' else ['x86_64', 's390x']
                        self.assertEqual(captured[0]['restricted'].arches, expected)
