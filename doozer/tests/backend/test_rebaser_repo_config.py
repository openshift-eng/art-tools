import configparser
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

import yaml
from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import RepoList
from artcommonlib.model import Model
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.repos import CONTENT_SETS, Repos


class TestRebaserRepoConfig(IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.dest_dir = Path(self.directory.name)
        self.arches = ['x86_64', 'aarch64', 'ppc64le', 's390x']
        repo_configs = []
        for name in ['base', 'restricted', 'non-shipping', 'disabled', 'not-enabled']:
            repo_configs.append(
                {
                    'name': name,
                    'type': 'plashet',
                    'conf': {'enabled': name != 'disabled'},
                    'content_set': {arch: f'{name}-{arch}' for arch in self.arches},
                    'plashet': {
                        'arches': ['x86_64', 'aarch64'] if name == 'restricted' else self.arches,
                        'source': {'type': 'brew', 'from_tags': []},
                    },
                }
            )
        self.repos = Repos(
            RepoList.model_validate(repo_configs),
            self.arches,
            plashet_config=PlashetConfig(base_url='https://example.com', base_dir='$slug', plashet_dir='latest'),
        )
        self.enabled_repos = ['base', 'restricted', 'non-shipping', 'disabled']
        self.metadata = SimpleNamespace(
            distgit_key='test-image',
            config=Model({'enabled_repos': self.enabled_repos, 'non_shipping_repos': ['non-shipping']}),
        )
        self.rebaser = SimpleNamespace(_logger=logging.getLogger(__name__))

    async def test_four_arch_image_content_sets_respect_repo_arches(self):
        """Keep unrelated and non-shipping repo sections while filtering shipping metadata."""
        await KonfluxRebaser._generate_repo_conf(self.rebaser, self.metadata, self.dest_dir, self.repos)

        content = self.dest_dir.joinpath('content_sets.yml').read_text()
        self.assertTrue(content.startswith(CONTENT_SETS))
        self.assertEqual(
            yaml.safe_load(content),
            {
                'aarch64': ['base-aarch64', 'restricted-aarch64'],
                'ppc64le': ['base-ppc64le'],
                's390x': ['base-s390x'],
                'x86_64': ['base-x86_64', 'restricted-x86_64'],
            },
        )
        for repo_type in self.repos.repotypes:
            repo_config = configparser.ConfigParser()
            repo_config.read(self.dest_dir.joinpath('.oit', f'art-{repo_type}.repo'))
            for arch in self.arches:
                self.assertIn(f'base-{arch}', repo_config)
                self.assertIn(f'non-shipping-{arch}', repo_config)
                self.assertNotIn(f'disabled-{arch}', repo_config)
                self.assertNotIn(f'not-enabled-{arch}', repo_config)
                self.assertEqual(f'restricted-{arch}' in repo_config, arch in ['x86_64', 'aarch64'])

    async def test_restricted_only_omits_unsupported_content_set_arches(self):
        self.metadata.config = Model({'enabled_repos': ['restricted']})
        await KonfluxRebaser._generate_repo_conf(self.rebaser, self.metadata, self.dest_dir, self.repos)

        self.assertEqual(
            yaml.safe_load(self.dest_dir.joinpath('content_sets.yml').read_text()),
            {'aarch64': ['restricted-aarch64'], 'x86_64': ['restricted-x86_64']},
        )

    async def test_no_enabled_repos_preserves_empty_output(self):
        self.metadata.config = Model({})
        await KonfluxRebaser._generate_repo_conf(self.rebaser, self.metadata, self.dest_dir, self.repos)

        self.assertEqual(yaml.safe_load(self.dest_dir.joinpath('content_sets.yml').read_text()), {})

    async def test_non_shipping_restricted_repo_is_not_in_content_sets(self):
        self.metadata.config = Model({'enabled_repos': ['restricted'], 'non_shipping_repos': ['restricted']})
        await KonfluxRebaser._generate_repo_conf(self.rebaser, self.metadata, self.dest_dir, self.repos)

        self.assertEqual(yaml.safe_load(self.dest_dir.joinpath('content_sets.yml').read_text()), {})
        self.assertIn('[restricted-x86_64]', self.dest_dir.joinpath('.oit', 'art-unsigned.repo').read_text())

    def test_legacy_call_still_rejects_unsupported_repo_arches(self):
        with self.assertRaisesRegex(ValueError, 'ppc64le is not a valid arch'):
            self.repos.content_sets(enabled_repos=['restricted'])

    def test_unrestricted_output_unchanged_by_opt_in(self):
        self.assertEqual(
            self.repos.content_sets(enabled_repos=['base']),
            self.repos.content_sets(enabled_repos=['base'], respect_repo_arches=True),
        )

    def test_undefined_enabled_repo_still_rejected(self):
        with self.assertRaisesRegex(ValueError, 'enabled_repos references undefined repo'):
            self.repos.content_sets(enabled_repos=['undefined'], respect_repo_arches=True)

    def test_supported_arch_content_set_errors_still_propagate(self):
        self.repos['restricted']._data.content_set = Model({})
        with self.assertRaisesRegex(ValueError, 'does not contain a content_set'):
            self.repos.content_sets(enabled_repos=['restricted'], respect_repo_arches=True)

    async def test_global_repo_intersection_and_disjoint_outputs(self):
        """Never emit metadata or URLs for uncomposed arches, even for enabled empty repos."""
        for valid_arches, repo_arches, expected in [
            (self.arches, ['x86_64', 's390x'], ['x86_64']),
            (['x86_64'], ['s390x'], []),
        ]:
            for enabled in [True, False]:
                with self.subTest(valid_arches=valid_arches, repo_arches=repo_arches, enabled=enabled):
                    configs = []
                    for name, arches in [('base', None), ('restricted', repo_arches)]:
                        configs.append(
                            {
                                'name': name,
                                'type': 'plashet',
                                'conf': {'enabled': enabled if name == 'restricted' else True},
                                'content_set': {arch: f'{name}-{arch}' for arch in self.arches},
                                'plashet': {'arches': arches, 'source': {'type': 'brew', 'from_tags': []}},
                            }
                        )
                    repos = Repos(
                        RepoList.model_validate(configs),
                        valid_arches,
                        plashet_config=PlashetConfig(
                            base_url='https://example.com',
                            base_dir='repos',
                            plashet_dir='latest',
                            arches=['x86_64', 'aarch64'],
                        ),
                        respect_repo_arches=True,
                    )
                    self.metadata.config = Model({'enabled_repos': ['base', 'restricted']})
                    await KonfluxRebaser._generate_repo_conf(self.rebaser, self.metadata, self.dest_dir, repos)
                    self.assertIn('restricted', repos.names)
                    self.assertEqual(repos['restricted'].arches, expected)
                    contents = yaml.safe_load(self.dest_dir.joinpath('content_sets.yml').read_text())
                    sections = configparser.ConfigParser()
                    sections.read(self.dest_dir.joinpath('.oit', 'art-unsigned.repo'))
                    for arch in self.arches:
                        self.assertEqual(f'restricted-{arch}' in sections, enabled and arch in expected)
                        self.assertEqual(f'restricted-{arch}' in contents.get(arch, []), enabled and arch in expected)
                        self.assertEqual(f'base-{arch}' in sections, arch in repos['base'].arches)
