#!/usr/bin/env python
"""
Test the Repo class
"""

import unittest
from unittest.mock import Mock, patch

from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import ContentSet, PlashetRepo, RepoSync
from artcommonlib.config.repo import Repo as RepoConf
from doozerlib.repos import Repo, Repos

EXPECTED_BASIC_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
name = rhaos-4.4-rhel-8-build
gpgcheck = 1
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

"""

EXPECTED_BASIC_UNSIGNED_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
name = rhaos-4.4-rhel-8-build
gpgcheck = 0
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

"""

EXPECTED_EXTRA_OPTIONS_REPO = """[rhaos-4.4-rhel-8-build]
baseurl = http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/
enabled = 1
module_hotfixes = 1
name = rhaos-4.4-rhel-8-build
gpgcheck = 1
gpgkey = file:///etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

"""


class MockRuntime(object):
    def __init__(self, logger):
        self.logger = logger


class TestRepo(unittest.IsolatedAsyncioTestCase):
    repo_config = {
        'conf': {
            'enabled': 1,
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            },
        },
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        },
    }
    repo_config_extras = {
        'conf': {
            'extra_options': {
                'module_hotfixes': 1,
            },
            'enabled': 1,
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            },
        },
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        },
    }

    no_baseurl_repo = {
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        },
    }
    no_config_sets_repo = {
        'conf': {
            'enabled': 1,
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            },
        },
    }
    enabled_no_baseurl_repo = {
        'conf': {
            'enabled': 1,
        },
        'content_set': {
            'default': 'rhel-7-server-ose-4.2-rpms',
            'ppc64le': 'rhel-7-for-power-le-ose-4.2-rpms',
            's390x': 'rhel-7-for-system-z-ose-4.2-rpms',
            'optional': True,
        },
    }
    no_config_set_arches_repo = {
        'conf': {
            'baseurl': {
                'ppc64le': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/ppc64le/',
                's390x': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/s390x/',
                'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
            },
        },
        'content_set': {
            'optional': True,
        },
    }
    arches = ['x86_64', 'ppc64le', 's390x']
    test_repo = 'rhaos-4.4-rhel-8-build'

    def setUp(self):
        self.repo = Repo(self.test_repo, self.repo_config, self.arches)
        self.repo_extras = Repo(self.test_repo, self.repo_config_extras, self.arches)

    def testRepoEnabled(self):
        """see if this repo correctly reports being enabled"""
        self.assertTrue(self.repo.enabled)

    def test_conf_section_basic(self):
        """ensure we can print a correct expected repo string"""
        conf_str = self.repo.conf_section('signed')
        # The two basic repo files are the same
        self.assertEqual(EXPECTED_BASIC_REPO, conf_str)
        conf_str = self.repo.conf_section('unsigned')
        # The two basic repo files are the same
        self.assertEqual(EXPECTED_BASIC_UNSIGNED_REPO, conf_str)

    def test_conf_section_extra_options(self):
        """ensure we can print a correct expected repo string with extra options"""
        conf_str_extra = self.repo_extras.conf_section('signed')
        # The repo with an 'extra_options' section is correct
        self.assertEqual(EXPECTED_EXTRA_OPTIONS_REPO, conf_str_extra)

    def test_content_set(self):
        """ensure content sets can be correctly selected"""
        self.assertEqual("rhel-7-server-ose-4.2-rpms", self.repo.content_set('x86_64'))

        with self.assertRaises(ValueError):
            # Will not be a valid content set
            self.repo.content_set('redhat')

        # Add a fake content set to the 'invalid sets' list
        self.repo.set_invalid_cs_arch('hatred')
        # Now ensure there is an error when we try to use it
        with self.assertRaises(ValueError):
            self.repo.content_set('hatred')

        # Manually indicate that a previously valid content set arch
        # is no longer valid
        self.repo.set_invalid_cs_arch('x86_64')
        self.assertIsNone(self.repo.content_set('x86_64'))

    def test_arches(self):
        """ensure we can get the arches we configured the repo with"""
        self.assertListEqual(self.repo.arches, self.arches)

    def test_init_validation(self):
        """ensure the init method checks for incorrectly configured repos"""

        with self.assertRaises(ValueError):
            Repo('no-conf', self.no_baseurl_repo, self.arches)

        with self.assertRaises(ValueError):
            Repo('no-content-sets', self.no_config_sets_repo, self.arches)

        with self.assertRaises(ValueError):
            Repo('no-base-urls', self.enabled_no_baseurl_repo, self.arches)

        # Implicitly assert that this does _not_ raise an exception
        Repo('no-config-set-arches', self.no_config_set_arches_repo, self.arches)

    async def test_get_repodata_includepkgs(self):
        """ensure includepkgs are correctly filtered"""

        repo_data = {
            'conf': {
                'extra_options': {
                    'exclude': '*debuginfo*',
                    'includepkgs': 'kernel* kernel-debuginfo*',
                },
                'baseurl': {
                    'x86_64': 'http://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.4-rhel-8-build/latest/x86_64/',
                },
            },
            'content_set': {
                'optional': True,
            },
        }

        repo = Repo('kernel-repo', repo_data, ['x86_64'])
        self.assertEqual(repo.includepkgs, ['kernel*', 'kernel-debuginfo*'])
        self.assertEqual(repo.excludepkgs, ['*debuginfo*'])

        # name is a special property of Mock, so it cannot be set in init e.g Mock(name='kernel')
        primary_rpms = []
        for pkg_name in ['kernel-devel', 'kernel', 'foo-kernel', 'bar', 'kernel-debuginfo', 'foo-debuginfo']:
            pkg = Mock()
            pkg.name = pkg_name
            primary_rpms.append(pkg)
        mock_repo = Mock(primary_rpms=primary_rpms)

        with patch('doozerlib.repos.RepodataLoader.load', return_value=mock_repo):
            expected = {'kernel-devel', 'kernel'}
            repodata = await repo.get_repodata('x86_64')
            actual = {r.name for r in repodata.primary_rpms}
            self.assertEqual(expected, actual)

    def test_init_with_new_style_config(self):
        """Test that Repo.__init__ accepts new-style RepoConf (Pydantic model)"""

        # Create a new-style RepoConf object (Pydantic model)
        new_style_config = RepoConf(
            name='test-new-style-repo',
            type='external',
            disabled=False,
            conf={
                'enabled': 1,
                'baseurl': {
                    'x86_64': 'http://example.com/repo/x86_64/',
                    'ppc64le': 'http://example.com/repo/ppc64le/',
                    's390x': 'http://example.com/repo/s390x/',
                },
            },
            content_set=ContentSet(
                default='test-content-set',
                x86_64='test-content-set-x86_64',
                ppc64le='test-content-set-ppc64le',
                s390x='test-content-set-s390x',
                optional=False,
            ),
            reposync=RepoSync(enabled=True, latest_only=True),
        )

        # Test that Repo can be instantiated with new-style config via from_repo_config
        repo = Repo.from_repo_config(new_style_config, self.arches)

        # Verify the repo was initialized correctly
        self.assertEqual(repo.name, 'test-new-style-repo')
        self.assertTrue(repo.enabled)
        self.assertTrue(repo.is_reposync_enabled())
        self.assertTrue(repo.is_reposync_latest_only())

        # Verify baseurl works
        self.assertEqual(repo.baseurl('unsigned', 'x86_64'), 'http://example.com/repo/x86_64/')

        # Verify content_set works
        self.assertEqual(repo.content_set('x86_64'), 'test-content-set-x86_64')
        self.assertEqual(repo.content_set('ppc64le'), 'test-content-set-ppc64le')

        # Verify arches
        self.assertListEqual(repo.arches, self.arches)

    def test_init_with_old_and_new_style_configs_equivalent(self):
        """Test that old-style dict and new-style RepoConf produce equivalent Repo objects"""

        # Old-style config (dict)
        old_style_config = {
            'conf': {
                'enabled': 1,
                'baseurl': {
                    'x86_64': 'http://example.com/repo/x86_64/',
                    'ppc64le': 'http://example.com/repo/ppc64le/',
                },
            },
            'content_set': {
                'default': 'test-content-set',
                'x86_64': 'test-content-set-x86_64',
                'ppc64le': 'test-content-set-ppc64le',
                'optional': False,
            },
            'reposync': {
                'enabled': True,
                'latest_only': False,
            },
        }

        # New-style config (Pydantic model)
        new_style_config = RepoConf(
            name='test-repo',
            type='external',
            disabled=False,
            conf={
                'enabled': 1,
                'baseurl': {
                    'x86_64': 'http://example.com/repo/x86_64/',
                    'ppc64le': 'http://example.com/repo/ppc64le/',
                },
            },
            content_set=ContentSet(
                default='test-content-set',
                x86_64='test-content-set-x86_64',
                ppc64le='test-content-set-ppc64le',
                optional=False,
            ),
            reposync=RepoSync(enabled=True, latest_only=False),
        )

        # Create repos with both configs
        repo_old = Repo('test-repo', old_style_config, ['x86_64', 'ppc64le'])
        repo_new = Repo.from_repo_config(new_style_config, ['x86_64', 'ppc64le'])

        # Verify they produce equivalent results
        self.assertEqual(repo_old.enabled, repo_new.enabled)
        self.assertEqual(repo_old.is_reposync_enabled(), repo_new.is_reposync_enabled())
        self.assertEqual(repo_old.is_reposync_latest_only(), repo_new.is_reposync_latest_only())
        self.assertEqual(repo_old.baseurl('unsigned', 'x86_64'), repo_new.baseurl('unsigned', 'x86_64'))
        self.assertEqual(repo_old.content_set('x86_64'), repo_new.content_set('x86_64'))
        self.assertListEqual(repo_old.arches, repo_new.arches)

    def test_plashet_repo_with_global_config(self):
        """Test that plashet type repos are constructed correctly with global plashet config"""

        # Create global plashet config (with concrete values, as {MAJOR}.{MINOR} would be replaced during config loading)
        plashet_config = PlashetConfig(
            base_dir="4.20/$runtime_assembly/$slug",
            plashet_dir="$yyyy-$MM/$revision",
            create_symlinks=True,
            symlink_name="latest",
            create_repo_subdirs=True,
            repo_subdir="os",
            arches=["x86_64", "s390x", "ppc64le", "aarch64"],
            download_url="https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.20/$runtime_assembly/$slug/latest/$arch/os/",
        )

        # Create plashet type repo config without baseurl
        plashet_repo_config = RepoConf(
            name='test-plashet-repo',
            type='plashet',
            disabled=False,
            plashet=PlashetRepo(
                disabled=False,
                slug='el9-embargoed',
                assembly_aware=False,
                embargo_aware=False,
                include_embargoed=False,
                source={'type': 'brew', 'from_tags': []},
            ),
            content_set=ContentSet(
                default='test-plashet-content-set',
                optional=False,
            ),
        )

        # Create template variables
        template_vars = {
            'runtime_assembly': 'stream',
        }

        # Create repo using from_repo_config
        repo = Repo.from_repo_config(
            plashet_repo_config, ['x86_64', 'ppc64le'], plashet_config=plashet_config, template_vars=template_vars
        )

        # Verify the repo was created correctly
        self.assertEqual(repo.name, 'test-plashet-repo')
        self.assertTrue(repo.enabled)

        # Verify baseurl was constructed for each architecture
        expected_x86_64_url = "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.20/stream/el9-embargoed/latest/x86_64/os/"
        expected_ppc64le_url = "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.20/stream/el9-embargoed/latest/ppc64le/os/"

        self.assertEqual(repo.baseurl('unsigned', 'x86_64'), expected_x86_64_url)
        self.assertEqual(repo.baseurl('unsigned', 'ppc64le'), expected_ppc64le_url)

        # Verify content_set works
        self.assertEqual(repo.content_set('x86_64'), 'test-plashet-content-set')

    def test_plashet_repo_without_config_raises_error(self):
        """Test that creating a plashet repo without plashet_config raises an error"""

        plashet_repo_config = RepoConf(
            name='test-plashet-repo',
            type='plashet',
            disabled=False,
            plashet=PlashetRepo(
                disabled=False,
                slug='el9-embargoed',
                source={'type': 'brew', 'from_tags': []},
            ),
            content_set=ContentSet(default='test-content-set'),
        )

        # Should raise ValueError when plashet_config is not provided
        with self.assertRaises(ValueError) as context:
            Repo.from_repo_config(plashet_repo_config, ['x86_64'])

        self.assertIn("plashet", str(context.exception).lower())
        self.assertIn("plashet_config", str(context.exception).lower())

    def test_plashet_repo_with_custom_baseurl(self):
        """Test that plashet repos can have custom baseurl that overrides template"""

        plashet_config = PlashetConfig(
            base_dir="4.20/$runtime_assembly/$slug",
            plashet_dir="$yyyy-$MM/$revision",
            download_url="https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets/4.20/$runtime_assembly/$slug/latest/$arch/os/",
        )

        # Create plashet repo with custom baseurl
        plashet_repo_config = RepoConf(
            name='test-plashet-custom',
            type='plashet',
            disabled=False,
            conf={
                'enabled': 1,
                'baseurl': 'https://custom.example.com/repo/',
            },
            plashet=PlashetRepo(
                disabled=False,
                slug='el9',
                source={'type': 'brew', 'from_tags': []},
            ),
            content_set=ContentSet(default='test-content-set'),
        )

        repo = Repo.from_repo_config(plashet_repo_config, ['x86_64'], plashet_config=plashet_config)

        # Custom baseurl should be used instead of generated one
        self.assertEqual(repo.baseurl('unsigned', 'x86_64'), 'https://custom.example.com/repo/')
