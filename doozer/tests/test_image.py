import logging
import os
import shutil
import tempfile
import unittest
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib import exectools
from artcommonlib.model import Missing, Model
from doozerlib.image import ImageMetadata
from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repos
from flexmock import flexmock

try:
    from importlib import reload
except ImportError:
    pass
from doozerlib import build_info, image

TEST_YAML = """---
name: 'openshift/test'
distgit:
  namespace: 'hello'"""

# base only images have an additional flag
TEST_BASE_YAML = """---
name: 'openshift/test_base'
base_only: true
distgit:
  namespace: 'hello'"""


class MockRuntime(object):
    def __init__(self, logger):
        self.logger = logger


class TestImageMetadata(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ocp-cd-test-logs")

        self.test_file = os.path.join(self.test_dir, "test_file")
        self.logger = logging.getLogger()

        self.cwd = os.getcwd()
        os.chdir(self.test_dir)

        test_yml = open('test.yml', 'w')
        test_yml.write(TEST_YAML)
        test_yml.close()

    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.test_dir)

    @unittest.skip("assertion failing, check if desired behavior changed")
    def test_init(self):
        """
        The metadata object appears to need to be created while CWD is
        in the root of a git repo containing a file called '<name>.yml'
        This file must contain a structure:
           {'distgit': {'namespace': '<value>'}}

        The metadata object requires:
          a type string <image|rpm>
          a Runtime object placeholder

        """

        #
        # Check the logs
        #
        logs = [log.rstrip() for log in open(self.test_file).readlines()]

        expected = 1
        actual = len(logs)
        self.assertEqual(expected, actual, "logging lines - expected: {}, actual: {}".format(expected, actual))

    @unittest.skip("raising AttributeError: 'str' object has no attribute 'base_dir'")
    def test_base_only(self):
        """
        Some images are used only as a base for other images.  These base images
        are not included in a formal release.
        """

        test_base_yml = open('test_base.yml', 'w')
        test_base_yml.write(TEST_BASE_YAML)
        test_base_yml.close()

        rt = MockRuntime(self.logger)
        name = 'test.yml'
        name_base = 'test_base.yml'

        md = image.ImageMetadata(rt, name)
        md_base = image.ImageMetadata(rt, name_base)

        # Test the internal config value (will fail if implementation changes)
        # If the flag is absent, default to false
        self.assertFalse(md.config.base_only)
        self.assertTrue(md_base.config.base_only)

        # Test the base_only property of the ImageMetadata object
        self.assertFalse(md.base_only)
        self.assertTrue(md_base.base_only)

    @unittest.skip("AttributeError: 'str' object has no attribute 'filename'")
    def test_pull_url(self):
        fake_runtime = flexmock(
            get_latest_build_info=lambda: ('openshift-cli', '1.1.1', '8'),
            group_config=flexmock(
                urls=flexmock(
                    brew_image_namespace='rh-osbs',
                    brew_image_host='brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888',
                )
            ),
        )

        fake_image = flexmock(
            pull_url=image.ImageMetadata.pull_url(), runtime=fake_runtime, config=flexmock(name='test')
        )

        self.assertEqual(
            fake_image.pull_url(), "brew-pulp-docker01.web.prod.ext.phx2.redhat.com:8888/rh-osbs/openshift-test"
        )

    @unittest.skip("AttributeError: 'str' object has no attribute 'filename'")
    def test_get_latest_build_info(self):
        expected_cmd = ["brew", "latest-build", "rhaos-4.2-rhel-7-buildgo-toolset-1.10"]

        latest_build_output = """
        Build                                     Tag                   Built by
        ----------------------------------------  --------------------  ----------------
        go-toolset-1.10-1.10.3-7.el7              devtools-2018.4-rhel-7  deparker
        """

        (
            flexmock(exectools)
            .should_receive("cmd_gather")
            .with_args(expected_cmd)
            .once()
            .and_return((0, latest_build_output))
        )

        test_base_yml = open('test_pull.yml', 'w')
        test_base_yml.write(TEST_BASE_YAML)
        test_base_yml.close()
        rt = MockRuntime(self.logger)
        name = 'test_pull.yml'
        md = image.ImageMetadata(rt, name)
        n, v, r = md.get_latest_build_info()
        self.assertEqual(n, "go-toolset-1.10")
        self.assertEqual(v, "1.10.3")
        self.assertEqual(r, "7")

    def test_get_brew_image_name_short(self):
        image_model = Model(
            {
                'name': 'openshift/test',
            }
        )
        data_obj = Model(
            {
                'key': 'my-distgit',
                'data': image_model,
                'filename': 'my-distgit.yaml',
            }
        )
        rt = mock.MagicMock()
        imeta = image.ImageMetadata(rt, data_obj)
        self.assertEqual(imeta.get_brew_image_name_short(), 'openshift-test')

    def _create_image_metadata(self, name):
        image_model = Model(
            {
                'name': name,
            }
        )
        data_obj = Model(
            {
                'key': 'my-distgit',
                'data': image_model,
                'filename': 'my-distgit.yaml',
            }
        )
        rt = MagicMock()
        rt.logger = logging.getLogger('test_runtime')  # Use real logger
        return image.ImageMetadata(rt, data_obj)

    def test_cachi2_enabled_1(self):
        metadata = self._create_image_metadata('openshift/test_0')
        mock_config = MagicMock()
        mock_config.konflux.cachi2.enabled = True

        self.assertTrue(metadata.is_cachi2_enabled())

    def test_cachi2_enabled_2(self):
        metadata = self._create_image_metadata('openshift/test')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.enabled = False
        metadata.config = mock_config

        self.assertFalse(metadata.is_cachi2_enabled())

    def test_cachi2_enabled_3(self):
        metadata = self._create_image_metadata('openshift/test_1')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.enabled = None
        metadata.config = mock_config

        metadata.runtime.group_config.konflux.cachi2.enabled = True

        self.assertTrue(metadata.is_cachi2_enabled())

    def test_cachi2_enabled_4(self):
        metadata = self._create_image_metadata('openshift/test_2')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.enabled = Missing
        metadata.config = mock_config

        metadata.runtime.group_config.konflux.cachi2.enabled = False

        self.assertFalse(metadata.is_cachi2_enabled())

    @patch("artcommonlib.util.is_cachito_enabled")
    def test_cachi2_enabled_5(self, is_cachito_enabled):
        metadata = self._create_image_metadata('openshift/test_3')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.enabled = Missing
        metadata.config = mock_config

        metadata.runtime.group_config.konflux.cachi2.enabled = Missing
        is_cachito_enabled.return_value = True

        self.assertTrue(metadata.is_cachi2_enabled())

    @patch("artcommonlib.util.is_cachito_enabled")
    def test_cachi2_enabled_6(self, is_cachito_enabled):
        metadata = self._create_image_metadata('openshift/test_4')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.enabled = False
        metadata.config = mock_config

        metadata.runtime.group_config.konflux.cachi2.enabled = Missing
        is_cachito_enabled.return_value = True

        self.assertFalse(metadata.is_cachi2_enabled())

    def test_lockfile_enabled_metadata_override_true(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = True
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = False

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=True):
            result = metadata.is_lockfile_generation_enabled()
        self.assertTrue(result)
        self.logger.info.assert_any_call("Lockfile generation set from metadata config True")

    def test_lockfile_enabled_metadata_override_false(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = False
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = True

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=True):
            result = metadata.is_lockfile_generation_enabled()
        self.assertFalse(result)

    def test_lockfile_enabled_group_override_true(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = None
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = True

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=True):
            result = metadata.is_lockfile_generation_enabled()
        self.assertTrue(result)
        self.logger.info.assert_any_call("Lockfile generation set from group config True")

    def test_lockfile_enabled_group_override_false(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = None
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = False

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=True):
            result = metadata.is_lockfile_generation_enabled()
        self.assertFalse(result)

    def test_lockfile_enabled_missing_overrides(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = Missing
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = Missing

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=True):
            result = metadata.is_lockfile_generation_enabled()
        self.assertTrue(result)

    def test_lockfile_enabled_cachi2_disabled(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = True
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = True

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=False):
            result = metadata.is_lockfile_generation_enabled()
        self.assertFalse(result)

    def test_lockfile_enabled_all_missing(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.enabled = Missing
        metadata.config = mock_config
        metadata.logger = self.logger

        metadata.runtime.group_config.konflux.cachi2.lockfile.enabled = Missing

        with patch.object(ImageMetadata, "is_cachi2_enabled", return_value=Missing):
            result = metadata.is_lockfile_generation_enabled()
        self.assertFalse(result)

    def test_get_enabled_repos_with_repos(self):
        """Test get_enabled_repos returns repos that are both globally enabled and in image config"""
        metadata = self._create_image_metadata('openshift/test_repos')

        # Mock config to return repos from image config
        mock_config = MagicMock()
        mock_config.get.return_value = ['repo1', 'repo2', 'repo3']
        metadata.config = mock_config

        # Mock runtime.repos to have globally enabled repos
        mock_repo1 = MagicMock()
        mock_repo1.name = 'repo1'
        mock_repo1.enabled = True

        mock_repo2 = MagicMock()
        mock_repo2.name = 'repo2'
        mock_repo2.enabled = True

        mock_repo3 = MagicMock()
        mock_repo3.name = 'repo3'
        mock_repo3.enabled = True

        metadata.runtime.repos = {
            'repo1': mock_repo1,
            'repo2': mock_repo2,
            'repo3': mock_repo3,
        }

        result = metadata.get_enabled_repos()

        self.assertEqual(result, {'repo1', 'repo2', 'repo3'})
        mock_config.get.assert_called_once_with("enabled_repos", [])

    def test_get_enabled_repos_empty_config(self):
        """Test get_enabled_repos returns empty set when no repos configured"""
        metadata = self._create_image_metadata('openshift/test_repos_empty')

        mock_config = MagicMock()
        mock_config.get.return_value = []
        metadata.config = mock_config

        result = metadata.get_enabled_repos()

        self.assertEqual(result, set())
        mock_config.get.assert_called_once_with("enabled_repos", [])

    def test_get_enabled_repos_intersection_logic(self):
        """Test get_enabled_repos returns only repos enabled in BOTH group.yml AND image config"""
        metadata = self._create_image_metadata('openshift/test_repos_intersection')

        # Mock config to return repos from image config
        mock_config = MagicMock()
        mock_config.get.return_value = ['repo1', 'repo2', 'repo3']
        metadata.config = mock_config

        # Mock runtime.repos where only repo1 and repo2 are globally enabled
        mock_repo1 = MagicMock()
        mock_repo1.name = 'repo1'
        mock_repo1.enabled = True

        mock_repo2 = MagicMock()
        mock_repo2.name = 'repo2'
        mock_repo2.enabled = True

        mock_repo3 = MagicMock()
        mock_repo3.name = 'repo3'
        mock_repo3.enabled = False  # NOT globally enabled

        mock_repo4 = MagicMock()
        mock_repo4.name = 'repo4'
        mock_repo4.enabled = True  # Globally enabled but not in image config

        metadata.runtime.repos = {
            'repo1': mock_repo1,
            'repo2': mock_repo2,
            'repo3': mock_repo3,
            'repo4': mock_repo4,
        }

        result = metadata.get_enabled_repos()

        # Should only return repos enabled in BOTH places (repo1 and repo2)
        self.assertEqual(result, {'repo1', 'repo2'})
        mock_config.get.assert_called_once_with("enabled_repos", [])

    def test_calculate_config_digest_old_style_repos(self):
        """
        Test calculate_config_digest with old-style repos (dict format).
        """
        metadata = self._create_image_metadata('openshift/test_digest_old')

        # Mock image config with enabled_repos
        metadata.config = Model({'name': 'test-image', 'enabled_repos': ['repo1', 'repo2'], 'non_shipping_repos': []})

        # Old-style group_config with repos as dict
        group_config = Model(
            {
                'repos': {
                    'repo1': {'conf': {'baseurl': 'http://example.com/repo1'}, 'content_set': {'default': 'cs1'}},
                    'repo2': {'conf': {'baseurl': 'http://example.com/repo2'}, 'content_set': {'default': 'cs2'}},
                }
            }
        )

        streams = Model({})

        # Should not raise KeyError
        digest = metadata.calculate_config_digest(group_config, streams)

        # Verify digest is a valid sha256 hash
        self.assertTrue(digest.startswith('sha256:'))
        self.assertEqual(len(digest), 71)  # 'sha256:' + 64 hex chars

    def test_calculate_config_digest_new_style_repos(self):
        """
        Test calculate_config_digest with new-style repos (list format).
        """
        metadata = self._create_image_metadata('openshift/test_digest_new')

        # Mock image config with enabled_repos
        metadata.config = Model({'name': 'test-image', 'enabled_repos': ['repo1', 'repo2'], 'non_shipping_repos': []})

        # New-style group_config with all_repos as list
        group_config = Model(
            {
                'all_repos': [
                    {
                        'name': 'repo1',
                        'type': 'external',
                        'conf': {'baseurl': 'http://example.com/repo1'},
                        'content_set': {'default': 'cs1'},
                    },
                    {
                        'name': 'repo2',
                        'type': 'external',
                        'conf': {'baseurl': 'http://example.com/repo2'},
                        'content_set': {'default': 'cs2'},
                    },
                    {
                        'name': 'repo3',
                        'type': 'external',
                        'conf': {'baseurl': 'http://example.com/repo3'},
                        'content_set': {'default': 'cs3'},
                    },
                ]
            }
        )

        streams = Model({})

        # Should not raise KeyError
        digest = metadata.calculate_config_digest(group_config, streams)

        # Verify digest is a valid sha256 hash
        self.assertTrue(digest.startswith('sha256:'))
        self.assertEqual(len(digest), 71)  # 'sha256:' + 64 hex chars

    def test_calculate_config_digest_no_repos(self):
        """
        Test calculate_config_digest with no repos enabled.
        """
        metadata = self._create_image_metadata('openshift/test_digest_no_repos')

        # Mock image config with no enabled_repos
        metadata.config = Model({'name': 'test-image', 'enabled_repos': [], 'non_shipping_repos': []})

        # Group config doesn't matter when no repos are enabled
        group_config = Model({})
        streams = Model({})

        # Should not raise any errors
        digest = metadata.calculate_config_digest(group_config, streams)

        # Verify digest is a valid sha256 hash
        self.assertTrue(digest.startswith('sha256:'))
        self.assertEqual(len(digest), 71)  # 'sha256:' + 64 hex chars

    @patch('doozerlib.image.SourceResolver')
    @patch('builtins.open', create=True)
    @patch('pathlib.Path.joinpath')
    def test_apply_alternative_upstream_config_matching_rhel_version(
        self, mock_joinpath, mock_open, mock_source_resolver
    ):
        """Test alternative config is merged when upstream RHEL version matches"""
        metadata = self._create_image_metadata('openshift/test_alt_config')

        # Mock config with alternative_upstream
        metadata.config = Model(
            {
                'name': 'openshift/test_alt_config',
                'distgit': {'branch': 'rhaos-4.16-rhel-9'},
                'content': {'source': {'git': {'url': 'https://github.com/test/repo.git'}}},
                'alternative_upstream': [{'when': 'el8', 'distgit': {'branch': 'rhaos-4.16-rhel-8'}}],
            }
        )

        # Mock branch_el_target to return 9 (ART intended RHEL version)
        metadata.branch_el_target = MagicMock(return_value=9)

        # Mock source resolution
        mock_source_resolution = MagicMock()
        metadata.runtime.source_resolver.resolve_source = MagicMock(return_value=mock_source_resolution)

        # Mock get_source_dir to return a path
        mock_source_dir = MagicMock()
        mock_source_resolver.get_source_dir = MagicMock(return_value=mock_source_dir)

        # Mock Dockerfile path
        mock_df_path = MagicMock()
        mock_joinpath.return_value = mock_df_path

        # Mock Dockerfile content with rhel-8 parent image
        dockerfile_content = """FROM registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.22-openshift-4.18
RUN echo "test"
"""
        mock_open.return_value.__enter__.return_value = mock.mock_open(read_data=dockerfile_content).return_value

        # Mock DockerfileParser
        with patch('doozerlib.image.DockerfileParser') as mock_dfp:
            mock_dfp_instance = MagicMock()
            mock_dfp_instance.parent_images = [
                'registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.22-openshift-4.18'
            ]
            mock_dfp.return_value = mock_dfp_instance

            # Mock determine_targets
            metadata.determine_targets = MagicMock(return_value=['target-1', 'target-2'])

            # Call the method
            metadata._apply_alternative_upstream_config()

        # Verify config was merged (branch should be updated to rhel-8)
        self.assertEqual(metadata.config['distgit']['branch'], 'rhaos-4.16-rhel-8')
        # Verify targets were updated
        metadata.determine_targets.assert_called_once()
        self.assertEqual(metadata.targets, ['target-1', 'target-2'])

    @patch('doozerlib.image.SourceResolver')
    @patch('builtins.open', create=True)
    @patch('pathlib.Path.joinpath')
    def test_apply_alternative_upstream_config_no_match(self, mock_joinpath, mock_open, mock_source_resolver):
        """Test that IOError is raised when upstream RHEL version doesn't match ART nor any alternative_upstream"""
        metadata = self._create_image_metadata('openshift/test_alt_config_no_match')

        # Mock config with alternative_upstream for el7 (won't match upstream's el8)
        metadata.config = Model(
            {
                'name': 'openshift/test_alt_config_no_match',
                'distgit': {'branch': 'rhaos-4.16-rhel-9'},
                'content': {'source': {'git': {'url': 'https://github.com/test/repo.git'}}},
                'alternative_upstream': [{'when': 'el7', 'distgit': {'branch': 'rhaos-4.16-rhel-7'}}],
            }
        )

        # Mock branch_el_target to return 9 (ART version)
        metadata.branch_el_target = MagicMock(return_value=9)

        # Mock source resolution
        mock_source_resolution = MagicMock()
        metadata.runtime.source_resolver.resolve_source = MagicMock(return_value=mock_source_resolution)

        # Mock get_source_dir
        mock_source_dir = MagicMock()
        mock_source_resolver.get_source_dir = MagicMock(return_value=mock_source_dir)

        # Mock Dockerfile path
        mock_df_path = MagicMock()
        mock_joinpath.return_value = mock_df_path

        # Mock Dockerfile with rhel-8 (upstream=8, ART=9, alternative_upstream=el7 - no match!)
        dockerfile_content = """FROM registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.22-openshift-4.18
RUN echo "test"
"""
        mock_open.return_value.__enter__.return_value = mock.mock_open(read_data=dockerfile_content).return_value

        # Mock DockerfileParser
        with patch('doozerlib.image.DockerfileParser') as mock_dfp:
            mock_dfp_instance = MagicMock()
            mock_dfp_instance.parent_images = [
                'registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.22-openshift-4.18'
            ]
            mock_dfp.return_value = mock_dfp_instance

            # Should raise IOError since upstream=8, ART=9, but only el7 alternative exists
            with self.assertRaises(IOError) as context:
                metadata._apply_alternative_upstream_config()

        # Verify error message mentions the mismatch
        self.assertIn('Upstream uses el8 but ART uses el9', str(context.exception))
        self.assertIn('no matching alternative_upstream', str(context.exception))

    def test_apply_alternative_upstream_config_no_source(self):
        """Test that IOError is raised when image has no source"""
        metadata = self._create_image_metadata('openshift/test_no_source')

        # Mock config without source
        metadata.config = Model({'name': 'openshift/test_no_source', 'distgit': {'branch': 'rhaos-4.16-rhel-9'}})

        # Mock has_source to return False
        metadata.has_source = MagicMock(return_value=False)

        # Should raise IOError
        with self.assertRaises(IOError) as context:
            metadata._apply_alternative_upstream_config()

        self.assertIn('does not have upstream source', str(context.exception))

    @patch('doozerlib.image.SourceResolver')
    @patch('builtins.open', create=True)
    @patch('pathlib.Path.joinpath')
    def test_apply_alternative_upstream_config_no_alternative_config(
        self, mock_joinpath, mock_open, mock_source_resolver
    ):
        """Test when upstream and ART versions match - no alternative config needed"""
        metadata = self._create_image_metadata('openshift/test_no_alt')

        original_branch = 'rhaos-4.16-rhel-9'
        # Mock config WITHOUT alternative_upstream
        metadata.config = Model(
            {
                'name': 'openshift/test_no_alt',
                'distgit': {'branch': original_branch},
                'content': {'source': {'git': {'url': 'https://github.com/test/repo.git'}}},
            }
        )

        # Mock branch_el_target to return 9
        metadata.branch_el_target = MagicMock(return_value=9)

        # Mock source resolution
        mock_source_resolution = MagicMock()
        metadata.runtime.source_resolver.resolve_source = MagicMock(return_value=mock_source_resolution)

        # Mock get_source_dir
        mock_source_dir = MagicMock()
        mock_source_resolver.get_source_dir = MagicMock(return_value=mock_source_dir)

        # Mock Dockerfile path
        mock_df_path = MagicMock()
        mock_joinpath.return_value = mock_df_path

        # Mock Dockerfile with rhel-9 (matches ART version, no merge needed)
        dockerfile_content = """FROM registry.ci.openshift.org/ocp/builder:rhel-9-golang-1.22-openshift-4.18
RUN echo "test"
"""
        mock_open.return_value.__enter__.return_value = mock.mock_open(read_data=dockerfile_content).return_value

        # Mock DockerfileParser
        with patch('doozerlib.image.DockerfileParser') as mock_dfp:
            mock_dfp_instance = MagicMock()
            mock_dfp_instance.parent_images = [
                'registry.ci.openshift.org/ocp/builder:rhel-9-golang-1.22-openshift-4.18'
            ]
            mock_dfp.return_value = mock_dfp_instance

            # Mock determine_targets
            metadata.determine_targets = MagicMock(return_value=['target-1'])

            # Call the method - should not fail since versions match
            metadata._apply_alternative_upstream_config()

        # Verify config was NOT changed (upstream=9 matches ART=9, no merge needed)
        self.assertEqual(metadata.config['distgit']['branch'], original_branch)
        # Targets should not be updated when versions match
        metadata.determine_targets.assert_not_called()

    @patch('doozerlib.image.SourceResolver')
    @patch('builtins.open', create=True)
    @patch('pathlib.Path.joinpath')
    def test_determine_upstream_rhel_version_ubi_pattern(self, mock_joinpath, mock_open, mock_source_resolver):
        """Test RHEL version detection from ubi-based images"""
        metadata = self._create_image_metadata('openshift/test_ubi')

        # Mock config
        metadata.config = Model(
            {
                'name': 'openshift/test_ubi',
                'distgit': {'branch': 'rhaos-4.16-rhel-9'},
                'content': {'source': {'git': {'url': 'https://github.com/test/repo.git'}}},
                'alternative_upstream': [{'when': 'el8', 'distgit': {'branch': 'rhaos-4.16-rhel-8'}}],
            }
        )

        # Mock branch_el_target
        metadata.branch_el_target = MagicMock(return_value=9)

        # Mock source resolution
        mock_source_resolution = MagicMock()
        metadata.runtime.source_resolver.resolve_source = MagicMock(return_value=mock_source_resolution)

        # Mock get_source_dir
        mock_source_dir = MagicMock()
        mock_source_resolver.get_source_dir = MagicMock(return_value=mock_source_dir)

        # Mock Dockerfile path
        mock_df_path = MagicMock()
        mock_joinpath.return_value = mock_df_path

        # Mock Dockerfile with ubi8 pattern in tag (no dash)
        dockerfile_content = """FROM registry.access.redhat.com/ubi/minimal:ubi8
RUN echo "test"
"""
        mock_open.return_value.__enter__.return_value = mock.mock_open(read_data=dockerfile_content).return_value

        # Mock DockerfileParser
        with patch('doozerlib.image.DockerfileParser') as mock_dfp:
            mock_dfp_instance = MagicMock()
            mock_dfp_instance.parent_images = ['registry.access.redhat.com/ubi/minimal:ubi8']
            mock_dfp.return_value = mock_dfp_instance

            # Mock determine_targets
            metadata.determine_targets = MagicMock(return_value=['target-1', 'target-2'])

            # Call the method
            metadata._apply_alternative_upstream_config()

        # Verify config was merged (should detect el8 from ubi8 tag)
        self.assertEqual(metadata.config['distgit']['branch'], 'rhaos-4.16-rhel-8')


class TestImageInspector(IsolatedAsyncioTestCase):
    @mock.patch("doozerlib.repos.Repo.get_repodata_threadsafe")
    @mock.patch("doozerlib.build_info.BrewImageInspector.get_installed_rpm_dicts")
    @mock.patch("doozerlib.build_info.BrewImageInspector.image_arch")
    @mock.patch("doozerlib.build_info.BrewImageInspector.get_image_meta")
    @mock.patch("doozerlib.build_info.BrewImageInspector.get_build_id")
    async def test_find_non_latest_rpms(
        self,
        get_build_id: mock.Mock,
        get_image_meta: mock.Mock,
        image_arch: mock.Mock,
        get_installed_rpm_dicts: mock.Mock,
        get_repodata_threadsafe: mock.AsyncMock,
    ):
        runtime = mock.MagicMock(
            repos=Repos(
                {
                    "rhel-8-baseos-rpms": {
                        "conf": {"baseurl": {"x86_64": "fake_url"}},
                        "content_set": {"default": "fake"},
                    },
                    "rhel-8-appstream-rpms": {
                        "conf": {"baseurl": {"x86_64": "fake_url"}},
                        "content_set": {"default": "fake"},
                    },
                    "rhel-8-rt-rpms": {"conf": {"baseurl": {"x86_64": "fake_url"}}, "content_set": {"default": "fake"}},
                },
                ["x86_64", "s390x", "ppc64le", "aarch64"],
            )
        )
        archive = mock.MagicMock()
        brew_build_inspector = mock.MagicMock(autospec=build_info.BrewBuildRecordInspector)
        get_build_id.return_value = 12345
        brew_build_inspector.get_build_id.return_value = 12345
        mock_meta = mock.MagicMock(
            autospec=image.ImageMetadata,
            config={
                "enabled_repos": ["rhel-8-baseos-rpms", "rhel-8-appstream-rpms"],
            },
        )
        mock_meta.get_enabled_repos.return_value = {"rhel-8-baseos-rpms", "rhel-8-appstream-rpms"}
        mock_meta.runtime = runtime
        get_image_meta.return_value = mock_meta
        image_arch.return_value = "x86_64"
        get_repodata_threadsafe.return_value = Repodata(
            name='rhel-8-appstream-rpms',
            primary_rpms=[
                Rpm.from_dict(
                    {
                        'name': 'foo',
                        'version': '1.0.0',
                        'release': '1.el9',
                        'epoch': '0',
                        'arch': 'x86_64',
                        'nvr': 'foo-1.0.0-1.el9',
                    }
                ),
                Rpm.from_dict(
                    {
                        'name': 'bar',
                        'version': '1.1.0',
                        'release': '1.el9',
                        'epoch': '0',
                        'arch': 'x86_64',
                        'nvr': 'bar-1.1.0-1.el9',
                    }
                ),
            ],
        )
        get_installed_rpm_dicts.return_value = [
            {
                'name': 'foo',
                'version': '1.0.0',
                'release': '1.el9',
                'epoch': '0',
                'arch': 'x86_64',
                'nvr': 'foo-1.0.0-1.el9',
            },
            {
                'name': 'bar',
                'version': '1.0.0',
                'release': '1.el9',
                'epoch': '0',
                'arch': 'x86_64',
                'nvr': 'bar-1.0.0-1.el9',
            },
        ]
        inspector = build_info.BrewImageInspector(runtime, archive, brew_build_inspector)
        actual = await inspector.find_non_latest_rpms()
        get_image_meta.assert_called_once_with()
        get_installed_rpm_dicts.assert_called_once_with()
        get_repodata_threadsafe.assert_awaited()
        self.assertEqual(actual, [('bar-0:1.0.0-1.el9.x86_64', 'bar-0:1.1.0-1.el9.x86_64', 'rhel-8-appstream-rpms')])


class TestImageMetadataAsyncMethods(IsolatedAsyncioTestCase):
    """Test class for async methods in ImageMetadata"""

    def setUp(self):
        self.logger = MagicMock()

    def _create_image_metadata(self, name, distgit_key=None):
        """Helper to create ImageMetadata with mock runtime"""
        image_model = Model({'name': name})
        data_obj = Model(
            {
                'key': distgit_key or 'test-image',
                'data': image_model,
                'filename': 'test-image.yaml',
            }
        )

        rt = MagicMock()
        rt.group = 'test-group'
        rt.build_system = 'konflux'
        rt.konflux_db = MagicMock()

        metadata = image.ImageMetadata(rt, data_obj)
        metadata.logger = self.logger
        return metadata

    async def test_fetch_rpms_from_build_cached_packages(self):
        """Test fetch_rpms_from_build returns cached packages"""
        metadata = self._create_image_metadata('openshift/test-cached')
        metadata.installed_rpms = ['pkg1', 'pkg2', 'pkg3']

        result = await metadata.fetch_rpms_from_build()

        self.assertEqual(result, {'pkg1', 'pkg2', 'pkg3'})
        # Should not call konflux_db when cached
        metadata.runtime.konflux_db.get_latest_build.assert_not_called()
        self.logger.debug.assert_called_with("Using cached installed_rpms for test-image: 3 RPMs")
        self.logger.error.assert_not_called()
        self.logger.warning.assert_not_called()

    async def test_fetch_rpms_from_build_cached_empty_packages(self):
        """Test fetch_rpms_from_build returns cached empty packages"""
        metadata = self._create_image_metadata('openshift/test-cached-empty')
        metadata.installed_rpms = []

        result = await metadata.fetch_rpms_from_build()

        self.assertEqual(result, set())
        metadata.runtime.konflux_db.get_latest_build.assert_not_called()
        self.logger.debug.assert_called_with("Using cached installed_rpms for test-image: 0 RPMs")
        self.logger.error.assert_not_called()
        self.logger.warning.assert_not_called()

    async def test_fetch_rpms_from_build_no_build_found(self):
        """Test fetch_rpms_from_build when no build is found"""
        metadata = self._create_image_metadata('openshift/test-no-build')

        metadata.runtime.konflux_db.get_latest_build = AsyncMock(return_value=None)

        result = await metadata.fetch_rpms_from_build()

        self.assertEqual(result, set())
        self.assertEqual(metadata.installed_rpms, [])
        self.logger.error.assert_not_called()

    async def test_fetch_rpms_from_build_build_no_packages(self):
        """Test fetch_rpms_from_build when build has no installed_rpms"""
        metadata = self._create_image_metadata('openshift/test-no-packages')

        mock_build = MagicMock()
        mock_build.installed_rpms = None

        metadata.runtime.konflux_db.get_latest_build = AsyncMock(return_value=mock_build)

        result = await metadata.fetch_rpms_from_build()

        self.assertEqual(result, set())
        self.assertEqual(metadata.installed_rpms, [])
        self.logger.error.assert_not_called()

    async def test_fetch_rpms_from_build_no_parent_full_package_set(self):
        """Test fetch_rpms_from_build when no parent found, uses full package set"""
        metadata = self._create_image_metadata('openshift/test-no-parent')

        # Ensure no cached packages
        metadata.installed_rpms = None

        mock_build = MagicMock()
        mock_build.installed_rpms = ['pkg1', 'pkg2', 'pkg3']

        metadata.runtime.konflux_db.get_latest_build = AsyncMock(return_value=mock_build)

        # Mock no parent members
        with patch.object(metadata, 'get_parent_members', return_value={}):
            result = await metadata.fetch_rpms_from_build()

        self.assertEqual(result, {'pkg1', 'pkg2', 'pkg3'})
        self.assertEqual(set(metadata.installed_rpms), {'pkg1', 'pkg2', 'pkg3'})

    async def test_fetch_rpms_from_build_exception_handling(self):
        """Test fetch_rpms_from_build handles exceptions gracefully"""
        metadata = self._create_image_metadata('openshift/test-exception')

        metadata.runtime.konflux_db.get_latest_build = AsyncMock(side_effect=Exception("Database error"))

        result = await metadata.fetch_rpms_from_build()

        self.assertEqual(result, set())
        self.assertEqual(metadata.installed_rpms, [])

    def _setup_mock_config(self, metadata, lockfile_rpms=None):
        """Helper to setup mock config with lockfile RPMs."""
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.get.return_value = lockfile_rpms or []
        metadata.config = mock_config
        return mock_config

    async def test_get_lockfile_rpms_union_of_build_and_config(self):
        """Test that method returns union of RPMs from build and config."""
        metadata = self._create_image_metadata('openshift/test-image')
        self._setup_mock_config(metadata, lockfile_rpms=['config-rpm-1', 'config-rpm-2'])

        # Mock fetch_rpms_from_build to return different RPMs
        metadata.fetch_rpms_from_build = AsyncMock(return_value={'build-rpm-1', 'build-rpm-2'})
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        expected = {'build-rpm-1', 'build-rpm-2', 'config-rpm-1', 'config-rpm-2'}
        self.assertEqual(result, expected)
        metadata.logger.info.assert_called_once_with('test-image adding 2 RPMs from lockfile config')

    async def test_get_lockfile_rpms_build_only(self):
        """Test when only build has RPMs (empty config)."""
        metadata = self._create_image_metadata('openshift/test-image')
        self._setup_mock_config(metadata, lockfile_rpms=[])

        metadata.fetch_rpms_from_build = AsyncMock(return_value={'build-rpm-1', 'build-rpm-2'})
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        expected = {'build-rpm-1', 'build-rpm-2'}
        self.assertEqual(result, expected)
        # Should not log when config is empty
        metadata.logger.info.assert_not_called()

    async def test_get_lockfile_rpms_config_only(self):
        """Test when only config has RPMs (empty build)."""
        metadata = self._create_image_metadata('openshift/test-image')
        self._setup_mock_config(metadata, lockfile_rpms=['config-rpm-1', 'config-rpm-2'])

        metadata.fetch_rpms_from_build = AsyncMock(return_value=set())
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        expected = {'config-rpm-1', 'config-rpm-2'}
        self.assertEqual(result, expected)
        metadata.logger.info.assert_called_once_with('test-image adding 2 RPMs from lockfile config')

    async def test_get_lockfile_rpms_empty_both_sources(self):
        """Test when both build and config are empty."""
        metadata = self._create_image_metadata('openshift/test-image')
        self._setup_mock_config(metadata, lockfile_rpms=[])

        metadata.fetch_rpms_from_build = AsyncMock(return_value=set())
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        self.assertEqual(result, set())
        metadata.logger.info.assert_not_called()

    async def test_get_lockfile_rpms_missing_config_field(self):
        """Test when config field is Missing."""
        metadata = self._create_image_metadata('openshift/test-image')
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.get.return_value = Missing
        metadata.config = mock_config

        metadata.fetch_rpms_from_build = AsyncMock(return_value={'build-rpm-1'})
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        expected = {'build-rpm-1'}
        self.assertEqual(result, expected)
        metadata.logger.info.assert_not_called()

    async def test_get_lockfile_rpms_none_config_field(self):
        """Test when config field is None."""
        metadata = self._create_image_metadata('openshift/test-image')
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.get.return_value = None
        metadata.config = mock_config

        metadata.fetch_rpms_from_build = AsyncMock(return_value={'build-rpm-1'})
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        expected = {'build-rpm-1'}
        self.assertEqual(result, expected)
        metadata.logger.info.assert_not_called()

    async def test_get_lockfile_rpms_duplicate_handling(self):
        """Test that duplicates are handled correctly (set union deduplication)."""
        metadata = self._create_image_metadata('openshift/test-image')
        self._setup_mock_config(metadata, lockfile_rpms=['rpm-1', 'rpm-2', 'rpm-3'])

        # Same RPM in both sources
        metadata.fetch_rpms_from_build = AsyncMock(return_value={'rpm-1', 'rpm-2', 'rpm-4'})
        metadata.logger = MagicMock()

        result = await metadata.get_lockfile_rpms_to_install()

        # Should deduplicate automatically (set union)
        expected = {'rpm-1', 'rpm-2', 'rpm-3', 'rpm-4'}
        self.assertEqual(result, expected)
        metadata.logger.info.assert_called_once_with('test-image adding 3 RPMs from lockfile config')

    def test_is_dnf_modules_enable_enabled_default(self):
        """Test dnf_modules_enable defaults to True when no configuration is set"""
        metadata = self._create_image_metadata('openshift/test-dnf-modules-enable')

        # Mock both configs as Missing
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.dnf_modules_enable = Missing
        metadata.config = mock_config

        mock_group_config = MagicMock()
        mock_group_config.konflux.cachi2.lockfile.dnf_modules_enable = Missing
        metadata.runtime.group_config = mock_group_config
        metadata.logger = MagicMock()

        result = metadata.is_dnf_modules_enable_enabled()

        self.assertTrue(result)
        # Should not log when using default
        metadata.logger.info.assert_not_called()

    def test_is_dnf_modules_enable_enabled_image_config_override(self):
        """Test dnf_modules_enable respects image-level configuration override"""
        metadata = self._create_image_metadata('openshift/test-dnf-modules-enable')

        # Mock image config override
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.dnf_modules_enable = False
        metadata.config = mock_config

        # Mock group config as Missing
        mock_group_config = MagicMock()
        mock_group_config.konflux.cachi2.lockfile.dnf_modules_enable = Missing
        metadata.runtime.group_config = mock_group_config
        metadata.logger = MagicMock()

        result = metadata.is_dnf_modules_enable_enabled()

        self.assertFalse(result)
        metadata.logger.info.assert_called_once_with("DNF modules enablement set from metadata config: False")

    def test_is_dnf_modules_enable_enabled_group_config_override(self):
        """Test dnf_modules_enable respects group-level configuration override"""
        metadata = self._create_image_metadata('openshift/test-dnf-modules-enable')

        # Mock image config as Missing
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.dnf_modules_enable = Missing
        metadata.config = mock_config

        # Mock group config override
        mock_group_config = MagicMock()
        mock_group_config.konflux.cachi2.lockfile.dnf_modules_enable = True
        metadata.runtime.group_config = mock_group_config
        metadata.logger = MagicMock()

        result = metadata.is_dnf_modules_enable_enabled()

        self.assertTrue(result)
        metadata.logger.info.assert_called_once_with("DNF modules enablement set from group config: True")

    def test_is_dnf_modules_enable_enabled_precedence(self):
        """Test dnf_modules_enable configuration hierarchy precedence (image > group)"""
        metadata = self._create_image_metadata('openshift/test-dnf-modules-enable')

        # Mock image config override (should take precedence)
        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.dnf_modules_enable = False
        metadata.config = mock_config

        # Mock group config with different value
        mock_group_config = MagicMock()
        mock_group_config.konflux.cachi2.lockfile.dnf_modules_enable = True
        metadata.runtime.group_config = mock_group_config
        metadata.logger = MagicMock()

        result = metadata.is_dnf_modules_enable_enabled()

        self.assertFalse(result)
        # Should use image config and log it
        metadata.logger.info.assert_called_once_with("DNF modules enablement set from metadata config: False")

    def test_get_required_artifacts_disabled(self):
        # Create a mock instance
        metadata = MagicMock()
        metadata.is_artifact_lockfile_enabled.return_value = False
        metadata.get_required_artifacts = ImageMetadata.get_required_artifacts.__get__(metadata, ImageMetadata)

        result = metadata.get_required_artifacts()
        self.assertEqual(result, [])

    def test_get_required_artifacts_enabled(self):
        # Create a mock instance
        metadata = MagicMock()
        metadata.is_artifact_lockfile_enabled.return_value = True
        metadata.config.konflux.cachi2.artifact_lockfile.resources = [
            "https://example.com/cert1.pem",
            "https://example.com/cert2.pem",
        ]
        metadata.get_required_artifacts = ImageMetadata.get_required_artifacts.__get__(metadata, ImageMetadata)

        result = metadata.get_required_artifacts()
        expected = ["https://example.com/cert1.pem", "https://example.com/cert2.pem"]
        self.assertEqual(result, expected)

    def test_get_required_artifacts_missing_resources(self):
        from artcommonlib.model import Missing

        # Create a mock instance
        metadata = MagicMock()
        metadata.is_artifact_lockfile_enabled.return_value = True
        metadata.config.konflux.cachi2.artifact_lockfile.resources = Missing
        metadata.get_required_artifacts = ImageMetadata.get_required_artifacts.__get__(metadata, ImageMetadata)

        result = metadata.get_required_artifacts()
        self.assertEqual(result, [])
