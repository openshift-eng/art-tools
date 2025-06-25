import logging
import os
import shutil
import tempfile
import unittest
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import MagicMock, patch

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
        logging.basicConfig(filename=self.test_file, level=logging.DEBUG)
        self.logger = logging.getLogger()

        self.cwd = os.getcwd()
        os.chdir(self.test_dir)

        test_yml = open('test.yml', 'w')
        test_yml.write(TEST_YAML)
        test_yml.close()

    def tearDown(self):
        os.chdir(self.cwd)

        logging.shutdown()
        reload(logging)
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

    def test_lockfile_force_enabled_metadata_override_true(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = True
        metadata.config = mock_config
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertTrue(result)
        self.logger.info.assert_any_call("Lockfile force generation set from metadata config: True")

    def test_lockfile_force_enabled_metadata_override_false(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = False
        metadata.config = mock_config
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertFalse(result)
        self.logger.info.assert_any_call("Lockfile force generation set from metadata config: False")

    def test_lockfile_force_enabled_missing_override(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = Missing
        metadata.config = mock_config
        metadata.runtime.group_config.konflux.cachi2.lockfile.force = Missing
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertFalse(result)
        # Should not log anything when using default

    def test_lockfile_force_enabled_none_override(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = None
        metadata.config = mock_config
        metadata.runtime.group_config.konflux.cachi2.lockfile.force = None
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertFalse(result)
        # Should not log anything when using default

    def test_lockfile_force_enabled_group_config_true(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = Missing
        metadata.config = mock_config
        metadata.runtime.group_config.konflux.cachi2.lockfile.force = True
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertTrue(result)
        self.logger.info.assert_any_call("Lockfile force generation set from group config: True")

    def test_lockfile_force_enabled_group_config_false(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = Missing
        metadata.config = mock_config
        metadata.runtime.group_config.konflux.cachi2.lockfile.force = False
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertFalse(result)
        self.logger.info.assert_any_call("Lockfile force generation set from group config: False")

    def test_lockfile_force_enabled_metadata_precedence(self):
        self.logger = MagicMock()
        metadata = self._create_image_metadata('openshift/test_lockfile_force')

        mock_config = MagicMock()
        mock_config.konflux.cachi2.lockfile.force = False
        metadata.config = mock_config
        metadata.runtime.group_config.konflux.cachi2.lockfile.force = True
        metadata.logger = self.logger

        result = metadata.is_lockfile_force_enabled()
        self.assertFalse(result)
        self.logger.info.assert_any_call("Lockfile force generation set from metadata config: False")


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
        get_image_meta.return_value = mock.MagicMock(
            autospec=image.ImageMetadata,
            config={
                "enabled_repos": ["rhel-8-baseos-rpms", "rhel-8-appstream-rpms"],
            },
        )
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
