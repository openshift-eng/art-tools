#!/usr/bin/env python

import logging
import unittest
from unittest.mock import MagicMock, Mock

from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from doozerlib.build_info import KonfluxBuildRecordInspector, KonfluxImageInspector


class MockRuntime:
    def __init__(self, logger):
        self.logger = logger


class TestKonfluxImageInspector(unittest.TestCase):
    def setUp(self):
        self.logger = MagicMock(spec=logging.Logger)
        self.runtime = MockRuntime(self.logger)

    def _create_image_info(self, env_vars=None, labels=None):
        """Helper to create a mock image_info dict"""
        if env_vars is None:
            env_vars = []
        if labels is None:
            labels = {}

        return {
            'name': 'test-image@sha256:abc123',
            'digest': 'sha256:abc123',
            'listDigest': 'sha256:def456',
            'config': {'architecture': 'amd64', 'config': {'Env': env_vars, 'Labels': labels}},
        }

    def _create_mock_build_record_inspector(self, image_pullspec='test-image@sha256:abc123'):
        """Helper to create a mock KonfluxBuildRecordInspector"""
        mock_build_record = Mock(spec=KonfluxBuildRecord)
        mock_build_record.image_pullspec = image_pullspec

        mock_inspector = Mock(spec=KonfluxBuildRecordInspector)
        mock_inspector.get_build_obj.return_value = mock_build_record

        return mock_inspector

    def test_get_image_envs_basic(self):
        """Test basic environment variable extraction"""
        image_info = self._create_image_info(env_vars=['PATH=/usr/bin', 'HOME=/root', 'TAGS=scos'])
        build_inspector = self._create_mock_build_record_inspector()

        # This should fail due to SCOS tag check, but let's test just the env parsing
        # We'll catch the exception and verify the env method works
        try:
            inspector = KonfluxImageInspector(self.runtime, image_info, build_inspector)
        except ValueError:
            pass

        # Create inspector without TAGS to test env parsing
        image_info_no_tags = self._create_image_info(
            env_vars=['PATH=/usr/bin', 'HOME=/root'], labels={'org.label-schema.vendor': 'Red Hat, Inc.'}
        )
        inspector = KonfluxImageInspector(self.runtime, image_info_no_tags, build_inspector)
        envs = inspector.get_image_envs()

        self.assertEqual(envs['PATH'], '/usr/bin')
        self.assertEqual(envs['HOME'], '/root')

    def test_get_image_labels_basic(self):
        """Test basic label extraction"""
        image_info = self._create_image_info(
            labels={'name': 'test-image', 'version': '1.0', 'org.label-schema.vendor': 'Red Hat, Inc.'}
        )
        build_inspector = self._create_mock_build_record_inspector()

        inspector = KonfluxImageInspector(self.runtime, image_info, build_inspector)
        labels = inspector.get_image_labels()

        self.assertEqual(labels['name'], 'test-image')
        self.assertEqual(labels['version'], '1.0')
        self.assertEqual(labels['org.label-schema.vendor'], 'Red Hat, Inc.')

    def test_scos_tag_with_centos_vendor_passes(self):
        """Test that TAGS=scos with CentOS vendor label passes validation"""
        image_info = self._create_image_info(
            env_vars=['TAGS=scos', 'PATH=/usr/bin'], labels={'org.label-schema.vendor': 'CentOS'}
        )
        build_inspector = self._create_mock_build_record_inspector()

        # Should not raise an exception
        inspector = KonfluxImageInspector(self.runtime, image_info, build_inspector)
        self.assertIsNotNone(inspector)

    def test_scos_tag_without_centos_vendor_fails(self):
        """Test that TAGS=scos without CentOS vendor label fails validation"""
        image_info = self._create_image_info(
            env_vars=['TAGS=scos', 'PATH=/usr/bin'], labels={'org.label-schema.vendor': 'Red Hat, Inc.'}
        )
        build_inspector = self._create_mock_build_record_inspector()

        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            KonfluxImageInspector(self.runtime, image_info, build_inspector)

        self.assertIn('SCOS tag safety check failed', str(context.exception))
        self.assertIn('org.label-schema.vendor label is \'Red Hat, Inc.\'', str(context.exception))

    def test_scos_tag_with_missing_vendor_label_fails(self):
        """Test that TAGS=scos with missing vendor label fails validation"""
        image_info = self._create_image_info(
            env_vars=['TAGS=scos', 'PATH=/usr/bin'],
            labels={},  # No vendor label
        )
        build_inspector = self._create_mock_build_record_inspector()

        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            KonfluxImageInspector(self.runtime, image_info, build_inspector)

        self.assertIn('SCOS tag safety check failed', str(context.exception))
        self.assertIn('org.label-schema.vendor label is \'\'', str(context.exception))

    def test_no_scos_tag_skips_validation(self):
        """Test that images without TAGS=scos skip the validation"""
        image_info = self._create_image_info(
            env_vars=['PATH=/usr/bin', 'OTHER=value'], labels={'org.label-schema.vendor': 'Red Hat, Inc.'}
        )
        build_inspector = self._create_mock_build_record_inspector()

        # Should not raise an exception even though vendor is not CentOS
        inspector = KonfluxImageInspector(self.runtime, image_info, build_inspector)
        self.assertIsNotNone(inspector)

    def test_scos_in_tags_comma_separated(self):
        """Test that TAGS=scos is detected even in comma-separated list"""
        image_info = self._create_image_info(
            env_vars=['TAGS=foo,scos,bar'], labels={'org.label-schema.vendor': 'CentOS'}
        )
        build_inspector = self._create_mock_build_record_inspector()

        # Should not raise because vendor is CentOS
        inspector = KonfluxImageInspector(self.runtime, image_info, build_inspector)
        self.assertIsNotNone(inspector)

    def test_scos_in_tags_comma_separated_wrong_vendor(self):
        """Test that TAGS with scos in comma-separated list fails with wrong vendor"""
        image_info = self._create_image_info(
            env_vars=['TAGS=foo,scos,bar'], labels={'org.label-schema.vendor': 'Red Hat, Inc.'}
        )
        build_inspector = self._create_mock_build_record_inspector()

        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            KonfluxImageInspector(self.runtime, image_info, build_inspector)

        self.assertIn('SCOS tag safety check failed', str(context.exception))


if __name__ == '__main__':
    unittest.main()
