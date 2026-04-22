#!/usr/bin/env python3

"""
Tests for el_target handling in scan_sources_konflux, particularly for OKD variant.
"""

from unittest import IsolatedAsyncioTestCase
from unittest.mock import MagicMock, patch

import aiohttp
from artcommonlib.variants import BuildVariant
from doozerlib.cli.scan_sources_konflux import ConfigScanSources
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime


class TestGetImageElTarget(IsolatedAsyncioTestCase):
    """
    Test the _get_image_el_target method for correct el_target extraction.
    """

    def setUp(self):
        """
        Set up test fixtures.
        """
        self.runtime = MagicMock(spec=Runtime)
        self.runtime.konflux_db = MagicMock()
        self.runtime.group = 'openshift-5.0'
        self.runtime.branch = 'rhaos-5.0-rhel-9'  # Default OCP branch
        self.runtime.load_disabled = False
        self.runtime.image_metas.return_value = []
        self.runtime.rpm_metas.return_value = []
        self.runtime.image_map = {}
        self.session = MagicMock(spec=aiohttp.ClientSession)

    def _create_scanner(self, variant: str = 'ocp', runtime_branch: str = None):
        """
        Create a ConfigScanSources instance for testing.

        Arg(s):
            variant (str): Variant to use ('ocp' or 'okd')
            runtime_branch (str): Override runtime.branch (for OKD testing)
        Return Value(s):
            ConfigScanSources: Scanner instance
        """
        self.runtime.variant = BuildVariant(variant)
        if runtime_branch:
            self.runtime.branch = runtime_branch
        with patch.dict('os.environ', {'GITHUB_TOKEN': 'test_token'}):
            scanner = ConfigScanSources(
                runtime=self.runtime,
                ci_kubeconfig="/tmp/test_kubeconfig",
                session=self.session,
                as_yaml=False,
                rebase_priv=False,
                dry_run=False,
                variant=variant,
            )
        return scanner

    def test_get_image_el_target_ocp_rhel9(self):
        """
        Test el_target extraction for standard OCP image with rhel-9 branch.
        """
        scanner = self._create_scanner(variant='ocp')

        # Mock image metadata with rhel-9 branch
        image_meta = MagicMock(spec=ImageMetadata)
        image_meta.distgit_key = 'test-image-rhel9'
        image_meta.branch_el_target.return_value = 9

        el_target = scanner._get_image_el_target(image_meta)

        self.assertEqual(el_target, 'el9')
        image_meta.branch_el_target.assert_called_once()

    def test_get_image_el_target_okd_rhel10(self):
        """
        Test el_target extraction for OKD image with rhel-10 branch.

        This simulates the openshift-enterprise-base-rhel9 case where:
        - Image name contains 'rhel9' (upstream Dockerfile uses el9)
        - But OKD runtime branch is rhaos-5.0-rhel-10 (scos10)

        OKD builds use 'scos' prefix (Stream CentOS) instead of 'el'.
        For OKD, ALL images use the same el_target from runtime.branch.
        """
        scanner = self._create_scanner(variant='okd', runtime_branch='rhaos-5.0-rhel-10')

        # Mock image metadata - for OKD, individual branch doesn't matter
        image_meta = MagicMock(spec=ImageMetadata)
        image_meta.distgit_key = 'openshift-enterprise-base-rhel9'

        el_target = scanner._get_image_el_target(image_meta)

        self.assertEqual(el_target, 'scos10')
        # For OKD, branch_el_target should NOT be called (uses runtime.branch instead)
        image_meta.branch_el_target.assert_not_called()

    def test_get_image_el_target_error_handling(self):
        """
        Test that errors in el_target extraction are handled gracefully.
        """
        scanner = self._create_scanner(variant='ocp')

        # Mock image metadata that raises an error
        image_meta = MagicMock(spec=ImageMetadata)
        image_meta.distgit_key = 'invalid-image'
        image_meta.branch_el_target.side_effect = IOError('Unable to determine rhel version')

        with self.assertLogs(level='WARNING') as log_context:
            el_target = scanner._get_image_el_target(image_meta)

        self.assertIsNone(el_target)
        self.assertTrue(any('Unable to determine el_target for invalid-image' in msg for msg in log_context.output))

    def test_get_image_el_target_rhel8(self):
        """
        Test el_target extraction for rhel-8 branch.
        """
        scanner = self._create_scanner(variant='ocp')

        # Mock image metadata with rhel-8 branch
        image_meta = MagicMock(spec=ImageMetadata)
        image_meta.distgit_key = 'test-image-rhel8'
        image_meta.branch_el_target.return_value = 8

        el_target = scanner._get_image_el_target(image_meta)

        self.assertEqual(el_target, 'el8')

    def test_get_image_el_target_okd_rhel9(self):
        """
        Test el_target extraction for OKD 4.x with rhel-9 runtime branch uses scos9.

        For OKD, ALL images use the same el_target from runtime.branch,
        regardless of their individual distgit branch settings.
        """
        scanner = self._create_scanner(variant='okd', runtime_branch='rhaos-4.21-rhel-9')

        # Mock image metadata - for OKD, individual branch doesn't matter
        image_meta = MagicMock(spec=ImageMetadata)
        image_meta.distgit_key = 'test-image-rhel9'

        el_target = scanner._get_image_el_target(image_meta)

        self.assertEqual(el_target, 'scos9')
        # For OKD, branch_el_target should NOT be called (uses runtime.branch instead)
        image_meta.branch_el_target.assert_not_called()
