import unittest
from unittest.mock import patch

from artcommonlib import ocp_version_lineage as lineage


class TestStandardTrain(unittest.TestCase):
    def setUp(self):
        self.mock_version_map = {3: 11, 4: 22, 5: None}
        self.patcher = patch.object(lineage, 'LAST_OCP_MINOR_VERSION', self.mock_version_map)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_standard_train_next_and_previous(self):
        self.assertEqual(lineage.get_standard_train_next(4, 21), (4, 22))
        self.assertEqual(lineage.get_standard_train_next(4, 22), (5, 0))
        self.assertEqual(lineage.get_standard_train_previous(5, 0), (4, 22))
        self.assertEqual(lineage.get_standard_train_previous(4, 22), (4, 21))

    def test_standard_train_rejects_bridge(self):
        with self.assertRaises(ValueError):
            lineage.get_standard_train_next(4, 23)
        with self.assertRaises(ValueError):
            lineage.get_standard_train_previous(4, 23)


class TestScopedLineageHelpers(unittest.TestCase):
    """Feature-specific helpers for 4.22, 4.23, and 5.0."""

    def setUp(self):
        self.mock_version_map = {3: 11, 4: 22, 5: None}
        self.patcher = patch.object(lineage, 'LAST_OCP_MINOR_VERSION', self.mock_version_map)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_resolve_inflight_schedule_group(self):
        self.assertEqual(lineage.resolve_inflight_schedule_group(4, 22), 'openshift-4.21')
        self.assertEqual(lineage.resolve_inflight_schedule_group(4, 23), 'openshift-4.22')
        self.assertEqual(lineage.resolve_inflight_schedule_group(4, 24), 'openshift-4.23')
        self.assertEqual(lineage.resolve_inflight_schedule_group(5, 0), 'openshift-4.22')

    def test_get_second_fix_reference_version(self):
        self.assertEqual(lineage.get_second_fix_reference_version(4, 22), (4, 21))
        self.assertEqual(lineage.get_second_fix_reference_version(4, 23), (4, 22))
        self.assertEqual(lineage.get_second_fix_reference_version(5, 0), (4, 22))

    def test_get_regression_check_gate_version(self):
        self.assertEqual(lineage.get_regression_check_gate_version(4, 22), '5.0')
        self.assertEqual(lineage.get_regression_check_gate_version(4, 23), '5.1')
        self.assertEqual(lineage.get_regression_check_gate_version(5, 0), '5.1')

    def test_get_blocking_bug_target_version(self):
        self.assertEqual(lineage.get_blocking_bug_target_version(4, 22), (5, 0))
        self.assertEqual(lineage.get_blocking_bug_target_version(4, 23), (5, 0))
        self.assertEqual(lineage.get_blocking_bug_target_version(5, 0), (5, 1))

    def test_get_next_scheduled_release_group(self):
        self.assertEqual(lineage.get_next_scheduled_release_group(4, 22), 'openshift-5.0')
        self.assertEqual(lineage.get_next_scheduled_release_group(4, 23), 'openshift-5.0')
        self.assertEqual(lineage.get_next_scheduled_release_group(5, 0), 'openshift-5.1')

    def test_get_reconciliation_depend_version(self):
        self.assertEqual(lineage.get_reconciliation_depend_version(4, 22), (5, 0))
        self.assertEqual(lineage.get_reconciliation_depend_version(4, 23), (5, 1))
        self.assertEqual(lineage.get_reconciliation_depend_version(5, 0), (5, 1))

    def test_get_ocp5_basis_release(self):
        self.assertEqual(lineage.get_ocp5_basis_release(4, 23), (5, 0))
        self.assertEqual(lineage.get_ocp5_basis_release(4, 24), (5, 1))
