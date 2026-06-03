import json
from unittest import TestCase

from artcommonlib import util as artlib_util
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)


class TestKonfluxBuildOutcome(TestCase):
    def test_granular_outcome_serialization(self):
        for outcome, value in (
            (KonfluxBuildOutcome.BUILD_ERROR, 'build_error'),
            (KonfluxBuildOutcome.ITS_ERROR, 'its_error'),
            (KonfluxBuildOutcome.RELEASE_ERROR, 'release_error'),
        ):
            build = KonfluxBuildRecord(outcome=outcome)
            self.assertEqual(build.to_dict()['outcome'], value)
            self.assertEqual(KonfluxBuildOutcome(value), outcome)

    def test_is_success_and_is_failure(self):
        self.assertTrue(KonfluxBuildOutcome.SUCCESS.is_success())
        self.assertFalse(KonfluxBuildOutcome.SUCCESS.is_failure())

        for outcome in (
            KonfluxBuildOutcome.FAILURE,
            KonfluxBuildOutcome.BUILD_ERROR,
            KonfluxBuildOutcome.ITS_ERROR,
            KonfluxBuildOutcome.RELEASE_ERROR,
            KonfluxBuildOutcome.TIMEOUT,
            KonfluxBuildOutcome.CANCELLED,
        ):
            self.assertTrue(outcome.is_failure(), outcome)
            self.assertFalse(outcome.is_success(), outcome)

        self.assertFalse(KonfluxBuildOutcome.PENDING.is_failure())
        self.assertFalse(KonfluxBuildOutcome.PENDING.is_success())

    def test_db_filter_values_includes_granular_outcomes(self):
        values = KonfluxBuildOutcome.db_filter_values()
        self.assertIn('success', values)
        self.assertIn('failure', values)
        self.assertIn('build_error', values)
        self.assertIn('its_error', values)
        self.assertIn('release_error', values)
        self.assertNotIn('pending', values)

    def test_extract_from_pipelinerun_failed_plr(self):
        failed_condition = artlib_util.KubeCondition(
            {'type': 'Succeeded', 'status': 'False', 'reason': 'Failed', 'message': 'build failed'}
        )
        self.assertEqual(
            KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(failed_condition),
            KonfluxBuildOutcome.BUILD_ERROR,
        )
        timeout_condition = artlib_util.KubeCondition(
            {'type': 'Succeeded', 'status': 'False', 'reason': 'Timeout', 'message': 'timed out'}
        )
        self.assertEqual(
            KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(timeout_condition),
            KonfluxBuildOutcome.TIMEOUT,
        )


class TestKonfluxBuild(TestCase):
    def test_empty_build(self):
        build_1 = KonfluxBuildRecord()
        build_2 = KonfluxBuildRecord()
        self.assertEqual(build_1.build_id, build_2.build_id)
        self.assertNotEqual(build_1.record_id, build_2.record_id)

    def test_build_id_reentrance(self):
        build = KonfluxBuildRecord()
        self.assertEqual(build.build_id, build.generate_build_id())

    def test_update_fields(self):
        build_1 = KonfluxBuildRecord()
        build_2 = KonfluxBuildRecord(embargoed=True)
        self.assertNotEqual(build_1.build_id, build_2.build_id)

        build_2.embargoed = False
        build_2.build_id = build_2.generate_build_id()
        self.assertEqual(build_1.build_id, build_2.build_id)

    def test_unique_record_id(self):
        build = KonfluxBuildRecord()
        record_id = build.generate_record_id()
        self.assertNotEqual(build.record_id, record_id)

    def test_string_repr(self):
        build = KonfluxBuildRecord()
        str_repr = str(build)
        self.assertEqual(str_repr, json.dumps(build.to_dict(), indent=4))

    def test_nvr(self):
        build = KonfluxBuildRecord()
        self.assertIsNotNone(build.nvr)
        self.assertEqual(build.nvr, build.get_nvr())

        build.nvr = 'nvr'
        self.assertNotEqual(build.nvr, build.get_nvr())

        build = KonfluxBuildRecord(name='image', version='v1', release='123456')
        self.assertEqual(build.nvr, 'image-v1-123456')
        build.version = 'v2'
        self.assertEqual(build.get_nvr(), build.nvr)

    def test_excluded_keys(self):
        build = KonfluxBuildRecord()
        build_id = build.build_id

        build.nvr = 'nvr'
        self.assertEqual(build.generate_build_id(), build_id)

        build.build_id = 'build_id'
        self.assertEqual(build.generate_build_id(), build_id)

        build.record_id = 'record_id'
        self.assertEqual(build.generate_build_id(), build_id)


class TestKonfluxBundleBuildRecordEC(TestCase):
    def test_ec_pipeline_url_default(self):
        build = KonfluxBundleBuildRecord()
        self.assertEqual(build.ec_pipeline_url, '')


class TestKonfluxFbcBuildRecordEC(TestCase):
    def test_ec_pipeline_url_default(self):
        build = KonfluxFbcBuildRecord()
        self.assertEqual(build.ec_pipeline_url, '')
