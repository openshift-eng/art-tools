import json
from unittest import TestCase

from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxECStatus,
    KonfluxFbcBuildRecord,
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

    def test_ec_status_default(self):
        build = KonfluxBuildRecord()
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)

    def test_ec_status_serialization(self):
        build = KonfluxBuildRecord(ec_status=KonfluxECStatus.PASSED)
        d = build.to_dict()
        self.assertEqual(d['ec_status'], 'passed')

        build = KonfluxBuildRecord(ec_status=KonfluxECStatus.FAILED)
        d = build.to_dict()
        self.assertEqual(d['ec_status'], 'failed')

        build = KonfluxBuildRecord(ec_status=KonfluxECStatus.NOT_APPLICABLE)
        d = build.to_dict()
        self.assertEqual(d['ec_status'], 'n/a')

    def test_ec_status_excluded_from_build_id(self):
        build_1 = KonfluxBuildRecord(ec_status=KonfluxECStatus.NOT_APPLICABLE)
        build_2 = KonfluxBuildRecord(ec_status=KonfluxECStatus.PASSED)
        build_3 = KonfluxBuildRecord(ec_status=KonfluxECStatus.FAILED)
        self.assertEqual(build_1.build_id, build_2.build_id)
        self.assertEqual(build_1.build_id, build_3.build_id)

    def test_ec_status_none_defaults_to_not_applicable(self):
        build = KonfluxBuildRecord(ec_status=None)
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)

    def test_ec_status_string_to_enum_conversion(self):
        build = KonfluxBuildRecord(ec_status='passed')
        self.assertEqual(build.ec_status, KonfluxECStatus.PASSED)

        build = KonfluxBuildRecord(ec_status='failed')
        self.assertEqual(build.ec_status, KonfluxECStatus.FAILED)

        build = KonfluxBuildRecord(ec_status='n/a')
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)


class TestKonfluxBundleBuildRecordEC(TestCase):
    def test_ec_status_default(self):
        build = KonfluxBundleBuildRecord()
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)
        self.assertEqual(build.ec_pipeline_url, '')

    def test_ec_status_serialization(self):
        build = KonfluxBundleBuildRecord(ec_status=KonfluxECStatus.PASSED, ec_pipeline_url='https://example.com/plr')
        d = build.to_dict()
        self.assertEqual(d['ec_status'], 'passed')
        self.assertEqual(d['ec_pipeline_url'], 'https://example.com/plr')

    def test_ec_status_none_defaults_to_not_applicable(self):
        build = KonfluxBundleBuildRecord(ec_status=None)
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)

    def test_ec_status_string_to_enum_conversion(self):
        build = KonfluxBundleBuildRecord(ec_status='passed')
        self.assertEqual(build.ec_status, KonfluxECStatus.PASSED)

        build = KonfluxBundleBuildRecord(ec_status='failed')
        self.assertEqual(build.ec_status, KonfluxECStatus.FAILED)

    def test_ec_status_excluded_from_build_id(self):
        build_1 = KonfluxBundleBuildRecord(ec_status=KonfluxECStatus.NOT_APPLICABLE)
        build_2 = KonfluxBundleBuildRecord(ec_status=KonfluxECStatus.PASSED)
        self.assertEqual(build_1.build_id, build_2.build_id)


class TestKonfluxFbcBuildRecordEC(TestCase):
    def test_ec_status_default(self):
        build = KonfluxFbcBuildRecord()
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)
        self.assertEqual(build.ec_pipeline_url, '')

    def test_ec_status_serialization(self):
        build = KonfluxFbcBuildRecord(ec_status=KonfluxECStatus.PASSED, ec_pipeline_url='https://example.com/plr')
        d = build.to_dict()
        self.assertEqual(d['ec_status'], 'passed')
        self.assertEqual(d['ec_pipeline_url'], 'https://example.com/plr')

    def test_ec_status_none_defaults_to_not_applicable(self):
        build = KonfluxFbcBuildRecord(ec_status=None)
        self.assertEqual(build.ec_status, KonfluxECStatus.NOT_APPLICABLE)

    def test_ec_status_string_to_enum_conversion(self):
        build = KonfluxFbcBuildRecord(ec_status='passed')
        self.assertEqual(build.ec_status, KonfluxECStatus.PASSED)

        build = KonfluxFbcBuildRecord(ec_status='failed')
        self.assertEqual(build.ec_status, KonfluxECStatus.FAILED)

    def test_ec_status_excluded_from_build_id(self):
        build_1 = KonfluxFbcBuildRecord(ec_status=KonfluxECStatus.NOT_APPLICABLE)
        build_2 = KonfluxFbcBuildRecord(ec_status=KonfluxECStatus.PASSED)
        self.assertEqual(build_1.build_id, build_2.build_id)
