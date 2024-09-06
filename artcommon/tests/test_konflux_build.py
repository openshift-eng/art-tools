import json
from unittest import TestCase

from artcommonlib.konflux.konflux_build import KonfluxBuild


class TestKonfluxBuild(TestCase):
    def test_empty_build(self):
        build_1 = KonfluxBuild()
        build_2 = KonfluxBuild()
        self.assertEqual(build_1.build_id, build_2.build_id)
        self.assertNotEqual(build_1.record_id, build_2.record_id)

    def test_build_id_reentrance(self):
        build = KonfluxBuild()
        self.assertEqual(build.build_id, build.generate_build_id())

    def test_update_fields(self):
        build_1 = KonfluxBuild()
        build_2 = KonfluxBuild(embargoed=True)
        self.assertNotEqual(build_1.build_id, build_2.build_id)

        build_2.embargoed = False
        build_2.build_id = build_2.generate_build_id()
        self.assertEqual(build_1.build_id, build_2.build_id)

    def test_unique_record_id(self):
        build = KonfluxBuild()
        record_id = build.generate_record_id()
        self.assertNotEqual(build.record_id, record_id)

    def test_string_repr(self):
        build = KonfluxBuild()
        str_repr = str(build)
        self.assertEqual(str_repr, json.dumps(build.to_dict(), indent=4))

    def test_nvr(self):
        build = KonfluxBuild()
        self.assertIsNotNone(build.nvr)
        self.assertEqual(build.nvr, build.get_nvr())

        build.nvr = 'nvr'
        self.assertNotEqual(build.nvr, build.get_nvr())

        build = KonfluxBuild(name='image', version='v1', release='123456')
        self.assertEqual(build.nvr, 'image-v1-123456')
        build.version = 'v2'
        self.assertEqual(build.get_nvr(), build.nvr)

    def test_excluded_keys(self):
        build = KonfluxBuild()
        build_id = build.build_id

        build.nvr = 'nvr'
        self.assertEqual(build.generate_build_id(), build_id)

        build.build_id = 'build_id'
        self.assertEqual(build.generate_build_id(), build_id)

        build.record_id = 'record_id'
        self.assertEqual(build.generate_build_id(), build_id)
