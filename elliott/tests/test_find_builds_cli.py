import unittest
from unittest import IsolatedAsyncioTestCase, TestCase, mock
from unittest.mock import AsyncMock, MagicMock

from elliottlib import errata as erratalib
from elliottlib.brew import Build
from elliottlib.cli.find_builds_cli import (
    _filter_out_attached_builds,
    _find_shipped_builds,
    find_builds_konflux,
    find_builds_konflux_all_types,
)
from flexmock import flexmock


class TestFindBuildsCli(TestCase):
    """
    Test elliott find-builds command and internal functions
    """

    def test_filter_out_attached_builds_inviable(self):
        flexmock(erratalib).should_receive("get_art_release_from_erratum").and_return('4.1')

        builds = flexmock(Build(nvr="test-1.1.1", product_version="RHEL-7-OSE-4.1"))
        builds.should_receive("all_errata").and_return([{"id": 12345}])

        builds, advisories = _filter_out_attached_builds([builds])
        self.assertEqual([], builds)
        self.assertEqual({12345: {'test-1.1.1'}}, advisories)

    def test_filter_out_attached_builds_viable(self):
        flexmock(erratalib).should_receive("get_art_release_from_erratum").and_return('4.1')

        builds = flexmock(Build(nvr="test-1.1.1", product_version="RHEL-7-OSE-4.5"))
        builds.should_receive("all_errata").and_return([{"id": 12345}])

        builds, advisories = _filter_out_attached_builds([builds])
        self.assertEqual([Build("test-1.1.1")], builds)
        self.assertEqual(dict(), advisories)

    @mock.patch("elliottlib.brew.get_builds_tags")
    def test_find_shipped_builds(self, get_builds_tags: mock.MagicMock):
        build_ids = [11, 12, 13, 14, 15]
        build_tags = [
            [{"name": "foo-candidate"}],
            [{"name": "bar-candidate"}, {"name": "bar-released"}],
            [{"name": "bar-candidate"}, {"name": "RHBA-2077:1001-released"}],
            [{"name": "bar-candidate"}, {"name": "RHSA-2077:1002-released"}],
            [],
        ]
        get_builds_tags.return_value = build_tags
        expected = {13, 14}
        actual = _find_shipped_builds(build_ids, mock.MagicMock())
        self.assertEqual(expected, actual)
        get_builds_tags.assert_called_once_with(build_ids, mock.ANY)


class TestFindBuildsKonflux(IsolatedAsyncioTestCase):
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    async def test_find_builds_konflux(self, MockKonfluxBuildRecord: mock.MagicMock):
        runtime = flexmock(konflux_db=flexmock())
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)

        image_meta_1 = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta_1.branch_el_target.return_value = "el8"
        image_meta_1.get_latest_build = AsyncMock(return_value={"nvr": "image1-1.0.0-1.el8"})

        image_meta_2 = MagicMock(base_only=False, is_release=True, is_payload=False, distgit_key="image2")
        image_meta_2.branch_el_target.return_value = "el9"
        image_meta_2.get_latest_build = AsyncMock(return_value={"nvr": "image2-2.0.0-1.el9"})

        runtime.should_receive("image_metas").and_return([image_meta_1, image_meta_2])
        actual_records = await find_builds_konflux(runtime, payload=True)
        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0]['nvr'], "image1-1.0.0-1.el8")


class TestFindBuildsKonfluxAllTypes(IsolatedAsyncioTestCase):
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_find_builds_konflux_all_types(self, MockKonfluxBundleBuildRecord, MockKonfluxBuildRecord):
        # Setup runtime and DB mocks
        runtime = flexmock(konflux_db=flexmock())
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBundleBuildRecord)

        # Mock image metadata
        image_meta_1 = MagicMock(
            base_only=False, is_release=True, is_payload=True, is_olm_operator=False, distgit_key="image1"
        )
        image_meta_1.branch_el_target.return_value = "el8"
        build_1 = MagicMock(nvr="image1-1.0.0-1.el8")
        image_meta_1.get_latest_build = AsyncMock(return_value=build_1)

        image_meta_2 = MagicMock(
            base_only=False, is_release=True, is_payload=False, is_olm_operator=True, distgit_key="image2"
        )
        image_meta_2.branch_el_target.return_value = "el9"
        build_2 = MagicMock(nvr="image2-2.0.0-1.el9")
        image_meta_2.get_latest_build = AsyncMock(return_value=build_2)

        # Mock OLM bundle search
        build_3 = MagicMock(nvr="image2-bundle-2.0.0-1.el9")
        runtime.konflux_db.should_receive("search_builds_by_fields").and_return(iter([build_3]))

        runtime.should_receive("image_metas").and_return([image_meta_1, image_meta_2])

        async def fake_anext(it, default):
            try:
                return next(it)
            except StopIteration:
                return default

        with mock.patch("elliottlib.cli.find_builds_cli.anext", side_effect=fake_anext):
            builds_map = await find_builds_konflux_all_types(runtime)

        # Assertions
        self.assertEqual(len(builds_map['payload']), 1)
        self.assertEqual(builds_map['payload'][0], build_1.nvr)
        self.assertEqual(len(builds_map['non_payload']), 1)
        self.assertEqual(builds_map['non_payload'][0], build_2.nvr)
        self.assertEqual(len(builds_map['olm_builds']), 1)
        self.assertEqual(builds_map['olm_builds'][0], build_3.nvr)
        self.assertEqual(len(builds_map['olm_builds_not_found']), 0)


if __name__ == "__main__":
    unittest.main()
