import asyncio
import unittest
from unittest import mock

from elliottlib import errata as erratalib
from elliottlib.brew import Build
from elliottlib.cli.find_builds_cli import _filter_out_attached_builds, _find_shipped_builds, find_builds_konflux
from flexmock import flexmock


class TestFindBuildsCli(unittest.TestCase):
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

    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    def test_find_builds_konflux(self, MockKonfluxBuildRecord: mock.MagicMock):
        runtime = flexmock.flexmock()
        runtime.should_receive("konflux_db.bind").with_args(MockKonfluxBuildRecord).once()

        image_meta_1 = flexmock.flexmock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta_1.should_receive("branch_el_target").and_return("el8").once()
        image_meta_1.should_receive("get_latest_build").with_args(el_target="el8").and_return(
            flexmock.flexmock(nvr="image1-1.0.0-1.el8")
        ).once()

        image_meta_2 = flexmock.flexmock(base_only=False, is_release=True, is_payload=False, distgit_key="image2")
        image_meta_2.should_receive("branch_el_target").and_return("el9").once()
        image_meta_2.should_receive("get_latest_build").with_args(el_target="el9").and_return(
            flexmock.flexmock(nvr="image2-2.0.0-1.el9")
        ).once()

        image_meta_3 = flexmock.flexmock(base_only=True, is_release=True, is_payload=True, distgit_key="image3")
        image_meta_3.should_receive("branch_el_target").never()
        image_meta_3.should_receive("get_latest_build").never()

        image_meta_4 = flexmock.flexmock(base_only=False, is_release=False, is_payload=True, distgit_key="image4")
        image_meta_4.should_receive("branch_el_target").never()
        image_meta_4.should_receive("get_latest_build").never()

        # Scenario 1: payload = True (only image1 should be processed)
        runtime.should_receive("image_metas").and_return(
            [image_meta_1, image_meta_2, image_meta_3, image_meta_4]
        ).once()
        actual_records = asyncio.run(find_builds_konflux(runtime, payload=True))
        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0].nvr, "image1-1.0.0-1.el8")


if __name__ == "__main__":
    unittest.main()
