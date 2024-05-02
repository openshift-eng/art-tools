import unittest
from unittest import mock

from flexmock import flexmock

from elliottlib import errata as erratalib
from elliottlib.brew import Build
from elliottlib.cli.find_builds_cli import _filter_out_attached_builds, _find_shipped_builds


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


if __name__ == "__main__":
    unittest.main()
