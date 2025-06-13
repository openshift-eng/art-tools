import unittest
from unittest import IsolatedAsyncioTestCase, mock
from unittest.mock import AsyncMock, MagicMock

from elliottlib.cli.find_builds_cli import _filter_out_attached_builds, _find_shipped_builds, find_builds_konflux


class TestFindBuildsCli(unittest.TestCase):
    """
    Test elliott find-builds command and internal functions
    """

    @mock.patch("elliottlib.errata.get_art_release_from_erratum")
    def test_filter_out_attached_builds_inviable(self, mock_get_art_release):
        mock_get_art_release.return_value = '4.1'
        mock_build = MagicMock(
            nvr="test-1.1.1",
            product_version="RHEL-7-OSE-4.1",
            all_errata=[{"id": 12345}],
        )
        builds, advisories = _filter_out_attached_builds([mock_build])
        self.assertEqual([], builds)
        self.assertEqual({12345: {'test-1.1.1'}}, advisories)

    @mock.patch("elliottlib.errata.get_art_release_from_erratum")
    def test_filter_out_attached_builds_viable(self, mock_get_art_release):
        mock_get_art_release.return_value = '4.1'
        mock_build = MagicMock(
            nvr="test-1.1.1",
            product_version="RHEL-7-OSE-4.5",
            all_errata=[{"id": 12345}],
        )
        builds, advisories = _filter_out_attached_builds([mock_build])
        self.assertEqual(len(builds), 1)
        self.assertEqual(builds[0].nvr, "test-1.1.1")
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
        # Create mock konflux_db
        mock_konflux_db = MagicMock()
        mock_konflux_db.bind = MagicMock()

        # Create mock runtime
        runtime = MagicMock()
        runtime.konflux_db = mock_konflux_db

        image_meta_1 = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta_1.branch_el_target.return_value = "el8"
        image_meta_1.get_latest_build = AsyncMock(return_value={"nvr": "image1-1.0.0-1.el8"})

        image_meta_2 = MagicMock(base_only=False, is_release=True, is_payload=False, distgit_key="image2")
        image_meta_2.branch_el_target.return_value = "el9"
        image_meta_2.get_latest_build = AsyncMock(return_value={"nvr": "image2-2.0.0-1.el9"})

        runtime.image_metas.return_value = [image_meta_1, image_meta_2]

        actual_records = await find_builds_konflux(runtime, payload=True)

        # Verify the mock was called correctly
        mock_konflux_db.bind.assert_called_once_with(MockKonfluxBuildRecord)

        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0]['nvr'], "image1-1.0.0-1.el8")

    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    async def test_find_builds_konflux_regular_builds(self, MockKonfluxBuildRecord: mock.MagicMock):
        builds = ["image1-1.0.0-1.el8"]
        expected_result = [{"nvr": builds[0]}]

        # Create mock runtime with mock konflux_db
        mock_konflux_db = MagicMock()
        mock_konflux_db.bind = MagicMock()
        mock_konflux_db.get_build_records_by_nvrs = AsyncMock(return_value=expected_result)

        runtime = MagicMock()
        runtime.konflux_db = mock_konflux_db
        runtime.group = "openshift-4.18"

        actual_records = await find_builds_konflux(runtime, payload=True, builds=builds)

        # Verify the mock was called correctly
        mock_konflux_db.bind.assert_called_once_with(MockKonfluxBuildRecord)
        mock_konflux_db.get_build_records_by_nvrs.assert_called_once_with(
            builds,
            where={'group': 'openshift-4.18', 'engine': 'konflux'},
            strict=True,
        )

        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0]['nvr'], builds[0])

    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_find_builds_konflux_bundle_builds(self, MockKonfluxBundleBuildRecord: mock.MagicMock):
        builds = ["image1-bundle-1.0.0-1.el8"]
        expected_result = [{"nvr": builds[0]}]

        # Create mock runtime with mock konflux_db
        mock_konflux_db = MagicMock()
        mock_konflux_db.bind = MagicMock()
        mock_konflux_db.get_build_records_by_nvrs = AsyncMock(return_value=expected_result)

        runtime = MagicMock()
        runtime.konflux_db = mock_konflux_db
        runtime.group = "openshift-4.18"

        actual_records = await find_builds_konflux(runtime, payload=True, builds=builds)

        # Verify the mock was called correctly
        mock_konflux_db.bind.assert_called_once_with(MockKonfluxBundleBuildRecord)
        mock_konflux_db.get_build_records_by_nvrs.assert_called_once_with(
            builds,
            where={'group': 'openshift-4.18', 'engine': 'konflux'},
            strict=True,
        )

        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0]['nvr'], builds[0])

    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxFbcBuildRecord")
    async def test_find_builds_konflux_bundle_builds(self, MockKonfluxFbcBuildRecord: mock.MagicMock):
        builds = ["image1-fbc-1.0.0-1.el8"]
        expected_result = [{"nvr": builds[0]}]

        # Create mock runtime with mock konflux_db
        mock_konflux_db = MagicMock()
        mock_konflux_db.bind = MagicMock()
        mock_konflux_db.get_build_records_by_nvrs = AsyncMock(return_value=expected_result)

        runtime = MagicMock()
        runtime.konflux_db = mock_konflux_db
        runtime.group = "openshift-4.18"

        actual_records = await find_builds_konflux(runtime, payload=True, builds=builds)

        # Verify the mock was called correctly
        mock_konflux_db.bind.assert_called_once_with(MockKonfluxFbcBuildRecord)
        mock_konflux_db.get_build_records_by_nvrs.assert_called_once_with(
            builds,
            where={'group': 'openshift-4.18', 'engine': 'konflux'},
            strict=True,
        )

        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0]['nvr'], builds[0])


if __name__ == "__main__":
    unittest.main()
