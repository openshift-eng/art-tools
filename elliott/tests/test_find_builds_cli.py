import unittest
from unittest import IsolatedAsyncioTestCase, TestCase, mock
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib import errata as erratalib
from elliottlib.brew import Build
from elliottlib.cli.find_builds_cli import (
    REGISTRY_CHECK_TIMEOUT,
    _fetch_builds_by_kind_rpm,
    _filter_out_attached_builds,
    _filter_shipped_konflux_builds,
    _find_shipped_builds,
    _is_image_released,
    find_builds_konflux,
    find_builds_konflux_all_types,
)
from elliottlib.exceptions import ElliottFatalError
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
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)

        image_meta_1 = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta_1.branch_el_target.return_value = "el8"
        image_meta_1.get_konflux_network_mode.return_value = "hermetic"
        image_meta_1.get_latest_build = AsyncMock(return_value={"nvr": "image1-1.0.0-1.el8"})

        image_meta_2 = MagicMock(base_only=False, is_release=True, is_payload=False, distgit_key="image2")
        image_meta_2.branch_el_target.return_value = "el9"
        image_meta_2.get_konflux_network_mode.return_value = "open"
        image_meta_2.get_latest_build = AsyncMock(return_value={"nvr": "image2-2.0.0-1.el9"})

        runtime.should_receive("image_metas").and_return([image_meta_1, image_meta_2])
        actual_records = await find_builds_konflux(runtime, payload=True, include_shipped=True)
        self.assertEqual(len(actual_records), 1)
        self.assertEqual(actual_records[0]['nvr'], "image1-1.0.0-1.el8")
        image_meta_1.get_latest_build.assert_called_once_with(el_target="el8", exclude_large_columns=True)

    @mock.patch("elliottlib.cli.find_builds_cli.assembly_excluded_components", return_value={"image2"})
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    async def test_find_builds_konflux_excludes_non_payload(self, MockKonfluxBuildRecord, _):
        runtime = flexmock(konflux_db=flexmock(), assembly="4.17.1")
        runtime.should_receive("get_releases_config").and_return(MagicMock())
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)

        image_meta_1 = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta_1.branch_el_target.return_value = "el8"
        image_meta_1.get_latest_build = AsyncMock(return_value={"nvr": "image1-1.0.0-1.el8"})

        image_meta_2 = MagicMock(base_only=False, is_release=True, is_payload=False, distgit_key="image2")
        image_meta_2.branch_el_target.return_value = "el9"
        image_meta_2.get_latest_build = AsyncMock(return_value={"nvr": "image2-2.0.0-1.el9"})

        runtime.should_receive("image_metas").and_return([image_meta_1, image_meta_2])
        actual_records = await find_builds_konflux(runtime, payload=False, include_shipped=True)
        self.assertEqual(len(actual_records), 0)
        image_meta_2.get_latest_build.assert_not_called()

    @mock.patch("elliottlib.cli.find_builds_cli.assembly_excluded_components", return_value={"image1"})
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    async def test_find_builds_konflux_errors_on_excluded_payload(self, MockKonfluxBuildRecord, _):
        runtime = flexmock(konflux_db=flexmock(), assembly="4.17.1")
        runtime.should_receive("get_releases_config").and_return(MagicMock())
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)

        image_meta_1 = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")

        runtime.should_receive("image_metas").and_return([image_meta_1])
        with self.assertRaises(ElliottFatalError):
            await find_builds_konflux(runtime, payload=True, include_shipped=True)


class TestFindBuildsKonfluxAllTypes(IsolatedAsyncioTestCase):
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_find_builds_konflux_all_types(self, MockKonfluxBundleBuildRecord, MockKonfluxBuildRecord):
        # Setup runtime and DB mocks
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBundleBuildRecord)

        # Mock image metadata
        image_meta_1 = MagicMock(
            base_only=False, is_release=True, is_payload=True, is_olm_operator=False, distgit_key="image1"
        )
        image_meta_1.branch_el_target.return_value = "el8"
        image_meta_1.get_konflux_network_mode.return_value = "hermetic"
        build_1 = MagicMock(nvr="image1-1.0.0-1.el8")
        image_meta_1.get_latest_build = AsyncMock(return_value=build_1)

        image_meta_2 = MagicMock(
            base_only=False, is_release=True, is_payload=False, is_olm_operator=True, distgit_key="image2"
        )
        image_meta_2.branch_el_target.return_value = "el9"
        image_meta_2.get_konflux_network_mode.return_value = "open"
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
            builds_map = await find_builds_konflux_all_types(runtime, include_shipped=True)

        # Assertions
        self.assertEqual(len(builds_map['payload']), 1)
        self.assertEqual(builds_map['payload'][0], build_1.nvr)
        self.assertEqual(len(builds_map['non_payload']), 1)
        self.assertEqual(builds_map['non_payload'][0], build_2.nvr)
        self.assertEqual(len(builds_map['olm_builds']), 1)
        self.assertEqual(builds_map['olm_builds'][0], build_3.nvr)
        self.assertEqual(len(builds_map['olm_builds_not_found']), 0)
        image_meta_1.get_latest_build.assert_called_once_with(el_target="el8", exclude_large_columns=True)
        image_meta_2.get_latest_build.assert_called_once_with(el_target="el9", exclude_large_columns=True)

    @mock.patch("elliottlib.cli.find_builds_cli.assembly_excluded_components", return_value={"image2"})
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_find_builds_konflux_all_types_excludes_non_payload(
        self, MockKonfluxBundleBuildRecord, MockKonfluxBuildRecord, _
    ):
        runtime = flexmock(konflux_db=flexmock(), assembly="4.17.1")
        runtime.should_receive("get_releases_config").and_return(MagicMock())
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBundleBuildRecord)

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
        image_meta_2.get_latest_build = AsyncMock(return_value=MagicMock(nvr="image2-2.0.0-1.el9"))

        runtime.should_receive("image_metas").and_return([image_meta_1, image_meta_2])

        builds_map = await find_builds_konflux_all_types(runtime, include_shipped=True)

        self.assertEqual(builds_map['payload'], [build_1.nvr])
        self.assertEqual(builds_map['non_payload'], [])
        self.assertEqual(builds_map['olm_builds'], [])
        self.assertEqual(builds_map['olm_builds_not_found'], [])
        image_meta_2.get_latest_build.assert_not_called()

    @mock.patch("elliottlib.cli.find_builds_cli.assembly_excluded_components", return_value={"image1"})
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @mock.patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_find_builds_konflux_all_types_errors_on_excluded_payload(
        self, MockKonfluxBundleBuildRecord, MockKonfluxBuildRecord, _
    ):
        runtime = flexmock(konflux_db=flexmock(), assembly="4.17.1")
        runtime.should_receive("get_releases_config").and_return(MagicMock())
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBundleBuildRecord)

        image_meta_1 = MagicMock(
            base_only=False, is_release=True, is_payload=True, is_olm_operator=False, distgit_key="image1"
        )

        runtime.should_receive("image_metas").and_return([image_meta_1])
        with self.assertRaises(ElliottFatalError):
            await find_builds_konflux_all_types(runtime, include_shipped=True)


class TestIsImageReleased(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.find_builds_cli.cmd_gather_async", new_callable=AsyncMock)
    async def test_released_image(self, mock_cmd):
        mock_cmd.return_value = (0, "", "")
        result = await _is_image_released("openshift4/ose-cli-rhel9", "4.19.0", "202505210330.p0.el9")
        self.assertTrue(result)
        mock_cmd.assert_called_once_with(
            [
                "skopeo",
                "inspect",
                "--raw",
                "docker://registry.redhat.io/openshift4/ose-cli-rhel9:v4.19.0-202505210330.p0.el9",
            ],
            check=False,
            timeout=REGISTRY_CHECK_TIMEOUT,
        )

    @patch("elliottlib.cli.find_builds_cli.cmd_gather_async", new_callable=AsyncMock)
    async def test_released_image_with_registry_config(self, mock_cmd):
        mock_cmd.return_value = (0, "", "")
        result = await _is_image_released(
            "openshift4/ose-cli-rhel9",
            "4.19.0",
            "202505210330.p0.el9",
            registry_config="/tmp/auth.json",
        )
        self.assertTrue(result)
        mock_cmd.assert_called_once_with(
            [
                "skopeo",
                "inspect",
                "--raw",
                "--authfile",
                "/tmp/auth.json",
                "docker://registry.redhat.io/openshift4/ose-cli-rhel9:v4.19.0-202505210330.p0.el9",
            ],
            check=False,
            timeout=REGISTRY_CHECK_TIMEOUT,
        )

    @patch("elliottlib.cli.find_builds_cli.cmd_gather_async", new_callable=AsyncMock)
    async def test_unreleased_image(self, mock_cmd):
        mock_cmd.return_value = (1, "", "not found")
        result = await _is_image_released("openshift4/ose-cli-rhel9", "4.19.0", "202505210330.p0.el9")
        self.assertFalse(result)

    @patch("elliottlib.cli.find_builds_cli.cmd_gather_async", new_callable=AsyncMock)
    async def test_skopeo_exception_returns_false(self, mock_cmd):
        mock_cmd.side_effect = OSError("skopeo not found")
        result = await _is_image_released("openshift4/ose-cli-rhel9", "4.19.0", "202505210330.p0.el9")
        self.assertFalse(result)


class TestFilterShippedKonfluxBuilds(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.find_builds_cli._is_image_released", new_callable=AsyncMock)
    async def test_filters_released_builds(self, mock_released):
        mock_released.side_effect = [True, False]

        image1 = MagicMock(distgit_key="img1")
        image1.config.delivery.delivery_repo_names = ["openshift4/img1-rhel9"]
        record1 = MagicMock(version="4.19.0", release="1.el9", nvr="img1-4.19.0-1.el9")

        image2 = MagicMock(distgit_key="img2")
        image2.config.delivery.delivery_repo_names = ["openshift4/img2-rhel9"]
        record2 = MagicMock(version="4.19.0", release="2.el9", nvr="img2-4.19.0-2.el9")

        filtered, shipped = await _filter_shipped_konflux_builds([image1, image2], [record1, record2])
        self.assertEqual(filtered, [record2])
        self.assertEqual(shipped, {0})

    @patch("elliottlib.cli.find_builds_cli._is_image_released", new_callable=AsyncMock)
    async def test_skips_missing_delivery_repo(self, mock_released):
        from artcommonlib.model import Missing

        mock_released.return_value = False

        image1 = MagicMock(distgit_key="img1")
        image1.config.delivery.delivery_repo_names = Missing
        record1 = MagicMock(version="4.19.0", release="1.el9")

        image2 = MagicMock(distgit_key="img2")
        image2.config.delivery.delivery_repo_names = ["openshift4/img2-rhel9"]
        record2 = MagicMock(version="4.19.0", release="2.el9")

        filtered, shipped = await _filter_shipped_konflux_builds([image1, image2], [record1, record2])
        self.assertEqual(filtered, [record1, record2])
        self.assertEqual(shipped, set())
        mock_released.assert_called_once()

    @patch("elliottlib.cli.find_builds_cli._is_image_released", new_callable=AsyncMock)
    async def test_skips_empty_delivery_repo_list(self, mock_released):
        image1 = MagicMock(distgit_key="img1")
        image1.config.delivery.delivery_repo_names = []
        record1 = MagicMock(version="4.19.0", release="1.el9")

        filtered, shipped = await _filter_shipped_konflux_builds([image1], [record1])
        self.assertEqual(filtered, [record1])
        self.assertEqual(shipped, set())
        mock_released.assert_not_called()

    @patch("elliottlib.cli.find_builds_cli._is_image_released", new_callable=AsyncMock)
    async def test_returns_all_when_none_released(self, mock_released):
        mock_released.return_value = False

        image1 = MagicMock(distgit_key="img1")
        image1.config.delivery.delivery_repo_names = ["openshift4/img1-rhel9"]
        record1 = MagicMock(version="4.19.0", release="1.el9")

        filtered, shipped = await _filter_shipped_konflux_builds([image1], [record1])
        self.assertEqual(filtered, [record1])
        self.assertEqual(shipped, set())

    @patch("elliottlib.cli.find_builds_cli._is_image_released", new_callable=AsyncMock)
    async def test_returns_empty_when_all_released(self, mock_released):
        mock_released.return_value = True

        image1 = MagicMock(distgit_key="img1")
        image1.config.delivery.delivery_repo_names = ["openshift4/img1-rhel9"]
        record1 = MagicMock(version="4.19.0", release="1.el9")

        filtered, shipped = await _filter_shipped_konflux_builds([image1], [record1])
        self.assertEqual(filtered, [])
        self.assertEqual(shipped, {0})

    @patch("elliottlib.cli.find_builds_cli._is_image_released", new_callable=AsyncMock)
    async def test_empty_records(self, mock_released):
        filtered, shipped = await _filter_shipped_konflux_builds([], [])
        self.assertEqual(filtered, [])
        self.assertEqual(shipped, set())
        mock_released.assert_not_called()


class TestFindBuildsKonfluxShippedFiltering(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.find_builds_cli._filter_shipped_konflux_builds", new_callable=AsyncMock)
    @patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    async def test_filters_shipped_by_default(self, MockKonfluxBuildRecord, mock_filter):
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)

        record1 = MagicMock(nvr="image1-1.0.0-1.el8")
        image_meta = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta.branch_el_target.return_value = "el8"
        image_meta.get_latest_build = AsyncMock(return_value=record1)
        runtime.should_receive("image_metas").and_return([image_meta])

        mock_filter.return_value = ([record1], set())
        result = await find_builds_konflux(runtime, payload=True)
        mock_filter.assert_called_once()
        self.assertEqual(len(result), 1)

    @patch("elliottlib.cli.find_builds_cli._filter_shipped_konflux_builds", new_callable=AsyncMock)
    @patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    async def test_skips_filter_with_include_shipped(self, MockKonfluxBuildRecord, mock_filter):
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonfluxBuildRecord)

        record1 = MagicMock(nvr="image1-1.0.0-1.el8")
        image_meta = MagicMock(base_only=False, is_release=True, is_payload=True, distgit_key="image1")
        image_meta.branch_el_target.return_value = "el8"
        image_meta.get_latest_build = AsyncMock(return_value=record1)
        runtime.should_receive("image_metas").and_return([image_meta])

        result = await find_builds_konflux(runtime, payload=True, include_shipped=True)
        mock_filter.assert_not_called()
        self.assertEqual(len(result), 1)

    @patch("elliottlib.cli.find_builds_cli._filter_shipped_konflux_builds", new_callable=AsyncMock)
    @patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_all_types_filters_shipped(self, MockBundle, MockKonflux, mock_filter):
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonflux)
        runtime.konflux_db.should_receive("bind").with_args(MockBundle)

        build1 = MagicMock(nvr="image1-1.0.0-1.el8")
        image_meta = MagicMock(
            base_only=False, is_release=True, is_payload=True, is_olm_operator=False, distgit_key="image1"
        )
        image_meta.branch_el_target.return_value = "el8"
        image_meta.get_latest_build = AsyncMock(return_value=build1)
        runtime.should_receive("image_metas").and_return([image_meta])

        mock_filter.return_value = ([build1], set())
        builds_map = await find_builds_konflux_all_types(runtime)
        mock_filter.assert_called_once()
        self.assertEqual(builds_map["payload"], [build1.nvr])

    @patch("elliottlib.cli.find_builds_cli._filter_shipped_konflux_builds", new_callable=AsyncMock)
    @patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_all_types_shipped_olm_operator_skips_bundle_lookup(self, MockBundle, MockKonflux, mock_filter):
        """
        When an OLM operator is filtered as shipped, its bundle lookup should also be skipped.
        """
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonflux)
        runtime.konflux_db.should_receive("bind").with_args(MockBundle)

        build_payload = MagicMock(nvr="payload-img-1.0.0-1.el8")
        meta_payload = MagicMock(
            base_only=False, is_release=True, is_payload=True, is_olm_operator=False, distgit_key="payload-img"
        )
        meta_payload.branch_el_target.return_value = "el8"
        meta_payload.get_latest_build = AsyncMock(return_value=build_payload)

        build_olm = MagicMock(nvr="olm-operator-2.0.0-1.el9")
        meta_olm = MagicMock(
            base_only=False, is_release=True, is_payload=False, is_olm_operator=True, distgit_key="olm-operator"
        )
        meta_olm.branch_el_target.return_value = "el9"
        meta_olm.get_latest_build = AsyncMock(return_value=build_olm)

        runtime.should_receive("image_metas").and_return([meta_payload, meta_olm])

        # OLM operator (index 1) is shipped — DB should NOT be queried for bundle builds
        mock_filter.return_value = ([build_payload], {1})
        runtime.konflux_db.should_receive("search_builds_by_fields").never()

        builds_map = await find_builds_konflux_all_types(runtime)

        self.assertEqual(builds_map["payload"], [build_payload.nvr])
        self.assertEqual(builds_map["non_payload"], [])
        self.assertEqual(builds_map["olm_builds"], [])
        self.assertEqual(builds_map["olm_builds_not_found"], [])

    @patch("elliottlib.cli.find_builds_cli._filter_shipped_konflux_builds", new_callable=AsyncMock)
    @patch("elliottlib.cli.find_builds_cli.KonfluxBuildRecord")
    @patch("elliottlib.cli.find_builds_cli.KonfluxBundleBuildRecord")
    async def test_all_types_preserves_flag_alignment(self, MockBundle, MockKonflux, mock_filter):
        """
        Verify that payload_flags, olm_flags, and results stay aligned after filtering.
        3 images: payload (kept), non-payload OLM (shipped), non-payload (kept).
        """
        runtime = flexmock(konflux_db=flexmock(), assembly=None, registry_config=None)
        runtime.should_receive("get_releases_config").and_return(None)
        runtime.konflux_db.should_receive("bind").with_args(MockKonflux)
        runtime.konflux_db.should_receive("bind").with_args(MockBundle)

        build1 = MagicMock(nvr="img1-1.0.0-1.el8")
        meta1 = MagicMock(base_only=False, is_release=True, is_payload=True, is_olm_operator=False, distgit_key="img1")
        meta1.branch_el_target.return_value = "el8"
        meta1.get_latest_build = AsyncMock(return_value=build1)

        build2 = MagicMock(nvr="olm-img-2.0.0-1.el9")
        meta2 = MagicMock(
            base_only=False, is_release=True, is_payload=False, is_olm_operator=True, distgit_key="olm-img"
        )
        meta2.branch_el_target.return_value = "el9"
        meta2.get_latest_build = AsyncMock(return_value=build2)

        build3 = MagicMock(nvr="nonpay-3.0.0-1.el9")
        meta3 = MagicMock(
            base_only=False, is_release=True, is_payload=False, is_olm_operator=False, distgit_key="nonpay"
        )
        meta3.branch_el_target.return_value = "el9"
        meta3.get_latest_build = AsyncMock(return_value=build3)

        runtime.should_receive("image_metas").and_return([meta1, meta2, meta3])

        # Image at index 1 (OLM) shipped
        mock_filter.return_value = ([build1, build3], {1})

        builds_map = await find_builds_konflux_all_types(runtime)

        self.assertEqual(sorted(builds_map["payload"]), [build1.nvr])
        self.assertEqual(sorted(builds_map["non_payload"]), [build3.nvr])
        self.assertEqual(builds_map["olm_builds"], [])
        self.assertEqual(builds_map["olm_builds_not_found"], [])


class TestFetchBuildsByKindRpm(IsolatedAsyncioTestCase):
    @mock.patch("elliottlib.cli.find_builds_cli.assembly_excluded_components", return_value={"rpm-b"})
    @mock.patch("elliottlib.cli.find_builds_cli._find_shipped_builds", return_value=set())
    @mock.patch("elliottlib.cli.find_builds_cli._ensure_accepted_tags")
    async def test_member_only_excludes_rpm(self, mock_ensure_tags, mock_shipped, _):
        runtime = flexmock(assembly="4.17.1", assembly_basis_event=None, rpm_map={})
        runtime.should_receive("get_releases_config").and_return(MagicMock())

        rpm_a = MagicMock(distgit_key="rpm-a")
        rpm_a.determine_targets.return_value = ["rhaos-4.17-rhel-9-candidate"]
        rpm_a.get_latest_build = AsyncMock(
            return_value={"id": 1, "name": "rpm-a", "version": "1.0", "release": "1.el9", "nvr": "rpm-a-1.0-1.el9"}
        )

        rpm_b = MagicMock(distgit_key="rpm-b")
        rpm_b.determine_targets.return_value = ["rhaos-4.17-rhel-9-candidate"]
        rpm_b.get_latest_build = AsyncMock(
            return_value={"id": 2, "name": "rpm-b", "version": "2.0", "release": "1.el9", "nvr": "rpm-b-2.0-1.el9"}
        )

        runtime.should_receive("rpm_metas").and_return([rpm_a, rpm_b])

        tag_pv_map = {"rhaos-4.17-rhel-9-candidate": "RHEL-9-OSE-4.17"}

        def fake_ensure_tags(builds, *args, **kwargs):
            for b in builds:
                b["tag_name"] = "rhaos-4.17-rhel-9-candidate"

        mock_ensure_tags.side_effect = fake_ensure_tags

        nvrps = await _fetch_builds_by_kind_rpm(
            runtime, tag_pv_map, brew_session=MagicMock(), include_shipped=False, member_only=True
        )

        rpm_a.get_latest_build.assert_called_once()
        rpm_b.get_latest_build.assert_not_called()
        self.assertEqual(len(nvrps), 1)
        self.assertEqual(nvrps[0][0], "rpm-a")

    @mock.patch("elliottlib.cli.find_builds_cli.assembly_excluded_components", return_value={"rpm-b"})
    @mock.patch("elliottlib.cli.find_builds_cli._find_shipped_builds", return_value=set())
    @mock.patch("elliottlib.cli.find_builds_cli._ensure_accepted_tags")
    @mock.patch("elliottlib.cli.find_builds_cli.BuildFinder")
    async def test_general_sweep_excludes_rpm(self, MockBuildFinder, mock_ensure_tags, mock_shipped, _):
        rpm_b_meta = MagicMock()
        rpm_b_meta.get_component_name.return_value = "rpm-b-pkg"

        runtime = flexmock(assembly="4.17.1", assembly_basis_event=None, brew_event=None, rpm_map={"rpm-b": rpm_b_meta})
        runtime.should_receive("get_releases_config").and_return(MagicMock())

        tag_pv_map = {"rhaos-4.17-rhel-9-candidate": "RHEL-9-OSE-4.17"}

        builder = MockBuildFinder.return_value
        builder.from_tag.return_value = {
            "rpm-a-pkg": {"id": 1, "name": "rpm-a", "version": "1.0", "release": "1.el9", "nvr": "rpm-a-1.0-1.el9"},
            "rpm-b-pkg": {"id": 2, "name": "rpm-b", "version": "2.0", "release": "1.el9", "nvr": "rpm-b-2.0-1.el9"},
        }

        def fake_ensure_tags(builds, *args, **kwargs):
            for b in builds:
                b["tag_name"] = "rhaos-4.17-rhel-9-candidate"

        mock_ensure_tags.side_effect = fake_ensure_tags

        nvrps = await _fetch_builds_by_kind_rpm(
            runtime, tag_pv_map, brew_session=MagicMock(), include_shipped=False, member_only=False
        )

        self.assertEqual(len(nvrps), 1)
        self.assertEqual(nvrps[0][0], "rpm-a")


if __name__ == "__main__":
    unittest.main()
