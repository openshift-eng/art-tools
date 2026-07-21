import os
import tempfile
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

import click
import koji
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from pyartcd.pipelines.update_golang import (
    UpdateGolangPipeline,
    extract_and_validate_golang_nvrs,
    get_latest_nvr_in_tag,
    is_available,
    is_latest,
    is_latest_and_available,
    move_golang_bugs,
)


class TestExtractAndValidateGolangNvrs(unittest.TestCase):
    """Test the extract_and_validate_golang_nvrs function"""

    def test_valid_nvrs_single_el(self):
        """Test with valid single el8 NVR"""
        go_version, el_nvr_map = extract_and_validate_golang_nvrs("4.16", ["golang-1.20.12-2.el8"])
        self.assertEqual(go_version, "1.20.12")
        self.assertEqual(el_nvr_map, {8: "golang-1.20.12-2.el8"})

    def test_valid_nvrs_multiple_els(self):
        """Test with valid multiple el NVRs"""
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(
            "4.16", ["golang-1.20.12-2.el8", "golang-1.20.12-2.el9"]
        )
        self.assertEqual(go_version, "1.20.12")
        self.assertEqual(el_nvr_map, {8: "golang-1.20.12-2.el8", 9: "golang-1.20.12-2.el9"})

    def test_invalid_ocp_version(self):
        """Test with invalid OCP version format"""
        with self.assertRaisesRegex(ValueError, "Invalid OCP version"):
            extract_and_validate_golang_nvrs("invalid", ["golang-1.20.12-2.el8"])

    def test_unsupported_ocp_minor_version(self):
        """Test with unsupported OCP minor version (< 4.12)"""
        with self.assertRaisesRegex(ValueError, "Only OCP 4.12\\+ is supported"):
            extract_and_validate_golang_nvrs("4.11", ["golang-1.20.12-2.el8"])

    def test_invalid_package_name(self):
        """Test with non-golang package name"""
        with self.assertRaisesRegex(ValueError, "Only `golang` nvrs are supported"):
            extract_and_validate_golang_nvrs("4.16", ["notgolang-1.20.12-2.el8"])

    def test_mismatched_golang_versions(self):
        """Test with different golang versions in NVRs"""
        with self.assertRaisesRegex(ValueError, "All nvrs should have the same golang version"):
            extract_and_validate_golang_nvrs("4.16", ["golang-1.20.12-2.el8", "golang-1.21.0-1.el9"])

    def test_missing_el_version(self):
        """Test with NVR missing el version"""
        with self.assertRaisesRegex(ValueError, "Cannot detect an el version"):
            extract_and_validate_golang_nvrs("4.16", ["golang-1.20.12-2"])

    def test_unsupported_el_version(self):
        """Test with unsupported RHEL version"""
        with self.assertRaisesRegex(ValueError, "Unsupported RHEL version detected"):
            extract_and_validate_golang_nvrs("4.16", ["golang-1.20.12-2.el7"])

    def test_duplicate_el_version(self):
        """Test with duplicate el version in NVRs"""
        with self.assertRaisesRegex(ValueError, "Cannot have two nvrs for the same rhel version"):
            extract_and_validate_golang_nvrs("4.16", ["golang-1.20.12-2.el8", "golang-1.20.12-3.el8"])

    def test_too_many_nvrs(self):
        """Test with too many NVRs (more than supported el versions)"""
        with self.assertRaisesRegex(click.BadParameter, "There should be max 1 nvr for each supported rhel version"):
            extract_and_validate_golang_nvrs(
                "4.16", ["golang-1.20.12-2.el8", "golang-1.20.12-2.el9", "golang-1.20.12-2.el10"]
            )

    def test_el10_unsupported_before_421(self):
        """Test that el10 is rejected for OCP versions before 4.21"""
        with self.assertRaisesRegex(ValueError, "Unsupported RHEL version detected"):
            extract_and_validate_golang_nvrs("4.20", ["golang-1.20.12-2.el10"])

    def test_el10_supported_from_421(self):
        """Test that el10 is accepted for OCP 4.21+"""
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(
            "4.21", ["golang-1.20.12-2.el8", "golang-1.20.12-2.el9", "golang-1.20.12-2.el10"]
        )
        self.assertEqual(go_version, "1.20.12")
        self.assertEqual(
            el_nvr_map,
            {
                8: "golang-1.20.12-2.el8",
                9: "golang-1.20.12-2.el9",
                10: "golang-1.20.12-2.el10",
            },
        )

    def test_el_with_minor_version_suffix(self):
        """Test NVRs with RHEL minor version suffix like el8_10, el9_5"""
        go_version, el_nvr_map = extract_and_validate_golang_nvrs(
            "4.16", ["golang-1.26.3-1.el8_10", "golang-1.26.3-1.el9_6"]
        )
        self.assertEqual(go_version, "1.26.3")
        self.assertEqual(el_nvr_map, {8: "golang-1.26.3-1.el8_10", 9: "golang-1.26.3-1.el9_6"})

    def test_el_minor_version_unsupported_major(self):
        """Test that el7_9 is still rejected even with minor version suffix"""
        with self.assertRaisesRegex(ValueError, "Unsupported RHEL version detected"):
            extract_and_validate_golang_nvrs("4.16", ["golang-1.20.12-2.el7_9"])


class TestGetLatestNvrInTag(unittest.TestCase):
    """Test the get_latest_nvr_in_tag function"""

    def test_package_found_in_tag(self):
        """Test when package is found in tag"""
        mock_koji_session = Mock()
        mock_koji_session.listTagged.return_value = [{"nvr": "golang-1.20.12-2.el8"}]

        result = get_latest_nvr_in_tag("rhaos-4.16-rhel-8-build", "golang", mock_koji_session)

        self.assertEqual(result, "golang-1.20.12-2.el8")
        mock_koji_session.listTagged.assert_called_once_with(
            "rhaos-4.16-rhel-8-build", latest=True, package="golang", inherit=False
        )

    def test_package_not_found_in_tag(self):
        """Test when package is not found in tag"""
        mock_koji_session = Mock()
        mock_koji_session.listTagged.return_value = []

        result = get_latest_nvr_in_tag("rhaos-4.16-rhel-8-build", "golang", mock_koji_session)

        self.assertIsNone(result)


class TestIsLatest(unittest.TestCase):
    """Test the is_latest function"""

    def test_build_is_latest(self):
        """Test when build is the latest"""
        mock_koji_session = Mock()
        mock_koji_session.getLatestBuilds.return_value = [{"nvr": "golang-1.20.12-2.el8"}]

        result = is_latest("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)

        self.assertTrue(result)
        mock_koji_session.getLatestBuilds.assert_called_once_with("rhaos-4.16-rhel-8-build", package="golang")

    def test_build_is_not_latest(self):
        """Test when build is not the latest"""
        mock_koji_session = Mock()
        mock_koji_session.getLatestBuilds.return_value = [{"nvr": "golang-1.20.13-1.el8"}]

        result = is_latest("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)

        self.assertFalse(result)

    def test_no_latest_build_found(self):
        """Test when no latest build is found in tag"""
        mock_koji_session = Mock()
        mock_koji_session.getLatestBuilds.return_value = []

        with self.assertRaisesRegex(ValueError, "Cannot find latest golang build"):
            is_latest("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)


class TestIsAvailable(IsolatedAsyncioTestCase):
    """Test the is_available async function"""

    @patch("artcommonlib.exectools.cmd_assert_async", return_value=0)
    async def test_build_is_available(self, mock_cmd_assert):
        result = await is_available("4.16", 8, "golang-1.20.12-2.el8")

        self.assertTrue(result)
        mock_cmd_assert.assert_called_once()

    @patch("artcommonlib.exectools.cmd_assert_async", return_value=1)
    async def test_build_is_not_available(self, mock_cmd_assert):
        with self.assertLogs("pyartcd.pipelines.update_golang", level="INFO") as cm:
            result = await is_available("4.16", 8, "golang-1.20.12-2.el8")

        self.assertFalse(result)
        self.assertIn("could not be confirmed available", cm.output[0])


class TestIsLatestAndAvailable(IsolatedAsyncioTestCase):
    """Test the is_latest_and_available wrapper"""

    @patch("pyartcd.pipelines.update_golang.is_available", new_callable=AsyncMock, return_value=True)
    @patch("pyartcd.pipelines.update_golang.is_latest", return_value=True)
    async def test_latest_and_available(self, mock_is_latest, mock_is_available):
        mock_koji_session = Mock()
        result = await is_latest_and_available("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)
        self.assertTrue(result)
        mock_is_latest.assert_called_once_with("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)
        mock_is_available.assert_called_once_with("4.16", 8, "golang-1.20.12-2.el8")

    @patch("pyartcd.pipelines.update_golang.is_available", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.is_latest", return_value=False)
    async def test_not_latest_short_circuits(self, mock_is_latest, mock_is_available):
        mock_koji_session = Mock()
        result = await is_latest_and_available("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)
        self.assertFalse(result)
        mock_is_available.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.is_available", new_callable=AsyncMock, return_value=False)
    @patch("pyartcd.pipelines.update_golang.is_latest", return_value=True)
    async def test_latest_but_not_available(self, mock_is_latest, mock_is_available):
        mock_koji_session = Mock()
        result = await is_latest_and_available("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)
        self.assertFalse(result)


class TestMoveGolangBugs(IsolatedAsyncioTestCase):
    """Test the move_golang_bugs async function"""

    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_move_golang_bugs_with_cves(self, mock_cmd_assert):
        """Test moving golang bugs with CVEs"""
        await move_golang_bugs(
            ocp_version="4.16",
            cves=["CVE-2024-1234", "CVE-2024-5678"],
            nvrs=["golang-1.20.12-2.el8"],
            components=["openshift-golang-builder-container"],
            force_update_tracker=False,
            dry_run=False,
        )

        expected_cmd = [
            "elliott",
            "--group",
            "openshift-4.16",
            "--assembly",
            "stream",
            "find-bugs:golang",
            "--analyze",
            "--update-tracker",
            "--cve-id",
            "CVE-2024-1234",
            "--cve-id",
            "CVE-2024-5678",
            "--fixed-in-nvr",
            "golang-1.20.12-2.el8",
            "--component",
            "openshift-golang-builder-container",
        ]
        mock_cmd_assert.assert_called_once_with(expected_cmd, log_stdout=True)

    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_move_golang_bugs_with_force_update(self, mock_cmd_assert):
        """Test moving golang bugs with force update tracker"""
        await move_golang_bugs(
            ocp_version="4.16",
            cves=["CVE-2024-1234"],
            force_update_tracker=True,
            dry_run=False,
        )

        mock_cmd_assert.assert_called_once()
        call_args = mock_cmd_assert.call_args[0][0]
        self.assertIn("--force-update-tracker", call_args)

    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_move_golang_bugs_dry_run(self, mock_cmd_assert):
        """Test moving golang bugs in dry-run mode"""
        await move_golang_bugs(
            ocp_version="4.16",
            cves=["CVE-2024-1234"],
            dry_run=True,
        )

        mock_cmd_assert.assert_called_once()
        call_args = mock_cmd_assert.call_args[0][0]
        self.assertIn("--dry-run", call_args)


class TestUpdateGolangPipeline(IsolatedAsyncioTestCase):
    """Test the UpdateGolangPipeline class"""

    def setUp(self):
        """Set up test environment"""
        os.environ.update(
            {
                "GITHUB_TOKEN": "fake-github-token",
                "KONFLUX_SA_KUBECONFIG": "/path/to/kubeconfig",
            }
        )

    def _make_test_runtime(self, working_dir: Path | None = None):
        if working_dir is None:
            temp_dir = Path(self.enterContext(tempfile.TemporaryDirectory()))
            working_dir = temp_dir / "working"
            working_dir.mkdir()
        mock_runtime = Mock(dry_run=False, working_dir=working_dir)
        mock_runtime.new_slack_client.return_value = Mock(bind_channel=Mock(), say_in_thread=AsyncMock())
        return mock_runtime

    def _make_pipeline(self, build_system="konflux", go_nvrs=None):
        if go_nvrs is None:
            go_nvrs = ["golang-1.25.8-1.el9"]
        return UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=go_nvrs,
            art_jira="ART-1234",
            tag_builds=True,
            build_system=build_system,
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_init_brew_build_system(self, mock_konflux_db):
        """Test initialization with Brew build system"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=["CVE-2024-1234"],
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            scratch=False,
            force_image_build=False,
            build_system="brew",
        )

        self.assertEqual(pipeline.ocp_version, "4.16")
        self.assertEqual(pipeline.cves, ["CVE-2024-1234"])
        self.assertEqual(pipeline.go_nvrs, ["golang-1.20.12-2.el8"])
        self.assertEqual(pipeline.art_jira, "ART-1234")
        self.assertEqual(pipeline.build_system, "brew")
        self.assertFalse(pipeline.scratch)
        self.assertFalse(pipeline.force_image_build)
        self.assertTrue(pipeline.tag_builds)
        self.assertIsInstance(pipeline.koji_session, koji.ClientSession)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_init_konflux_build_system(self, mock_konflux_db):
        """Test initialization with Konflux build system"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        self.assertEqual(pipeline.build_system, "konflux")
        self.assertIsNotNone(pipeline.konflux_db)
        mock_konflux_db.assert_called_once()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_init_with_custom_kubeconfig(self, mock_konflux_db):
        """Test initialization with custom kubeconfig path"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            kubeconfig="/custom/kubeconfig",
        )

        self.assertEqual(pipeline.kubeconfig, "/custom/kubeconfig")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_init_with_data_path_and_gitref(self, mock_konflux_db):
        """Test initialization with custom data path and gitref"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            data_path="/custom/data/path",
            data_gitref="my-branch",
        )

        self.assertEqual(pipeline.data_path, "/custom/data/path")
        self.assertEqual(pipeline.data_gitref, "my-branch")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_doozer_var_args(self, mock_konflux_db):
        """Test _get_doozer_var_args returns --var args when use_new_golang_branch is True"""
        pipeline = self._make_pipeline()
        pipeline.use_new_golang_branch = True
        self.assertEqual(pipeline._get_doozer_var_args(), ['--var', 'MAJOR=4', '--var', 'MINOR=16'])

        pipeline.ocp_version = "5.0"
        self.assertEqual(pipeline._get_doozer_var_args(), ['--var', 'MAJOR=5', '--var', 'MINOR=0'])

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_tag_builds_go_latest_accepts_matching_major_minor(self, mock_konflux_db, mock_get_github_client):
        """Test tag-build validation accepts a build version matching group.yml GO_LATEST major.minor"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(decoded_content=b"vars:\n  GO_LATEST: 1.22\n")
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        branch, allowed_major_minors, build_major_minor = pipeline.validate_go_version_matches_group_vars("1.22.9")
        pipeline.validate_tag_builds_go_latest(branch, allowed_major_minors, build_major_minor)

        requested_paths = [call.args[0] for call in upstream_repo.get_contents.call_args_list]
        self.assertEqual(sorted(requested_paths), ["group.yml", "streams.yml"])

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_go_version_matches_group_vars_accepts_matching_go_extra(
        self, mock_konflux_db, mock_get_github_client
    ):
        """Test version validation accepts a build version matching group.yml GO_EXTRA major.minor"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(decoded_content=b"vars:\n  GO_LATEST: 1.22\n  GO_EXTRA: 1.23\n")
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.23.9-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
        )

        pipeline.validate_go_version_matches_group_vars("1.23.9")

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_go_version_matches_group_vars_accepts_unquoted_trailing_zero(
        self, mock_konflux_db, mock_get_github_client
    ):
        """Test version validation accepts unquoted trailing-zero YAML scalars like 1.20"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(decoded_content=b"vars:\n  GO_LATEST: 1.20\n")
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=False,
        )

        pipeline.validate_go_version_matches_group_vars("1.20.12")

        requested_paths = [call.args[0] for call in upstream_repo.get_contents.call_args_list]
        self.assertEqual(sorted(requested_paths), ["group.yml", "streams.yml"])

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_update_golang_streams_reuses_cached_branch_content(self, mock_konflux_db, mock_get_github_client):
        """Test update_golang_streams reuses the same cached branch content loaded during validation"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime.new_slack_client.return_value = mock_slack

        upstream_repo = Mock()

        def get_contents(path, ref):
            if path == "group.yml":
                return Mock(decoded_content=b"vars:\n  GO_LATEST: 1.22\n")
            if path == "streams.yml":
                return Mock(decoded_content=b"{}\n")
            raise AssertionError(f"Unexpected path requested: {path}")

        upstream_repo.get_contents.side_effect = get_contents
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
        )

        pipeline.validate_go_version_matches_group_vars("1.22.9")
        await pipeline.update_golang_streams("1.22.9", {})

        requested_paths = [call.args[0] for call in upstream_repo.get_contents.call_args_list]
        self.assertEqual(requested_paths.count("group.yml"), 1)
        self.assertEqual(requested_paths.count("streams.yml"), 1)

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_go_version_matches_group_vars_accepts_matching_go_previous(
        self, mock_konflux_db, mock_get_github_client
    ):
        """Test version validation accepts a build version matching group.yml GO_PREVIOUS major.minor"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(
            decoded_content=b"vars:\n  GO_LATEST: 1.22\n  GO_PREVIOUS: 1.21\n"
        )
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.21.13-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
        )

        pipeline.validate_go_version_matches_group_vars("1.21.13")

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_go_version_matches_group_vars_rejects_mismatched_major_minor(
        self, mock_konflux_db, mock_get_github_client
    ):
        """Test version validation rejects build versions that match none of GO_LATEST/GO_EXTRA/GO_PREVIOUS"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(
            decoded_content=b"vars:\n  GO_LATEST: 1.22\n  GO_EXTRA: 1.23\n  GO_PREVIOUS: 1.21\n"
        )
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.24.1-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"must match one of .*GO_LATEST \(1\.22\).*GO_EXTRA \(1\.23\).*GO_PREVIOUS \(1\.21\)",
        ):
            pipeline.validate_go_version_matches_group_vars("1.24.1")

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_go_version_matches_group_vars_permits_mismatch_with_major_bump(
        self, mock_konflux_db, mock_get_github_client
    ):
        """Test version validation permits mismatched major.minor when --major-bump is set"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(
            decoded_content=b"vars:\n  GO_LATEST: 1.22\n  GO_PREVIOUS: 1.21\n"
        )
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.23.1-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            major_bump=True,
        )

        branch, allowed_major_minors, build_major_minor = pipeline.validate_go_version_matches_group_vars("1.23.1")
        self.assertEqual(build_major_minor, "1.23")
        self.assertEqual(allowed_major_minors["GO_LATEST"], "1.22")

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_tag_builds_go_latest_permits_mismatch_with_major_bump(
        self, mock_konflux_db, mock_get_github_client
    ):
        """Test tag-build validation permits mismatched GO_LATEST when --major-bump is set"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(decoded_content=b"vars:\n  GO_LATEST: 1.22\n")
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.23.9-1.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            major_bump=True,
        )

        pipeline.validate_tag_builds_go_latest("openshift-4.16", {"GO_LATEST": "1.22"}, "1.23")

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_validate_tag_builds_go_latest_rejects_go_extra_match(self, mock_konflux_db, mock_get_github_client):
        """Test tag-build validation still requires GO_LATEST even when GO_EXTRA matches"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = Mock(decoded_content=b"vars:\n  GO_LATEST: 1.22\n  GO_EXTRA: 1.23\n")
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.23.9-1.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        with self.assertRaisesRegex(ValueError, r"--tag-builds.*\(1\.23\).*\(1\.22\)"):
            pipeline.validate_tag_builds_go_latest("openshift-4.16", {"GO_LATEST": "1.22", "GO_EXTRA": "1.23"}, "1.23")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_brew_login_when_logged_out(self, mock_konflux_db):
        """Test brew_login when session is logged out"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.logged_in = False

        pipeline.brew_login()

        pipeline.koji_session.gssapi_login.assert_called_once()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_brew_login_when_already_logged_in(self, mock_konflux_db):
        """Test brew_login when session is already logged in"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.logged_in = True

        pipeline.brew_login()

        pipeline.koji_session.gssapi_login.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_content_repo_url_suffix(self, mock_konflux_db):
        """Test get_content_repo_url_suffix returns plashet URL pattern"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        suffix = pipeline.get_content_repo_url_suffix(8)
        self.assertEqual(suffix, "/pub/RHOCP/plashets/4.16/stream/golang-el8/latest")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_builder_pullspec(self, mock_konflux_db):
        """Test stream-update pullspec uses the published registry.redhat.io form"""
        pipeline = self._make_pipeline(build_system="brew")

        builder_nvr = "openshift-golang-builder-container-v1.25.8-202604150744.p2.gf28329a.el9"
        pullspec = pipeline._get_builder_pullspec(builder_nvr)

        self.assertEqual(
            pullspec,
            "registry.redhat.io/openshift/art-images-base:"
            "openshift-golang-builder-container-v1.25.8-202604150744.p2.gf28329a.el9",
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_builder_pullspec_normalizes_konflux_nvr_name(self, mock_konflux_db):
        """Test stream-update pullspec normalizes Konflux NVRs to the published container name"""
        pipeline = self._make_pipeline(build_system="konflux")

        builder_nvr = "openshift-golang-builder-v1.25.8-202604150744.p2.gf28329a.el9"
        pullspec = pipeline._get_builder_pullspec(builder_nvr)

        self.assertEqual(
            pullspec,
            "registry.redhat.io/openshift/art-images-base:"
            "openshift-golang-builder-container-v1.25.8-202604150744.p2.gf28329a.el9",
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_builder_pullspec_rejects_non_golang_nvr(self, mock_konflux_db):
        """Test stream-update pullspec helper rejects non-golang image NVRs"""
        pipeline = self._make_pipeline(build_system="konflux")

        with self.assertRaisesRegex(ValueError, "Expected a golang builder image NVR"):
            pipeline._get_builder_pullspec("ose-cli-v4.16.0-202604150744.p2.gdeadbee.el9")

    @patch("pyartcd.pipelines.update_golang.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_ensure_builder_pullspec_available_reuses_oc_helper(self, mock_konflux_db, get_image_info):
        """Test published pullspec availability check reuses pyartcd.oc.get_image_info with quay auth"""
        pipeline = self._make_pipeline(build_system="konflux")

        pullspec = "registry.redhat.io/openshift/art-images-base:openshift-golang-builder-container-v1.25.8-test"
        quay_auth_file = str(Path(self.enterContext(tempfile.TemporaryDirectory())) / "quay-auth.json")

        with patch.dict(
            "os.environ",
            {"QUAY_AUTH_FILE": quay_auth_file},
            clear=False,
        ):
            await pipeline._ensure_builder_pullspec_available(pullspec)

        get_image_info.assert_awaited_once_with(pullspec, raise_if_not_found=True, registry_config=quay_auth_file)

    @patch("pyartcd.pipelines.update_golang.get_image_info", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_ensure_builder_pullspec_available_errors_when_oc_helper_fails(self, mock_konflux_db, get_image_info):
        """Test published pullspec availability check raises when pyartcd.oc.get_image_info fails"""
        pipeline = self._make_pipeline(build_system="konflux")

        pullspec = "registry.redhat.io/openshift/art-images-base:openshift-golang-builder-container-v1.25.8-test"
        get_image_info.side_effect = ValueError("Image pullspec is not found.")
        quay_auth_file = str(Path(self.enterContext(tempfile.TemporaryDirectory())) / "quay-auth.json")

        with patch.dict(
            "os.environ",
            {"QUAY_AUTH_FILE": quay_auth_file},
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "Published golang builder pullspec is not available"):
                await pipeline._ensure_builder_pullspec_available(pullspec)

    @patch("pyartcd.pipelines.update_golang.kinit", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.move_golang_bugs", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_run_brew_only_skips_updating_streams(self, mock_konflux_db, move_golang_bugs, mock_kinit):
        """Test brew-only runs skip streams.yml updates because streams use Konflux pullspecs"""
        pipeline = self._make_pipeline(build_system="brew")
        pipeline.validate_go_version_matches_group_vars = Mock(
            return_value=("openshift-4.16", {"GO_LATEST": "1.25"}, "1.25")
        )
        pipeline.validate_tag_builds_go_latest = Mock()
        pipeline.process_build = AsyncMock(return_value=True)
        pipeline._build_golang_plashets = AsyncMock()
        pipeline.get_existing_builders_brew = Mock(
            return_value={9: "openshift-golang-builder-container-v1.25.8-202604150744.p2.gf28329a.el9"}
        )
        pipeline.update_golang_streams = AsyncMock()

        await pipeline.run()

        mock_kinit.assert_awaited_once()
        pipeline.update_golang_streams.assert_not_awaited()
        move_golang_bugs.assert_awaited_once()
        slack_messages = [call.args[0] for call in pipeline._slack_client.say_in_thread.await_args_list]
        self.assertTrue(
            any("Skipping streams.yml update for brew-only run" in message for message in slack_messages),
            slack_messages,
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_module_tag(self, mock_konflux_db):
        """Test get_module_tag method"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.listTags.return_value = [
            {"name": "some-other-tag"},
            {"name": "module-go-toolset-rhel8-8090020240101-abcd1234"},
            {"name": "module-go-toolset-rhel8-8090020240101-abcd1234-build"},
        ]

        tag = pipeline.get_module_tag("golang-1.20.12-2.el8", 8)
        self.assertEqual(tag, "module-go-toolset-rhel8-8090020240101-abcd1234")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_module_tag_not_found(self, mock_konflux_db):
        """Test get_module_tag when no module tag is found"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.listTags.return_value = [
            {"name": "some-other-tag"},
        ]

        tag = pipeline.get_module_tag("golang-1.20.12-2.el8", 8)
        self.assertIsNone(tag)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.is_available", new_callable=AsyncMock, return_value=True)
    @patch("pyartcd.pipelines.update_golang.is_latest", return_value=True)
    @patch.object(UpdateGolangPipeline, "ensure_signed", new_callable=AsyncMock)
    async def test_process_build_already_latest_and_available(
        self, mock_ensure_signed, mock_is_latest, mock_is_available, mock_konflux_db
    ):
        """Test process_build when build is already latest and available"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        result = await pipeline.process_build(8, "golang-1.20.12-2.el8")

        self.assertTrue(result)
        mock_is_latest.assert_called_once_with("4.16", 8, "golang-1.20.12-2.el8", pipeline.koji_session)
        mock_is_available.assert_called_once_with("4.16", 8, "golang-1.20.12-2.el8")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.is_latest", return_value=False)
    @patch.object(UpdateGolangPipeline, "ensure_signed", new_callable=AsyncMock)
    async def test_process_build_not_latest_no_tag(self, mock_ensure_signed, mock_is_latest, mock_konflux_db):
        """Test process_build when build is not latest and tag_builds is False"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=False,  # Don't tag builds
        )

        result = await pipeline.process_build(8, "golang-1.20.12-2.el8")

        self.assertFalse(result)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.is_available", new_callable=AsyncMock, side_effect=[False, False, True])
    @patch("pyartcd.pipelines.update_golang.is_latest", return_value=True)
    @patch.object(UpdateGolangPipeline, "ensure_signed", new_callable=AsyncMock)
    async def test_process_build_available_after_retries(
        self, mock_ensure_signed, mock_is_latest, mock_is_available, mock_konflux_db
    ):
        """Test process_build succeeds after retrying request_repo"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )
        pipeline.request_repo = AsyncMock()

        result = await pipeline.process_build(8, "golang-1.20.12-2.el8")

        self.assertTrue(result)
        self.assertEqual(mock_is_available.call_count, 3)
        self.assertEqual(pipeline.request_repo.call_count, 2)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_tag_build_dry_run(self, mock_konflux_db):
        """Test tag_build in dry-run mode"""
        mock_runtime = Mock(
            dry_run=True,
            working_dir=Path("/tmp/working"),
        )
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime.new_slack_client.return_value = mock_slack

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.logged_in = True
        # Mock listTags to return empty list (no module tags for this test)
        pipeline.koji_session.listTags.return_value = []

        await pipeline.tag_build(8, "golang-1.20.12-2.el8")

        # Should not actually tag in dry-run
        pipeline.koji_session.tagBuild.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_tag_build_el9(self, mock_konflux_db):
        """Test tag_build for el9 (no module builds)"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime.new_slack_client.return_value = mock_slack

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.logged_in = True

        await pipeline.tag_build(9, "golang-1.20.12-2.el9")

        pipeline.koji_session.tagBuild.assert_called_once_with("rhaos-4.16-rhel-9-override", "golang-1.20.12-2.el9")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_tag_build_el8_with_module(self, mock_konflux_db):
        """Test tag_build for el8 with module builds"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime.new_slack_client.return_value = mock_slack

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.logged_in = True
        pipeline.koji_session.listTagged.return_value = [
            {"nvr": "delve-1.2.3-1.module_el8"},
            {"nvr": "go-toolset-1.20.12-1.module_el8"},
            {"nvr": "golang-1.20.12-2.el8"},
        ]

        # Mock get_module_tag to return a module tag
        with patch.object(pipeline, 'get_module_tag', return_value='module-go-toolset-rhel8-123'):
            await pipeline.tag_build(8, "golang-1.20.12-2.el8")

        # Should tag all 3 module builds into override
        self.assertEqual(pipeline.koji_session.tagBuild.call_count, 3)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("elliottlib.util.get_golang_container_nvrs_brew")
    def test_get_existing_builders_brew(self, mock_get_golang_nvrs, mock_konflux_db):
        """Test get_existing_builders_brew for Brew"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="brew",
        )

        pipeline.koji_session = Mock()
        pipeline.koji_session.getPackage.return_value = {"id": 12345}
        pipeline.koji_session.listBuilds.return_value = [
            {
                "name": "openshift-golang-builder-container",
                "version": "v1.20.12",
                "release": "202403212137.el8.g144a3f8",
                "nvr": "openshift-golang-builder-container-v1.20.12-202403212137.el8.g144a3f8",
            }
        ]

        mock_get_golang_nvrs.return_value = {
            "1.20.12-2.el8": [("openshift-golang-builder-container", "v1.20.12", "202403212137.el8.g144a3f8")]
        }

        el_nvr_map = {8: "golang-1.20.12-2.el8"}
        builder_nvrs = pipeline.get_existing_builders_brew(el_nvr_map, "1.20.12")

        self.assertEqual(builder_nvrs, {8: "openshift-golang-builder-container-v1.20.12-202403212137.el8.g144a3f8"})

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux(self, mock_get_golang_nvrs, mock_konflux_db_class):
        """Test Konflux builder lookup returns the build record on exact RPM match"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        # Create a mock KonfluxDb instance
        mock_db_instance = Mock()
        mock_konflux_db_class.return_value = mock_db_instance

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        mock_build_record = Mock(spec=KonfluxBuildRecord)
        mock_build_record.nvr = "openshift-golang-builder-v1.20.12-202403212137.el8.g144a3f8"

        async def mock_search_builds(*_args, **_kwargs):
            yield mock_build_record

        mock_db_instance.search_builds_by_fields = Mock(side_effect=mock_search_builds)

        mock_get_golang_nvrs.return_value = {
            "golang-1.20.12-2.el8": {("ignored-builder", "ignored-version", "ignored-release")}
        }

        el_nvr_map = {8: "golang-1.20.12-2.el8"}
        builder_nvrs = await pipeline.get_existing_builders_konflux(el_nvr_map, "1.20.12")

        self.assertEqual(builder_nvrs, {8: mock_build_record})
        mock_get_golang_nvrs.assert_called_once()
        self.assertEqual(mock_get_golang_nvrs.call_args.args[0], [mock_build_record])
        self.assertEqual(mock_get_golang_nvrs.call_args.kwargs, {"exact": True})

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux_el_suffix(self, mock_get_golang_nvrs, mock_konflux_db_class):
        """Test Konflux builder lookup rejects an installed RPM that differs from the expected NVR"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        mock_db_instance = Mock()
        mock_konflux_db_class.return_value = mock_db_instance

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.7-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        mock_build_record = Mock(spec=KonfluxBuildRecord)
        mock_build_record.nvr = "openshift-golang-builder-v1.25.7-202602170955.g5015a16.el9"

        async def mock_search_builds(*_args, **_kwargs):
            yield mock_build_record

        mock_db_instance.search_builds_by_fields = Mock(side_effect=mock_search_builds)

        mock_get_golang_nvrs.return_value = {
            "golang-1.25.7-1.el9_5": {("openshift-golang-builder", "v1.25.7", "202602170955.g5015a16.el9")}
        }

        el_nvr_map = {9: "golang-1.25.7-1.el9"}
        builder_nvrs = await pipeline.get_existing_builders_konflux(el_nvr_map, "1.25.7")

        self.assertEqual(builder_nvrs, {})

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_rebase_brew(self, mock_cmd_assert, mock_konflux_db):
        """Test _rebase_brew method for Brew"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            use_new_golang_branch=True,
        )

        await pipeline._rebase_brew(8, "1.20.12", "golang-1.20.12-2.el8")

        mock_cmd_assert.assert_called_once()
        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("doozer", cmd)
        self.assertIn("--var", cmd)
        self.assertIn("MAJOR=4", cmd)
        self.assertIn("MINOR=16", cmd)
        self.assertIn("--group", cmd)
        self.assertIn("golang", cmd)
        self.assertIn("images:rebase", cmd)
        self.assertIn("--version", cmd)
        self.assertIn("v1.20.12", cmd)
        self.assertIn("--push", cmd)
        self.assertIn("--extra-label", cmd)
        self.assertIn("io.openshift.build.golang-nvr=golang-1.20.12-2.el8", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_rebase_brew_dry_run(self, mock_cmd_assert, mock_konflux_db):
        """Test _rebase_brew method in dry-run mode"""
        mock_runtime = Mock(
            dry_run=True,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        await pipeline._rebase_brew(8, "1.20.12", "golang-1.20.12-2.el8")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertNotIn("--push", cmd)
        self.assertIn("--extra-label", cmd)
        self.assertIn("io.openshift.build.golang-nvr=golang-1.20.12-2.el8", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_build_brew(self, mock_cmd_assert, mock_konflux_db):
        """Test _build_brew method for Brew"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            scratch=True,
        )

        await pipeline._build_brew(8, "1.20.12")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("images:build", cmd)
        self.assertIn("--repo-type", cmd)
        self.assertIn("unsigned", cmd)
        self.assertIn("--push-to-defaults", cmd)
        self.assertIn("--scratch", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_rebase_konflux(self, mock_cmd_assert, mock_konflux_db):
        """Test _rebase_konflux method"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        await pipeline._rebase_konflux(8, "1.20.12", "golang-1.20.12-2.el8")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("beta:images:konflux:rebase", cmd)
        self.assertIn("--extra-label", cmd)
        self.assertIn("io.openshift.build.golang-nvr=golang-1.20.12-2.el8", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_rebase_konflux_dry_run(self, mock_cmd_assert, mock_konflux_db):
        """Test _rebase_konflux method in dry-run mode"""
        mock_runtime = Mock(
            dry_run=True,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        await pipeline._rebase_konflux(8, "1.20.12", "golang-1.20.12-2.el8")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertNotIn("--push", cmd)
        self.assertIn("--extra-label", cmd)
        self.assertIn("io.openshift.build.golang-nvr=golang-1.20.12-2.el8", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_build_konflux(self, mock_cmd_assert, mock_konflux_db):
        """Test _build_konflux method"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
            kubeconfig="/custom/kubeconfig",
        )

        await pipeline._build_konflux(8, "1.20.12")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("beta:images:konflux:build", cmd)
        self.assertIn("--konflux-kubeconfig", cmd)
        self.assertIn("/custom/kubeconfig", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_build_konflux_dry_run(self, mock_cmd_assert, mock_konflux_db):
        """Test _build_konflux method in dry-run mode"""
        mock_runtime = Mock(
            dry_run=True,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        await pipeline._build_konflux(8, "1.20.12")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("--dry-run", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_rebase_and_build_brew(self, mock_cmd_assert, mock_konflux_db):
        """Test _rebase_and_build_brew combines rebase and build"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        await pipeline._rebase_and_build_brew(8, "1.20.12", "golang-1.20.12-2.el8")

        # Should call both rebase and build
        self.assertEqual(mock_cmd_assert.call_count, 2)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_rebase_and_build_konflux(self, mock_cmd_assert, mock_konflux_db):
        """Test _rebase_and_build_konflux combines rebase and build for Konflux"""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        await pipeline._rebase_and_build_konflux(8, "1.20.12", "golang-1.20.12-2.el8")

        # Should call both rebase and build
        self.assertEqual(mock_cmd_assert.call_count, 2)


class TestShouldSignGolangRpm(unittest.TestCase):
    """Test the should_sign_golang_rpm method"""

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    def test_returns_true_when_config_set(self, mock_get_github_client, mock_konflux_db):
        mock_repo = Mock()
        mock_repo.get_contents.return_value = Mock(decoded_content=b"sign_golang_rpm: true\n")
        mock_get_github_client.return_value.get_repo.return_value = mock_repo

        pipeline = UpdateGolangPipeline(
            runtime=Mock(dry_run=False, working_dir=Path("/tmp/working")),
            ocp_version="4.18",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
            use_new_golang_branch=True,
        )

        result = pipeline.should_sign_golang_rpm(9, "1.22.9")

        self.assertTrue(result)
        mock_repo.get_contents.assert_called_with("group.yml", ref="golang")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    def test_returns_false_when_config_not_set(self, mock_get_github_client, mock_konflux_db):
        mock_repo = Mock()
        mock_repo.get_contents.return_value = Mock(decoded_content=b"name: rhel-9-golang-1.25\n")
        mock_get_github_client.return_value.get_repo.return_value = mock_repo

        pipeline = UpdateGolangPipeline(
            runtime=Mock(dry_run=False, working_dir=Path("/tmp/working")),
            ocp_version="4.22",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.3-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
            use_new_golang_branch=True,
        )

        result = pipeline.should_sign_golang_rpm(9, "1.25.3")

        self.assertFalse(result)
        mock_repo.get_contents.assert_called_with("group.yml", ref="golang")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    def test_returns_false_when_explicitly_false(self, mock_get_github_client, mock_konflux_db):
        mock_repo = Mock()
        mock_repo.get_contents.return_value = Mock(decoded_content=b"sign_golang_rpm: false\n")
        mock_get_github_client.return_value.get_repo.return_value = mock_repo

        pipeline = UpdateGolangPipeline(
            runtime=Mock(dry_run=False, working_dir=Path("/tmp/working")),
            ocp_version="4.22",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.3-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        result = pipeline.should_sign_golang_rpm(9, "1.25.3")

        self.assertFalse(result)


class TestEnsureSigned(IsolatedAsyncioTestCase):
    """Test the ensure_signed method"""

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_already_signed_skips(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=Mock(dry_run=False, working_dir=Path("/tmp/working")),
            ocp_version="4.18",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )
        pipeline.is_rpm_signed = Mock(return_value=True)
        pipeline.should_sign_golang_rpm = Mock()

        await pipeline.ensure_signed(9, "golang-1.22.9-1.el9")

        pipeline.should_sign_golang_rpm.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_unsigned_sustaining_tags_for_signing(self, mock_konflux_db):
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime = Mock(dry_run=False, working_dir=Path("/tmp/working"))
        mock_runtime.new_slack_client.return_value = mock_slack

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.18",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )
        pipeline.is_rpm_signed = Mock(return_value=False)
        pipeline.should_sign_golang_rpm = Mock(return_value=True)
        pipeline.koji_session = Mock(logged_in=True)

        await pipeline.ensure_signed(9, "golang-1.22.9-1.el9")

        pipeline.koji_session.tagBuild.assert_called_once_with("rhaos-4.18-rhel-9-golang", "golang-1.22.9-1.el9")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_unsigned_sustaining_dry_run(self, mock_konflux_db):
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime = Mock(dry_run=True, working_dir=Path("/tmp/working"))
        mock_runtime.new_slack_client.return_value = mock_slack

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.18",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )
        pipeline.is_rpm_signed = Mock(return_value=False)
        pipeline.should_sign_golang_rpm = Mock(return_value=True)
        pipeline.koji_session = Mock(logged_in=True)

        await pipeline.ensure_signed(9, "golang-1.22.9-1.el9")

        pipeline.koji_session.tagBuild.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_unsigned_rhel_golang_raises(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=Mock(dry_run=False, working_dir=Path("/tmp/working")),
            ocp_version="4.22",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.3-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )
        pipeline.is_rpm_signed = Mock(return_value=False)
        pipeline.should_sign_golang_rpm = Mock(return_value=False)

        with self.assertRaisesRegex(ValueError, "not signed.*RHEL"):
            await pipeline.ensure_signed(9, "golang-1.25.3-1.el9")


class TestIsRpmSigned(unittest.TestCase):
    """Test the is_rpm_signed static method"""

    def test_signed_rpm_found(self):
        parsed = {"name": "golang", "version": "1.22.9", "release": "1.el9"}
        with tempfile.TemporaryDirectory() as tmpdir:
            signed_dir = Path(tmpdir) / 'packages' / 'golang' / '1.22.9' / '1.el9' / 'data' / 'signed' / 'fd431d51'
            signed_dir.mkdir(parents=True)
            with patch("pyartcd.pipelines.update_golang.Path", return_value=Path(tmpdir)):
                result = UpdateGolangPipeline.is_rpm_signed(parsed)
            self.assertTrue(result)

    def test_unsigned_rpm(self):
        parsed = {"name": "golang", "version": "1.22.9", "release": "1.el9"}
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create the build path but no signed directory
            build_dir = Path(tmpdir) / 'packages' / 'golang' / '1.22.9' / '1.el9'
            build_dir.mkdir(parents=True)
            with patch("pyartcd.pipelines.update_golang.Path", return_value=Path(tmpdir)):
                result = UpdateGolangPipeline.is_rpm_signed(parsed)
            self.assertFalse(result)


class TestBuildGolangPlashets(IsolatedAsyncioTestCase):
    """Test the _build_golang_plashets method"""

    def _make_pipeline(self, use_new_golang_branch=False, dry_run=False):
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime = Mock(dry_run=dry_run, working_dir=Path("/tmp/working"))
        mock_runtime.new_slack_client.return_value = mock_slack
        return UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.18",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
            use_new_golang_branch=use_new_golang_branch,
        )

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_monobranch_triggers_jenkins_for_each_el_version(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline(use_new_golang_branch=True)
        mock_jenkins.start_build_plashets.return_value = "SUCCESS"

        await pipeline._build_golang_plashets("1.22.9", [8, 9])

        self.assertEqual(mock_jenkins.start_build_plashets.call_count, 2)
        calls = mock_jenkins.start_build_plashets.call_args_list
        self.assertEqual(calls[0].kwargs["group"], "golang")
        self.assertEqual(calls[0].kwargs["repos"], ["rhel-8-golang-rpms"])
        self.assertEqual(calls[0].kwargs["version"], "4.18")
        self.assertEqual(calls[1].kwargs["group"], "golang")
        self.assertEqual(calls[1].kwargs["repos"], ["rhel-9-golang-rpms"])
        self.assertEqual(calls[1].kwargs["version"], "4.18")

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_separated_branch_triggers_jenkins_per_el_and_go_version(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline(use_new_golang_branch=False)
        mock_jenkins.start_build_plashets.return_value = "SUCCESS"

        await pipeline._build_golang_plashets("1.22.9", [8, 9])

        self.assertEqual(mock_jenkins.start_build_plashets.call_count, 2)
        calls = mock_jenkins.start_build_plashets.call_args_list
        self.assertEqual(calls[0].kwargs["group"], "rhel-8-golang-1.22")
        self.assertEqual(calls[0].kwargs["repos"], ["rhel-8-golang-rpms"])
        self.assertIsNone(calls[0].kwargs["version"])
        self.assertEqual(calls[1].kwargs["group"], "rhel-9-golang-1.22")
        self.assertEqual(calls[1].kwargs["repos"], ["rhel-9-golang-rpms"])
        self.assertIsNone(calls[1].kwargs["version"])

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_raises_on_jenkins_failure(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline(use_new_golang_branch=True)
        mock_jenkins.start_build_plashets.return_value = "FAILURE"

        with self.assertRaisesRegex(RuntimeError, "failed with result: FAILURE"):
            await pipeline._build_golang_plashets("1.22.9", [9])

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_dry_run_passes_dry_run_flag(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline(use_new_golang_branch=True, dry_run=True)
        mock_jenkins.start_build_plashets.return_value = "SUCCESS"

        await pipeline._build_golang_plashets("1.22.9", [9])

        mock_jenkins.start_build_plashets.assert_called_once()
        self.assertTrue(mock_jenkins.start_build_plashets.call_args.kwargs["dry_run"])


class TestMonobranchDispatch(IsolatedAsyncioTestCase):
    """Test that branch-dependent methods dispatch correctly based on use_new_golang_branch"""

    def _make_pipeline(self, use_new_golang_branch):
        mock_runtime = Mock(dry_run=False, working_dir=Path("/tmp/working"))
        mock_runtime.new_slack_client.return_value = Mock()
        return UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.18",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.22.9-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
            use_new_golang_branch=use_new_golang_branch,
        )

    def test_doozer_group_uses_monobranch_when_flag_is_true(self):
        pipeline = self._make_pipeline(use_new_golang_branch=True)
        group, image_key = pipeline._get_doozer_group_and_image(9, "1.22.9")
        self.assertEqual(group, "golang")
        self.assertEqual(image_key, "openshift-golang-builder-1-22.rhel9")

    def test_doozer_group_uses_separated_branch_when_flag_is_false(self):
        pipeline = self._make_pipeline(use_new_golang_branch=False)
        group, image_key = pipeline._get_doozer_group_and_image(9, "1.22.9")
        self.assertEqual(group, "rhel-9-golang-1.22")
        self.assertEqual(image_key, "openshift-golang-builder")

    def test_doozer_var_args_with_monobranch(self):
        pipeline = self._make_pipeline(use_new_golang_branch=True)
        args = pipeline._get_doozer_var_args()
        self.assertEqual(args, ['--var', 'MAJOR=4', '--var', 'MINOR=18'])

    def test_doozer_var_args_with_separated_branch(self):
        pipeline = self._make_pipeline(use_new_golang_branch=False)
        args = pipeline._get_doozer_var_args()
        self.assertEqual(args, [])


if __name__ == "__main__":
    unittest.main()
