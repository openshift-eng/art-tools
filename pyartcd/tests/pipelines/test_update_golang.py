import os
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call, patch

import click
import koji
from artcommonlib.brew import BuildStates
from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from pyartcd.pipelines.update_golang import (
    UpdateGolangPipeline,
    extract_and_validate_golang_nvrs,
    get_latest_nvr_in_tag,
    is_latest_and_available,
    is_latest_build,
    move_golang_bugs,
)

from pyartcd import constants


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

    def test_unsupported_ocp_major_version(self):
        """Test with unsupported OCP major version"""
        with self.assertRaisesRegex(ValueError, "Only OCP major version 4 is supported"):
            extract_and_validate_golang_nvrs("5.16", ["golang-1.20.12-2.el8"])

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


class TestIsLatestBuild(unittest.TestCase):
    """Test the is_latest_build function"""

    def test_build_is_latest(self):
        """Test when build is the latest"""
        mock_koji_session = Mock()
        mock_koji_session.getLatestBuilds.return_value = [{"nvr": "golang-1.20.12-2.el8"}]

        result = is_latest_build("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)

        self.assertTrue(result)
        mock_koji_session.getLatestBuilds.assert_called_once_with("rhaos-4.16-rhel-8-build", package="golang")

    def test_build_is_not_latest(self):
        """Test when build is not the latest"""
        mock_koji_session = Mock()
        mock_koji_session.getLatestBuilds.return_value = [{"nvr": "golang-1.20.13-1.el8"}]

        result = is_latest_build("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)

        self.assertFalse(result)

    def test_no_latest_build_found(self):
        """Test when no latest build is found in tag"""
        mock_koji_session = Mock()
        mock_koji_session.getLatestBuilds.return_value = []

        with self.assertRaisesRegex(ValueError, "Cannot find latest golang build"):
            is_latest_build("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)


class TestIsLatestAndAvailable(IsolatedAsyncioTestCase):
    """Test the is_latest_and_available async function"""

    @patch("pyartcd.pipelines.update_golang.is_latest_build")
    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_build_is_latest_and_available(self, mock_cmd_gather, mock_is_latest):
        """Test when build is latest and available"""
        mock_is_latest.return_value = True
        mock_cmd_gather.return_value = (0, "", "")
        mock_koji_session = Mock()

        result = await is_latest_and_available("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)

        self.assertTrue(result)
        mock_is_latest.assert_called_once_with("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)
        mock_cmd_gather.assert_called_once()

    @patch("pyartcd.pipelines.update_golang.is_latest_build")
    async def test_build_is_not_latest(self, mock_is_latest):
        """Test when build is not latest"""
        mock_is_latest.return_value = False
        mock_koji_session = Mock()

        result = await is_latest_and_available("4.16", 8, "golang-1.20.12-2.el8", mock_koji_session)

        self.assertFalse(result)

    @patch("pyartcd.pipelines.update_golang.is_latest_build")
    @patch("artcommonlib.exectools.cmd_gather_async")
    async def test_build_is_latest_but_not_available(self, mock_cmd_gather, mock_is_latest):
        """Test when build is latest but not available in repo"""
        mock_is_latest.return_value = True
        mock_cmd_gather.return_value = (1, "", "timeout")
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
        mock_cmd_assert.assert_called_once_with(expected_cmd)

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

    def test_init_missing_github_token(self):
        """Test initialization fails without GITHUB_TOKEN"""
        del os.environ["GITHUB_TOKEN"]
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        with self.assertRaisesRegex(ValueError, "GITHUB_TOKEN environment variable is required"):
            UpdateGolangPipeline(
                runtime=mock_runtime,
                ocp_version="4.16",
                cves=None,
                force_update_tracker=False,
                go_nvrs=["golang-1.20.12-2.el8"],
                art_jira="ART-1234",
                tag_builds=True,
            )

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

    def test_get_golang_branch(self):
        """Test get_golang_branch static method"""
        branch = UpdateGolangPipeline.get_golang_branch(8, "1.20.12")
        self.assertEqual(branch, "rhel-8-golang-1.20")

        branch = UpdateGolangPipeline.get_golang_branch(9, "1.21.5")
        self.assertEqual(branch, "rhel-9-golang-1.21")

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
    def test_get_content_repo_url(self, mock_konflux_db):
        """Test get_content_repo_url method"""
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

        url = pipeline.get_content_repo_url(8)
        self.assertEqual(
            url, "https://download-node-02.eng.bos.redhat.com/brewroot/repos/rhaos-4.16-rhel-8-build/latest"
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
    @patch("pyartcd.pipelines.update_golang.is_latest_and_available")
    async def test_process_build_already_latest_and_available(self, mock_is_latest, mock_konflux_db):
        """Test process_build when build is already latest and available"""
        mock_is_latest.return_value = True
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

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.is_latest_and_available")
    async def test_process_build_not_latest_no_tag(self, mock_is_latest, mock_konflux_db):
        """Test process_build when build is not latest and tag_builds is False"""
        mock_is_latest.return_value = False
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

        # Should tag all module builds
        self.assertEqual(pipeline.koji_session.tagBuild.call_count, 3)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("elliottlib.util.get_golang_container_nvrs_brew")
    def test_get_existing_builders(self, mock_get_golang_nvrs, mock_konflux_db):
        """Test get_existing_builders for Brew"""
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
        builder_nvrs = pipeline.get_existing_builders(el_nvr_map, "1.20.12")

        self.assertEqual(builder_nvrs, {8: "openshift-golang-builder-container-v1.20.12-202403212137.el8.g144a3f8"})

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux(self, mock_get_golang_nvrs, mock_konflux_db_class):
        """Test get_existing_builders_konflux for Konflux"""
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

        # Mock the build record
        mock_build_record = Mock(spec=KonfluxBuildRecord)

        # Mock the async generator
        async def mock_search_builds(*args, **kwargs):
            yield mock_build_record

        mock_db_instance.search_builds_by_fields = Mock(side_effect=mock_search_builds)

        mock_get_golang_nvrs.return_value = {
            "1.20.12-2.el8": [("openshift-golang-builder", "v1.20.12", "202403212137.el8.g144a3f8")]
        }

        el_nvr_map = {8: "golang-1.20.12-2.el8"}
        builder_nvrs = await pipeline.get_existing_builders_konflux(el_nvr_map, "1.20.12")

        self.assertEqual(builder_nvrs, {8: "openshift-golang-builder-v1.20.12-202403212137.el8.g144a3f8"})

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
        )

        await pipeline._rebase_brew(8, "1.20.12")

        mock_cmd_assert.assert_called_once()
        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("doozer", cmd)
        self.assertIn("--group", cmd)
        self.assertIn("rhel-8-golang-1.20", cmd)
        self.assertIn("images:rebase", cmd)
        self.assertIn("--version", cmd)
        self.assertIn("v1.20.12", cmd)
        self.assertIn("--push", cmd)

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

        await pipeline._rebase_brew(8, "1.20.12")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertNotIn("--push", cmd)

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

        await pipeline._rebase_konflux(8, "1.20.12")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("beta:images:konflux:rebase", cmd)

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

        await pipeline._rebase_konflux(8, "1.20.12")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertNotIn("--push", cmd)

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

        await pipeline._rebase_and_build_brew(8, "1.20.12")

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

        await pipeline._rebase_and_build_konflux(8, "1.20.12")

        # Should call both rebase and build
        self.assertEqual(mock_cmd_assert.call_count, 2)


if __name__ == "__main__":
    unittest.main()
