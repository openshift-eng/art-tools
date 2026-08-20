import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import ANY, AsyncMock, Mock, patch

import click
import koji
from artcommonlib.constants import GOLANG_BUILDER_IMAGE_NAME
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from pyartcd.pipelines.update_golang import (
    DEFAULT_GOLANG_ASSEMBLY,
    GOLANG_ASSEMBLIES,
    UpdateGolangPipeline,
    extract_and_validate_golang_nvrs,
    get_latest_nvr_in_tag,
    is_available,
    is_latest,
    is_latest_and_available,
    move_golang_bugs,
    update_golang,
)
from tenacity import wait_none


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

    def test_cli_only_offers_stream_type_assemblies(self):
        assembly_param = next(param for param in update_golang.params if param.name == "assembly")

        self.assertEqual(assembly_param.default, DEFAULT_GOLANG_ASSEMBLY)
        self.assertEqual(tuple(assembly_param.type.choices), GOLANG_ASSEMBLIES)
        with self.assertRaises(click.BadParameter):
            assembly_param.type.convert("art-1234", assembly_param, None)

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
    def test_existing_build_lookup_treats_legacy_builds_as_stream(self, mock_konflux_db):
        pipeline = self._make_pipeline(build_system="brew")

        self.assertTrue(pipeline._existing_build_matches_assembly("202608071200.p0.el9"))
        self.assertTrue(pipeline._existing_build_matches_assembly("202608071200.p0.assembly.stream.el9"))
        self.assertFalse(pipeline._existing_build_matches_assembly("202608071200.p0.assembly.test.el9"))

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_existing_test_build_lookup_requires_explicit_test_assembly(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.8-1.el9"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )

        self.assertFalse(pipeline._existing_build_matches_assembly("202608071200.p0.el9"))
        self.assertFalse(pipeline._existing_build_matches_assembly("202608071200.p0.assembly.stream.el9"))
        self.assertTrue(pipeline._existing_build_matches_assembly("202608071200.p0.assembly.test.el9"))

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_rejects_non_stream_type_assembly(self, mock_konflux_db):
        with self.assertRaisesRegex(ValueError, "Unsupported golang assembly"):
            UpdateGolangPipeline(
                runtime=self._make_test_runtime(),
                ocp_version="4.16",
                cves=None,
                force_update_tracker=False,
                go_nvrs=["golang-1.25.8-1.el9"],
                art_jira="ART-1234",
                tag_builds=False,
                build_system="brew",
                assembly="art-1234",
            )

    def test_test_assembly_rejects_brew_build_systems(self):
        for build_system in ("brew", "both"):
            with self.subTest(build_system=build_system):
                with self.assertLogs("pyartcd.pipelines.update_golang", level="ERROR") as logs:
                    with self.assertRaisesRegex(ValueError, "Brew floating tags are updated"):
                        UpdateGolangPipeline(
                            runtime=self._make_test_runtime(),
                            ocp_version="5.0",
                            cves=None,
                            force_update_tracker=False,
                            go_nvrs=["golang-1.26.5-1.el8"],
                            art_jira="ART-1234",
                            tag_builds=False,
                            build_system=build_system,
                            assembly="test",
                        )
                self.assertTrue(any("successful build" in message for message in logs.output), logs.output)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_get_doozer_var_args(self, mock_konflux_db):
        """Test _get_doozer_var_args returns --var args"""
        pipeline = self._make_pipeline()
        self.assertEqual(pipeline._get_doozer_var_args(), ['--var', 'MAJOR=4', '--var', 'MINOR=16'])

        pipeline.ocp_version = "5.0"
        self.assertEqual(pipeline._get_doozer_var_args(), ['--var', 'MAJOR=5', '--var', 'MINOR=0'])

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_monobranch_validation_requires_assemblies_enabled(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        repo = Mock()
        repo.get_contents.return_value = Mock(decoded_content=b"assemblies:\n  enabled: false\n")
        pipeline._get_ocp_build_data_repo_and_branch = Mock(return_value=(repo, "golang"))

        with self.assertRaisesRegex(ValueError, "Assemblies are not enabled.*golang"):
            pipeline.validate_go_version_matches_group_vars("1.25.8")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_monobranch_validation_accepts_assemblies_enabled(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        repo = Mock()
        repo.get_contents.return_value = Mock(decoded_content=b"assemblies:\n  enabled: true\n")
        pipeline._get_ocp_build_data_repo_and_branch = Mock(return_value=(repo, "golang"))
        pipeline._get_allowed_go_major_minors = Mock(return_value=("openshift-4.16", {"GO_LATEST": "1.25"}))

        result = pipeline.validate_go_version_matches_group_vars("1.25.8")

        self.assertEqual(result, ("openshift-4.16", {"GO_LATEST": "1.25"}, "1.25"))

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
        upstream_repo.get_contents.return_value = Mock(
            decoded_content=b"assemblies:\n  enabled: true\nvars:\n  GO_LATEST: 1.22\n"
        )
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
        self.assertEqual(sorted(requested_paths), ["group.yml", "group.yml", "streams.yml"])

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
        upstream_repo.get_contents.return_value = Mock(
            decoded_content=b"assemblies:\n  enabled: true\nvars:\n  GO_LATEST: 1.22\n  GO_EXTRA: 1.23\n"
        )
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
        upstream_repo.get_contents.return_value = Mock(
            decoded_content=b"assemblies:\n  enabled: true\nvars:\n  GO_LATEST: 1.20\n"
        )
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
        self.assertEqual(sorted(requested_paths), ["group.yml", "group.yml", "streams.yml"])

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
                if ref == "golang":
                    return Mock(decoded_content=b"assemblies:\n  enabled: true\n")
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
        self.assertEqual(requested_paths.count("group.yml"), 2)
        self.assertEqual(requested_paths.count("streams.yml"), 1)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_update_golang_streams_is_stream_only(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )
        pipeline._get_branch_content = Mock()

        await pipeline.update_golang_streams("1.26.5", {8: "registry.example.com/builder:test"})

        pipeline._get_branch_content.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_update_golang_streams_updates_go_extra_literal_streams(self, mock_konflux_db):
        """Test GO_EXTRA updates literal streams and other streams sharing the same builder"""
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.11-1.el8_10"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
        )
        pipeline.dry_run = True
        old_pullspec = "registry.example.com/golang-builder:v1.25.8-el8"
        new_pullspec = "registry.example.com/golang-builder:v1.25.11-el8"
        pipeline._branch_content = {
            "branch": "openshift-5.0",
            "repo": Mock(),
            "group": {"vars": {"GO_LATEST": "1.26", "GO_EXTRA": "1.25"}},
            "streams": {
                "rhel-8-golang": {
                    "aliases": ["rhel-8-golang-{GO_LATEST}"],
                    "image": "registry.example.com/golang-builder:v1.26.5-el8",
                },
                "rhel-8-golang-1.25": {"image": old_pullspec},
                "partner-rhel-8-golang-1.25": {"image": old_pullspec},
            },
        }

        await pipeline.update_golang_streams("1.25.11", {8: new_pullspec})

        streams = pipeline._branch_content["streams"]
        self.assertEqual(streams["rhel-8-golang-1.25"]["image"], new_pullspec)
        self.assertEqual(streams["partner-rhel-8-golang-1.25"]["image"], new_pullspec)
        self.assertEqual(
            streams["rhel-8-golang"]["image"],
            "registry.example.com/golang-builder:v1.26.5-el8",
        )

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
            decoded_content=b"assemblies:\n  enabled: true\nvars:\n  GO_LATEST: 1.22\n  GO_PREVIOUS: 1.21\n"
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
            decoded_content=(
                b"assemblies:\n  enabled: true\nvars:\n  GO_LATEST: 1.22\n  GO_EXTRA: 1.23\n  GO_PREVIOUS: 1.21\n"
            )
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
            decoded_content=b"assemblies:\n  enabled: true\nvars:\n  GO_LATEST: 1.22\n  GO_PREVIOUS: 1.21\n"
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
    def test_get_content_repo_url_suffix_uses_test_assembly(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )

        self.assertEqual(
            pipeline.get_content_repo_url_suffix(8),
            "/pub/RHOCP/plashets/5.0/test/golang-el8/latest",
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_verify_golang_builder_repo_formats_test_assembly(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )
        repo = Mock()
        repo.get_contents.return_value = Mock(
            decoded_content=b"""
repos:
  rhel-8-golang-rpms:
    conf:
      baseurl:
        x86_64: https://example.com/pub/RHOCP/plashets/{MAJOR}.{MINOR}/{runtime_assembly}/golang-el8/latest/x86_64/os/
"""
        )
        pipeline._get_ocp_build_data_repo_and_branch = Mock(return_value=(repo, "golang"))

        pipeline.verify_golang_builder_repo(8, "1.26.5")

        repo.get_contents.assert_called_once_with("group.yml", ref="golang")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    def test_verify_golang_builder_repo_error_mentions_assembly_template(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )
        repo = Mock()
        repo.get_contents.return_value = Mock(
            decoded_content=b"""
repos:
  rhel-8-golang-rpms:
    conf:
      baseurl:
        x86_64: https://example.com/pub/RHOCP/plashets/{MAJOR}.{MINOR}/stream/golang-el8/latest/x86_64/os/
"""
        )
        pipeline._get_ocp_build_data_repo_and_branch = Mock(return_value=(repo, "golang"))

        with self.assertRaisesRegex(ValueError, r"\{runtime_assembly\}.*test"):
            pipeline.verify_golang_builder_repo(8, "1.26.5")

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
        pipeline._reconcile_ci_images = AsyncMock()

        await pipeline.run()

        mock_kinit.assert_awaited_once()
        pipeline.update_golang_streams.assert_not_awaited()
        pipeline._reconcile_ci_images.assert_awaited_once()
        move_golang_bugs.assert_awaited_once()
        slack_messages = [call.args[0] for call in pipeline._slack_client.say_in_thread.await_args_list]
        self.assertTrue(
            any("Skipping streams.yml update for brew-only run" in message for message in slack_messages),
            slack_messages,
        )

    @patch("pyartcd.pipelines.update_golang.kinit", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.move_golang_bugs", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_run_test_assembly_skips_production_operations(self, mock_konflux_db, move_golang_bugs, mock_kinit):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )
        pipeline.validate_go_version_matches_group_vars = Mock(return_value=("golang", {"GO_LATEST": "1.26"}, "1.26"))
        pipeline.process_build = AsyncMock(return_value=True)
        pipeline._build_golang_plashets = AsyncMock()
        builder_record = Mock(nvr="openshift-golang-builder-container-v1.26.5-202608071200.p0.assembly.test.el8")
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={8: builder_record})
        pipeline._get_builder_pullspec = Mock()
        pipeline._ensure_builder_pullspec_available = AsyncMock()
        pipeline.update_golang_streams = AsyncMock()

        await pipeline.run()

        mock_kinit.assert_awaited_once()
        pipeline._build_golang_plashets.assert_awaited_once()
        self.assertEqual(pipeline._build_golang_plashets.await_args.args[0], "1.26.5")
        self.assertEqual(list(pipeline._build_golang_plashets.await_args.args[1]), [8])
        pipeline._get_builder_pullspec.assert_not_called()
        pipeline._ensure_builder_pullspec_available.assert_not_awaited()
        pipeline.update_golang_streams.assert_not_awaited()
        move_golang_bugs.assert_not_awaited()
        slack_messages = [call.args[0] for call in pipeline._slack_client.say_in_thread.await_args_list]
        self.assertTrue(any("test assembly" in message for message in slack_messages), slack_messages)

    @patch("pyartcd.pipelines.update_golang.kinit", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.move_golang_bugs", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_run_go_extra_reuses_builder_without_processing_rpm(
        self, mock_konflux_db, move_golang_bugs, mock_kinit
    ):
        """Test GO_EXTRA skips RPM buildroot processing and reuses an existing builder"""
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.11-1.el8_10"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
        )
        pipeline.validate_go_version_matches_group_vars = Mock(
            return_value=("openshift-5.0", {"GO_LATEST": "1.26", "GO_EXTRA": "1.25"}, "1.25")
        )
        pipeline.process_build = AsyncMock()
        pipeline._build_golang_plashets = AsyncMock()
        builder_record = Mock(nvr="openshift-golang-builder-container-v1.25.11-202607281030.p2.gbbb222.el8")
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={8: builder_record})
        pipeline._get_builder_pullspec = Mock(return_value="registry.example.com/golang-builder:v1.25.11-el8")
        pipeline._ensure_builder_pullspec_available = AsyncMock()
        pipeline.update_golang_streams = AsyncMock()
        pipeline._reconcile_ci_images = AsyncMock()

        await pipeline.run()

        pipeline.process_build.assert_not_awaited()
        pipeline._build_golang_plashets.assert_not_awaited()
        pipeline.get_existing_builders_konflux.assert_awaited_once_with(
            {8: "golang-1.25.11-1.el8_10"},
            "1.25.11",
        )
        pipeline.update_golang_streams.assert_awaited_once_with(
            "1.25.11",
            {8: "registry.example.com/golang-builder:v1.25.11-el8"},
        )
        pipeline._reconcile_ci_images.assert_awaited_once()
        move_golang_bugs.assert_awaited_once()

    @patch("pyartcd.pipelines.update_golang.kinit", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.move_golang_bugs", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_run_go_extra_fails_before_building_when_builder_is_missing(
        self, mock_konflux_db, move_golang_bugs, mock_kinit
    ):
        """Test GO_EXTRA without external RPMs never rebases a missing builder"""
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.11-1.el8_10"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
        )
        pipeline.validate_go_version_matches_group_vars = Mock(
            return_value=("openshift-5.0", {"GO_LATEST": "1.26", "GO_EXTRA": "1.25"}, "1.25")
        )
        pipeline.process_build = AsyncMock()
        pipeline._build_golang_plashets = AsyncMock()
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={})
        pipeline.verify_golang_builder_repo = Mock()
        pipeline._rebase_and_build_konflux = AsyncMock()

        with self.assertRaisesRegex(ValueError, "Cannot build missing non-GO_LATEST"):
            await pipeline.run()

        pipeline.process_build.assert_not_awaited()
        pipeline._build_golang_plashets.assert_not_awaited()
        pipeline.verify_golang_builder_repo.assert_not_called()
        pipeline._rebase_and_build_konflux.assert_not_awaited()
        move_golang_bugs.assert_not_awaited()

    @patch("pyartcd.pipelines.update_golang.kinit", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.move_golang_bugs", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_run_go_extra_external_rpms_builds_missing_builder(
        self, mock_konflux_db, move_golang_bugs, mock_kinit
    ):
        """Test external RPMs allow a missing GO_EXTRA builder to be rebased and built"""
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.11-1.el8_10"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            external_golang_rpms=True,
        )
        pipeline.validate_go_version_matches_group_vars = Mock(
            return_value=("openshift-5.0", {"GO_LATEST": "1.26", "GO_EXTRA": "1.25"}, "1.25")
        )
        pipeline.process_build = AsyncMock()
        pipeline._build_golang_plashets = AsyncMock()
        builder_record = Mock(nvr="openshift-golang-builder-container-v1.25.11-202607281030.p2.gbbb222.el8")
        pipeline.get_existing_builders_konflux = AsyncMock(side_effect=[{}, {8: builder_record}])
        pipeline.verify_golang_builder_repo = Mock()
        pipeline._rebase_and_build_konflux = AsyncMock()
        pipeline._get_builder_pullspec = Mock(return_value="registry.example.com/golang-builder:v1.25.11-el8")
        pipeline._ensure_builder_pullspec_available = AsyncMock()
        pipeline.update_golang_streams = AsyncMock()
        pipeline._reconcile_ci_images = AsyncMock()

        await pipeline.run()

        pipeline.process_build.assert_not_awaited()
        pipeline._build_golang_plashets.assert_not_awaited()
        pipeline.verify_golang_builder_repo.assert_not_called()
        pipeline._rebase_and_build_konflux.assert_awaited_once_with(
            8,
            "1.25.11",
            "golang-1.25.11-1.el8_10",
        )
        self.assertEqual(pipeline.get_existing_builders_konflux.await_count, 2)
        pipeline.update_golang_streams.assert_awaited_once()
        pipeline._reconcile_ci_images.assert_awaited_once()
        move_golang_bugs.assert_awaited_once()

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
    @patch(
        "artcommonlib.exectools.cmd_assert_async",
        new_callable=AsyncMock,
        side_effect=[ChildProcessError("connection timed out"), ChildProcessError("connection timed out"), 0],
    )
    async def test_request_repo_retries_command_failures(self, mock_cmd_assert, mock_konflux_db):
        mock_runtime = Mock(dry_run=False, working_dir=Path("/tmp/working"))
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
        original_wait = UpdateGolangPipeline.request_repo.retry.wait
        UpdateGolangPipeline.request_repo.retry.wait = wait_none()
        self.addCleanup(setattr, UpdateGolangPipeline.request_repo.retry, "wait", original_wait)

        await pipeline.request_repo(8, "golang-1.20.12-2.el8")

        self.assertEqual(mock_cmd_assert.call_count, 3)
        mock_cmd_assert.assert_called_with(
            "brew wait-repo rhaos-4.16-rhel-8-build --build=golang-1.20.12-2.el8 --request --verbose",
            log_stdout=True,
            timeout=3600,
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch(
        "artcommonlib.exectools.cmd_assert_async",
        new_callable=AsyncMock,
        side_effect=asyncio.TimeoutError,
    )
    async def test_request_repo_does_not_retry_python_timeout(self, mock_cmd_assert, mock_konflux_db):
        mock_runtime = Mock(dry_run=False, working_dir=Path("/tmp/working"))
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

        with self.assertRaises(asyncio.TimeoutError):
            await pipeline.request_repo(8, "golang-1.20.12-2.el8")

        mock_cmd_assert.assert_awaited_once()

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
    @patch("elliottlib.util.get_golang_container_nvrs_brew")
    def test_get_existing_builders_brew_ignores_non_stream_assembly(self, mock_get_golang_nvrs, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="brew",
        )
        pipeline.koji_session = Mock()
        pipeline.koji_session.getPackage.return_value = {"id": 12345}
        stream_nvr = "openshift-golang-builder-container-v1.20.12-2.assembly.stream.el8"
        named_nvr = "openshift-golang-builder-container-v1.20.12-1.assembly.art-1234.el8"
        pipeline.koji_session.listBuilds.return_value = [
            {
                "name": "openshift-golang-builder-container",
                "version": "v1.20.12",
                "release": "1.assembly.art-1234.el8",
                "nvr": named_nvr,
            },
            {
                "name": "openshift-golang-builder-container",
                "version": "v1.20.12",
                "release": "2.assembly.stream.el8",
                "nvr": stream_nvr,
            },
        ]
        mock_get_golang_nvrs.return_value = {
            "1.20.12-2.el8": [("openshift-golang-builder-container", "v1.20.12", "2.assembly.stream.el8")]
        }

        builder_nvrs = pipeline.get_existing_builders_brew({8: "golang-1.20.12-2.el8"}, "1.20.12")

        self.assertEqual(builder_nvrs, {8: stream_nvr})
        self.assertEqual(mock_get_golang_nvrs.call_count, 1)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux(self, mock_get_golang_nvrs, mock_konflux_db_class):
        """Test Konflux builder lookup falls back to the legacy name on exact RPM match"""
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

        async def mock_search_builds(*_args, **kwargs):
            if kwargs["where"]["name"] == GOLANG_BUILDER_IMAGE_NAME:
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
        self.assertEqual(
            [call.kwargs["where"]["name"] for call in mock_db_instance.search_builds_by_fields.call_args_list],
            [
                "openshift-golang-builder-1-20.rhel8",
                "openshift-golang-builder-1-20.rhel8",
                GOLANG_BUILDER_IMAGE_NAME,
            ],
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux_monobranch_names(self, mock_get_golang_nvrs, mock_konflux_db_class):
        """Test Konflux builder lookup uses the same names with the Golang monobranch enabled."""
        mock_runtime = Mock(
            dry_run=False,
            working_dir=Path("/tmp/working"),
        )
        mock_runtime.new_slack_client.return_value = Mock()

        mock_db_instance = Mock()
        mock_konflux_db_class.return_value = mock_db_instance
        mock_build_record = Mock(
            spec=KonfluxBuildRecord,
            nvr="openshift-golang-builder-container-v1.26.5-202607272002.p2.g5a9ab9d.el8",
        )
        mock_get_golang_nvrs.return_value = {
            "golang-1.26.5-1.el8": {("ignored-builder", "ignored-version", "ignored-release")}
        }

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8", "golang-1.26.5-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
        )

        async def mock_search_builds(*_args, **kwargs):
            if kwargs["where"]["name"] == "openshift-golang-builder-1-26.rhel8":
                yield mock_build_record

        mock_db_instance.search_builds_by_fields = Mock(side_effect=mock_search_builds)

        builder_records = await pipeline.get_existing_builders_konflux(
            {8: "golang-1.26.5-1.el8", 9: "golang-1.26.5-1.el9"},
            "1.26.5",
        )

        self.assertEqual(builder_records, {8: mock_build_record})
        self.assertEqual(mock_get_golang_nvrs.call_count, 1)
        self.assertEqual(
            [call.kwargs["where"]["name"] for call in mock_db_instance.search_builds_by_fields.call_args_list],
            [
                "openshift-golang-builder-1-26.rhel8",
                "openshift-golang-builder-1-26.rhel9",
                "openshift-golang-builder-1-26.rhel9",
                GOLANG_BUILDER_IMAGE_NAME,
                GOLANG_BUILDER_IMAGE_NAME,
            ],
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux_stream_falls_back_to_legacy_assembly(
        self, mock_get_golang_nvrs, mock_konflux_db_class
    ):
        mock_db_instance = Mock()
        mock_konflux_db_class.return_value = mock_db_instance
        legacy_record = Mock(
            spec=KonfluxBuildRecord,
            nvr="openshift-golang-builder-container-v1.26.5-202607272002.p2.g5a9ab9d.el8",
        )
        test_record = Mock(
            spec=KonfluxBuildRecord,
            nvr="openshift-golang-builder-container-v1.26.5-202607272003.p2.assembly.test.el8",
        )
        mock_get_golang_nvrs.return_value = {
            "golang-1.26.5-1.el8": {("ignored-builder", "ignored-version", "ignored-release")}
        }
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
        )

        async def mock_search_builds(*_args, **kwargs):
            if "assembly" not in kwargs["where"]:
                yield test_record
                yield legacy_record

        mock_db_instance.search_builds_by_fields = Mock(side_effect=mock_search_builds)

        records = await pipeline.get_existing_builders_konflux({8: "golang-1.26.5-1.el8"}, "1.26.5")

        self.assertEqual(records, {8: legacy_record})
        mock_get_golang_nvrs.assert_called_once_with([legacy_record], ANY, exact=True)
        self.assertEqual(
            [call.kwargs["where"].get("assembly") for call in mock_db_instance.search_builds_by_fields.call_args_list],
            ["stream", None],
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("pyartcd.pipelines.update_golang.elliottutil.get_golang_container_nvrs_for_konflux_record")
    async def test_get_existing_builders_konflux_test_scans_older_matching_builds(
        self, mock_get_golang_nvrs, mock_konflux_db_class
    ):
        mock_db_instance = Mock()
        mock_konflux_db_class.return_value = mock_db_instance
        newest_record = Mock(
            spec=KonfluxBuildRecord,
            nvr="openshift-golang-builder-container-v1.26.5-202608081200.p0.assembly.test.el8",
        )
        matching_record = Mock(
            spec=KonfluxBuildRecord,
            nvr="openshift-golang-builder-container-v1.26.5-202608071200.p0.assembly.test.el8",
        )

        def mock_installed_golang_nvrs(records, *_args, **_kwargs):
            installed_nvr = "golang-1.26.6-1.el8" if records[0] is newest_record else "golang-1.26.5-1.el8"
            return {installed_nvr: {("ignored-builder", "ignored-version", "ignored-release")}}

        mock_get_golang_nvrs.side_effect = mock_installed_golang_nvrs
        pipeline = UpdateGolangPipeline(
            runtime=self._make_test_runtime(),
            ocp_version="5.0",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.26.5-1.el8"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )

        async def mock_search_builds(*_args, **kwargs):
            self.assertEqual(kwargs["where"]["assembly"], "test")
            for record in [newest_record, matching_record][: kwargs["limit"]]:
                yield record

        mock_db_instance.search_builds_by_fields = Mock(side_effect=mock_search_builds)

        records = await pipeline.get_existing_builders_konflux({8: "golang-1.26.5-1.el8"}, "1.26.5")

        self.assertEqual(records, {8: matching_record})
        self.assertEqual(mock_get_golang_nvrs.call_count, 2)
        self.assertEqual(
            [call.args[0][0] for call in mock_get_golang_nvrs.call_args_list],
            [newest_record, matching_record],
        )
        self.assertEqual(mock_db_instance.search_builds_by_fields.call_args.kwargs["limit"], 50)
        self.assertEqual(mock_db_instance.search_builds_by_fields.call_count, 1)

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
        )

        await pipeline._rebase_brew(8, "1.20.12", "golang-1.20.12-2.el8")

        mock_cmd_assert.assert_called_once()
        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("doozer", cmd)
        self.assertEqual(cmd[cmd.index("--assembly") + 1], "stream")
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
        self.assertEqual(cmd[cmd.index("--assembly") + 1], "stream")
        self.assertIn("--repo-type", cmd)
        self.assertIn("unsigned", cmd)
        self.assertIn("--push-to-defaults", cmd)
        self.assertIn("--scratch", cmd)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_build_brew_dry_run(self, mock_cmd_assert, mock_konflux_db):
        runtime = self._make_test_runtime()
        runtime.dry_run = True
        pipeline = UpdateGolangPipeline(
            runtime=runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
        )

        await pipeline._build_brew(8, "1.20.12")

        cmd = mock_cmd_assert.call_args.args[0]
        self.assertIn("--dry-run", cmd)

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
            assembly="test",
        )

        await pipeline._rebase_konflux(8, "1.20.12", "golang-1.20.12-2.el8")

        cmd = mock_cmd_assert.call_args[0][0]
        self.assertIn("beta:images:konflux:rebase", cmd)
        self.assertEqual(cmd[cmd.index("--assembly") + 1], "test")
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
        self.assertEqual(cmd[cmd.index("--assembly") + 1], "stream")
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
    async def test_test_assembly_dry_run_commands_for_konflux(self, mock_cmd_assert, mock_konflux_db):
        runtime = self._make_test_runtime()
        runtime.dry_run = True
        pipeline = UpdateGolangPipeline(
            runtime=runtime,
            ocp_version="4.16",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.20.12-2.el8"],
            art_jira="ART-1234",
            tag_builds=True,
            build_system="konflux",
            assembly="test",
        )

        await pipeline._rebase_konflux(8, "1.20.12", "golang-1.20.12-2.el8")
        await pipeline._build_konflux(8, "1.20.12")

        konflux_rebase, konflux_build = [call.args[0] for call in mock_cmd_assert.call_args_list]
        for cmd in (konflux_rebase, konflux_build):
            self.assertEqual(cmd[cmd.index("--assembly") + 1], "test")
        self.assertNotIn("--push", konflux_rebase)
        self.assertNotIn("--dry-run", konflux_rebase)
        self.assertIn("--dry-run", konflux_build)

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
        pipeline.koji_session = Mock(logged_in=True)

        await pipeline.ensure_signed(9, "golang-1.22.9-1.el9")

        pipeline.koji_session.tagBuild.assert_not_called()

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
        pipeline.koji_session = Mock(logged_in=True)

        await pipeline.ensure_signed(9, "golang-1.22.9-1.el9")

        pipeline.koji_session.tagBuild.assert_not_called()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_unsigned_rhel_golang_tags_for_signing(self, mock_konflux_db):
        mock_slack = Mock()
        mock_slack.say_in_thread = AsyncMock()
        mock_runtime = Mock(dry_run=False, working_dir=Path("/tmp/working"))
        mock_runtime.new_slack_client.return_value = mock_slack

        pipeline = UpdateGolangPipeline(
            runtime=mock_runtime,
            ocp_version="4.22",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.3-1.el9"],
            art_jira="ART-1234",
            tag_builds=True,
        )
        pipeline.is_rpm_signed = Mock(return_value=False)
        pipeline.koji_session = Mock(logged_in=True)

        await pipeline.ensure_signed(9, "golang-1.25.3-1.el9")

        pipeline.koji_session.tagBuild.assert_called_once_with("rhaos-4.22-rhel-9-golang", "golang-1.25.3-1.el9")

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_non_stream_assembly_skips_signing(self, mock_konflux_db):
        pipeline = UpdateGolangPipeline(
            runtime=Mock(dry_run=False, working_dir=Path("/tmp/working")),
            ocp_version="4.22",
            cves=None,
            force_update_tracker=False,
            go_nvrs=["golang-1.25.3-1.el9"],
            art_jira="ART-1234",
            tag_builds=False,
            build_system="konflux",
            assembly="test",
        )
        pipeline.is_rpm_signed = Mock()
        pipeline.koji_session = Mock(logged_in=True)

        await pipeline.ensure_signed(9, "golang-1.25.3-1.el9")

        pipeline.is_rpm_signed.assert_not_called()
        pipeline.koji_session.tagBuild.assert_not_called()


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

    def _make_pipeline(self, dry_run=False, assembly=DEFAULT_GOLANG_ASSEMBLY):
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
            build_system="konflux",
            assembly=assembly,
        )

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_monobranch_triggers_jenkins_for_each_el_version(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline()
        mock_jenkins.start_build_plashets.return_value = "SUCCESS"

        await pipeline._build_golang_plashets("1.22.9", [8, 9])

        self.assertEqual(mock_jenkins.start_build_plashets.call_count, 2)
        calls = mock_jenkins.start_build_plashets.call_args_list
        self.assertEqual(calls[0].kwargs["group"], "golang")
        self.assertEqual(calls[0].kwargs["repos"], ["rhel-8-golang-rpms"])
        self.assertEqual(calls[0].kwargs["version"], "4.18")
        self.assertEqual(calls[0].kwargs["assembly"], "stream")
        self.assertEqual(calls[1].kwargs["group"], "golang")
        self.assertEqual(calls[1].kwargs["repos"], ["rhel-9-golang-rpms"])
        self.assertEqual(calls[1].kwargs["version"], "4.18")
        self.assertEqual(calls[1].kwargs["assembly"], "stream")

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_test_assembly_triggers_test_plashet(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline(assembly="test")
        mock_jenkins.start_build_plashets.return_value = "SUCCESS"

        await pipeline._build_golang_plashets("1.22.9", [9])

        mock_jenkins.start_build_plashets.assert_called_once()
        self.assertEqual(mock_jenkins.start_build_plashets.call_args.kwargs["assembly"], "test")

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_raises_on_jenkins_failure(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline()
        mock_jenkins.start_build_plashets.return_value = "FAILURE"

        with self.assertRaisesRegex(RuntimeError, "failed with result: FAILURE"):
            await pipeline._build_golang_plashets("1.22.9", [9])

    @patch("pyartcd.pipelines.update_golang.jenkins")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_dry_run_passes_dry_run_flag(self, mock_konflux_db, mock_jenkins):
        pipeline = self._make_pipeline(dry_run=True)
        mock_jenkins.start_build_plashets.return_value = "SUCCESS"

        await pipeline._build_golang_plashets("1.22.9", [9])

        mock_jenkins.start_build_plashets.assert_called_once()
        self.assertTrue(mock_jenkins.start_build_plashets.call_args.kwargs["dry_run"])


class TestReconcileCiImages(IsolatedAsyncioTestCase):
    """Test the CI golang builder / build-root image reconciliation stage"""

    def _make_pipeline(self, dry_run=False):
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
        )

    @staticmethod
    def _content(name):
        content = Mock()
        content.name = name
        return content

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_get_ci_image_keys_filters_by_prefix(self, mock_konflux_db, mock_get_github_client):
        pipeline = self._make_pipeline()
        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = [
            self._content("ci-openshift-golang-builder-latest.rhel9.yml"),
            self._content("ci-openshift-golang-builder-extra.rhel8.yml"),
            self._content("ci-openshift-build-root-latest.rhel9.yml"),
            self._content("openshift-enterprise-ansible-operator.yml"),
        ]
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        image_keys = await pipeline._get_ci_image_keys()

        self.assertEqual(
            image_keys,
            [
                "ci-openshift-build-root-latest.rhel9",
                "ci-openshift-golang-builder-extra.rhel8",
                "ci-openshift-golang-builder-latest.rhel9",
            ],
        )
        upstream_repo.get_contents.assert_called_once_with("images", ref="openshift-4.18")

    @patch("pyartcd.pipelines.update_golang.get_github_client_for_org")
    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_get_ci_image_keys_returns_empty_when_none_found(self, mock_konflux_db, mock_get_github_client):
        pipeline = self._make_pipeline()
        upstream_repo = Mock()
        upstream_repo.get_contents.return_value = [self._content("openshift-enterprise-ansible-operator.yml")]
        mock_get_github_client.return_value.get_repo.return_value = upstream_repo

        image_keys = await pipeline._get_ci_image_keys()

        self.assertEqual(image_keys, [])

    @staticmethod
    def _build_record(nvr, start_time):
        return Mock(nvr=nvr, start_time=start_time)

    # Common args for _reconcile_ci_images: a GO_LATEST bump on rhel9.
    RECONCILE_ARGS = ("1.22.9", "1.22", {"GO_LATEST": "1.22"}, {9: "golang-1.22.9-1.el9"})

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_noops_when_variant_unmatched(self, mock_konflux_db):
        """No GO_LATEST/GO_EXTRA/GO_PREVIOUS var matches this build; nothing to check."""
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock()
        pipeline._rebase_and_build_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images("1.23.1", "1.23", {"GO_LATEST": "1.22"}, {9: "golang"})

        pipeline._get_ci_image_keys.assert_not_awaited()
        pipeline._rebase_and_build_ci_images.assert_not_awaited()
        pipeline._slack_client.say_in_thread.assert_not_awaited()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_noops_when_no_matching_images_found(self, mock_konflux_db):
        """GO_LATEST matches, but no ci-openshift-golang-builder-latest.rhel* image exists."""
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=[])
        pipeline.get_existing_builders_konflux = AsyncMock()
        pipeline._rebase_and_build_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline.get_existing_builders_konflux.assert_not_awaited()
        pipeline._rebase_and_build_ci_images.assert_not_awaited()
        pipeline._slack_client.say_in_thread.assert_not_awaited()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_noops_when_ci_image_already_newer_than_builder(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=["ci-openshift-golang-builder-latest.rhel9"])
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 100)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(
            return_value=self._build_record("ci-openshift-golang-builder-latest-container-v4.18.0-1", 200)
        )
        pipeline._rebase_and_build_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline._rebase_and_build_ci_images.assert_not_awaited()
        pipeline._slack_client.say_in_thread.assert_not_awaited()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_rebuilds_image_older_than_builder(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=["ci-openshift-golang-builder-latest.rhel9"])
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 200)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(
            return_value=self._build_record("ci-openshift-golang-builder-latest-container-v4.18.0-1", 100)
        )
        pipeline._rebase_and_build_ci_images = AsyncMock()
        pipeline._sync_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline.get_existing_builders_konflux.assert_awaited_once_with({9: "golang-1.22.9-1.el9"}, "1.22.9")
        pipeline._rebase_and_build_ci_images.assert_awaited_once_with(["ci-openshift-golang-builder-latest.rhel9"])
        pipeline._sync_ci_images.assert_awaited_once_with(["ci-openshift-golang-builder-latest.rhel9"])
        self.assertTrue(
            any(
                "Rebuilding CI golang builder image" in call.args[0]
                for call in pipeline._slack_client.say_in_thread.await_args_list
            )
        )
        self.assertTrue(
            any("Synced CI image" in call.args[0] for call in pipeline._slack_client.say_in_thread.await_args_list)
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_rebuilds_image_never_built(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=["ci-openshift-golang-builder-latest.rhel9"])
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 200)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(return_value=None)
        pipeline._rebase_and_build_ci_images = AsyncMock()
        pipeline._sync_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline._rebase_and_build_ci_images.assert_awaited_once_with(["ci-openshift-golang-builder-latest.rhel9"])
        pipeline._sync_ci_images.assert_awaited_once_with(["ci-openshift-golang-builder-latest.rhel9"])

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_skips_when_no_builder_record_found(self, mock_konflux_db):
        """If there's no Konflux build of the golang-builder itself, we can't compare -- skip that image."""
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=["ci-openshift-golang-builder-latest.rhel9"])
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={})
        pipeline._find_latest_ci_build = AsyncMock()
        pipeline._rebase_and_build_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline._find_latest_ci_build.assert_not_awaited()
        pipeline._rebase_and_build_ci_images.assert_not_awaited()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_raises_when_rebuild_fails(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=["ci-openshift-golang-builder-latest.rhel9"])
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 200)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(return_value=None)
        pipeline._rebase_and_build_ci_images = AsyncMock(side_effect=RuntimeError("Failed to rebuild 1/1"))

        with self.assertRaisesRegex(RuntimeError, "Failed to rebuild"):
            await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_triggers_build_root_after_golang_builder_rebuild(self, mock_konflux_db):
        """
        Build-root images pull their parent via a `member` reference to the golang-builder CI
        image, not via Konflux staleness comparison, so once golang builder is rebuilt its
        build-root sibling is triggered unconditionally -- no extra Konflux lookup for build-root.
        """
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(
            return_value=["ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"]
        )
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 200)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(return_value=None)
        pipeline._rebase_and_build_ci_images = AsyncMock()
        pipeline._sync_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        # _find_latest_ci_build is only used to check golang-builder staleness; build-root is
        # triggered directly, without a Konflux lookup of its own.
        pipeline._find_latest_ci_build.assert_awaited_once_with("ci-openshift-golang-builder-latest.rhel9", 9)
        # Golang builder and build-root are rebuilt as two separate, ordered batch calls.
        self.assertEqual(
            [call.args[0] for call in pipeline._rebase_and_build_ci_images.await_args_list],
            [["ci-openshift-golang-builder-latest.rhel9"], ["ci-openshift-build-root-latest.rhel9"]],
        )
        # Both families are synced together in a single call, after both rebuild steps complete.
        pipeline._sync_ci_images.assert_awaited_once_with(
            ["ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"]
        )
        slack_messages = [call.args[0] for call in pipeline._slack_client.say_in_thread.await_args_list]
        self.assertTrue(any("Rebuilding CI golang builder image" in m for m in slack_messages))
        self.assertTrue(any("Rebuilding CI build-root image" in m for m in slack_messages))
        self.assertTrue(any("Synced CI image" in m for m in slack_messages))

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_skips_build_root_when_golang_builder_already_fresh(self, mock_konflux_db):
        """If golang builder wasn't rebuilt, its build-root sibling is left untouched."""
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(
            return_value=["ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"]
        )
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 100)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(
            return_value=self._build_record("ci-openshift-golang-builder-latest-container-v4.18.0-1", 200)
        )
        pipeline._rebase_and_build_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline._rebase_and_build_ci_images.assert_not_awaited()
        pipeline._slack_client.say_in_thread.assert_not_awaited()

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_skips_build_root_when_none_defined_for_variant(self, mock_konflux_db):
        """Golang builder rebuilds fine even when no matching build-root image is defined."""
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(return_value=["ci-openshift-golang-builder-latest.rhel9"])
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 200)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(return_value=None)
        pipeline._rebase_and_build_ci_images = AsyncMock()
        pipeline._sync_ci_images = AsyncMock()

        await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        pipeline._rebase_and_build_ci_images.assert_awaited_once_with(["ci-openshift-golang-builder-latest.rhel9"])
        pipeline._sync_ci_images.assert_awaited_once_with(["ci-openshift-golang-builder-latest.rhel9"])

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_reconcile_raises_when_build_root_rebuild_fails(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._get_ci_image_keys = AsyncMock(
            return_value=["ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"]
        )
        builder_record = self._build_record("openshift-golang-builder-container-v1.22.9-1.el9", 200)
        pipeline.get_existing_builders_konflux = AsyncMock(return_value={9: builder_record})
        pipeline._find_latest_ci_build = AsyncMock(return_value=None)
        pipeline._rebase_and_build_ci_images = AsyncMock(
            side_effect=[None, RuntimeError("Failed to rebuild 1/1")],
        )
        pipeline._sync_ci_images = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "Failed to rebuild"):
            await pipeline._reconcile_ci_images(*self.RECONCILE_ARGS)

        # Sync only happens after both rebuild stages complete, so a build-root failure means
        # nothing gets mirrored to CI this run -- even though golang-builder itself rebuilt fine.
        pipeline._sync_ci_images.assert_not_awaited()


class TestRebaseAndBuildCiImages(IsolatedAsyncioTestCase):
    """Test the low-level rebase+build batch helper used by CI image reconciliation"""

    def _make_pipeline(self):
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
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_rebases_and_builds_each_image(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._rebase_ci_image = AsyncMock()
        pipeline._build_ci_image = AsyncMock()

        await pipeline._rebase_and_build_ci_images(
            ["ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"]
        )

        self.assertEqual(
            {call.args[0] for call in pipeline._rebase_ci_image.await_args_list},
            {"ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"},
        )
        self.assertEqual(
            {call.args[0] for call in pipeline._build_ci_image.await_args_list},
            {"ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"},
        )

    @patch("pyartcd.pipelines.update_golang.KonfluxDb")
    async def test_raises_aggregated_error_when_some_images_fail(self, mock_konflux_db):
        pipeline = self._make_pipeline()
        pipeline._rebase_ci_image = AsyncMock()

        async def _build_side_effect(image_key):
            if image_key == "ci-openshift-build-root-latest.rhel9":
                raise ChildProcessError("build failed")

        pipeline._build_ci_image = AsyncMock(side_effect=_build_side_effect)

        with self.assertRaisesRegex(RuntimeError, r"Failed to rebuild 1/2 CI image\(s\)"):
            await pipeline._rebase_and_build_ci_images(
                ["ci-openshift-golang-builder-latest.rhel9", "ci-openshift-build-root-latest.rhel9"]
            )


class TestMonobranchDispatch(IsolatedAsyncioTestCase):
    """Test that doozer group and image methods work with monobranch"""

    def _make_pipeline(self):
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
        )

    def test_doozer_group_uses_monobranch(self):
        pipeline = self._make_pipeline()
        group, image_key = pipeline._get_doozer_group_and_image(9, "1.22.9")
        self.assertEqual(group, "golang")
        self.assertEqual(image_key, "openshift-golang-builder-1-22.rhel9")

    def test_doozer_var_args_with_monobranch(self):
        pipeline = self._make_pipeline()
        args = pipeline._get_doozer_var_args()
        self.assertEqual(args, ['--var', 'MAJOR=4', '--var', 'MINOR=18'])


if __name__ == "__main__":
    unittest.main()
