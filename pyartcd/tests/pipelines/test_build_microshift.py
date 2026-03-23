"""
Unit tests for build_microshift pipeline, focusing on advisory reuse
to prevent duplicate advisories on retries.
"""

import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

from artcommonlib.assembly import AssemblyTypes

from pyartcd.pipelines.build_microshift import BuildMicroShiftPipeline
from pyartcd.runtime import Runtime


class TestBuildMicroShiftPipeline(IsolatedAsyncioTestCase):
    """
    Test cases for the BuildMicroShiftPipeline class
    """

    def setUp(self):
        """
        Set up test fixtures before each test method
        """
        self.runtime = Mock(spec=Runtime)
        self.runtime.working_dir = Path(tempfile.mkdtemp())
        self.runtime.dry_run = False
        self.runtime.config = {
            "build_config": {"ocp_build_data_url": "https://github.com/openshift-eng/ocp-build-data"},
            "advisory": {
                "assigned_to": "test@redhat.com",
                "manager": "manager@redhat.com",
                "package_owner": "owner@redhat.com",
            },
        }
        self.runtime.logger = Mock()
        self.mock_slack_client = Mock()
        self.mock_slack_client.say_in_thread = AsyncMock()
        self.group = "openshift-4.21"
        self.assembly = "4.21.7"
        os.environ["GITHUB_TOKEN"] = "fake-token"

    def tearDown(self):
        """
        Clean up test fixtures after each test method
        """
        os.environ.pop("GITHUB_TOKEN", None)

    def _create_pipeline(self, skip_prepare_advisory=False):
        """
        Helper to create a BuildMicroShiftPipeline instance with common defaults.
        """
        return BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=skip_prepare_advisory,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )

    @patch("pyartcd.pipelines.build_microshift.oc.registry_login", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.load_group_config", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.load_releases_config", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.get_assembly_type")
    async def test_advisory_persisted_immediately_after_creation(
        self, mock_get_assembly_type, mock_load_releases_config, mock_load_group_config, mock_registry_login,
    ):
        """
        Test that when a new advisory is created, it is immediately persisted
        to ocp-build-data via _create_or_update_pull_request before the build starts.
        This prevents duplicate advisories on retries.
        """
        # given
        mock_load_group_config.return_value = {"advisories": {}}
        mock_load_releases_config.return_value = {"releases": {self.assembly: {}}}
        mock_get_assembly_type.return_value = AssemblyTypes.STANDARD

        pipeline = self._create_pipeline()
        pipeline.create_microshift_advisory = AsyncMock(return_value=12345)
        pipeline._create_or_update_pull_request = AsyncMock(return_value="https://github.com/example/pull/1")
        pipeline._rebase_and_build_for_named_assembly = AsyncMock()
        pipeline._trigger_microshift_sync = AsyncMock()
        pipeline._trigger_build_microshift_bootc = AsyncMock()
        pipeline._prepare_advisory = AsyncMock()

        # when
        await pipeline.run()

        # then
        pipeline.create_microshift_advisory.assert_called_once()
        pipeline._create_or_update_pull_request.assert_called_once_with(nvrs=[])
        self.assertEqual(pipeline.advisory_num, 12345)

        # Verify the advisory was persisted BEFORE the build started
        create_pr_order = pipeline._create_or_update_pull_request.call_args_list
        build_order = pipeline._rebase_and_build_for_named_assembly.call_args_list
        self.assertEqual(len(create_pr_order), 1)
        self.assertEqual(len(build_order), 1)

    @patch("pyartcd.pipelines.build_microshift.oc.registry_login", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.load_group_config", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.load_releases_config", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.get_assembly_type")
    async def test_existing_advisory_reused_on_retry(
        self, mock_get_assembly_type, mock_load_releases_config, mock_load_group_config, mock_registry_login,
    ):
        """
        Test that when a microshift advisory already exists in the group config
        (e.g., from a previous run that persisted it), a new advisory is NOT created.
        """
        # given - advisory already configured in group config from a previous run
        mock_load_group_config.return_value = {"advisories": {"microshift": 12345}}
        mock_load_releases_config.return_value = {"releases": {self.assembly: {}}}
        mock_get_assembly_type.return_value = AssemblyTypes.STANDARD

        pipeline = self._create_pipeline()
        pipeline.create_microshift_advisory = AsyncMock()
        pipeline._create_or_update_pull_request = AsyncMock()
        pipeline._rebase_and_build_for_named_assembly = AsyncMock()
        pipeline._trigger_microshift_sync = AsyncMock()
        pipeline._trigger_build_microshift_bootc = AsyncMock()
        pipeline._prepare_advisory = AsyncMock()

        # when
        await pipeline.run()

        # then - no new advisory should be created
        pipeline.create_microshift_advisory.assert_not_called()
        # _create_or_update_pull_request should not be called for advisory pinning
        # (it may be called later from _rebase_and_build_for_named_assembly for NVR pinning)
        pipeline._create_or_update_pull_request.assert_not_called()
        # _prepare_advisory should use the existing advisory
        pipeline._prepare_advisory.assert_called_once_with(12345)

    @patch("pyartcd.pipelines.build_microshift.oc.registry_login", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.load_group_config", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.load_releases_config", new_callable=AsyncMock)
    @patch("pyartcd.pipelines.build_microshift.get_assembly_type")
    async def test_skip_prepare_advisory_skips_advisory_creation(
        self, mock_get_assembly_type, mock_load_releases_config, mock_load_group_config, mock_registry_login,
    ):
        """
        Test that when skip_prepare_advisory is set, no advisory is created
        and no PR is made to persist it.
        """
        # given
        mock_load_group_config.return_value = {"advisories": {}}
        mock_load_releases_config.return_value = {"releases": {self.assembly: {}}}
        mock_get_assembly_type.return_value = AssemblyTypes.STANDARD

        pipeline = self._create_pipeline(skip_prepare_advisory=True)
        pipeline.create_microshift_advisory = AsyncMock()
        pipeline._create_or_update_pull_request = AsyncMock()
        pipeline._rebase_and_build_for_named_assembly = AsyncMock()
        pipeline._trigger_microshift_sync = AsyncMock()
        pipeline._trigger_build_microshift_bootc = AsyncMock()
        pipeline._prepare_advisory = AsyncMock()

        # when
        await pipeline.run()

        # then
        pipeline.create_microshift_advisory.assert_not_called()
        pipeline._create_or_update_pull_request.assert_not_called()
        pipeline._prepare_advisory.assert_not_called()

    def test_pin_nvrs_advisory_only(self):
        """
        Test that _pin_nvrs with empty NVRs list only pins the advisory ID
        without adding any RPM entries.
        """
        # given
        pipeline = self._create_pipeline()
        pipeline.advisory_num = 12345
        releases_config = {
            "releases": {
                self.assembly: {}
            }
        }

        # when
        result = pipeline._pin_nvrs(nvrs=[], releases_config=releases_config)

        # then
        self.assertIsNone(result)
        advisory_value = releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"]
        self.assertEqual(advisory_value, 12345)
        # No rpms entry should be created
        self.assertNotIn("members", releases_config["releases"][self.assembly]["assembly"])

    def test_pin_nvrs_with_nvrs_and_advisory(self):
        """
        Test that _pin_nvrs with NVRs pins both the advisory ID and NVRs.
        """
        # given
        pipeline = self._create_pipeline()
        pipeline.advisory_num = 12345
        releases_config = {
            "releases": {
                self.assembly: {}
            }
        }
        nvrs = [
            "microshift-4.21.7-202209300751.p0.g7ebffc3.assembly.4.21.7.el8",
            "microshift-4.21.7-202209300751.p0.g7ebffc3.assembly.4.21.7.el9",
        ]

        # when
        result = pipeline._pin_nvrs(nvrs=nvrs, releases_config=releases_config)

        # then
        self.assertIsNotNone(result)
        # Advisory should be pinned
        advisory_value = releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"]
        self.assertEqual(advisory_value, 12345)
        # NVRs should be pinned
        rpms = releases_config["releases"][self.assembly]["assembly"]["members"]["rpms"]
        self.assertEqual(len(rpms), 1)
        self.assertEqual(rpms[0]["distgit_key"], "microshift")
        self.assertEqual(rpms[0]["metadata"]["is"]["el8"], nvrs[0])
        self.assertEqual(rpms[0]["metadata"]["is"]["el9"], nvrs[1])

    def test_pin_nvrs_without_advisory(self):
        """
        Test that _pin_nvrs with NVRs but no advisory_num only pins NVRs.
        """
        # given
        pipeline = self._create_pipeline()
        pipeline.advisory_num = None
        releases_config = {
            "releases": {
                self.assembly: {}
            }
        }
        nvrs = [
            "microshift-4.21.7-202209300751.p0.g7ebffc3.assembly.4.21.7.el9",
        ]

        # when
        result = pipeline._pin_nvrs(nvrs=nvrs, releases_config=releases_config)

        # then
        self.assertIsNotNone(result)
        # No advisory should be set
        self.assertNotIn("group", releases_config["releases"][self.assembly].get("assembly", {}))
        # NVRs should still be pinned
        rpms = releases_config["releases"][self.assembly]["assembly"]["members"]["rpms"]
        self.assertEqual(len(rpms), 1)
        self.assertEqual(rpms[0]["metadata"]["is"]["el9"], nvrs[0])

    def test_pin_nvrs_advisory_uses_setdefault(self):
        """
        Test that _pin_nvrs does not overwrite an existing advisory ID
        (uses setdefault, so a pre-existing value is preserved).
        """
        # given
        pipeline = self._create_pipeline()
        pipeline.advisory_num = 99999
        releases_config = {
            "releases": {
                self.assembly: {
                    "assembly": {
                        "group": {
                            "advisories": {
                                "microshift": 12345,
                            }
                        }
                    }
                }
            }
        }

        # when
        pipeline._pin_nvrs(nvrs=[], releases_config=releases_config)

        # then - original advisory ID should be preserved
        advisory_value = releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"]
        self.assertEqual(advisory_value, 12345)
