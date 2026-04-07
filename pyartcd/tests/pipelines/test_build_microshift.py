"""
Unit tests for build_microshift pipeline
"""

import os
import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import Mock

from pyartcd.pipelines.build_microshift import BuildMicroShiftPipeline
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient


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
        self.runtime.config = {}
        self.runtime.logger = Mock()
        self.mock_slack_client = Mock(spec=SlackClient)
        self.group = "openshift-4.17"
        self.assembly = "4.17.1"
        os.environ["GITHUB_TOKEN"] = "fake-token"

    def tearDown(self):
        """
        Clean up test fixtures after each test method
        """
        os.environ.pop("GITHUB_TOKEN", None)

    def test_pin_nvrs_no_existing_advisory(self):
        """
        Test that when no advisory exists in releases config, the new advisory is saved
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 12345

        releases_config = {"releases": {self.assembly: {}}}

        # when
        pipeline._pin_nvrs(nvrs=None, releases_config=releases_config)

        # then
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"], 12345
        )
        self.assertEqual(pipeline.advisory_num, 12345, "advisory_num should remain unchanged")

    def test_pin_nvrs_same_advisory_already_exists(self):
        """
        Test that when the same advisory already exists, no conflict is detected (idempotent)
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 12345

        releases_config = {"releases": {self.assembly: {"assembly": {"group": {"advisories": {"microshift": 12345}}}}}}

        # when
        pipeline._pin_nvrs(nvrs=None, releases_config=releases_config)

        # then
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"], 12345
        )
        self.assertEqual(pipeline.advisory_num, 12345, "advisory_num should remain unchanged")
        # Should not log a warning
        pipeline._logger.warning.assert_not_called()

    def test_pin_nvrs_different_advisory_exists_adopts_existing(self):
        """
        Test that when a different advisory already exists in releases config,
        the pipeline adopts the existing advisory and logs a warning
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 99999  # Newly created advisory

        releases_config = {
            "releases": {
                self.assembly: {
                    "assembly": {
                        "group": {
                            "advisories": {
                                "microshift": 12345  # Existing advisory
                            }
                        }
                    }
                }
            }
        }

        # when
        pipeline._pin_nvrs(nvrs=None, releases_config=releases_config)

        # then
        # Existing advisory should remain in config
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"],
            12345,
            "Existing advisory should not be overwritten",
        )
        # Pipeline should adopt the existing advisory
        self.assertEqual(pipeline.advisory_num, 12345, "Pipeline should adopt the existing advisory")
        # Should log a warning
        pipeline._logger.warning.assert_called_once()
        warning_msg = pipeline._logger.warning.call_args[0][0]
        self.assertIn("already references microshift advisory 12345", warning_msg)
        self.assertIn("adopting existing advisory instead of newly created advisory 99999", warning_msg)

    def test_pin_nvrs_existing_advisory_is_zero(self):
        """
        Test that when advisory is 0 (treated as "not set"), the new advisory is saved
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 12345

        releases_config = {
            "releases": {
                self.assembly: {
                    "assembly": {
                        "group": {
                            "advisories": {
                                "microshift": 0  # 0 means "not set"
                            }
                        }
                    }
                }
            }
        }

        # when
        pipeline._pin_nvrs(nvrs=None, releases_config=releases_config)

        # then
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"],
            12345,
            "Advisory 0 should be overwritten with new advisory",
        )
        self.assertEqual(pipeline.advisory_num, 12345)
        # Should not log a warning (0 is treated as "not set")
        pipeline._logger.warning.assert_not_called()

    def test_pin_nvrs_existing_advisory_is_negative(self):
        """
        Test that when advisory is negative (treated as "not set"), the new advisory is saved
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 12345

        releases_config = {
            "releases": {
                self.assembly: {
                    "assembly": {
                        "group": {
                            "advisories": {
                                "microshift": -1  # -1 means "not set"
                            }
                        }
                    }
                }
            }
        }

        # when
        pipeline._pin_nvrs(nvrs=None, releases_config=releases_config)

        # then
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"],
            12345,
            "Advisory -1 should be overwritten with new advisory",
        )
        self.assertEqual(pipeline.advisory_num, 12345)
        # Should not log a warning (-1 is treated as "not set")
        pipeline._logger.warning.assert_not_called()

    def test_pin_nvrs_with_nvrs_saves_advisory_and_pins_builds(self):
        """
        Test that when NVRs are provided, both advisory and NVRs are saved
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 12345

        releases_config = {"releases": {self.assembly: {}}}

        nvrs = [
            "microshift-4.17.1-202601290005.p0.g7ebffc3.assembly.4.17.1.el8",
            "microshift-4.17.1-202601290005.p0.g7ebffc3.assembly.4.17.1.el9",
        ]

        # when
        result = pipeline._pin_nvrs(nvrs=nvrs, releases_config=releases_config)

        # then
        # Advisory should be saved
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"], 12345
        )
        # NVRs should be pinned
        self.assertIsNotNone(result)
        rpms = releases_config["releases"][self.assembly]["assembly"]["members"]["rpms"]
        microshift_entry = next(filter(lambda rpm: rpm.get("distgit_key") == "microshift", rpms), None)
        self.assertIsNotNone(microshift_entry)
        self.assertEqual(
            microshift_entry["metadata"]["is"]["el8"], "microshift-4.17.1-202601290005.p0.g7ebffc3.assembly.4.17.1.el8"
        )
        self.assertEqual(
            microshift_entry["metadata"]["is"]["el9"], "microshift-4.17.1-202601290005.p0.g7ebffc3.assembly.4.17.1.el9"
        )

    def test_pin_nvrs_without_nvrs_only_saves_advisory(self):
        """
        Test that when nvrs=None, only the advisory is saved (no NVR pinning)
        """
        # given
        pipeline = BuildMicroShiftPipeline(
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            payloads=(),
            no_rebase=False,
            force=False,
            skip_prepare_advisory=False,
            data_path="https://github.com/openshift-eng/ocp-build-data",
            slack_client=self.mock_slack_client,
        )
        pipeline.advisory_num = 12345

        releases_config = {"releases": {self.assembly: {}}}

        # when
        result = pipeline._pin_nvrs(nvrs=None, releases_config=releases_config)

        # then
        # Advisory should be saved
        self.assertEqual(
            releases_config["releases"][self.assembly]["assembly"]["group"]["advisories"]["microshift"], 12345
        )
        # No RPMs should be pinned
        self.assertIsNone(result)
        self.assertNotIn("members", releases_config["releases"][self.assembly].get("assembly", {}))
