import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from elliottlib.cli.verify_csv_versions_cli import (
    CSV_VERSION_OVERRIDES,
    OperatorCsvResult,
    VerifyCsvVersionsResult,
    _find_head_bundle,
    render_and_check_csv_versions,
)
from elliottlib.verify_common import render_verify_result


class TestOperatorCsvResult(IsolatedAsyncioTestCase):
    def test_match(self):
        r = OperatorCsvResult(package="sriov", channel="stable", csv_version="4.22.0-202407221", match=True)
        self.assertTrue(r.match)

    def test_mismatch(self):
        r = OperatorCsvResult(package="sriov", channel="stable", csv_version="4.21.0-202407221", match=False)
        self.assertFalse(r.match)


class TestVerifyCsvVersionsResult(IsolatedAsyncioTestCase):
    def test_passed_all_match(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.22.0-1", match=True),
                OperatorCsvResult(package="ptp", channel="stable", csv_version="4.22.0-2", match=True),
            ],
        )
        self.assertTrue(r.passed)
        self.assertEqual(len(r.mismatches), 0)

    def test_failed_mismatch(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.22.0-1", match=True),
                OperatorCsvResult(package="ptp", channel="stable", csv_version="4.21.0-1", match=False),
            ],
        )
        self.assertFalse(r.passed)
        self.assertEqual(len(r.mismatches), 1)
        self.assertEqual(r.mismatches[0].package, "ptp")

    def test_failed_no_operators(self):
        r = VerifyCsvVersionsResult(expected_version="4.22", catalog_image="test:v4.22")
        self.assertFalse(r.passed)

    def test_failed_error(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            error="opm not found",
        )
        self.assertFalse(r.passed)

    def test_to_dict(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.22.0-1", match=True),
            ],
        )
        d = r.to_dict()
        self.assertTrue(d["passed"])
        self.assertEqual(d["summary"]["total"], 1)
        self.assertEqual(d["summary"]["match"], 1)
        self.assertEqual(d["summary"]["mismatch"], 0)

    def test_render_text_passed(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.22.0-1", match=True),
            ],
        )
        text = r.render_text()
        self.assertIn("PASSED", text)
        self.assertIn("sriov", text)
        self.assertIn("MATCH", text)

    def test_render_text_failed(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            operators=[
                OperatorCsvResult(package="ptp", channel="stable", csv_version="4.21.0-1", match=False),
            ],
        )
        text = r.render_text()
        self.assertIn("FAILED", text)
        self.assertIn("MISMATCH", text)

    def test_render_text_error(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            error="opm render failed",
        )
        text = r.render_text()
        self.assertIn("ERROR", text)
        self.assertIn("opm render failed", text)

    def test_render_json(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.22",
            catalog_image="test:v4.22",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.22.0-1", match=True),
            ],
        )
        output = render_verify_result(r, "json")
        parsed = json.loads(output)
        self.assertTrue(parsed["passed"])


class TestFindHeadBundle(IsolatedAsyncioTestCase):
    def test_simple_chain(self):
        entries = [
            {"name": "op.v4.22.0-1", "replaces": "op.v4.22.0-0"},
            {"name": "op.v4.22.0-0"},
        ]
        self.assertEqual(_find_head_bundle(entries), "op.v4.22.0-1")

    def test_single_entry(self):
        entries = [{"name": "op.v4.22.0-1"}]
        self.assertEqual(_find_head_bundle(entries), "op.v4.22.0-1")

    def test_with_skips(self):
        entries = [
            {"name": "op.v4.22.0-2", "replaces": "op.v4.22.0-0", "skips": ["op.v4.22.0-1"]},
            {"name": "op.v4.22.0-1", "replaces": "op.v4.22.0-0"},
            {"name": "op.v4.22.0-0"},
        ]
        self.assertEqual(_find_head_bundle(entries), "op.v4.22.0-2")

    def test_empty_entries(self):
        self.assertIsNone(_find_head_bundle([]))


FBC_YAML_ALL_MATCH = """---
schema: olm.package
name: sriov-network-operator
defaultChannel: stable
---
schema: olm.package
name: ptp-operator
defaultChannel: stable
---
schema: olm.channel
package: sriov-network-operator
name: stable
entries:
  - name: sriov-network-operator.v4.22.0-202407221
---
schema: olm.channel
package: ptp-operator
name: stable
entries:
  - name: ptp-operator.v4.22.0-202407221
---
schema: olm.bundle
name: sriov-network-operator.v4.22.0-202407221
package: sriov-network-operator
properties:
  - type: olm.package
    value:
      packageName: sriov-network-operator
      version: "4.22.0-202407221"
---
schema: olm.bundle
name: ptp-operator.v4.22.0-202407221
package: ptp-operator
properties:
  - type: olm.package
    value:
      packageName: ptp-operator
      version: "4.22.0-202407221"
"""

FBC_YAML_WITH_MISMATCH = """---
schema: olm.package
name: sriov-network-operator
defaultChannel: stable
---
schema: olm.package
name: dpu-operator
defaultChannel: stable
---
schema: olm.channel
package: sriov-network-operator
name: stable
entries:
  - name: sriov-network-operator.v4.22.0-202407221
---
schema: olm.channel
package: dpu-operator
name: stable
entries:
  - name: dpu-operator.v4.21.0-202407221
---
schema: olm.bundle
name: sriov-network-operator.v4.22.0-202407221
package: sriov-network-operator
properties:
  - type: olm.package
    value:
      packageName: sriov-network-operator
      version: "4.22.0-202407221"
---
schema: olm.bundle
name: dpu-operator.v4.21.0-202407221
package: dpu-operator
properties:
  - type: olm.package
    value:
      packageName: dpu-operator
      version: "4.21.0-202407221"
"""


FBC_YAML_WITH_OVERRIDE = """---
schema: olm.package
name: sriov-network-operator
defaultChannel: stable
---
schema: olm.package
name: local-storage-operator
defaultChannel: stable
---
schema: olm.channel
package: sriov-network-operator
name: stable
entries:
  - name: sriov-network-operator.v4.12.0-202407221
---
schema: olm.channel
package: local-storage-operator
name: stable
entries:
  - name: local-storage-operator.v4.18.0-202407221
---
schema: olm.bundle
name: sriov-network-operator.v4.12.0-202407221
package: sriov-network-operator
properties:
  - type: olm.package
    value:
      packageName: sriov-network-operator
      version: "4.12.0-202407221"
---
schema: olm.bundle
name: local-storage-operator.v4.18.0-202407221
package: local-storage-operator
properties:
  - type: olm.package
    value:
      packageName: local-storage-operator
      version: "4.18.0-202407221"
"""


class TestCsvVersionOverrides(IsolatedAsyncioTestCase):
    def test_override_match_with_override_field(self):
        r = OperatorCsvResult(
            package="local-storage-operator",
            channel="stable",
            csv_version="4.18.0-202407221",
            match=True,
            override="4.18",
        )
        self.assertTrue(r.match)
        self.assertEqual(r.override, "4.18")

    def test_render_text_shows_override(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.12",
            catalog_image="test:v4.12",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.12.0-1", match=True),
                OperatorCsvResult(
                    package="local-storage-operator",
                    channel="stable",
                    csv_version="4.18.0-1",
                    match=True,
                    override="4.18",
                ),
            ],
        )
        text = r.render_text()
        self.assertIn("MATCH (override: 4.18)", text)
        self.assertIn("PASSED", text)

    def test_to_dict_includes_override(self):
        r = VerifyCsvVersionsResult(
            expected_version="4.12",
            catalog_image="test:v4.12",
            operators=[
                OperatorCsvResult(package="sriov", channel="stable", csv_version="4.12.0-1", match=True),
                OperatorCsvResult(
                    package="local-storage-operator",
                    channel="stable",
                    csv_version="4.18.0-1",
                    match=True,
                    override="4.18",
                ),
            ],
        )
        d = r.to_dict()
        self.assertTrue(d["passed"])
        lso = next(o for o in d["operators"] if o["package"] == "local-storage-operator")
        self.assertEqual(lso["override"], "4.18")
        sriov = next(o for o in d["operators"] if o["package"] == "sriov")
        self.assertNotIn("override", sriov)

    def test_overrides_dict_has_local_storage(self):
        for minor in range(12, 18):
            self.assertEqual(
                CSV_VERSION_OVERRIDES["local-storage-operator"][f"4.{minor}"],
                "4.18",
            )
        self.assertNotIn("4.18", CSV_VERSION_OVERRIDES["local-storage-operator"])

    def test_overrides_dict_has_smb_csi(self):
        for minor in range(16, 18):
            self.assertEqual(
                CSV_VERSION_OVERRIDES["smb-csi-driver-operator"][f"4.{minor}"],
                "4.18",
            )
        self.assertNotIn("4.15", CSV_VERSION_OVERRIDES["smb-csi-driver-operator"])
        self.assertNotIn("4.18", CSV_VERSION_OVERRIDES["smb-csi-driver-operator"])


class TestRenderAndCheckCsvVersions(IsolatedAsyncioTestCase):
    @patch("elliottlib.cli.verify_csv_versions_cli.cmd_gather_async")
    async def test_all_match(self, mock_cmd):
        mock_cmd.return_value = (0, FBC_YAML_ALL_MATCH, "")
        result = await render_and_check_csv_versions("test:v4.22", "4.22")
        self.assertTrue(result.passed)
        self.assertEqual(len(result.operators), 2)
        self.assertTrue(all(o.match for o in result.operators))

    @patch("elliottlib.cli.verify_csv_versions_cli.cmd_gather_async")
    async def test_with_mismatch(self, mock_cmd):
        mock_cmd.return_value = (0, FBC_YAML_WITH_MISMATCH, "")
        result = await render_and_check_csv_versions("test:v4.22", "4.22")
        self.assertFalse(result.passed)
        self.assertEqual(len(result.mismatches), 1)
        self.assertEqual(result.mismatches[0].package, "dpu-operator")
        self.assertEqual(result.mismatches[0].csv_version, "4.21.0-202407221")

    @patch("elliottlib.cli.verify_csv_versions_cli.cmd_gather_async")
    async def test_with_override(self, mock_cmd):
        mock_cmd.return_value = (0, FBC_YAML_WITH_OVERRIDE, "")
        result = await render_and_check_csv_versions("test:v4.12", "4.12")
        self.assertTrue(result.passed)
        lso = next(o for o in result.operators if o.package == "local-storage-operator")
        self.assertTrue(lso.match)
        self.assertEqual(lso.override, "4.18")
        self.assertEqual(lso.csv_version, "4.18.0-202407221")

    @patch("elliottlib.cli.verify_csv_versions_cli.cmd_gather_async")
    async def test_opm_failure(self, mock_cmd):
        mock_cmd.return_value = (1, "", "image not found")
        result = await render_and_check_csv_versions("test:v4.22", "4.22")
        self.assertFalse(result.passed)
        self.assertIn("opm render failed", result.error)

    @patch("elliottlib.cli.verify_csv_versions_cli.cmd_gather_async")
    async def test_opm_exception(self, mock_cmd):
        mock_cmd.side_effect = FileNotFoundError("opm not found")
        result = await render_and_check_csv_versions("test:v4.22", "4.22")
        self.assertFalse(result.passed)
        self.assertIn("opm not found", result.error)
