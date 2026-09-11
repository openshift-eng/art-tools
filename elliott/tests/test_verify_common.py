import json
from dataclasses import dataclass
from typing import Optional
from unittest import TestCase
from unittest.mock import MagicMock

from elliottlib.verify_common import (
    VerifyResultBase,
    get_assembly_advisory_ids,
    handle_verify_result,
    render_verify_result,
)


class TestGetAssemblyAdvisoryIds(TestCase):
    def _make_runtime(self, advisories: dict):
        runtime = MagicMock()
        runtime.assembly = "4.18.51"
        mock_config = MagicMock()
        mock_config.get.return_value = advisories
        runtime.get_releases_config.return_value = MagicMock()

        import artcommonlib.assembly as asm

        original = asm.assembly_config_struct

        def fake_config_struct(releases_config, assembly, key, default):
            if key == "group":
                return mock_config
            return original(releases_config, assembly, key, default)

        self._patcher = MagicMock()
        import unittest.mock

        self._patcher = unittest.mock.patch(
            "elliottlib.verify_common.assembly_config_struct",
            side_effect=fake_config_struct,
        )
        self._patcher.start()
        return runtime

    def tearDown(self):
        if hasattr(self, "_patcher") and hasattr(self._patcher, "stop"):
            self._patcher.stop()

    def test_no_filter(self):
        runtime = self._make_runtime({"rpm": 111, "image": 222, "rhcos": 333})
        result = get_assembly_advisory_ids(runtime)
        self.assertEqual(result, {"rpm": 111, "image": 222, "rhcos": 333})

    def test_include_types(self):
        runtime = self._make_runtime({"rpm": 111, "image": 222, "rhcos": 333})
        result = get_assembly_advisory_ids(runtime, include_types=("rpm", "rhcos"))
        self.assertEqual(result, {"rpm": 111, "rhcos": 333})

    def test_exclude_types(self):
        runtime = self._make_runtime({"rpm": 111, "image": 222, "microshift": 444})
        result = get_assembly_advisory_ids(runtime, exclude_types=("microshift",))
        self.assertEqual(result, {"rpm": 111, "image": 222})

    def test_include_and_exclude(self):
        runtime = self._make_runtime({"rpm": 111, "image": 222, "rhcos": 333})
        result = get_assembly_advisory_ids(runtime, include_types=("rpm", "image", "rhcos"), exclude_types=("image",))
        self.assertEqual(result, {"rpm": 111, "rhcos": 333})

    def test_skips_empty_ids(self):
        runtime = self._make_runtime({"rpm": 111, "image": 0, "rhcos": None})
        result = get_assembly_advisory_ids(runtime)
        self.assertEqual(result, {"rpm": 111})

    def test_converts_to_int(self):
        runtime = self._make_runtime({"rpm": "111"})
        result = get_assembly_advisory_ids(runtime)
        self.assertEqual(result, {"rpm": 111})
        self.assertIsInstance(result["rpm"], int)


# Concrete subclass for testing VerifyResultBase
@dataclass
class _TestResult(VerifyResultBase):
    success: bool = True
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.success and not self.error

    def to_dict(self) -> dict:
        return {"passed": self.passed, "failed": self.failed, "error": self.error}

    def render_text(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"Test result: {status}"]
        if self.error:
            lines.append(f"  Error: {self.error}")
        lines.append(f"Overall: {status}")
        return "\n".join(lines)


class TestVerifyResultBase(TestCase):
    def test_passed(self):
        r = _TestResult(success=True)
        self.assertTrue(r.passed)
        self.assertFalse(r.failed)

    def test_failed(self):
        r = _TestResult(success=False)
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)

    def test_error_implies_failed(self):
        r = _TestResult(success=True, error="boom")
        self.assertFalse(r.passed)
        self.assertTrue(r.failed)


class TestRenderVerifyResult(TestCase):
    def test_json(self):
        r = _TestResult(success=True)
        output = render_verify_result(r, "json")
        data = json.loads(output)
        self.assertTrue(data["passed"])
        self.assertFalse(data["failed"])

    def test_text(self):
        r = _TestResult(success=True)
        output = render_verify_result(r, "text")
        self.assertIn("PASS", output)

    def test_text_failed(self):
        r = _TestResult(success=False, error="something broke")
        output = render_verify_result(r, "text")
        self.assertIn("FAIL", output)
        self.assertIn("something broke", output)


class TestHandleVerifyResult(TestCase):
    def test_passed_no_exit(self):
        r = _TestResult(success=True)
        # Should not raise
        try:
            handle_verify_result(r, "text")
        except SystemExit:
            self.fail("handle_verify_result raised SystemExit on passing result")

    def test_failed_exits(self):
        r = _TestResult(success=False)
        with self.assertRaises(SystemExit) as ctx:
            handle_verify_result(r, "text")
        self.assertEqual(ctx.exception.code, 1)
