import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from pyartcd import tekton


class TestIsTektonContext(IsolatedAsyncioTestCase):
    def test_true_when_env_var_set(self):
        with patch.dict(os.environ, {"TEKTON_PIPELINERUN_NAME": "my-plr-abc123"}):
            self.assertTrue(tekton.is_tekton_context())

    def test_false_when_env_var_absent(self):
        env = {k: v for k, v in os.environ.items() if k != "TEKTON_PIPELINERUN_NAME"}
        with patch.dict(os.environ, env, clear=True):
            self.assertFalse(tekton.is_tekton_context())


class TestGetCurrentPipelinerunName(IsolatedAsyncioTestCase):
    def test_returns_name_when_set(self):
        with patch.dict(os.environ, {"TEKTON_PIPELINERUN_NAME": "my-plr-abc123"}):
            self.assertEqual(tekton.get_current_pipelinerun_name(), "my-plr-abc123")

    def test_returns_none_when_absent(self):
        env = {k: v for k, v in os.environ.items() if k != "TEKTON_PIPELINERUN_NAME"}
        with patch.dict(os.environ, env, clear=True):
            self.assertIsNone(tekton.get_current_pipelinerun_name())


class TestPipelinerunUrl(IsolatedAsyncioTestCase):
    def test_builds_correct_url(self):
        url = tekton.pipelinerun_url("my-run-xyz", "layered-products")
        self.assertIn("my-run-xyz", url)
        self.assertIn("layered-products", url)
        self.assertIn("artc2023", url)
        self.assertTrue(url.startswith("https://"))

    def test_default_namespace(self):
        url = tekton.pipelinerun_url("run-abc")
        self.assertIn(tekton.DEFAULT_NAMESPACE, url)


class TestStartPipeline(IsolatedAsyncioTestCase):
    async def test_success_returns_pipelinerun_name(self):
        with patch("pyartcd.tekton.exectools.cmd_gather_async", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (0, "pipelineruns.tekton.dev/build-layered-products-run123\n", "")
            plr = await tekton.start_pipeline("build-layered-products", {"group": "oadp-1.5"})
        self.assertEqual(plr, "pipelineruns.tekton.dev/build-layered-products-run123")
        cmd_args = mock_cmd.call_args[0][0]
        self.assertIn("tkn", cmd_args)
        self.assertIn("build-layered-products", cmd_args)
        self.assertIn("--param", cmd_args)
        self.assertIn("group=oadp-1.5", cmd_args)

    async def test_failure_raises_child_process_error(self):
        with patch("pyartcd.tekton.exectools.cmd_gather_async", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (1, "", "error: pipeline not found")
            with self.assertRaises(ChildProcessError):
                await tekton.start_pipeline("build-layered-products", {})

    async def test_empty_name_raises_runtime_error(self):
        with patch("pyartcd.tekton.exectools.cmd_gather_async", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (0, "", "")
            with self.assertRaises(RuntimeError):
                await tekton.start_pipeline("build-layered-products", {})
