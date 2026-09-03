import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

from pyartcd.pipelines.olm_bundle_konflux import _trigger_fbc


class TestTriggerFbc(IsolatedAsyncioTestCase):
    async def test_triggers_tekton_pipeline_in_tekton_context(self):
        with patch.dict(os.environ, {"TEKTON_PIPELINERUN_NAME": "my-plr-123"}):
            with patch(
                "pyartcd.pipelines.olm_bundle_konflux.tekton.start_pipeline", new_callable=AsyncMock
            ) as mock_start:
                mock_start.return_value = "build-fbc-run-xyz"
                await _trigger_fbc(
                    version="1.5.0",
                    group="oadp-1.5",
                    assembly="stream",
                    operator_nvrs=["oadp-operator-container-1.5.0-1"],
                    data_path="https://github.com/openshift-eng/ocp-build-data",
                    data_gitref="",
                    dry_run=False,
                    ocp_target_version="4.17",
                    force_build=True,
                )
                mock_start.assert_called_once()
                call_kwargs = mock_start.call_args
                self.assertEqual(call_kwargs.kwargs["pipeline_name"], "build-fbc")
                params = call_kwargs.kwargs["params"]
                self.assertEqual(params["ocp-target-version"], "4.17")
                self.assertEqual(params["force"], "true")

    async def test_triggers_jenkins_outside_tekton(self):
        env = {k: v for k, v in os.environ.items() if k != "TEKTON_PIPELINERUN_NAME"}
        with patch.dict(os.environ, env, clear=True):
            with patch("pyartcd.pipelines.olm_bundle_konflux.jenkins.start_build_fbc") as mock_jenkins:
                await _trigger_fbc(
                    version="1.5.0",
                    group="oadp-1.5",
                    assembly="stream",
                    operator_nvrs=["oadp-operator-container-1.5.0-1"],
                    data_path="https://github.com/openshift-eng/ocp-build-data",
                    data_gitref="",
                    dry_run=False,
                )
                mock_jenkins.assert_called_once()
