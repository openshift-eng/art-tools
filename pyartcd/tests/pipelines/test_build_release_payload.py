import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.pipelines.build_release_payload import BuildReleasePayloadPipeline, _default_release


def _make_pipeline(**kwargs) -> BuildReleasePayloadPipeline:
    runtime = MagicMock()
    runtime.logger = MagicMock()
    runtime.doozer_working = "/tmp/doozer_working"
    runtime.dry_run = False

    defaults = dict(
        runtime=runtime,
        group="openshift-4.21",
        assembly="4.21.1",
        nvr=None,
        release=None,
        version=None,
        arch="x86_64",
        sync=True,
        konflux_kubeconfig=None,
        konflux_namespace="rhtap-releng-tenant",
        release_image_repo="quay.io/openshift-release-dev/ocp-release",
        data_path=None,
        registry_config=None,
        skip_cosign=False,
        skip_checks=False,
        dry_run=False,
    )
    defaults.update(kwargs)
    return BuildReleasePayloadPipeline(**defaults)


SAMPLE_DOOZER_RESULT = {
    "synced": True,
    "release_pullspec": "quay.io/art-images/ocp-release@sha256:listdigest",
    "arch_pullspecs": [
        "quay.io/art-images/ocp-release@sha256:x86digest",
        "quay.io/art-images/ocp-release@sha256:armdigest",
    ],
    "release_repo": "quay.io/openshift-release-dev/ocp-release",
}


class TestDefaultRelease(unittest.TestCase):
    def test_default_release_format(self):
        r = _default_release()
        # Should be 12 digits followed by ".p2"
        self.assertRegex(r, r"^\d{12}\.p2$")


class TestBuildReleasePayloadPipelineCosign(unittest.IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.build_release_payload.SigstoreSignatory")
    async def test_cosign_signs_unsigned_pullspecs(self, MockSignatory):
        pipeline = _make_pipeline()
        signatory_inst = MockSignatory.return_value
        signatory_inst.has_cosign_signature = AsyncMock(return_value=False)
        signatory_inst.sign_component_images = AsyncMock(return_value={})

        await pipeline._cosign(SAMPLE_DOOZER_RESULT)

        signed_pullspecs = signatory_inst.sign_component_images.call_args[0][0]
        expected = [
            "quay.io/openshift-release-dev/ocp-release@sha256:listdigest",
            "quay.io/openshift-release-dev/ocp-release@sha256:x86digest",
            "quay.io/openshift-release-dev/ocp-release@sha256:armdigest",
        ]
        self.assertEqual(sorted(signed_pullspecs), sorted(expected))

    @patch("pyartcd.pipelines.build_release_payload.SigstoreSignatory")
    async def test_cosign_skips_already_signed_pullspecs(self, MockSignatory):
        pipeline = _make_pipeline()
        signatory_inst = MockSignatory.return_value
        signatory_inst.has_cosign_signature = AsyncMock(return_value=True)
        signatory_inst.sign_component_images = AsyncMock(return_value={})

        await pipeline._cosign(SAMPLE_DOOZER_RESULT)

        signatory_inst.sign_component_images.assert_not_called()

    @patch("pyartcd.pipelines.build_release_payload.SigstoreSignatory")
    async def test_cosign_mixed_signed_and_unsigned(self, MockSignatory):
        pipeline = _make_pipeline()
        signatory_inst = MockSignatory.return_value

        already_signed = {"quay.io/openshift-release-dev/ocp-release@sha256:listdigest"}

        async def has_sig(ps):
            return ps in already_signed

        signatory_inst.has_cosign_signature = AsyncMock(side_effect=has_sig)
        signatory_inst.sign_component_images = AsyncMock(return_value={})

        await pipeline._cosign(SAMPLE_DOOZER_RESULT)

        signed_pullspecs = signatory_inst.sign_component_images.call_args[0][0]
        self.assertEqual(len(signed_pullspecs), 2)
        self.assertNotIn("quay.io/openshift-release-dev/ocp-release@sha256:listdigest", signed_pullspecs)

    @patch("pyartcd.pipelines.build_release_payload.SigstoreSignatory")
    async def test_cosign_dry_run_passed_through(self, MockSignatory):
        pipeline = _make_pipeline(dry_run=True)
        signatory_inst = MockSignatory.return_value
        signatory_inst.has_cosign_signature = AsyncMock(return_value=False)
        signatory_inst.sign_component_images = AsyncMock(return_value={})

        await pipeline._cosign(SAMPLE_DOOZER_RESULT)

        _, kwargs = MockSignatory.call_args
        self.assertTrue(kwargs["dry_run"])

    @patch("pyartcd.pipelines.build_release_payload.SigstoreSignatory")
    async def test_cosign_raises_on_signing_failure(self, MockSignatory):
        pipeline = _make_pipeline()
        signatory_inst = MockSignatory.return_value
        signatory_inst.has_cosign_signature = AsyncMock(return_value=False)
        signatory_inst.sign_component_images = AsyncMock(
            return_value={"quay.io/openshift-release-dev/ocp-release@sha256:x86digest": RuntimeError("cosign failed")}
        )

        with self.assertRaises(RuntimeError) as cm:
            await pipeline._cosign(SAMPLE_DOOZER_RESULT)

        self.assertIn("Cosign signing failed", str(cm.exception))

    @patch("pyartcd.pipelines.build_release_payload.SigstoreSignatory")
    async def test_cosign_raises_when_release_pullspec_missing(self, MockSignatory):
        pipeline = _make_pipeline()

        with self.assertRaises(RuntimeError) as cm:
            await pipeline._cosign({"synced": True, "arch_pullspecs": []})

        self.assertIn("missing release_pullspec", str(cm.exception))

    @patch.dict(os.environ, {"KMS_CRED_FILE": "", "KMS_KEY_ID": ""}, clear=False)
    async def test_check_env_vars_raises_when_missing_in_non_dry_run(self):
        pipeline = _make_pipeline(dry_run=False)
        with self.assertRaises(ValueError) as cm:
            pipeline._check_environment_variables()
        self.assertIn("KMS_CRED_FILE", str(cm.exception))

    @patch.dict(os.environ, {"KMS_CRED_FILE": "", "KMS_KEY_ID": ""}, clear=False)
    async def test_check_env_vars_warns_in_dry_run(self):
        pipeline = _make_pipeline(dry_run=True)
        pipeline._check_environment_variables()
        pipeline._logger.warning.assert_called()


class TestBuildReleasePayloadPipelineRun(unittest.IsolatedAsyncioTestCase):
    @patch("pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._cosign", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._run_doozer",
        new_callable=AsyncMock,
    )
    @patch.dict(os.environ, {"KMS_CRED_FILE": "creds", "KMS_KEY_ID": "key"}, clear=False)
    async def test_run_calls_doozer_and_cosigns(self, mock_doozer, mock_cosign):
        mock_doozer.return_value = SAMPLE_DOOZER_RESULT
        pipeline = _make_pipeline()

        await pipeline.run()

        mock_doozer.assert_awaited_once()
        mock_cosign.assert_awaited_once_with(SAMPLE_DOOZER_RESULT)

    @patch("pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._cosign", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._run_doozer",
        new_callable=AsyncMock,
    )
    @patch.dict(os.environ, {"KMS_CRED_FILE": "creds", "KMS_KEY_ID": "key"}, clear=False)
    async def test_run_skips_cosign_when_sync_false(self, mock_doozer, mock_cosign):
        mock_doozer.return_value = {**SAMPLE_DOOZER_RESULT, "synced": False}
        pipeline = _make_pipeline()

        await pipeline.run()

        mock_cosign.assert_not_called()

    @patch("pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._cosign", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._run_doozer",
        new_callable=AsyncMock,
    )
    async def test_run_skip_cosign_flag_skips_signing(self, mock_doozer, mock_cosign):
        mock_doozer.return_value = SAMPLE_DOOZER_RESULT
        pipeline = _make_pipeline(skip_cosign=True)

        await pipeline.run()

        mock_doozer.assert_awaited_once()
        mock_cosign.assert_not_called()

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_command_normal_path_push_and_sync(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps(SAMPLE_DOOZER_RESULT), "")
        pipeline = _make_pipeline(sync=True, skip_cosign=True)

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--push", cmd)
        self.assertIn("--sync", cmd)
        self.assertNotIn("--dry-run", cmd)

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_command_push_without_sync(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps({**SAMPLE_DOOZER_RESULT, "synced": False}), "")
        pipeline = _make_pipeline(sync=False, skip_cosign=True)

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--push", cmd)
        self.assertNotIn("--sync", cmd)

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_command_dry_run_no_push(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps({**SAMPLE_DOOZER_RESULT, "synced": False}), "")
        pipeline = _make_pipeline(dry_run=True, sync=True, skip_cosign=True)

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--dry-run", cmd)
        self.assertNotIn("--push", cmd)
        self.assertNotIn("--sync", cmd)

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_command_nvr_path(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps(SAMPLE_DOOZER_RESULT), "")
        pipeline = _make_pipeline(nvr="release-payload-4.21.1-202608011200.p2", skip_cosign=True)

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--nvr=release-payload-4.21.1-202608011200.p2", cmd)
        self.assertNotIn("--push", cmd)
        self.assertNotIn("--sync", cmd)
        # --release=<value> should not appear (only --release-image-repo is allowed)
        self.assertFalse(any(a.startswith("--release=") for a in cmd))

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_command_auto_generates_release_when_not_provided(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps(SAMPLE_DOOZER_RESULT), "")
        pipeline = _make_pipeline(release=None, skip_cosign=True)

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        release_args = [a for a in cmd if a.startswith("--release=")]
        self.assertEqual(len(release_args), 1)
        self.assertRegex(release_args[0], r"^--release=\d{12}\.p2$")

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_command_uses_explicit_release(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps(SAMPLE_DOOZER_RESULT), "")
        pipeline = _make_pipeline(release="202608011200.p2", skip_cosign=True)

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--release=202608011200.p2", cmd)

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_includes_all_optional_flags(self, mock_cmd):
        mock_cmd.return_value = (0, json.dumps(SAMPLE_DOOZER_RESULT), "")
        pipeline = _make_pipeline(
            version="v4.21.1",
            konflux_kubeconfig="/path/to/kube",
            registry_config="/path/to/auth",
            skip_checks=True,
            skip_cosign=True,
        )

        await pipeline._run_doozer()

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("--version=v4.21.1", cmd)
        self.assertIn("--konflux-kubeconfig=/path/to/kube", cmd)
        self.assertIn("--registry-config=/path/to/auth", cmd)
        self.assertIn("--skip-checks", cmd)

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_raises_on_doozer_failure(self, mock_cmd):
        mock_cmd.return_value = (1, "", "some doozer error")
        pipeline = _make_pipeline(skip_cosign=True)

        with self.assertRaises(RuntimeError) as cm:
            await pipeline._run_doozer()

        self.assertIn("doozer beta:release-payload:rebase-and-build failed", str(cm.exception))

    @patch("pyartcd.pipelines.build_release_payload.exectools.cmd_gather_async", new_callable=AsyncMock)
    async def test_run_doozer_raises_on_invalid_json(self, mock_cmd):
        mock_cmd.return_value = (0, "not json", "")
        pipeline = _make_pipeline(skip_cosign=True)

        with self.assertRaises(RuntimeError) as cm:
            await pipeline._run_doozer()

        self.assertIn("Could not parse doozer JSON output", str(cm.exception))

    @patch("pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._cosign", new_callable=AsyncMock)
    @patch(
        "pyartcd.pipelines.build_release_payload.BuildReleasePayloadPipeline._run_doozer",
        new_callable=AsyncMock,
    )
    @patch.dict(os.environ, {"KMS_CRED_FILE": "creds", "KMS_KEY_ID": "key"}, clear=False)
    async def test_run_nvr_path_syncs_and_cosigns(self, mock_doozer, mock_cosign):
        mock_doozer.return_value = SAMPLE_DOOZER_RESULT
        pipeline = _make_pipeline(nvr="release-payload-4.21.1-202608011200.p2")

        await pipeline.run()

        mock_doozer.assert_awaited_once()
        mock_cosign.assert_awaited_once_with(SAMPLE_DOOZER_RESULT)
