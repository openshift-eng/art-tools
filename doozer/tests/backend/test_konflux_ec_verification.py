"""Tests for EC verification gating logic in KonfluxImageBuilder.build().

Verifies that enterprise-contract verification is only triggered for images
that are for_release=True, in an OCP group, and not skipped via --skip-ec-verify.
"""

import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxECStatus
from doozerlib.backend.konflux_client import ECVerificationResult
from doozerlib.backend.konflux_image_builder import (
    KonfluxImageBuilder,
    KonfluxImageBuilderConfig,
    KonfluxImageBuildError,
)


def _make_config(**overrides) -> KonfluxImageBuilderConfig:
    defaults = dict(
        base_dir=Path("/tmp/test-builds"),
        group_name="openshift-4.18",
        namespace="ocp-art-tenant",
        plr_template="https://example.com/template",
        dry_run=True,
        build_priority="5",
        skip_ec_verify=False,
    )
    defaults.update(overrides)
    return KonfluxImageBuilderConfig(**defaults)


def _make_successful_pipelinerun_info():
    """Create a mock PipelineRunInfo that represents a successful build."""
    plr_info = MagicMock()
    plr_info.name = "test-plr-abc12"
    plr_info.to_dict.return_value = {
        "kind": "PipelineRun",
        "metadata": {
            "name": "test-plr-abc12",
            "namespace": "ocp-art-tenant",
            "uid": "fake-uid",
            "labels": {
                "appstudio.openshift.io/application": "openshift-4-18",
                "appstudio.openshift.io/component": "ose-4-18-test-image",
            },
        },
        "status": {
            "results": [
                {"name": "IMAGE_URL", "value": "quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:tag123"},
                {"name": "IMAGE_DIGEST", "value": "sha256:abc123def456"},
            ],
            "startTime": "2026-04-10T00:00:00Z",
            "completionTime": "2026-04-10T00:30:00Z",
        },
    }
    succeeded_condition = MagicMock()
    plr_info.find_condition.return_value = succeeded_condition
    return plr_info


def _make_metadata(distgit_key="test-image", for_release=True, is_base_image=False, ec_failure_is_error=False):
    """Create a mock ImageMetadata."""
    metadata = MagicMock()
    metadata.distgit_key = distgit_key
    metadata.qualified_key = f"containers/{distgit_key}"
    metadata.image_name_short = distgit_key
    metadata.is_olm_operator = False
    metadata.for_release = for_release
    metadata.is_base_image.return_value = is_base_image
    metadata.has_source.return_value = True
    metadata.get_konflux_build_attempts.return_value = 1
    metadata.get_arches.return_value = ["x86_64"]
    metadata.get_latest_build = AsyncMock(return_value=None)
    metadata.build_event = asyncio.Event()
    metadata.get_parent_members.return_value = {}
    metadata.runtime.assembly = "stream"
    metadata.runtime.assembly_type = MagicMock()
    metadata.runtime.konflux_db = None
    metadata.runtime.group_config = {"konflux": {"ec_failure_is_error": ec_failure_is_error}}
    return metadata


def _ec_passed_result():
    return ECVerificationResult(
        ec_status=KonfluxECStatus.PASSED, ec_pipeline_url="https://example.com/ec-plr", ec_failed=False
    )


def _ec_failed_result():
    return ECVerificationResult(
        ec_status=KonfluxECStatus.FAILED, ec_pipeline_url="https://example.com/ec-plr", ec_failed=True
    )


@patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
class TestEcVerificationGating(IsolatedAsyncioTestCase):
    """Test that EC verification is only triggered under the correct conditions."""

    async def _run_build_and_get_ec_calls(self, config, metadata, mock_kc_init, ec_result=None):
        """Helper: run build() with all heavy methods mocked, return verify_enterprise_contract mock."""
        if ec_result is None:
            ec_result = _ec_passed_result()

        builder = KonfluxImageBuilder(config)

        plr_info = _make_successful_pipelinerun_info()

        builder._start_build = AsyncMock(return_value=plr_info)
        builder._validate_build_attestation_and_signature = AsyncMock()
        builder._wait_for_parent_members = AsyncMock(return_value=[])
        builder.update_konflux_db = AsyncMock(return_value=None)
        builder._parse_dockerfile = MagicMock(
            return_value=("uuid-tag", "test-component", "v4.18.0", "202604100000.p0.el9")
        )

        with patch.object(Path, "exists", return_value=True):
            with patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new_callable=AsyncMock,
            ) as mock_build_repo:
                mock_repo = MagicMock()
                mock_repo.url = "https://github.com/openshift/test-repo"
                mock_repo.commit_hash = "deadbeef"
                mock_repo.branch = "art-openshift-4.18-assembly-stream-dgk-test-image"
                mock_build_repo.return_value = mock_repo

                wait_plr_info = _make_successful_pipelinerun_info()
                builder._konflux_client.wait_for_pipelinerun = AsyncMock(return_value=wait_plr_info)
                builder._konflux_client.resource_url = MagicMock(return_value="https://example.com/plr")

                builder._konflux_client.verify_enterprise_contract = AsyncMock(return_value=ec_result)

                with patch.object(
                    KonfluxBuildOutcome,
                    "extract_from_pipelinerun_succeeded_condition",
                    return_value=KonfluxBuildOutcome.SUCCESS,
                ):
                    await builder.build(metadata)

        return builder._konflux_client.verify_enterprise_contract

    async def test_ec_runs_for_release_ocp_image(self, mock_kc_init):
        """EC verification should run for a for_release=True OCP image."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=True)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()

    async def test_ec_skipped_for_base_image(self, mock_kc_init):
        """EC verification should be skipped for base images (not for_release)."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=False, is_base_image=True)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_ec_skipped_for_non_release_image(self, mock_kc_init):
        """EC verification should be skipped for non-release images."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=False, is_base_image=False)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_ec_skipped_when_skip_flag_set(self, mock_kc_init):
        """EC verification should be skipped when --skip-ec-verify is set."""
        config = _make_config(group_name="openshift-4.18", skip_ec_verify=True)
        metadata = _make_metadata(for_release=True)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_ec_skipped_for_non_ocp_group(self, mock_kc_init):
        """EC verification should be skipped for non-OCP groups (e.g. logging)."""
        config = _make_config(group_name="logging-6.2")
        metadata = _make_metadata(for_release=True)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_ec_failure_does_not_fail_build_by_default(self, mock_kc_init):
        """When ec_failure_is_error is false (default), EC failure should not fail the build."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=True, ec_failure_is_error=False)

        verify_ec = await self._run_build_and_get_ec_calls(
            config, metadata, mock_kc_init, ec_result=_ec_failed_result()
        )
        verify_ec.assert_called_once()
        self.assertTrue(metadata.build_status)

    async def test_no_retry_when_ec_fails(self, mock_kc_init):
        """When ec_failure_is_error is true and EC verification fails, the build should NOT be retried."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=True, ec_failure_is_error=True)
        metadata.get_konflux_build_attempts.return_value = 3

        builder = KonfluxImageBuilder(config)

        plr_info = _make_successful_pipelinerun_info()
        builder._start_build = AsyncMock(return_value=plr_info)
        builder._validate_build_attestation_and_signature = AsyncMock()
        builder._wait_for_parent_members = AsyncMock(return_value=[])
        builder.update_konflux_db = AsyncMock(return_value=None)
        builder._parse_dockerfile = MagicMock(
            return_value=("uuid-tag", "test-component", "v4.18.0", "202604100000.p0.el9")
        )

        with patch.object(Path, "exists", return_value=True):
            with patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new_callable=AsyncMock,
            ) as mock_build_repo:
                mock_repo = MagicMock()
                mock_repo.url = "https://github.com/openshift/test-repo"
                mock_repo.commit_hash = "deadbeef"
                mock_repo.branch = "art-openshift-4.18-assembly-stream-dgk-test-image"
                mock_build_repo.return_value = mock_repo

                wait_plr_info = _make_successful_pipelinerun_info()
                builder._konflux_client.wait_for_pipelinerun = AsyncMock(return_value=wait_plr_info)
                builder._konflux_client.resource_url = MagicMock(return_value="https://example.com/plr")
                builder._konflux_client.verify_enterprise_contract = AsyncMock(return_value=_ec_failed_result())

                with patch.object(
                    KonfluxBuildOutcome,
                    "extract_from_pipelinerun_succeeded_condition",
                    return_value=KonfluxBuildOutcome.SUCCESS,
                ):
                    with self.assertRaises(KonfluxImageBuildError):
                        await builder.build(metadata)

        builder._start_build.assert_called_once()
