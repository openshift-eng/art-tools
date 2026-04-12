"""Tests for EC verification gating logic in KonfluxImageBuilder.build().

Verifies that enterprise-contract verification is only triggered for images
that are for_release=True, in an OCP group, and not skipped via --skip-ec-verify.
"""

import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder, KonfluxImageBuilderConfig


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


def _make_metadata(distgit_key="test-image", for_release=True):
    """Create a mock ImageMetadata."""
    metadata = MagicMock()
    metadata.distgit_key = distgit_key
    metadata.qualified_key = f"containers/{distgit_key}"
    metadata.image_name_short = distgit_key
    metadata.is_olm_operator = False
    metadata.for_release = for_release
    metadata.has_source.return_value = True
    metadata.get_konflux_build_attempts.return_value = 1
    metadata.get_arches.return_value = ["x86_64"]
    metadata.get_latest_build = AsyncMock(return_value=None)
    metadata.build_event = asyncio.Event()
    metadata.get_parent_members.return_value = {}
    metadata.runtime.assembly = "stream"
    metadata.runtime.assembly_type = MagicMock()
    metadata.runtime.konflux_db = None
    return metadata


@patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
class TestEcVerificationGating(IsolatedAsyncioTestCase):
    """Test that EC verification is only triggered under the correct conditions."""

    async def _run_build_and_get_ec_calls(self, config, metadata, mock_kc_init):
        """Helper: run build() with all heavy methods mocked, return EC-related call info."""
        builder = KonfluxImageBuilder(config)

        plr_info = _make_successful_pipelinerun_info()

        builder._start_build = AsyncMock(return_value=plr_info)
        builder._validate_build_attestation_and_signature = AsyncMock()
        builder._wait_for_parent_members = AsyncMock(return_value=[])
        builder.update_konflux_db = AsyncMock(return_value=None)
        builder._parse_dockerfile = MagicMock(
            return_value=("uuid-tag", "test-component", "v4.18.0", "202604100000.p0.el9")
        )

        # Make dest_dir.exists() return True so we skip cloning
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

                # Mock wait_for_pipelinerun to return a successful PLR
                wait_plr_info = _make_successful_pipelinerun_info()
                builder._konflux_client.wait_for_pipelinerun = AsyncMock(return_value=wait_plr_info)
                builder._konflux_client.resource_url = MagicMock(return_value="https://example.com/plr")

                # Mock EC methods
                builder._konflux_client.ensure_integration_test_scenario = AsyncMock()
                ec_plr_info = MagicMock()
                ec_plr_info.name = "test-ec-plr"
                ec_plr_info.to_dict.return_value = {
                    "kind": "PipelineRun",
                    "metadata": {
                        "name": "test-ec-plr",
                        "namespace": "ocp-art-tenant",
                        "labels": {"appstudio.openshift.io/application": "openshift-4-18"},
                    },
                }
                ec_succeeded = MagicMock()
                ec_plr_info.find_condition.return_value = ec_succeeded
                builder._konflux_client.start_ec_pipeline_run = AsyncMock(return_value=ec_plr_info)

                # Make the second wait_for_pipelinerun call (for EC) also return success
                builder._konflux_client.wait_for_pipelinerun = AsyncMock(side_effect=[wait_plr_info, ec_plr_info])

                with patch.object(
                    KonfluxBuildOutcome,
                    "extract_from_pipelinerun_succeeded_condition",
                    return_value=KonfluxBuildOutcome.SUCCESS,
                ):
                    await builder.build(metadata)

        return builder._konflux_client.ensure_integration_test_scenario, builder._konflux_client.start_ec_pipeline_run

    async def test_ec_runs_for_release_ocp_image(self, mock_kc_init):
        """EC verification should run for a for_release=True OCP image."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=True)

        ensure_its, start_ec_plr = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        ensure_its.assert_called_once()
        start_ec_plr.assert_called_once()

    async def test_ec_skipped_for_non_release_image(self, mock_kc_init):
        """EC verification should be skipped for base images (for_release=False)."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=False)

        ensure_its, start_ec_plr = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        ensure_its.assert_not_called()
        start_ec_plr.assert_not_called()

    async def test_ec_skipped_when_skip_flag_set(self, mock_kc_init):
        """EC verification should be skipped when --skip-ec-verify is set."""
        config = _make_config(group_name="openshift-4.18", skip_ec_verify=True)
        metadata = _make_metadata(for_release=True)

        ensure_its, start_ec_plr = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        ensure_its.assert_not_called()
        start_ec_plr.assert_not_called()

    async def test_ec_skipped_for_non_ocp_group(self, mock_kc_init):
        """EC verification should be skipped for non-OCP groups (e.g. logging)."""
        config = _make_config(group_name="logging-6.2")
        metadata = _make_metadata(for_release=True)

        ensure_its, start_ec_plr = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        ensure_its.assert_not_called()
        start_ec_plr.assert_not_called()
