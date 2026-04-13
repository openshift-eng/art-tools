"""Tests for EC verification gating logic in KonfluxImageBuilder.build().

Verifies that enterprise-contract verification is only triggered for images
that are for_release=True, with a product configured in PRODUCT_EC_POLICY_MAP,
and not skipped via --skip-ec-verify.
"""

import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxECStatus
from doozerlib import constants
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


def _make_metadata(distgit_key="test-image", for_release=True, is_base_image=False, product="ocp"):
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
    metadata.runtime.group_config.software_lifecycle.phase = "release"
    metadata.runtime.konflux_db = None
    metadata.runtime.product = product
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

    async def test_ec_runs_for_layered_product(self, mock_kc_init):
        """EC verification should run for layered products with a configured EC policy."""
        config = _make_config(group_name="logging-6.2")
        metadata = _make_metadata(for_release=True, product="logging")

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()
        call_kwargs = verify_ec.call_args.kwargs
        self.assertEqual(call_kwargs["ec_policy"], "rhtap-releng-tenant/registry-art-logging-stage")

    async def test_ec_runs_for_oadp_with_correct_policy(self, mock_kc_init):
        """EC verification should use the OADP-specific policy for OADP builds."""
        config = _make_config(group_name="oadp-1.5")
        metadata = _make_metadata(for_release=True, product="oadp")

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()
        call_kwargs = verify_ec.call_args.kwargs
        self.assertEqual(call_kwargs["ec_policy"], "rhtap-releng-tenant/registry-art-oadp-stage")

    async def test_ec_skipped_for_unknown_product(self, mock_kc_init):
        """EC verification should be skipped for products not in PRODUCT_EC_POLICY_MAP."""
        config = _make_config(group_name="unknown-1.0")
        metadata = _make_metadata(for_release=True, product="unknown")

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_no_retry_when_ec_fails(self, mock_kc_init):
        """When EC verification fails, the build should NOT be retried."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=True)
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


class TestEcPolicyHelpers(TestCase):
    """Test the product-to-EC-policy resolution helpers."""

    def test_ocp_registry_policy(self):
        self.assertEqual(
            constants.get_ec_policy_for_product("ocp"),
            "rhtap-releng-tenant/registry-ocp-art-stage",
        )

    def test_logging_registry_policy(self):
        self.assertEqual(
            constants.get_ec_policy_for_product("logging"),
            "rhtap-releng-tenant/registry-art-logging-stage",
        )

    def test_oadp_registry_policy(self):
        self.assertEqual(
            constants.get_ec_policy_for_product("oadp"),
            "rhtap-releng-tenant/registry-art-oadp-stage",
        )

    def test_mta_registry_policy(self):
        self.assertEqual(
            constants.get_ec_policy_for_product("mta"),
            "rhtap-releng-tenant/registry-art-mta-stage",
        )

    def test_rhmtc_registry_policy(self):
        self.assertEqual(
            constants.get_ec_policy_for_product("rhmtc"),
            "rhtap-releng-tenant/registry-art-mtc-stage",
        )

    def test_unknown_product_falls_back_to_default(self):
        self.assertEqual(
            constants.get_ec_policy_for_product("unknown"),
            constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION,
        )

    def test_oadp_fbc_policy(self):
        self.assertEqual(
            constants.get_fbc_ec_policy_for_product("oadp"),
            "rhtap-releng-tenant/fbc-art-oadp-stage",
        )

    def test_logging_fbc_policy(self):
        self.assertEqual(
            constants.get_fbc_ec_policy_for_product("logging"),
            "rhtap-releng-tenant/fbc-ocp-art-stage",
        )

    def test_ocp_not_in_fbc_policy_map(self):
        """OCP FBC EC was removed (verified at release time); should fall back to default."""
        self.assertNotIn("ocp", constants.PRODUCT_FBC_EC_POLICY_MAP)
        self.assertEqual(
            constants.get_fbc_ec_policy_for_product("ocp"),
            constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION,
        )

    def test_unknown_product_fbc_falls_back_to_default(self):
        self.assertEqual(
            constants.get_fbc_ec_policy_for_product("unknown"),
            constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION,
        )
