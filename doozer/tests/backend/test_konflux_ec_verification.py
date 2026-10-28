"""Tests for EC verification gating logic in KonfluxImageBuilder.build().

Verifies build-time EC and custom ITS eligibility, including the per-image
override, without bypassing other prerequisites or explicit skips.
"""

import asyncio
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.conforma import resolve_stage_policy
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from doozerlib import constants
from doozerlib.backend.konflux_client import CustomIntegrationTestResult, ECVerificationResult
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
        ec_policy_configuration=constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION,
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


def _make_metadata(distgit_key="test-image", for_release=True, is_base_image=False, variant=None):
    """Create a mock ImageMetadata."""
    metadata = MagicMock()
    metadata.distgit_key = distgit_key
    metadata.qualified_key = f"containers/{distgit_key}"
    metadata.image_name_short = distgit_key
    metadata.is_olm_operator = False
    metadata.for_release = for_release
    metadata.config = Model({})
    metadata.is_base_image.return_value = is_base_image
    metadata.has_source.return_value = True
    metadata.get_konflux_build_attempts.return_value = 1
    metadata.get_arches.return_value = ["x86_64"]
    metadata.get_latest_build = AsyncMock(return_value=None)
    metadata.build_event = asyncio.Event()
    metadata.get_parent_members.return_value = {}
    metadata.runtime.assembly = "stream"
    metadata.runtime.variant = variant if variant is not None else BuildVariant.OCP
    metadata.runtime.group_config.software_lifecycle.phase = "release"
    metadata.runtime.konflux_db = MagicMock()
    metadata.runtime.konflux_db.get_latest_build = AsyncMock(return_value=None)
    return metadata


def _ec_passed_result():
    return ECVerificationResult(ec_pipeline_url="https://example.com/ec-plr", ec_failed=False)


def _ec_failed_result():
    return ECVerificationResult(ec_pipeline_url="https://example.com/ec-plr", ec_failed=True)


def _ec_exception_result():
    """EC verification failed due to exception (e.g., ITS creation failed) before PLR was created."""
    return ECVerificationResult(ec_pipeline_url="", ec_failed=True)


@patch("doozerlib.backend.konflux_client.KonfluxClient.from_kubeconfig")
class TestEcVerificationGating(IsolatedAsyncioTestCase):
    """Test that EC verification is only triggered under the correct conditions."""

    async def _run_build_and_get_ec_calls(
        self,
        config,
        metadata,
        mock_kc_init,
        ec_result=None,
        custom_its_result=None,
        blocking_custom_its=None,
        completed_plr_info=None,
        expect_build_error=False,
        build_outcome=KonfluxBuildOutcome.SUCCESS,
    ):
        """Helper: run build() with all heavy methods mocked, return verify_enterprise_contract mock."""
        if ec_result is None:
            ec_result = _ec_passed_result()

        builder = KonfluxImageBuilder(config)
        builder._blocking_custom_integration_test_scenarios = set(blocking_custom_its or ())
        self.last_builder = builder

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

                if completed_plr_info is None:
                    completed_plr_info = _make_successful_pipelinerun_info()
                builder._konflux_client.wait_for_pipelinerun = AsyncMock(return_value=completed_plr_info)
                builder._konflux_client.resource_url = MagicMock(return_value="https://example.com/plr")

                builder._konflux_client.verify_enterprise_contract = AsyncMock(return_value=ec_result)
                builder._konflux_client.run_integration_test_scenarios = AsyncMock(
                    return_value=custom_its_result or CustomIntegrationTestResult(False, "", [])
                )

                with patch.object(
                    KonfluxBuildOutcome,
                    "extract_from_pipelinerun_succeeded_condition",
                    return_value=build_outcome,
                ):
                    if expect_build_error:
                        with self.assertRaises(KonfluxImageBuildError):
                            await builder.build(metadata)
                    else:
                        await builder.build(metadata)

        return builder._konflux_client.verify_enterprise_contract

    async def test_image_its_enabled_eligibility(self, mock_kc_init):
        for for_release, its_enabled, expected in (
            (False, None, False),
            (False, "unset", False),
            (False, False, False),
            (False, True, True),
            (True, None, True),
            (True, "unset", True),
            (True, False, False),
            (True, True, True),
        ):
            with self.subTest(for_release=for_release, its_enabled=its_enabled):
                config = _make_config(integration_test_scenarios=("qe-test",))
                metadata = _make_metadata(for_release=for_release)
                if its_enabled == "unset":
                    metadata.config = Model({"konflux": {"its": {}}})
                elif its_enabled is not None:
                    metadata.config = Model({"konflux": {"its": {"enabled": its_enabled}}})
                metadata.runtime.group_config.konflux.its.enabled = not expected

                verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

                self.assertEqual(verify_ec.await_count, int(expected))
                self.assertEqual(
                    self.last_builder._konflux_client.run_integration_test_scenarios.await_count, int(expected)
                )
                self.assertEqual(metadata.for_release, for_release)

    async def test_its_enabled_runs_for_non_release_base_image(self, mock_kc_init):
        config = _make_config(integration_test_scenarios=("qe-test",))
        metadata = _make_metadata(for_release=False, is_base_image=True)
        metadata.config = Model({"base_only": True, "konflux": {"its": {"enabled": True}}})

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        verify_ec.assert_awaited_once()
        self.last_builder._konflux_client.run_integration_test_scenarios.assert_awaited_once()
        self.assertFalse(metadata.for_release)
        self.assertTrue(metadata.config.base_only)

    async def test_its_enabled_preserves_independent_explicit_skips(self, mock_kc_init):
        for skip_ec, skip_custom in ((True, False), (False, True), (True, True)):
            with self.subTest(skip_ec=skip_ec, skip_custom=skip_custom):
                config = _make_config(
                    integration_test_scenarios=("qe-test",), skip_ec_verify=skip_ec, skip_custom_its=skip_custom
                )
                metadata = _make_metadata(for_release=False)
                metadata.config = Model({"konflux": {"its": {"enabled": True}}})

                verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

                self.assertEqual(verify_ec.await_count, int(not skip_ec))
                self.assertEqual(
                    self.last_builder._konflux_client.run_integration_test_scenarios.await_count, int(not skip_custom)
                )

    async def test_its_enabled_preserves_policy_and_scenario_prerequisites(self, mock_kc_init):
        for policy, scenarios, variant, phase, expected_ec, expected_custom in (
            (None, ("qe-test",), BuildVariant.OCP, "release", False, True),
            (None, ("qe-test",), BuildVariant.OCP, "pre-release", False, True),
            ("policy", (), BuildVariant.OCP, "release", True, False),
            ("policy", ("qe-test",), BuildVariant.OKD, "release", False, True),
        ):
            with self.subTest(policy=policy, scenarios=scenarios, variant=variant, phase=phase):
                config = _make_config(
                    ec_policy_configuration=policy,
                    integration_test_scenarios=scenarios,
                )
                metadata = _make_metadata(for_release=False, variant=variant)
                metadata.config = Model({"konflux": {"its": {"enabled": True}}})
                metadata.runtime.group_config.software_lifecycle.phase = phase

                verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

                self.assertEqual(verify_ec.await_count, int(expected_ec))
                self.assertEqual(
                    self.last_builder._konflux_client.run_integration_test_scenarios.await_count, int(expected_custom)
                )

    async def test_its_enabled_preserves_policy_selection_and_effective_time(self, mock_kc_init):
        for phase, expected_policy in (("release", "release-policy"), ("pre-release", "prega-policy")):
            with self.subTest(phase=phase):
                config = _make_config(
                    ec_policy_configuration=expected_policy,
                    effective_time="2026-04-10T00:00:00Z",
                )
                metadata = _make_metadata(for_release=False)
                metadata.config = Model({"konflux": {"its": {"enabled": True}}})
                metadata.runtime.group_config.software_lifecycle.phase = phase

                verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

                verify_ec.assert_awaited_once()
                self.assertEqual(verify_ec.call_args.kwargs["ec_policy"], expected_policy)
                self.assertEqual(verify_ec.call_args.kwargs["effective_time"], config.effective_time)

    async def test_its_enabled_requires_pullspec_results(self, mock_kc_init):
        for missing_results in (("IMAGE_URL",), ("IMAGE_DIGEST",), ("IMAGE_URL", "IMAGE_DIGEST")):
            with self.subTest(missing_results=missing_results):
                config = _make_config(integration_test_scenarios=("qe-test",))
                metadata = _make_metadata(for_release=False)
                metadata.config = Model({"konflux": {"its": {"enabled": True}}})
                completed = _make_successful_pipelinerun_info()
                results = completed.to_dict.return_value["status"]["results"]
                completed.to_dict.return_value["status"]["results"] = [
                    result for result in results if result["name"] not in missing_results
                ]

                verify_ec = await self._run_build_and_get_ec_calls(
                    config, metadata, mock_kc_init, completed_plr_info=completed
                )

                verify_ec.assert_not_awaited()
                self.last_builder._konflux_client.run_integration_test_scenarios.assert_not_awaited()

    async def test_its_enabled_requires_successful_build(self, mock_kc_init):
        config = _make_config(integration_test_scenarios=("qe-test",))
        metadata = _make_metadata(for_release=False)
        metadata.config = Model({"konflux": {"its": {"enabled": True}}})

        verify_ec = await self._run_build_and_get_ec_calls(
            config, metadata, mock_kc_init, build_outcome=KonfluxBuildOutcome.BUILD_ERROR, expect_build_error=True
        )

        verify_ec.assert_not_awaited()
        self.last_builder._konflux_client.run_integration_test_scenarios.assert_not_awaited()

    async def test_its_enabled_preserves_failure_outcomes(self, mock_kc_init):
        failed_url = "https://example.com/pipelineruns/verification-failed"
        for failure in ("ec", "blocking-custom", "optional-custom"):
            with self.subTest(failure=failure):
                blocking_failed = failure == "blocking-custom"
                expect_error = failure != "optional-custom"
                config = _make_config(dry_run=False, integration_test_scenarios=("qe-test",))
                metadata = _make_metadata(for_release=False)
                metadata.config = Model({"konflux": {"its": {"enabled": True}}})
                metadata.should_trigger_base_image_release.return_value = False
                metadata.should_create_golang_builder_shipment.return_value = False

                verify_ec = await self._run_build_and_get_ec_calls(
                    config,
                    metadata,
                    mock_kc_init,
                    ec_result=_ec_failed_result() if failure == "ec" else _ec_passed_result(),
                    custom_its_result=CustomIntegrationTestResult(
                        blocking_failed, failed_url if blocking_failed else "", [failed_url]
                    ),
                    blocking_custom_its={"qe-test"} if blocking_failed else set(),
                    expect_build_error=expect_error,
                )

                verify_ec.assert_awaited_once()
                self.assertEqual(
                    self.last_builder._konflux_client.run_integration_test_scenarios.await_count,
                    int(failure != "ec"),
                )
                completion = self.last_builder.update_konflux_db.await_args_list[-1]
                expected_outcome = KonfluxBuildOutcome.ITS_ERROR if expect_error else KonfluxBuildOutcome.SUCCESS
                self.assertEqual(completion.args[3], expected_outcome)
                if blocking_failed:
                    self.assertEqual(completion.kwargs["ec_pipeline_url"], failed_url)
                elif failure == "ec":
                    self.assertEqual(completion.kwargs["ec_pipeline_url"], _ec_failed_result().ec_pipeline_url)

    async def test_custom_its_preflight_stores_explicit_blocking_scenarios(self, mock_kc_init):
        config = _make_config(integration_test_scenarios=("qe-optional", "qe-blocking"))
        mock_kc_init.return_value.validate_integration_test_scenarios = AsyncMock(return_value={"qe-blocking"})
        builder = KonfluxImageBuilder(config)

        await builder.validate_custom_integration_test_scenarios()

        self.assertEqual(builder._blocking_custom_integration_test_scenarios, {"qe-blocking"})

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

    async def test_ec_skipped_when_no_policy_configured(self, mock_kc_init):
        """EC verification should be skipped when no EC policy is configured for the product."""
        config = _make_config(group_name="logging-6.2", ec_policy_configuration=None)
        metadata = _make_metadata(for_release=True)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_ec_skipped_for_okd_variant(self, mock_kc_init):
        """EC verification should be skipped for OKD variant (not OCP)."""
        config = _make_config(group_name="openshift-5.0")
        metadata = _make_metadata(for_release=True, variant=BuildVariant.OKD)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_not_called()

    async def test_ec_skipped_when_prega_policy_is_not_configured(self, mock_kc_init):
        config = _make_config(ec_policy_configuration=None)
        metadata = _make_metadata(for_release=True)
        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        verify_ec.assert_not_called()

    async def test_custom_its_runs_independently_of_ec(self, mock_kc_init):
        config = _make_config(
            ec_policy_configuration=None,
            integration_test_scenarios=("qe-test",),
            integration_test_snapshot_annotations={"pac.test.appstudio.openshift.io/branch": "release-4.18"},
        )
        metadata = _make_metadata(for_release=True)

        await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        mock_kc_init.return_value.run_integration_test_scenarios.assert_awaited_once_with(
            scenario_names=("qe-test",),
            application_name="openshift-4-18",
            component_name=metadata.get_konflux_component_name.return_value,
            image_pullspec="quay.io/openshift-release-dev/ocp-v4.0-art-dev-test@sha256:abc123def456",
            source_url="https://github.com/openshift/test-repo",
            commit_sha="deadbeef",
            blocking_scenario_names=set(),
            snapshot_annotations={"pac.test.appstudio.openshift.io/branch": "release-4.18"},
            namespace="ocp-art-tenant",
        )

    async def test_custom_its_skipped_in_dry_run_without_image_results(self, mock_kc_init):
        config = _make_config(
            dry_run=True,
            integration_test_scenarios=("qe-test",),
        )
        metadata = _make_metadata(for_release=True)
        completed_plr_info = _make_successful_pipelinerun_info()
        completed_plr_info.to_dict.return_value["status"]["results"] = []

        verify_ec = await self._run_build_and_get_ec_calls(
            config,
            metadata,
            mock_kc_init,
            completed_plr_info=completed_plr_info,
        )

        verify_ec.assert_not_awaited()
        mock_kc_init.return_value.run_integration_test_scenarios.assert_not_awaited()
        self.assertTrue(metadata.build_status)

    async def test_skip_custom_its_does_not_skip_ec(self, mock_kc_init):
        config = _make_config(integration_test_scenarios=("qe-test",), skip_custom_its=True)
        metadata = _make_metadata(for_release=True)

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)

        verify_ec.assert_awaited_once()
        mock_kc_init.return_value.run_integration_test_scenarios.assert_not_awaited()

    async def test_custom_its_failure_sets_its_error_and_pipeline_url(self, mock_kc_init):
        config = _make_config(
            dry_run=False,
            ec_policy_configuration=None,
            integration_test_scenarios=("qe-test",),
        )
        metadata = _make_metadata(for_release=True)
        metadata.should_trigger_base_image_release.return_value = False
        metadata.should_create_golang_builder_shipment.return_value = False
        failed_url = "https://example.com/pipelineruns/qe-test-123"

        await self._run_build_and_get_ec_calls(
            config,
            metadata,
            mock_kc_init,
            custom_its_result=CustomIntegrationTestResult(True, failed_url, [failed_url]),
            blocking_custom_its={"qe-test"},
            expect_build_error=True,
        )

        completion_call = self.last_builder.update_konflux_db.await_args_list[-1]
        self.assertEqual(completion_call.args[3], KonfluxBuildOutcome.ITS_ERROR)
        self.assertEqual(completion_call.kwargs["ec_pipeline_url"], failed_url)

    async def test_blocking_custom_its_failure_without_url_does_not_retry(self, mock_kc_init):
        config = _make_config(
            dry_run=False,
            ec_policy_configuration=None,
            integration_test_scenarios=("qe-test",),
        )
        metadata = _make_metadata(for_release=True)
        metadata.get_konflux_build_attempts.return_value = 3
        metadata.should_trigger_base_image_release.return_value = False
        metadata.should_create_golang_builder_shipment.return_value = False

        await self._run_build_and_get_ec_calls(
            config,
            metadata,
            mock_kc_init,
            custom_its_result=CustomIntegrationTestResult(True, "", []),
            blocking_custom_its={"qe-test"},
            expect_build_error=True,
        )

        self.last_builder._start_build.assert_awaited_once()
        completion_call = self.last_builder.update_konflux_db.await_args_list[-1]
        self.assertEqual(completion_call.args[3], KonfluxBuildOutcome.ITS_ERROR)
        self.assertEqual(completion_call.kwargs["ec_pipeline_url"], "")

    async def test_optional_custom_its_failure_does_not_fail_build(self, mock_kc_init):
        config = _make_config(
            dry_run=False,
            ec_policy_configuration=None,
            integration_test_scenarios=("qe-test",),
        )
        metadata = _make_metadata(for_release=True)
        metadata.should_trigger_base_image_release.return_value = False
        metadata.should_create_golang_builder_shipment.return_value = False
        failed_url = "https://example.com/pipelineruns/qe-test-123"
        await self._run_build_and_get_ec_calls(
            config,
            metadata,
            mock_kc_init,
            custom_its_result=CustomIntegrationTestResult(False, "", [failed_url]),
        )

        completion_call = self.last_builder.update_konflux_db.await_args_list[-1]
        self.assertEqual(completion_call.args[3], KonfluxBuildOutcome.SUCCESS)

    async def test_no_retry_when_ec_fails(self, mock_kc_init):
        """When EC verification fails, the build should NOT be retried."""
        config = _make_config(group_name="openshift-4.18", dry_run=False)
        metadata = _make_metadata(for_release=True)
        metadata.get_konflux_build_attempts.return_value = 3

        record_logger = MagicMock()
        builder = KonfluxImageBuilder(config, record_logger=record_logger)

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
        self.assertEqual(builder.update_konflux_db.await_count, 2)
        completion_call = builder.update_konflux_db.await_args_list[1]
        self.assertEqual(completion_call.args[3], KonfluxBuildOutcome.ITS_ERROR)

        record_logger.add_record.assert_called_once()
        add_args, add_kwargs = record_logger.add_record.call_args
        self.assertEqual(add_args[0], 'image_build_konflux')
        self.assertEqual(add_kwargs['outcome'], str(KonfluxBuildOutcome.ITS_ERROR))

    async def test_retry_when_ec_exception_without_plr_url(self, mock_kc_init):
        """
        When EC verification fails due to exception (e.g., ITS creation failed) before
        a PipelineRun is created, ec_failed=True but ec_pipeline_url=''. This should:
        1. Allow retries (not break the retry loop)
        2. Be counted as a build failure, not EC failure (for pyartcd metrics)
        """
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
                builder._konflux_client.verify_enterprise_contract = AsyncMock(return_value=_ec_exception_result())

                with patch.object(
                    KonfluxBuildOutcome,
                    "extract_from_pipelinerun_succeeded_condition",
                    return_value=KonfluxBuildOutcome.SUCCESS,
                ):
                    with self.assertRaises(KonfluxImageBuildError):
                        await builder.build(metadata)

        # Verify that all retries were attempted (exception path allows retries)
        self.assertEqual(builder._start_build.call_count, 3)

    async def test_ec_uses_default_policy_for_release_phase(self, mock_kc_init):
        """Default (non-test) assembly with release lifecycle uses the default stage policy."""
        config = _make_config(group_name="openshift-4.18")
        metadata = _make_metadata(for_release=True)
        metadata.runtime.group_config.software_lifecycle.phase = "release"

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()
        call_kwargs = verify_ec.call_args
        self.assertEqual(call_kwargs.kwargs["ec_policy"], constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION)

    async def test_ec_uses_prega_policy_for_pre_release_phase(self, mock_kc_init):
        """Pre-release lifecycle phase uses the prega policy from config."""
        config = _make_config(ec_policy_configuration=resolve_stage_policy("ocp", lifecycle_phase="pre-release"))
        metadata = _make_metadata(for_release=True)
        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()
        call_kwargs = verify_ec.call_args
        self.assertEqual(call_kwargs.kwargs["ec_policy"], constants.KONFLUX_PREGA_EC_POLICY_CONFIGURATION)

    async def test_ec_uses_test_policy_from_config(self, mock_kc_init):
        """Test assembly builds use the test-specific EC policy passed via config."""
        config = _make_config(
            group_name="openshift-4.18",
            ec_policy_configuration=constants.KONFLUX_TEST_EC_POLICY_CONFIGURATION,
        )
        metadata = _make_metadata(for_release=True)
        metadata.runtime.group_config.software_lifecycle.phase = "release"

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()
        call_kwargs = verify_ec.call_args
        self.assertEqual(call_kwargs.kwargs["ec_policy"], constants.KONFLUX_TEST_EC_POLICY_CONFIGURATION)

    async def test_ec_uses_test_prega_policy_for_pre_release(self, mock_kc_init):
        """Test assembly builds with pre-release lifecycle use the test prega policy from config."""
        config = _make_config(
            group_name="openshift-4.18",
            ec_policy_configuration=resolve_stage_policy("ocp", assembly="test", lifecycle_phase="pre-release"),
        )
        metadata = _make_metadata(for_release=True)
        metadata.runtime.group_config.software_lifecycle.phase = "pre-release"

        verify_ec = await self._run_build_and_get_ec_calls(config, metadata, mock_kc_init)
        verify_ec.assert_called_once()
        call_kwargs = verify_ec.call_args
        self.assertEqual(call_kwargs.kwargs["ec_policy"], constants.KONFLUX_TEST_PREGA_EC_POLICY_CONFIGURATION)
