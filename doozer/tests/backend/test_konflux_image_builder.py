import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from doozerlib.backend.konflux_image_builder import KonfluxImageBuilder, KonfluxImageBuilderConfig
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo


class TestKonfluxImageBuilder(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.konflux_client_patcher = patch("doozerlib.backend.konflux_image_builder.KonfluxClient.from_kubeconfig")
        self.mock_konflux_client_factory = self.konflux_client_patcher.start()
        self.addCleanup(self.konflux_client_patcher.stop)

        self.mock_konflux_client = MagicMock()
        self.mock_konflux_client_factory.return_value = self.mock_konflux_client
        self.mock_konflux_client.resource_url.return_value = "https://example.com/pipelinerun"

        self.builder = KonfluxImageBuilder(
            KonfluxImageBuilderConfig(
                base_dir=Path(self.temp_dir.name),
                group_name="openshift-4.17",
                namespace="test-namespace",
                plr_template="test-template",
                build_priority="5",
            )
        )

    def _metadata(self):
        metadata = MagicMock()
        metadata.distgit_key = "test-image"
        metadata.qualified_key = "containers/test-image"
        metadata.image_name_short = "test-image"
        metadata.is_olm_operator = False
        metadata.build_status = False
        metadata.build_event = MagicMock()
        metadata.runtime = MagicMock()
        metadata.runtime.assembly = "test-assembly"
        metadata.runtime.konflux_db = MagicMock()
        metadata.runtime.group_config.software_lifecycle.phase = "release"
        metadata.for_release = True
        metadata.get_latest_build = AsyncMock(return_value=None)
        metadata.get_konflux_build_attempts.return_value = 1
        metadata.get_arches.return_value = ["x86_64"]
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.is_base_image.return_value = False
        return metadata

    async def test_build_uses_definitive_pullspec_for_attestation_validation(self):
        metadata = self._metadata()
        dest_dir = self.builder._config.base_dir.joinpath(metadata.qualified_key)
        dest_dir.mkdir(parents=True)

        build_repo = MagicMock()
        build_repo.local_dir = dest_dir
        build_repo.url = "https://github.com/test/repo.git"
        build_repo.commit_hash = "test-commit"

        initial_pipelinerun = MagicMock()
        initial_pipelinerun.name = "test-pipelinerun"
        initial_pipelinerun.to_dict.return_value = {"metadata": {"name": "test-pipelinerun"}}

        completed_pipelinerun = MagicMock()
        completed_pipelinerun.name = "test-pipelinerun"
        completed_pipelinerun.find_condition.return_value = {"status": "True"}
        completed_pipelinerun.to_dict.return_value = {
            "metadata": {"name": "test-pipelinerun"},
            "status": {
                "results": [
                    {"name": "IMAGE_URL", "value": "quay.io/test/image:test-tag"},
                    {"name": "IMAGE_DIGEST", "value": "sha256:testdigest"},
                ]
            },
        }
        self.mock_konflux_client.wait_for_pipelinerun = AsyncMock(return_value=completed_pipelinerun)

        # Mock EC verification result
        ec_result_mock = MagicMock()
        ec_result_mock.ec_status = "PASSED"
        ec_result_mock.ec_pipeline_url = "https://example.com/ec-pipeline"
        ec_result_mock.ec_failed = False

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(self.builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(self.builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(self.builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))),
            patch.object(self.builder, "_validate_build_attestation_and_signature", new=AsyncMock()) as mock_validate,
            patch.object(
                self.builder._konflux_client, "verify_enterprise_contract", new=AsyncMock(return_value=ec_result_mock)
            ),
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            await self.builder.build(metadata)

        mock_validate.assert_awaited_once_with("quay.io/test/image@sha256:testdigest", "test-image")

    async def test_build_skips_slsa_validation_for_non_ocp_groups(self):
        """Test that SLSA attestation validation is skipped for non-OCP groups like OKD."""
        # Create a builder with an OKD group name
        okd_builder = KonfluxImageBuilder(
            KonfluxImageBuilderConfig(
                base_dir=Path(self.temp_dir.name),
                group_name="okd-4.17",
                namespace="test-namespace",
                plr_template="test-template",
                build_priority="5",
            )
        )

        metadata = self._metadata()
        dest_dir = okd_builder._config.base_dir.joinpath(metadata.qualified_key)
        dest_dir.mkdir(parents=True)

        build_repo = MagicMock()
        build_repo.local_dir = dest_dir
        build_repo.url = "https://github.com/test/okd-repo.git"
        build_repo.commit_hash = "test-okd-commit"

        initial_pipelinerun = MagicMock()
        initial_pipelinerun.name = "test-pipelinerun"
        initial_pipelinerun.to_dict.return_value = {"metadata": {"name": "test-pipelinerun"}}

        completed_pipelinerun = MagicMock()
        completed_pipelinerun.name = "test-pipelinerun"
        completed_pipelinerun.find_condition.return_value = {"status": "True"}
        completed_pipelinerun.to_dict.return_value = {
            "metadata": {"name": "test-pipelinerun"},
            "status": {
                "results": [
                    {"name": "IMAGE_URL", "value": "quay.io/test/okd-image:test-tag"},
                    {"name": "IMAGE_DIGEST", "value": "sha256:okddigest"},
                ]
            },
        }
        self.mock_konflux_client.wait_for_pipelinerun = AsyncMock(return_value=completed_pipelinerun)

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(okd_builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(okd_builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(okd_builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(okd_builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))),
            patch.object(okd_builder, "_validate_build_attestation_and_signature", new=AsyncMock()) as mock_validate,
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            await okd_builder.build(metadata)

        # Validation should NOT be called for OKD groups
        mock_validate.assert_not_awaited()

    async def test_update_konflux_db_uses_definitive_pullspec_for_installed_packages(self):
        metadata = self._metadata()
        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "test-commit-hash"
        build_repo.local_dir = Path(self.temp_dir.name)

        pipelinerun = PipelineRunInfo(
            {
                "metadata": {
                    "name": "test-pipelinerun",
                    "uid": "test-uid",
                    "labels": {"appstudio.openshift.io/component": "test-component"},
                },
                "status": {
                    "results": [
                        {"name": "IMAGE_URL", "value": "quay.io/test/image:test-tag"},
                        {"name": "IMAGE_DIGEST", "value": "sha256:testdigest"},
                    ],
                    "startTime": "2023-10-01T12:00:00Z",
                    "completionTime": "2023-10-01T12:30:00Z",
                },
            },
            {},
        )

        with (
            patch("doozerlib.backend.konflux_image_builder.DockerfileParser") as mock_dockerfile_parser,
            patch.object(self.builder, "extract_parent_image_nvrs", new=AsyncMock(return_value=[])),
            patch.object(
                self.builder,
                "get_installed_packages",
                new=AsyncMock(return_value=({"pkg-1.0-1"}, {"srcpkg-1.0-1"})),
            ) as mock_get_installed_packages,
            patch("doozerlib.backend.konflux_image_builder.bigquery.BigQueryClient") as mock_bigquery_client,
        ):
            mock_dockerfile = MagicMock()
            mock_dockerfile.labels = {
                "io.openshift.build.source-location": "https://example.com/source-repo.git",
                "io.openshift.build.commit.id": "source-commit-id",
                "com.redhat.component": "test-component",
                "version": "1.0",
                "release": "1.el9",
            }
            mock_dockerfile.parent_images = []
            mock_dockerfile_parser.return_value = mock_dockerfile
            mock_bigquery_client.return_value.client.insert_rows_json.return_value = None

            await self.builder.update_konflux_db(
                metadata,
                build_repo,
                pipelinerun,
                KonfluxBuildOutcome.SUCCESS,
                ["x86_64"],
                "5",
            )

        mock_get_installed_packages.assert_awaited_once_with("quay.io/test/image@sha256:testdigest", ["x86_64"], None)

    async def test_ec_policy_selection_missing_lifecycle_phase(self):
        """Test that default EC policy is used when lifecycle phase is Missing."""
        from artcommonlib.model import Missing
        from doozerlib import constants

        metadata = self._metadata()
        # Set lifecycle phase to Missing
        metadata.runtime.group_config.software_lifecycle.phase = Missing

        dest_dir = self.builder._config.base_dir.joinpath(metadata.qualified_key)
        dest_dir.mkdir(parents=True)

        build_repo = MagicMock()
        build_repo.local_dir = dest_dir
        build_repo.url = "https://github.com/test/repo.git"
        build_repo.commit_hash = "test-commit"

        initial_pipelinerun = MagicMock()
        initial_pipelinerun.name = "test-pipelinerun"
        initial_pipelinerun.to_dict.return_value = {"metadata": {"name": "test-pipelinerun"}}

        completed_pipelinerun = MagicMock()
        completed_pipelinerun.name = "test-pipelinerun"
        completed_pipelinerun.find_condition.return_value = {"status": "True"}
        completed_pipelinerun.to_dict.return_value = {
            "metadata": {"name": "test-pipelinerun"},
            "status": {
                "results": [
                    {"name": "IMAGE_URL", "value": "quay.io/test/image:test-tag"},
                    {"name": "IMAGE_DIGEST", "value": "sha256:testdigest"},
                ]
            },
        }
        self.mock_konflux_client.wait_for_pipelinerun = AsyncMock(return_value=completed_pipelinerun)

        ec_result_mock = MagicMock()
        ec_result_mock.ec_status = "PASSED"
        ec_result_mock.ec_pipeline_url = "https://example.com/ec-pipeline"
        ec_result_mock.ec_failed = False

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(self.builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(self.builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(self.builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))),
            patch.object(self.builder, "_validate_build_attestation_and_signature", new=AsyncMock()),
            patch.object(
                self.builder._konflux_client, "verify_enterprise_contract", new=AsyncMock(return_value=ec_result_mock)
            ) as mock_verify_ec,
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            await self.builder.build(metadata)

        # Verify that default EC policy was used (not the pre-GA policy)
        mock_verify_ec.assert_awaited_once()
        call_kwargs = mock_verify_ec.await_args[1]
        self.assertEqual(call_kwargs['ec_policy'], constants.KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION)
