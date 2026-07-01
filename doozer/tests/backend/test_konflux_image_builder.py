import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from doozerlib.backend.konflux_image_builder import (
    KonfluxImageBuilder,
    KonfluxImageBuilderConfig,
    KonfluxImageBuildError,
    _normalize_version,
)
from doozerlib.backend.pipelinerun_utils import PipelineRunInfo
from doozerlib.util import konflux_golang_builder_component_name


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
        metadata.config.konflux.get.return_value = False
        metadata.for_release = True
        metadata.get_latest_build = AsyncMock(return_value=None)
        metadata.get_konflux_build_attempts.return_value = 1
        metadata.get_arches.return_value = ["x86_64"]
        metadata.get_konflux_network_mode.return_value = "open"
        metadata.is_base_image.return_value = False
        metadata.should_trigger_base_image_release.return_value = False
        metadata.is_golang_builder = MagicMock(return_value=False)
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

    async def test_update_konflux_db_skips_rpm_extraction_when_no_shell(self):
        """RPM extraction is skipped for no_shell images (e.g. FROM scratch ISO builds)."""
        metadata = self._metadata()
        metadata.config.konflux.get.return_value = True  # no_shell=True
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

        mock_get_installed_packages.assert_not_awaited()
        # DB record should have empty package lists
        add_build_call = metadata.runtime.konflux_db.add_build
        add_build_call.assert_called_once()
        build_record = add_build_call.call_args[0][0]
        self.assertEqual(build_record.installed_packages, [])
        self.assertEqual(build_record.installed_rpms, [])
        self.assertEqual(build_record.image_pullspec, "quay.io/test/image@sha256:testdigest")

    async def test_update_konflux_db_skips_rpm_extraction_for_scratch_image(self):
        """RPM extraction is auto-skipped when from.stream is 'scratch', even without explicit no_shell."""
        metadata = self._metadata()
        metadata.config.konflux.get.return_value = False  # no_shell not explicitly set
        metadata.config.get.return_value = {'stream': 'scratch'}  # from.stream == 'scratch'
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

        mock_get_installed_packages.assert_not_awaited()
        add_build_call = metadata.runtime.konflux_db.add_build
        add_build_call.assert_called_once()
        build_record = add_build_call.call_args[0][0]
        self.assertEqual(build_record.installed_packages, [])
        self.assertEqual(build_record.installed_rpms, [])

    async def test_update_konflux_db_strips_rhel_minor_version_from_el_target(self):
        """el_target should be 'el9' even when the release contains a minor version suffix like 'el9_6'."""
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
            ),
            patch("doozerlib.backend.konflux_image_builder.bigquery.BigQueryClient") as mock_bigquery_client,
        ):
            mock_dockerfile = MagicMock()
            mock_dockerfile.labels = {
                "io.openshift.build.source-location": "https://example.com/source-repo.git",
                "io.openshift.build.commit.id": "source-commit-id",
                "com.redhat.component": "test-component",
                "version": "1.0",
                "release": "1.el9_6",
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

        add_build_call = metadata.runtime.konflux_db.add_build
        add_build_call.assert_called_once()
        build_record = add_build_call.call_args[0][0]
        self.assertEqual(build_record.el_target, "el9")

    async def test_update_konflux_db_preserves_scos_prefix_in_el_target(self):
        """el_target should be 'scos9' for OKD builds, not 'el9'."""
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
            ),
            patch("doozerlib.backend.konflux_image_builder.bigquery.BigQueryClient") as mock_bigquery_client,
        ):
            mock_dockerfile = MagicMock()
            mock_dockerfile.labels = {
                "io.openshift.build.source-location": "https://example.com/source-repo.git",
                "io.openshift.build.commit.id": "source-commit-id",
                "com.redhat.component": "test-component",
                "version": "1.0",
                "release": "1.scos9",
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

        add_build_call = metadata.runtime.konflux_db.add_build
        add_build_call.assert_called_once()
        build_record = add_build_call.call_args[0][0]
        self.assertEqual(build_record.el_target, "scos9")

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

    async def test_update_konflux_db_success_keeps_konflux_digest_after_base_release_completion(self):
        """Final SUCCESS after base-image release keeps Konflux digest in DB for SBOM (not RH tag)."""
        metadata = self._metadata()
        metadata.should_trigger_base_image_release.return_value = True
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

        expected_pullspec = "quay.io/test/image@sha256:testdigest"
        mock_get_installed_packages.assert_awaited_once_with(expected_pullspec, ["x86_64"], None)
        mock_add_build = metadata.runtime.konflux_db.add_build
        mock_add_build.assert_called_once()
        persisted = mock_add_build.call_args[0][0]
        self.assertEqual(persisted.image_pullspec, expected_pullspec)

    async def test_update_konflux_db_persists_released_fields_when_passed(self):
        """release_pipeline and released_pullspec are forwarded to KonfluxBuildRecord."""
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
            ),
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
                release_pipeline="https://release.example/pipeline",
                released_pullspec="registry.redhat.io/foo:bar",
            )

        mock_add_build = metadata.runtime.konflux_db.add_build
        mock_add_build.assert_called_once()
        persisted = mock_add_build.call_args[0][0]
        self.assertEqual(persisted.release_pipeline, "https://release.example/pipeline")
        self.assertEqual(persisted.released_pullspec, "registry.redhat.io/foo:bar")

    async def test_trigger_base_image_release_success_flow(self):
        """SUCCESS after base snapshot release refreshes Konflux DB without swapping image_pullspec to RH."""
        metadata = self._metadata()
        metadata.should_trigger_base_image_release.return_value = True
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

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(self.builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(self.builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(
                self.builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))
            ) as mock_update_db,
            patch.object(self.builder, "_trigger_base_image_release", new_callable=AsyncMock) as mock_trigger_release,
            patch.object(self.builder, "_validate_build_attestation_and_signature", new=AsyncMock()),
            patch.object(
                self.builder._konflux_client,
                "verify_enterprise_contract",
                new=AsyncMock(return_value=MagicMock(ec_pipeline_url="", ec_failed=False)),
            ),
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            from doozerlib.backend import base_image_handler

            mock_trigger_release.return_value = base_image_handler.BaseImageReleaseResult(
                release_name="r",
                snapshot_name="s",
                nvr="test-nvr",
                release_pipeline="https://example.com/rel",
                released_pullspec="registry.example/pull:test",
            )
            await self.builder.build(metadata)

        mock_trigger_release.assert_awaited_once()

        # PENDING write + single completion write (SUCCESS after base image snapshot-release)
        self.assertEqual(mock_update_db.await_count, 2)
        success_call_args = mock_update_db.await_args_list[1][0]
        self.assertEqual(success_call_args[3], KonfluxBuildOutcome.SUCCESS)
        success_call_kwargs = mock_update_db.await_args_list[1].kwargs
        self.assertEqual(success_call_kwargs.get("release_pipeline"), "https://example.com/rel")
        self.assertEqual(success_call_kwargs.get("released_pullspec"), "registry.example/pull:test")

    async def test_trigger_base_image_release_failure_flow(self):
        """Test base image release failure handling when _trigger_base_image_release returns None."""
        metadata = self._metadata()
        metadata.should_trigger_base_image_release.return_value = True
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

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(self.builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(self.builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(
                self.builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))
            ) as mock_update_db,
            patch.object(
                self.builder, "_trigger_base_image_release", new=AsyncMock(return_value=None)
            ) as mock_trigger_release,
            patch.object(self.builder, "_validate_build_attestation_and_signature", new=AsyncMock()),
            patch.object(
                self.builder._konflux_client,
                "verify_enterprise_contract",
                new=AsyncMock(return_value=MagicMock(ec_pipeline_url="", ec_failed=False)),
            ),
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            with self.assertRaises(KonfluxImageBuildError):
                await self.builder.build(metadata)

        mock_trigger_release.assert_awaited_once()
        self.assertEqual(mock_update_db.await_count, 2)

        # Final update records RELEASE_ERROR outcome; released_* omitted when base release failed
        failure_call_args = mock_update_db.await_args_list[1][0]
        self.assertEqual(failure_call_args[3], KonfluxBuildOutcome.RELEASE_ERROR)
        failure_kw = mock_update_db.await_args_list[1].kwargs
        self.assertEqual(failure_kw.get("release_pipeline", ""), "")
        self.assertEqual(failure_kw.get("released_pullspec", ""), "")

    async def test_trigger_base_image_release_failure_preserves_pipeline_url(self):
        """Test base image release failure preserves release_pipeline URL when available."""
        metadata = self._metadata()
        metadata.should_trigger_base_image_release.return_value = True
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

        # Create a failed release result with release_pipeline but no released_pullspec
        from doozerlib.backend.base_image_handler import BaseImageReleaseResult

        failed_result = BaseImageReleaseResult(
            release_name="failed-release",
            snapshot_name="test-snapshot",
            nvr="test-nvr",
            release_pipeline="https://example.com/failed-release",
            released_pullspec="",  # Empty indicates failure
        )

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(self.builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(self.builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(
                self.builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))
            ) as mock_update_db,
            patch.object(
                self.builder, "_trigger_base_image_release", new=AsyncMock(return_value=failed_result)
            ) as mock_trigger_release,
            patch.object(self.builder, "_validate_build_attestation_and_signature", new=AsyncMock()),
            patch.object(
                self.builder._konflux_client,
                "verify_enterprise_contract",
                new=AsyncMock(return_value=MagicMock(ec_pipeline_url="", ec_failed=False)),
            ),
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            with self.assertRaises(KonfluxImageBuildError):
                await self.builder.build(metadata)

        mock_trigger_release.assert_awaited_once()
        self.assertEqual(mock_update_db.await_count, 2)

        # Final update records RELEASE_ERROR outcome with release_pipeline preserved
        failure_call_args = mock_update_db.await_args_list[1][0]
        self.assertEqual(failure_call_args[3], KonfluxBuildOutcome.RELEASE_ERROR)
        failure_kw = mock_update_db.await_args_list[1].kwargs
        self.assertEqual(failure_kw.get("release_pipeline"), "https://example.com/failed-release")
        self.assertEqual(failure_kw.get("released_pullspec", ""), "")

    async def test_non_base_image_skips_release_trigger(self):
        """Test that non-base images skip the release trigger logic entirely."""
        metadata = self._metadata()
        metadata.should_trigger_base_image_release.return_value = False
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

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(self.builder, "_wait_for_parent_members", new=AsyncMock(return_value=[])),
            patch.object(self.builder, "_start_build", new=AsyncMock(return_value=initial_pipelinerun)),
            patch.object(
                self.builder, "update_konflux_db", new=AsyncMock(return_value=MagicMock(record_id="1"))
            ) as mock_update_db,
            patch.object(self.builder, "_trigger_base_image_release", new=AsyncMock()) as mock_trigger_release,
            patch.object(self.builder, "_validate_build_attestation_and_signature", new=AsyncMock()),
            patch.object(
                self.builder._konflux_client,
                "verify_enterprise_contract",
                new=AsyncMock(return_value=MagicMock(ec_pipeline_url="", ec_failed=False)),
            ),
            patch(
                "doozerlib.backend.konflux_image_builder.KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition",
                return_value=KonfluxBuildOutcome.SUCCESS,
            ),
        ):
            await self.builder.build(metadata)

        # Base image release should not be triggered for non-base images
        mock_trigger_release.assert_not_awaited()

        # Only 2 database updates: PENDING + final SUCCESS (no registry update)
        self.assertEqual(mock_update_db.await_count, 2)

    async def test_trigger_base_image_release_uses_base_image_handler(self):
        """Per-build release uses BaseImageHandler with snapshot_input; no Jenkins."""
        from doozerlib.backend import base_image_handler

        metadata = self._metadata()
        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "abc123"
        pullspec = "quay.io/test/img@sha256:deadbeef"

        release_result = base_image_handler.BaseImageReleaseResult(
            release_name="test-release",
            snapshot_name="test-snapshot",
            nvr="test-nvr",
            release_pipeline="https://example.com/pipeline",
            released_pullspec="example.com/openshift/foo:test-nvr",
        )

        with patch.object(
            base_image_handler.BaseImageHandler,
            "snapshot_release",
            new=AsyncMock(return_value=release_result),
        ) as mock_snap:
            result = await self.builder._trigger_base_image_release(metadata, "test-nvr", pullspec, build_repo)

        self.assertIs(result, release_result)
        mock_snap.assert_awaited_once()
        inp = mock_snap.await_args[0][0]
        self.assertEqual(inp.nvr, "test-nvr")
        self.assertEqual(inp.distgit_key, metadata.distgit_key)
        self.assertEqual(inp.container_image, pullspec)
        self.assertFalse(inp.is_golang_builder)

    async def test_trigger_base_image_release_golang_calls_is_golang_builder(self):
        """Inline release passes through metadata.is_golang_builder() (openshift/golang-builder vs hyphen)."""
        from doozerlib.backend import base_image_handler

        metadata = self._metadata()
        metadata.is_golang_builder = MagicMock(return_value=True)
        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "abc123"
        pullspec = "quay.io/test/img@sha256:deadbeef"

        release_result = base_image_handler.BaseImageReleaseResult(
            release_name="test-release",
            snapshot_name="test-snapshot",
            nvr="test-nvr",
            release_pipeline="https://example.com/pipeline",
            released_pullspec="example.com/openshift/foo:test-nvr",
        )

        with patch.object(
            base_image_handler.BaseImageHandler,
            "snapshot_release",
            new=AsyncMock(return_value=release_result),
        ) as mock_snap:
            result = await self.builder._trigger_base_image_release(metadata, "test-nvr", pullspec, build_repo)

        self.assertIs(result, release_result)
        metadata.is_golang_builder.assert_called_once()
        inp = mock_snap.await_args[0][0]
        self.assertTrue(inp.is_golang_builder)

    async def test_trigger_base_image_release_returns_none_when_handler_returns_none(self):
        """When handler returns None, _trigger_base_image_release returns None."""
        from doozerlib.backend import base_image_handler

        metadata = self._metadata()
        build_repo = MagicMock()
        build_repo.https_url = "https://example.com/repo.git"
        build_repo.commit_hash = "abc123"

        with patch.object(
            base_image_handler.BaseImageHandler,
            "snapshot_release",
            new=AsyncMock(return_value=None),
        ) as mock_snap:
            result = await self.builder._trigger_base_image_release(
                metadata, "test-nvr", "quay.io/x@sha256:y", build_repo
            )

        self.assertIsNone(result)
        mock_snap.assert_awaited_once()

    async def test_build_parent_failure_captures_message_in_record(self):
        """When parent images fail, the record message should capture the actual error, not 'Unknown failure'."""
        metadata = self._metadata()
        dest_dir = self.builder._config.base_dir.joinpath(metadata.qualified_key)
        dest_dir.mkdir(parents=True)

        build_repo = MagicMock()
        build_repo.local_dir = dest_dir
        build_repo.url = "https://github.com/test/repo.git"
        build_repo.commit_hash = "test-commit"

        # Simulate a parent member that failed to build
        failed_parent = MagicMock()
        failed_parent.distgit_key = "openshift-enterprise-base-rhel9"
        failed_parent.build_status = False

        record_logger = MagicMock()
        self.builder._record_logger = record_logger

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(self.builder, "_parse_dockerfile", return_value=("test-uuid", "test-component", "1.0", "1")),
            patch.object(
                self.builder,
                "_wait_for_parent_members",
                new=AsyncMock(return_value=[failed_parent]),
            ),
        ):
            with self.assertRaises(IOError) as ctx:
                await self.builder.build(metadata)

        # Verify the error message mentions the parent failure
        self.assertIn("parent images failed to build", str(ctx.exception))
        self.assertIn("openshift-enterprise-base-rhel9", str(ctx.exception))

        # Verify the record message was captured (not left as "Unknown failure")
        add_record_call = record_logger.add_record
        add_record_call.assert_called_once()
        _, kwargs = add_record_call.call_args
        self.assertIn("parent images failed to build", kwargs["message"])
        self.assertNotEqual(kwargs["message"], "Unknown failure")

    async def test_build_unknown_exception_captures_message_in_record(self):
        """Any exception before the build loop should capture its message in the record."""
        metadata = self._metadata()
        dest_dir = self.builder._config.base_dir.joinpath(metadata.qualified_key)
        dest_dir.mkdir(parents=True)

        build_repo = MagicMock()
        build_repo.local_dir = dest_dir

        record_logger = MagicMock()
        self.builder._record_logger = record_logger

        with (
            patch(
                "doozerlib.backend.konflux_image_builder.BuildRepo.from_local_dir",
                new=AsyncMock(return_value=build_repo),
            ),
            patch.object(
                self.builder,
                "_parse_dockerfile",
                side_effect=ValueError("Target NVR 1.0-1 is not greater than the latest"),
            ),
        ):
            with self.assertRaises(ValueError):
                await self.builder.build(metadata)

        add_record_call = record_logger.add_record
        add_record_call.assert_called_once()
        _, kwargs = add_record_call.call_args
        self.assertIn("Target NVR", kwargs["message"])
        self.assertNotEqual(kwargs["message"], "Unknown failure")


class TestNormalizeVersion(unittest.TestCase):
    """
    Tests for _normalize_version.
    """

    def test_two_segments_padded(self):
        """
        v4.20 -> v4.20.0
        """
        self.assertEqual(_normalize_version("v4.20"), "v4.20.0")

    def test_three_segments_unchanged(self):
        """
        v4.20.0 stays v4.20.0.
        """
        self.assertEqual(_normalize_version("v4.20.0"), "v4.20.0")

    def test_three_segments_nonzero_unchanged(self):
        """
        v4.20.1 stays v4.20.1.
        """
        self.assertEqual(_normalize_version("v4.20.1"), "v4.20.1")

    def test_no_prefix(self):
        """
        Works without the 'v' prefix too.
        """
        self.assertEqual(_normalize_version("4.20"), "4.20.0")

    def test_single_segment_unchanged(self):
        """
        A single segment is unchanged (only 2-segment gets padded).
        """
        self.assertEqual(_normalize_version("4"), "4")

    def test_four_segments_unchanged(self):
        """
        Four segments are left as-is.
        """
        self.assertEqual(_normalize_version("v4.20.0.1"), "v4.20.0.1")


class TestKonfluxImageBuilderGolangComponent(unittest.TestCase):
    """Test golang builder component name generation."""

    def test_get_golang_builder_component_name_basic(self):
        """Test basic golang builder component name generation."""
        nvr = "openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el8"
        expected = "golang-builder-v1.25-rhel8"
        result = konflux_golang_builder_component_name(nvr)
        self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_el9(self):
        """Test golang builder component name with el9."""
        nvr = "openshift-golang-builder-container-v1.24.13-202603271102.p2.ge8e5642.el9"
        expected = "golang-builder-v1.24-rhel9"
        result = konflux_golang_builder_component_name(nvr)
        self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_various_versions(self):
        """Test various golang versions extract major.minor correctly."""
        test_cases = [
            ("openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el8", "golang-builder-v1.25-rhel8"),
            ("openshift-golang-builder-container-v1.24.13-202603271102.p2.ge8e5642.el9", "golang-builder-v1.24-rhel9"),
            ("openshift-golang-builder-container-v1.19.13-202604151155.p2.g47c3be5.el9", "golang-builder-v1.19-rhel9"),
            ("openshift-golang-builder-container-v1.23.10-202604151125.p2.gd0321dd.el9", "golang-builder-v1.23-rhel9"),
            ("openshift-golang-builder-container-v1.21.13-202603251649.p0.g670cbfa.el9", "golang-builder-v1.21-rhel9"),
            ("openshift-golang-builder-container-v1.22.12-202603171846.p2.g3a22db8.el8", "golang-builder-v1.22-rhel8"),
            ("openshift-golang-builder-container-v1.20.12-202604101100.p0.g6e050e4.el8", "golang-builder-v1.20-rhel8"),
        ]

        for nvr, expected in test_cases:
            with self.subTest(nvr=nvr):
                result = konflux_golang_builder_component_name(nvr)
                self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_comprehensive_dataset(self):
        """Test with comprehensive dataset of real NVRs."""
        test_nvrs = [
            ("openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el8", "golang-builder-v1.25-rhel8"),
            ("openshift-golang-builder-container-v1.25.8-202604081723.p0.gf28329a.el9", "golang-builder-v1.25-rhel9"),
            ("openshift-golang-builder-container-v1.25.8-202604081550.p2.gf28329a.el9", "golang-builder-v1.25-rhel9"),
            ("openshift-golang-builder-container-v1.25.8-202604150842.p2.g2aa6a05.el8", "golang-builder-v1.25-rhel8"),
            ("openshift-golang-builder-container-v1.19.13-202604151155.p2.g47c3be5.el9", "golang-builder-v1.19-rhel9"),
            ("openshift-golang-builder-container-v1.23.10-202604151125.p2.gd0321dd.el9", "golang-builder-v1.23-rhel9"),
            ("openshift-golang-builder-container-v1.24.13-202604151125.p2.g04d2cd5.el9", "golang-builder-v1.24-rhel9"),
            ("openshift-golang-builder-container-v1.21.13-202603251649.p0.g670cbfa.el9", "golang-builder-v1.21-rhel9"),
            ("openshift-golang-builder-container-v1.25.7-202604020943.p2.g5015a16.el9", "golang-builder-v1.25-rhel9"),
            ("openshift-golang-builder-container-v1.25.7-202604021351.p2.g5015a16.el9", "golang-builder-v1.25-rhel9"),
            ("openshift-golang-builder-container-v1.22.12-202603171846.p2.g3a22db8.el8", "golang-builder-v1.22-rhel8"),
            ("openshift-golang-builder-container-v1.24.13-202603270926.p2.g1f0d617.el8", "golang-builder-v1.24-rhel8"),
            ("openshift-golang-builder-container-v1.24.13-202603271102.p2.ge8e5642.el9", "golang-builder-v1.24-rhel9"),
            ("openshift-golang-builder-container-v1.20.12-202604101100.p0.g6e050e4.el8", "golang-builder-v1.20-rhel8"),
        ]

        for nvr, expected in test_nvrs:
            with self.subTest(nvr=nvr):
                result = konflux_golang_builder_component_name(nvr)
                self.assertEqual(result, expected, f"Failed for NVR: {nvr}")

    def test_get_golang_builder_component_name_no_el_version_defaults_to_rhel9(self):
        """Test that missing RHEL version defaults to rhel9."""
        nvr = "openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.unknown"
        expected = "golang-builder-v1.25-rhel9"
        result = konflux_golang_builder_component_name(nvr)
        self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_version_without_v_prefix(self):
        """Test version without 'v' prefix."""
        nvr = "openshift-golang-builder-container-1.25.8-202604081607.p0.g2aa6a05.el8"
        expected = "golang-builder-v1.25-rhel8"
        result = konflux_golang_builder_component_name(nvr)
        self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_single_version_part(self):
        """Test with single version part (fallback behavior)."""
        nvr = "openshift-golang-builder-container-v1-202604081607.p0.g2aa6a05.el9"
        expected = "golang-builder-v1-rhel9"  # Should fallback to original version
        result = konflux_golang_builder_component_name(nvr)
        self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_edge_cases(self):
        """Test edge cases and boundary conditions."""
        test_cases = [
            # Two-part version (standard case)
            ("openshift-golang-builder-container-v1.24-202604081607.p0.g2aa6a05.el9", "golang-builder-v1.24-rhel9"),
            # Three-part version
            ("openshift-golang-builder-container-v1.24.0-202604081607.p0.g2aa6a05.el8", "golang-builder-v1.24-rhel8"),
            # Four-part version
            ("openshift-golang-builder-container-v1.24.0.1-202604081607.p0.g2aa6a05.el9", "golang-builder-v1.24-rhel9"),
        ]

        for nvr, expected in test_cases:
            with self.subTest(nvr=nvr):
                result = konflux_golang_builder_component_name(nvr)
                self.assertEqual(result, expected)

    def test_get_golang_builder_component_name_multiple_el_patterns(self):
        """Test theoretical edge case where both .el8 and .el9 appear in release."""
        # This documents the expected behavior: .el8 override wins due to simplified logic
        nvr = "openshift-golang-builder-container-v1.25.8-202604081607.p0.g2aa6a05.el8.dependency.el9"
        expected = "golang-builder-v1.25-rhel8"  # .el8 override takes precedence
        result = konflux_golang_builder_component_name(nvr)
        self.assertEqual(result, expected)


class TestGetInstalledPackages(unittest.IsolatedAsyncioTestCase):
    """Tests for KonfluxImageBuilder.get_installed_packages() SBOM parsing."""

    def _make_spdx_sbom(self, packages):
        """Build a minimal SPDX-2.3 SBOM with the given package dicts."""
        return {
            "spdxVersion": "SPDX-2.3",
            "packages": packages,
        }

    def _make_rpm_package(self, purl_string, name="pkg"):
        """Build a minimal SPDX package entry with a PURL external ref."""
        return {
            "SPDXID": f"SPDXRef-{name}",
            "name": name,
            "externalRefs": [
                {
                    "referenceCategory": "PACKAGE_MANAGER",
                    "referenceLocator": purl_string,
                    "referenceType": "purl",
                }
            ],
        }

    async def test_old_format_with_upstream(self):
        """Mobster <1.2.1: PURLs include upstream qualifier -> source_rpms derived from upstream."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=x86_64&upstream=acl-2.3.1-4.el9.src.rpm&distro=rhel-9.8",
                    name="acl",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/libacl@2.3.1-4.el9?arch=x86_64&upstream=acl-2.3.1-4.el9.src.rpm&distro=rhel-9.8",
                    name="libacl",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64"]
            )

        self.assertEqual(package_nvrs, {"acl-2.3.1-4.el9", "libacl-2.3.1-4.el9"})
        self.assertEqual(source_rpms, {"acl-2.3.1-4.el9"})

    async def test_new_format_without_upstream(self):
        """Mobster >=1.2.1: PURLs lack upstream -> binary NVR used as fallback for source_rpms."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=x86_64&checksum=sha256:150d7232&repository_id=rhel-9-baseos",
                    name="acl",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=x86_64&checksum=sha256:d3adf8b0&repository_id=rhel-9-baseos",
                    name="bash",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64"]
            )

        self.assertEqual(package_nvrs, {"acl-2.3.1-4.el9", "bash-5.1.8-9.el9"})
        self.assertEqual(source_rpms, {"acl-2.3.1-4.el9", "bash-5.1.8-9.el9"})

    async def test_mixed_format(self):
        """Some PURLs have upstream (old), some don't (new) -> both paths work."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=x86_64&upstream=acl-2.3.1-4.el9.src.rpm&distro=rhel-9.8",
                    name="acl",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=x86_64&checksum=sha256:d3adf8b0&repository_id=rhel-9-baseos",
                    name="bash",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64"]
            )

        self.assertEqual(package_nvrs, {"acl-2.3.1-4.el9", "bash-5.1.8-9.el9"})
        self.assertEqual(source_rpms, {"acl-2.3.1-4.el9", "bash-5.1.8-9.el9"})

    async def test_gpg_pubkey_filtered_out(self):
        """gpg-pubkey pseudo-packages should be skipped."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/gpg-pubkey@5a6340b3-6229229e?distro=rhel-9.8",
                    name="gpg-pubkey",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=x86_64&checksum=sha256:150d7232&repository_id=rhel-9-baseos",
                    name="acl",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64"]
            )

        self.assertEqual(package_nvrs, {"acl-2.3.1-4.el9"})
        self.assertNotIn("gpg-pubkey-5a6340b3-6229229e", package_nvrs)

    async def test_src_arch_filtered_out(self):
        """Source RPM entries (arch=src) should be skipped."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=src",
                    name="acl",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=x86_64&checksum=sha256:abc&repository_id=rhel-9-baseos",
                    name="bash",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64"]
            )

        self.assertEqual(package_nvrs, {"bash-5.1.8-9.el9"})
        self.assertNotIn("acl-2.3.1-4.el9", package_nvrs)

    async def test_empty_sbom_raises_error(self):
        """An SBOM with no RPMs should raise ChildProcessError."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:npm/%40npmcli/agent@2.2.2",
                    name="@npmcli/agent",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            with self.assertRaises(ChildProcessError):
                await KonfluxImageBuilder.get_installed_packages("quay.io/test/image@sha256:abc", ["x86_64"])

    async def test_non_rpm_packages_ignored(self):
        """Non-RPM packages (npm, oci) are ignored."""
        sbom = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:npm/%40npmcli/agent@2.2.2",
                    name="@npmcli/agent",
                ),
                self._make_rpm_package(
                    "pkg:oci/art-images@sha256:abc?repository_url=quay.io/test",
                    name="art-images",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=x86_64&checksum=sha256:abc&repository_id=baseos",
                    name="bash",
                ),
            ]
        )

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async") as mock_cmd:
            import json

            mock_cmd.return_value = (0, json.dumps(sbom), "")
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64"]
            )

        self.assertEqual(package_nvrs, {"bash-5.1.8-9.el9"})

    async def test_multiple_arches(self):
        """Results are merged across arches."""
        sbom_amd64 = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=x86_64&checksum=sha256:aaa&repository_id=baseos",
                    name="acl",
                ),
            ]
        )
        sbom_arm64 = self._make_spdx_sbom(
            [
                self._make_rpm_package(
                    "pkg:rpm/redhat/acl@2.3.1-4.el9?arch=aarch64&checksum=sha256:bbb&repository_id=baseos",
                    name="acl",
                ),
                self._make_rpm_package(
                    "pkg:rpm/redhat/bash@5.1.8-9.el9?arch=aarch64&checksum=sha256:ccc&repository_id=baseos",
                    name="bash",
                ),
            ]
        )

        import json

        async def mock_cmd(cmd, env=None):
            if "amd64" in " ".join(cmd):
                return (0, json.dumps(sbom_amd64), "")
            return (0, json.dumps(sbom_arm64), "")

        with patch("doozerlib.backend.konflux_image_builder.exectools.cmd_gather_async", side_effect=mock_cmd):
            package_nvrs, source_rpms = await KonfluxImageBuilder.get_installed_packages(
                "quay.io/test/image@sha256:abc", ["x86_64", "aarch64"]
            )

        self.assertEqual(package_nvrs, {"acl-2.3.1-4.el9", "bash-5.1.8-9.el9"})
