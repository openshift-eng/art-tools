import asyncio
import json
import unittest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from pyartcd.pipelines.prepare_release_lp import PrepareReleaseLPPipeline


class TestPrepareReleaseLPPipeline(unittest.TestCase):
    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = overrides.pop('dry_run', True)
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
            "gitlab_url": "https://gitlab.example.com",
        }

        kwargs = dict(
            runtime=runtime,
            group="acm-2.17",
            assembly="2.17.3",
        )
        kwargs.update(overrides)
        return PrepareReleaseLPPipeline(**kwargs)

    def test_extract_operand_nvrs_from_assembly(self):
        """Should extract NVRs from assembly members.images."""
        pipeline = self._make_pipeline()
        assembly_config = {
            'assembly': {
                'type': 'standard',
                'basis': {'fbc_pullspecs': ['quay.io/test/fbc@sha256:abc']},
                'members': {
                    'images': [
                        {
                            'distgit_key': 'search-v2-api-container',
                            'metadata': {'is': {'nvr': 'search-v2-api-container-2.17.3-1'}},
                        },
                        {
                            'distgit_key': 'console-container',
                            'metadata': {'is': {'nvr': 'console-container-2.17.3-1'}},
                        },
                    ],
                },
            },
        }

        nvrs = pipeline._extract_operand_nvrs(assembly_config)
        self.assertEqual(len(nvrs), 2)
        self.assertIn('search-v2-api-container-2.17.3-1', nvrs)
        self.assertIn('console-container-2.17.3-1', nvrs)

    def test_extract_operand_nvrs_with_override_operator(self):
        """Should handle nvr! (override) entries from inherited assemblies."""
        pipeline = self._make_pipeline()
        assembly_config = {
            'assembly': {
                'type': 'standard',
                'basis': {'assembly': '2.17.2'},
                'members': {
                    'images': [
                        {
                            'distgit_key': 'search-v2-api-container',
                            'metadata': {'is': {'nvr!': 'search-v2-api-container-2.17.3-2'}},
                        },
                    ],
                },
            },
        }

        nvrs = pipeline._extract_operand_nvrs(assembly_config)
        self.assertEqual(len(nvrs), 1)
        self.assertIn('search-v2-api-container-2.17.3-2', nvrs)

    def test_extract_operand_nvrs_empty_assembly(self):
        """Should return empty list for assembly with no members."""
        pipeline = self._make_pipeline()
        assembly_config = {
            'assembly': {
                'type': 'standard',
                'basis': {},
                'members': {'images': []},
            },
        }

        nvrs = pipeline._extract_operand_nvrs(assembly_config)
        self.assertEqual(nvrs, [])

    def test_doozer_command_uses_named_assembly(self):
        """Verify that doozer base command uses the named assembly, not 'stream'."""
        pipeline = self._make_pipeline()
        self.assertIn('--assembly=2.17.3', pipeline._doozer_base_command)
        self.assertNotIn('--assembly=stream', pipeline._doozer_base_command)
        self.assertIn('--group=acm-2.17', pipeline._doozer_base_command)

    def test_elliott_command_uses_named_assembly(self):
        """Verify that elliott base command uses the named assembly, not 'stream'."""
        pipeline = self._make_pipeline()
        self.assertIn('--assembly=2.17.3', pipeline._elliott_base_command)
        self.assertNotIn('--assembly=stream', pipeline._elliott_base_command)

    def test_check_env_vars_requires_gitlab_token(self):
        """Should raise when GITLAB_TOKEN is missing and create_mr is True."""
        pipeline = self._make_pipeline(create_mr=True)
        with patch.dict('os.environ', {}, clear=True):
            with self.assertRaises(ValueError):
                pipeline._check_env_vars()


class TestPrepareReleaseLPMultiFBC(unittest.TestCase):
    """Tests for multi-FBC (multi-OCP-version) FBC builds in prepare-release-lp."""

    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = overrides.pop('dry_run', True)
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
        }

        kwargs = dict(
            runtime=runtime,
            group="logging-6.4",
            assembly="6.4.1",
        )
        kwargs.update(overrides)
        return PrepareReleaseLPPipeline(**kwargs)

    @patch.object(PrepareReleaseLPPipeline, '_load_ocp_target_versions', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async', new_callable=AsyncMock)
    def test_trigger_fbc_build_loops_over_ocp_versions(self, mock_cmd, mock_target_versions):
        """_trigger_fbc_build should call doozer once per OCP target version with --major-minor."""
        mock_target_versions.return_value = ["4.18", "4.19", "4.20"]
        mock_cmd.side_effect = [
            (
                0,
                json.dumps(
                    {
                        "nvrs": ["fbc-6.4.1-1.ocp4.18"],
                        "pullspecs": ["quay.io/test/fbc@sha256:a18"],
                        "errors": [],
                        "failed_count": 0,
                        "success_count": 1,
                    }
                ),
                "",
            ),
            (
                0,
                json.dumps(
                    {
                        "nvrs": ["fbc-6.4.1-1.ocp4.19"],
                        "pullspecs": ["quay.io/test/fbc@sha256:a19"],
                        "errors": [],
                        "failed_count": 0,
                        "success_count": 1,
                    }
                ),
                "",
            ),
            (
                0,
                json.dumps(
                    {
                        "nvrs": ["fbc-6.4.1-1.ocp4.20"],
                        "pullspecs": ["quay.io/test/fbc@sha256:a20"],
                        "errors": [],
                        "failed_count": 0,
                        "success_count": 1,
                    }
                ),
                "",
            ),
        ]

        pipeline = self._make_pipeline(dry_run=False)
        operator_nvrs = ["loki-rhel9-operator-container-6.4.1-1"]
        nvrs, pullspecs = asyncio.run(pipeline._trigger_fbc_build(operator_nvrs))

        self.assertEqual(len(nvrs), 3)
        self.assertEqual(len(pullspecs), 3)
        self.assertEqual(mock_cmd.call_count, 3)
        for call_args in mock_cmd.call_args_list:
            cmd = call_args[0][0]
            self.assertTrue(any('--major-minor=' in arg for arg in cmd))
            self.assertIn('--output=json', cmd)
            self.assertTrue(any('--version=' in arg for arg in cmd))
            self.assertTrue(any('--release=' in arg for arg in cmd))
            self.assertTrue(any('--message=' in arg for arg in cmd))
            self.assertIn('--', cmd)
            self.assertIn('loki-rhel9-operator-container-6.4.1-1', cmd)

    @patch.object(PrepareReleaseLPPipeline, '_load_ocp_target_versions', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async', new_callable=AsyncMock)
    def test_trigger_fbc_build_fallback_no_target_versions(self, mock_cmd, mock_target_versions):
        """Without OCP_TARGET_VERSIONS, should fall back to single FBC build."""
        mock_target_versions.return_value = []
        mock_cmd.return_value = (
            0,
            json.dumps(
                {
                    "nvrs": ["fbc-6.4.1-1"],
                    "pullspecs": ["quay.io/test/fbc@sha256:single"],
                    "errors": [],
                    "failed_count": 0,
                    "success_count": 1,
                }
            ),
            "",
        )

        pipeline = self._make_pipeline(dry_run=False)
        operator_nvrs = ["loki-rhel9-operator-container-6.4.1-1"]
        nvrs, pullspecs = asyncio.run(pipeline._trigger_fbc_build(operator_nvrs))

        self.assertEqual(len(nvrs), 1)
        self.assertEqual(len(pullspecs), 1)
        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        self.assertFalse(any('--major-minor=' in arg for arg in cmd))
        self.assertIn('--output=json', cmd)
        self.assertTrue(any('--version=6.4.1' in arg for arg in cmd))
        self.assertTrue(any('--release=' in arg for arg in cmd))
        self.assertIn('--', cmd)
        self.assertIn('loki-rhel9-operator-container-6.4.1-1', cmd)

    @patch.object(PrepareReleaseLPPipeline, '_load_ocp_target_versions', new_callable=AsyncMock)
    def test_trigger_fbc_build_dry_run_with_target_versions(self, mock_target_versions):
        """Dry run with target versions should not call doozer."""
        mock_target_versions.return_value = ["4.18", "4.19"]

        pipeline = self._make_pipeline(dry_run=True)
        result = asyncio.run(pipeline._trigger_fbc_build())

        self.assertEqual(result, ([], []))


class TestPrepareReleaseLPBundleScoping(unittest.TestCase):
    """Tests that bundle builds are scoped to assembly-pinned operators only."""

    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = overrides.pop('dry_run', False)
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
        }

        kwargs = dict(
            runtime=runtime,
            group="logging-6.0",
            assembly="6.0.15",
        )
        kwargs.update(overrides)
        return PrepareReleaseLPPipeline(**kwargs)

    @patch.object(PrepareReleaseLPPipeline, '_find_existing_bundles', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async', new_callable=AsyncMock)
    def test_filters_operators_not_in_assembly(self, mock_cmd, mock_find):
        """Operators not pinned in the assembly should be skipped, not built."""
        mock_find.return_value = {
            "metadata": [],
            "olm_builds_not_found": [
                "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
                "ose-cluster-logging-operator-container-6.0.15-1.assembly.stream.el9",
            ],
            "olm_operator_nvrs": [
                "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
                "ose-cluster-logging-operator-container-6.0.15-1.assembly.stream.el9",
            ],
        }
        mock_cmd.return_value = (
            0,
            json.dumps({"nvrs": ["loki-rhel9-operator-metadata-container-6.0.15-1"], "errors": []}),
            "",
        )

        pipeline = self._make_pipeline()
        operand_nvrs = ["loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9"]
        bundle_nvrs, operator_nvrs = asyncio.run(pipeline._trigger_bundle_build(operand_nvrs))

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9", cmd)
        self.assertNotIn("ose-cluster-logging-operator-container-6.0.15-1.assembly.stream.el9", cmd)
        self.assertEqual(operator_nvrs, ["loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9"])

    @patch.object(PrepareReleaseLPPipeline, '_find_existing_bundles', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async', new_callable=AsyncMock)
    def test_matches_operators_by_name_not_full_nvr(self, mock_cmd, mock_find):
        """Scoping should match by NVR name component, not full NVR string.

        When a new build happens after assembly creation, the release timestamp
        in the latest NVR differs from the assembly-pinned NVR. Name-based
        matching ensures the operator is still recognised.
        """
        mock_find.return_value = {
            "metadata": [],
            "olm_builds_not_found": [
                "loki-rhel9-operator-container-6.0.15-202606051158.p2.g3d09d2c.assembly.stream.el9",
                "ose-cluster-logging-operator-container-6.0.15-202606051158.p2.assembly.stream.el9",
            ],
            "olm_operator_nvrs": [
                "loki-rhel9-operator-container-6.0.15-202606051158.p2.g3d09d2c.assembly.stream.el9",
                "ose-cluster-logging-operator-container-6.0.15-202606051158.p2.assembly.stream.el9",
            ],
        }
        mock_cmd.return_value = (
            0,
            json.dumps({"nvrs": ["loki-rhel9-operator-metadata-container-6.0.15-1"], "errors": []}),
            "",
        )

        pipeline = self._make_pipeline()
        # Assembly pins use an older timestamp
        operand_nvrs = ["loki-rhel9-operator-container-6.0.15-202606050226.p2.g3d09d2c.assembly.stream.el9"]
        bundle_nvrs, operator_nvrs = asyncio.run(pipeline._trigger_bundle_build(operand_nvrs))

        cmd = mock_cmd.call_args[0][0]
        self.assertIn("loki-rhel9-operator-container-6.0.15-202606051158.p2.g3d09d2c.assembly.stream.el9", cmd)
        self.assertNotIn("ose-cluster-logging-operator-container-6.0.15-202606051158.p2.assembly.stream.el9", cmd)
        self.assertEqual(
            operator_nvrs,
            ["loki-rhel9-operator-container-6.0.15-202606051158.p2.g3d09d2c.assembly.stream.el9"],
        )

    @patch.object(PrepareReleaseLPPipeline, '_find_existing_bundles', new_callable=AsyncMock)
    def test_all_operators_filtered_skips_build(self, mock_find):
        """When no operators are pinned in the assembly, bundle build should be skipped."""
        mock_find.return_value = {
            "metadata": ["existing-bundle-1"],
            "olm_builds_not_found": [
                "ose-cluster-logging-operator-container-6.0.15-1.assembly.stream.el9",
            ],
            "olm_operator_nvrs": [
                "ose-cluster-logging-operator-container-6.0.15-1.assembly.stream.el9",
            ],
        }

        pipeline = self._make_pipeline()
        operand_nvrs = ["some-operand-container-6.0.15-1"]
        bundle_nvrs, operator_nvrs = asyncio.run(pipeline._trigger_bundle_build(operand_nvrs))

        self.assertEqual(bundle_nvrs, ["existing-bundle-1"])
        self.assertEqual(operator_nvrs, [])

    @patch.object(PrepareReleaseLPPipeline, '_find_existing_bundles', new_callable=AsyncMock)
    @patch('pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async', new_callable=AsyncMock)
    def test_stale_bundle_triggers_rebuild(self, mock_cmd, mock_find):
        """Bundles referencing old operand images must be rebuilt when the assembly pins a new version."""
        mock_find.return_value = {
            "metadata": [
                "loki-rhel9-operator-metadata-container-6.0.15-1.assembly.stream.el9-1",
            ],
            "olm_builds_not_found": [],
            "olm_operator_nvrs": [
                "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
            ],
            "olm_builds_detail": {
                "loki-rhel9-operator-metadata-container-6.0.15-1.assembly.stream.el9-1": {
                    "operator_nvr": "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
                    "operand_nvrs": [
                        "logging-loki-rhel9-container-6.0.15-OLD.assembly.stream.el9",
                    ],
                },
            },
        }
        mock_cmd.return_value = (
            0,
            json.dumps({"nvrs": ["loki-rhel9-operator-metadata-container-6.0.15-2"], "errors": []}),
            "",
        )

        pipeline = self._make_pipeline()
        operand_nvrs = [
            "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
            "logging-loki-rhel9-container-6.0.15-NEW.assembly.stream.el9",
        ]
        bundle_nvrs, operator_nvrs = asyncio.run(pipeline._trigger_bundle_build(operand_nvrs))

        # The stale bundle should have been removed and a rebuild triggered
        mock_cmd.assert_called_once()
        cmd = mock_cmd.call_args[0][0]
        self.assertIn("loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9", cmd)
        self.assertIn("loki-rhel9-operator-metadata-container-6.0.15-2", bundle_nvrs)
        # The old stale bundle should not be in the result
        self.assertNotIn("loki-rhel9-operator-metadata-container-6.0.15-1.assembly.stream.el9-1", bundle_nvrs)

    @patch.object(PrepareReleaseLPPipeline, '_find_existing_bundles', new_callable=AsyncMock)
    def test_matching_operands_reuses_bundle(self, mock_find):
        """Bundles whose operands match the assembly pins should be reused."""
        mock_find.return_value = {
            "metadata": [
                "loki-rhel9-operator-metadata-container-6.0.15-1.assembly.stream.el9-1",
            ],
            "olm_builds_not_found": [],
            "olm_operator_nvrs": [
                "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
            ],
            "olm_builds_detail": {
                "loki-rhel9-operator-metadata-container-6.0.15-1.assembly.stream.el9-1": {
                    "operator_nvr": "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
                    "operand_nvrs": [
                        "logging-loki-rhel9-container-6.0.15-SAME.assembly.stream.el9",
                    ],
                },
            },
        }

        pipeline = self._make_pipeline()
        operand_nvrs = [
            "loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9",
            "logging-loki-rhel9-container-6.0.15-SAME.assembly.stream.el9",
        ]
        bundle_nvrs, operator_nvrs = asyncio.run(pipeline._trigger_bundle_build(operand_nvrs))

        # The bundle should be reused since operands match
        self.assertEqual(bundle_nvrs, ["loki-rhel9-operator-metadata-container-6.0.15-1.assembly.stream.el9-1"])
        self.assertEqual(operator_nvrs, ["loki-rhel9-operator-container-6.0.15-1.assembly.stream.el9"])


class TestPrepareReleaseLPRun(unittest.TestCase):
    """Integration-level tests for the run() method with mocked externals."""

    def _make_pipeline(self, tmp_dir, **overrides):
        from pathlib import Path

        runtime = MagicMock()
        runtime.dry_run = True
        runtime.working_dir = Path(tmp_dir)
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
        }

        kwargs = dict(
            runtime=runtime,
            group="acm-2.17",
            assembly="2.17.3",
        )
        kwargs.update(overrides)
        return PrepareReleaseLPPipeline(**kwargs)

    @patch.object(PrepareReleaseLPPipeline, '_load_release_notes_template', return_value=None)
    @patch.object(PrepareReleaseLPPipeline, '_create_snapshot', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_trigger_fbc_build', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_trigger_bundle_build', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_load_assembly', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_load_product_from_group_config', new_callable=AsyncMock)
    def test_run_dry_run(self, mock_product, mock_assembly, mock_bundle, mock_fbc, mock_snapshot, mock_template):
        import tempfile

        mock_product.return_value = "acm"
        mock_assembly.return_value = {
            'assembly': {
                'type': 'standard',
                'basis': {'fbc_pullspecs': ['quay.io/test/fbc@sha256:abc']},
                'members': {
                    'images': [
                        {
                            'distgit_key': 'search-v2-api-container',
                            'metadata': {'is': {'nvr': 'search-v2-api-container-2.17.3-1'}},
                        },
                    ],
                },
            },
        }
        mock_bundle.return_value = ([], [])
        mock_fbc.return_value = ([], [])
        mock_snapshot.return_value = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            pipeline = self._make_pipeline(tmp_dir)
            asyncio.run(pipeline.run())

        mock_product.assert_called_once()
        mock_assembly.assert_called_once()
        mock_bundle.assert_called_once_with(['search-v2-api-container-2.17.3-1'])
        mock_fbc.assert_called_once_with([])
        mock_snapshot.assert_called_once_with(['search-v2-api-container-2.17.3-1'])

    @patch('pyartcd.pipelines.prepare_release_lp.validate_fbc_related_images', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_load_release_notes_template', return_value=None)
    @patch.object(PrepareReleaseLPPipeline, '_create_snapshot', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_trigger_fbc_build', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_trigger_bundle_build', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_load_assembly', new_callable=AsyncMock)
    @patch.object(PrepareReleaseLPPipeline, '_load_product_from_group_config', new_callable=AsyncMock)
    def test_run_validates_multi_fbc(
        self,
        mock_product,
        mock_assembly,
        mock_bundle,
        mock_fbc,
        mock_snapshot,
        mock_template,
        mock_validate,
    ):
        """When multiple FBC NVRs are produced, consistency validation should run."""
        import tempfile

        mock_product.return_value = "logging"
        mock_assembly.return_value = {
            'assembly': {
                'type': 'standard',
                'members': {
                    'images': [
                        {'distgit_key': 'op1', 'metadata': {'is': {'nvr': 'op1-6.4.1-1'}}},
                    ]
                },
            },
        }
        mock_bundle.return_value = ([], [])
        mock_fbc.return_value = (
            ["fbc-6.4.1-1.ocp4.18", "fbc-6.4.1-1.ocp4.19"],
            ["quay.io/test/fbc@sha256:a18", "quay.io/test/fbc@sha256:a19"],
        )
        mock_validate.return_value = ["op1-6.4.1-1"]
        mock_snapshot.return_value = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            pipeline = self._make_pipeline(tmp_dir, group="logging-6.4", assembly="6.4.1")
            asyncio.run(pipeline.run())

        mock_validate.assert_called_once_with(
            ["quay.io/test/fbc@sha256:a18", "quay.io/test/fbc@sha256:a19"],
            "logging",
        )


class TestPrepareReleaseLPCreateSnapshot(unittest.TestCase):
    """Tests for _create_snapshot handling multi-document YAML from elliott."""

    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = overrides.pop('dry_run', False)
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
        }
        kwargs = dict(runtime=runtime, group="logging-6.0", assembly="6.0.15")
        kwargs.update(overrides)
        return PrepareReleaseLPPipeline(**kwargs)

    @patch('pyartcd.pipelines.prepare_release_lp.exectools')
    def test_parses_multi_document_yaml(self, mock_exectools):
        """When elliott snapshot new returns multiple YAML documents, all are parsed."""
        multi_doc_yaml = (
            "apiVersion: appstudio.redhat.com/v1alpha1\n"
            "kind: Snapshot\n"
            "spec:\n"
            "  application: app-one\n"
            "  components:\n"
            "  - name: comp-a\n"
            "    containerImage: quay.io/test/a@sha256:aaa\n"
            "    source:\n"
            "      git:\n"
            "        url: https://github.com/test/a\n"
            "        revision: abc123\n"
            "---\n"
            "apiVersion: appstudio.redhat.com/v1alpha1\n"
            "kind: Snapshot\n"
            "spec:\n"
            "  application: app-two\n"
            "  components:\n"
            "  - name: comp-b\n"
            "    containerImage: quay.io/test/b@sha256:bbb\n"
            "    source:\n"
            "      git:\n"
            "        url: https://github.com/test/b\n"
            "        revision: def456\n"
        )
        mock_exectools.cmd_assert.return_value = (multi_doc_yaml, "")

        pipeline = self._make_pipeline()
        snapshots = asyncio.run(pipeline._create_snapshot(["nvr-a-1.0-1", "nvr-b-1.0-1"]))

        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0].spec.application, "app-one")
        self.assertEqual(snapshots[1].spec.application, "app-two")
        self.assertEqual(len(snapshots[0].spec.components), 1)
        self.assertEqual(len(snapshots[1].spec.components), 1)

    @patch('pyartcd.pipelines.prepare_release_lp.exectools')
    def test_parses_single_document_yaml(self, mock_exectools):
        """Single-document output still works."""
        single_doc_yaml = (
            "apiVersion: appstudio.redhat.com/v1alpha1\n"
            "kind: Snapshot\n"
            "spec:\n"
            "  application: my-app\n"
            "  components:\n"
            "  - name: comp-a\n"
            "    containerImage: quay.io/test/a@sha256:aaa\n"
            "    source:\n"
            "      git:\n"
            "        url: https://github.com/test/a\n"
            "        revision: abc123\n"
        )
        mock_exectools.cmd_assert.return_value = (single_doc_yaml, "")

        pipeline = self._make_pipeline()
        snapshots = asyncio.run(pipeline._create_snapshot(["nvr-a-1.0-1"]))

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].spec.application, "my-app")

    def test_returns_empty_for_no_builds(self):
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._create_snapshot([]))
        self.assertEqual(result, [])


class TestLoadMrApproversFromGroupConfig(unittest.TestCase):
    """Tests for _load_mr_approvers_from_group_config."""

    def _make_pipeline(self):
        runtime = MagicMock()
        runtime.dry_run = False
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
        }
        return PrepareReleaseLPPipeline(runtime=runtime, group="acm-2.17", assembly="2.17.3")

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_valid_dict_returned(self, mock_cmd):
        mock_cmd.return_value = (0, "QE:\n- user1\n- user2\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {"QE": ["user1", "user2"]})

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_non_dict_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "- user1\n- user2\n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_none_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "None", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_null_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "null", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_empty_output_returns_empty(self, mock_cmd):
        mock_cmd.return_value = (0, "  \n", "")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_exception_returns_empty(self, mock_cmd):
        mock_cmd.side_effect = RuntimeError("doozer failed")
        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline._load_mr_approvers_from_group_config())
        self.assertEqual(result, {})


class TestCreateShipmentMrApprovalRules(unittest.TestCase):
    """Tests for approval-rule handling inside _create_shipment_mr."""

    def _make_pipeline(self, dry_run=False):
        runtime = MagicMock()
        runtime.dry_run = dry_run
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {
            "build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"},
            "gitlab_url": "https://gitlab.example.com",
        }

        pipeline = PrepareReleaseLPPipeline(
            runtime=runtime,
            group="acm-2.17",
            assembly="2.17.3",
            create_mr=True,
        )
        pipeline.product = "acm"
        pipeline.shipment_data_repo = AsyncMock()
        pipeline.shipment_data_repo_push_url = "https://gitlab.example.com/user/ocp-shipment-data.git"
        pipeline.shipment_data_repo_pull_url = "https://gitlab.example.com/org/ocp-shipment-data.git"
        return pipeline

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_approval_rules_set_after_mr_creation(self, mock_cmd):
        """Approval rules from group config should be applied to the created MR."""
        mock_cmd.return_value = (0, "QE:\n- reviewer1\n", "")
        pipeline = self._make_pipeline()

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        type(pipeline)._gitlab = PropertyMock(return_value=mock_gitlab)

        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/ocp-shipment-data/-/merge_requests/1"
        mock_source_project = MagicMock()
        mock_source_project.mergerequests.create.return_value = mock_mr
        mock_target_project = MagicMock()
        mock_target_project.id = 42

        with (
            patch.object(pipeline, '_get_gitlab_project', side_effect=[mock_source_project, mock_target_project]),
            patch.object(pipeline, '_write_shipment_file', new_callable=AsyncMock),
        ):
            pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)
            pipeline.shipment_data_repo.add_all = AsyncMock()
            pipeline.shipment_data_repo.log_diff = AsyncMock()
            pipeline.shipment_data_repo.create_branch = AsyncMock()

            result = asyncio.run(pipeline._create_shipment_mr({"image": MagicMock()}))

            mock_gitlab.set_mr_approval_rules.assert_called_once_with(mock_mr.web_url, {"QE": ["reviewer1"]})
            self.assertEqual(result, mock_mr.web_url)

    @patch("pyartcd.pipelines.prepare_release_lp.exectools.cmd_gather_async")
    def test_no_approvers_skips_set_rules(self, mock_cmd):
        """When mr_approvers is not configured, set_mr_approval_rules should not be called."""
        mock_cmd.return_value = (0, "None", "")
        pipeline = self._make_pipeline()

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        type(pipeline)._gitlab = PropertyMock(return_value=mock_gitlab)

        mock_mr = MagicMock()
        mock_mr.web_url = "https://gitlab.example.com/org/ocp-shipment-data/-/merge_requests/2"
        mock_source_project = MagicMock()
        mock_source_project.mergerequests.create.return_value = mock_mr
        mock_target_project = MagicMock()
        mock_target_project.id = 42

        with (
            patch.object(pipeline, '_get_gitlab_project', side_effect=[mock_source_project, mock_target_project]),
            patch.object(pipeline, '_write_shipment_file', new_callable=AsyncMock),
        ):
            pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)
            pipeline.shipment_data_repo.add_all = AsyncMock()
            pipeline.shipment_data_repo.log_diff = AsyncMock()
            pipeline.shipment_data_repo.create_branch = AsyncMock()

            asyncio.run(pipeline._create_shipment_mr({"image": MagicMock()}))

            mock_gitlab.set_mr_approval_rules.assert_not_called()

    def test_dry_run_skips_mr_creation(self):
        """In dry-run mode, no MR is created and no approval rules are set."""
        pipeline = self._make_pipeline(dry_run=True)

        mock_gitlab = MagicMock()
        mock_gitlab.set_mr_approval_rules = AsyncMock()
        type(pipeline)._gitlab = PropertyMock(return_value=mock_gitlab)

        with patch.object(pipeline, '_write_shipment_file', new_callable=AsyncMock):
            pipeline.shipment_data_repo.commit_push = AsyncMock(return_value=True)
            pipeline.shipment_data_repo.add_all = AsyncMock()
            pipeline.shipment_data_repo.log_diff = AsyncMock()
            pipeline.shipment_data_repo.create_branch = AsyncMock()

            result = asyncio.run(pipeline._create_shipment_mr({"image": MagicMock()}))

            self.assertIn("placeholder", result)
            mock_gitlab.set_mr_approval_rules.assert_not_called()


if __name__ == '__main__':
    unittest.main()
