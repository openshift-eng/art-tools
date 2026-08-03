import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import click
from pyartcd.pipelines.gen_assembly_lp import GenAssemblyLPPipeline


class TestGenAssemblyLPPipeline(unittest.TestCase):
    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = overrides.pop('dry_run', True)
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {"build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"}}

        kwargs = dict(
            runtime=runtime,
            group="acm-2.17",
            assembly="2.17.3",
            fbc_pullspecs=["quay.io/test/acm-fbc@sha256:abc"],
        )
        kwargs.update(overrides)
        pipeline = GenAssemblyLPPipeline(**kwargs)
        pipeline.product = "acm"
        return pipeline

    def test_init_requires_fbc_or_basis(self):
        """Constructor raises when neither fbc_pullspecs nor basis_assembly is provided."""
        runtime = MagicMock()
        runtime.dry_run = True
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {}

        with self.assertRaises(ValueError):
            GenAssemblyLPPipeline(
                runtime=runtime,
                group="acm-2.17",
                assembly="2.17.3",
                fbc_pullspecs=[],
                basis_assembly=None,
            )

    def test_init_accepts_fbc_only(self):
        pipeline = self._make_pipeline()
        self.assertEqual(pipeline.fbc_pullspecs, ["quay.io/test/acm-fbc@sha256:abc"])
        self.assertIsNone(pipeline.basis_assembly)

    def test_init_accepts_basis_only(self):
        pipeline = self._make_pipeline(fbc_pullspecs=[], basis_assembly="2.17.2")
        self.assertEqual(pipeline.basis_assembly, "2.17.2")
        self.assertEqual(pipeline.fbc_pullspecs, [])

    def test_is_nvr_from_current_group_matching(self):
        pipeline = self._make_pipeline()
        self.assertTrue(pipeline._is_nvr_from_current_group("acm-operator-2.17.3-20260101"))

    def test_is_nvr_from_current_group_not_matching(self):
        pipeline = self._make_pipeline()
        self.assertFalse(pipeline._is_nvr_from_current_group("kube-rbac-proxy-v4.20.0-20260101"))

    def test_is_nvr_from_current_group_prevents_false_positive(self):
        """Assembly 2.17 should NOT match NVR version 2.170 (prevents prefix false positives)."""
        pipeline = self._make_pipeline()
        self.assertFalse(pipeline._is_nvr_from_current_group("something-2.170.0-1"))

    def test_categorize_nvrs(self):
        pipeline = self._make_pipeline()
        nvrs = [
            "search-v2-api-container-2.17.3-1",
            "console-container-2.17.3-1",
            "acm-operator-fbc-2.17.3-1",
            "ose-cluster-logging-operator-metadata-container-2.17.3-1",
            "kube-rbac-proxy-v4.20.0-1",
        ]
        result = pipeline._categorize_nvrs(nvrs)
        self.assertEqual(len(result["image"]), 2)
        self.assertEqual(len(result["bundle"]), 1)
        self.assertEqual(len(result["fbc"]), 1)
        self.assertEqual(len(result["external"]), 1)
        self.assertIn("search-v2-api-container-2.17.3-1", result["image"])
        self.assertIn("console-container-2.17.3-1", result["image"])
        self.assertIn("ose-cluster-logging-operator-metadata-container-2.17.3-1", result["bundle"])
        self.assertIn("acm-operator-fbc-2.17.3-1", result["fbc"])
        self.assertIn("kube-rbac-proxy-v4.20.0-1", result["external"])

    def test_nvrs_to_distgit_map(self):
        pipeline = self._make_pipeline()
        nvrs = ["search-v2-api-container-2.17.3-1", "console-container-2.17.3-1"]
        result = pipeline._nvrs_to_distgit_map(nvrs)
        self.assertEqual(result["search-v2-api-container"], "search-v2-api-container-2.17.3-1")
        self.assertEqual(result["console-container"], "console-container-2.17.3-1")

    def test_apply_includes_swaps_existing(self):
        pipeline = self._make_pipeline(include=["search-v2-api-container=search-v2-api-container-2.17.3-2"])
        operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
            "console-container": "console-container-2.17.3-1",
        }
        result = pipeline._apply_includes(operand_map)
        self.assertEqual(result["search-v2-api-container"], "search-v2-api-container-2.17.3-2")
        self.assertEqual(result["console-container"], "console-container-2.17.3-1")

    def test_apply_includes_rejects_unknown_key(self):
        pipeline = self._make_pipeline(include=["nonexistent=some-nvr-1.0-1"])
        operand_map = {"search-v2-api-container": "search-v2-api-container-2.17.3-1"}
        with self.assertRaises(click.ClickException):
            pipeline._apply_includes(operand_map)

    def test_apply_includes_rejects_bad_format(self):
        pipeline = self._make_pipeline(include=["bad-format-no-equals"])
        operand_map = {"search-v2-api-container": "search-v2-api-container-2.17.3-1"}
        with self.assertRaises(click.ClickException):
            pipeline._apply_includes(operand_map)

    def test_apply_extra_image_nvrs_adds_new(self):
        pipeline = self._make_pipeline(extra_image_nvrs=["extra-operand-container-2.17.3-1"])
        operand_map = {"search-v2-api-container": "search-v2-api-container-2.17.3-1"}
        result = pipeline._apply_extra_image_nvrs(operand_map)
        self.assertIn("extra-operand-container", result)
        self.assertEqual(result["extra-operand-container"], "extra-operand-container-2.17.3-1")

    def test_apply_extra_image_nvrs_rejects_version_mismatch(self):
        pipeline = self._make_pipeline(extra_image_nvrs=["wrong-operand-container-3.1.0-1"])
        operand_map = {"search-v2-api-container": "search-v2-api-container-2.17.3-1"}
        with self.assertRaises(click.ClickException) as ctx:
            pipeline._apply_extra_image_nvrs(operand_map)
        self.assertIn("3.1.0", str(ctx.exception))
        self.assertIn("does not match assembly", str(ctx.exception))

    def test_generate_assembly_definition_fresh(self):
        pipeline = self._make_pipeline()
        operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
            "console-container": "console-container-2.17.3-1",
        }

        result = pipeline._generate_assembly_definition(operand_map)

        assembly = result['releases']['2.17.3']['assembly']
        self.assertEqual(assembly['type'], 'standard')
        self.assertIn('fbc_pullspecs', assembly['basis'])
        self.assertEqual(len(assembly['members']['images']), 2)

        nvrs = {e['distgit_key']: e['metadata']['is']['nvr'] for e in assembly['members']['images']}
        self.assertEqual(nvrs['console-container'], 'console-container-2.17.3-1')
        self.assertEqual(nvrs['search-v2-api-container'], 'search-v2-api-container-2.17.3-1')

    def test_generate_assembly_definition_with_fbc_pullspecs_map_single_operator(self):
        """Single operator per version: basis.fbc_pullspecs values should be strings."""
        pipeline = self._make_pipeline(
            fbc_pullspecs=[
                "quay.io/test/fbc@sha256:abc",
                "quay.io/test/fbc@sha256:def",
            ]
        )
        operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
        }
        fbc_pullspecs_map = {
            "4.18": ["quay.io/test/fbc@sha256:abc"],
            "4.19": ["quay.io/test/fbc@sha256:def"],
        }

        result = pipeline._generate_assembly_definition(operand_map, fbc_pullspecs_map=fbc_pullspecs_map)

        assembly = result['releases']['2.17.3']['assembly']
        self.assertIsInstance(assembly['basis']['fbc_pullspecs'], dict)
        self.assertEqual(assembly['basis']['fbc_pullspecs']['4.18'], "quay.io/test/fbc@sha256:abc")
        self.assertEqual(assembly['basis']['fbc_pullspecs']['4.19'], "quay.io/test/fbc@sha256:def")

    def test_generate_assembly_definition_with_fbc_pullspecs_map_multi_operator(self):
        """Multiple operators per version: basis.fbc_pullspecs values should be lists."""
        pipeline = self._make_pipeline(
            fbc_pullspecs=[
                "quay.io/test/op-a@sha256:418",
                "quay.io/test/op-b@sha256:418",
                "quay.io/test/op-a@sha256:419",
                "quay.io/test/op-b@sha256:419",
            ]
        )
        operand_map = {"some-container": "some-container-2.17.3-1"}
        fbc_pullspecs_map = {
            "4.18": ["quay.io/test/op-a@sha256:418", "quay.io/test/op-b@sha256:418"],
            "4.19": ["quay.io/test/op-a@sha256:419", "quay.io/test/op-b@sha256:419"],
        }

        result = pipeline._generate_assembly_definition(operand_map, fbc_pullspecs_map=fbc_pullspecs_map)

        assembly = result['releases']['2.17.3']['assembly']
        self.assertIsInstance(assembly['basis']['fbc_pullspecs']['4.18'], list)
        self.assertEqual(len(assembly['basis']['fbc_pullspecs']['4.18']), 2)

    def test_generate_assembly_definition_without_fbc_pullspecs_map_falls_back_to_list(self):
        """When no fbc_pullspecs_map (or empty), basis.fbc_pullspecs should be a flat list."""
        pipeline = self._make_pipeline()
        operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
        }

        result = pipeline._generate_assembly_definition(operand_map, fbc_pullspecs_map=None)

        assembly = result['releases']['2.17.3']['assembly']
        self.assertIsInstance(assembly['basis']['fbc_pullspecs'], list)

    def test_generate_assembly_definition_inherited_with_diff(self):
        """Inherited assembly should only include changed entries with '!' merge operator."""
        pipeline = self._make_pipeline(
            fbc_pullspecs=[],
            basis_assembly="2.17.2",
        )
        operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-2",
            "console-container": "console-container-2.17.3-1",
        }
        parent_operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.2-1",
            "console-container": "console-container-2.17.3-1",
        }

        result = pipeline._generate_assembly_definition(operand_map, parent_operand_map)

        assembly = result['releases']['2.17.3']['assembly']
        self.assertEqual(assembly['basis']['assembly'], '2.17.2')
        self.assertEqual(len(assembly['members']['images']), 1)

        changed_entry = assembly['members']['images'][0]
        self.assertEqual(changed_entry['distgit_key'], 'search-v2-api-container')
        self.assertEqual(changed_entry['metadata']['is']['nvr!'], 'search-v2-api-container-2.17.3-2')

    def test_generate_assembly_definition_inherited_no_diff(self):
        """Inherited assembly with no changes should have empty members.images."""
        pipeline = self._make_pipeline(
            fbc_pullspecs=[],
            basis_assembly="2.17.2",
        )
        operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
        }
        parent_operand_map = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
        }

        result = pipeline._generate_assembly_definition(operand_map, parent_operand_map)

        assembly = result['releases']['2.17.3']['assembly']
        self.assertEqual(assembly['basis']['assembly'], '2.17.2')
        self.assertEqual(assembly['members']['images'], [])


class TestGenAssemblyLPMultiFBC(unittest.TestCase):
    """Tests for multi-FBC (multi-OCP-version) validation in gen-assembly-lp."""

    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = True
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {"build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"}}

        kwargs = dict(
            runtime=runtime,
            group="logging-6.4",
            assembly="6.4.1",
            fbc_pullspecs=[
                "quay.io/test/fbc@sha256:ocp418",
                "quay.io/test/fbc@sha256:ocp419",
            ],
        )
        kwargs.update(overrides)
        pipeline = GenAssemblyLPPipeline(**kwargs)
        pipeline.product = "openshift-logging"
        return pipeline

    @patch('pyartcd.pipelines.gen_assembly_lp.validate_fbc_related_images', new_callable=AsyncMock)
    def test_extract_nvrs_calls_validation_for_multiple_pullspecs(self, mock_validate):
        """When multiple FBC pullspecs are provided, validation should be called."""
        mock_validate.return_value = [
            "cluster-logging-operator-6.4.1-1",
            "log-file-metric-exporter-6.4.1-1",
        ]
        pipeline = self._make_pipeline()

        result = asyncio.run(pipeline._extract_nvrs_from_fbc())

        mock_validate.assert_called_once_with(
            ["quay.io/test/fbc@sha256:ocp418", "quay.io/test/fbc@sha256:ocp419"],
            "openshift-logging",
        )
        self.assertIn("cluster-logging-operator", result)

    @patch('pyartcd.pipelines.gen_assembly_lp.validate_fbc_related_images', new_callable=AsyncMock)
    @patch('elliottlib.util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    def test_extract_nvrs_skips_validation_for_single_pullspec(self, mock_extract_nvrs, mock_validate):
        """When only one FBC pullspec is provided, skip validation and extract directly."""
        mock_extract_nvrs.return_value = [
            "cluster-logging-operator-6.4.1-1",
            "log-file-metric-exporter-6.4.1-1",
        ]
        pipeline = self._make_pipeline(
            fbc_pullspecs=["quay.io/test/fbc@sha256:single"],
        )

        result = asyncio.run(pipeline._extract_nvrs_from_fbc())

        mock_validate.assert_not_called()
        mock_extract_nvrs.assert_called_once_with("quay.io/test/fbc@sha256:single", "openshift-logging")
        self.assertIn("cluster-logging-operator", result)
        self.assertIn("log-file-metric-exporter", result)

    @patch('pyartcd.pipelines.gen_assembly_lp.extract_fbc_labels', new_callable=AsyncMock)
    def test_build_fbc_pullspecs_map(self, mock_labels):
        """Should map pullspecs to OCP versions from NVR labels."""
        mock_labels.side_effect = [
            {'nvr': 'logging-fbc-6.4.1-1234.ocp4.18', 'doozer_key': 'cluster-logging-operator'},
            {'nvr': 'logging-fbc-6.4.1-1234.ocp4.19', 'doozer_key': 'cluster-logging-operator'},
        ]
        pipeline = self._make_pipeline()

        result = asyncio.run(pipeline._build_fbc_pullspecs_map())

        self.assertEqual(
            result,
            {
                "4.18": ["quay.io/test/fbc@sha256:ocp418"],
                "4.19": ["quay.io/test/fbc@sha256:ocp419"],
            },
        )

    @patch('pyartcd.pipelines.gen_assembly_lp.extract_fbc_labels', new_callable=AsyncMock)
    def test_build_fbc_pullspecs_map_falls_back_on_missing_version(self, mock_labels):
        """Should return empty dict if OCP version cannot be determined."""
        mock_labels.return_value = {'nvr': 'some-fbc-6.4.1-1234', 'doozer_key': 'some-operator'}
        pipeline = self._make_pipeline()

        result = asyncio.run(pipeline._build_fbc_pullspecs_map())

        self.assertEqual(result, {})


class TestGenAssemblyLPRun(unittest.TestCase):
    """Integration-level tests for the run() method with mocked externals."""

    def _make_pipeline(self, **overrides):
        runtime = MagicMock()
        runtime.dry_run = True
        runtime.working_dir = MagicMock()
        runtime.working_dir.absolute.return_value = MagicMock()
        runtime.config = {"build_config": {"ocp_build_data_repo_push_url": "https://github.com/test/ocp-build-data"}}

        kwargs = dict(
            runtime=runtime,
            group="acm-2.17",
            assembly="2.17.3",
            fbc_pullspecs=["quay.io/test/acm-fbc@sha256:abc"],
        )
        kwargs.update(overrides)
        return GenAssemblyLPPipeline(**kwargs)

    @patch.object(GenAssemblyLPPipeline, '_create_or_update_pull_request', new_callable=AsyncMock)
    @patch.object(GenAssemblyLPPipeline, '_build_fbc_pullspecs_map', new_callable=AsyncMock)
    @patch.object(GenAssemblyLPPipeline, '_extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch.object(GenAssemblyLPPipeline, '_load_product_from_group_config', new_callable=AsyncMock)
    def test_run_fresh_assembly(self, mock_load_product, mock_extract, mock_fbc_map, mock_pr):
        mock_load_product.return_value = "acm"
        mock_extract.return_value = {
            "search-v2-api-container": "search-v2-api-container-2.17.3-1",
            "console-container": "console-container-2.17.3-1",
        }
        mock_fbc_map.return_value = {}
        mock_pr.return_value = None

        pipeline = self._make_pipeline()
        result = asyncio.run(pipeline.run())

        mock_extract.assert_called_once()
        mock_fbc_map.assert_called_once()
        mock_pr.assert_called_once()
        self.assertIn('releases', result)
        self.assertIn('2.17.3', result['releases'])

    @patch.object(GenAssemblyLPPipeline, '_create_or_update_pull_request', new_callable=AsyncMock)
    @patch.object(GenAssemblyLPPipeline, '_resolve_parent_operands', new_callable=AsyncMock)
    @patch.object(GenAssemblyLPPipeline, '_load_product_from_group_config', new_callable=AsyncMock)
    def test_run_inherited_assembly(self, mock_load_product, mock_resolve, mock_pr):
        mock_load_product.return_value = "acm"
        mock_resolve.return_value = {
            "search-v2-api-container": "search-v2-api-container-2.17.2-1",
            "console-container": "console-container-2.17.2-1",
        }
        mock_pr.return_value = None

        pipeline = self._make_pipeline(
            fbc_pullspecs=[],
            basis_assembly="2.17.2",
            include=["search-v2-api-container=search-v2-api-container-2.17.3-1"],
        )
        result = asyncio.run(pipeline.run())

        mock_resolve.assert_called_once()
        mock_pr.assert_called_once()

        assembly = result['releases']['2.17.3']['assembly']
        self.assertEqual(assembly['basis']['assembly'], '2.17.2')
        self.assertEqual(len(assembly['members']['images']), 1)
        self.assertEqual(
            assembly['members']['images'][0]['metadata']['is']['nvr!'],
            'search-v2-api-container-2.17.3-1',
        )


if __name__ == '__main__':
    unittest.main()
