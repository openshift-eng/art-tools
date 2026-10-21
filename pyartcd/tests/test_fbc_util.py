import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pyartcd.fbc_util import (
    _get_source_bundle_versions,
    extract_ocp_version_from_nvr,
    is_nvr_from_assembly,
    validate_fbc_related_images,
    validate_layered_product_fbc_nvrs,
    validate_layered_product_group_assembly,
)


class TestExtractOcpVersionFromNvr(unittest.TestCase):
    def test_extracts_version_from_lp_fbc_nvr(self):
        self.assertEqual(
            extract_ocp_version_from_nvr("cluster-logging-operator-fbc-6.4.1-1234.ocp4.18"),
            "4.18",
        )

    def test_extracts_version_multi_digit_minor(self):
        self.assertEqual(
            extract_ocp_version_from_nvr("oadp-operator-fbc-1.5.3-999.ocp4.21"),
            "4.21",
        )

    def test_returns_none_for_ocp_fbc_nvr(self):
        """OCP FBC NVRs don't have .ocp suffix."""
        self.assertIsNone(extract_ocp_version_from_nvr("cluster-nfd-operator-fbc-4.18.0-20260101"))

    def test_returns_none_for_non_fbc_nvr(self):
        self.assertIsNone(extract_ocp_version_from_nvr("search-v2-api-container-2.17.3-1"))


class TestValidateLayeredProductReleaseVersions(unittest.TestCase):
    def test_group_and_assembly_match(self):
        validate_layered_product_group_assembly("logging-6.2", "6.2.13")

    def test_group_and_assembly_mismatch(self):
        with self.assertRaisesRegex(ValueError, "Assembly '6.5.13' does not belong to group 'logging-6.2'"):
            validate_layered_product_group_assembly("logging-6.2", "6.5.13")

    def test_fbc_nvrs_match_full_assembly(self):
        validate_layered_product_fbc_nvrs(
            "6.2.13",
            [
                "cluster-logging-operator-fbc-6.2.13-20260910151430.ocp4.16",
                "loki-operator-fbc-6.2.13-20260910151552.ocp4.20",
            ],
        )

    def test_two_component_assembly_accepts_matching_release_train(self):
        validate_layered_product_fbc_nvrs(
            "6.2",
            ["cluster-logging-operator-fbc-6.2.13-20260910151430.ocp4.16"],
        )

    def test_fbc_nvr_mismatch(self):
        nvr = "cluster-logging-operator-fbc-6.2.13-20260910151430.ocp4.16"
        with self.assertRaisesRegex(ValueError, "FBC NVRs do not match assembly '6.5.13'"):
            validate_layered_product_fbc_nvrs("6.5.13", [nvr])

    def test_fbc_nvr_patch_mismatch_fails_by_default(self):
        nvr = "opentelemetry-operator-fbc-0.158.0-20261006220437.ocp4.16"
        with self.assertRaisesRegex(ValueError, "FBC NVRs do not match assembly '0.158.1'"):
            validate_layered_product_fbc_nvrs("0.158.1", [nvr])

    def test_fbc_nvr_patch_mismatch_accepted_when_ignored(self):
        nvr = "opentelemetry-operator-fbc-0.158.0-20261006220437.ocp4.16"
        validate_layered_product_fbc_nvrs("0.158.1", [nvr], ignore_patch_version=True)

    def test_fbc_nvr_major_minor_mismatch_still_fails_when_patch_ignored(self):
        nvr = "cluster-logging-operator-fbc-6.3.12-20260910151430.ocp4.16"
        with self.assertRaisesRegex(ValueError, "FBC NVRs do not match assembly '6.2.13'"):
            validate_layered_product_fbc_nvrs("6.2.13", [nvr], ignore_patch_version=True)

    def test_unparseable_fbc_nvr_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "Cannot determine.*version of FBC NVR"):
            validate_layered_product_fbc_nvrs("6.2.13", ["not-an-nvr"])


class TestIsNvrFromAssembly(unittest.TestCase):
    def test_version_classification(self):
        cases = [
            ("image-6.3.4-1", "6.3.3", True),
            ("image-v6.3.4-1", "v6.3.3", True),
            ("image-6.30.0-1", "6.3.3", False),
            ("image-7.3.3-1", "6.3.3", False),
            ("image-6.3.3-1", "6", True),
            ("image-6-1", "6.3.3", False),
            ("postgresql-15-container-1-1791974001", "3.18.2", False),
            ("kube-rbac-proxy-container-v4.22.0-1", "3.18.2", False),
            ("malformed", "3.18.2", True),
        ]
        for nvr, assembly, expected in cases:
            with self.subTest(nvr=nvr, assembly=assembly):
                self.assertEqual(is_nvr_from_assembly(nvr, assembly), expected)


class TestSourceBundleVersions(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.fbc_util.KonfluxDb')
    async def test_version_comes_from_selected_csv_not_nvr(self, mock_db):
        db = mock_db.return_value
        db.get_build_records_by_nvrs = AsyncMock(
            side_effect=[
                [SimpleNamespace(nvr='operator-fbc-99.0.0-1', bundle_nvrs=['bundle-42.0.0-1'])],
                [SimpleNamespace(nvr='bundle-42.0.0-1', bundle_csv_name='quay-operator.v3.16.6')],
            ]
        )
        result = await _get_source_bundle_versions(['operator-fbc-99.0.0-1'])
        self.assertEqual(result, {'operator-fbc-99.0.0-1': '3.16.6'})
        self.assertEqual(db.get_build_records_by_nvrs.await_count, 2)

    @patch('pyartcd.fbc_util.KonfluxDb')
    async def test_missing_or_ambiguous_provenance_fails(self, mock_db):
        for bundle_nvrs, csv_names in [
            ([], []),
            (['bundle'], [None]),
            (['bundle'], ['unversioned']),
            (['bundle', 'other-bundle'], ['operator.v1.0.0', 'operator.v1.0.1']),
        ]:
            with self.subTest(csv_names=csv_names):
                mock_db.return_value.get_build_records_by_nvrs = AsyncMock(
                    side_effect=[
                        [SimpleNamespace(nvr='fbc', bundle_nvrs=bundle_nvrs)],
                        [SimpleNamespace(nvr=nvr, bundle_csv_name=csv) for nvr, csv in zip(bundle_nvrs, csv_names)],
                    ]
                )
                with self.assertRaises(RuntimeError):
                    await _get_source_bundle_versions(['fbc'])

    async def test_missing_identity_fails(self):
        with self.assertRaisesRegex(RuntimeError, 'identity labels'):
            await _get_source_bundle_versions([None])


class TestFinalFbcOperands(unittest.IsolatedAsyncioTestCase):
    async def test_overrides_win_before_consistency_and_versions_do_not_filter_operands(self):
        pullspecs = ['quay.io/fbc@sha256:a', 'quay.io/fbc@sha256:b']
        operator = 'quay-operator-container-3.17.7-202610202329.p2.g015d069.assembly.stream.el9'
        quay = 'quay-quay-container-3.16.7-202610201501.p3.gcfeea14.assembly.stream.el9'
        overrides = {'quay-operator-container': operator, 'quay-quay-container': quay}
        clair = 'quay-clair-container-3.15.1-1'
        with (
            patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock) as labels,
            patch('pyartcd.fbc_util._get_source_bundle_versions', new_callable=AsyncMock) as versions,
            patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock) as extract,
        ):
            labels.side_effect = [{'nvr': nvr, 'doozer_key': 'quay-operator'} for nvr in ('fbc-a', 'fbc-b')]
            versions.return_value = {'fbc-a': '3.16.6', 'fbc-b': '3.16.6'}
            extract.side_effect = [
                ['quay-operator-container-3.16.6-1', 'quay-quay-container-3.16.6-1', clair],
                ['quay-operator-container-3.16.6-2', 'quay-quay-container-3.16.6-2', clair],
            ]
            result = await validate_fbc_related_images(
                pullspecs,
                'quay',
                assembly='3.17.7',
                use_source_bundle=True,
                image_components={*overrides, 'quay-clair-container'},
                image_overrides=overrides,
            )
            self.assertEqual(result, sorted([operator, quay, clair]))
            for pullspec in pullspecs:
                extract.assert_any_await(
                    pullspec, 'quay', assembly='3.17.7', version_scoped=True, bundle_version='3.16.6'
                )

    async def test_inconsistent_bundle_version_or_remaining_operand_fails(self):
        for versions, images, error in [
            (
                {'fbc-a': '3.16.6', 'fbc-b': '3.16.7'},
                [['quay-clair-container-3.15.1-1']] * 2,
                'Inconsistent source bundle versions',
            ),
            (
                {'fbc-a': '3.16.6', 'fbc-b': '3.16.6'},
                [['quay-clair-container-3.15.1-1'], ['quay-clair-container-3.15.1-2']],
                'mismatched related images',
            ),
        ]:
            with (
                self.subTest(versions=versions, images=images),
                patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock) as labels,
                patch('pyartcd.fbc_util._get_source_bundle_versions', new_callable=AsyncMock) as source,
                patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock) as extract,
            ):
                labels.side_effect = [{'nvr': nvr, 'doozer_key': 'quay-operator'} for nvr in ('fbc-a', 'fbc-b')]
                source.return_value = versions
                extract.side_effect = images
                with self.assertRaisesRegex(RuntimeError, error):
                    await validate_fbc_related_images(
                        ['quay.io/fbc@sha256:a', 'quay.io/fbc@sha256:b'],
                        'quay',
                        assembly='3.17.7',
                        use_source_bundle=True,
                        image_components={'quay-clair-container'},
                    )


class TestValidateFbcRelatedImages(unittest.TestCase):
    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_external_differences_preserve_union_and_provenance(self, mock_labels, mock_extract):
        """Reproduce the external-only Quay mismatches from Jenkins build #6."""
        pullspecs = [f"quay.io/test/fbc:{i}" for i in range(5)]
        operators = ["quay-operator", "quay-operator", "quay-bridge-operator", "quay-bridge-operator", "cso"]
        mock_labels.side_effect = [{'doozer_key': operator} for operator in operators]
        databases = [
            "postgresql-13-container-1-1791973912",
            "postgresql-15-container-1-1791974001",
            "redis-7-container-1-1791974270",
        ]
        proxy = "kube-rbac-proxy-container-v4.22.0-202609291750.p2.gd12e274.assembly.stream.el9"
        related = [
            ["quay-quay-container-3.18.2-1"],
            ["quay-quay-container-3.18.2-1", *databases],
            ["quay-bridge-operator-container-3.18.2-1", proxy],
            ["quay-bridge-operator-container-3.18.2-1"],
            ["cso-container-3.18.2-1", "redis-7-container-1-older"],
        ]
        mock_extract.side_effect = related
        groups, operator_by_fbc, related_by_fbc = {}, {}, {}

        with self.assertLogs('pyartcd.fbc_util', level='INFO') as logs:
            result = asyncio.run(
                validate_fbc_related_images(
                    pullspecs,
                    "quay",
                    operator_groups_out=groups,
                    operator_by_fbc_out=operator_by_fbc,
                    related_nvrs_by_fbc_out=related_by_fbc,
                    assembly="3.18.2",
                )
            )

        self.assertEqual(result, sorted({nvr for nvrs in related for nvr in nvrs}))
        self.assertEqual(groups['quay-operator'], pullspecs[:2])
        self.assertEqual(operator_by_fbc, dict(zip(pullspecs, operators)))
        self.assertEqual(related_by_fbc, {fbc: sorted(nvrs) for fbc, nvrs in zip(pullspecs, related)})
        for nvr in [*databases, proxy]:
            self.assertTrue(any('Skipping' in line and nvr in line for line in logs.output))

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_product_differences_and_malformed_nvrs_still_fail(self, mock_labels, mock_extract):
        mock_labels.return_value = {'doozer_key': 'quay-operator'}
        for differing in ["quay-quay-container-3.18.2-2", "quay-quay-container-3.18.3-1", "malformed", None]:
            with self.subTest(differing=differing):
                mock_extract.side_effect = [
                    ["quay-quay-container-3.18.2-1", "postgresql-15-container-1-1791974001"],
                    [differing] if differing else [],
                ]
                with self.assertRaisesRegex(RuntimeError, "quay-quay-container-3.18.2-1") as ctx:
                    asyncio.run(
                        validate_fbc_related_images(["quay.io/fbc:a", "quay.io/fbc:b"], "quay", assembly="3.18.2")
                    )
                self.assertNotIn("postgresql", str(ctx.exception))
                if differing:
                    self.assertIn(differing, str(ctx.exception))

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_without_assembly_external_differences_still_fail(self, mock_labels, mock_extract):
        mock_labels.return_value = {'doozer_key': 'quay-operator'}
        mock_extract.side_effect = [
            ["quay-quay-container-3.18.2-1"],
            ["quay-quay-container-3.18.2-1", "postgresql-15-container-1-1791974001"],
        ]
        with self.assertRaisesRegex(RuntimeError, "postgresql-15-container"):
            asyncio.run(validate_fbc_related_images(["quay.io/fbc:a", "quay.io/fbc:b"], "quay"))

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_passes_when_related_images_match(self, mock_labels, mock_extract):
        mock_labels.side_effect = [
            {'nvr': 'op-fbc-1.0-1.ocp4.18', 'doozer_key': 'my-operator'},
            {'nvr': 'op-fbc-1.0-1.ocp4.19', 'doozer_key': 'my-operator'},
        ]
        mock_extract.side_effect = [
            ["image-a-1.0-1", "image-b-1.0-1"],
            ["image-a-1.0-1", "image-b-1.0-1"],
        ]
        operator_by_fbc = {}
        related_nvrs_by_fbc = {}

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc:ocp4.18", "quay.io/fbc:ocp4.19"],
                "my-product",
                operator_by_fbc_out=operator_by_fbc,
                related_nvrs_by_fbc_out=related_nvrs_by_fbc,
                assembly="1.0.0",
            )
        )

        self.assertEqual(sorted(result), ["image-a-1.0-1", "image-b-1.0-1"])
        self.assertEqual(
            operator_by_fbc,
            {"quay.io/fbc:ocp4.18": "my-operator", "quay.io/fbc:ocp4.19": "my-operator"},
        )
        self.assertEqual(
            related_nvrs_by_fbc,
            {
                "quay.io/fbc:ocp4.18": ["image-a-1.0-1", "image-b-1.0-1"],
                "quay.io/fbc:ocp4.19": ["image-a-1.0-1", "image-b-1.0-1"],
            },
        )
        mock_extract.assert_any_call(
            "quay.io/fbc:ocp4.18",
            "my-product",
            assembly="1.0.0",
            version_scoped=True,
            bundle_version=None,
        )

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_unscoped_mode_skips_assembly(self, mock_labels, mock_extract):
        """OCP optional path must not force version-scoped extraction."""
        mock_labels.return_value = {'nvr': 'op-fbc-4.22.0-1', 'doozer_key': 'my-operator'}
        mock_extract.return_value = ["image-a-4.22.0-1"]

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc:ocp4.22"],
                "ocp",
                assembly="4.22.0",
                version_scoped=False,
            )
        )

        self.assertEqual(result, ["image-a-4.22.0-1"])
        mock_extract.assert_called_once_with(
            "quay.io/fbc:ocp4.22",
            "ocp",
            assembly=None,
            version_scoped=False,
            bundle_version=None,
        )

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_fails_when_related_images_mismatch(self, mock_labels, mock_extract):
        mock_labels.side_effect = [
            {'nvr': 'op-fbc-1.0-1.ocp4.18', 'doozer_key': 'my-operator'},
            {'nvr': 'op-fbc-1.0-1.ocp4.19', 'doozer_key': 'my-operator'},
        ]
        mock_extract.side_effect = [
            ["image-a-1.0-1", "image-b-1.0-1"],
            ["image-a-1.0-1", "image-c-1.0-1"],
        ]

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(
                validate_fbc_related_images(
                    ["quay.io/fbc:ocp4.18", "quay.io/fbc:ocp4.19"],
                    "my-product",
                )
            )
        self.assertIn("mismatched", str(ctx.exception))

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_single_fbc_skips_comparison(self, mock_labels, mock_extract):
        mock_labels.return_value = {'nvr': 'op-fbc-1.0-1.ocp4.18', 'doozer_key': 'my-operator'}
        mock_extract.return_value = ["image-a-1.0-1"]

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc:ocp4.18"],
                "my-product",
            )
        )

        self.assertEqual(result, ["image-a-1.0-1"])

    @patch('pyartcd.fbc_util.extract_nvrs_from_fbc', new_callable=AsyncMock)
    @patch('pyartcd.fbc_util.extract_fbc_labels', new_callable=AsyncMock)
    def test_different_operators_may_differ(self, mock_labels, mock_extract):
        """FBCs from different operators may have different related images."""
        mock_labels.side_effect = [
            {'nvr': 'op-a-fbc-1.0-1.ocp4.18', 'doozer_key': 'operator-a'},
            {'nvr': 'op-b-fbc-1.0-1.ocp4.18', 'doozer_key': 'operator-b'},
        ]
        mock_extract.side_effect = [
            ["image-a-1.0-1"],
            ["image-b-1.0-1"],
        ]

        result = asyncio.run(
            validate_fbc_related_images(
                ["quay.io/fbc-a:ocp4.18", "quay.io/fbc-b:ocp4.18"],
                "my-product",
            )
        )

        self.assertEqual(sorted(result), ["image-a-1.0-1", "image-b-1.0-1"])


if __name__ == '__main__':
    unittest.main()
