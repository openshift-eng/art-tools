"""Unit tests for elliott go:report floating-tag support.

Scenarios covered:
  (a) is_floating_golang_builder_tag: True for floating, False for all three NVR formats
  (b) go_version_from_floating_tag: correct major.minor + RHEL extraction
  (c) go_version_from_floating_tag_exact: mocked oc image info returns correct NVR
  (d) go_version_from_nvr_string: all three legacy NVR tag formats still parse
  (e) golang_report_for_version with exact=True and a floating-tag stream (SC-5 / find-bugs path)
"""

import unittest.mock
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from elliottlib.cli.get_golang_report_cli import (
    go_version_from_floating_tag,
    go_version_from_floating_tag_exact,
    go_version_from_nvr_string,
    golang_report_for_version,
    is_floating_golang_builder_tag,
)

# ---------------------------------------------------------------------------
# (a) floating-tag detection
# ---------------------------------------------------------------------------


class TestIsFloatingGolangBuilderTag(TestCase):
    # --- rejection cases added by regex hardening ---

    def test_embedded_v_not_after_separator_rejected(self):
        """foov1.22-rhel9 must not match — 'v' is embedded in a word."""
        self.assertFalse(is_floating_golang_builder_tag("foov1.22-rhel9"))

    def test_unicode_digits_rejected(self):
        """Unicode digit characters must not satisfy the [0-9] constraint."""
        # U+0661 ARABIC-INDIC DIGIT ONE, etc.
        self.assertFalse(is_floating_golang_builder_tag("v\u0661.\u0662\u0662-rhel\u0669"))

    def test_trailing_garbage_rejected(self):
        """Extra characters after the RHEL number must not match."""
        self.assertFalse(is_floating_golang_builder_tag("golang-builder-v1.22-rhel9extra"))

    def test_known_prefix_accepted(self):
        """golang-builder-vX.Y-rhelN is the normalised form produced by tag replacement."""
        self.assertTrue(is_floating_golang_builder_tag("golang-builder-v1.22-rhel9"))

    def test_junk_prefix_rejected(self):
        """Arbitrary '-'-terminated prefix must not match the new explicit-prefix regex."""
        self.assertFalse(is_floating_golang_builder_tag("junk-v1.22-rhel9"))

    def test_floating_rhel9(self):
        self.assertTrue(is_floating_golang_builder_tag('openshift-golang-builder-container-v1.22-rhel9'))

    def test_floating_rhel8(self):
        self.assertTrue(is_floating_golang_builder_tag('openshift-golang-builder-container-v1.23-rhel8'))

    def test_nvr_format_legacy(self):
        # openshift/golang-builder:v1.23.9-202506111225.g6c23478.el9 after prefix replacement
        self.assertFalse(
            is_floating_golang_builder_tag('openshift-golang-builder-container-v1.23.9-202506111225.g6c23478.el9')
        )

    def test_nvr_format_registry_nvr_tag(self):
        # registry.redhat.io/openshift/golang-builder:openshift-golang-builder-container-v1.25.8-...
        self.assertFalse(
            is_floating_golang_builder_tag(
                'openshift-golang-builder-container-v1.25.8-202608271548.p2.gedd1cdd.assembly.stream.el9'
            )
        )

    def test_nvr_format_quay_konflux(self):
        # quay.io/...art-images:golang-builder-v1.23.10-... after tag replace
        self.assertFalse(
            is_floating_golang_builder_tag(
                'openshift-golang-builder-container-v1.23.10-202608241902.p2.gedd1cdd.assembly.stream.el9'
            )
        )


# ---------------------------------------------------------------------------
# (b) non-exact version extraction from floating tag
# ---------------------------------------------------------------------------


class TestGoVersionFromFloatingTag(TestCase):
    def test_rhel9_with_rhel(self):
        version = go_version_from_floating_tag('openshift-golang-builder-container-v1.22-rhel9', ignore_rhel=False)
        self.assertEqual(version, '1.22.el9')

    def test_rhel8_with_rhel(self):
        version = go_version_from_floating_tag('openshift-golang-builder-container-v1.23-rhel8', ignore_rhel=False)
        self.assertEqual(version, '1.23.el8')

    def test_ignore_rhel(self):
        version = go_version_from_floating_tag('openshift-golang-builder-container-v1.22-rhel9', ignore_rhel=True)
        self.assertEqual(version, '1.22')

    def test_non_floating_raises(self):
        with self.assertRaises(ValueError):
            go_version_from_floating_tag(
                'openshift-golang-builder-container-v1.22.5-202506111225.el9', ignore_rhel=False
            )


# ---------------------------------------------------------------------------
# (c) exact-mode resolution via mocked oc image info
# ---------------------------------------------------------------------------


class TestGoVersionFromFloatingTagExact(TestCase):
    def test_resolves_via_golang_nvr_label(self):
        """Happy path: io.openshift.build.golang-nvr label present — no DB lookup needed."""
        fake_image_data = {
            'config': {
                'config': {
                    'Labels': {
                        'io.openshift.build.golang-nvr': 'golang-1.22.12-2.el9',
                        'com.redhat.component': 'openshift-golang-builder-container',
                        'version': 'v1.22.12',
                        'release': '202608131106.p2.g7d3050a.assembly.stream.el9',
                    }
                }
            }
        }
        with (
            patch(
                'elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch',
                return_value=fake_image_data,
            ) as mock_oi,
            patch(
                'elliottlib.cli.get_golang_report_cli.get_golang_container_nvrs',
            ) as mock_nvrs,
        ):
            result = go_version_from_floating_tag_exact('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')

        self.assertEqual(result, 'golang-1.22.12-2.el9')
        mock_oi.assert_called_once_with('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')
        mock_nvrs.assert_not_called()

    def test_falls_back_to_db_when_golang_nvr_label_absent(self):
        """Fallback path: no io.openshift.build.golang-nvr label — reuse extract_nvr_from_pullspec."""
        fake_image_data = {'config': {'config': {'Labels': {}}}}
        with (
            patch(
                'elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch',
                return_value=fake_image_data,
            ) as mock_oi,
            patch(
                'elliottlib.cli.get_golang_report_cli.extract_nvr_from_pullspec',
                new_callable=AsyncMock,
                return_value=(
                    'openshift-golang-builder-container',
                    'v1.22.12',
                    '202608131106.p2.g7d3050a.assembly.stream.el9',
                ),
            ) as mock_extract,
            patch(
                'elliottlib.cli.get_golang_report_cli.get_golang_container_nvrs',
                return_value={
                    'golang-1.22.12-1.el9': {
                        (
                            'openshift-golang-builder-container',
                            'v1.22.12',
                            '202608131106.p2.g7d3050a.assembly.stream.el9',
                        )
                    }
                },
            ) as mock_nvrs,
        ):
            result = go_version_from_floating_tag_exact('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')

        self.assertEqual(result, 'golang-1.22.12-1.el9')
        mock_oi.assert_called_once_with('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')
        mock_extract.assert_called_once_with('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')
        mock_nvrs.assert_called_once_with(
            [('openshift-golang-builder-container', 'v1.22.12', '202608131106.p2.g7d3050a.assembly.stream.el9')],
            unittest.mock.ANY,
            exact=True,
        )

    def test_raises_on_missing_labels(self):
        """extract_nvr_from_pullspec IOError is re-raised as ValueError."""
        fake_image_data = {'config': {'config': {'Labels': {}}}}
        with (
            patch('elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch', return_value=fake_image_data),
            patch(
                'elliottlib.cli.get_golang_report_cli.extract_nvr_from_pullspec',
                new_callable=AsyncMock,
                side_effect=IOError("Missing NVR labels"),
            ),
        ):
            with self.assertRaises(ValueError):
                go_version_from_floating_tag_exact('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')

    def test_raises_when_labels_is_null(self):
        """Labels: null must not cause AttributeError on the fast-path check."""
        fake_image_data = {'config': {'config': {'Labels': None}}}
        with (
            patch('elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch', return_value=fake_image_data),
            patch(
                'elliottlib.cli.get_golang_report_cli.extract_nvr_from_pullspec',
                new_callable=AsyncMock,
                side_effect=IOError("Missing NVR labels"),
            ),
        ):
            with self.assertRaises(ValueError):
                go_version_from_floating_tag_exact('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')

    def test_raises_on_multiple_nvr_map_entries(self):
        fake_image_data = {'config': {'config': {'Labels': {}}}}
        with (
            patch('elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch', return_value=fake_image_data),
            patch(
                'elliottlib.cli.get_golang_report_cli.extract_nvr_from_pullspec',
                new_callable=AsyncMock,
                return_value=('openshift-golang-builder-container', 'v1.22.12', '202608131106.el9'),
            ),
            patch(
                'elliottlib.cli.get_golang_report_cli.get_golang_container_nvrs',
                return_value={'golang-1.22.12-1.el9': set(), 'golang-1.22.11-1.el9': set()},
            ),
        ):
            with self.assertRaises(ValueError):
                go_version_from_floating_tag_exact('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')

    def test_raises_on_empty_nvr_map(self):
        fake_image_data = {'config': {'config': {'Labels': {}}}}
        with (
            patch('elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch', return_value=fake_image_data),
            patch(
                'elliottlib.cli.get_golang_report_cli.extract_nvr_from_pullspec',
                new_callable=AsyncMock,
                return_value=('openshift-golang-builder-container', 'v1.22.12', '202608131106.el9'),
            ),
            patch('elliottlib.cli.get_golang_report_cli.get_golang_container_nvrs', return_value={}),
        ):
            with self.assertRaises(ValueError):
                go_version_from_floating_tag_exact('registry.redhat.io/openshift/golang-builder:v1.22-rhel9')


# ---------------------------------------------------------------------------
# (d) legacy NVR formats still parse without error
# ---------------------------------------------------------------------------


class TestGoVersionFromNvrString(TestCase):
    def test_legacy_openshift_builder_format(self):
        # openshift/golang-builder:v1.23.9-202506111225.g6c23478.el9 → prefix replaced
        nvr = 'openshift-golang-builder-container-v1.23.9-202506111225.g6c23478.el9'
        result = go_version_from_nvr_string(nvr, ignore_rhel=False)
        self.assertEqual(result, '1.23.9.el9')

    def test_legacy_format_ignore_rhel(self):
        nvr = 'openshift-golang-builder-container-v1.23.9-202506111225.g6c23478.el9'
        result = go_version_from_nvr_string(nvr, ignore_rhel=True)
        self.assertEqual(result, '1.23.9')

    def test_registry_nvr_tag_format(self):
        # registry.redhat.io tag where the tag is already in NVR name format
        nvr = 'openshift-golang-builder-container-v1.25.8-202608271548.p2.gedd1cdd.assembly.stream.el9'
        result = go_version_from_nvr_string(nvr, ignore_rhel=False)
        self.assertEqual(result, '1.25.8.el9')

    def test_quay_konflux_format(self):
        # quay.io tag after golang-builder → openshift-golang-builder-container replacement
        nvr = 'openshift-golang-builder-container-v1.23.10-202608241902.p2.gedd1cdd.assembly.stream.el9'
        result = go_version_from_nvr_string(nvr, ignore_rhel=False)
        self.assertEqual(result, '1.23.10.el9')


# ---------------------------------------------------------------------------
# (e) golang_report_for_version with exact=True and floating-tag stream
#     Mirrors the find-bugs:golang call site (exact=True) — SC-5 guard
# ---------------------------------------------------------------------------


class TestGolangReportForVersionFloatingExact(TestCase):
    def _make_runtime(self, stream_image: str):
        runtime = MagicMock()
        # image_metas must be non-empty to pass the initialization guard.
        # Give the image a builder reference to 'rhel-9-golang' so the stream count becomes 1
        # and the resolved NVR actually appears in the output (needed to verify SC-5).
        mock_image = MagicMock()
        mock_image.enabled = True
        mock_image.config_filename = 'test-image.yml'
        mock_image.config = {'from': {'builder': [{'stream': 'rhel-9-golang'}]}}
        runtime.image_metas.return_value = [mock_image]
        # rpm_metas must be non-empty; use an rpm not in the golang_rpms set.
        mock_rpm = MagicMock()
        mock_rpm.config_filename = 'unrelated-rpm.yml'
        runtime.rpm_metas.return_value = [mock_rpm]
        runtime.get_streams_config.return_value = {
            'rhel-9-golang': {
                'image': stream_image,
                'aliases': [],
            }
        }
        # shared_koji_client_session as context manager returning a session with no results
        koji_ctx = MagicMock()
        koji_ctx.__enter__ = MagicMock(return_value=MagicMock(getLatestBuilds=MagicMock(return_value=[])))
        koji_ctx.__exit__ = MagicMock(return_value=False)
        runtime.shared_koji_client_session.return_value = koji_ctx
        return runtime

    def test_exact_floating_tag_stream(self):
        """golang_report_for_version(exact=True) works when the stream uses a floating tag."""
        stream_image = 'registry.redhat.io/openshift/golang-builder:golang-builder-v1.22-rhel9'
        runtime = self._make_runtime(stream_image)

        fake_image_data = {
            'config': {
                'config': {
                    'Labels': {
                        'io.openshift.build.golang-nvr': 'golang-1.22.12-1.el9',
                        'com.redhat.component': 'openshift-golang-builder-container',
                        'version': 'v1.22.12',
                        'release': '202608131106.p2.g7d3050a.assembly.stream.el9',
                    }
                }
            }
        }
        with (
            patch(
                'elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch',
                return_value=fake_image_data,
            ),
            patch(
                'elliottlib.cli.get_golang_report_cli.get_golang_container_nvrs',
                return_value={
                    'golang-1.22.12-1.el9': {
                        (
                            'openshift-golang-builder-container',
                            'v1.22.12',
                            '202608131106.p2.g7d3050a.assembly.stream.el9',
                        )
                    }
                },
            ),
        ):
            result = golang_report_for_version(runtime, '4.18', ignore_rhel=False, exact=True)

        # The image references stream 'rhel-9-golang', so building_image_count=1 and the
        # resolved NVR must appear in the output — verifying SC-5 (find-bugs:golang inherits fix).
        self.assertEqual(result, [{'go_version': 'golang-1.22.12-1.el9', 'building_image_count': 1}])


# ---------------------------------------------------------------------------
# (f) golang_report_for_version with exact=False and floating-tag stream
#     Verifies go_version_from_floating_tag dispatch (not oc image info)
# ---------------------------------------------------------------------------


class TestGolangReportForVersionFloatingNonExact(TestCase):
    def _make_runtime(self, stream_image: str):
        runtime = MagicMock()
        mock_image = MagicMock()
        mock_image.enabled = True
        mock_image.config_filename = 'test-image.yml'
        mock_image.config = {'from': {'builder': [{'stream': 'rhel-9-golang'}]}}
        runtime.image_metas.return_value = [mock_image]
        mock_rpm = MagicMock()
        mock_rpm.config_filename = 'unrelated-rpm.yml'
        runtime.rpm_metas.return_value = [mock_rpm]
        runtime.get_streams_config.return_value = {
            'rhel-9-golang': {
                'image': stream_image,
                'aliases': [],
            }
        }
        koji_ctx = MagicMock()
        koji_ctx.__enter__ = MagicMock(return_value=MagicMock(getLatestBuilds=MagicMock(return_value=[])))
        koji_ctx.__exit__ = MagicMock(return_value=False)
        runtime.shared_koji_client_session.return_value = koji_ctx
        return runtime

    def test_nonexact_floating_tag_does_not_call_oc_image_info(self):
        """exact=False with a floating-tag stream must NOT call oc_image_info_for_arch."""
        # quay.io tag whose tag portion normalises to golang-builder-v1.22-rhel9
        stream_image = 'quay.io/redhat-user-workloads/ocp-art-tenant/art-images:golang-builder-v1.22-rhel9'
        runtime = self._make_runtime(stream_image)

        with (
            patch(
                'elliottlib.cli.get_golang_report_cli.oc_image_info_for_arch',
            ) as mock_oi,
        ):
            result = golang_report_for_version(runtime, '4.18', ignore_rhel=False, exact=False)

        # oc_image_info_for_arch must NOT be called in non-exact mode
        mock_oi.assert_not_called()
        # go_version_from_floating_tag('openshift-golang-builder-container-v1.22-rhel9', ignore_rhel=False) → '1.22.el9'
        self.assertEqual(result, [{'go_version': '1.22.el9', 'building_image_count': 1}])
