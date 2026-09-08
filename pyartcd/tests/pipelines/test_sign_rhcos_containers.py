"""
Tests for pyartcd.pipelines.sign_rhcos_containers.

Covers _get_rhcos_container_digests() for both the legacy single-stream
(.data.stream) payload and the OCP 5.x multi-stream payload that adds
a .data.streams.rhel-10 entry — the latter being the source of the
unsigned s390x kubevirt image tracked in ART-23308.
"""

import json
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock

from pyartcd.pipelines.sign_rhcos_containers import SignRhcosContainersPipeline


def _make_pipeline(rhcos_file: str, arch: str = "s390x") -> SignRhcosContainersPipeline:
    """Return a pipeline instance wired to rhcos_file without KMS env vars."""
    runtime = MagicMock()
    runtime.dry_run = True
    runtime.logger = MagicMock()
    # Bypass the missing-env-var check by exercising only _get_rhcos_container_digests
    pipeline = object.__new__(SignRhcosContainersPipeline)
    pipeline.runtime = runtime
    pipeline.rhcos_file = Path(rhcos_file)
    pipeline.arch = arch
    pipeline.signing_env = "stage"
    pipeline.logger = runtime.logger
    return pipeline


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_RHEL9_STREAM = {
    "stream": "rhcos-4.21",
    "architectures": {
        "s390x": {
            "images": {
                "kubevirt": {
                    "digest-ref": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
                    "@sha256:05c3890349865e894afb7977ec40b3a7ada3b7d92a4dece03c799e558cd126b9"
                },
                "qemu": {
                    "location": "https://mirror.openshift.com/...",
                    "sha256": "abc123",
                    "uncompressed-sha256": "def456",
                },
            }
        },
        "x86_64": {
            "images": {
                "kubevirt": {
                    "digest-ref": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
                    "@sha256:1dc8aa4ed62574ca7dc7d9674f86521c73d24d8013a5e78b69f1bc084fc669e8"
                }
            }
        },
    },
}

_RHEL10_STREAM = {
    "stream": "rhel-10",
    "architectures": {
        "s390x": {
            "images": {
                "kubevirt": {
                    # The digest confirmed unsigned in OCPBUGS-86850 / ART-23308
                    "digest-ref": "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
                    "@sha256:79badc5fad21d22814d077560c10a90abd7d1896d415548281afbe3b7ea75861"
                },
                "qemu": {
                    "location": "https://mirror.openshift.com/...",
                    "sha256": "aabbcc",
                    "uncompressed-sha256": "ddeeff",
                },
            }
        }
    },
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetRhcosContainerDigests(TestCase):
    """Unit tests for SignRhcosContainersPipeline._get_rhcos_container_digests."""

    def _write_stream_file(self, stream_data: dict) -> str:
        """Write stream_data to a temp file and return its path."""
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(stream_data, f)
        f.flush()
        return f.name

    def test_single_stream_returns_s390x_kubevirt_digest(self):
        """Legacy single-stream payload: s390x kubevirt digest-ref is returned."""
        path = self._write_stream_file(_RHEL9_STREAM)
        pipeline = _make_pipeline(path, arch="s390x")

        result = pipeline._get_rhcos_container_digests()

        self.assertIn("kubevirt", result)
        self.assertIn("sha256:05c38903", result["kubevirt"])
        # qemu has no digest-ref so it must not appear
        self.assertNotIn("qemu", result)

    def test_single_stream_ignores_other_arch(self):
        """x86_64 kubevirt digest is not returned when arch=s390x."""
        path = self._write_stream_file(_RHEL9_STREAM)
        pipeline = _make_pipeline(path, arch="s390x")

        result = pipeline._get_rhcos_container_digests()

        for v in result.values():
            self.assertNotIn("1dc8aa4e", v, "x86_64 digest must not appear for s390x pipeline")

    def test_rhel10_stream_returns_unsigned_s390x_kubevirt_digest(self):
        """
        OCP 5.x rhel-10 stream payload: the s390x kubevirt digest-ref
        (sha256:79badc5f) is correctly discovered.

        This is the regression test for ART-23308 / OCPBUGS-86850:
        sign_rhcos_containers must process this stream JSON and return
        the digest so it can be signed.
        """
        path = self._write_stream_file(_RHEL10_STREAM)
        pipeline = _make_pipeline(path, arch="s390x")

        result = pipeline._get_rhcos_container_digests()

        self.assertIn("kubevirt", result)
        self.assertIn(
            "sha256:79badc5fad21d22814d077560c10a90abd7d1896d415548281afbe3b7ea75861",
            result["kubevirt"],
            "RHEL-10 s390x kubevirt digest must be returned for signing",
        )

    def test_no_quay_images_returns_empty(self):
        """Stream with no quay.io digest-ref entries yields an empty dict."""
        stream = {
            "architectures": {
                "s390x": {"images": {"qemu": {"location": "https://mirror.openshift.com/foo", "sha256": "aaa"}}}
            }
        }
        path = self._write_stream_file(stream)
        pipeline = _make_pipeline(path, arch="s390x")

        result = pipeline._get_rhcos_container_digests()

        self.assertEqual(result, {})

    def test_missing_arch_returns_empty(self):
        """Stream that does not contain the requested arch yields an empty dict."""
        path = self._write_stream_file(_RHEL9_STREAM)
        pipeline = _make_pipeline(path, arch="aarch64")

        result = pipeline._get_rhcos_container_digests()

        self.assertEqual(result, {})
