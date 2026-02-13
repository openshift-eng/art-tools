from unittest.mock import MagicMock, patch

from artcommonlib.model import Model
from doozerlib import distgit

from .support import TestDistgit


class TestResolveImageFromUpstreamParent(TestDistgit):
    @patch("doozerlib.image.ImageMetadata.canonical_builders_enabled")
    def setUp(self, canonical_mock):
        super().setUp()
        canonical_mock.return_value = False
        self.dg = distgit.ImageDistGitRepo(self.md, autoclone=False)
        self.dg.runtime.group_config = Model()

    @patch("doozerlib.util.oc_image_info_for_arch__caching")
    def test_resolve_parent_1(
        self,
        oc_mock,
    ):
        # Matching MAJOR.MINOR
        # Matching RHEL version
        oc_mock.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "version": "v1.19.3",
                        "release": "202401221732.el9.g00c615b",
                    }
                }
            }
        }
        streams = {
            "golang": {"image": "openshift/golang-builder:v1.19.13-202310161903.el9.g0f9bb4c"},
        }
        self.dg.runtime.streams = streams
        image = self.dg._resolve_image_from_upstream_parent("unused", MagicMock())
        self.assertEqual(image, streams["golang"]["image"])

    @patch("doozerlib.util.oc_image_info_for_arch__caching")
    def test_resolve_parent_2(
        self,
        oc_mock,
    ):
        # Matching MAJOR.MINOR
        # Mismatching RHEL version
        oc_mock.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "version": "v1.19.3",
                        "release": "202401221732.el8.g00c615b",
                    }
                }
            }
        }
        self.dg.runtime.streams = {
            "golang": {"image": "openshift/golang-builder:v1.19.13-202310161903.el9.g0f9bb4c"},
        }
        image = self.dg._resolve_image_from_upstream_parent("unused", MagicMock())
        self.assertEqual(image, None)

    @patch("doozerlib.util.oc_image_info_for_arch__caching")
    def test_resolve_parent_3(
        self,
        oc_mock,
    ):
        # Misatching MAJOR
        # Matching RHEL version
        oc_mock.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "version": "v2.19.3",
                        "release": "202401221732.el9.g00c615b",
                    }
                }
            }
        }
        self.dg.runtime.streams = {
            "golang": {"image": "openshift/golang-builder:v1.19.13-202310161903.el9.g0f9bb4c"},
        }
        image = self.dg._resolve_image_from_upstream_parent("unused", MagicMock())
        self.assertEqual(image, None)

    @patch("doozerlib.util.oc_image_info_for_arch__caching")
    def test_resolve_parent_4(
        self,
        oc_mock,
    ):
        # Misatching MINOR
        # Matching RHEL version
        oc_mock.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "version": "v1.21.3",
                        "release": "202401221732.el9.g00c615b",
                    }
                }
            }
        }
        self.dg.runtime.streams = {
            "golang": {"image": "openshift/golang-builder:v1.19.13-202310161903.el9.g0f9bb4c"},
        }
        image = self.dg._resolve_image_from_upstream_parent("unused", MagicMock())
        self.assertEqual(image, None)

    @patch("doozerlib.util.oc_image_info_for_arch__caching")
    def test_resolve_parent_5(
        self,
        oc_mock,
    ):
        # Misatching MINOR
        # Matching RHEL version
        oc_mock.return_value = {
            "config": {
                "config": {
                    "Labels": {
                        "version": "v1.21.3",
                        "release": "202401221732.el9.g00c615b",
                    }
                }
            }
        }
        streams = {
            "golang-1.20-rhel9": {"image": "openshift/golang-builder:v1.20.13-202310161903.el9.g0f9bb4c"},
            "golang-1.21-rhel9": {"image": "openshift/golang-builder:v1.21.13-202310161903.el9.g0f9bb4c"},
            "golang-1.21-rhel8": {"image": "openshift/golang-builder:v1.21.13-202310161903.el8.g0f9bb4c"},
        }
        self.dg.runtime.streams = streams
        image = self.dg._resolve_image_from_upstream_parent("unused", MagicMock())
        self.assertEqual(image, streams["golang-1.21-rhel9"]["image"])
