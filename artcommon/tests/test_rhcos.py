import json
import unittest
from unittest.mock import patch

from artcommonlib import rhcos
from artcommonlib.model import Model


class TestGetLatestLayeredRhcosBuild(unittest.TestCase):
    @patch("artcommonlib.rhcos.oc_image_info__cached")
    def test_pullspec_repo_taken_from_config(self, oc_info_mock):
        # rhel_build_id_index == rhcos_index_tag so only one image lookup is needed
        container_conf = Model(
            {
                "rhel_build_id_index": "quay.io/openshift-release-dev/ocp-v4.0-art-dev:5.0-9.8-node-image",
                "rhcos_index_tag": "quay.io/openshift-release-dev/ocp-v4.0-art-dev:5.0-9.8-node-image",
            }
        )
        oc_info_mock.return_value = json.dumps(
            {
                "config": {"config": {"Labels": {"org.opencontainers.image.version": "5.0.0-ec"}}},
                "digest": "sha256:abcd",
            }
        )

        build_id, pullspec = rhcos.get_latest_layered_rhcos_build(container_conf, "x86_64")
        self.assertEqual(build_id, "5.0.0-ec")
        # Digest is pinned against the repo the index tag was resolved from, not a reconstructed one
        self.assertEqual(pullspec, "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:abcd")

    @patch("artcommonlib.rhcos.oc_image_info__cached")
    def test_pullspec_repo_from_rhcos_index_tag_when_indexes_differ(self, oc_info_mock):
        # rhel_build_id_index != rhcos_index_tag: digest (and repo) come from rhcos_index_tag
        container_conf = Model(
            {
                "rhel_build_id_index": "quay.io/openshift-release-dev/ocp-v4.0-art-dev:5.0-9.8-node-image",
                "rhcos_index_tag": "quay.io/openshift-release-dev/ocp-v4.0-art-dev:5.0-9.8-node-image-extensions",
            }
        )
        oc_info_mock.side_effect = [
            json.dumps(
                {
                    "config": {"config": {"Labels": {"org.opencontainers.image.version": "5.0.0-ec"}}},
                    "digest": "sha256:rhel",
                }
            ),
            json.dumps({"digest": "sha256:ext"}),
        ]

        build_id, pullspec = rhcos.get_latest_layered_rhcos_build(container_conf, "x86_64")
        self.assertEqual(build_id, "5.0.0-ec")
        self.assertEqual(pullspec, "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:ext")


if __name__ == "__main__":
    unittest.main()
