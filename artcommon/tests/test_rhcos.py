import json
import unittest
from unittest.mock import patch

from artcommonlib import rhcos
from artcommonlib.model import Model


class TestGetLatestLayeredRhcosBuild(unittest.TestCase):
    def _container_conf(self):
        # rhel_build_id_index == rhcos_index_tag so only one image lookup is needed
        return Model(
            {
                "rhel_build_id_index": "registry.example.com/rhcos-index:latest",
                "rhcos_index_tag": "registry.example.com/rhcos-index:latest",
            }
        )

    @patch("artcommonlib.rhcos.oc_image_info__cached")
    def test_repo_tracks_major_version(self, oc_info_mock):
        oc_info_mock.return_value = json.dumps(
            {
                "config": {"config": {"Labels": {"org.opencontainers.image.version": "5.0.0-ec"}}},
                "digest": "sha256:abcd",
            }
        )

        build_id, pullspec = rhcos.get_latest_layered_rhcos_build(self._container_conf(), "x86_64", major=5)
        self.assertEqual(build_id, "5.0.0-ec")
        self.assertEqual(pullspec, "quay.io/openshift-release-dev/ocp-v5.0-art-dev@sha256:abcd")

    @patch("artcommonlib.rhcos.oc_image_info__cached")
    def test_repo_for_major_4(self, oc_info_mock):
        oc_info_mock.return_value = json.dumps(
            {
                "config": {"config": {"Labels": {"org.opencontainers.image.version": "4.19.0"}}},
                "digest": "sha256:1234",
            }
        )

        _, pullspec = rhcos.get_latest_layered_rhcos_build(self._container_conf(), "x86_64", major=4)
        self.assertEqual(pullspec, "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:1234")


if __name__ == "__main__":
    unittest.main()
