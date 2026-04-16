import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.konflux.konflux_build_record import Engine
from artcommonlib.olm.operator_image_refs import (
    replace_olm_manifest_image_references,
    validate_olm_manifest_image_references,
)


class TestOlmOperatorImageRefs(unittest.IsolatedAsyncioTestCase):
    @patch("artcommonlib.olm.operator_image_refs.oc_image_info_for_arch_async", new_callable=AsyncMock)
    async def test_replace_resolves_tag_reference(self, mock_oc):
        mock_oc.return_value = {
            'config': {
                'config': {
                    'Labels': {
                        'com.redhat.component': 'test-brew-component',
                        'version': '1.0',
                        'release': '1',
                    },
                },
            },
            'listDigest': 'sha256:1234567890abcdef',
            'contentDigest': 'sha256:abcdef1234567890',
        }
        group_config = {'csv_namespace': 'namespace'}
        metadata = MagicMock()
        metadata.runtime.group = "openshift-4.19"
        metadata.runtime.data_dir = "/tmp/nonexistent-art-data"

        content = """
        image: registry.example.com/namespace/image:tag
        """
        new_content, found = await replace_olm_manifest_image_references(
            "registry.example.com",
            content,
            Engine.KONFLUX,
            metadata,
            group_config,
            "quay.io/example/art-images",
        )
        self.assertIn("registry.redhat.io/openshift4/image@sha256:1234567890abcdef", new_content)
        self.assertIn("image", found)

    @patch("artcommonlib.olm.operator_image_refs.replace_olm_manifest_image_references", new_callable=AsyncMock)
    async def test_validate_delegates_to_replace(self, mock_replace):
        mock_replace.side_effect = ValueError("simulated oc failure")
        ok, err = await validate_olm_manifest_image_references(
            "r.io",
            "x",
            Engine.KONFLUX,
            MagicMock(),
            MagicMock(),
            "quay.io/x/art-images",
        )
        self.assertFalse(ok)
        self.assertIn("simulated oc failure", err or "")


if __name__ == "__main__":
    unittest.main()
