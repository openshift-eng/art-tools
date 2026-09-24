import unittest
from unittest.mock import MagicMock

from artcommonlib.model import Missing, Model
from doozerlib.olm.bundle import OLMBundle
from flexmock import flexmock


class TestOLMBundle(unittest.TestCase):
    def test_get_bundle_image_name_no_ose_prefix(self):
        name = 'foo-operator'
        olm = flexmock(
            OLMBundle(
                runtime=None,
                operator_nvr_or_dict={
                    'nvr': f'{name}-1.0.0-1',
                    'source': f'https://pkgs.devel.redhat.com/git/containers/{name}'
                    '#d37b219bb1227aed06e32a995f74595f845bb981',
                },
                brew_session=MagicMock(),
            )
        )
        self.assertEqual(olm.bundle_image_name, 'openshift/ose-foo-operator-bundle')

    def test_get_bundle_image_name_with_ose_prefix(self):
        name = 'ose-foo-operator'
        olm = flexmock(
            OLMBundle(
                runtime=None,
                operator_nvr_or_dict={
                    'nvr': f'{name}-1.0.0-1',
                    'source': f'https://pkgs.devel.redhat.com/git/containers/{name}'
                    '#d37b219bb1227aed06e32a995f74595f845bb981',
                },
                brew_session=MagicMock(),
            )
        )
        self.assertEqual(olm.bundle_image_name, 'openshift/ose-foo-operator-bundle')

    def _make_olm_bundle_with_runtime(self, name, config):
        runtime = MagicMock()
        image_meta = MagicMock()
        image_meta.config = config
        runtime.image_map = {name: image_meta}
        return OLMBundle(
            runtime=runtime,
            operator_nvr_or_dict={
                'nvr': f'{name}-1.0.0-1',
                'source': f'https://pkgs.devel.redhat.com/git/containers/{name}'
                '#d37b219bb1227aed06e32a995f74595f845bb981',
            },
            brew_session=MagicMock(),
        )

    def test_bundle_name_no_override(self):
        name = 'foo-operator'
        olm = self._make_olm_bundle_with_runtime(name, Model({}))
        self.assertEqual(olm.bundle_name, 'foo-operator-bundle')

    def test_bundle_name_with_override(self):
        name = 'foo-operator'
        olm = self._make_olm_bundle_with_runtime(name, Model({'bundle_name_override': 'custom-bundle-name'}))
        self.assertEqual(olm.bundle_name, 'custom-bundle-name')

    def test_bundle_name_override_missing_falls_back(self):
        name = 'foo-operator'
        olm = self._make_olm_bundle_with_runtime(name, Model({'bundle_name_override': Missing}))
        self.assertEqual(olm.bundle_name, 'foo-operator-bundle')

    def test_operator_framework_tags_default_stable_channel(self):
        """Test that operator_framework_tags uses 'stable' when operator_stable_channel_name is not set."""
        name = 'foo-operator'
        olm = self._make_olm_bundle_with_runtime(name, Model({}))
        olm.channel = 'test-channel'
        olm.package = 'test-package'

        # Use Model for group_config so .get() returns defaults properly
        olm.runtime.group_config = Model({'operator_channel_stable': 'default'})
        tags = olm.operator_framework_tags
        self.assertEqual(tags['operators.operatorframework.io.bundle.channel.default.v1'], 'stable')
        self.assertIn('stable', tags['operators.operatorframework.io.bundle.channels.v1'])

    def test_operator_framework_tags_custom_stable_channel(self):
        """Test that operator_stable_channel_name in group config overrides the default 'stable' channel name."""
        name = 'foo-operator'
        olm = self._make_olm_bundle_with_runtime(name, Model({}))
        olm.channel = 'test-channel'
        olm.package = 'test-package'

        custom_stable = 'stable-5.0'

        # Test with operator_channel_stable='default': custom stable becomes the default channel
        olm.runtime.group_config = Model(
            {
                'operator_channel_stable': 'default',
                'operator_stable_channel_name': custom_stable,
            }
        )
        tags = olm.operator_framework_tags
        self.assertEqual(tags['operators.operatorframework.io.bundle.channel.default.v1'], custom_stable)
        self.assertIn(custom_stable, tags['operators.operatorframework.io.bundle.channels.v1'])

        # Test with operator_channel_stable='extra': custom stable is added but not default
        olm.runtime.group_config = Model(
            {
                'operator_channel_stable': 'extra',
                'operator_stable_channel_name': custom_stable,
            }
        )
        tags = olm.operator_framework_tags
        self.assertEqual(tags['operators.operatorframework.io.bundle.channel.default.v1'], 'test-channel')
        self.assertIn(custom_stable, tags['operators.operatorframework.io.bundle.channels.v1'])

        # Test with operator_channel_stable not set: custom stable is NOT used at all
        olm.runtime.group_config = Model(
            {
                'operator_stable_channel_name': custom_stable,
            }
        )
        tags = olm.operator_framework_tags
        self.assertEqual(tags['operators.operatorframework.io.bundle.channel.default.v1'], 'test-channel')
        self.assertNotIn(custom_stable, tags['operators.operatorframework.io.bundle.channels.v1'])
