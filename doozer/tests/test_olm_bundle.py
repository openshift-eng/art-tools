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
