import unittest
from unittest.mock import MagicMock

from doozerlib.olm.bundle import OLMBundle
from flexmock import flexmock


class TestOLMBundle(unittest.TestCase):
    def test_get_bundle_image_name_no_ose_prefix(self):
        name = "foo-operator"
        olm = flexmock(
            OLMBundle(
                runtime=None,
                operator_nvr_or_dict={
                    "nvr": f"{name}-1.0.0-1",
                    "source": f"https://pkgs.devel.redhat.com/git/containers/{name}"
                    "#d37b219bb1227aed06e32a995f74595f845bb981",
                },
                brew_session=MagicMock(),
            )
        )
        self.assertEqual(olm.bundle_image_name, "openshift/ose-foo-operator-bundle")

    def test_get_bundle_image_name_with_ose_prefix(self):
        name = "ose-foo-operator"
        olm = flexmock(
            OLMBundle(
                runtime=None,
                operator_nvr_or_dict={
                    "nvr": f"{name}-1.0.0-1",
                    "source": f"https://pkgs.devel.redhat.com/git/containers/{name}"
                    "#d37b219bb1227aed06e32a995f74595f845bb981",
                },
                brew_session=MagicMock(),
            )
        )
        self.assertEqual(olm.bundle_image_name, "openshift/ose-foo-operator-bundle")
