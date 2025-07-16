from unittest import TestCase
from unittest.mock import patch

from artcommonlib.model import Model
from doozerlib.backend.konflux_client import KonfluxClient


class TestResourceUrl(TestCase):
    @patch("doozerlib.constants.KONFLUX_UI_HOST", "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com")
    def test_resource_url(self):
        pipeline_run = Model(
            {
                "kind": "PipelineRun",
                "metadata": {
                    "name": "ose-4-19-ose-ovn-kubernetes-6wv6l",
                    "namespace": "foobar-tenant",
                    "labels": {
                        "appstudio.openshift.io/application": "openshift-4-19",
                    },
                },
            }
        )
        actual = KonfluxClient.resource_url(pipeline_run)
        expected = "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com/ns/foobar-tenant/applications/openshift-4-19/pipelineruns/ose-4-19-ose-ovn-kubernetes-6wv6l"

        self.assertEqual(actual, expected)
