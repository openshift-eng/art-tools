from unittest import TestCase
from unittest.mock import patch
from doozerlib.backend.konflux_client import KonfluxClient


class TestPLRUrl(TestCase):
    @patch("doozerlib.constants.KONFLUX_UI_HOST", "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com")
    @patch("doozerlib.constants.KONFLUX_DEFAULT_NAMESPACE", "ocp-art-tenant")
    def test_pipelinerun_url(self):
        pipeline_run = {
            "metadata": {
                "name": "ose-4-19-ose-ovn-kubernetes-6wv6l",
                "labels": {
                    "appstudio.openshift.io/application": "openshift-4-19",
                },

            },
        }
        actual = KonfluxClient.build_pipeline_url(pipeline_run)
        expected = "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com/ns/ocp-art-tenant/applications/openshift-4-19/pipelineruns/ose-4-19-ose-ovn-kubernetes-6wv6l"

        self.assertEqual(actual, expected)
