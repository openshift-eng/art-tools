import json
import logging
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

import jinja2
from artcommonlib.konflux.konflux_build_record import KonfluxECStatus
from doozerlib.backend.konflux_client import (
    API_VERSION,
    API_VERSION_V1BETA2,
    KIND_APPLICATION,
    KIND_INTEGRATION_TEST_SCENARIO,
    ECVerificationResult,
    GitHubApiUrlInfo,
    ImageBuildParams,
    KonfluxClient,
    parse_github_api_url,
)

# Shared minimal PLR template used by all _new_pipelinerun_for_image_build tests
_MINIMAL_PLR_TEMPLATE = """
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: test-plr
  namespace: test-ns
  annotations:
    build.appstudio.openshift.io/repo: "{{ source_url }}?rev={{ revision }}"
    pipelinesascode.tekton.dev/on-cel-expression: "true"
  labels:
    appstudio.openshift.io/application: test-app
    appstudio.openshift.io/component: test-component
spec:
  params:
  - name: output-image
    value: ""
  - name: skip-checks
    value: "false"
  - name: build-source-image
    value: "false"
  - name: build-platforms
    value: []
  - name: build-args
    value: []
  pipelineSpec:
    tasks:
    - name: build-images
      params:
      - name: IMAGE
        value: ""
    - name: apply-tags
      params:
      - name: ADDITIONAL_TAGS
        value: []
    - name: clone-repository
      params: []
  taskRunTemplate:
    serviceAccountName: default
  workspaces:
  - name: git-auth
    secret:
      secretName: "{{ git_auth_secret }}"
"""

# Common required kwargs for _new_pipelinerun_for_image_build calls
_COMMON_KWARGS = dict(
    generate_name="test-",
    namespace="test-ns",
    application_name="test-app",
    component_name="test-component",
    git_url="https://github.com/openshift/test.git",
    commit_sha="abc123",
    target_branch="main",
    output_image="quay.io/test/image:tag",
    build_platforms=["linux/amd64"],
)


def _make_mock_client(mock_get_template):
    """Create a KonfluxClient instance with a mocked template."""
    import jinja2

    mock_get_template.return_value = jinja2.Template(_MINIMAL_PLR_TEMPLATE, autoescape=True)
    client = KonfluxClient.__new__(KonfluxClient)
    client._logger = MagicMock()
    return client


class TestResourceUrl(TestCase):
    @patch("doozerlib.constants.KONFLUX_UI_HOST", "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com")
    def test_resource_url(self):
        pipeline_run_dict = {
            "kind": "PipelineRun",
            "metadata": {
                "name": "ose-4-19-ose-ovn-kubernetes-6wv6l",
                "namespace": "foobar-tenant",
                "labels": {
                    "appstudio.openshift.io/application": "openshift-4-19",
                },
            },
        }
        actual = KonfluxClient.resource_url(pipeline_run_dict)
        expected = "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com/ns/foobar-tenant/applications/openshift-4-19/pipelineruns/ose-4-19-ose-ovn-kubernetes-6wv6l"

        self.assertEqual(actual, expected)


class TestParseGitHubApiUrl(TestCase):
    """Tests for parse_github_api_url function."""

    def test_parse_standard_url(self):
        """Test parsing a standard GitHub API contents URL."""
        url = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-konflux-template-push.yaml?ref=main"
        result = parse_github_api_url(url)

        self.assertEqual(result.owner, "openshift-priv")
        self.assertEqual(result.repo, "art-konflux-template")
        self.assertEqual(result.file_path, ".tekton/art-konflux-template-push.yaml")
        self.assertEqual(result.ref, "main")

    def test_parse_url_with_different_ref(self):
        """Test parsing URL with a different ref (branch/tag)."""
        url = "https://api.github.com/repos/my-org/my-repo/contents/path/to/file.yaml?ref=v1.0.0"
        result = parse_github_api_url(url)

        self.assertEqual(result.owner, "my-org")
        self.assertEqual(result.repo, "my-repo")
        self.assertEqual(result.file_path, "path/to/file.yaml")
        self.assertEqual(result.ref, "v1.0.0")

    def test_parse_url_without_ref(self):
        """Test parsing URL without ref parameter (should default to main)."""
        url = "https://api.github.com/repos/owner/repo/contents/file.yaml"
        result = parse_github_api_url(url)

        self.assertEqual(result.owner, "owner")
        self.assertEqual(result.repo, "repo")
        self.assertEqual(result.file_path, "file.yaml")
        self.assertEqual(result.ref, "main")

    def test_parse_url_with_nested_path(self):
        """Test parsing URL with deeply nested file path."""
        url = "https://api.github.com/repos/org/repo/contents/a/b/c/d/file.yaml?ref=develop"
        result = parse_github_api_url(url)

        self.assertEqual(result.file_path, "a/b/c/d/file.yaml")

    def test_invalid_host(self):
        """Test that non-GitHub API URLs raise ValueError."""
        url = "https://github.com/owner/repo/blob/main/file.yaml"
        with self.assertRaises(ValueError) as context:
            parse_github_api_url(url)
        self.assertIn("api.github.com", str(context.exception))

    def test_invalid_path_format(self):
        """Test that URLs with wrong path format raise ValueError."""
        url = "https://api.github.com/users/octocat"
        with self.assertRaises(ValueError) as context:
            parse_github_api_url(url)
        self.assertIn("does not match expected format", str(context.exception))

    def test_result_is_named_tuple(self):
        """Test that result is a GitHubApiUrlInfo named tuple."""
        url = "https://api.github.com/repos/owner/repo/contents/file.yaml?ref=main"
        result = parse_github_api_url(url)

        self.assertIsInstance(result, GitHubApiUrlInfo)
        self.assertEqual(result[0], "owner")
        self.assertEqual(result[1], "repo")
        self.assertEqual(result[2], "file.yaml")
        self.assertEqual(result[3], "main")


class TestNewIntegrationTestScenario(TestCase):
    def test_manifest_structure(self):
        manifest = KonfluxClient._new_integration_test_scenario(
            name="openshift-4-21-ec-registry-ocp-art-stage",
            application_name="openshift-4-21",
            application_uid="test-uid-1234",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
        )

        self.assertEqual(manifest["apiVersion"], API_VERSION_V1BETA2)
        self.assertEqual(manifest["kind"], KIND_INTEGRATION_TEST_SCENARIO)
        self.assertEqual(manifest["metadata"]["name"], "openshift-4-21-ec-registry-ocp-art-stage")
        self.assertEqual(manifest["metadata"]["labels"]["test.appstudio.openshift.io/optional"], "true")
        self.assertEqual(manifest["metadata"]["annotations"]["test.appstudio.openshift.io/kind"], "enterprise-contract")

        # ownerReferences
        owner_refs = manifest["metadata"]["ownerReferences"]
        self.assertEqual(len(owner_refs), 1)
        self.assertEqual(owner_refs[0]["apiVersion"], API_VERSION)
        self.assertEqual(owner_refs[0]["kind"], KIND_APPLICATION)
        self.assertEqual(owner_refs[0]["name"], "openshift-4-21")
        self.assertEqual(owner_refs[0]["uid"], "test-uid-1234")

        # spec - contexts must include disabled to prevent automatic EC runs
        self.assertEqual(len(manifest["spec"]["contexts"]), 1)
        self.assertEqual(manifest["spec"]["contexts"][0]["name"], "disabled")

        self.assertEqual(manifest["spec"]["application"], "openshift-4-21")
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        self.assertEqual(params["POLICY_CONFIGURATION"], "rhtap-releng-tenant/registry-ocp-art-stage")
        self.assertEqual(params["SINGLE_COMPONENT"], "true")

        # resolverRef
        resolver_ref = manifest["spec"]["resolverRef"]
        self.assertEqual(resolver_ref["resolver"], "git")
        self.assertEqual(resolver_ref["resourceKind"], "pipeline")
        resolver_params = {p["name"]: p["value"] for p in resolver_ref["params"]}
        self.assertEqual(resolver_params["url"], "https://github.com/konflux-ci/build-definitions")
        self.assertEqual(resolver_params["revision"], "main")
        self.assertEqual(resolver_params["pathInRepo"], "pipelines/enterprise-contract.yaml")


class TestNewIntegrationTestScenarioPreGA(TestCase):
    """Test that preGA (PREVIEW) assemblies use the ec-stage policy."""

    def test_prega_policy_manifest(self):
        manifest = KonfluxClient._new_integration_test_scenario(
            name="openshift-4-22-ec-registry-ocp-art-ec-stage",
            application_name="openshift-4-22",
            application_uid="test-uid-prega",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-ec-stage",
        )

        self.assertEqual(manifest["metadata"]["name"], "openshift-4-22-ec-registry-ocp-art-ec-stage")
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        self.assertEqual(params["POLICY_CONFIGURATION"], "rhtap-releng-tenant/registry-ocp-art-ec-stage")


class TestNewEcPipelinerun(TestCase):
    def test_manifest_structure(self):
        snapshot_spec = {
            "application": "openshift-4-21",
            "components": [
                {
                    "name": "ose-4-21-openshift-apiserver",
                    "containerImage": "quay.io/repo@sha256:abc123",
                    "source": {"git": {"url": "https://github.com/org/repo", "revision": "deadbeef"}},
                }
            ],
        }
        snapshot_json = json.dumps(snapshot_spec)
        watch_labels = {"doozer-watch-id": "12345"}

        manifest = KonfluxClient._new_ec_pipelinerun(
            generate_name="openshift-4-21-ec-ose-4-21-openshift-apiserver-",
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            snapshot_json=snapshot_json,
            its_name="openshift-4-21-ec-registry-ocp-art-stage",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
            watch_labels=watch_labels,
        )

        self.assertEqual(manifest["apiVersion"], "tekton.dev/v1")
        self.assertEqual(manifest["kind"], "PipelineRun")
        self.assertEqual(manifest["metadata"]["generateName"], "openshift-4-21-ec-ose-4-21-openshift-apiserver-")
        self.assertEqual(manifest["metadata"]["namespace"], "ocp-art-tenant")

        # labels
        labels = manifest["metadata"]["labels"]
        self.assertEqual(labels["appstudio.openshift.io/application"], "openshift-4-21")
        self.assertEqual(labels["appstudio.openshift.io/component"], "ose-4-21-openshift-apiserver")
        self.assertEqual(labels["test.appstudio.openshift.io/scenario"], "openshift-4-21-ec-registry-ocp-art-stage")
        self.assertEqual(labels["kueue.x-k8s.io/priority-class"], "build-priority-2")
        self.assertEqual(labels["doozer-watch-id"], "12345")

        # annotations
        self.assertEqual(manifest["metadata"]["annotations"]["test.appstudio.openshift.io/kind"], "enterprise-contract")
        self.assertIn("art-jenkins-job-url", manifest["metadata"]["annotations"])

        # pipelineRef
        pipeline_ref = manifest["spec"]["pipelineRef"]
        self.assertEqual(pipeline_ref["resolver"], "git")
        ref_params = {p["name"]: p["value"] for p in pipeline_ref["params"]}
        self.assertEqual(ref_params["url"], "https://github.com/konflux-ci/build-definitions")
        self.assertEqual(ref_params["pathInRepo"], "pipelines/enterprise-contract.yaml")

        # params
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        self.assertEqual(params["POLICY_CONFIGURATION"], "rhtap-releng-tenant/registry-ocp-art-stage")
        self.assertEqual(params["SINGLE_COMPONENT"], "true")
        parsed_snapshot = json.loads(params["SNAPSHOT"])
        self.assertEqual(parsed_snapshot["application"], "openshift-4-21")
        self.assertEqual(len(parsed_snapshot["components"]), 1)
        self.assertEqual(parsed_snapshot["components"][0]["name"], "ose-4-21-openshift-apiserver")

        # timeouts
        self.assertEqual(manifest["spec"]["timeouts"]["pipeline"], "1h")


class TestEnsureIntegrationTestScenario(IsolatedAsyncioTestCase):
    async def test_creates_its_with_application_uid(self):
        client = MagicMock(spec=KonfluxClient)
        client.get_application = AsyncMock()
        client.get_application.return_value = MagicMock()
        client.get_application.return_value.metadata.uid = "app-uid-5678"
        client._create_or_patch = AsyncMock()
        client._create_or_patch.return_value = MagicMock()
        # Ensure the static method is not mocked away
        client._new_integration_test_scenario = KonfluxClient._new_integration_test_scenario

        # Call the real method with mocked self
        await KonfluxClient.ensure_integration_test_scenario(
            client,
            name="openshift-4-21-ec-registry-ocp-art-stage",
            application_name="openshift-4-21",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
        )

        client.get_application.assert_called_once_with("openshift-4-21", strict=True)
        client._create_or_patch.assert_called_once()
        manifest = client._create_or_patch.call_args[0][0]
        self.assertEqual(manifest["metadata"]["ownerReferences"][0]["uid"], "app-uid-5678")
        self.assertEqual(manifest["spec"]["application"], "openshift-4-21")


class TestStartEcPipelineRun(IsolatedAsyncioTestCase):
    @patch("doozerlib.backend.konflux_client.get_common_runtime_watcher_labels", return_value={"doozer-watch-id": "99"})
    async def test_dry_run_returns_fake_plr(self, _mock_labels):
        client = MagicMock(spec=KonfluxClient)
        client.dry_run = True
        client.dyn_client = MagicMock()
        client._logger = MagicMock()
        # Ensure the static method is not mocked away
        client._new_ec_pipelinerun = KonfluxClient._new_ec_pipelinerun

        result = await KonfluxClient.start_ec_pipeline_run(
            client,
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            its_name="openshift-4-21-ec-registry-ocp-art-stage",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
        )

        self.assertEqual(result.name, "ose-4-21-openshift-apiserver-ec-dry-run")

    @patch("doozerlib.backend.konflux_client.get_common_runtime_watcher_labels", return_value={"doozer-watch-id": "99"})
    async def test_non_dry_run_creates_plr(self, _mock_labels):
        client = MagicMock(spec=KonfluxClient)
        client.dry_run = False
        client._logger = MagicMock()
        client._new_ec_pipelinerun = KonfluxClient._new_ec_pipelinerun

        mock_plr = MagicMock()
        mock_plr.to_dict.return_value = {
            "kind": "PipelineRun",
            "metadata": {
                "name": "openshift-4-21-ec-ose-4-21-openshift-apiserver-abc12",
                "namespace": "ocp-art-tenant",
                "labels": {"appstudio.openshift.io/application": "openshift-4-21"},
            },
        }
        client._create = AsyncMock(return_value=mock_plr)
        client.resource_url = KonfluxClient.resource_url

        result = await KonfluxClient.start_ec_pipeline_run(
            client,
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            its_name="openshift-4-21-ec-registry-ocp-art-stage",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
        )

        client._create.assert_called_once()
        self.assertEqual(result.name, "openshift-4-21-ec-ose-4-21-openshift-apiserver-abc12")

        # Verify the manifest includes the correct serviceAccountName
        manifest = client._create.call_args[0][0]
        self.assertEqual(
            manifest["spec"]["taskRunTemplate"]["serviceAccountName"],
            "build-pipeline-ose-4-21-openshift-apiserver",
        )

    @patch("doozerlib.backend.konflux_client.get_common_runtime_watcher_labels", return_value={"doozer-watch-id": "99"})
    async def test_generate_name_truncation(self, _mock_labels):
        client = MagicMock(spec=KonfluxClient)
        client.dry_run = True
        client.dyn_client = MagicMock()
        client._logger = MagicMock()

        captured_kwargs = {}
        real_method = KonfluxClient._new_ec_pipelinerun

        def spy_new_ec_pipelinerun(**kwargs):
            captured_kwargs.update(kwargs)
            return real_method(**kwargs)

        client._new_ec_pipelinerun = spy_new_ec_pipelinerun

        long_app_name = "a" * 150
        long_component_name = "b" * 150

        await KonfluxClient.start_ec_pipeline_run(
            client,
            namespace="ocp-art-tenant",
            application_name=long_app_name,
            component_name=long_component_name,
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            its_name="test-its",
            policy_configuration="test-policy",
        )

        self.assertLessEqual(len(captured_kwargs["generate_name"]), 248)

    @patch("doozerlib.backend.konflux_client.get_common_runtime_watcher_labels", return_value={"doozer-watch-id": "99"})
    async def test_snapshot_json_structure(self, _mock_labels):
        client = MagicMock(spec=KonfluxClient)
        client.dry_run = True
        client.dyn_client = MagicMock()
        client._logger = MagicMock()

        captured_kwargs = {}
        real_method = KonfluxClient._new_ec_pipelinerun

        def spy_new_ec_pipelinerun(**kwargs):
            captured_kwargs.update(kwargs)
            return real_method(**kwargs)

        client._new_ec_pipelinerun = spy_new_ec_pipelinerun

        await KonfluxClient.start_ec_pipeline_run(
            client,
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            its_name="openshift-4-21-ec-registry-ocp-art-stage",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
        )

        snapshot = json.loads(captured_kwargs["snapshot_json"])

        self.assertEqual(snapshot["application"], "openshift-4-21")
        self.assertEqual(len(snapshot["components"]), 1)
        component = snapshot["components"][0]
        self.assertEqual(component["name"], "ose-4-21-openshift-apiserver")
        self.assertEqual(component["containerImage"], "quay.io/repo@sha256:abc123")
        self.assertEqual(component["source"]["git"]["url"], "https://github.com/org/repo")
        self.assertEqual(component["source"]["git"]["revision"], "deadbeef")


class TestEnsureIntegrationTestScenarioNotFound(IsolatedAsyncioTestCase):
    async def test_application_not_found_propagates(self):
        from kubernetes.dynamic import exceptions as dyn_exceptions

        client = MagicMock(spec=KonfluxClient)
        client.get_application = AsyncMock(side_effect=dyn_exceptions.NotFoundError(MagicMock(status=404)))
        client._new_integration_test_scenario = KonfluxClient._new_integration_test_scenario

        with self.assertRaises(dyn_exceptions.NotFoundError):
            await KonfluxClient.ensure_integration_test_scenario(
                client,
                name="openshift-4-21-ec-test",
                application_name="nonexistent-app",
                policy_configuration="test-policy",
            )


class TestVerifyEnterpriseContract(IsolatedAsyncioTestCase):
    def _make_client_with_ec_result(self, is_success: bool):
        """Create a mocked KonfluxClient that returns the given EC outcome."""
        from artcommonlib.util import KubeCondition

        client = MagicMock(spec=KonfluxClient)
        client.ensure_integration_test_scenario = AsyncMock()

        ec_plr_mock = MagicMock()
        ec_plr_mock.name = "test-ec-plr"
        client.start_ec_pipeline_run = AsyncMock(return_value=ec_plr_mock)

        condition = KubeCondition(
            {
                'type': 'Succeeded',
                'status': 'True' if is_success else 'False',
                'reason': 'Succeeded' if is_success else 'Failed',
                'message': '',
            }
        )
        waited_plr = MagicMock()
        waited_plr.find_condition.return_value = condition
        waited_plr.to_dict.return_value = {"metadata": {"name": "test-ec-plr"}}
        client.wait_for_pipelinerun = AsyncMock(return_value=waited_plr)
        client.resource_url = MagicMock(return_value="https://example.com/plr/test")
        return client

    async def test_ec_passed(self):
        client = self._make_client_with_ec_result(is_success=True)

        result = await KonfluxClient.verify_enterprise_contract(
            client,
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            ec_policy="rhtap-releng-tenant/registry-ocp-art-stage",
            logger=logging.getLogger("test"),
        )

        self.assertIsInstance(result, ECVerificationResult)
        self.assertEqual(result.ec_status, KonfluxECStatus.PASSED)
        self.assertFalse(result.ec_failed)
        self.assertEqual(result.ec_pipeline_url, "https://example.com/plr/test")
        client.ensure_integration_test_scenario.assert_called_once_with(
            name="openshift-4-21-ec-registry-ocp-art-stage",
            application_name="openshift-4-21",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-stage",
        )

    async def test_ec_failed(self):
        client = self._make_client_with_ec_result(is_success=False)

        result = await KonfluxClient.verify_enterprise_contract(
            client,
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            ec_policy="rhtap-releng-tenant/registry-ocp-art-stage",
            logger=logging.getLogger("test"),
        )

        self.assertEqual(result.ec_status, KonfluxECStatus.FAILED)
        self.assertTrue(result.ec_failed)

    async def test_ec_exception_returns_failed(self):
        client = MagicMock(spec=KonfluxClient)
        client.ensure_integration_test_scenario = AsyncMock(side_effect=RuntimeError("connection error"))

        result = await KonfluxClient.verify_enterprise_contract(
            client,
            namespace="ocp-art-tenant",
            application_name="openshift-4-21",
            component_name="ose-4-21-openshift-apiserver",
            image_pullspec="quay.io/repo@sha256:abc123",
            source_url="https://github.com/org/repo",
            commit_sha="deadbeef",
            ec_policy="rhtap-releng-tenant/registry-ocp-art-stage",
            logger=logging.getLogger("test"),
        )

        self.assertEqual(result.ec_status, KonfluxECStatus.FAILED)
        self.assertTrue(result.ec_failed)

    async def test_its_name_derived_from_fbc_policy(self):
        client = self._make_client_with_ec_result(is_success=True)

        await KonfluxClient.verify_enterprise_contract(
            client,
            namespace="ns",
            application_name="fbc-openshift-4-21",
            component_name="comp",
            image_pullspec="quay.io/repo@sha256:abc",
            source_url="https://github.com/org/repo",
            commit_sha="abc",
            ec_policy="rhtap-releng-tenant/fbc-ocp-art-stage",
            logger=logging.getLogger("test"),
        )

        client.ensure_integration_test_scenario.assert_called_once_with(
            name="fbc-openshift-4-21-ec-fbc-ocp-art-stage",
            application_name="fbc-openshift-4-21",
            policy_configuration="rhtap-releng-tenant/fbc-ocp-art-stage",
        )

    async def test_its_name_derived_from_base_image_policy(self):
        client = self._make_client_with_ec_result(is_success=True)

        await KonfluxClient.verify_enterprise_contract(
            client,
            namespace="ns",
            application_name="openshift-4-21",
            component_name="comp",
            image_pullspec="quay.io/repo@sha256:abc",
            source_url="https://github.com/org/repo",
            commit_sha="abc",
            ec_policy="rhtap-releng-tenant/registry-ocp-art-base-prod",
            logger=logging.getLogger("test"),
        )

        client.ensure_integration_test_scenario.assert_called_once_with(
            name="openshift-4-21-ec-registry-ocp-art-base-prod",
            application_name="openshift-4-21",
            policy_configuration="rhtap-releng-tenant/registry-ocp-art-base-prod",
        )


class TestNewPipelinerunBuildArgs(IsolatedAsyncioTestCase):
    """Tests for build_args in _new_pipelinerun_for_image_build."""

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_build_args_set_on_params(self, mock_get_template):
        """Test that build_args are set as the build-args pipeline parameter."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(
                build_args=[
                    "RELEASE_FLAG=--release-image-url",
                    "RELEASE_VALUE=$(params.release-value)",
                    "MAJOR_MINOR_VERSION=$(params.major-minor-version)",
                    "ARCH=x86_64",
                ]
            ),
        )

        plr_params = result["spec"]["params"]
        param_dict = {p["name"]: p["value"] for p in plr_params}

        self.assertEqual(
            param_dict["build-args"],
            [
                "RELEASE_FLAG=--release-image-url",
                "RELEASE_VALUE=$(params.release-value)",
                "MAJOR_MINOR_VERSION=$(params.major-minor-version)",
                "ARCH=x86_64",
            ],
        )

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_build_args_none_is_noop(self, mock_get_template):
        """Test that None build_args leaves the default build-args param unchanged."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(build_args=None),
        )

        plr_params = result["spec"]["params"]
        param_dict = {p["name"]: p["value"] for p in plr_params}
        self.assertEqual(param_dict["build-args"], [])


class TestNewPipelinerunAdditionalSecret(IsolatedAsyncioTestCase):
    """Tests for additional_secret in _new_pipelinerun_for_image_build."""

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_additional_secret_set_on_build_images_task(self, mock_get_template):
        """Test that additional_secret is set as ADDITIONAL_SECRET on the build-images task."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(additional_secret="ove-ui-image-pull-secret"),
        )

        tasks = result["spec"]["pipelineSpec"]["tasks"]
        build_images_task = next(t for t in tasks if t["name"] == "build-images")
        task_param_dict = {p["name"]: p["value"] for p in build_images_task["params"]}

        self.assertEqual(task_param_dict["ADDITIONAL_SECRET"], "ove-ui-image-pull-secret")

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_additional_secret_none_is_noop(self, mock_get_template):
        """Test that None additional_secret doesn't add ADDITIONAL_SECRET to the task."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(additional_secret=None),
        )

        tasks = result["spec"]["pipelineSpec"]["tasks"]
        build_images_task = next(t for t in tasks if t["name"] == "build-images")
        task_param_names = {p["name"] for p in build_images_task["params"]}

        self.assertNotIn("ADDITIONAL_SECRET", task_param_names)


class TestNewPipelinerunPrivilegedNested(IsolatedAsyncioTestCase):
    """Tests for privileged_nested in _new_pipelinerun_for_image_build."""

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_privileged_nested_true_set_on_build_images_task(self, mock_get_template):
        """Test that privileged_nested=True sets PRIVILEGED_NESTED=true on the build-images task."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(privileged_nested=True),
        )

        tasks = result["spec"]["pipelineSpec"]["tasks"]
        build_images_task = next(t for t in tasks if t["name"] == "build-images")
        task_param_dict = {p["name"]: p["value"] for p in build_images_task["params"]}

        self.assertEqual(task_param_dict["PRIVILEGED_NESTED"], "true")

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_privileged_nested_none_is_noop(self, mock_get_template):
        """Test that None privileged_nested doesn't add PRIVILEGED_NESTED to the task."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(privileged_nested=None),
        )

        tasks = result["spec"]["pipelineSpec"]["tasks"]
        build_images_task = next(t for t in tasks if t["name"] == "build-images")
        task_param_names = {p["name"] for p in build_images_task["params"]}

        self.assertNotIn("PRIVILEGED_NESTED", task_param_names)


class TestNewPipelinerunBuildStepResources(IsolatedAsyncioTestCase):
    """Tests for build_step_resources in _new_pipelinerun_for_image_build."""

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_build_step_resources_set_on_build_step(self, mock_get_template):
        """Test that build_step_resources are applied to the build step."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(build_step_resources={"memory": "8Gi"}),
        )

        task_run_specs = result["spec"]["taskRunSpecs"]
        build_images_spec = next(s for s in task_run_specs if s["pipelineTaskName"] == "build-images")
        build_step = next(s for s in build_images_spec["stepSpecs"] if s["name"] == "build")
        self.assertEqual(build_step["computeResources"]["requests"]["memory"], "8Gi")

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_build_step_resources_none_is_noop(self, mock_get_template):
        """Test that None build_step_resources doesn't add a build stepSpec."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(build_step_resources=None),
        )

        task_run_specs = result["spec"]["taskRunSpecs"]
        build_images_spec = next(s for s in task_run_specs if s["pipelineTaskName"] == "build-images")
        step_names = {s["name"] for s in build_images_spec["stepSpecs"]}
        self.assertNotIn("build", step_names)


class TestNewPipelinerunWorkspaceStorage(IsolatedAsyncioTestCase):
    """Tests for workspace_storage volumeClaimTemplate injection."""

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_workspace_storage_adds_volume_claim(self, mock_get_template):
        """Test that workspace_storage injects a volumeClaimTemplate into the PLR."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(workspace_storage="100Gi"),
        )

        pipeline_workspaces = result["spec"]["pipelineSpec"].get("workspaces", [])
        ws_entry = next((ws for ws in pipeline_workspaces if ws["name"] == "workspace"), None)
        self.assertIsNotNone(ws_entry)
        self.assertTrue(ws_entry.get("optional"))

        plr_workspaces = result["spec"].get("workspaces", [])
        plr_ws = next((ws for ws in plr_workspaces if ws["name"] == "workspace"), None)
        self.assertIsNotNone(plr_ws)
        self.assertIn("volumeClaimTemplate", plr_ws)
        self.assertEqual(
            plr_ws["volumeClaimTemplate"]["spec"]["resources"]["requests"]["storage"],
            "100Gi",
        )

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_no_workspace_storage_no_volume_claim(self, mock_get_template):
        """Test that without workspace_storage, no volumeClaimTemplate is injected."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(),
        )

        plr_workspaces = result["spec"].get("workspaces", [])
        ws_entry = next((ws for ws in plr_workspaces if ws.get("name") == "workspace"), None)
        self.assertIsNone(ws_entry)


class TestNewPipelinerunEphemeralStorage(IsolatedAsyncioTestCase):
    """Tests for ephemeral-storage in build_step_resources propagating to post-build steps."""

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_ephemeral_storage_propagates_to_post_build_steps(self, mock_get_template):
        """Test that ephemeral-storage in build_step_resources adds 1Gi to post-build steps."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(build_step_resources={"memory": "8Gi", "ephemeral-storage": "250Gi"}),
        )

        task_run_specs = result["spec"]["taskRunSpecs"]
        build_images_spec = next(s for s in task_run_specs if s["pipelineTaskName"] == "build-images")
        step_specs = {s["name"]: s for s in build_images_spec["stepSpecs"]}

        self.assertEqual(step_specs["build"]["computeResources"]["requests"]["ephemeral-storage"], "250Gi")
        for step_name in ("push", "sbom-syft-generate", "prepare-sboms", "upload-sbom"):
            self.assertIn(step_name, step_specs, f"Missing stepSpec for {step_name}")
            self.assertEqual(
                step_specs[step_name]["computeResources"]["requests"]["ephemeral-storage"],
                "1Gi",
                f"Wrong ephemeral-storage for {step_name}",
            )

    @patch("doozerlib.backend.konflux_client.KonfluxClient._get_pipelinerun_template")
    async def test_no_ephemeral_storage_no_post_build_steps(self, mock_get_template):
        """Test that without ephemeral-storage, post-build steps don't get extra resources."""
        client = _make_mock_client(mock_get_template)

        result = await client._new_pipelinerun_for_image_build(
            **_COMMON_KWARGS,
            build_params=ImageBuildParams(build_step_resources={"memory": "8Gi"}),
        )

        task_run_specs = result["spec"]["taskRunSpecs"]
        build_images_spec = next(s for s in task_run_specs if s["pipelineTaskName"] == "build-images")
        step_names = {s["name"] for s in build_images_spec["stepSpecs"]}
        self.assertIn("build", step_names)
        self.assertNotIn("push", step_names)
        self.assertNotIn("prepare-sboms", step_names)
        self.assertNotIn("upload-sbom", step_names)


class TestSkipTasks(IsolatedAsyncioTestCase):
    """Tests for the generic skip_tasks mechanism in _new_pipelinerun_for_image_build."""

    MINIMAL_TEMPLATE = jinja2.Template(
        json.dumps(
            {
                "metadata": {
                    "name": "test-plr",
                    "namespace": "test-ns",
                    "annotations": {
                        "pipelinesascode.tekton.dev/on-cel-expression": "dummy",
                        "build.appstudio.openshift.io/repo": "",
                    },
                    "labels": {
                        "appstudio.openshift.io/application": "test-app",
                        "appstudio.openshift.io/component": "test-comp",
                    },
                },
                "spec": {
                    "params": [],
                    "timeouts": {},
                    "taskRunTemplate": {"serviceAccountName": "default"},
                    "pipelineSpec": {
                        "tasks": [
                            {"name": "clone-repository", "params": [{"name": "refspec", "value": ""}]},
                            {"name": "build-images", "params": [{"name": "SBOM_TYPE", "value": ""}]},
                            {"name": "clair-scan", "params": []},
                            {"name": "sast-snyk-check", "params": []},
                            {"name": "sast-unicode-check", "params": []},
                            {"name": "sast-shell-check", "params": []},
                            {"name": "apply-tags", "params": [{"name": "ADDITIONAL_TAGS", "value": []}]},
                            {"name": "fbc-fips-check-oci-ta", "params": []},
                        ],
                    },
                },
            }
        )
    )

    def _make_client(self):
        client = MagicMock(spec=KonfluxClient)
        client._logger = logging.getLogger("test")
        client.dry_run = False

        async def fake_get_template(url):
            return self.MINIMAL_TEMPLATE

        client._get_pipelinerun_template = fake_get_template
        return client

    async def _build_plr(self, **kwargs):
        """Helper to call _new_pipelinerun_for_image_build with minimal defaults."""
        defaults = dict(
            generate_name="test-",
            namespace="ns",
            application_name="app",
            component_name="comp",
            git_url="https://github.com/org/repo",
            commit_sha="abc123",
            target_branch="main",
            output_image="quay.io/test:latest",
            build_platforms=["linux/x86_64"],
        )
        defaults.update(kwargs)
        client = self._make_client()
        return await KonfluxClient._new_pipelinerun_for_image_build(client, **defaults)

    async def test_skip_tasks_removes_named_tasks(self):
        result = await self._build_plr(build_params=ImageBuildParams(skip_tasks=["clair-scan"]))
        task_names = [t["name"] for t in result["spec"]["pipelineSpec"]["tasks"]]
        self.assertNotIn("clair-scan", task_names)
        self.assertIn("build-images", task_names)
        self.assertIn("sast-snyk-check", task_names)

    async def test_skip_tasks_multiple(self):
        result = await self._build_plr(build_params=ImageBuildParams(skip_tasks=["clair-scan", "sast-snyk-check"]))
        task_names = [t["name"] for t in result["spec"]["pipelineSpec"]["tasks"]]
        self.assertNotIn("clair-scan", task_names)
        self.assertNotIn("sast-snyk-check", task_names)
        self.assertIn("build-images", task_names)

    async def test_legacy_sast_false_still_removes_tasks(self):
        result = await self._build_plr(build_params=ImageBuildParams(sast=False))
        task_names = [t["name"] for t in result["spec"]["pipelineSpec"]["tasks"]]
        self.assertNotIn("sast-unicode-check", task_names)
        self.assertNotIn("sast-shell-check", task_names)
        self.assertIn("clair-scan", task_names)

    async def test_legacy_skip_fips_check_still_removes_task(self):
        result = await self._build_plr(build_params=ImageBuildParams(skip_fips_check=True))
        task_names = [t["name"] for t in result["spec"]["pipelineSpec"]["tasks"]]
        self.assertNotIn("fbc-fips-check-oci-ta", task_names)
        self.assertIn("clair-scan", task_names)

    async def test_skip_tasks_combined_with_legacy_flags(self):
        result = await self._build_plr(
            build_params=ImageBuildParams(
                skip_tasks=["clair-scan"],
                sast=False,
                skip_fips_check=True,
            )
        )
        task_names = [t["name"] for t in result["spec"]["pipelineSpec"]["tasks"]]
        self.assertNotIn("clair-scan", task_names)
        self.assertNotIn("sast-unicode-check", task_names)
        self.assertNotIn("sast-shell-check", task_names)
        self.assertNotIn("fbc-fips-check-oci-ta", task_names)
        self.assertIn("build-images", task_names)
        self.assertIn("clone-repository", task_names)

    async def test_skip_tasks_unknown_name_is_silently_ignored(self):
        result = await self._build_plr(build_params=ImageBuildParams(skip_tasks=["nonexistent-task"]))
        task_names = [t["name"] for t in result["spec"]["pipelineSpec"]["tasks"]]
        self.assertEqual(len(task_names), 8)
