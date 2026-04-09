import json
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib.backend.konflux_client import (
    API_VERSION,
    API_VERSION_V1BETA2,
    KIND_APPLICATION,
    KIND_INTEGRATION_TEST_SCENARIO,
    GitHubApiUrlInfo,
    KonfluxClient,
    parse_github_api_url,
)


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
        # Test named tuple access
        self.assertEqual(result[0], "owner")  # owner
        self.assertEqual(result[1], "repo")  # repo
        self.assertEqual(result[2], "file.yaml")  # file_path
        self.assertEqual(result[3], "main")  # ref


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

        # spec
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
