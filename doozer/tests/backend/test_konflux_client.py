from unittest import TestCase
from unittest.mock import patch

from artcommonlib.model import Model
from doozerlib.backend.konflux_client import GitHubApiUrlInfo, KonfluxClient, parse_github_api_url


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
