import asyncio
import base64
import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.backend.konflux_client import (
    _GIT_AUTH_GENERATED_BY_LABEL_KEY,
    _GIT_AUTH_GENERATED_BY_LABEL_VALUE,
    _GIT_AUTH_SECRET_LABEL_KEY,
    _GIT_AUTH_SECRET_LABEL_VALUE,
    _GIT_AUTH_SECRET_PREFIX,
    KonfluxClient,
)
from kubernetes.client import ApiException


def _make_client(dry_run=False) -> KonfluxClient:
    """Build a KonfluxClient with mocked Kubernetes plumbing."""
    config = MagicMock()
    with (
        patch("doozerlib.backend.konflux_client.ApiClient"),
        patch("doozerlib.backend.konflux_client.DynamicClient"),
        patch("doozerlib.backend.konflux_client.CoreV1Api"),
    ):
        client = KonfluxClient(
            default_namespace="test-ns",
            config=config,
            dry_run=dry_run,
        )
    return client


def _run(coro):
    """Helper to run async code in sync tests."""
    return asyncio.run(coro)


class TestEnsureGitAuthSecret(TestCase):
    @patch.dict("os.environ", {}, clear=True)
    def test_fallback_when_no_github_app_id(self):
        """Without GITHUB_APP_ID the static PAT secret name is returned."""
        client = _make_client()
        name = _run(client.ensure_git_auth_secret())
        self.assertEqual(name, "pipelines-as-code-secret")

    @patch.dict("os.environ", {"GITHUB_APP_ID": "12345", "BUILD_URL": "https://jenkins.example.com/job/test/42/"})
    @patch("doozerlib.backend.konflux_client.get_github_app_token_for_org", return_value="ghp_fake_token")
    @patch("doozerlib.backend.konflux_client.time")
    def test_creates_secret_with_correct_shape(self, mock_time, mock_token_fn):
        mock_time.time_ns.return_value = 1700000000000000000
        client = _make_client()

        name = _run(client.ensure_git_auth_secret(namespace="my-ns"))

        self.assertEqual(name, f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000")
        client.corev1_client.create_namespaced_secret.assert_called_once()
        call_kwargs = client.corev1_client.create_namespaced_secret.call_args
        body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        self.assertEqual(body.type, "kubernetes.io/basic-auth")
        self.assertEqual(body.metadata.namespace, "my-ns")
        self.assertEqual(
            body.metadata.labels[_GIT_AUTH_SECRET_LABEL_KEY],
            _GIT_AUTH_SECRET_LABEL_VALUE,
        )
        self.assertEqual(
            body.metadata.labels[_GIT_AUTH_GENERATED_BY_LABEL_KEY],
            _GIT_AUTH_GENERATED_BY_LABEL_VALUE,
        )
        self.assertEqual(
            body.metadata.annotations["art-jenkins-job-url"],
            "https://jenkins.example.com/job/test/42/",
        )
        self.assertEqual(
            base64.b64decode(body.data["username"]).decode(),
            "x-access-token",
        )
        self.assertEqual(
            base64.b64decode(body.data["password"]).decode(),
            "ghp_fake_token",
        )

    @patch.dict("os.environ", {"GITHUB_APP_ID": "12345"})
    @patch("doozerlib.backend.konflux_client.get_github_app_token_for_org", return_value="ghp_fake_token")
    @patch("doozerlib.backend.konflux_client.time")
    def test_caches_secret_name(self, mock_time, mock_token_fn):
        mock_time.time_ns.return_value = 1700000000000000000
        client = _make_client()

        name1 = _run(client.ensure_git_auth_secret())
        name2 = _run(client.ensure_git_auth_secret())

        self.assertEqual(name1, name2)
        # create_namespaced_secret should only be called once
        self.assertEqual(client.corev1_client.create_namespaced_secret.call_count, 1)

    @patch.dict("os.environ", {"GITHUB_APP_ID": "12345"})
    @patch("doozerlib.backend.konflux_client.get_github_app_token_for_org", return_value="ghp_fake_token")
    @patch("doozerlib.backend.konflux_client.time")
    def test_dry_run_does_not_create(self, mock_time, mock_token_fn):
        mock_time.time_ns.return_value = 1700000000000000000
        client = _make_client(dry_run=True)

        name = _run(client.ensure_git_auth_secret())

        self.assertTrue(name.startswith(_GIT_AUTH_SECRET_PREFIX))
        client.corev1_client.create_namespaced_secret.assert_not_called()


class TestCleanupStaleGitAuthSecrets(TestCase):
    def _make_secret(self, name: str, created: datetime.datetime):
        secret = MagicMock()
        secret.metadata.name = name
        secret.metadata.creation_timestamp = created
        return secret

    def test_deletes_old_secrets(self):
        client = _make_client()
        now = datetime.datetime.now(datetime.timezone.utc)
        old = now - datetime.timedelta(hours=48)
        recent = now - datetime.timedelta(hours=1)

        old_secret = self._make_secret("art-transient-pipeline-auth-100", old)
        recent_secret = self._make_secret("art-transient-pipeline-auth-200", recent)

        mock_list = MagicMock()
        mock_list.items = [old_secret, recent_secret]
        client.corev1_client.list_namespaced_secret.return_value = mock_list

        _run(client.cleanup_stale_git_auth_secrets(namespace="test-ns"))

        # Verify that list was scoped to the transient-secret label
        list_kwargs = client.corev1_client.list_namespaced_secret.call_args.kwargs
        self.assertEqual(list_kwargs["namespace"], "test-ns")
        self.assertEqual(
            list_kwargs["label_selector"],
            f"{_GIT_AUTH_SECRET_LABEL_KEY}={_GIT_AUTH_SECRET_LABEL_VALUE},"
            f"{_GIT_AUTH_GENERATED_BY_LABEL_KEY}={_GIT_AUTH_GENERATED_BY_LABEL_VALUE}",
        )

        # Only the old secret should be deleted
        client.corev1_client.delete_namespaced_secret.assert_called_once_with(
            name="art-transient-pipeline-auth-100",
            namespace="test-ns",
            _request_timeout=client.request_timeout,
        )

    def test_ignores_404_on_delete(self):
        """If another process already deleted the secret, the 404 should be silently ignored."""
        client = _make_client()
        old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=48)
        old_secret = self._make_secret("art-transient-pipeline-auth-100", old)

        mock_list = MagicMock()
        mock_list.items = [old_secret]
        client.corev1_client.list_namespaced_secret.return_value = mock_list
        client.corev1_client.delete_namespaced_secret.side_effect = ApiException(status=404, reason="Not Found")

        # Should not raise
        _run(client.cleanup_stale_git_auth_secrets(namespace="test-ns"))

    def test_does_not_delete_recent_secrets(self):
        client = _make_client()
        recent = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
        recent_secret = self._make_secret("art-transient-pipeline-auth-200", recent)

        mock_list = MagicMock()
        mock_list.items = [recent_secret]
        client.corev1_client.list_namespaced_secret.return_value = mock_list

        _run(client.cleanup_stale_git_auth_secrets(namespace="test-ns"))

        client.corev1_client.delete_namespaced_secret.assert_not_called()

    def test_dry_run_does_not_delete(self):
        client = _make_client(dry_run=True)
        old = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=48)
        old_secret = self._make_secret("art-transient-pipeline-auth-100", old)

        mock_list = MagicMock()
        mock_list.items = [old_secret]
        client.corev1_client.list_namespaced_secret.return_value = mock_list

        _run(client.cleanup_stale_git_auth_secrets(namespace="test-ns"))

        client.corev1_client.delete_namespaced_secret.assert_not_called()


class TestDeleteGitAuthSecret(TestCase):
    def test_deletes_own_secret_and_resets_cache(self):
        client = _make_client()
        client._git_auth_secret_name = f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000"

        _run(client.delete_git_auth_secret(namespace="test-ns"))

        client.corev1_client.delete_namespaced_secret.assert_called_once_with(
            name=f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000",
            namespace="test-ns",
            _request_timeout=client.request_timeout,
        )
        self.assertIsNone(client._git_auth_secret_name)

    def test_noop_when_no_secret_created(self):
        client = _make_client()
        self.assertIsNone(client._git_auth_secret_name)

        _run(client.delete_git_auth_secret(namespace="test-ns"))

        client.corev1_client.delete_namespaced_secret.assert_not_called()

    def test_noop_for_static_fallback(self):
        """When the fallback pipelines-as-code-secret was used, nothing is deleted."""
        client = _make_client()
        client._git_auth_secret_name = "pipelines-as-code-secret"

        _run(client.delete_git_auth_secret(namespace="test-ns"))

        client.corev1_client.delete_namespaced_secret.assert_not_called()
        self.assertEqual(client._git_auth_secret_name, "pipelines-as-code-secret")

    def test_ignores_404(self):
        client = _make_client()
        client._git_auth_secret_name = f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000"
        client.corev1_client.delete_namespaced_secret.side_effect = ApiException(status=404, reason="Not Found")

        _run(client.delete_git_auth_secret(namespace="test-ns"))

        self.assertIsNone(client._git_auth_secret_name)

    def test_dry_run_skips_deletion(self):
        client = _make_client(dry_run=True)
        client._git_auth_secret_name = f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000"

        _run(client.delete_git_auth_secret(namespace="test-ns"))

        client.corev1_client.delete_namespaced_secret.assert_not_called()
        self.assertIsNone(client._git_auth_secret_name)


class TestRefreshGitAuthSecret(TestCase):
    @patch("doozerlib.backend.konflux_client.get_github_app_token_for_org", return_value="ghp_refreshed_token")
    def test_refreshes_secret_with_new_token(self, mock_token_fn):
        client = _make_client()
        secret_name = f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000"
        client._git_auth_secret_name = secret_name

        existing_secret = MagicMock()
        existing_secret.data = {"username": base64.b64encode(b"x-access-token").decode(), "password": "old"}
        existing_secret.metadata.annotations = {"art.openshift.io/created-at": "old-time"}
        client.corev1_client.read_namespaced_secret.return_value = existing_secret

        _run(client.refresh_git_auth_secret(namespace="test-ns"))

        client.corev1_client.read_namespaced_secret.assert_called_once_with(
            name=secret_name,
            namespace="test-ns",
            _request_timeout=client.request_timeout,
        )
        client.corev1_client.replace_namespaced_secret.assert_called_once()
        call_kwargs = client.corev1_client.replace_namespaced_secret.call_args
        body = call_kwargs.kwargs.get("body") or call_kwargs[1].get("body")
        self.assertEqual(
            base64.b64decode(body.data["password"]).decode(),
            "ghp_refreshed_token",
        )

    def test_noop_when_no_secret(self):
        client = _make_client()
        self.assertIsNone(client._git_auth_secret_name)

        _run(client.refresh_git_auth_secret(namespace="test-ns"))

        client.corev1_client.read_namespaced_secret.assert_not_called()

    def test_noop_for_static_fallback(self):
        client = _make_client()
        client._git_auth_secret_name = "pipelines-as-code-secret"

        _run(client.refresh_git_auth_secret(namespace="test-ns"))

        client.corev1_client.read_namespaced_secret.assert_not_called()

    @patch("doozerlib.backend.konflux_client.get_github_app_token_for_org", return_value="ghp_refreshed_token")
    def test_handles_404(self, mock_token_fn):
        client = _make_client()
        client._git_auth_secret_name = f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000"
        client.corev1_client.read_namespaced_secret.side_effect = ApiException(status=404, reason="Not Found")

        # Should not raise
        _run(client.refresh_git_auth_secret(namespace="test-ns"))

        client.corev1_client.replace_namespaced_secret.assert_not_called()

    def test_dry_run_skips_refresh(self):
        client = _make_client(dry_run=True)
        client._git_auth_secret_name = f"{_GIT_AUTH_SECRET_PREFIX}1700000000000000000"

        _run(client.refresh_git_auth_secret(namespace="test-ns"))

        client.corev1_client.read_namespaced_secret.assert_not_called()
        client.corev1_client.replace_namespaced_secret.assert_not_called()
