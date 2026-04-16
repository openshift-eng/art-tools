import os
from unittest.mock import MagicMock, patch

import artcommonlib.github_auth as gh_auth
import pytest
from artcommonlib.github_auth import (
    _extract_org_from_github_url,
    get_github_app_token,
    get_github_app_token_for_org,
    get_github_app_token_from_env,
    get_github_client_for_org,
    get_github_git_auth_env,
    get_github_git_pat_env,
)

FAKE_PEM = "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"


class TestGetGithubAppToken:
    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_with_explicit_installation_id(self, mock_app_auth_cls, mock_gi_cls):
        mock_token_obj = MagicMock()
        mock_token_obj.token = "ghs_fake_token_123"
        mock_gi_cls.return_value.get_access_token.return_value = mock_token_obj

        result = get_github_app_token(app_id=12345, private_key=FAKE_PEM, installation_id=99)

        mock_app_auth_cls.assert_called_once_with(12345, FAKE_PEM)
        mock_gi_cls.return_value.get_access_token.assert_called_once_with(99)
        assert result == "ghs_fake_token_123"

    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_auto_detect_installation_id(self, mock_app_auth_cls, mock_gi_cls):
        mock_install = MagicMock()
        mock_install.id = 42
        mock_gi_cls.return_value.get_installations.return_value = [mock_install]
        mock_token_obj = MagicMock()
        mock_token_obj.token = "ghs_auto_detected"
        mock_gi_cls.return_value.get_access_token.return_value = mock_token_obj

        result = get_github_app_token(app_id=12345, private_key=FAKE_PEM)

        mock_gi_cls.return_value.get_access_token.assert_called_once_with(42)
        assert result == "ghs_auto_detected"

    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_no_installations_raises(self, mock_app_auth_cls, mock_gi_cls):
        mock_gi_cls.return_value.get_installations.return_value = []

        with pytest.raises(ValueError, match="No installations found"):
            get_github_app_token(app_id=12345, private_key=FAKE_PEM)

    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_multiple_installations_raises(self, mock_app_auth_cls, mock_gi_cls):
        mock_gi_cls.return_value.get_installations.return_value = [
            _make_installation("org-a", 10),
            _make_installation("org-b", 20),
        ]

        with pytest.raises(ValueError, match="has 2 installations"):
            get_github_app_token(app_id=12345, private_key=FAKE_PEM)


class TestGetGithubAppTokenFromEnv:
    @patch("artcommonlib.github_auth.get_github_app_token", return_value="ghs_env_token")
    def test_inline_private_key(self, mock_get_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.setenv("GITHUB_APP_INSTALLATION_ID", "99")

        result = get_github_app_token_from_env()

        mock_get_token.assert_called_once_with(12345, FAKE_PEM, 99)
        assert result == "ghs_env_token"

    @patch("artcommonlib.github_auth.get_github_app_token", return_value="ghs_file_token")
    def test_private_key_from_file(self, mock_get_token, monkeypatch, tmp_path):
        key_file = tmp_path / "app.pem"
        key_file.write_text(FAKE_PEM)

        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY", raising=False)
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY_PATH", str(key_file))
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

        result = get_github_app_token_from_env()

        mock_get_token.assert_called_once_with(12345, FAKE_PEM, None)
        assert result == "ghs_file_token"

    def test_missing_app_id_raises(self, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)

        with pytest.raises(EnvironmentError, match="GITHUB_APP_ID"):
            get_github_app_token_from_env()

    def test_missing_private_key_raises(self, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY", raising=False)
        monkeypatch.delenv("GITHUB_APP_PRIVATE_KEY_PATH", raising=False)

        with pytest.raises(EnvironmentError, match="GITHUB_APP_PRIVATE_KEY"):
            get_github_app_token_from_env()


class TestGetGithubAppTokenForOrg:
    """Tests for the org-aware token generation function."""

    @pytest.fixture(autouse=True)
    def _clear_caches(self):
        gh_auth._installation_map.clear()
        yield
        gh_auth._installation_map.clear()

    @patch("artcommonlib.github_auth.get_github_app_token", return_value="ghs_org_token")
    @patch("artcommonlib.github_auth._resolve_installation_id", return_value=42)
    def test_resolves_org_when_no_explicit_id(self, mock_resolve, mock_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

        result = get_github_app_token_for_org("openshift-priv")

        mock_resolve.assert_called_once_with("openshift-priv", 100, FAKE_PEM)
        mock_token.assert_called_once_with(100, FAKE_PEM, 42)
        assert result == "ghs_org_token"

    @patch("artcommonlib.github_auth.get_github_app_token", return_value="ghs_explicit_token")
    def test_explicit_installation_id_takes_precedence(self, mock_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.setenv("GITHUB_APP_INSTALLATION_ID", "777")

        result = get_github_app_token_for_org("openshift-priv")

        mock_token.assert_called_once_with(100, FAKE_PEM, 777)
        assert result == "ghs_explicit_token"

    def test_missing_credentials_raises(self, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)

        with pytest.raises(EnvironmentError, match="GITHUB_APP_ID"):
            get_github_app_token_for_org("openshift-priv")


def _make_installation(org_login: str, install_id: int) -> MagicMock:
    inst = MagicMock()
    inst.id = install_id
    inst.account.login = org_login
    return inst


class TestGetGithubClientForOrg:
    """Tests for the org-aware Github client retrieval."""

    @pytest.fixture(autouse=True)
    def _clear_caches(self):
        """Reset module-level caches between tests."""
        gh_auth._installation_map.clear()
        gh_auth._client_cache.clear()
        gh_auth._pat_client = None
        yield
        gh_auth._installation_map.clear()
        gh_auth._client_cache.clear()
        gh_auth._pat_client = None

    @patch("artcommonlib.github_auth.Github")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_env_override_takes_precedence(self, mock_app_auth_cls, mock_github_cls, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.setenv("GITHUB_APP_INSTALLATION_ID_OPENSHIFT_ENG", "777")
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

        mock_app_auth = mock_app_auth_cls.return_value
        mock_install_auth = MagicMock()
        mock_app_auth.get_installation_auth.return_value = mock_install_auth
        mock_client = MagicMock()
        mock_github_cls.return_value = mock_client

        result = get_github_client_for_org("openshift-eng")

        mock_app_auth.get_installation_auth.assert_called_once_with(777)
        mock_github_cls.assert_called_once_with(auth=mock_install_auth)
        assert result is mock_client

    @patch("artcommonlib.github_auth.Github")
    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_auto_detect_from_api(self, mock_app_auth_cls, mock_gi_cls, mock_github_cls, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID_OPENSHIFT_ENG", raising=False)
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

        mock_gi_cls.return_value.get_installations.return_value = [
            _make_installation("openshift-eng", 10),
            _make_installation("openshift-priv", 20),
            _make_installation("openshift", 30),
        ]

        mock_app_auth = mock_app_auth_cls.return_value
        mock_install_auth = MagicMock()
        mock_app_auth.get_installation_auth.return_value = mock_install_auth
        mock_client = MagicMock()
        mock_github_cls.return_value = mock_client

        result = get_github_client_for_org("openshift-eng")

        mock_app_auth.get_installation_auth.assert_called_once_with(10)
        assert result is mock_client

    @patch("artcommonlib.github_auth.Github")
    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_fallback_to_generic_env_var(self, mock_app_auth_cls, mock_gi_cls, mock_github_cls, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID_UNKNOWN_ORG", raising=False)
        monkeypatch.setenv("GITHUB_APP_INSTALLATION_ID", "999")

        mock_gi_cls.return_value.get_installations.return_value = [
            _make_installation("openshift-eng", 10),
        ]

        mock_app_auth = mock_app_auth_cls.return_value
        mock_install_auth = MagicMock()
        mock_app_auth.get_installation_auth.return_value = mock_install_auth
        mock_client = MagicMock()
        mock_github_cls.return_value = mock_client

        result = get_github_client_for_org("unknown-org")

        mock_app_auth.get_installation_auth.assert_called_once_with(999)
        assert result is mock_client

    @patch("artcommonlib.github_auth.Github")
    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_caches_clients_per_installation(self, mock_app_auth_cls, mock_gi_cls, mock_github_cls, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

        mock_gi_cls.return_value.get_installations.return_value = [
            _make_installation("openshift-eng", 10),
            _make_installation("openshift-priv", 20),
        ]

        mock_app_auth = mock_app_auth_cls.return_value
        mock_install_auth = MagicMock()
        mock_app_auth.get_installation_auth.return_value = mock_install_auth

        client1 = get_github_client_for_org("openshift-eng")
        client2 = get_github_client_for_org("openshift-eng")

        assert client1 is client2
        assert mock_github_cls.call_count == 1

    @patch("artcommonlib.github_auth.GithubIntegration")
    @patch("artcommonlib.github_auth.Auth.AppAuth")
    def test_unknown_org_no_fallback_raises(self, mock_app_auth_cls, mock_gi_cls, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.delenv("GITHUB_APP_INSTALLATION_ID", raising=False)

        mock_gi_cls.return_value.get_installations.return_value = [
            _make_installation("openshift-eng", 10),
        ]

        with pytest.raises(ValueError, match="No GitHub App installation found for org 'totally-unknown'"):
            get_github_client_for_org("totally-unknown")

    @patch("artcommonlib.github_auth.Github")
    def test_pat_fallback_when_no_app_id(self, mock_github_cls, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_my_pat_token")

        mock_client = MagicMock()
        mock_github_cls.return_value = mock_client

        result = get_github_client_for_org("openshift-eng")

        mock_github_cls.assert_called_once()
        assert result is mock_client

    @patch("artcommonlib.github_auth.Github")
    def test_pat_fallback_caches_client(self, mock_github_cls, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_my_pat_token")

        mock_client = MagicMock()
        mock_github_cls.return_value = mock_client

        client1 = get_github_client_for_org("openshift-eng")
        client2 = get_github_client_for_org("openshift-priv")

        assert client1 is client2
        assert mock_github_cls.call_count == 1

    def test_pat_fallback_no_token_raises(self, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        with pytest.raises(EnvironmentError, match="Neither GitHub App credentials.*nor GITHUB_TOKEN"):
            get_github_client_for_org("openshift-eng")


class TestExtractOrgFromGithubUrl:
    """Tests for _extract_org_from_github_url helper."""

    def test_https_url(self):
        assert _extract_org_from_github_url("https://github.com/openshift-eng/ocp-build-data") == "openshift-eng"

    def test_https_url_with_git_suffix(self):
        assert _extract_org_from_github_url("https://github.com/openshift-priv/kubernetes.git") == "openshift-priv"

    def test_ssh_url(self):
        assert _extract_org_from_github_url("git@github.com:openshift/origin.git") == "openshift"

    def test_non_github_url_returns_none(self):
        assert _extract_org_from_github_url("https://gitlab.com/some/repo") is None

    def test_empty_string_returns_none(self):
        assert _extract_org_from_github_url("") is None


class TestGetGithubGitAuthEnv:
    """Tests for the GIT_ASKPASS-based git CLI auth helper."""

    @pytest.fixture(autouse=True)
    def _clear_askpass_cache(self):
        """Reset the cached askpass script path between tests."""
        gh_auth._askpass_script_path = None
        gh_auth._git_token_cache.clear()
        yield
        # Clean up temp file if created
        if gh_auth._askpass_script_path and os.path.exists(gh_auth._askpass_script_path):
            os.unlink(gh_auth._askpass_script_path)
        gh_auth._askpass_script_path = None
        gh_auth._git_token_cache.clear()

    @pytest.fixture(autouse=True)
    def _clear_installation_map(self):
        """Reset the installation map cache between tests."""
        gh_auth._installation_map.clear()
        yield
        gh_auth._installation_map.clear()

    @patch("artcommonlib.github_auth._generate_app_token_for_url", return_value="ghs_app_token_xyz")
    def test_returns_auth_env_with_app_creds(self, mock_gen_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")

        result = get_github_git_auth_env()

        mock_gen_token.assert_called_once_with(None)
        assert "GIT_ASKPASS" in result
        assert result["GIT_PASSWORD"] == "ghs_app_token_xyz"
        assert result["GIT_TERMINAL_PROMPT"] == "0"
        assert os.path.isfile(result["GIT_ASKPASS"])
        assert os.access(result["GIT_ASKPASS"], os.X_OK)

    def test_returns_auth_env_with_pat(self, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_my_pat")

        result = get_github_git_auth_env()

        assert "GIT_ASKPASS" in result
        assert result["GIT_PASSWORD"] == "ghp_my_pat"
        assert result["GIT_TERMINAL_PROMPT"] == "0"

    def test_returns_empty_dict_when_no_creds(self, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        result = get_github_git_auth_env()

        assert result == {}

    @patch("artcommonlib.github_auth._generate_app_token_for_url", return_value="ghs_token")
    def test_app_creds_take_precedence_over_pat(self, mock_gen_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_should_not_use")

        result = get_github_git_auth_env()

        assert result["GIT_PASSWORD"] == "ghs_token"

    @patch("artcommonlib.github_auth._generate_app_token_for_url", side_effect=EnvironmentError("missing key"))
    def test_falls_back_to_pat_when_app_auth_fails(self, mock_gen_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_fallback")

        result = get_github_git_auth_env()

        assert result["GIT_PASSWORD"] == "ghp_fallback"

    @patch("artcommonlib.github_auth._generate_app_token_for_url", return_value="ghs_cached")
    def test_askpass_script_is_cached(self, mock_gen_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")

        result1 = get_github_git_auth_env()
        result2 = get_github_git_auth_env()

        assert result1["GIT_ASKPASS"] == result2["GIT_ASKPASS"]

    @patch("artcommonlib.github_auth._generate_app_token_for_url", return_value="ghs_url_token")
    def test_passes_url_to_token_generator(self, mock_gen_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")

        result = get_github_git_auth_env(url="https://github.com/openshift-eng/ocp-build-data")

        mock_gen_token.assert_called_once_with("https://github.com/openshift-eng/ocp-build-data")
        assert result["GIT_PASSWORD"] == "ghs_url_token"

    def test_non_github_url_returns_empty(self, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", FAKE_PEM)
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_should_not_leak")

        assert get_github_git_auth_env(url="https://pkgs.devel.redhat.com/rpms/kernel") == {}
        assert get_github_git_auth_env(url="https://gitlab.cee.redhat.com/some/repo") == {}
        assert get_github_git_auth_env(url="git@gitlab.com:org/repo.git") == {}

    @patch("artcommonlib.github_auth._generate_app_token_for_url", return_value="ghs_generic")
    def test_none_url_still_generates_credentials(self, mock_gen_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")

        result = get_github_git_auth_env(url=None)

        mock_gen_token.assert_called_once_with(None)
        assert result["GIT_PASSWORD"] == "ghs_generic"

    @patch("artcommonlib.github_auth.get_github_app_token", return_value="ghs_resolved")
    @patch("artcommonlib.github_auth._resolve_installation_id", return_value=42)
    @patch("artcommonlib.github_auth._read_env_credentials", return_value=(100, FAKE_PEM, None))
    def test_url_resolves_org_installation(self, mock_creds, mock_resolve, mock_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")

        result = get_github_git_auth_env(url="https://github.com/openshift-eng/ocp-build-data")

        mock_resolve.assert_called_once_with("openshift-eng", 100, FAKE_PEM)
        mock_token.assert_called_once_with(100, FAKE_PEM, 42)
        assert result["GIT_PASSWORD"] == "ghs_resolved"

    @patch("artcommonlib.github_auth.get_github_app_token", return_value="ghs_explicit")
    @patch("artcommonlib.github_auth._read_env_credentials", return_value=(100, FAKE_PEM, 777))
    def test_explicit_installation_id_overrides_url(self, mock_creds, mock_token, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "100")

        result = get_github_git_auth_env(url="https://github.com/openshift-eng/ocp-build-data")

        mock_token.assert_called_once_with(100, FAKE_PEM, 777)
        assert result["GIT_PASSWORD"] == "ghs_explicit"


class TestGetGithubGitPatEnv:
    """Tests for the PAT-only GIT_ASKPASS auth helper."""

    @pytest.fixture(autouse=True)
    def _clear_askpass_cache(self):
        gh_auth._askpass_script_path = None
        yield
        if gh_auth._askpass_script_path and os.path.exists(gh_auth._askpass_script_path):
            os.unlink(gh_auth._askpass_script_path)
        gh_auth._askpass_script_path = None

    def test_returns_pat_env(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_my_pat")

        result = get_github_git_pat_env()

        assert "GIT_ASKPASS" in result
        assert result["GIT_PASSWORD"] == "ghp_my_pat"
        assert result["GIT_TERMINAL_PROMPT"] == "0"

    def test_ignores_app_credentials(self, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_pat_only")

        result = get_github_git_pat_env()

        assert result["GIT_PASSWORD"] == "ghp_pat_only"

    def test_returns_empty_when_no_pat(self, monkeypatch):
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        result = get_github_git_pat_env()

        assert result == {}
