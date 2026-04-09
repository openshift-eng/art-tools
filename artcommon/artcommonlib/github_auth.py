"""
GitHub App authentication helpers for art-tools.

Primary API -- org-aware client with PAT fallback:
    from artcommonlib.github_auth import get_github_client_for_org
    client = get_github_client_for_org("openshift-eng")
    # auto-discovers installation ID for the org, caches clients per-org
    # falls back to GITHUB_TOKEN PAT if GITHUB_APP_ID is not set

Raw token (for non-PyGithub callers / shell scripts):
    from artcommonlib.github_auth import get_github_app_token
    token = get_github_app_token(app_id=12345, private_key=pem_string)

Git CLI auth (GIT_ASKPASS-based, for git clone / push / fetch):
    from artcommonlib.github_auth import get_github_git_auth_env
    env = get_github_git_auth_env(url="https://github.com/openshift-eng/repo")
    # Pass url= to auto-select the right App installation for multi-org setups
    # Returns {} when no credentials are available (SSH fallback)

Usage from shell (prints token to stdout):
    python -m artcommonlib.github_auth

Environment variables:
    GITHUB_APP_ID              - GitHub App numeric ID
    GITHUB_APP_PRIVATE_KEY     - PEM-encoded private key (inline string)
    GITHUB_APP_PRIVATE_KEY_PATH - Path to .pem file (used if GITHUB_APP_PRIVATE_KEY is not set)
    GITHUB_APP_INSTALLATION_ID - (optional) Installation ID; auto-detected if omitted

Per-org overrides (optional, take precedence over auto-detection):
    GITHUB_APP_INSTALLATION_ID_OPENSHIFT_ENG
    GITHUB_APP_INSTALLATION_ID_OPENSHIFT_PRIV
    GITHUB_APP_INSTALLATION_ID_OPENSHIFT
    GITHUB_APP_INSTALLATION_ID_OPENSHIFT_BOT
"""

import logging
import os
import re
import stat
import tempfile
import time
from pathlib import Path

from github import Auth, Github, GithubIntegration

LOGGER = logging.getLogger(__name__)


def get_github_app_token(
    app_id: int,
    private_key: str,
    installation_id: int | None = None,
) -> str:
    """
    Generate a short-lived GitHub installation access token from App credentials.

    The returned token can be used anywhere a GitHub PAT is accepted (e.g. GITHUB_TOKEN).
    Tokens expire after 1 hour. For long-running processes, prefer
    get_github_client_for_org() which auto-refreshes.

    :param app_id: GitHub App's numeric ID
    :param private_key: PEM-encoded RSA private key string
    :param installation_id: Target installation ID. If None, auto-detects (requires exactly one installation).
    :return: Installation access token string
    :raises ValueError: If no installations or multiple ambiguous installations are found
    """
    app_auth = Auth.AppAuth(app_id, private_key)
    if installation_id is None:
        installation_id = _auto_detect_single_installation(app_auth, app_id)

    gi = GithubIntegration(auth=app_auth)
    token_obj = gi.get_access_token(installation_id)
    LOGGER.info("Generated GitHub App installation token")
    return token_obj.token


def _auto_detect_single_installation(app_auth: Auth.AppAuth, app_id: int) -> int:
    """Auto-detect installation ID when exactly one installation exists.

    Refuses to guess when the app is installed in multiple orgs/repos.
    For multi-org apps, callers should use get_github_client_for_org() or
    supply an explicit installation_id.
    """
    gi = GithubIntegration(auth=app_auth)
    installations = list(gi.get_installations())
    if not installations:
        raise ValueError(
            f"No installations found for GitHub App {app_id}. "
            "Ensure the app is installed on at least one organization or repository."
        )
    if len(installations) > 1:
        orgs = [getattr(inst.account, "login", "unknown") for inst in installations]
        raise ValueError(
            f"GitHub App {app_id} has {len(installations)} installations ({orgs}). "
            "Supply an explicit installation_id, set GITHUB_APP_INSTALLATION_ID, "
            "or use get_github_client_for_org(org) instead."
        )
    installation_id = installations[0].id
    LOGGER.info("Auto-detected single GitHub App installation")
    return installation_id


def get_github_app_token_from_env() -> str:
    """
    Read GitHub App credentials from environment variables and return an installation token.

    Reads:
        GITHUB_APP_ID              (required)
        GITHUB_APP_PRIVATE_KEY     (PEM string, checked first)
        GITHUB_APP_PRIVATE_KEY_PATH (path to .pem file, fallback)
        GITHUB_APP_INSTALLATION_ID (optional)

    :return: Installation access token string
    :raises EnvironmentError: If required environment variables are missing
    """
    app_id, private_key, installation_id = _read_env_credentials()
    return get_github_app_token(app_id, private_key, installation_id)


def get_github_app_token_for_org(org: str) -> str:
    """
    Generate a short-lived GitHub installation access token for a specific org.

    Like get_github_app_token_from_env() but resolves the installation ID
    from the org name instead of requiring exactly one installation.
    If GITHUB_APP_INSTALLATION_ID is set it takes precedence over org-based
    resolution.

    :param org: GitHub organisation name (e.g. "openshift-priv")
    :return: Installation access token string
    :raises EnvironmentError: If required environment variables are missing
    :raises ValueError: If no installation matches the org
    """
    app_id, private_key, installation_id = _read_env_credentials()
    if not installation_id:
        installation_id = _resolve_installation_id(org, app_id, private_key)
    return get_github_app_token(app_id, private_key, installation_id)


_askpass_script_path: str | None = None

_git_token_cache: dict[str | None, tuple[str, float]] = {}
_GIT_TOKEN_TTL = 50 * 60  # 50 min (installation tokens last 60 min)


def get_github_git_auth_env(url: str | None = None, force_refresh: bool = False) -> dict[str, str]:
    """
    Return environment variables that configure git CLI to authenticate
    against GitHub using App tokens or a PAT via GIT_ASKPASS.

    Priority:
    1. GITHUB_APP_ID is set -> generate an App installation token
       (if *url* is provided, the GitHub org is extracted to select
       the correct installation; without a URL, falls back to
       GITHUB_APP_INSTALLATION_ID env var or PAT)
    2. GITHUB_TOKEN is set -> use it as the token
    3. Neither -> return {} (git falls through to SSH agent / other config)

    Tokens are cached per org for up to 50 minutes to avoid repeated API calls.

    :param url: Optional GitHub repository URL; used to resolve the correct
                App installation for multi-org setups.
    :param force_refresh: If True, regenerate the token even if cached.
                          Useful for pipelines that run longer than 1 hour.
    :return: Dict with GIT_ASKPASS, GIT_PASSWORD, GIT_TERMINAL_PROMPT; or {}
    """
    # Only inject GitHub credentials for GitHub URLs; non-GitHub remotes
    # (distgit, GitLab, etc.) keep their own auth mechanisms.
    if url and not _extract_org_from_github_url(url):
        return {}

    token = None
    cache_key = _extract_org_from_github_url(url) if url else None

    if not force_refresh and cache_key in _git_token_cache:
        cached_token, expiry = _git_token_cache[cache_key]
        if time.time() < expiry:
            token = cached_token

    if token is None and os.environ.get("GITHUB_APP_ID"):
        try:
            token = _generate_app_token_for_url(url)
            if cache_key is not None:
                _git_token_cache[cache_key] = (token, time.time() + _GIT_TOKEN_TTL)
            LOGGER.info("Using GitHub App token for git CLI auth")
        except (EnvironmentError, ValueError) as exc:
            LOGGER.warning("GitHub App token generation failed (%s); checking GITHUB_TOKEN", exc)

    if token is None:
        token = os.environ.get("GITHUB_TOKEN")
        if token:
            LOGGER.info("Using GITHUB_TOKEN (PAT) for git CLI auth")
    if not token:
        return {}

    script = _ensure_askpass_script()
    return {
        "GIT_ASKPASS": script,
        "GIT_PASSWORD": token,
        "GIT_TERMINAL_PROMPT": "0",
    }


def _generate_app_token_for_url(url: str | None) -> str:
    """Generate an App installation token, using the URL's org when available."""
    app_id, private_key, installation_id = _read_env_credentials()

    if installation_id:
        return get_github_app_token(app_id, private_key, installation_id)

    if url:
        org = _extract_org_from_github_url(url)
        if org:
            inst_id = _resolve_installation_id(org, app_id, private_key)
            return get_github_app_token(app_id, private_key, inst_id)

    return get_github_app_token(app_id, private_key)


_GITHUB_ORG_RE = re.compile(
    r"(?:(?:https?|ssh)://(?:[^@]+@)?github\.com(?::\d+)?/|git@github\.com[:/])([^/]+)",
    re.IGNORECASE,
)


def _extract_org_from_github_url(url: str) -> str | None:
    """Extract the organisation/owner from a GitHub HTTPS, SCP-style, or SSH URL.

    Recognised forms:
        https://github.com/owner/repo
        git@github.com:owner/repo
        ssh://git@github.com/owner/repo.git
        ssh://git@github.com:22/owner/repo.git
    """
    m = _GITHUB_ORG_RE.match(url)
    return m.group(1) if m else None


def _ensure_askpass_script() -> str:
    """Write the GIT_ASKPASS helper script once per process and return its path."""
    global _askpass_script_path
    if _askpass_script_path and os.path.exists(_askpass_script_path):
        return _askpass_script_path

    fd, path = tempfile.mkstemp(prefix="art-git-askpass-", suffix=".sh")
    with os.fdopen(fd, "w") as f:
        f.write('#!/bin/sh\ncase "$1" in\n  Username*) echo "x-access-token" ;;\n  *) echo "$GIT_PASSWORD" ;;\nesac\n')
    os.chmod(path, stat.S_IRWXU)
    _askpass_script_path = path
    return path


_installation_map: dict[str, int] = {}
_client_cache: dict[int, Github] = {}
_pat_client: Github | None = None


def get_github_client_for_org(org: str) -> Github:
    """
    Get an authenticated Github client for the given org.

    Tries GitHub App auth first, falls back to GITHUB_TOKEN PAT:
    1. If GITHUB_APP_ID is set, resolves the org's installation and returns
       an auto-refreshing App client (cached per installation_id).
    2. Otherwise, returns a PAT client from GITHUB_TOKEN (cached, shared across orgs).
    3. Raises EnvironmentError if neither is configured.

    :param org: GitHub organization name (e.g. "openshift-eng", "openshift-priv")
    :return: Authenticated Github client
    :raises EnvironmentError: If neither App credentials nor GITHUB_TOKEN are configured
    :raises ValueError: If App auth is configured but no installation matches the org
    """
    if not os.environ.get("GITHUB_APP_ID"):
        LOGGER.info("GITHUB_APP_ID not set; using GITHUB_TOKEN (PAT) for org '%s'", org)
        return _get_pat_client()

    LOGGER.info("Using GitHub App auth for org '%s'", org)
    app_id, private_key, _ = _read_env_credentials()
    installation_id = _resolve_installation_id(org, app_id, private_key)

    if installation_id not in _client_cache:
        app_auth = Auth.AppAuth(app_id, private_key)
        install_auth = app_auth.get_installation_auth(installation_id)
        _client_cache[installation_id] = Github(auth=install_auth)
        LOGGER.info("Created cached Github client for org '%s'", org)

    return _client_cache[installation_id]


def _resolve_installation_id(org: str, app_id: int, private_key: str) -> int:
    """Resolve an installation ID for the given org using env vars or auto-detection."""
    env_key = f"GITHUB_APP_INSTALLATION_ID_{org.upper().replace('-', '_')}"
    env_val = os.environ.get(env_key)
    if env_val:
        LOGGER.info("Using org-specific env var %s for org '%s'", env_key, org)
        return int(env_val)

    if not _installation_map:
        _build_installation_map(app_id, private_key)
    if org in _installation_map:
        LOGGER.info("Auto-detected installation for org '%s'", org)
        return _installation_map[org]

    fallback = os.environ.get("GITHUB_APP_INSTALLATION_ID")
    if fallback:
        LOGGER.warning("No installation found for org '%s'; falling back to GITHUB_APP_INSTALLATION_ID env var", org)
        return int(fallback)

    available = list(_installation_map.keys()) if _installation_map else []
    raise ValueError(
        f"No GitHub App installation found for org '{org}'. "
        f"Available orgs: {available}. "
        f"Set {env_key} or GITHUB_APP_INSTALLATION_ID to specify one explicitly."
    )


def _build_installation_map(app_id: int, private_key: str):
    """Query the GitHub App's installations and populate the org -> installation_id cache."""
    app_auth = Auth.AppAuth(app_id, private_key)
    gi = GithubIntegration(auth=app_auth)
    for inst in gi.get_installations():
        org_login = inst.account.login
        _installation_map[org_login] = inst.id
        LOGGER.debug("Discovered installation for org '%s'", org_login)
    LOGGER.info("Built installation map for orgs: %s", list(_installation_map.keys()))


def _get_pat_client() -> Github:
    """Return a cached Github client using GITHUB_TOKEN as a PAT."""
    global _pat_client
    if _pat_client is None:
        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            raise EnvironmentError(
                "Neither GitHub App credentials (GITHUB_APP_ID + private key) nor GITHUB_TOKEN are configured."
            )
        _pat_client = Github(auth=Auth.Token(token))
        LOGGER.info("Created PAT-based Github client from GITHUB_TOKEN")
    return _pat_client


def _read_env_credentials() -> tuple[int, str, int | None]:
    app_id_str = os.environ.get("GITHUB_APP_ID")
    if not app_id_str:
        raise EnvironmentError("GITHUB_APP_ID environment variable is not set")

    app_id = int(app_id_str)

    private_key = os.environ.get("GITHUB_APP_PRIVATE_KEY")
    if not private_key:
        key_path = os.environ.get("GITHUB_APP_PRIVATE_KEY_PATH")
        if not key_path:
            raise EnvironmentError("Either GITHUB_APP_PRIVATE_KEY or GITHUB_APP_PRIVATE_KEY_PATH must be set")
        private_key = Path(key_path).read_text()

    installation_id = None
    install_id_str = os.environ.get("GITHUB_APP_INSTALLATION_ID")
    if install_id_str:
        installation_id = int(install_id_str)

    return app_id, private_key, installation_id


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    try:
        token = get_github_app_token_from_env()
        print(token)
    except (EnvironmentError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
