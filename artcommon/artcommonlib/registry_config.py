"""
Container registry credential manager for oc / Podman-style tools.

RegistryConfig creates a temporary Docker config.json file containing only
the specified registry credentials, built by cherry-picking entries from
source auth files, kubeconfig-based ``oc registry login``, and/or explicit
username:password pairs.

Usage examples:

    # Extract specific registries from a source auth file
    with RegistryConfig(
        source_files=['/path/to/auth.json'],
        registries=['registry.redhat.io', 'quay.io/openshift-release-dev'],
    ) as auth_file:
        cmd.extend(['--registry-config', auth_file])

    # Use kubeconfig to obtain CI registry credentials
    with RegistryConfig(
        kubeconfig=os.environ.get('KUBECONFIG'),
        source_files=['/path/to/quay_auth.json'],
        registries=[
            'quay.io/openshift-release-dev',
            'registry.ci.openshift.org',
        ],
    ) as auth_file:
        cmd.extend(['--registry-config', auth_file])

    # Combine source file extraction with explicit credentials
    with RegistryConfig(
        source_files=['/path/to/auth.json'],
        registries=['registry.redhat.io'],
        credentials=[
            RegistryCredential('quay.io/qci', 'user', 'password'),
        ],
    ) as auth_file:
        cmd.extend(['--registry-config', auth_file])

    # Credentials only (no source files needed)
    with RegistryConfig(
        credentials=[
            RegistryCredential('quay.io/qci', 'user', 'password'),
        ],
    ) as auth_file:
        cmd.extend(['--registry-config', auth_file])

The auth_file can be passed to:
    - oc image mirror --registry-config=<auth_file>
    - oc image mirror -a <auth_file>
    - REGISTRY_AUTH_FILE=<auth_file>
"""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from artcommonlib import exectools
from artcommonlib.constants import REGISTRY_BREW, REGISTRY_REDHAT_IO

logger = logging.getLogger(__name__)


def _normalize_registry_path(path: str) -> str:
    """
    Normalize a registry path for consistent matching.

    Strips URI schemes, digest suffixes, and trailing slashes.
    Lowercases the host portion while preserving path case.

    Arg(s):
        path (str): Raw registry path.

    Return Value(s):
        str: Normalized registry path.

    Raises:
        ValueError: If path is empty after normalization.
    """
    path = path.strip()

    # Strip common URI schemes
    for prefix in ("docker://", "oci://", "https://", "http://"):
        if path.startswith(prefix):
            path = path[len(prefix) :]
            break

    # Strip digest suffix
    path = path.split("@", 1)[0].rstrip("/")

    if not path:
        raise ValueError("empty registry reference after normalization")

    # Lowercase host, preserve path case
    if "/" in path:
        host, rest = path.split("/", 1)
        return f"{host.lower()}/{rest}"
    return path.lower()


@dataclass
class RegistryCredential:
    """
    Explicit username/password credentials for a single registry.

    Arg(s):
        registry (str): Registry path (e.g., 'quay.io/openshift/ci').
        username (str): Username for authentication.
        password (str): Password for authentication.
    """

    registry: str
    username: str
    password: str

    def __post_init__(self):
        if not self.registry or not self.registry.strip():
            raise ValueError("RegistryCredential registry cannot be empty")
        if not self.username or not self.username.strip():
            raise ValueError(f"RegistryCredential username cannot be empty for registry '{self.registry}'")
        if not self.password:
            raise ValueError(f"RegistryCredential password cannot be empty for registry '{self.registry}'")
        self.registry = _normalize_registry_path(self.registry)


class RegistryConfig:
    """
    Context manager that builds a temporary Docker auth config.json.

    Constructs a minimal auth file containing only the requested registries,
    sourced from existing auth files, kubeconfig-based ``oc registry login``,
    and/or explicit credentials.

    When a registry appears in both source files and credentials, the
    explicit credential takes precedence.  When a registry appears in
    multiple source files, the first file wins.

    Arg(s):
        kubeconfig (str | None): Path to a kubeconfig file. When provided,
            ``oc --kubeconfig=<path> registry login`` is run during ``__enter__``
            to obtain CI registry credentials, which are added as an additional
            source.
        source_files (list[str] | None): Paths to existing auth.json files to extract from.
        registries (list[str] | None): Registry paths to cherry-pick from source_files / kubeconfig.
        credentials (list[RegistryCredential] | None): Explicit username/password credentials.
    """

    def __init__(
        self,
        source_files: Optional[list[str]] = None,
        registries: Optional[list[str]] = None,
        credentials: Optional[list[RegistryCredential]] = None,
        kubeconfig: Optional[str] = None,
    ) -> None:
        """
        Initialize the RegistryConfig context manager.

        Arg(s):
            source_files (list[str] | None): Paths to existing auth.json files.
            registries (list[str] | None): Registry paths to extract from source_files / kubeconfig.
            credentials (list[RegistryCredential] | None): Explicit credentials.
            kubeconfig (str | None): Path to a kubeconfig file for ``oc registry login``.

        Raises:
            ValueError: If no registries or credentials are provided, or if
                        registries are specified without source_files or kubeconfig.
        """
        self._source_files = list(source_files) if source_files else []
        self._registries = [_normalize_registry_path(r) for r in registries] if registries else []
        self._credentials = list(credentials) if credentials else []
        self._kubeconfig = kubeconfig
        self._temp_auth_file: Optional[Path] = None
        self._merged_auths: dict[str, dict[str, str]] = {}

        # Must have at least one registry or credential
        if not self._registries and not self._credentials:
            raise ValueError("RegistryConfig requires at least one registry or credential")

        # Can't extract registries without a credential source
        if self._registries and not self._source_files and not self._kubeconfig:
            raise ValueError("source_files or kubeconfig must be provided when registries are specified")

    def __enter__(self) -> str:
        """
        Enter the context: resolve credentials and write a temporary auth file.

        Return Value(s):
            str: Path to the temporary auth file containing merged credentials.

        Raises:
            ValueError: If a requested registry is not found in any source file.
            FileNotFoundError: If a source file does not exist.
            RuntimeError: If ``oc registry login`` fails.
        """
        self._resolve_and_write()
        return str(self._temp_auth_file)

    def _run_oc_registry_login(self) -> dict[str, dict]:
        """
        Run ``oc --kubeconfig=<path> registry login`` to obtain CI registry
        credentials. The temp file used by ``oc registry login`` is created,
        read, and removed entirely within this method.

        Return Value(s):
            dict: The ``auths`` section from the login output (registry -> auth entry).

        Raises:
            RuntimeError: If ``oc registry login`` fails.
            ValueError: If the login output has invalid JSON or format.
        """
        fd, temp_path = tempfile.mkstemp(prefix="oc_login_auth_", suffix=".json", text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"auths": {}}, f)

            cmd = f"oc --kubeconfig {self._kubeconfig} registry login --to={temp_path}"
            rc, _, stderr = exectools.cmd_gather(cmd)
            if rc != 0:
                raise RuntimeError(f"Failed to run 'oc registry login' with kubeconfig '{self._kubeconfig}': {stderr}")

            with open(temp_path, encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict) or "auths" not in data:
                raise ValueError("oc registry login output missing 'auths' key. Expected Docker config.json format.")

            logger.info("Obtained CI registry credentials via kubeconfig '%s'", self._kubeconfig)
            return data["auths"]
        finally:
            try:
                os.unlink(temp_path)
                logger.debug("Removed oc login temp file %s", temp_path)
            except OSError as err:
                logger.warning("Could not remove %s: %s", temp_path, err)

    def _resolve_and_write(self) -> None:
        """
        Core credential resolution logic: optionally run ``oc registry login``
        to obtain CI credentials, load source files, match registries, apply
        explicit credentials, and write the merged auth file.
        """
        merged: dict[str, dict[str, str]] = {}

        # Registry aliases: registries that share authentication realms
        # When the requested registry is not found, try these fallbacks
        registry_aliases = {
            REGISTRY_BREW: REGISTRY_REDHAT_IO,
        }

        # Step 0: if kubeconfig is set, add CI credentials directly to merged
        if self._kubeconfig:
            oc_auths = self._run_oc_registry_login()
            for key, value in oc_auths.items():
                try:
                    normalized = _normalize_registry_path(key)
                except ValueError:
                    continue
                if normalized in self._registries:
                    merged[normalized] = value
                    logger.info("Resolved credentials for '%s' from kubeconfig", normalized)

        # Step 1: resolve remaining registries from source files
        source_auths = self._load_source_files()
        for registry in self._registries:
            if registry in merged:
                logger.debug("Registry '%s' already resolved, skipping duplicate", registry)
                continue

            auth_value = self._find_in_sources(registry, source_auths)

            # If not found, try fallback for known aliases
            if auth_value is None and registry in registry_aliases:
                fallback_registry = registry_aliases[registry]
                auth_value = self._find_in_sources(fallback_registry, source_auths)
                if auth_value:
                    logger.info(
                        "Registry '%s' not found, using credentials from '%s' (shared auth realm)",
                        registry,
                        fallback_registry,
                    )

            if auth_value is None:
                available_keys: list[str] = []
                for file_auths in source_auths.values():
                    available_keys.extend(file_auths.keys())
                raise ValueError(
                    f"Registry '{registry}' not found in any source file. "
                    f"Available registries: {sorted(set(available_keys))}"
                )

            merged[registry] = {"auth": auth_value}
            logger.info("Resolved credentials for '%s' from source file", registry)

        # Step 2: add/override with explicit credentials (first credential wins
        # for duplicates; credentials override source-file entries)
        seen_cred_registries: set[str] = set()
        for cred in self._credentials:
            if cred.registry in seen_cred_registries:
                logger.debug("Duplicate credential for '%s', skipping", cred.registry)
                continue
            seen_cred_registries.add(cred.registry)

            if cred.registry in merged:
                logger.debug(
                    "Credential for '%s' overrides source file entry",
                    cred.registry,
                )

            auth_str = base64.b64encode(f"{cred.username}:{cred.password}".encode()).decode()
            merged[cred.registry] = {"auth": auth_str}
            logger.info(
                "Resolved credentials for '%s' from explicit credential",
                cred.registry,
            )

        self._merged_auths = merged
        self._write_temp_file()

    def _load_source_files(self) -> dict[str, dict[str, dict]]:
        """
        Load all source auth files and return their "auths" sections.

        Return Value(s):
            dict: Mapping of file path to its auths dict.

        Raises:
            FileNotFoundError: If a source file does not exist.
            ValueError: If a source file has invalid JSON or format.
        """
        result: dict[str, dict] = {}
        for file_path in self._source_files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Source auth file not found: {file_path}")

            with open(file_path, encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in source file {file_path}: {e}") from e

            if not isinstance(data, dict) or "auths" not in data:
                raise ValueError(f"Source file {file_path} missing 'auths' key. Expected Docker config.json format.")

            result[file_path] = data["auths"]
            logger.debug(
                "Loaded %d registry entries from %s",
                len(data["auths"]),
                file_path,
            )

        return result

    def _find_in_sources(self, registry: str, source_auths: dict[str, dict]) -> Optional[str]:
        """
        Find a registry's auth value across the loaded source files.

        Source files are searched in order; first match wins.  Keys in each
        source file are normalized before comparison so that host casing and
        URI-scheme differences do not prevent a match.

        Arg(s):
            registry (str): Normalized registry path to look up.
            source_auths (dict): Loaded source file auths (file path -> auths dict).

        Return Value(s):
            str | None: Base64 auth string if found, None otherwise.
        """
        for file_path, auths in source_auths.items():
            for key, value in auths.items():
                try:
                    normalized_key = _normalize_registry_path(key)
                except ValueError:
                    continue
                if normalized_key == registry:
                    auth = value.get("auth")
                    if auth:
                        logger.debug(
                            "Found '%s' (as '%s') in %s",
                            registry,
                            key,
                            file_path,
                        )
                        return auth
        return None

    def _write_temp_file(self) -> None:
        """
        Write merged credentials to a temporary file.

        The file is created with restrictive permissions (0600) via mkstemp.
        """
        try:
            fd, temp_path = tempfile.mkstemp(prefix="registry_config_", suffix=".json", text=True)
            self._temp_auth_file = Path(temp_path)

            merged_config = {"auths": self._merged_auths}
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(merged_config, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

            logger.info(
                "Wrote %d credentials to %s",
                len(self._merged_auths),
                self._temp_auth_file,
            )
        except Exception:
            if self._temp_auth_file and self._temp_auth_file.exists():
                try:
                    self._temp_auth_file.unlink()
                except OSError:
                    pass
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        Clean up the merged auth temp file.
        """
        self._merged_auths = {}
        path = self._temp_auth_file
        self._temp_auth_file = None
        if path and path.exists():
            try:
                path.unlink()
                logger.debug("Removed temp file %s", path)
            except OSError as err:
                logger.warning("Could not remove %s: %s", path, err)
        return False

    def get_registries(self) -> list[str]:
        """
        Return the list of registries in the merged config.

        Only valid inside the context (after __enter__).

        Return Value(s):
            list[str]: Registry paths with resolved credentials.
        """
        return list(self._merged_auths.keys())

    def has_credential_for(self, registry: str) -> bool:
        """
        Check if the merged config has credentials for a registry (exact match).

        Only valid inside the context (after __enter__).

        Arg(s):
            registry (str): Registry path to check.

        Return Value(s):
            bool: True if credentials exist for the registry.
        """
        return registry in self._merged_auths
