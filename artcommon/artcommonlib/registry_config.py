"""Container registry credentials for ``oc`` / Podman-style tools.

:class:`RegistryConfig` creates a temporary Docker ``config.json`` file
(``{"auths": {...}}``) for use with OpenShift CLI registry operations.

**``oc image mirror`` (and related ``oc image`` commands)** accept
``-a PATH`` or ``--registry-config=PATH`` with this file.

**Usage:**

Specify explicit credentials from trusted credential stores::

    with RegistryConfig(
        registries=[
            Registry('quay.io/openshift-release-dev',
                    auth_file=os.getenv('QUAY_AUTH_FILE')),
            Registry('registry.redhat.io',
                    username_password=os.getenv('RH_REGISTRY_CREDS')),
        ]
    ) as auth_file:
        # auth_file is a temporary file with merged credentials
        cmd.extend(['--registry-auth', auth_file])
        ...

Using common registries helper::

    with RegistryConfig(
        registries=[
            Registry('quay.io/special', auth_file=special_auth),  # Override
            *COMMON_READ_ONLY_REGISTRIES(),  # Common defaults
        ]
    ) as auth_file:
        ...
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

logger = logging.getLogger(__name__)


@dataclass
class Registry:
    """
    Represents a single registry with one authentication source.

    Exactly one of auth_file, username_password, or base64_auth must be provided.

    :param path: Registry path (e.g., 'quay.io' or 'quay.io/openshift-release-dev')
    :param auth_file: Path to an auth.json file containing credentials for this registry
    :param username_password: Plain text "username:password" (will be base64 encoded)
    :param base64_auth: Base64-encoded "username:password" string
    """

    path: str
    auth_file: Optional[str] = None
    username_password: Optional[str] = None
    base64_auth: Optional[str] = None

    def __post_init__(self):
        # Validate exactly one auth source
        sources = [self.auth_file, self.username_password, self.base64_auth]
        if sum(bool(s) for s in sources) != 1:
            raise ValueError(
                f"Registry '{self.path}' requires exactly one of: auth_file, username_password, or base64_auth"
            )

        # Validate path is not empty
        if not self.path or not self.path.strip():
            raise ValueError("Registry path cannot be empty")

        # Validate auth_file path is not empty if provided
        if self.auth_file is not None and not self.auth_file.strip():
            raise ValueError(f"auth_file path cannot be empty for registry '{self.path}'")

        # Normalize the path (lowercase host, preserve path case)
        self.path = self._normalize_path(self.path)

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Normalize registry path: lowercase host, preserve path case, strip schemes"""
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


class RegistryConfig:
    """Build a merged ``auths`` JSON file for ``oc``/Podman pull secrets.

    The yielded path can be passed to ``oc image mirror`` as ``--registry-config``
    (short: ``-a``), or exported as ``REGISTRY_AUTH_FILE``.

    **Usage:**

    Explicit credentials from trusted sources::

        with RegistryConfig(
            registries=[
                Registry('quay.io/openshift-release-dev',
                        auth_file=os.getenv('QUAY_AUTH_FILE')),
                Registry('registry.redhat.io',
                        username_password=os.getenv('RH_CREDS')),
            ]
        ) as auth_file:
            cmd.extend(['--registry-auth', auth_file])
            ...

    Composing with common configs (first source wins)::

        with RegistryConfig(
            registries=[
                Registry('quay.io/special', auth_file=special_auth),  # Override
                *COMMON_READ_ONLY_REGISTRIES(),  # Common defaults
            ]
        ) as auth_file:
            ...
    """

    def __init__(self, registries: list[Registry]) -> None:
        """
        Initialize the RegistryConfig context manager.

        :param registries: List of Registry objects with explicit auth sources
        """
        if not registries:
            raise ValueError("RegistryConfig requires at least one registry")

        self.registries = list(registries)
        self.temp_auth_file: Optional[Path] = None
        self.merged_auths: dict[str, dict[str, str]] = {}

    def __enter__(self) -> str:
        """
        Enter the context manager: resolve credentials and write to temporary file.

        :return: Path to the temporary auth file containing merged credentials
        :raises ValueError: If any registry cannot be resolved
        """
        resolved: dict[str, dict[str, str]] = {}

        for registry in self.registries:
            registry_key = registry.path

            # First source wins - skip if already resolved
            if registry_key in resolved:
                logger.debug("RegistryConfig: registry '%s' already resolved by earlier source, skipping", registry_key)
                continue

            # Resolve auth for this registry
            auth_str = self._resolve_auth(registry)
            if auth_str:
                resolved[registry_key] = {"auth": auth_str}
                logger.info("RegistryConfig: resolved credentials for '%s'", registry_key)
            else:
                raise ValueError(f"RegistryConfig: could not resolve credentials for registry '{registry_key}'")

        self.merged_auths = resolved
        self._write_temp_file()
        return str(self.temp_auth_file)

    def _resolve_auth(self, registry: Registry) -> Optional[str]:
        """
        Resolve a single Registry to a base64 auth string.

        :param registry: The Registry to resolve
        :return: Base64 encoded auth string, or None if not found
        """
        registry_key = registry.path

        # Explicit base64 string
        if registry.base64_auth:
            auth_str = registry.base64_auth.strip()
            if not auth_str:
                raise ValueError(f"Empty base64_auth for '{registry_key}'")
            logger.debug("RegistryConfig: using explicit base64 auth for '%s'", registry_key)
            return auth_str

        # Plain username:password
        elif registry.username_password:
            plain = registry.username_password.strip()
            if not plain:
                raise ValueError(f"Empty username_password for '{registry_key}'")
            logger.debug("RegistryConfig: using username:password for '%s'", registry_key)
            return base64.b64encode(plain.encode()).decode()

        # Auth file
        elif registry.auth_file:
            file_path = registry.auth_file.strip()
            if not file_path:
                raise ValueError(f"Empty auth_file path for '{registry_key}'")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Auth file not found: {file_path}")

            logger.debug("RegistryConfig: reading auth file %s for '%s'", file_path, registry_key)

            with open(file_path, encoding="utf-8") as f:
                file_config = json.load(f)
                file_auths = file_config.get("auths", {})

            # Exact match required
            if registry_key in file_auths:
                logger.debug("RegistryConfig: found exact match for '%s' in %s", registry_key, file_path)
                return file_auths[registry_key].get("auth")
            else:
                raise ValueError(
                    f"RegistryConfig: registry '{registry_key}' not found in auth file {file_path}. "
                    f"Available keys: {list(file_auths.keys())}"
                )

        return None

    def _write_temp_file(self):
        """Write merged credentials to temporary file in current directory"""
        try:
            fd, temp_path = tempfile.mkstemp(prefix="registry_config_", suffix=".json", text=True)

            self.temp_auth_file = Path(temp_path)

            merged_config = {"auths": self.merged_auths}
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(merged_config, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

            logger.info("RegistryConfig: wrote %d credentials to %s", len(self.merged_auths), self.temp_auth_file)
            logger.debug("RegistryConfig: enabled registries: %s", list(self.merged_auths.keys()))

        except Exception:
            if self.temp_auth_file and self.temp_auth_file.exists():
                try:
                    self.temp_auth_file.unlink()
                except OSError:
                    pass
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Clean up temporary auth file"""
        self.merged_auths = {}
        path = self.temp_auth_file
        self.temp_auth_file = None
        if path and path.exists():
            try:
                path.unlink()
                logger.debug("RegistryConfig: removed temp file %s", path)
            except OSError as err:
                logger.warning("RegistryConfig: could not remove %s: %s", path, err)
        return False

    # Public helper methods

    def get_registries(self) -> list[str]:
        """Return auth keys in the merged config (only valid inside the context after ``__enter__``)."""
        return list(self.merged_auths.keys())

    def has_credential_for(self, registry: str) -> bool:
        """True if merged auths include credentials for *registry* (exact match only)."""
        return registry in self.merged_auths


# Common registry configurations for reuse


def COMMON_READ_ONLY_REGISTRIES() -> list[Registry]:
    """
    Common read-only registries used across ART tools.

    These use credentials from Jenkins-provided environment variables.
    Override specific registries by placing them earlier in the registries list
    (first source wins for duplicate registry paths).

    Example::

        with RegistryConfig(
            registries=[
                # This specific override wins
                Registry('quay.io', auth_file=special_auth),
                # Common configs used as fallback
                *COMMON_READ_ONLY_REGISTRIES(),
            ]
        ) as auth_file:
            ...
    """
    quay_auth_file = os.getenv("QUAY_AUTH_FILE", "")
    rh_registry_auth_file = os.getenv("REGISTRY_REDHAT_IO_AUTH_FILE", "")

    registries = []

    if quay_auth_file:
        registries.extend(
            [
                Registry(
                    "quay.io/openshift-release-dev",
                    auth_file=quay_auth_file,
                ),
                Registry(
                    "quay.io",
                    auth_file=quay_auth_file,
                ),
            ]
        )

    if rh_registry_auth_file:
        registries.append(
            Registry(
                "registry.redhat.io",
                auth_file=rh_registry_auth_file,
            )
        )

    return registries
