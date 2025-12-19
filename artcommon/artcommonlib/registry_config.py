"""
Context manager for managing container registry credentials from multiple sources.

This module provides RegistryConfig, a context manager that merges credentials
from multiple auth files and provides a temporary file path for use with
container tools like oc, podman, etc.
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import List, Optional


class RegistryConfig:
    """
    Context manager for managing container registry credentials from multiple sources.

    This class merges credentials from:
    1. Existing default auth file (e.g., /run/user/<uid>/containers/auth.json)
    2. Additional auth files specified via environment variables

    It filters credentials to avoid conflicts (e.g., skipping org-level
    credentials from additional sources if they conflict with existing repo-specific ones).

    The context manager creates a temporary auth file in the current directory and
    returns its path. The file is automatically deleted when exiting the context.

    Example usage:
        # Using filter_patterns to filter credentials
        with RegistryConfig(
            additional_sources=['KONFLUX_ART_IMAGES_AUTH_FILE'],
            filter_patterns=['quay.io/redhat-user-workloads/']
        ) as auth_file:
            # Use auth_file path with oc or other tools
            exectools.cmd_assert(f'oc image mirror --registry-config={auth_file} ...')

        # Using registries to require specific registries (ignores filter_patterns)
        with RegistryConfig(
            registries=['quay.io/openshift-release-dev', 'registry.redhat.io'],
            additional_sources=['KONFLUX_ART_IMAGES_AUTH_FILE'],
        ) as auth_file:
            # Validates that both registries exist in merged credentials
            exectools.cmd_assert(f'oc image mirror --registry-config={auth_file} ...')
    """

    def __init__(
        self,
        registries: Optional[List[str]] = None,
        additional_sources: Optional[List[str]] = None,
        filter_patterns: Optional[List[str]] = None,
        default_auth_file: Optional[str] = None,
    ):
        """
        Initialize the RegistryConfig context manager.

        :param Optional[List[str]] registries: List of specific registries that must be present in the merged credentials.
            If provided, filter_patterns is ignored and all credentials are loaded.
            After merging, validates that all specified registries exist in the credentials.
        :param Optional[List[str]] additional_sources: List of environment variable names containing paths to additional auth files
        :param Optional[List[str]] filter_patterns: List of registry patterns to include.
            Only credentials matching these patterns will be added. If None, all credentials are included.
            Ignored if registries parameter is provided.
        :param Optional[str] default_auth_file: Path to the default auth file. If None, uses XDG_RUNTIME_DIR/containers/auth.json
        """

        self.registries = registries
        self.additional_sources = additional_sources
        self.filter_patterns = filter_patterns
        self.logger = logging.getLogger(__name__)

        # Determine default auth file location
        if default_auth_file:
            self.default_auth_file = default_auth_file

        else:
            runtime_dir = os.environ.get('XDG_RUNTIME_DIR', f'/run/user/{os.getuid()}')
            self.default_auth_file = os.path.join(runtime_dir, 'containers', 'auth.json')

        # Temporary file for merged credentials
        self.temp_auth_file: Optional[Path] = None
        self.merged_auths: dict = {}

    def __enter__(self) -> str:
        """
        Enter the context manager: read, merge, and write credentials to a temporary file.

        :return: Path to the temporary auth file containing merged credentials
        :raises ValueError: If registries parameter is provided and any required registry is not found
        """

        self.logger.info(f'Default auth file: {self.default_auth_file}')

        # Read existing credentials
        self._read_existing_credentials()

        # Add credentials from additional sources
        self._add_additional_credentials()

        # Validate required registries are present
        if self.registries:
            self._validate_required_registries()

        # Write merged credentials to temporary file in current directory
        self._write_merged_credentials()

        return str(self.temp_auth_file)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager: clean up temporary auth file.

        :param exc_type: Exception type if an exception occurred
        :param exc_val: Exception value if an exception occurred
        :param exc_tb: Exception traceback if an exception occurred
        :return: False to not suppress exceptions
        """

        if self.temp_auth_file and self.temp_auth_file.exists():
            try:
                self.temp_auth_file.unlink()
                self.logger.debug(f'Cleaned up temporary auth file: {self.temp_auth_file}')

            except Exception as e:
                self.logger.error(f'Failed to clean up temporary auth file {self.temp_auth_file}: {e}')

        # Don't suppress exceptions
        return False

    def _read_existing_credentials(self):
        """
        Read existing credentials from the default auth file.
        If registries parameter is provided, filtering is skipped.
        """

        if os.path.exists(self.default_auth_file):
            try:
                with open(self.default_auth_file, 'r') as f:
                    existing_config = json.load(f)
                    all_auths = existing_config.get('auths', {})

                    self.logger.debug(f'Read {len(all_auths)} existing auth entries from default file')

                    # Skip filtering if registries parameter is provided
                    if self.registries is not None:
                        # No filtering when specific registries are required
                        self.merged_auths = all_auths
                        self.logger.debug(
                            f'Registries parameter provided - kept all {len(self.merged_auths)} credentials'
                        )

                    # Apply filtering to default credentials if filter_patterns is provided
                    elif self.filter_patterns is None:
                        # No filtering - include all credentials
                        self.merged_auths = all_auths
                        self.logger.debug(f'No filtering applied - kept all {len(self.merged_auths)} credentials')

                    else:
                        # Filter credentials based on patterns
                        filtered_count = 0
                        for registry, creds in all_auths.items():
                            if any(registry.startswith(pattern) for pattern in self.filter_patterns):
                                self.merged_auths[registry] = creds
                                filtered_count += 1
                            else:
                                self.logger.debug(f'Skipping {registry} (does not match filter patterns)')

                        self.logger.debug(f'Kept {filtered_count} credentials after filtering')

            except Exception as e:
                self.logger.error(f'Failed to read existing auth file: {e}')
                raise

        else:
            self.logger.warning(f'No existing credentials found at {self.default_auth_file}')
            self.merged_auths = {}

    def _add_additional_credentials(self):
        """
        Add credentials from additional sources.
        If registries parameter is provided, filtering is skipped.
        """

        if not self.additional_sources:
            self.logger.debug('No additional sources specified')
            return

        total_added = 0

        for source_env_var in self.additional_sources:
            auth_file_path = os.environ.get(source_env_var)

            if not auth_file_path:
                self.logger.debug(f'Environment variable {source_env_var} not set, skipping')
                continue

            if not os.path.exists(auth_file_path):
                self.logger.warning(f'Auth file not found: {auth_file_path} (from {source_env_var})')
                continue

            try:
                with open(auth_file_path, 'r') as f:
                    additional_config = json.load(f)
                    additional_auths = additional_config.get('auths', {})

                    self.logger.info(f'Found {len(additional_auths)} auth entries in {source_env_var}')

                    # Skip filtering if registries parameter is provided
                    added_count = 0
                    if self.registries is not None:
                        # No filtering when specific registries are required
                        for registry, creds in additional_auths.items():
                            self.merged_auths[registry] = creds
                            added_count += 1
                            self.logger.info(f'Added credential for {registry}')

                    # Filter credentials based on patterns (if filter_patterns is provided)
                    elif self.filter_patterns is None:
                        # No filtering - add all credentials
                        for registry, creds in additional_auths.items():
                            self.merged_auths[registry] = creds
                            added_count += 1
                            self.logger.info(f'Added credential for {registry}')
                    else:
                        # Filter credentials based on patterns
                        for registry, creds in additional_auths.items():
                            # Check if registry matches any filter pattern
                            if any(registry.startswith(pattern) for pattern in self.filter_patterns):
                                self.merged_auths[registry] = creds
                                added_count += 1
                                self.logger.info(f'Added credential for {registry}')
                            else:
                                self.logger.debug(f'Skipping {registry} (does not match filter patterns)')

                    self.logger.info(f'Added {added_count} credentials from {source_env_var}')
                    total_added += added_count

            except Exception as e:
                self.logger.error(f'Failed to read auth file from {source_env_var}: {e}')
                raise

        if total_added == 0:
            self.logger.warning('No additional credentials were added')

    def _validate_required_registries(self):
        """
        Validate that all required registries are present in merged credentials.

        :raises ValueError: If any required registry is not found in the merged credentials
        """

        missing_registries = []
        for required_registry in self.registries:
            if required_registry not in self.merged_auths:
                missing_registries.append(required_registry)
                self.logger.error(f'Required registry not found: {required_registry}')

        if missing_registries:
            available = list(self.merged_auths.keys())
            raise ValueError(
                f"Required registries not found in credentials: {missing_registries}. Available registries: {available}"
            )

        self.logger.info(f'All required registries present: {self.registries}')

    def _write_merged_credentials(self):
        """
        Write merged credentials to a temporary file in the current directory.
        """
        try:
            # Create temporary file in current directory with delete=False so we control deletion
            fd, temp_path = tempfile.mkstemp(prefix='registry-auth-', suffix='.json', dir='.', text=True)

            self.temp_auth_file = Path(temp_path)

            # Write the merged credentials
            merged_config = {'auths': self.merged_auths}
            with os.fdopen(fd, 'w') as f:
                json.dump(merged_config, f, indent=2)

            self.logger.debug(f'Wrote {len(self.merged_auths)} total auth entries to {self.temp_auth_file}')
            self.logger.info(f'Enabled registries: {list(self.merged_auths.keys())}')

            # Verify writing was successful
            with open(self.temp_auth_file, 'r') as f:
                verify_config = json.load(f)
                verify_auths = verify_config.get('auths', {})
                if len(verify_auths) != len(self.merged_auths):
                    raise RuntimeError(
                        f'Verification failed: Expected {len(self.merged_auths)} entries, got {len(verify_auths)}'
                    )

        except Exception:
            # Clean up on error
            if self.temp_auth_file and self.temp_auth_file.exists():
                try:
                    self.temp_auth_file.unlink()
                except:
                    pass
            raise

    def get_registries(self) -> List[str]:
        """
        Get list of registries in the merged credentials.

        :return: List of registry names
        """

        return list(self.merged_auths.keys())

    def has_credential_for(self, registry: str) -> bool:
        """
        Check if credentials exist for a specific registry.

        :param str registry: Registry name to check (e.g., 'quay.io/openshift/')
        :return: True if credentials exist, False otherwise
        """

        # Check exact match first
        if registry in self.merged_auths:
            return True

        # Check if any existing credential is a parent/child of the requested registry
        return any(
            registry.startswith(auth_key) or auth_key.startswith(registry) for auth_key in self.merged_auths.keys()
        )
