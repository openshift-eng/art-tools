"""
Konflux utility functions for group-specific configurations and naming.
"""

import os
import re
from typing import Optional

from artcommonlib import logutil
from artcommonlib.constants import GROUP_KUBECONFIG_MAP, GROUP_NAMESPACE_MAP, KONFLUX_DEFAULT_NAMESPACE

LOGGER = logutil.get_logger(__name__)


def normalize_group_name_for_k8s(group_name: str) -> str:
    """
    Normalize a group name to comply with Kubernetes DNS label rules.

    Kubernetes DNS label rules:
    - Must be lowercase alphanumeric or '-'
    - Must start and end with alphanumeric character
    - Cannot be longer than 63 characters
    - Cannot have consecutive '-'

    Args:
        group_name: The group name to normalize (e.g., "Test_Group-1.5")

    Returns:
        Normalized group name (e.g., "test-group-1-5")
    """
    if not group_name:
        return ""

    # Convert to lowercase
    normalized = group_name.lower()

    # Replace dots and any non-alphanumeric characters (except '-') with '-'
    normalized = re.sub(r'[^a-z0-9\-]', '-', normalized)

    # Collapse consecutive '-' into a single '-'
    normalized = re.sub(r'-+', '-', normalized)

    # Trim leading/trailing non-alphanumeric characters (including '-')
    normalized = re.sub(r'^[^a-z0-9]+|[^a-z0-9]+$', '', normalized)

    # Truncate to 63 characters if needed (leave room for timestamp suffix)
    # Reserve space for timestamp format like "-20251031141128-1" (18 chars)
    max_group_length = 63 - 18 - 1  # -1 for the connecting dash
    if len(normalized) > max_group_length:
        normalized = normalized[:max_group_length]
        # Ensure we don't end with a dash after truncation
        normalized = normalized.rstrip('-')

    return normalized


def resolve_konflux_kubeconfig(group: str, provided_kubeconfig: Optional[str] = None) -> Optional[str]:
    """
    Resolve the Konflux kubeconfig path based on group-specific mappings.

    Args:
        group: The group name (e.g., "openshift-4.17", "oadp-1.5")
        provided_kubeconfig: Explicitly provided kubeconfig path (takes precedence)

    Returns:
        Resolved kubeconfig path or None to rely on oc login
    """
    if provided_kubeconfig:
        return provided_kubeconfig

    kubeconfig_env_var = None
    matched_prefix = None

    # Find matching prefix for kubeconfig environment variable
    # Sort by prefix length descending to prefer longest matches first
    try:
        sorted_mappings = sorted(GROUP_KUBECONFIG_MAP.items(), key=lambda x: len(x[0]), reverse=True)
        for prefix, env_var in sorted_mappings:
            if group.startswith(prefix):
                kubeconfig_env_var = env_var
                matched_prefix = prefix
                break

        if kubeconfig_env_var:
            try:
                kubeconfig = os.environ.get(kubeconfig_env_var)
                if kubeconfig:
                    LOGGER.info(
                        f"Using kubeconfig from {kubeconfig_env_var} for group '{group}' (matched prefix: '{matched_prefix}')"
                    )
                    return kubeconfig
                else:
                    LOGGER.warning(
                        f"Environment variable {kubeconfig_env_var} is not set for group '{group}' (matched prefix: '{matched_prefix}')"
                    )
            except AttributeError as e:
                LOGGER.warning(f"Error accessing environment variable {kubeconfig_env_var} for group '{group}': {e}")
        else:
            LOGGER.warning(
                f"No kubeconfig mapping found for group '{group}'. Available mappings: {dict(GROUP_KUBECONFIG_MAP)}"
            )

        available_env_vars = list(GROUP_KUBECONFIG_MAP.values())
        LOGGER.info(
            f"No kubeconfig specified via --konflux-kubeconfig or environment variables. "
            f"Available env vars: {', '.join(available_env_vars)}. Will rely on oc being logged in to the cluster."
        )
        return None

    except (AttributeError, TypeError) as e:
        LOGGER.warning(
            f"Error processing kubeconfig mappings for group '{group}': {e}. Will rely on oc being logged in."
        )
        return None
    except Exception:
        LOGGER.exception(
            f"Unexpected error determining kubeconfig for group '{group}'. Will rely on oc being logged in."
        )
        return None


def resolve_konflux_namespace(group: str, provided_namespace: Optional[str] = None) -> str:
    """
    Resolve the Konflux namespace based on group-specific mappings.

    Args:
        group: The group name (e.g., "openshift-4.17", "oadp-1.5")
        provided_namespace: Explicitly provided namespace (takes precedence)

    Returns:
        Resolved namespace (guaranteed to return a valid namespace)
    """
    if provided_namespace:
        return provided_namespace

    # Sort by prefix length descending to prefer longest matches first
    try:
        sorted_mappings = sorted(GROUP_NAMESPACE_MAP.items(), key=lambda x: len(x[0]), reverse=True)
        for prefix, namespace in sorted_mappings:
            if group.startswith(prefix):
                LOGGER.info(f"Using namespace '{namespace}' for group '{group}' (matched prefix: '{prefix}')")
                return namespace

        # No mapping found - use default
        LOGGER.warning(
            f"No namespace mapping found for group '{group}'. Available mappings: {dict(GROUP_NAMESPACE_MAP)}. Using default: '{KONFLUX_DEFAULT_NAMESPACE}'"
        )
        return KONFLUX_DEFAULT_NAMESPACE

    except (AttributeError, TypeError) as e:
        LOGGER.warning(
            f"Error processing namespace mappings for group '{group}': {e}. Using default: '{KONFLUX_DEFAULT_NAMESPACE}'"
        )
        return KONFLUX_DEFAULT_NAMESPACE
    except Exception:
        LOGGER.exception(
            f"Unexpected error determining namespace for group '{group}'. Using default: '{KONFLUX_DEFAULT_NAMESPACE}'"
        )
        return KONFLUX_DEFAULT_NAMESPACE
