import logging
from typing import Optional, Tuple

from kubernetes import client, config

logger = logging.getLogger(__name__)


def parse_k8s_lock_path(lock_path: str) -> Optional[Tuple[str, str]]:
    """
    Parse k8s lock path to extract namespace and resource name.

    Format: k8s/ns/{namespace}/{group}~{version}~{kind}/{resource_name}
    Example: k8s/ns/art-quay-tenant/tekton.dev~v1~PipelineRun/build-layered-products-quay-5-0-vrzgj

    Args:
        lock_path: Full k8s lock path from Redis

    Returns:
        Tuple of (namespace, resource_name) or None if invalid
    """
    if not lock_path.startswith('k8s/ns/'):
        return None

    try:
        # Remove 'k8s/ns/' prefix
        remainder = lock_path[7:]

        # Split into parts: namespace/apiversion/resource_name
        parts = remainder.split('/', 2)
        if len(parts) != 3:
            return None

        namespace = parts[0]
        api_version = parts[1]
        resource_name = parts[2]

        # Only accept PipelineRun resources for now
        if api_version != 'tekton.dev~v1~PipelineRun':
            return None

        return (namespace, resource_name)
    except Exception as e:
        logger.warning(f"Could not parse k8s lock path {lock_path}: {e}")
        return None


def is_pipelinerun_active(kubeconfig_path: str, namespace: str, resource_name: str) -> Optional[bool]:
    """
    Query a specific PipelineRun and check if it's Running/Pending.

    Args:
        kubeconfig_path: Path to kubeconfig for artc cluster
        namespace: Namespace containing the PipelineRun
        resource_name: Name of the PipelineRun

    Returns:
        True if PipelineRun exists and is Running/Pending
        False if PipelineRun exists but is completed
        None if PipelineRun doesn't exist or query fails
    """
    try:
        config.load_kube_config(config_file=kubeconfig_path)
        api = client.CustomObjectsApi()

        pipelinerun = api.get_namespaced_custom_object(
            group='tekton.dev', version='v1', namespace=namespace, plural='pipelineruns', name=resource_name
        )

        # Check if still active (Running/Pending)
        status = pipelinerun.get('status', {})
        conditions = status.get('conditions', [])

        for condition in conditions:
            if condition.get('type') == 'Succeeded':
                # Check status: Unknown means still running, True/False means completed
                cond_status = condition.get('status', '')
                if cond_status == 'Unknown':
                    return True  # Still running, protect lock
                else:
                    return False  # Completed (True=success, False=failure), safe to delete

        # No Succeeded condition means still running/initializing
        return True

    except client.exceptions.ApiException as e:
        if e.status == 404:
            # PipelineRun doesn't exist
            return None
        raise
