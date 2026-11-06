from typing import Optional, Tuple

from kubernetes import client, config


def parse_k8s_lock_path(lock_path: str) -> Optional[Tuple[str, str, str]]:
    """
    Parse a Tekton lock identifier into namespace, resource name, and kind.

    Format: k8s/ns/{namespace}/{group}~{version}~{kind}/{resource_name}
    Supports both PipelineRun and TaskRun identities.

    Args:
        lock_path: Full k8s lock path from Redis

    Returns:
        Tuple of (namespace, resource_name, resource_kind) or None if invalid
    """
    parts = lock_path.split('/')
    if len(parts) != 5 or parts[:2] != ['k8s', 'ns'] or not parts[2] or not parts[4]:
        return None
    kinds = {'tekton.dev~v1~PipelineRun': 'PipelineRun', 'tekton.dev~v1~TaskRun': 'TaskRun'}
    kind = kinds.get(parts[3])
    if not kind:
        return None
    return parts[2], parts[4], kind


def is_tekton_run_active(
    kubeconfig_path: str, namespace: str, resource_name: str, resource_kind: str = "PipelineRun"
) -> Optional[bool]:
    """
    Query a specific PipelineRun or TaskRun and check if it's Running/Pending.

    Args:
        kubeconfig_path: Path to kubeconfig for artc cluster
        namespace: Namespace containing the run
        resource_name: Name of the run
        resource_kind: PipelineRun or TaskRun

    Returns:
        True if the run exists and is Running/Pending
        False if the run exists but is completed
        None if the run doesn't exist (404); other query errors are raised
    """
    resources = {'PipelineRun': 'pipelineruns', 'TaskRun': 'taskruns'}
    if resource_kind not in resources:
        raise ValueError(f'Unsupported Tekton resource kind: {resource_kind}')
    try:
        config.load_kube_config(config_file=kubeconfig_path)
        api = client.CustomObjectsApi()

        run = api.get_namespaced_custom_object(
            group='tekton.dev', version='v1', namespace=namespace, plural=resources[resource_kind], name=resource_name
        )

        # Check if still active (Running/Pending)
        status = run.get('status', {})
        conditions = status.get('conditions', [])

        for condition in conditions:
            if condition.get('type') == 'Succeeded':
                # Check status: Unknown means still running, True/False means completed
                cond_status = condition.get('status', '')
                return cond_status not in ('True', 'False')

        # No Succeeded condition means still running/initializing
        return True

    except client.exceptions.ApiException as e:
        if e.status == 404:
            # The requested run doesn't exist
            return None
        raise
