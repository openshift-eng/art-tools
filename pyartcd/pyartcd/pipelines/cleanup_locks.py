import asyncio
import enum
import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlparse

from aioredlock import Lock
from artcommonlib import redis
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from pyartcd import constants, jenkins
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.locks import LockManager
from pyartcd.runtime import Runtime

TEKTON_CLEANUP_KUBECONFIG_ENV = 'TEKTON_CLEANUP_KUBECONFIG'
TEKTON_PIPELINERUN_OWNER_RE = re.compile(
    r'^k8s/ns/(?P<namespace>[^/]+)/tekton\.dev~(?P<version>[^/~]+)~PipelineRun/(?P<name>[^/]+)/?$'
)


@dataclass(frozen=True)
class PipelineRunRef:
    namespace: str
    version: str
    name: str


class PipelineRunState(enum.Enum):
    ACTIVE = 'active'
    TERMINAL = 'terminal'
    MISSING = 'missing'
    UNKNOWN = 'unknown'


def _owner_path(owner_id: str) -> str:
    if owner_id.startswith(('http://', 'https://')):
        return urlparse(owner_id).path.strip('/')
    return owner_id.strip('/')


def is_kubernetes_owner(owner_id: str) -> bool:
    """Identify Kubernetes console paths so they are never sent to Jenkins."""
    return _owner_path(owner_id).startswith('k8s/ns/')


def parse_tekton_pipelinerun_owner(owner_id: str) -> PipelineRunRef | None:
    """Parse the owner path emitted by PipelineRuns in the OpenShift console."""
    match = TEKTON_PIPELINERUN_OWNER_RE.fullmatch(_owner_path(owner_id))
    if not match:
        return None
    return PipelineRunRef(
        namespace=match.group('namespace'),
        version=match.group('version'),
        name=match.group('name'),
    )


def _get_pipelinerun_state(resource: dict) -> PipelineRunState:
    status = resource.get('status')
    if not isinstance(status, dict):
        return PipelineRunState.UNKNOWN

    conditions = status.get('conditions')
    if not isinstance(conditions, list):
        return PipelineRunState.UNKNOWN

    succeeded = next(
        (condition for condition in conditions if isinstance(condition, dict) and condition.get('type') == 'Succeeded'),
        None,
    )
    if succeeded is None:
        return PipelineRunState.UNKNOWN

    condition_status = succeeded.get('status')
    if condition_status in ('True', 'False'):
        return PipelineRunState.TERMINAL
    if condition_status == 'Unknown':
        return PipelineRunState.ACTIVE
    return PipelineRunState.UNKNOWN


def _is_missing_pipelinerun(exc: ApiException, run_ref: PipelineRunRef) -> bool:
    if exc.status != 404:
        return False

    try:
        error = json.loads(exc.body)
    except (TypeError, ValueError):
        return False

    details = error.get('details', {})
    kind = details.get('kind', '').lower()
    return (
        error.get('reason') == 'NotFound'
        and details.get('name') == run_ref.name
        and details.get('group') == 'tekton.dev'
        and kind in ('pipelinerun', 'pipelineruns')
    )


async def get_pipelinerun_state(run_ref: PipelineRunRef) -> PipelineRunState:
    """Read a PipelineRun using the cleanup job's read-only kubeconfig."""
    kubeconfig = os.environ.get(TEKTON_CLEANUP_KUBECONFIG_ENV)
    if not kubeconfig:
        raise RuntimeError(f'{TEKTON_CLEANUP_KUBECONFIG_ENV} is not set')

    api_client = config.new_client_from_config(config_file=kubeconfig)
    try:
        custom_objects_api = client.CustomObjectsApi(api_client)
        try:
            resource = await asyncio.to_thread(
                custom_objects_api.get_namespaced_custom_object,
                group='tekton.dev',
                version=run_ref.version,
                namespace=run_ref.namespace,
                plural='pipelineruns',
                name=run_ref.name,
                _request_timeout=30,
            )
        except ApiException as exc:
            if _is_missing_pipelinerun(exc, run_ref):
                return PipelineRunState.MISSING
            raise
    finally:
        api_client.close()

    return _get_pipelinerun_state(resource)


async def cleanup_tekton_lock(
    lock_manager: LockManager,
    lock_name: str,
    owner_id: str,
    run_ref: PipelineRunRef,
    logger,
) -> bool:
    """Release a Tekton-owned lock only when its PipelineRun is terminal or missing."""
    try:
        state = await get_pipelinerun_state(run_ref)
    except Exception as exc:
        logger.warning(
            'Unable to check PipelineRun %s/%s for lock %s; retaining lock: %s',
            run_ref.namespace,
            run_ref.name,
            lock_name,
            exc,
        )
        return False

    if state not in (PipelineRunState.TERMINAL, PipelineRunState.MISSING):
        logger.info(
            'Retaining lock %s owned by PipelineRun %s/%s (state: %s)',
            lock_name,
            run_ref.namespace,
            run_ref.name,
            state.value,
        )
        return False

    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=owner_id)
    await lock_manager.unlock(lock)
    logger.warning(
        'Deleting lock %s owned by PipelineRun %s/%s (state: %s)',
        lock_name,
        run_ref.namespace,
        run_ref.name,
        state.value,
    )
    return True


@cli.command('cleanup-locks')
@pass_runtime
@click_coroutine
async def cleanup_locks(runtime: Runtime):
    lock_manager = LockManager([redis.redis_url()])
    active_locks = await lock_manager.get_locks()
    runtime.logger.info('Found %s active locks', len(active_locks))

    removed_locks = []
    jenkins_url = jenkins.get_jenkins_url()

    try:
        for lock_name in active_locks:
            # Lock ID is the build URL path, or an identifier generated by the lock caller.
            owner_id = await lock_manager.get_lock_id(lock_name)

            if not owner_id:
                runtime.logger.warning(
                    'Skipping lock %s with None or empty owner ID (cannot validate or delete)', lock_name
                )
                continue

            if is_kubernetes_owner(owner_id):
                run_ref = parse_tekton_pipelinerun_owner(owner_id)
                if run_ref is None:
                    runtime.logger.warning(
                        'Skipping lock %s with unrecognized Kubernetes owner ID %s', lock_name, owner_id
                    )
                    continue

                runtime.logger.info(
                    'Checking lock %s owned by PipelineRun %s/%s', lock_name, run_ref.namespace, run_ref.name
                )
                if await cleanup_tekton_lock(lock_manager, lock_name, owner_id, run_ref, runtime.logger):
                    removed_locks.append(lock_name)
                continue

            if owner_id.startswith('random-'):
                runtime.logger.info(
                    'Skipping lock %s with random identifier %s (not a Jenkins build)', lock_name, owner_id
                )
                continue

            build_url = f'{constants.JENKINS_UI_URL}/{owner_id}'
            runtime.logger.info('Found build_url for lock %s: %s', lock_name, build_url)

            try:
                is_build_running = jenkins.is_build_running(owner_id)
                runtime.logger.info('Found build %s associated with lock %s', build_url, lock_name)

                if not is_build_running:
                    runtime.logger.warning(
                        'Deleting lock %s that was created by %s that\'s not currently running',
                        lock_name,
                        build_url.replace(jenkins_url, constants.JENKINS_UI_URL),
                    )
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=owner_id)
                    await lock_manager.unlock(lock)
                    removed_locks.append(lock_name)

                else:
                    runtime.logger.info('Build %s is still running: won\'t delete lock %s', owner_id, lock_name)

            except ValueError:
                runtime.logger.info('could not see if build is running.. checking if api is reachable')
                if jenkins.is_api_reachable():
                    # Make sure Jenkins API are responding
                    # Assume the build is not found because it was manually deleted, and clean up the orphan lock
                    runtime.logger.warning('Could not get build from lock %s with id %s: deleting', lock_name, owner_id)
                    lock: Lock = await lock_manager.get_lock(resource=lock_name, lock_identifier=owner_id)
                    await lock_manager.unlock(lock)
                else:
                    runtime.logger.info("api isn't reachable :(")

    finally:
        await lock_manager.destroy()

    # Display removed locks in the build title
    jenkins.init_jenkins()
    if os.getenv('BUILD_URL') and os.getenv('JOB_NAME'):
        if removed_locks:
            jenkins.update_title(f' [{", ".join(removed_locks)}]')
