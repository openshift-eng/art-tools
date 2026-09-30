import asyncio
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner
from pyartcd.pipelines.cleanup_locks import cleanup_locks
from pyartcd.runtime import Runtime

LOCK_NAME = 'lock:layered-products-build:quay-3.17'
JENKINS_OWNER = 'job/aos-cd-jobs/job/build/123'


def invoke_cleanup(
    owner_id,
    *,
    jenkins_running=False,
    jenkins_error=None,
    api_reachable=False,
    build_url='',
    job_name='',
):
    lock_manager = MagicMock()
    lock_manager.get_locks = AsyncMock(return_value=[LOCK_NAME])
    lock_manager.get_lock_id = AsyncMock(return_value=owner_id)
    lock_manager.get_lock = AsyncMock(return_value=object())
    lock_manager.unlock = AsyncMock()
    lock_manager.destroy = AsyncMock()
    runtime = Runtime(config={}, working_dir=Path.cwd(), dry_run=False)
    runtime.logger = logging.getLogger('test.cleanup_locks')

    with (
        patch('pyartcd.pipelines.cleanup_locks.LockManager', return_value=lock_manager),
        patch('pyartcd.pipelines.cleanup_locks.redis.redis_url', return_value='redis://test'),
        patch('pyartcd.pipelines.cleanup_locks.jenkins.get_jenkins_url', return_value='https://jenkins.example.com'),
        patch(
            'pyartcd.pipelines.cleanup_locks.jenkins.is_build_running',
            return_value=jenkins_running,
            side_effect=jenkins_error,
        ) as check_jenkins,
        patch('pyartcd.pipelines.cleanup_locks.jenkins.is_api_reachable', return_value=api_reachable) as check_api,
        patch('pyartcd.pipelines.cleanup_locks.jenkins.init_jenkins') as init_jenkins,
        patch('pyartcd.pipelines.cleanup_locks.jenkins.update_title') as update_title,
        patch.dict('os.environ', {'BUILD_URL': build_url, 'JOB_NAME': job_name}),
    ):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = CliRunner().invoke(cleanup_locks, [], obj=runtime)
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    assert result.exit_code == 0, result.exception
    lock_manager.destroy.assert_awaited_once()
    return lock_manager, check_jenkins, check_api, init_jenkins, update_title


@pytest.mark.parametrize(
    'owner_id',
    [
        'k8s/ns/art-quay-tenant/tekton.dev~v1~PipelineRun/build-abc',
        'k8s/ns/art-quay-tenant/tekton.dev~v1~TaskRun/task-abc',
        'k8s/ns/art-quay-tenant/unknown-resource',
        'k8s/malformed',
    ],
)
def test_kubernetes_owner_is_retained_without_jenkins_lookup(owner_id):
    lock_manager, check_jenkins, _, init_jenkins, _ = invoke_cleanup(owner_id)

    check_jenkins.assert_not_called()
    lock_manager.get_lock.assert_not_awaited()
    lock_manager.unlock.assert_not_awaited()
    init_jenkins.assert_not_called()


@pytest.mark.parametrize('owner_id', [None, '', 'random-123'])
def test_empty_and_random_owners_are_retained(owner_id):
    lock_manager, check_jenkins, _, _, _ = invoke_cleanup(owner_id)

    check_jenkins.assert_not_called()
    lock_manager.unlock.assert_not_awaited()


def test_running_jenkins_build_keeps_its_lock():
    lock_manager, check_jenkins, _, _, _ = invoke_cleanup(JENKINS_OWNER, jenkins_running=True)

    check_jenkins.assert_called_once_with(JENKINS_OWNER)
    lock_manager.unlock.assert_not_awaited()


def test_completed_jenkins_build_releases_its_lock_and_updates_title():
    build_url = f'https://jenkins.example.com/{JENKINS_OWNER}'
    lock_manager, check_jenkins, _, init_jenkins, update_title = invoke_cleanup(
        JENKINS_OWNER, build_url=build_url, job_name='aos-cd-jobs/build'
    )

    check_jenkins.assert_called_once_with(JENKINS_OWNER)
    lock_manager.get_lock.assert_awaited_once_with(resource=LOCK_NAME, lock_identifier=JENKINS_OWNER)
    lock_manager.unlock.assert_awaited_once()
    init_jenkins.assert_called_once()
    update_title.assert_called_once_with(f' [{LOCK_NAME}]')


def test_jenkins_title_is_not_updated_from_a_tekton_job():
    build_url = 'https://console.example.com/k8s/ns/art-quay-tenant/tekton.dev~v1~PipelineRun/cleanup-abc'
    lock_manager, _, _, init_jenkins, update_title = invoke_cleanup(
        JENKINS_OWNER, build_url=build_url, job_name='cleanup-locks'
    )

    lock_manager.unlock.assert_awaited_once()
    init_jenkins.assert_not_called()
    update_title.assert_not_called()


@pytest.mark.parametrize('api_reachable', [True, False])
def test_missing_jenkins_build_uses_existing_api_reachability_check(api_reachable):
    lock_manager, check_jenkins, check_api, _, _ = invoke_cleanup(
        JENKINS_OWNER, jenkins_error=ValueError('missing build'), api_reachable=api_reachable
    )

    check_jenkins.assert_called_once_with(JENKINS_OWNER)
    check_api.assert_called_once()
    assert lock_manager.unlock.await_count == int(api_reachable)
