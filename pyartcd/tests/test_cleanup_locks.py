import asyncio
import json
import logging
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
from kubernetes.client.rest import ApiException
from pyartcd.pipelines.cleanup_locks import (
    PipelineRunRef,
    PipelineRunState,
    _get_pipelinerun_state,
    cleanup_locks,
    cleanup_tekton_lock,
    get_pipelinerun_state,
    is_kubernetes_owner,
    parse_tekton_pipelinerun_owner,
)
from pyartcd.runtime import Runtime


class TestTektonLockOwner:
    OWNER_ID = 'k8s/ns/art-quay-tenant/tekton.dev~v1~PipelineRun/build-layered-products-quay-3-17-cv5f6'

    def test_parse_pipelinerun_owner(self):
        assert parse_tekton_pipelinerun_owner(self.OWNER_ID) == PipelineRunRef(
            namespace='art-quay-tenant', version='v1', name='build-layered-products-quay-3-17-cv5f6'
        )

    def test_parse_full_console_url_with_trailing_slash(self):
        owner_url = f'https://console.example.com/{self.OWNER_ID}/'
        assert parse_tekton_pipelinerun_owner(owner_url) == PipelineRunRef(
            namespace='art-quay-tenant', version='v1', name='build-layered-products-quay-3-17-cv5f6'
        )

    def test_unrecognized_kubernetes_owner_is_not_a_pipelinerun(self):
        owner_id = 'k8s/ns/art-quay-tenant/tekton.dev~v1~TaskRun/task-123'
        assert is_kubernetes_owner(owner_id)
        assert parse_tekton_pipelinerun_owner(owner_id) is None

    def test_non_kubernetes_owner(self):
        assert not is_kubernetes_owner('job/aos-cd-jobs/job/build/123')


class TestPipelineRunState:
    def test_succeeded_true_is_terminal(self):
        resource = {'status': {'conditions': [{'type': 'Succeeded', 'status': 'True'}]}}
        assert _get_pipelinerun_state(resource) is PipelineRunState.TERMINAL

    def test_succeeded_false_is_terminal(self):
        resource = {'status': {'conditions': [{'type': 'Succeeded', 'status': 'False', 'reason': 'Cancelled'}]}}
        assert _get_pipelinerun_state(resource) is PipelineRunState.TERMINAL

    def test_succeeded_unknown_is_active(self):
        resource = {'status': {'conditions': [{'type': 'Succeeded', 'status': 'Unknown'}]}}
        assert _get_pipelinerun_state(resource) is PipelineRunState.ACTIVE

    def test_missing_or_invalid_condition_is_unknown(self):
        assert _get_pipelinerun_state({}) is PipelineRunState.UNKNOWN
        assert _get_pipelinerun_state({'status': {'conditions': []}}) is PipelineRunState.UNKNOWN


class TestGetPipelineRunState(IsolatedAsyncioTestCase):
    async def test_fetches_run_with_dedicated_kubeconfig(self):
        api_client = MagicMock()
        custom_api = MagicMock()
        custom_api.get_namespaced_custom_object.return_value = {
            'status': {'conditions': [{'type': 'Succeeded', 'status': 'Unknown'}]}
        }
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v1', name='build-abc')

        with (
            patch(
                'pyartcd.pipelines.cleanup_locks.config.new_client_from_config', return_value=api_client
            ) as load_config,
            patch('pyartcd.pipelines.cleanup_locks.client.CustomObjectsApi', return_value=custom_api),
            patch.dict('os.environ', {'TEKTON_CLEANUP_KUBECONFIG': '/tmp/tekton-readonly-kubeconfig'}),
        ):
            state = await get_pipelinerun_state(run_ref)

        load_config.assert_called_once_with(config_file='/tmp/tekton-readonly-kubeconfig')
        custom_api.get_namespaced_custom_object.assert_called_once_with(
            group='tekton.dev',
            version='v1',
            namespace='art-quay-tenant',
            plural='pipelineruns',
            name='build-abc',
            _request_timeout=30,
        )
        api_client.close.assert_called_once()
        self.assertIs(state, PipelineRunState.ACTIVE)

    async def test_not_found_is_reported_as_missing(self):
        api_client = MagicMock()
        custom_api = MagicMock()
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v1', name='deleted-run')
        not_found = ApiException(status=404, reason='Not Found')
        not_found.body = json.dumps(
            {
                'reason': 'NotFound',
                'details': {'name': 'deleted-run', 'group': 'tekton.dev', 'kind': 'pipelineruns'},
            }
        )
        custom_api.get_namespaced_custom_object.side_effect = not_found

        with (
            patch('pyartcd.pipelines.cleanup_locks.config.new_client_from_config', return_value=api_client),
            patch('pyartcd.pipelines.cleanup_locks.client.CustomObjectsApi', return_value=custom_api),
            patch.dict('os.environ', {'TEKTON_CLEANUP_KUBECONFIG': '/tmp/tekton-readonly-kubeconfig'}),
        ):
            state = await get_pipelinerun_state(run_ref)

        api_client.close.assert_called_once()
        self.assertIs(state, PipelineRunState.MISSING)

    async def test_missing_api_resource_is_not_treated_as_missing_run(self):
        api_client = MagicMock()
        custom_api = MagicMock()
        not_found = ApiException(status=404, reason='Not Found')
        not_found.body = json.dumps(
            {'reason': 'NotFound', 'message': 'the server could not find the requested resource'}
        )
        custom_api.get_namespaced_custom_object.side_effect = not_found
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v9', name='build-abc')

        with (
            patch('pyartcd.pipelines.cleanup_locks.config.new_client_from_config', return_value=api_client),
            patch('pyartcd.pipelines.cleanup_locks.client.CustomObjectsApi', return_value=custom_api),
            patch.dict('os.environ', {'TEKTON_CLEANUP_KUBECONFIG': '/tmp/tekton-readonly-kubeconfig'}),
        ):
            with self.assertRaises(ApiException):
                await get_pipelinerun_state(run_ref)

        api_client.close.assert_called_once()

    async def test_other_api_errors_propagate_for_fail_closed_handling(self):
        api_client = MagicMock()
        custom_api = MagicMock()
        custom_api.get_namespaced_custom_object.side_effect = ApiException(status=403, reason='Forbidden')
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v1', name='build-abc')

        with (
            patch('pyartcd.pipelines.cleanup_locks.config.new_client_from_config', return_value=api_client),
            patch('pyartcd.pipelines.cleanup_locks.client.CustomObjectsApi', return_value=custom_api),
            patch.dict('os.environ', {'TEKTON_CLEANUP_KUBECONFIG': '/tmp/tekton-readonly-kubeconfig'}),
        ):
            with self.assertRaises(ApiException):
                await get_pipelinerun_state(run_ref)

        api_client.close.assert_called_once()


class TestCleanupTektonLock(IsolatedAsyncioTestCase):
    async def test_retains_active_and_unknown_runs(self):
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v1', name='build-abc')
        lock_manager = MagicMock()
        lock_manager.get_lock = AsyncMock()
        lock_manager.unlock = AsyncMock()
        logger = MagicMock()

        for state in (PipelineRunState.ACTIVE, PipelineRunState.UNKNOWN):
            with (
                self.subTest(state=state),
                patch(
                    'pyartcd.pipelines.cleanup_locks.get_pipelinerun_state', new_callable=AsyncMock, return_value=state
                ),
            ):
                removed = await cleanup_tekton_lock(lock_manager, 'lock:build:3.17', 'owner', run_ref, logger)
                self.assertFalse(removed)

        lock_manager.get_lock.assert_not_awaited()
        lock_manager.unlock.assert_not_awaited()

    async def test_releases_terminal_and_missing_runs(self):
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v1', name='build-abc')
        lock_manager = MagicMock()
        lock_manager.get_lock = AsyncMock(return_value=object())
        lock_manager.unlock = AsyncMock()
        logger = MagicMock()

        for state in (PipelineRunState.TERMINAL, PipelineRunState.MISSING):
            with (
                self.subTest(state=state),
                patch(
                    'pyartcd.pipelines.cleanup_locks.get_pipelinerun_state', new_callable=AsyncMock, return_value=state
                ),
            ):
                removed = await cleanup_tekton_lock(lock_manager, 'lock:build:3.17', 'owner', run_ref, logger)
                self.assertTrue(removed)

        self.assertEqual(lock_manager.get_lock.await_count, 2)
        self.assertEqual(lock_manager.unlock.await_count, 2)

    async def test_api_error_retains_lock(self):
        run_ref = PipelineRunRef(namespace='art-quay-tenant', version='v1', name='build-abc')
        lock_manager = MagicMock()
        lock_manager.get_lock = AsyncMock()
        lock_manager.unlock = AsyncMock()
        logger = MagicMock()

        with patch(
            'pyartcd.pipelines.cleanup_locks.get_pipelinerun_state',
            new_callable=AsyncMock,
            side_effect=ApiException(status=403, reason='Forbidden'),
        ):
            removed = await cleanup_tekton_lock(lock_manager, 'lock:build:3.17', 'owner', run_ref, logger)

        self.assertFalse(removed)
        lock_manager.get_lock.assert_not_awaited()
        lock_manager.unlock.assert_not_awaited()
        logger.warning.assert_called_once()


class TestCleanupLocksCommand:
    def _invoke(self, owner_id, *, run_state=None, jenkins_running=False):
        lock_name = 'lock:layered-products-build:quay-3.17'
        lock_manager = MagicMock()
        lock_manager.get_locks = AsyncMock(return_value=[lock_name])
        lock_manager.get_lock_id = AsyncMock(return_value=owner_id)
        lock_manager.get_lock = AsyncMock(return_value=object())
        lock_manager.unlock = AsyncMock()
        lock_manager.destroy = AsyncMock()
        runtime = Runtime(config={}, working_dir=Path.cwd(), dry_run=False)
        runtime.logger = logging.getLogger('test.cleanup_locks')

        with (
            patch('pyartcd.pipelines.cleanup_locks.LockManager', return_value=lock_manager),
            patch('pyartcd.pipelines.cleanup_locks.redis.redis_url', return_value='redis://test'),
            patch(
                'pyartcd.pipelines.cleanup_locks.jenkins.get_jenkins_url', return_value='https://jenkins.example.com'
            ),
            patch('pyartcd.pipelines.cleanup_locks.jenkins.init_jenkins'),
            patch(
                'pyartcd.pipelines.cleanup_locks.jenkins.is_build_running', return_value=jenkins_running
            ) as check_jenkins,
            patch(
                'pyartcd.pipelines.cleanup_locks.get_pipelinerun_state',
                new_callable=AsyncMock,
                return_value=run_state,
            ) as check_pipelinerun,
            patch.dict('os.environ', {'BUILD_URL': '', 'JOB_NAME': ''}),
        ):
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = CliRunner().invoke(cleanup_locks, [], obj=runtime)
            finally:
                asyncio.set_event_loop(None)
                loop.close()

        return result, lock_manager, check_jenkins, check_pipelinerun

    def test_active_tekton_owner_is_never_checked_in_jenkins(self):
        owner_id = 'k8s/ns/art-quay-tenant/tekton.dev~v1~PipelineRun/build-abc'
        result, lock_manager, check_jenkins, check_pipelinerun = self._invoke(
            owner_id, run_state=PipelineRunState.ACTIVE
        )

        assert result.exit_code == 0, result.exception
        check_jenkins.assert_not_called()
        check_pipelinerun.assert_awaited_once_with(
            PipelineRunRef(namespace='art-quay-tenant', version='v1', name='build-abc')
        )
        lock_manager.unlock.assert_not_awaited()

    def test_malformed_kubernetes_owner_is_retained_without_jenkins_lookup(self):
        owner_id = 'k8s/ns/art-quay-tenant/tekton.dev~v1~TaskRun/task-abc'
        result, lock_manager, check_jenkins, check_pipelinerun = self._invoke(owner_id)

        assert result.exit_code == 0, result.exception
        check_jenkins.assert_not_called()
        check_pipelinerun.assert_not_awaited()
        lock_manager.unlock.assert_not_awaited()

    def test_jenkins_owner_keeps_existing_cleanup_behavior(self):
        owner_id = 'job/aos-cd-jobs/job/build/123'
        result, lock_manager, check_jenkins, check_pipelinerun = self._invoke(owner_id, jenkins_running=False)

        assert result.exit_code == 0, result.exception
        check_jenkins.assert_called_once_with(owner_id)
        check_pipelinerun.assert_not_awaited()
        lock_manager.unlock.assert_awaited_once()
