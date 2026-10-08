from unittest.mock import MagicMock, patch

import pytest
from pyartcd.tekton_locks import is_pipelinerun_active, parse_k8s_lock_path


def test_parse_k8s_lock_path_valid():
    """Test parsing a valid k8s lock path"""
    lock_path = 'k8s/ns/art-quay-tenant/tekton.dev~v1~PipelineRun/build-layered-products-quay-5-0-vrzgj'
    namespace, resource_name = parse_k8s_lock_path(lock_path)

    assert namespace == 'art-quay-tenant'
    assert resource_name == 'build-layered-products-quay-5-0-vrzgj'


def test_parse_k8s_lock_path_invalid_prefix():
    """Test parsing fails for non-k8s lock"""
    lock_path = 'job/aos-cd-jobs/job/build/123'
    result = parse_k8s_lock_path(lock_path)

    assert result is None


def test_parse_k8s_lock_path_malformed():
    """Test parsing fails for malformed path"""
    lock_path = 'k8s/ns/only-two-parts'
    result = parse_k8s_lock_path(lock_path)

    assert result is None


def test_parse_k8s_lock_path_unsupported_resource_type():
    """Test parsing fails for non-PipelineRun resource types"""
    lock_path = 'k8s/ns/art-quay-tenant/tekton.dev~v1~TaskRun/task-abc'
    result = parse_k8s_lock_path(lock_path)

    assert result is None


def test_is_pipelinerun_active_running():
    """Test active running PipelineRun"""
    with patch('pyartcd.tekton_locks.config.load_kube_config'):
        with patch('pyartcd.tekton_locks.client.CustomObjectsApi') as mock_api_class:
            mock_api = MagicMock()
            mock_api_class.return_value = mock_api

            # Running pipeline with no Succeeded condition
            mock_api.get_namespaced_custom_object.return_value = {
                'metadata': {'name': 'build-abc'},
                'status': {'conditions': [{'type': 'Started', 'status': 'True'}]},
            }

            result = is_pipelinerun_active('/path/to/kubeconfig', 'art-quay-tenant', 'build-abc')

            assert result is True


def test_is_pipelinerun_active_completed():
    """Test completed PipelineRun"""
    with patch('pyartcd.tekton_locks.config.load_kube_config'):
        with patch('pyartcd.tekton_locks.client.CustomObjectsApi') as mock_api_class:
            mock_api = MagicMock()
            mock_api_class.return_value = mock_api

            # Completed pipeline
            mock_api.get_namespaced_custom_object.return_value = {
                'metadata': {'name': 'build-abc'},
                'status': {'conditions': [{'type': 'Succeeded', 'status': 'True'}]},
            }

            result = is_pipelinerun_active('/path/to/kubeconfig', 'art-quay-tenant', 'build-abc')

            assert result is False


def test_is_pipelinerun_active_failed():
    """Test failed/timed-out PipelineRun"""
    with patch('pyartcd.tekton_locks.config.load_kube_config'):
        with patch('pyartcd.tekton_locks.client.CustomObjectsApi') as mock_api_class:
            mock_api = MagicMock()
            mock_api_class.return_value = mock_api

            # Failed/timed-out pipeline (Succeeded=False)
            mock_api.get_namespaced_custom_object.return_value = {
                'metadata': {'name': 'build-abc'},
                'status': {'conditions': [{'type': 'Succeeded', 'status': 'False'}]},
            }

            result = is_pipelinerun_active('/path/to/kubeconfig', 'art-quay-tenant', 'build-abc')

            assert result is False


def test_is_pipelinerun_active_not_found():
    """Test PipelineRun doesn't exist"""
    with patch('pyartcd.tekton_locks.config.load_kube_config'):
        with patch('pyartcd.tekton_locks.client.CustomObjectsApi') as mock_api_class:
            from kubernetes import client

            mock_api = MagicMock()
            mock_api_class.return_value = mock_api

            # 404 error
            mock_api.get_namespaced_custom_object.side_effect = client.exceptions.ApiException(status=404)

            result = is_pipelinerun_active('/path/to/kubeconfig', 'art-quay-tenant', 'build-abc')

            assert result is None


def test_is_pipelinerun_active_query_error():
    """Test query error other than 404 raises exception"""
    with patch('pyartcd.tekton_locks.config.load_kube_config'):
        with patch('pyartcd.tekton_locks.client.CustomObjectsApi') as mock_api_class:
            from kubernetes import client

            mock_api = MagicMock()
            mock_api_class.return_value = mock_api

            # 500 error should be raised
            mock_api.get_namespaced_custom_object.side_effect = client.exceptions.ApiException(status=500)

            with pytest.raises(client.exceptions.ApiException):
                is_pipelinerun_active('/path/to/kubeconfig', 'art-quay-tenant', 'build-abc')
