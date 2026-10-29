import json
from unittest.mock import MagicMock, patch

import pytest

from pyartcd import tekton


class TestIsTektonContext:
    """Test is_tekton_context() detection logic."""

    def test_tekton_context_both_vars_present(self, monkeypatch):
        """When both TASKRUN_NAME and TEKTON_PIPELINERUN_NAME are set, return True."""
        monkeypatch.setenv("TASKRUN_NAME", "my-task-run")
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "my-pipeline-run")
        assert tekton.is_tekton_context() is True

    def test_jenkins_context_no_vars(self, monkeypatch):
        """When neither env var is set, return False (Jenkins context)."""
        monkeypatch.delenv("TASKRUN_NAME", raising=False)
        monkeypatch.delenv("TEKTON_PIPELINERUN_NAME", raising=False)
        assert tekton.is_tekton_context() is False

    def test_jenkins_context_only_taskrun(self, monkeypatch):
        """When only TASKRUN_NAME is set, return False (incomplete Tekton config)."""
        monkeypatch.setenv("TASKRUN_NAME", "my-task-run")
        monkeypatch.delenv("TEKTON_PIPELINERUN_NAME", raising=False)
        assert tekton.is_tekton_context() is False

    def test_jenkins_context_only_pipelinerun(self, monkeypatch):
        """When only TEKTON_PIPELINERUN_NAME is set, return False (incomplete Tekton config)."""
        monkeypatch.delenv("TASKRUN_NAME", raising=False)
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "my-pipeline-run")
        assert tekton.is_tekton_context() is False

    def test_jenkins_context_empty_strings(self, monkeypatch):
        """When vars are set to empty strings, return False."""
        monkeypatch.setenv("TASKRUN_NAME", "")
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "")
        assert tekton.is_tekton_context() is False

    def test_tekton_context_with_empty_taskrun(self, monkeypatch):
        """When TASKRUN_NAME is empty but TEKTON_PIPELINERUN_NAME is set, return False."""
        monkeypatch.setenv("TASKRUN_NAME", "")
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "my-pipeline-run")
        assert tekton.is_tekton_context() is False

    def test_tekton_context_with_empty_pipelinerun(self, monkeypatch):
        """When TEKTON_PIPELINERUN_NAME is empty but TASKRUN_NAME is set, return False."""
        monkeypatch.setenv("TASKRUN_NAME", "my-task-run")
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "")
        assert tekton.is_tekton_context() is False


class TestGetCurrentPipelineRunName:
    """Test get_current_pipelinerun_name() function."""

    def test_returns_name_when_set(self, monkeypatch):
        """Return the PipelineRun name when TEKTON_PIPELINERUN_NAME is set."""
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "my-pipeline-run-abc123")
        assert tekton.get_current_pipelinerun_name() == "my-pipeline-run-abc123"

    def test_returns_none_when_not_set(self, monkeypatch):
        """Return None when TEKTON_PIPELINERUN_NAME is not set."""
        monkeypatch.delenv("TEKTON_PIPELINERUN_NAME", raising=False)
        assert tekton.get_current_pipelinerun_name() is None

    def test_returns_none_when_empty(self, monkeypatch):
        """Return None when TEKTON_PIPELINERUN_NAME is empty string."""
        monkeypatch.setenv("TEKTON_PIPELINERUN_NAME", "")
        assert tekton.get_current_pipelinerun_name() is None


class TestStartPipelineRunLineage:
    @pytest.fixture(autouse=True)
    def context(self, monkeypatch):
        monkeypatch.setenv('TASKRUN_NAMESPACE', 'art-test-tenant')
        monkeypatch.setenv('TEKTON_PIPELINERUN_NAME', 'new-scan')
        monkeypatch.setenv('TEKTON_PIPELINE_NAME', 'layered-products-scan')
        monkeypatch.setenv('TEKTON_CONSOLE_URL', 'https://console.example')

    def create(self, **kwargs):
        with patch.object(tekton.subprocess, 'run', return_value=MagicMock(stdout='build-fbc-retry created')) as oc:
            assert tekton.start_pipeline_run('build-fbc', {'group': 'test-1.0'}, **kwargs) == 'build-fbc-retry'
        return json.loads(oc.call_args.kwargs['input'])['metadata']

    def test_normal_trigger_still_uses_current_parent(self):
        metadata = self.create()
        assert metadata['labels']['art.openshift.io/parent-pipelinerun'] == 'new-scan'
        assert metadata['labels']['art.openshift.io/parent-pipeline'] == 'layered-products-scan'

    def test_recovery_uses_original_parent_and_retry_uid(self):
        parent = {
            'metadata': {'name': 'original-bundle', 'namespace': 'art-test-tenant'},
            'spec': {'pipelineRef': {'name': 'olm-bundle-konflux'}},
        }
        original = {
            'metadata': {'name': 'failed-fbc', 'namespace': 'art-test-tenant', 'uid': 'original-uid'},
            'spec': {'pipelineRef': {'name': 'build-fbc'}},
        }
        metadata = self.create(parent_run=parent, rebuilt_from=original, use_current_parent=False)
        assert metadata['labels']['art.openshift.io/parent-pipelinerun'] == 'original-bundle'
        assert metadata['labels']['art.openshift.io/parent-pipeline'] == 'olm-bundle-konflux'
        assert metadata['annotations'] == {
            'art.openshift.io/parent-pipelinerun-console-url': 'https://console.example/k8s/ns/art-test-tenant/tekton.dev~v1~PipelineRun/original-bundle',
            'art.openshift.io/rebuilt-from': 'original-uid',
            'art.openshift.io/rebuilt-from-name': 'failed-fbc',
            'art.openshift.io/recovery-scan': 'new-scan',
        }

    def test_unresolved_recovery_does_not_use_scan_as_parent(self):
        metadata = self.create(use_current_parent=False)
        assert 'art.openshift.io/parent-pipelinerun' not in metadata['labels']
        assert metadata['annotations']['art.openshift.io/recovery-scan'] == 'new-scan'
        assert metadata['annotations']['art.openshift.io/recovery-lineage'] == 'unresolved'

    def test_cross_namespace_parent_is_rejected_before_create(self):
        with patch.object(tekton.subprocess, 'run') as oc, pytest.raises(ValueError, match='namespace'):
            self.create(parent_run={'metadata': {'namespace': 'other'}})
        oc.assert_not_called()
