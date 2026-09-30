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
