"""
Utility classes for handling Konflux PipelineRun, Pod, and Container information.

These classes provide immutable snapshots of Kubernetes resources and encapsulate
common operations for accessing their data.
"""

import datetime
from typing import Dict, List, Optional, Tuple, Union

from artcommonlib import util as art_util
from kubernetes.dynamic import resource


class ContainerInfo:
    """Encapsulates information about a container within a Kubernetes Pod."""

    def __init__(self, container_status: Dict, is_init_container: bool, log_content: Optional[str] = None):
        """
        Initialize a ContainerInfo instance.

        :param container_status: The container status dictionary from Kubernetes API
        :param is_init_container: Whether this is an init container
        :param log_content: Optional pre-fetched log content for this container
        """
        self._container_status = container_status
        self._is_init_container = is_init_container
        self._log_content = log_content

    @property
    def name(self) -> str:
        """Get the container name."""
        return self._container_status.get("name", "")

    @property
    def image(self) -> str:
        """Get the container image."""
        return self._container_status.get("image", "")

    @property
    def is_init_container(self) -> bool:
        """Check if this is an init container."""
        return self._is_init_container

    def get_state(self) -> Tuple[str, Optional[str]]:
        """
        Get the container state and reason.

        :return: Tuple of (state, reason) where state is one of: pending, waiting, running, terminated
        """
        state_obj = self._container_status.get("state", {})

        if state_obj.get("waiting"):
            return "waiting", state_obj.get("waiting", {}).get("reason")
        elif state_obj.get("running"):
            return "running", state_obj.get("running", {}).get("reason")
        elif state_obj.get("terminated"):
            return "terminated", state_obj.get("terminated", {}).get("reason")
        else:
            return "pending", None

    @property
    def exit_code(self) -> Optional[int]:
        """Get the container exit code if terminated."""
        state_obj = self._container_status.get("state", {})
        terminated = state_obj.get("terminated", {})
        return terminated.get("exitCode")

    @property
    def started_time(self) -> Optional[datetime.datetime]:
        """Get the container start time."""
        state_obj = self._container_status.get("state", {})

        # Check running state first
        if state_obj.get("running"):
            started_at = state_obj.get("running", {}).get("startedAt")
            if started_at:
                return datetime.datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=datetime.timezone.utc
                )

        # Check terminated state
        if state_obj.get("terminated"):
            started_at = state_obj.get("terminated", {}).get("startedAt")
            if started_at:
                return datetime.datetime.strptime(started_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=datetime.timezone.utc
                )

        return None

    @property
    def finished_time(self) -> Optional[datetime.datetime]:
        """Get the container finish time if terminated."""
        state_obj = self._container_status.get("state", {})
        terminated = state_obj.get("terminated", {})
        finished_at = terminated.get("finishedAt")
        if finished_at:
            return datetime.datetime.strptime(finished_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc)
        return None

    @property
    def is_terminated(self) -> bool:
        """Check if the container is terminated."""
        return self.get_state()[0] == "terminated"

    @property
    def is_failed(self) -> bool:
        """Check if the container failed (terminated with non-zero exit code)."""
        exit_code = self.exit_code
        return exit_code is not None and exit_code != 0

    def get_log_content(self) -> Optional[str]:
        """Get the pre-fetched log content for this container."""
        return self._log_content

    def get_status_dict(self) -> Dict:
        """Get the raw container status dictionary."""
        return self._container_status

    def __repr__(self):
        state, reason = self.get_state()
        return f"ContainerInfo(name={self.name}, state={state}, exit_code={self.exit_code})"


class PodInfo:
    """Encapsulates information about a Kubernetes Pod as an immutable snapshot."""

    def __init__(self, pod: Union[Dict, resource.ResourceInstance], container_logs: Optional[Dict[str, str]] = None):
        """
        Initialize a PodInfo instance.

        :param pod: The pod dictionary from Kubernetes API or a ResourceInstance
        :param container_logs: Optional dict mapping container names to their log content (pre-fetched)
        """
        # Convert ResourceInstance to dict if needed
        if hasattr(pod, 'to_dict'):
            self._pod_dict = pod.to_dict()
        else:
            self._pod_dict = pod
        self._container_logs = container_logs or {}

    def to_dict(self) -> Dict:
        """Return the pod dictionary representation."""
        return self._pod_dict

    @property
    def name(self) -> str:
        """Get the pod name."""
        return self._pod_dict.get("metadata", {}).get("name", "")

    @property
    def namespace(self) -> str:
        """Get the pod namespace."""
        return self._pod_dict.get("metadata", {}).get("namespace", "")

    @property
    def phase(self) -> str:
        """Get the pod phase (Pending, Running, Succeeded, Failed, Unknown)."""
        return self._pod_dict.get("status", {}).get("phase", "Unknown")

    @property
    def creation_timestamp(self) -> Optional[datetime.datetime]:
        """Get the pod creation timestamp."""
        creation_time_str = self._pod_dict.get("metadata", {}).get("creationTimestamp")
        if creation_time_str:
            return datetime.datetime.strptime(creation_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=datetime.timezone.utc
            )
        return None

    @property
    def start_time(self) -> Optional[datetime.datetime]:
        """Get the pod start time."""
        start_time_str = self._pod_dict.get("status", {}).get("startTime")
        if start_time_str:
            return datetime.datetime.strptime(start_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=datetime.timezone.utc
            )
        return None

    def get_age(self, reference_time: Optional[datetime.datetime] = None) -> datetime.timedelta:
        """
        Get the age of the pod.

        :param reference_time: The reference time to calculate age from (defaults to now)
        :return: The age as a timedelta
        """
        if reference_time is None:
            reference_time = datetime.datetime.now(tz=datetime.timezone.utc)
        creation_time = self.creation_timestamp
        if creation_time:
            return reference_time - creation_time
        return datetime.timedelta(0)

    def get_container_statuses(self) -> List[ContainerInfo]:
        """Get the list of container statuses as ContainerInfo objects."""
        container_statuses = self._pod_dict.get("status", {}).get("containerStatuses", [])
        return [
            ContainerInfo(
                container_status=status,
                is_init_container=False,
                log_content=self._container_logs.get(status.get("name")),
            )
            for status in container_statuses
        ]

    def get_init_container_statuses(self) -> List[ContainerInfo]:
        """Get the list of init container statuses as ContainerInfo objects."""
        init_container_statuses = self._pod_dict.get("status", {}).get("initContainerStatuses", [])
        return [
            ContainerInfo(
                container_status=status,
                is_init_container=True,
                log_content=self._container_logs.get(status.get("name")),
            )
            for status in init_container_statuses
        ]

    def get_all_containers(self) -> List[ContainerInfo]:
        """Get all containers (init and regular) as ContainerInfo objects."""
        return self.get_init_container_statuses() + self.get_container_statuses()

    def get_log_content(self, container_name: str) -> Optional[str]:
        """
        Get the log content for a specific container from the snapshot.

        This returns pre-fetched log content only. Logs are captured at the time
        the PodInfo snapshot was created by the watcher.

        :param container_name: The name of the container
        :return: The log content as a string, or None if not available
        """
        return self._container_logs.get(container_name)

    def find_condition(self, condition_type: str) -> Optional[art_util.KubeCondition]:
        """
        Find a specific condition in the pod status.

        :param condition_type: The type of condition to find (e.g., 'PodScheduled', 'Initialized', 'Ready')
        :return: KubeCondition object if found, None otherwise
        """
        return art_util.KubeCondition.find_condition(self._pod_dict, condition_type)

    def __repr__(self):
        return f"PodInfo(name={self.name}, namespace={self.namespace}, phase={self.phase})"

    def __str__(self):
        """Return a human-readable string representation of the Pod."""
        containers = self.get_all_containers()
        container_count = len(containers)
        failed_containers = [c for c in containers if c.is_failed()]
        failed_count = len(failed_containers)

        status_info = f"phase={self.phase}"
        if failed_count > 0:
            status_info += f", {failed_count}/{container_count} containers failed"
        else:
            status_info += f", {container_count} containers"

        return f"Pod '{self.name}' in {self.namespace}: {status_info}"


class PipelineRunInfo:
    """Encapsulates information about a Konflux PipelineRun."""

    def __init__(self, pipelinerun: Union[Dict, resource.ResourceInstance], pods: Dict[str, PodInfo]):
        """
        Initialize a PipelineRunInfo instance.

        :param pipelinerun: The PipelineRun dictionary from Kubernetes API or a ResourceInstance
        :param pods: Dictionary mapping pod names to PodInfo objects
        """
        # Convert ResourceInstance to dict if needed
        if hasattr(pipelinerun, 'to_dict'):
            self._pipelinerun_dict = pipelinerun.to_dict()
        else:
            self._pipelinerun_dict = pipelinerun
        self._pods = pods

    def to_dict(self) -> Dict:
        """Return the PipelineRun dictionary representation."""
        return self._pipelinerun_dict

    @property
    def name(self) -> str:
        """Get the PipelineRun name."""
        return self._pipelinerun_dict.get("metadata", {}).get("name", "")

    @property
    def namespace(self) -> str:
        """Get the PipelineRun namespace."""
        return self._pipelinerun_dict.get("metadata", {}).get("namespace", "")

    @property
    def labels(self) -> Dict[str, str]:
        """Get the PipelineRun labels."""
        return self._pipelinerun_dict.get("metadata", {}).get("labels", {})

    def find_condition(self, condition_type: str) -> Optional[art_util.KubeCondition]:
        """
        Find a condition by type in the PipelineRun status.

        :param condition_type: The type of condition to find (e.g., 'Succeeded')
        :return: The KubeCondition if found, None otherwise
        """
        return art_util.KubeCondition.find_condition(self._pipelinerun_dict, condition_type)

    def get_pods(self) -> List[PodInfo]:
        """Get the list of PodInfo objects associated with this PipelineRun."""
        return list(self._pods.values())

    def get_pod(self, pod_name: str) -> Optional[PodInfo]:
        """Get a specific PodInfo by name."""
        return self._pods.get(pod_name)

    def get_pod_dicts(self) -> List[Dict]:
        """
        Get the list of pod dictionaries (for backward compatibility).
        Each pod dict includes 'log_output' field with container logs.
        """
        pod_dicts = []
        for pod_info in self._pods.values():
            pod_dict = pod_info.to_dict().copy()
            # For backward compatibility, add log_output field
            log_output = {}
            for container in pod_info.get_all_containers():
                container_name = container.name
                log_content = container.get_log_content()
                if log_content:
                    log_output[container_name] = log_content
            if log_output:
                pod_dict["log_output"] = log_output
            pod_dicts.append(pod_dict)
        return pod_dicts

    def is_terminal(self) -> bool:
        """Check if the PipelineRun is in a terminal state."""
        # Check if the PipelineRun has a completion time
        completion_time = self._pipelinerun_dict.get("status", {}).get("completionTime")
        if completion_time:
            return True

        # Check conditions
        conditions = self._pipelinerun_dict.get("status", {}).get("conditions", [])
        for condition in conditions:
            if condition.get("type") == "Succeeded" and condition.get("status") in ["True", "False"]:
                return True

        return False

    def __repr__(self):
        return f"PipelineRunInfo(name={self.name}, namespace={self.namespace})"

    def __str__(self):
        """Return a human-readable string representation of the PipelineRun."""
        succeeded_condition = self.find_condition('Succeeded')
        status = "Unknown"
        if succeeded_condition:
            if succeeded_condition.status == "True":
                status = "Succeeded"
            elif succeeded_condition.status == "False":
                status = f"Failed ({succeeded_condition.reason})"
            else:
                status = f"Running ({succeeded_condition.reason})"

        pod_count = len(self._pods)
        return f"PipelineRun '{self.name}' in {self.namespace}: {status} with {pod_count} pod(s)"
