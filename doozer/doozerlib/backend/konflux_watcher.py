import asyncio
import datetime
import hashlib
import json
import logging
import threading
import time
import traceback
from typing import Dict, List, Optional, Tuple

from artcommonlib import exectools
from artcommonlib import util as art_util
from kubernetes import config, watch
from kubernetes.client import ApiClient, Configuration, CoreV1Api
from kubernetes.dynamic import DynamicClient, exceptions, resource

from .pipelinerun_utils import ContainerInfo, PipelineRunInfo, PodInfo

LOGGER = logging.getLogger(__name__)


class KonfluxWatcher:
    """
    A shared watcher that monitors PipelineRuns and associated Pods in a Konflux namespace.

    This class uses a singleton pattern per namespace+config_file combination to efficiently
    watch multiple PipelineRuns from the same doozer invocation using a single daemon thread.
    """

    _instances: Dict[str, "KonfluxWatcher"] = {}
    _instances_lock = threading.Lock()

    def __init__(
        self,
        namespace: str,
        cfg: Configuration,
        watch_labels: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a KonfluxWatcher.

        :param namespace: The Kubernetes namespace to watch
        :param cfg: Kubernetes Configuration object
        :param watch_labels: Optional dict of labels to filter PipelineRuns. If None, watches all PipelineRuns in namespace.
        """
        self.namespace = namespace
        self._logger = LOGGER
        self._watch_labels = watch_labels

        # Initialize Kubernetes clients from Configuration
        self.api_client = ApiClient(configuration=cfg)
        self.dyn_client = DynamicClient(self.api_client)
        self.corev1_client = CoreV1Api(self.api_client)
        self.request_timeout = 60 * 5  # 5 minutes

        # Cache for PipelineRun and Pod information
        self._cache_lock = threading.RLock()
        self._pipelinerun_cache: Dict[str, Dict] = {}  # pipelinerun_name -> pipelinerun_dict
        self._pod_cache: Dict[
            str, Dict[str, Tuple[Dict, Dict[str, str]]]
        ] = {}  # pipelinerun_name -> {pod_name -> (pod_dict, container_logs)}

        # Event for signaling cache updates
        self._update_events: Dict[str, threading.Event] = {}  # pipelinerun_name -> Event

        # Daemon thread
        self._stop_event = threading.Event()
        self._watcher_thread = threading.Thread(
            target=self._watch_loop, daemon=True, name=f"KonfluxWatcher-{namespace}"
        )
        self._watcher_thread.start()

        label_desc = f"labels={watch_labels}" if watch_labels else "all PipelineRuns"
        self._logger.info(f"Started KonfluxWatcher for namespace={namespace}, {label_desc}")
        time.sleep(10)
        self._logger.info(f"KonfluxWatcher initialized for namespace={namespace}, {label_desc}")

    @staticmethod
    def get_shared_watcher(
        namespace: str,
        cfg: Configuration,
        watch_labels: Optional[Dict[str, str]] = None,
    ) -> "KonfluxWatcher":
        """
        Get or create a shared KonfluxWatcher instance.

        :param namespace: The Kubernetes namespace
        :param cfg: Kubernetes Configuration object
        :param watch_labels: Optional dict of labels to filter PipelineRuns. If None, watches all PipelineRuns in namespace.
        :return: A shared KonfluxWatcher instance
        """
        # Create a stable hash from namespace, cfg.host, and watch_labels
        # Dicts with same keys/values should produce the same hash
        key_parts = [namespace, cfg.host or "default"]
        if watch_labels:
            # Sort the labels for stable hashing
            labels_str = json.dumps(watch_labels, sort_keys=True)
            key_parts.append(labels_str)
        else:
            key_parts.append("no-labels")

        key = hashlib.sha256(":".join(key_parts).encode()).hexdigest()

        with KonfluxWatcher._instances_lock:
            if key not in KonfluxWatcher._instances:
                KonfluxWatcher._instances[key] = KonfluxWatcher(namespace, cfg, watch_labels)
            return KonfluxWatcher._instances[key]

    def _watch_loop(self):
        """Main watch loop that runs in a daemon thread."""
        while not self._stop_event.is_set():
            try:
                label_desc = f"labels={self._watch_labels}" if self._watch_labels else "all PipelineRuns"
                self._logger.debug(f"Starting watch loop for namespace={self.namespace}, {label_desc}")
                self._watch_pipelineruns()
            except Exception as e:
                self._logger.error(f"Error in watch loop: {e}")
                traceback.print_exc()
                # Wait before retrying
                time.sleep(10)

    def _watch_pipelineruns(self):
        """Watch PipelineRuns with optional label filtering."""
        try:
            # Get the API for PipelineRun
            api = self.dyn_client.resources.get(api_version="tekton.dev/v1", kind="PipelineRun")
            pod_api = self.dyn_client.resources.get(api_version="v1", kind="Pod")

            watcher = watch.Watch()

            # Build label selector from watch_labels dict
            label_selector = None
            if self._watch_labels:
                label_selector = ",".join([f"{k}={v}" for k, v in self._watch_labels.items()])

            # Build watch parameters
            # Use serialize=False to get plain dicts
            watch_params = {
                "namespace": self.namespace,
                "serialize": False,
                "resource_version": 0,
                "timeout_seconds": 60,
                "_request_timeout": self.request_timeout,
            }
            if label_selector:
                watch_params["label_selector"] = label_selector

            for event in watcher.stream(api.get, **watch_params):
                if self._stop_event.is_set():
                    watcher.stop()
                    break

                event_type = event["type"]  # ADDED, MODIFIED, DELETED
                obj = event["object"]  # This is a plain dict when serialize=False

                # Get live version of the object
                pipelinerun_name = obj.get('metadata', {}).get('name')
                try:
                    live_obj = api.get(
                        name=pipelinerun_name,
                        namespace=self.namespace,
                        serialize=True,
                        _request_timeout=self.request_timeout,
                    )
                    # Convert ResourceInstance to plain dict
                    pipelinerun_dict = live_obj.to_dict()
                except exceptions.NotFoundError:
                    # PipelineRun was deleted
                    pipelinerun_dict = None

                # Update cache
                with self._cache_lock:
                    if pipelinerun_dict:
                        self._pipelinerun_cache[pipelinerun_name] = pipelinerun_dict
                    # If PipelineRun was garbage collected (pipelinerun_dict is None),
                    # we simply don't update the cache - keeping the last known snapshot

                    # Fetch associated pods
                    if pipelinerun_name not in self._pod_cache:
                        self._pod_cache[pipelinerun_name] = {}

                    try:
                        pods = pod_api.get(
                            namespace=self.namespace,
                            label_selector=f"tekton.dev/pipelineRun={pipelinerun_name}",
                            _request_timeout=self.request_timeout,
                        )
                        for pod_instance in pods.items:
                            # Create temporary PodInfo to use its parsing capabilities
                            temp_pod_info = PodInfo(pod_instance)
                            pod_name = temp_pod_info.name
                            if pod_name:
                                pod_dict = temp_pod_info.get_snapshot()
                                container_logs = {}

                                # Only fetch logs for pods that are not in a successful/pending/running state
                                if temp_pod_info.phase not in ["Succeeded", "Pending", "Running"]:
                                    # Check all containers (init and regular) for failures
                                    for container in temp_pod_info.get_all_containers():
                                        if container.is_failed:
                                            # Fetch logs for failed containers
                                            try:
                                                log_content = self.corev1_client.read_namespaced_pod_log(
                                                    name=pod_name,
                                                    namespace=self.namespace,
                                                    container=container.name,
                                                    _request_timeout=self.request_timeout,
                                                )
                                                container_logs[container.name] = log_content
                                                self._logger.debug(
                                                    f"Fetched logs for pod {pod_name} container {container.name}"
                                                )
                                            except Exception as log_err:
                                                self._logger.warning(
                                                    f"Failed to fetch logs for pod {pod_name} container {container.name}: {log_err}"
                                                )

                                # Create PodInfo snapshot with pre-fetched logs
                                # Store in cache - don't overwrite if we already have it, as we want to retain history
                                if pod_name not in self._pod_cache[pipelinerun_name]:
                                    self._pod_cache[pipelinerun_name][pod_name] = (pod_dict, container_logs)
                                else:
                                    # Update with latest snapshot (pod may have progressed)
                                    # Merge logs - keep any previously fetched logs
                                    existing_dict, existing_logs = self._pod_cache[pipelinerun_name][pod_name]
                                    merged_logs = {**existing_logs, **container_logs}
                                    self._pod_cache[pipelinerun_name][pod_name] = (pod_dict, merged_logs)
                    except Exception as e:
                        self._logger.warning(f"Error fetching pods for PipelineRun {pipelinerun_name}: {e}")

                    # Signal any waiting threads
                    if pipelinerun_name in self._update_events:
                        self._update_events[pipelinerun_name].set()

                self._logger.info(
                    f"Updated cache for PipelineRun {pipelinerun_name} (event={event_type}, exists={pipelinerun_dict is not None})"
                )

        except Exception as e:
            if not self._stop_event.is_set():
                self._logger.error(f"Error watching PipelineRuns: {e}")
                traceback.print_exc()

    async def get_pipelinerun_info(self, pipelinerun_name: str) -> PipelineRunInfo:
        """
        Get the current PipelineRunInfo from the cache.

        :param pipelinerun_name: The name of the PipelineRun
        :return: The PipelineRunInfo object
        :raises ValueError: If the PipelineRun is not found in cache
        """

        def _get_from_cache():
            with self._cache_lock:
                if pipelinerun_name not in self._pipelinerun_cache:
                    raise ValueError(f"PipelineRun {pipelinerun_name} not found in cache")

                pipelinerun_dict = self._pipelinerun_cache[pipelinerun_name]
                pod_cache_entries = self._pod_cache.get(pipelinerun_name, {})

                # Create PodInfo objects from cached snapshots
                pods = {
                    pod_name: PodInfo(pod_dict, container_logs)
                    for pod_name, (pod_dict, container_logs) in pod_cache_entries.items()
                }

                return PipelineRunInfo(pipelinerun_dict, pods)

        return await exectools.to_thread(_get_from_cache)

    async def get_pipelinerun_infos(self) -> List[PipelineRunInfo]:
        """
        Get all PipelineRunInfo objects from the cache.

        :return: List of PipelineRunInfo objects for all cached PipelineRuns
        """

        def _get_all_from_cache():
            with self._cache_lock:
                infos = []
                for pipelinerun_name, pipelinerun_dict in self._pipelinerun_cache.items():
                    pod_cache_entries = self._pod_cache.get(pipelinerun_name, {})

                    # Create PodInfo objects from cached snapshots
                    pods = {
                        pod_name: PodInfo(pod_dict, container_logs)
                        for pod_name, (pod_dict, container_logs) in pod_cache_entries.items()
                    }

                    infos.append(PipelineRunInfo(pipelinerun_dict, pods))

                return infos

        return await exectools.to_thread(_get_all_from_cache)

    async def wait_for_pipelinerun_termination(
        self,
        pipelinerun_name: str,
        overall_timeout_timedelta: datetime.timedelta,
        pod_pending_timeout_timedelta: datetime.timedelta,
    ) -> PipelineRunInfo:
        """
        Wait for a PipelineRun to reach a terminal state.

        :param pipelinerun_name: The name of the PipelineRun
        :param overall_timeout_timedelta: Maximum time to wait for completion
        :param pod_pending_timeout_timedelta: Maximum time to wait for pending pods
        :return: The PipelineRunInfo object
        :raises ValueError: If the PipelineRun is not found
        :raises TimeoutError: If timeouts are exceeded
        """

        def _wait():
            # Create an event for this PipelineRun if it doesn't exist
            with self._cache_lock:
                if pipelinerun_name not in self._update_events:
                    self._update_events[pipelinerun_name] = threading.Event()
                event = self._update_events[pipelinerun_name]

            start_time = datetime.datetime.now(tz=datetime.timezone.utc)
            timeout_datetime = start_time + overall_timeout_timedelta
            should_cancel = False

            while True:
                current_time = datetime.datetime.now(tz=datetime.timezone.utc)

                with self._cache_lock:
                    if pipelinerun_name not in self._pipelinerun_cache:
                        # Wait a bit for the PipelineRun to appear in cache
                        if (current_time - start_time).total_seconds() < 30:
                            event.clear()
                            event.wait(timeout=5)
                            continue
                        raise ValueError(f"PipelineRun {pipelinerun_name} not found")

                    pipelinerun_dict = self._pipelinerun_cache[pipelinerun_name]
                    pod_cache_entries = self._pod_cache.get(pipelinerun_name, {})

                    # Create PipelineRunInfo from cached snapshots
                    pods = {
                        pod_name: PodInfo(pod_dict, container_logs)
                        for pod_name, (pod_dict, container_logs) in pod_cache_entries.items()
                    }
                    info = PipelineRunInfo(pipelinerun_dict, pods)

                    # Check for terminal state
                    if info.is_terminal():
                        self._logger.info(f"PipelineRun {pipelinerun_name} reached terminal state")
                        return info

                    # Check for pending pods timeout
                    for pod_info in pods.values():
                        if pod_info.phase == "Pending":
                            age = pod_info.get_age(current_time)
                            if age > pod_pending_timeout_timedelta:
                                self._logger.error(
                                    f"Pod {pod_info.name} has been pending for {age}, exceeds threshold {pod_pending_timeout_timedelta}"
                                )
                                should_cancel = True
                                break

                    # Check overall timeout
                    if current_time > timeout_datetime:
                        self._logger.error(
                            f"PipelineRun {pipelinerun_name} exceeded overall timeout {overall_timeout_timedelta}"
                        )
                        should_cancel = True

                    # Cancel the PipelineRun if needed
                    if should_cancel:
                        self._logger.info(f"Cancelling PipelineRun {pipelinerun_name}")
                        try:
                            api = self.dyn_client.resources.get(api_version="tekton.dev/v1", kind="PipelineRun")
                            api.patch(
                                name=pipelinerun_name,
                                namespace=self.namespace,
                                body={"spec": {"status": "Cancelled"}},
                                content_type="application/merge-patch+json",
                                _request_timeout=self.request_timeout,
                            )
                        except Exception as e:
                            self._logger.error(f"Failed to cancel PipelineRun {pipelinerun_name}: {e}")

                        # Wait a bit for the cancellation to take effect
                        time.sleep(5)
                        # Return the current state
                        return info

                # Log current state
                succeeded_condition = info.find_condition("Succeeded")
                succeeded_status = succeeded_condition.status if succeeded_condition else "Not Found"
                succeeded_reason = succeeded_condition.reason if succeeded_condition else "Not Found"

                successful_pods = sum(1 for p in pods.values() if p.phase == "Succeeded")
                pod_desc = [
                    f"\tPod {p.name} [phase={p.phase}][age={p.get_age(current_time)}]"
                    for p in pods.values()
                    if p.phase != "Succeeded"
                ]

                self._logger.info(
                    f"PipelineRun {pipelinerun_name} [status={succeeded_status}][reason={succeeded_reason}]; "
                    f"pods[total={len(pods)}][successful={successful_pods}]\n" + "\n".join(pod_desc)
                )

                # Wait for next update
                event.clear()
                event.wait(timeout=60)  # Wake up every minute even if no updates

        return await exectools.to_thread(_wait)

    def stop(self):
        """Stop the watcher thread."""
        self._logger.info(f"Stopping KonfluxWatcher for namespace={self.namespace}")
        self._stop_event.set()
        self._watcher_thread.join(timeout=10)
