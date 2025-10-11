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
from dateutil import parser
from kubernetes import config
from kubernetes.client import ApiClient, Configuration, CoreV1Api
from kubernetes.dynamic import DynamicClient, exceptions, resource

from .pipelinerun_utils import ContainerInfo, PipelineRunInfo, PodInfo

LOGGER = logging.getLogger(__name__)


class KonfluxWatcher:
    """
    A shared watcher that monitors PipelineRuns and associated Pods in a Konflux namespace.
    Do not instantiate this class directly. Use get_shared_watcher instead.

    This class uses a singleton pattern per namespace+config_file combination to efficiently
    poll multiple PipelineRuns from the same doozer invocation using a single daemon thread.

    The watcher automatically removes PipelineRuns from the cache if they disappear from the
    cluster before reaching a terminal state. This prevents callers from waiting indefinitely
    on a PLR that will never update.
    """

    _instances: Dict[str, "KonfluxWatcher"] = {}
    _instances_lock = threading.Lock()

    # Default Maximum time for a PipelineRun to complete before cancelling it.
    # Can be overridden in build metadata.
    DEFAULT_OVERALL_TIMEOUT = datetime.timedelta(hours=5)
    # Maximum time for a pod to stay pending before cancelling the PipelineRun
    DEFAULT_POD_PENDING_TIMEOUT = datetime.timedelta(hours=2)

    def __init__(
        self,
        namespace: str,
        cfg: Configuration,
        event_loop: asyncio.AbstractEventLoop,
        watch_labels: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a KonfluxWatcher.

        :param namespace: The Kubernetes namespace to watch
        :param cfg: Kubernetes Configuration object
        :param event_loop: The asyncio event loop (required)
        :param watch_labels: Optional dict of labels to filter PipelineRuns. If None, watches all PipelineRuns in namespace.
        """
        if not event_loop:
            raise ValueError("event_loop is required")
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

        # Asyncio events for notifying waiters
        self._loop = event_loop
        self._waiter_events: Dict[int, asyncio.Event] = {}  # unique_id -> Event
        self._waiter_events_lock = threading.Lock()
        self._next_waiter_id = 0

        # Daemon thread
        self._stop_event = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True, name=f"KonfluxWatcher-{namespace}")
        self._poll_thread.start()

        label_desc = f"labels={watch_labels}" if watch_labels else "all PipelineRuns"
        self._logger.info(f"Started KonfluxWatcher polling for namespace={namespace}, {label_desc}")

    @staticmethod
    async def get_shared_watcher(
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
        # Get the current event loop
        loop = asyncio.get_event_loop()

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
                KonfluxWatcher._instances[key] = KonfluxWatcher(namespace, cfg, loop, watch_labels)
            return KonfluxWatcher._instances[key]

    def _poll_loop(self):
        """Main polling loop that runs in a daemon thread."""
        while not self._stop_event.is_set():
            try:
                label_desc = f"labels={self._watch_labels}" if self._watch_labels else "all PipelineRuns"
                self._logger.debug(f"Starting poll cycle for namespace={self.namespace}, {label_desc}")

                # Track polling duration
                start_time = time.time()
                self._poll_pipelineruns()
                poll_duration = time.time() - start_time

                # Calculate remaining time to wait (minimum 1 minute between polls)
                wait_time = max(0, 60 - poll_duration)
                if wait_time > 0:
                    self._logger.debug(f"Polling took {poll_duration:.2f}s, waiting {wait_time:.2f}s before next poll")
                    self._stop_event.wait(wait_time)
                else:
                    self._logger.debug(f"Polling took {poll_duration:.2f}s (>60s), starting next poll immediately")

            except Exception as e:
                self._logger.error(f"Error in polling loop: {e}")
                traceback.print_exc()
                # Wait before retrying on error
                self._stop_event.wait(10)

    def _poll_pipelineruns(self):
        """Poll PipelineRuns with optional label filtering."""
        try:
            # Get the API for PipelineRun
            api = self.dyn_client.resources.get(api_version="tekton.dev/v1", kind="PipelineRun")
            pod_api = self.dyn_client.resources.get(api_version="v1", kind="Pod")

            # Build label selector from watch_labels dict
            label_selector = None
            if self._watch_labels:
                label_selector = ",".join([f"{k}={v}" for k, v in self._watch_labels.items()])

            # List all PipelineRuns with the label selector
            list_params = {
                "namespace": self.namespace,
                "_request_timeout": self.request_timeout,
            }
            if label_selector:
                list_params["label_selector"] = label_selector

            pipelineruns = api.get(**list_params)

            # Track which PLRs we've seen in this poll
            seen_plrs = set()

            for plr_instance in pipelineruns.items:
                pipelinerun_name = plr_instance.metadata.name
                seen_plrs.add(pipelinerun_name)
                pipelinerun_dict = plr_instance.to_dict()

                # Update cache
                with self._cache_lock:
                    self._pipelinerun_cache[pipelinerun_name] = pipelinerun_dict

                    # Initialize pod cache for this PLR if needed
                    if pipelinerun_name not in self._pod_cache:
                        self._pod_cache[pipelinerun_name] = {}

                    # Fetch associated pods
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

                                # Only fetch logs for pods that are not in a successful/pending/running state.
                                # We only collect container logs for containers which failed.
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

                                # Store/update pod info
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

                # Check for timeouts and cancel if needed
                self._cancel_if_timed_out(pipelinerun_name, pipelinerun_dict, api)

                # Log PLR status with pod information
                self._log_pipelinerun_status(pipelinerun_name, pipelinerun_dict)

            # Clean up PLRs that disappeared before reaching terminal state
            self._cleanup_disappeared_pipelineruns(seen_plrs)

            # Notify any waiters
            self._notify_waiters()

        except Exception as e:
            if not self._stop_event.is_set():
                self._logger.error(f"Error polling PipelineRuns: {e}")
                traceback.print_exc()

    def _log_pipelinerun_status(self, pipelinerun_name: str, pipelinerun_dict: Dict):
        """Log the status of a PipelineRun and its pods."""
        with self._cache_lock:
            pod_cache_entries = self._pod_cache.get(pipelinerun_name, {})

            # Create PodInfo objects from cached snapshots
            pods = {
                pod_name: PodInfo(pod_dict, container_logs)
                for pod_name, (pod_dict, container_logs) in pod_cache_entries.items()
            }

            info = PipelineRunInfo(pipelinerun_dict, pods)

            # Get status information
            succeeded_condition = info.find_condition("Succeeded")
            succeeded_status = succeeded_condition.status if succeeded_condition else "Not Found"
            succeeded_reason = succeeded_condition.reason if succeeded_condition else "Not Found"

            # Count pod statuses
            successful_pods = sum(1 for p in pods.values() if p.phase == "Succeeded")
            pod_desc = [
                f"\tPod {p.name} [phase={p.phase}][age={p.get_age(datetime.datetime.now(tz=datetime.timezone.utc))}]"
                for p in pods.values()
                if p.phase != "Succeeded"
            ]

            self._logger.info(
                f"Observed PipelineRun {pipelinerun_name} [status={succeeded_status}][reason={succeeded_reason}]; "
                f"pods[total={len(pods)}][successful={successful_pods}]\n" + "\n".join(pod_desc)
            )

    def _cancel_if_timed_out(self, pipelinerun_name: str, pipelinerun_dict: Dict, api):
        """Check for timeout conditions and cancel the PipelineRun if needed."""
        # Skip if PLR is already terminal
        info = PipelineRunInfo(pipelinerun_dict, {})
        if info.is_terminal():
            return

        # Get creation timestamp from the PLR metadata
        creation_timestamp_str = pipelinerun_dict.get('metadata', {}).get('creationTimestamp')
        if creation_timestamp_str:
            # Parse the timestamp (it's in ISO format)
            try:
                start_time = parser.isoparse(creation_timestamp_str)
                current_time = datetime.datetime.now(tz=datetime.timezone.utc)

                should_cancel = False

                # Check for custom timeout annotation
                overall_timeout = self.DEFAULT_OVERALL_TIMEOUT
                annotations = pipelinerun_dict.get('metadata', {}).get('annotations', {})
                timeout_minutes_str = annotations.get('art-overall-timeout-minutes')
                if timeout_minutes_str:
                    try:
                        timeout_minutes = int(timeout_minutes_str)
                        overall_timeout = datetime.timedelta(minutes=timeout_minutes)
                        self._logger.debug(f"Using custom timeout for {pipelinerun_name}: {timeout_minutes} minutes")
                    except ValueError:
                        self._logger.warning(
                            f"Invalid timeout annotation value for {pipelinerun_name}: {timeout_minutes_str}"
                        )

                # Check overall timeout
                if current_time - start_time > overall_timeout:
                    self._logger.error(f"PipelineRun {pipelinerun_name} exceeded overall timeout {overall_timeout}")
                    should_cancel = True
                else:
                    # Check pending pods timeout
                    with self._cache_lock:
                        for pod_name, (pod_dict, _) in self._pod_cache.get(pipelinerun_name, {}).items():
                            pod_info = PodInfo(pod_dict, {})
                            if pod_info.phase == "Pending":
                                age = pod_info.get_age(current_time)
                                if age > self.DEFAULT_POD_PENDING_TIMEOUT:
                                    self._logger.error(
                                        f"Pod {pod_info.name} has been pending for {age}, exceeds threshold {self.DEFAULT_POD_PENDING_TIMEOUT}"
                                    )
                                    should_cancel = True
                                    break  # Don't check other pods after finding one that exceeded timeout

                # Cancel if either timeout condition was met
                if should_cancel:
                    self._logger.info(f"Cancelling PipelineRun {pipelinerun_name}")
                    try:
                        api.patch(
                            name=pipelinerun_name,
                            namespace=self.namespace,
                            body={"spec": {"status": "Cancelled"}},
                            content_type="application/merge-patch+json",
                            _request_timeout=self.request_timeout,
                        )
                    except Exception as e:
                        self._logger.error(f"Failed to cancel PipelineRun {pipelinerun_name}: {e}")
            except Exception as e:
                self._logger.warning(f"Failed to parse creationTimestamp for {pipelinerun_name}: {e}")
        else:
            self._logger.warning(f"PipelineRun {pipelinerun_name} has no creationTimestamp")

    def _cleanup_disappeared_pipelineruns(self, seen_plrs: set):
        """Remove non-terminal PipelineRuns that have disappeared from the cluster.

        :param seen_plrs: Set of PipelineRun names seen in the current poll
        """
        with self._cache_lock:
            # Find PLRs in cache that weren't seen in this poll
            cached_plr_names = list(self._pipelinerun_cache.keys())
            for plr_name in cached_plr_names:
                if plr_name not in seen_plrs:
                    # PLR wasn't seen in this poll - check if it's non-terminal
                    pipelinerun_dict = self._pipelinerun_cache[plr_name]
                    info = PipelineRunInfo(pipelinerun_dict, {})
                    if not info.is_terminal():
                        # Non-terminal PLR has disappeared - remove from cache
                        self._logger.error(
                            f"PipelineRun {plr_name} disappeared before reaching terminal state. "
                            f"Last known status: {info.find_condition('Succeeded')}"
                        )
                        del self._pipelinerun_cache[plr_name]
                        # Also clean up pod cache for this PLR
                        if plr_name in self._pod_cache:
                            del self._pod_cache[plr_name]

    def _notify_waiters(self):
        """Notify all async waiters about cache updates."""
        with self._waiter_events_lock:
            for event in self._waiter_events.values():
                self._loop.call_soon_threadsafe(event.set)

    async def get_pipelinerun_info(self, pipelinerun_name: str) -> PipelineRunInfo:
        """
        Get the current PipelineRunInfo from the cache.

        :param pipelinerun_name: The name of the PipelineRun
        :return: The PipelineRunInfo object
        :raises ValueError: If the PipelineRun is not found in cache after waiting
        """

        # Create a unique event for this caller
        event = asyncio.Event()
        with self._waiter_events_lock:
            waiter_id = self._next_waiter_id
            self._next_waiter_id += 1
            self._waiter_events[waiter_id] = event

        try:
            # Check if PLR is already in cache
            for attempt in range(3):  # Check immediately, then wait for up to 2 polling intervals
                with self._cache_lock:
                    if pipelinerun_name in self._pipelinerun_cache:
                        pipelinerun_dict = self._pipelinerun_cache[pipelinerun_name]
                        pod_cache_entries = self._pod_cache.get(pipelinerun_name, {})

                        # Create PodInfo objects from cached snapshots
                        pods = {
                            pod_name: PodInfo(pod_dict, container_logs)
                            for pod_name, (pod_dict, container_logs) in pod_cache_entries.items()
                        }

                        return PipelineRunInfo(pipelinerun_dict, pods)

                # Wait for next poll notification (except on last iteration)
                if attempt < 2:
                    await event.wait()
                    event.clear()

            # PLR not found after waiting
            raise ValueError(f"PipelineRun {pipelinerun_name} not found")
        finally:
            # Clean up our event
            with self._waiter_events_lock:
                del self._waiter_events[waiter_id]

    async def get_pipelinerun_infos(self) -> List[PipelineRunInfo]:
        """
        Get all PipelineRunInfo objects from the cache.

        :return: List of PipelineRunInfo objects for all cached PipelineRuns
        """
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

    async def wait_for_pipelinerun_termination(
        self,
        pipelinerun_name: str,
    ) -> PipelineRunInfo:
        """
        Wait for a PipelineRun to reach a terminal state.

        :param pipelinerun_name: The name of the PipelineRun
        :return: The PipelineRunInfo object
        :raises ValueError: If the PipelineRun is not found
        """
        not_found_count = 0

        # Create a unique event for this caller
        event = asyncio.Event()
        with self._waiter_events_lock:
            waiter_id = self._next_waiter_id
            self._next_waiter_id += 1
            self._waiter_events[waiter_id] = event

        try:
            while True:
                with self._cache_lock:
                    if pipelinerun_name not in self._pipelinerun_cache:
                        not_found_count += 1
                        if not_found_count >= 2:  # Wait for 2 polling intervals
                            raise ValueError(f"PipelineRun {pipelinerun_name} not found")
                    else:
                        not_found_count = 0  # Reset counter when found

                        pipelinerun_dict = self._pipelinerun_cache[pipelinerun_name]
                        pod_cache_entries = self._pod_cache.get(pipelinerun_name, {})

                        # Create PipelineRunInfo from cached snapshots
                        pods = {
                            pod_name: PodInfo(pod_dict, container_logs)
                            for pod_name, (pod_dict, container_logs) in pod_cache_entries.items()
                        }
                        info = PipelineRunInfo(pipelinerun_dict, pods)

                        # Log current status
                        succeeded_condition = info.find_condition("Succeeded")
                        succeeded_status = succeeded_condition.status if succeeded_condition else "Not Found"
                        succeeded_reason = succeeded_condition.reason if succeeded_condition else "Not Found"

                        self._logger.info(
                            f"PipelineRun {pipelinerun_name} status check: "
                            f"[status={succeeded_status}][reason={succeeded_reason}][pods={len(pods)}]"
                        )

                        # Check for terminal state
                        if info.is_terminal():
                            self._logger.info(f"PipelineRun {pipelinerun_name} reached terminal state")
                            # Log final information before returning
                            self._logger.info(
                                f"Returning final PipelineRunInfo for {pipelinerun_name}: "
                                f"status={succeeded_status}, reason={succeeded_reason}, "
                                f"total_pods={len(pods)}, pod_phases={dict((p.name, p.phase) for p in pods.values())}"
                            )
                            return info

                # Wait for next poll notification
                await event.wait()
                event.clear()
        finally:
            # Clean up our event
            with self._waiter_events_lock:
                del self._waiter_events[waiter_id]

    def stop(self):
        """Stop the polling thread."""
        self._logger.info(f"Stopping KonfluxWatcher for namespace={self.namespace}")
        self._stop_event.set()
        self._poll_thread.join(timeout=10)
