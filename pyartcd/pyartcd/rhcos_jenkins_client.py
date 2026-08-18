"""Shared RHCOS Jenkins client for authentication and job triggering.

Extracted from pyartcd.pipelines.build_rhcos to allow reuse in other pipelines
(e.g. ocp4-konflux integration test triggering).
"""

import base64
import logging
import os
import time

import openshift_client as oc
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

JENKINS_BASE_URL = "https://jenkins-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com"


# Lifted verbatim from
# https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, timeout=5, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().send(request, **kwargs)


class RhcosJenkinsClient:
    """Client for interacting with the RHCOS Jenkins instance.

    Handles authentication via OpenShift service account tokens and provides
    methods for triggering and monitoring Jenkins builds.

    The kubeconfig_env_var parameter allows callers to specify which environment
    variable holds the kubeconfig path. This is important because ocp4-konflux
    already uses KUBECONFIG for app.ci via withAppCiAsArtPublish(), so we need
    a separate env var (e.g. RHCOS_JENKINS_KUBECONFIG) for the RHCOS cluster.
    """

    def __init__(self, kubeconfig_env_var: str = 'RHCOS_JENKINS_KUBECONFIG'):
        self.kubeconfig_env_var = kubeconfig_env_var
        self._token = None
        self._session = None

    def _get_session(self) -> requests.Session:
        """Create or return a requests session with retry logic."""
        if self._session is None:
            self._session = requests.Session()
            retries = Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=[401, 403, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "POST"],
                raise_on_status=True,
            )
            self._session.mount("https://", TimeoutHTTPAdapter(max_retries=retries))
        return self._session

    def retrieve_auth_token(self) -> str:
        """Retrieve the auth token from the Jenkins service account.

        Uses the kubeconfig specified by self.kubeconfig_env_var to connect to
        the RHCOS cluster and find a valid Jenkins SA token.

        Falls back to the standard KUBECONFIG env var if the primary is not set.

        Returns:
            A valid Bearer token string for Jenkins API authentication.

        Raises:
            Exception: If no valid Jenkins service account token is found.
        """
        if self._token:
            return self._token

        kubeconfig = os.environ.get(self.kubeconfig_env_var) or os.environ.get('KUBECONFIG')
        if not kubeconfig:
            raise Exception(
                f"Neither {self.kubeconfig_env_var} nor KUBECONFIG environment variable is set. "
                "Cannot authenticate to RHCOS Jenkins cluster."
            )

        logger.info("Retrieving RHCOS Jenkins auth token using kubeconfig from %s", self.kubeconfig_env_var)
        session = self._get_session()

        with oc.api_server(oc.get_config_context()['cluster']), oc.options({'kubeconfig': kubeconfig}):
            jenkins_uid = oc.selector('sa/jenkins').objects()[0].model.metadata.uid
            for s in oc.selector('secrets'):
                if (
                    s.model.type == "kubernetes.io/service-account-token"
                    and s.model.metadata.annotations["kubernetes.io/service-account.name"] == "jenkins"
                    and s.model.metadata.annotations["kubernetes.io/service-account.uid"] == jenkins_uid
                ):
                    secret_maybe = base64.b64decode(s.model.data.token).decode('utf-8')
                    r = session.get(
                        f"{JENKINS_BASE_URL}/me/api/json",
                        headers={"Authorization": f"Bearer {secret_maybe}"},
                    )
                    if r.status_code == 200:
                        self._token = secret_maybe
                        logger.info("Successfully authenticated to RHCOS Jenkins")
                        return self._token

        raise Exception("Unable to find a valid Jenkins service account token for RHCOS Jenkins")

    def trigger_build(self, job: str, params: dict) -> int:
        """Trigger a Jenkins build and wait for it to start.

        Posts to buildWithParameters and polls the queue until the build
        gets an executor and a build number is assigned.

        Args:
            job: Jenkins job name (e.g. 'build-node-image').
            params: Build parameters to pass to the job.

        Returns:
            The build number of the started build.

        Raises:
            Exception: If the build fails to start within 300 seconds.
        """
        token = self.retrieve_auth_token()
        session = self._get_session()
        session.headers.update({"Authorization": f"Bearer {token}"})

        job_url = f"{JENKINS_BASE_URL}/job/{job}/buildWithParameters"
        logger.info("Triggering Jenkins build: %s with params %s", job, params)

        response = session.post(job_url, data=params)
        if response.status_code not in (200, 201):
            raise Exception(f"Failed to trigger RHCOS Jenkins build: {response.status_code} {response.text}")

        queue_url = response.headers.get('Location')
        if not queue_url:
            raise Exception("No Location header in Jenkins queue response")

        logger.info("Build queued at %s, waiting for executor...", queue_url)
        start_time = time.time()
        while time.time() - start_time < 300:
            queue_response = session.get(f"{queue_url}/api/json").json()
            if 'executable' in queue_response:
                build_number = queue_response['executable']['number']
                logger.info("Build started: %s #%d", job, build_number)
                return build_number
            time.sleep(5)

        raise Exception(f"Jenkins build for {job} didn't start within 300 seconds")

    def wait_for_build(self, job: str, build_number: int, timeout: int = 1800) -> dict:
        """Wait for a Jenkins build to complete and return its info.

        Polls the build API until the result field is set (build is complete).

        Args:
            job: Jenkins job name.
            build_number: The build number to wait for.
            timeout: Maximum seconds to wait (default: 1800 = 30 minutes).

        Returns:
            A dict with build info including 'result', 'url', and 'description'.

        Raises:
            Exception: If the build does not complete within the timeout.
        """
        token = self.retrieve_auth_token()
        session = self._get_session()
        session.headers.update({"Authorization": f"Bearer {token}"})

        build_url = f"{JENKINS_BASE_URL}/job/{job}/{build_number}/api/json"
        logger.info("Waiting for build %s #%d to complete (timeout: %ds)...", job, build_number, timeout)

        start_time = time.time()
        while time.time() - start_time < timeout:
            build_info = session.get(build_url).json()
            if build_info.get('result') is not None:
                logger.info("Build %s #%d completed with result: %s", job, build_number, build_info['result'])
                return {
                    'result': build_info['result'],
                    'url': build_info.get('url', ''),
                    'description': build_info.get('description', ''),
                }
            time.sleep(15)

        raise Exception(f"Jenkins build {job} #{build_number} did not complete within {timeout} seconds")

    def query_existing_builds(self, job: str, match_params: dict) -> list:
        """Query for in-progress builds matching the given parameters.

        Checks the Jenkins job for builds that are currently running and have
        parameters matching the provided dict.

        Args:
            job: Jenkins job name.
            match_params: Dict of parameter name->value pairs to match against.

        Returns:
            A list of dicts with 'result', 'description', and 'url' for each
            matching in-progress build.
        """
        token = self.retrieve_auth_token()
        session = self._get_session()
        session.headers.update({"Authorization": f"Bearer {token}"})

        response = session.get(
            f"{JENKINS_BASE_URL}/job/{job}/api/json"
            "?tree=builds[number,description,url,result,actions[parameters[name,value]]]",
        )
        builds_info = response.json()["builds"]

        matching = []
        for build in builds_info:
            if build["result"] is not None:
                continue
            for action in build.get("actions", []):
                if "parameters" not in action:
                    continue
                build_params = {p["name"]: p["value"] for p in action["parameters"]}
                if all(build_params.get(k) == v for k, v in match_params.items()):
                    matching.append(
                        {
                            "result": None,
                            "description": build.get("description", ""),
                            "url": build["url"],
                        }
                    )
                    break

        logger.info("Found %d in-progress builds for %s matching %s", len(matching), job, match_params)
        return matching
