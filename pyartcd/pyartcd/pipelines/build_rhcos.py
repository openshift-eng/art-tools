import asyncio
import base64
import json
import re
import sys
import time
from typing import Dict, List, Tuple

import click
import openshift_client as oc
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from pyartcd.cli import cli, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.util import load_group_config

JENKINS_BASE_URL = "https://jenkins-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com"


# lifted verbatim from
# https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, timeout=5, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().send(request, **kwargs)


class BuildRhcosPipeline:
    """Use the Jenkins API to query for existing builds and perhaps kick off a new one and wait for it."""

    def __init__(self, runtime: Runtime, new_build: bool, ignore_running: bool, version: str, job: str):
        self.runtime = runtime
        self.new_build = new_build
        self.ignore_running = ignore_running
        self.version = version.split('-')[0]
        self.job = job
        self.rhel_version = version.split('-')[1] if len(version.split('-')) > 1 else None
        self.api_token = None
        self.dry_run = self.runtime.dry_run
        self.request_session = requests.Session()
        self._stream = self.get_stream()  # rhcos stream the version maps to
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[401, 403, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST"],
            raise_on_status=True,
        )
        self.request_session.mount("https://", TimeoutHTTPAdapter(max_retries=retries))

    def run(self):
        self.request_session.headers.update({"Authorization": f"Bearer {self.retrieve_auth_token()}"})
        current = self.query_existing_builds()
        if self.dry_run:
            print('DRY RUN - Exiting', file=sys.stderr)
        else:
            print(
                json.dumps(
                    {"action": "skip", "builds": current}
                    if current
                    else {"action": "build", "builds": self.start_build()}
                )
            )

    def retrieve_auth_token(self) -> str:
        """Retrieve the auth token from the Jenkins service account to use with Jenkins API"""
        # https://github.com/coreos/fedora-coreos-pipeline/blob/main/HACKING.md#triggering-builds-remotely
        secret = None
        jenkins_uid = oc.selector('sa/jenkins').objects()[0].model.metadata.uid
        for s in oc.selector('secrets'):
            if (
                s.model.type == "kubernetes.io/service-account-token"
                and s.model.metadata.annotations["kubernetes.io/service-account.name"] == "jenkins"
                and s.model.metadata.annotations["kubernetes.io/service-account.uid"] == jenkins_uid
            ):
                secret_maybe = base64.b64decode(s.model.data.token).decode('utf-8')
                r = self.request_session.get(
                    f"{JENKINS_BASE_URL}/me/api/json",
                    headers={"Authorization": f"Bearer {secret_maybe}"},
                )
                if r.status_code == 200:
                    secret = secret_maybe
                    break
        if secret is None:
            raise Exception("Unable to find a valid Jenkins service account token")
        return secret

    def query_existing_builds(self) -> List[Dict]:
        """Check if there are any existing builds for the given job with given version. Returns builds dicts."""
        if self.ignore_running:
            return []
        response = self.request_session.get(
            f"{JENKINS_BASE_URL}/job/{self.job}/api/json?tree=builds[number,description,url,result,actions[parameters[name,value]]]",
        )
        builds_info = response.json()["builds"]
        return [
            {"result": None, "description": build["description"], "url": build["url"]}
            for build in builds_info
            if build["result"] is None
            and any(
                param["name"] == "STREAM" and param["value"] == self._stream  # check build parameter with given stream
                for action in build["actions"]
                if "parameters" in action
                for param in action["parameters"]
            )
        ]

    def get_stream(self):
        """Get rhcos job's stream parameter value, for 4.12+ it looks like 4.x-9.x for 4.12 is just 4.12"""
        group_file = asyncio.run(load_group_config(group=f"openshift-{self.version}", assembly="stream"))
        if "layered_rhcos" in group_file['rhcos'].keys():  # for layered rhcos job
            if self.job == 'build-node-image':  # for build node job release value is 4.x-9.x
                if self.rhel_version:
                    return f"{self.version}-{self.rhel_version}"
                return f"{self.version}-{group_file['vars']['RHCOS_EL_MAJOR']}.{group_file['vars']['RHCOS_EL_MINOR']}"
            else:  # for build job release value is rhel-9.x
                if self.rhel_version:
                    return f"rhel-{self.rhel_version}"
                return f"rhel-{group_file['vars']['RHCOS_EL_MAJOR']}.{group_file['vars']['RHCOS_EL_MINOR']}"
        else:
            return (
                f"{self.version}-{group_file['vars']['RHCOS_EL_MAJOR']}.{group_file['vars']['RHCOS_EL_MINOR']}"
                if group_file['vars']['MINOR'] > 12 and group_file['vars']['MAJOR'] == 4
                else self.version
            )

    def start_build(self):
        """Start a new build for the given version"""
        job_url = f"{JENKINS_BASE_URL}/job/{self.job}/buildWithParameters"
        if self.job == 'build-node-image':
            params = dict(RELEASE=self._stream)
        else:  # buld job params
            params = dict(STREAM=self._stream, EARLY_ARCH_JOBS="false", FORCE=self.new_build)
        build_number = self.trigger_build(job_url, params)
        build = self.request_session.get(f"{JENKINS_BASE_URL}/job/{self.job}/{build_number}/api/json").json()
        return dict(url=build['url'], result=build['result'], description=build['description'])

    def trigger_build(self, job_url, params):
        """trigger build and return build bumber"""
        response = self.request_session.post(job_url, data=params)
        if response.status_code not in (201, 200):
            raise Exception(f"Failed to trigger rhcos build: {response}")
        queue_url = response.headers.get('Location')
        start_time = time.time()
        while time.time() - start_time < 300:
            response = self.request_session.get(f"{queue_url}/api/json").json()
            if 'executable' in response:
                return response['executable']['number']
            time.sleep(5)
        raise Exception("Build didn't start within 300 seconds")


@cli.command("build-rhcos")
@click.option("--version", required=True, type=str, help="The version to build, e.g. '4.13'")
@click.option(
    "--ignore-running",
    required=False,
    default=False,
    type=bool,
    help="Ignore in-progress builds instead of just exiting like usual",
)
@click.option(
    "--new-build",
    required=False,
    default=False,
    type=bool,
    help="Force a new build even if no changes were detected from the last build",
)
@click.option(
    "--job",
    required=False,
    type=click.Choice(['build', 'build-node-image']),
    default="build",
    help="RHCOS pipeline job name",
)
@pass_runtime
def build_rhcos(runtime: Runtime, new_build: bool, ignore_running: bool, version: str, job: str):
    BuildRhcosPipeline(runtime, new_build, ignore_running, version, job).run()
