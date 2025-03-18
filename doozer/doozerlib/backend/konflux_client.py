import os
import asyncio
import logging
import datetime
import random
import time
from typing import Dict, List, Optional, Sequence, Union, cast
import traceback

import aiohttp
import jinja2
from async_lru import alru_cache
from kubernetes import config, watch
from kubernetes.client import ApiClient, Configuration, CoreV1Api
from kubernetes.dynamic import DynamicClient, exceptions, resource
from ruamel.yaml import YAML

from artcommonlib import exectools
from artcommonlib import util as art_util
from doozerlib import constants

yaml = YAML(typ="safe")
LOGGER = logging.getLogger(__name__)

API_VERSION = "appstudio.redhat.com/v1alpha1"
KIND_SNAPSHOT = "Snapshot"
KIND_COMPONENT = "Component"
KIND_APPLICATION = "Application"


class KonfluxClient:
    """
    KonfluxClient is a client for interacting with the Konflux API.
    """
    # https://konflux.pages.redhat.com/docs/users/getting-started/multi-platform-builds.html
    # The arch to Konflux VM name mapping. The specs for each of the VMs can be seen in the doc link shared above.
    SUPPORTED_ARCHES = {
        "x86_64": ["linux/x86_64"],
        "s390x": ["linux-large/s390x"],
        "ppc64le": ["linux-large/ppc64le", "linux/ppc64le"],
        "aarch64": ["linux/arm64"],
    }

    def __init__(self, default_namespace: str, config: Configuration, dry_run: bool = False, logger: logging.Logger = LOGGER) -> None:
        self.api_client = ApiClient(configuration=config)
        self.dyn_client = DynamicClient(self.api_client)
        self.corev1_client = CoreV1Api(self.api_client)
        self.default_namespace = default_namespace
        self.dry_run = dry_run
        self._logger = logger

    def verify_connection(self):
        try:
            self.corev1_client.get_api_resources()
            self._logger.info("Successfully authenticated to the Kubernetes cluster.")
        except Exception as e:
            self._logger.error(f"Failed to authenticate to the Kubernetes cluster: {e}")
            raise

    @staticmethod
    def from_kubeconfig(default_namespace: str, config_file: Optional[str], context: Optional[str], dry_run: bool = False, logger: logging.Logger = LOGGER) -> "KonfluxClient":
        """ Create a KonfluxClient from a kubeconfig file.

        :param config_file: The path to the kubeconfig file.
        :param context: The context to use.
        :param default_namespace: The default namespace.
        :param dry_run: Whether to run in dry-run mode.
        :param logger: The logger.
        :return: The KonfluxClient.
        """
        cfg = Configuration()
        config.load_kube_config(config_file=config_file, context=context, persist_config=False, client_configuration=cfg)
        return KonfluxClient(default_namespace=default_namespace, config=cfg, dry_run=dry_run, logger=logger)

    @alru_cache
    async def _get_api(self, api_version: str, kind: str):
        """ Get the API object for the given API version and kind.

        :param api_version: The API version.
        :param kind: The kind.
        :return: The API object.
        """
        api = await exectools.to_thread(
            self.dyn_client.resources.get,
            api_version=api_version,
            kind=kind,
        )
        return api

    async def _get_corev1(self):
        return self.corev1_client

    def _extract_manifest_metadata(self, manifest: dict):
        """ Extract the metadata from a manifest.

        :param manifest: The manifest.
        :return: The API version, kind, name, and namespace.
        """
        api_version = manifest["apiVersion"]
        kind = manifest["kind"]
        name = manifest["metadata"].get("name")
        namespace = manifest["metadata"].get("namespace", self.default_namespace)
        return api_version, kind, name, namespace

    async def _get(self, api_version: str, kind: str, name: str,
                   namespace: Optional[str] = None, strict: bool = True):
        """ Get a resource by name and namespace.

        :param api_version: The API version.
        :param kind: The kind.
        :param name: The name of the resource.
        :param namespace: The namespace of the resource.
        :param strict: Whether to raise an exception if the resource is not found.
        :return: The resource.
        """
        api = await self._get_api(api_version, kind)
        resource = None
        try:
            resource = await exectools.to_thread(api.get, name=name, namespace=namespace or self.default_namespace)
        except exceptions.NotFoundError:
            if strict:
                raise
        return resource

    @alru_cache
    async def _get__caching(self, api_version: str, kind: str, name: str,
                            namespace: Optional[str] = None, strict: bool = True):
        """ Get a resource by name and namespace, with caching.

        :param api_version: The API version.
        :param kind: The kind.
        :param name: The name of the resource.
        :param namespace: The namespace of the resource.
        :param strict: Whether to raise an exception if the resource is not found.
        :return: The resource.
        """
        return await self._get(api_version, kind, name, namespace, strict)

    async def _create(self, manifest: dict, **kwargs):
        """ Create a resource.

        :param manifest: The manifest.
        :param kwargs: Additional keyword arguments to pass to the API.
        :return: The resource.
        """
        api_version, kind, name, namespace = self._extract_manifest_metadata(manifest)
        api = await self._get_api(api_version, kind)
        if self.dry_run:
            self._logger.warning(f"[DRY RUN] Would have created {api_version}/{kind} {namespace}/{name}")
            return resource.ResourceInstance(self.dyn_client, manifest)
        self._logger.info(f"Creating {api_version}/{kind} {namespace}/{name or '<dynamic>'}...")
        new = await exectools.to_thread(api.create, namespace=namespace, body=manifest, **kwargs)
        new = cast(resource.ResourceInstance, new)
        api_version, kind, name, namespace = self._extract_manifest_metadata(new.to_dict())
        self._logger.info(f"Created {api_version}/{kind} {namespace}/{name}")
        return new

    async def _patch(self, manifest: dict):
        """ Patch a resource.

        :param manifest: The manifest.
        :return: The resource.
        """
        api_version, kind, name, namespace = self._extract_manifest_metadata(manifest)
        api = await self._get_api(api_version, kind)
        if self.dry_run:
            self._logger.warning(f"[DRY RUN] Would have patched {api_version}/{kind} {namespace}/{name}")
            return resource.ResourceInstance(self.dyn_client, manifest)
        self._logger.info(f"Patching {api_version}/{kind} {namespace}/{name}")
        new = await exectools.to_thread(
            api.patch,
            body=manifest,
            namespace=namespace,
            content_type="application/merge-patch+json",
        )
        new = cast(resource.ResourceInstance, new)
        api_version, kind, name, namespace = self._extract_manifest_metadata(new.to_dict())
        self._logger.info(f"Patched {api_version}/{kind} {namespace}/{name}")
        return new

    async def _replace(self, manifest: dict):
        """ Replace a resource.

        :param manifest: The manifest.
        :return: The resource.
        """
        api_version, kind, name, namespace = self._extract_manifest_metadata(manifest)
        api = await self._get_api(api_version, kind)
        if self.dry_run:
            self._logger.warning(f"[DRY RUN] Would have replaced {api_version}/{kind} {namespace}/{name}")
            return resource.ResourceInstance(self.dyn_client, manifest)
        self._logger.info(f"Replacing {api_version}/{kind} {namespace}/{name}")
        new = await exectools.to_thread(api.replace, body=manifest, namespace=namespace)
        new = cast(resource.ResourceInstance, new)
        api_version, kind, name, namespace = self._extract_manifest_metadata(new.to_dict())
        self._logger.info(f"Replaced {api_version}/{kind} {namespace}/{name}")
        return new

    async def _delete(self, api_version: str, kind: str, name: str, namespace: str):
        """ Delete a resource.

        :param api_version: The API version.
        :param kind: The kind.
        :param name: The name of the resource.
        :param namespace: The namespace of the resource.
        """
        api = await self._get_api(api_version, kind)
        if self.dry_run:
            self._logger.warning(f"[DRY RUN] Would have deleted {api_version}/{kind} {namespace}/{name}")
            return
        self._logger.info(f"Deleting {api_version}/{kind} {namespace}/{name}")
        await exectools.to_thread(api.delete, name=name, namespace=namespace)
        self._logger.info(f"Deleted {api_version}/{kind} {namespace}/{name}")

    async def _create_or_patch(self, manifest: dict):
        """ Create or patch a resource.

        :param manifest: The manifest.
        :return: The resource.
        """
        api_version, kind, name, namespace = self._extract_manifest_metadata(manifest)
        resource = await self._get(api_version, kind, name, namespace, strict=False)
        if not resource:
            try:
                return await self._create(manifest)
            except exceptions.ConflictError as e:
                if "already exists" in e.summary():
                    # This indicates this resource has been created by another process; ignore the error
                    LOGGER.debug("Error creating %s/%s %s/%s because it already exists; ignoring", api_version, kind, namespace, name)
                    return await self._get(api_version, kind, name, namespace, strict=True)
                raise
        return await self._patch(manifest)

    async def _create_or_replace(self, manifest: dict):
        """ Create or replace a resource.

        :param manifest: The manifest.
        :return: The resource.
        """
        api_version, kind, name, namespace = self._extract_manifest_metadata(manifest)
        resource = await self._get(api_version, kind, name, namespace, strict=False)
        if not resource:
            return await self._create(manifest)
        if not manifest.get("metadata", {}).get("resourceVersion"):
            # resourceVersion is required for replace
            manifest.setdefault("metadata", {})["resourceVersion"] = resource.metadata.resourceVersion
        while True:
            try:
                return await self._replace(manifest)
            except exceptions.ConflictError:
                # If the resource has changed since we fetched it, retry
                resource = await self._get(api_version, kind, name, namespace, strict=False)
                manifest["metadata"]["resourceVersion"] = resource.metadata.resourceVersion
                continue

    @staticmethod
    def _new_application(name: str, display_name: str) -> dict:
        obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_APPLICATION,
            "metadata": {
                "name": name
            },
            "spec": {
                "displayName": display_name,
                "appModelRepository": {"url": ""},
                "gitOpsRepository": {"url": ""},
            }
        }
        return obj

    async def ensure_application(self, name: str, display_name: str) -> resource.ResourceInstance:
        application = self._new_application(name, display_name)
        return await self._create_or_patch(application)

    @staticmethod
    def _new_component(name: str, application: str, component_name: str,
                       image_repo: Optional[str], source_url: Optional[str], revision: Optional[str]) -> dict:
        obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_COMPONENT,
            "metadata": {
                "name": name,
                "annotations": {
                    "build.appstudio.openshift.io/pipeline": '{"name":"docker-build-multi-platform-oci-ta","bundle":"latest"}',
                    "build.appstudio.openshift.io/status": '{"pac":{"state":"disabled"}}',  # will raise PRs to upstream repos (openshift-priv) if this is not set to false
                    # "build.appstudio.openshift.io/request": "configure-pac",
                    "mintmaker.appstudio.redhat.com/disabled": "true",  # https://gitlab.cee.redhat.com/konflux/docs/users/-/blob/main/topics/mintmaker/user.md#offboarding-a-repository
                }
            },
            "spec": {
                "application": application,
                "componentName": component_name,
                "source": {
                    "git": {
                        "url": source_url,
                        "revision": revision,
                    }
                }
            }
        }
        if image_repo:
            obj["spec"]["containerImage"] = image_repo
        if source_url:
            obj["spec"].setdefault("source", {}).setdefault("git", {})["url"] = source_url
        if revision:
            obj["spec"].setdefault("source", {}).setdefault("git", {})["revision"] = revision
        return obj

    async def ensure_component(self, name: str, application: str, component_name: str,
                               image_repo: Optional[str], source_url: Optional[str], revision: Optional[str]) -> resource.ResourceInstance:
        component = self._new_component(name, application, component_name, image_repo, source_url, revision)
        return await self._create_or_replace(component)

    @alru_cache
    async def _get_pipelinerun_template(self, template_url: str):
        """ Get a PipelineRun template.

        :param template_url: The URL to the template.
        :return: The template.
        """
        self._logger.info(f"Pulling Konflux PLR template from: {template_url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(template_url) as response:
                response.raise_for_status()
                template_text = await response.text()
                template = jinja2.Template(template_text, autoescape=True)
                return template

    async def _new_pipelinerun_for_image_build(self, generate_name: str, namespace: Optional[str], application_name: str, component_name: str,
                                               git_url: str, commit_sha: str, target_branch: str, output_image: str,
                                               build_platforms: Sequence[str], prefetch: Optional[list] = None, git_auth_secret: str = "pipelines-as-code-secret",
                                               additional_tags: Optional[Sequence[str]] = None, skip_checks: bool = False,
                                               hermetic: Optional[bool] = None,
                                               dockerfile: Optional[str] = None,
                                               pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL) -> dict:
        if additional_tags is None:
            additional_tags = []
        https_url = art_util.convert_remote_git_to_https(git_url)

        template = await self._get_pipelinerun_template(pipelinerun_template_url)
        rendered = template.render({
            "source_url": https_url,
            "revision": commit_sha,
            "target_branch": target_branch,
            "git_auth_secret": git_auth_secret,

        })
        obj = yaml.load(rendered)
        # Those fields in the template are specific to an image. They need to be removed.
        del obj["metadata"]["name"]
        del obj["metadata"]["annotations"]["pipelinesascode.tekton.dev/on-cel-expression"]
        # Override the generated name with the provided one
        if generate_name:
            obj["metadata"]["generateName"] = generate_name
        if namespace:
            obj["metadata"]["namespace"] = namespace
        else:
            del obj["metadata"]["namespace"]
        # Set the application and component names
        obj["metadata"]["annotations"]["build.appstudio.openshift.io/repo"] = f"{https_url}?rev={commit_sha}"
        obj["metadata"]["annotations"]["art-jenkins-job-url"] = os.getenv("BUILD_URL", "n/a")
        obj["metadata"]["labels"]["appstudio.openshift.io/application"] = application_name
        obj["metadata"]["labels"]["appstudio.openshift.io/component"] = component_name

        def _modify_param(params: List, name: str, value: Union[str, bool, list[str]]):
            """ Modify a parameter in the params list. If the parameter does not exist, it is added.

            :param params: The list of parameters.
            :param name: The name of the parameter.
            :param value: The value of the parameter.
            """
            if isinstance(value, bool):
                value = "true" if value else "false"  # boolean value should be converted to string
            for param in params:
                if param["name"] == name:
                    param["value"] = value
                    return
            params.append({"name": name, "value": value})

        # PipelineRun parameters to override in the template
        params = obj["spec"]["params"]
        _modify_param(params, "output-image", output_image)
        _modify_param(params, "skip-checks", skip_checks)
        _modify_param(params, "build-source-image", "true")  # Have to be true always to satisfy Enterprise Contract Policy
        _modify_param(params, "build-platforms", list(build_platforms))
        if dockerfile:
            _modify_param(params, "dockerfile", dockerfile)

        if prefetch:
            _modify_param(params, "prefetch-input", prefetch)
        if hermetic is not None:
            _modify_param(params, "hermetic", hermetic)

        # See https://konflux-ci.dev/docs/how-tos/configuring/customizing-the-build/#configuring-timeouts
        obj["spec"]["timeouts"] = {"pipeline": "12h"}

        # Task specific parameters to override in the template
        has_build_images_task = False
        for task in obj["spec"]["pipelineSpec"]["tasks"]:
            match task["name"]:
                case "build-images":
                    has_build_images_task = True
                    task["timeout"] = "12h"
                    _modify_param(task["params"], "SBOM_TYPE", "cyclonedx")
                case "prefetch-dependencies":
                    _modify_param(task["params"], "sbom-type", "cyclonedx")
                case "apply-tags":
                    _modify_param(task["params"], "ADDITIONAL_TAGS", list(additional_tags))
                case "clone-repository":
                    _modify_param(task["params"], "refspec", f"{commit_sha}:refs/remotes/origin/{target_branch} refs/tags/*:refs/tags/*")

        # https://konflux.pages.redhat.com/docs/users/how-tos/configuring/overriding-compute-resources.html
        # ose-installer-artifacts fails with OOM with default values, hence bumping memory limit
        if has_build_images_task:
            obj["spec"]["taskRunSpecs"] = [{
                "pipelineTaskName": "build-images",
                "stepSpecs": [{
                    "name": "sbom-syft-generate",
                    "computeResources": {
                        "requests": {
                            "memory": "5Gi"
                        },
                        "limits": {
                            "memory": "10Gi"
                        }
                    }
                }]
            }]

        return obj

    async def start_pipeline_run_for_image_build(
        self,
        generate_name: str,
        namespace: Optional[str],
        application_name: str,
        component_name: str,
        git_url: str,
        commit_sha: str,
        target_branch: str,
        output_image: str,
        vm_override: dict,
        building_arches: Sequence[str],
        prefetch: Optional[list] = None,
        git_auth_secret: str = "pipelines-as-code-secret",
        additional_tags: Sequence[str] = [],
        skip_checks: bool = False,
        hermetic: Optional[bool] = None,
        dockerfile: Optional[str] = None,
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL,
    ):
        """
        Start a PipelineRun for building an image.

        :param generate_name: The generateName for the PipelineRun.
        :param namespace: The namespace for the PipelineRun.
        :param application_name: The application name.
        :param component_name: The component name.
        :param git_url: The git URL.
        :param commit_sha: The commit SHA.
        :param target_branch: The target branch.
        :param vm_override: Override the default konflux VM flavor (in case we need more specs)
        :param output_image: The output image.
        :param building_arches: The architectures to build.
        :param git_auth_secret: The git auth secret.
        :param additional_tags: Additional tags to apply to the image.
        :param skip_checks: Whether to skip checks.
        :param hermetic: Whether to build the image in a hermetic environment. If None, the default value is used.
        :param image_metadata: Image metadata
        :param pipelinerun_template_url: The URL to the PipelineRun template.
        :param prefetch: The param values for Konflux prefetch dependencies task
        :return: The PipelineRun resource.
        """
        unsupported_arches = set(building_arches) - set(self.SUPPORTED_ARCHES)
        if unsupported_arches:
            raise ValueError(f"Unsupported architectures: {unsupported_arches}")

        # If vm_override is not one and an override exists for a particular arch, use that. Otherwise, use the default
        build_platforms = [vm_override[arch] if vm_override and arch in vm_override else random.choice(self.SUPPORTED_ARCHES[arch]) for arch in building_arches]

        pipelinerun_manifest = await self._new_pipelinerun_for_image_build(
            generate_name=generate_name,
            namespace=namespace,
            application_name=application_name,
            component_name=component_name,
            git_url=git_url,
            commit_sha=commit_sha,
            target_branch=target_branch,
            output_image=output_image,
            build_platforms=build_platforms,
            git_auth_secret=git_auth_secret,
            skip_checks=skip_checks,
            hermetic=hermetic,
            additional_tags=additional_tags,
            dockerfile=dockerfile,
            pipelinerun_template_url=pipelinerun_template_url,
            prefetch=prefetch
        )
        if self.dry_run:
            fake_pipelinerun = resource.ResourceInstance(self.dyn_client, pipelinerun_manifest)
            fake_pipelinerun.metadata.name = f"{component_name}-dry-run"
            LOGGER.warning(f"[DRY RUN] Would have created PipelineRun: {fake_pipelinerun.metadata.name}")
            return fake_pipelinerun

        pipelinerun = await self._create(pipelinerun_manifest, async_req=True)
        LOGGER.debug(f"Created PipelineRun: {self.build_pipeline_url(pipelinerun)}")
        return pipelinerun

    @staticmethod
    def build_pipeline_url(pipelinerun):
        """ Returns the URL to the Konflux UI for the given PipelineRun.

        :param pipelinerun: The PipelineRun.
        :return: The URL.
        """
        pipelinerun_name = pipelinerun['metadata']['name']
        application = pipelinerun['metadata']['labels']['appstudio.openshift.io/application']
        return (f"{constants.KONFLUX_UI_HOST}/application-pipeline/"
                f"workspaces/{constants.KONFLUX_UI_DEFAULT_WORKSPACE}/"
                f"applications/{application}/"
                f"pipelineruns/{pipelinerun_name}")

    async def wait_for_pipelinerun(self, pipelinerun_name: str, namespace: Optional[str] = None,
                                   overall_timeout_timedelta: Optional[datetime.timedelta] = None,
                                   pending_timeout_timedelta: Optional[datetime.timedelta] = None) -> (resource.ResourceInstance, List[Dict]):
        """
        Wait for a PipelineRun to complete.

        :param pipelinerun_name: The name of the PipelineRun.
        :param namespace: The namespace of the PipelineRun.
        :param overall_timeout_timedelta: Maximum time to wait for pipeline to complete before canceling it (defaults to 5 hour)
        :param pending_timeout_timedelta: Maximum time to wait for a pending pod in a pipeline to run before cancelling the pipeline (defaults to 1 hour)
        :return: The PipelineRun ResourceInstance and a List[Dict] with a copy of an associated Pod.
        """
        if overall_timeout_timedelta is None:
            overall_timeout_timedelta = datetime.timedelta(hours=5)

        if pending_timeout_timedelta is None:
            pending_timeout_timedelta = datetime.timedelta(hours=1)

        namespace = namespace or self.default_namespace
        if self.dry_run:
            await asyncio.sleep(3)
            pipelinerun = {
                "metadata": {"name": pipelinerun_name, "namespace": namespace},
                "apiVersion": "tekton.dev/v1",
                "kind": "PipelineRun",
                "status": {"conditions": [{"status": "True"}]}
            }
            self._logger.warning(f"[DRY RUN] Would have waited for PipelineRun {pipelinerun_name} to complete")
            return resource.ResourceInstance(self.dyn_client, pipelinerun), resource.ResourceList(self.dyn_client, api_version="v1", kind="Pod")

        api = await self._get_api("tekton.dev/v1", "PipelineRun")
        pod_resource = await self._get_api("v1", "Pod")
        corev1_client = await self._get_corev1()

        def _inner():
            nonlocal overall_timeout_timedelta
            watcher = watch.Watch()
            succeeded_status = "Not Found"
            succeeded_reason = "Not Found"
            timeout_datetime = datetime.datetime.now() + overall_timeout_timedelta

            # If a pipelinerun runs more than an hour, successful pods
            # might be garbage collected. Keep track of pod state across
            # the pipeline run so that we can record information in bigquery.
            pod_history: Dict[str, Dict] = dict()

            while True:
                try:

                    for event in watcher.stream(
                        api.get,
                        # Specifying resource_version=0 tells the API to pull the current
                        # version of the object, give us an update, and then watch for new
                        # events. Combined with timeout_seconds, it ensures we periodically print an
                        # update about the current running pods for the pipeline
                        resource_version=0,
                        namespace=namespace,
                        serialize=False,
                        field_selector=f"metadata.name={pipelinerun_name}",
                        # timeout_seconds specifies a server side timeout. If there
                        # is no activity during this period, the for loop will exit
                        # gracefully. This ensures we will at least log *something*
                        # while waiting for a long pipelinerun. If we somehow miss
                        # an event, it also ensures we will come back and check
                        # the object with an explicit get at least once per period.
                        timeout_seconds=5 * 60
                    ):
                        assert isinstance(event, Dict)
                        cancel_pipelinerun = False  # If set to true, an attempt will be made to cancel the pipelinerun within the loop
                        obj = resource.ResourceInstance(api, event["object"])
                        # status takes some time to appear
                        try:
                            succeeded_condition = art_util.KubeCondition.find_condition(obj, 'Succeeded')
                            if succeeded_condition:
                                succeeded_status = succeeded_condition.status
                                succeeded_reason = succeeded_condition.reason
                        except AttributeError:
                            pass

                        pod_desc = []
                        pods = pod_resource.get(namespace=namespace, label_selector=f"tekton.dev/pipeline={pipelinerun_name}")
                        current_time = datetime.datetime.now()
                        for pod_instance in pods.items:
                            pod_name = pod_instance.metadata.name
                            try:
                                pod_history[pod_name] = pod_instance.to_dict()  # Convert to normal dict for pod_history
                                pod_phase = pod_instance.status.phase
                                if pod_phase == 'Succeeded':
                                    # Cut down on log output. No need to see successful pods again and again.
                                    continue
                                # Calculate the pod age based on the creation timestamp
                                creation_time_str = pod_instance.metadata.get('creationTimestamp')
                                if creation_time_str:
                                    creation_time = datetime.datetime.strptime(creation_time_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=None)
                                else:
                                    creation_time = current_time
                                age = current_time - creation_time

                                if pod_phase == 'Pending' and age > pending_timeout_timedelta:
                                    self._logger.error("PipelineRun %s pod %s pending beyond threshold %s; cancelling run", pipelinerun_name, pod_name, str(pending_timeout_timedelta))
                                    cancel_pipelinerun = True

                                age_str = f"{age.days}d {age.seconds // 3600}h {(age.seconds // 60) % 60}m"
                                pod_desc.append(f"\tPod {pod_name} [phase={pod_phase}][age={age_str}]")
                            except:
                                e_str = traceback.format_exc()
                                pod_desc.append(f"\tPod {pod_name} - unable to report information: {e_str}")

                        # Count all successful pods, not just ones that are still around.
                        successful_pods = 0
                        for _, pod in pod_history.items():
                            if pod.get('status', {}).get('phase') == 'Succeeded':
                                successful_pods += 1

                        self._logger.info("PipelineRun %s [status=%s][reason=%s]; pods[total=%d][successful=%d][extant=%d]\n%s", pipelinerun_name, succeeded_status, succeeded_reason, len(pod_history), successful_pods, len(pods.items), '\n'.join(pod_desc))

                        if succeeded_status not in ["Unknown", "Not Found"]:
                            time.sleep(5)  # allow final pods to update their status if they can
                            pods_instances = pod_resource.get(namespace=namespace, label_selector=f"tekton.dev/pipeline={pipelinerun_name}")
                            # We will convert ResourceInstances to Dicts so that they can be manipulated with
                            # extra information.
                            for pod_instance in pods_instances.items:
                                pod = pod_instance.to_dict()  # Convert to normal dict so that we can store log_output later.
                                pod_name = pod.get('metadata').get('name')
                                pod_status = pod.get('status', {})
                                pod_phase = pod_status.get('phase')
                                pod_history[pod_name] = pod  # Update pod history with the final snapshot
                                if pod_phase != 'Succeeded':
                                    self._logger.warning(f'PipelineRun {pipelinerun_name} finished with pod {pod_name} in unexpected phase: {pod_phase}')

                                    # Now iterate through containers and record logs for unexpected exit_code values
                                    container_statuses = pod_status.get("containerStatuses", [])
                                    for container_status in container_statuses:
                                        container_name = container_status.get("name")
                                        state = container_status.get("state", {})
                                        terminated = state.get("terminated", {})
                                        exit_code = terminated.get("exitCode")
                                        if exit_code is None or exit_code != 0:
                                            try:
                                                log_response = corev1_client.read_namespaced_pod_log(
                                                    name=pod_name,
                                                    namespace=namespace,
                                                    container=container_name
                                                )
                                                # stuff log information into the container_status, so that it can be
                                                # included in the bigquery database.
                                                container_status['log_output'] = log_response
                                                self._logger.warning(f'Pod {pod_name} container {container_name} exited with {exit_code}; logs:\n------START LOGS {pod_name}:{container_name}------\n{log_response}\n------END LOGS {pod_name}:{container_name}------\n')
                                            except:
                                                e_str = traceback.format_exc()
                                                self._logger.warning(f'Failed to retrieve logs for pod {pod_name} container {container_name}: {e_str}')

                            watcher.stop()
                            return obj, list(pod_history.values())

                        if datetime.datetime.now() > timeout_datetime:
                            self._logger.error("PipelineRun %s has run longer than timeout %s; cancelling run", pipelinerun_name, str(overall_timeout_timedelta))
                            cancel_pipelinerun = True

                        if cancel_pipelinerun:
                            self._logger.info("PipelineRun %s is being cancelled", pipelinerun_name)
                            try:
                                # Setting spec.status in the PipelineRun should cause tekton to start canceling the pipeline.
                                # This includes terminating pods associated with the run.
                                api.patch(
                                    name=obj.metadata.name,
                                    namespace=namespace,
                                    body={
                                        'spec': {
                                            'status': 'Cancelled'
                                        }
                                    },
                                    content_type="application/merge-patch+json"
                                )
                            except:
                                self._logger.error('Error trying to cancel PipelineRun %s', pipelinerun_name)
                                traceback.print_exc()

                    self._logger.info("No updates for PipelineRun %s during watch timeout period; requerying", pipelinerun_name)
                except TimeoutError:
                    self._logger.error("Timeout waiting for PipelineRun %s to complete", pipelinerun_name)
                    continue
                except exceptions.ApiException as e:
                    if e.status == 410:
                        # If the last result is too old, an `ApiException` exception will be thrown with
                        # `code` 410. In that case we have to recover by retrying without resource_version.
                        self._logger.debug("%s: Resource version is too old. Recovering...", pipelinerun_name)
                        continue
                    raise

        return await exectools.to_thread(_inner)
