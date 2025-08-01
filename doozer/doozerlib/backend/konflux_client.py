import asyncio
import datetime
import logging
import os
import random
import time
import traceback
from typing import Dict, List, Optional, Sequence, Union, cast

import aiohttp
import jinja2
from artcommonlib import exectools
from artcommonlib import util as art_util
from async_lru import alru_cache
from doozerlib import constants
from kubernetes import config, watch
from kubernetes.client import ApiClient, Configuration, CoreV1Api
from kubernetes.dynamic import DynamicClient, exceptions, resource
from ruamel.yaml import YAML

yaml = YAML(typ="safe")
LOGGER = logging.getLogger(__name__)

API_VERSION = "appstudio.redhat.com/v1alpha1"
KIND_SNAPSHOT = "Snapshot"
KIND_COMPONENT = "Component"
KIND_APPLICATION = "Application"
KIND_RELEASE = "Release"
KIND_RELEASE_PLAN = "ReleasePlan"

DEFAULT_WAIT_HOURS_RELEASE = 5


class KonfluxClient:
    """
    KonfluxClient is a client for interacting with the Konflux API.
    """

    # https://konflux.pages.redhat.com/docs/users/getting-started/multi-platform-builds.html
    # The arch to Konflux VM name mapping. The specs for each of the VMs can be seen in the doc link shared above.
    SUPPORTED_ARCHES = {
        "x86_64": ["linux/x86_64"],
        "s390x": ["linux/s390x"],
        "ppc64le": ["linux/ppc64le"],
        "aarch64": ["linux/arm64"],
    }

    def __init__(
        self, default_namespace: str, config: Configuration, dry_run: bool = False, logger: logging.Logger = LOGGER
    ) -> None:
        self.api_client = ApiClient(configuration=config)
        self.dyn_client = DynamicClient(self.api_client)
        self.corev1_client = CoreV1Api(self.api_client)
        self.default_namespace = default_namespace
        self.dry_run = dry_run
        self._logger = logger
        # In case of a network outage,  the client may hang indefinitely without raising any exception.
        # This is a workaround to set a timeout for the requests.
        # https://github.com/kubernetes-client/python/blob/master/examples/watch/timeout-settings.md
        self.request_timeout = 60 * 5  # 5 minutes

    def verify_connection(self):
        try:
            self.corev1_client.get_api_resources(_request_timeout=self.request_timeout)
            self._logger.info("Successfully authenticated to the Kubernetes cluster.")
        except Exception as e:
            self._logger.error(f"Failed to authenticate to the Kubernetes cluster: {e}")
            raise

    @staticmethod
    def from_kubeconfig(
        default_namespace: str,
        config_file: Optional[str],
        context: Optional[str],
        dry_run: bool = False,
        logger: logging.Logger | None = None,
    ) -> "KonfluxClient":
        """Create a KonfluxClient from a kubeconfig file.

        :param config_file: The path to the kubeconfig file.
        :param context: The context to use.
        :param default_namespace: The default namespace.
        :param dry_run: Whether to run in dry-run mode.
        :param logger: The logger.
        :return: The KonfluxClient.
        """
        cfg = Configuration()
        config.load_kube_config(
            config_file=config_file, context=context, persist_config=False, client_configuration=cfg
        )
        return KonfluxClient(default_namespace=default_namespace, config=cfg, dry_run=dry_run, logger=logger or LOGGER)

    @alru_cache
    async def _get_api(self, api_version: str, kind: str):
        """Get the API object for the given API version and kind.

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
        """Extract the metadata from a manifest.

        :param manifest: The manifest.
        :return: The API version, kind, name, and namespace.
        """
        api_version = manifest["apiVersion"]
        kind = manifest["kind"]
        name = manifest["metadata"].get("name")
        namespace = manifest["metadata"].get("namespace", self.default_namespace)
        return api_version, kind, name, namespace

    async def _get(self, api_version: str, kind: str, name: str, namespace: Optional[str] = None, strict: bool = True):
        """Get a resource by name and namespace.

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
            resource = await exectools.to_thread(
                api.get, name=name, namespace=namespace or self.default_namespace, _request_timeout=self.request_timeout
            )
        except exceptions.NotFoundError:
            if strict:
                raise
        return resource

    @alru_cache
    async def _get__caching(
        self, api_version: str, kind: str, name: str, namespace: Optional[str] = None, strict: bool = True
    ):
        """Get a resource by name and namespace, with caching.

        :param api_version: The API version.
        :param kind: The kind.
        :param name: The name of the resource.
        :param namespace: The namespace of the resource.
        :param strict: Whether to raise an exception if the resource is not found.
        :return: The resource.
        """
        return await self._get(api_version, kind, name, namespace, strict)

    async def _create(self, manifest: dict, **kwargs):
        """Create a resource.

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
        new = await exectools.to_thread(
            api.create, namespace=namespace, body=manifest, _request_timeout=self.request_timeout, **kwargs
        )
        new = cast(resource.ResourceInstance, new)
        api_version, kind, name, namespace = self._extract_manifest_metadata(new.to_dict())
        self._logger.info(f"Created {api_version}/{kind} {namespace}/{name}")
        return new

    async def _patch(self, manifest: dict):
        """Patch a resource.

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
            _request_timeout=self.request_timeout,
        )
        new = cast(resource.ResourceInstance, new)
        api_version, kind, name, namespace = self._extract_manifest_metadata(new.to_dict())
        self._logger.info(f"Patched {api_version}/{kind} {namespace}/{name}")
        return new

    async def _replace(self, manifest: dict):
        """Replace a resource.

        :param manifest: The manifest.
        :return: The resource.
        """
        api_version, kind, name, namespace = self._extract_manifest_metadata(manifest)
        api = await self._get_api(api_version, kind)
        if self.dry_run:
            self._logger.warning(f"[DRY RUN] Would have replaced {api_version}/{kind} {namespace}/{name}")
            return resource.ResourceInstance(self.dyn_client, manifest)
        self._logger.info(f"Replacing {api_version}/{kind} {namespace}/{name}")
        new = await exectools.to_thread(
            api.replace, body=manifest, namespace=namespace, _request_timeout=self.request_timeout
        )
        new = cast(resource.ResourceInstance, new)
        api_version, kind, name, namespace = self._extract_manifest_metadata(new.to_dict())
        self._logger.info(f"Replaced {api_version}/{kind} {namespace}/{name}")
        return new

    async def _delete(self, api_version: str, kind: str, name: str, namespace: str):
        """Delete a resource.

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
        await exectools.to_thread(api.delete, name=name, namespace=namespace, _request_timeout=self.request_timeout)
        self._logger.info(f"Deleted {api_version}/{kind} {namespace}/{name}")

    async def _create_or_patch(self, manifest: dict):
        """Create or patch a resource.

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
                    LOGGER.debug(
                        "Error creating %s/%s %s/%s because it already exists; ignoring",
                        api_version,
                        kind,
                        namespace,
                        name,
                    )
                    return await self._get(api_version, kind, name, namespace, strict=True)
                raise
        return await self._patch(manifest)

    async def _create_or_replace(self, manifest: dict):
        """Create or replace a resource.

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
                "name": name,
            },
            "spec": {
                "displayName": display_name,
                "appModelRepository": {"url": ""},
                "gitOpsRepository": {"url": ""},
            },
        }
        return obj

    async def get_application(self, name: str, strict: bool = True) -> Optional[resource.ResourceInstance]:
        """Get an application by name.

        :param name: The name of the application.
        :param strict: Whether to raise an exception if the application is not found.
        :return: The application resource, or None if not found.
        :raises exceptions.NotFoundError: If the application is not found and strict is True.
        """
        try:
            return await self._get(API_VERSION, KIND_APPLICATION, name, strict=strict)
        except exceptions.NotFoundError as e:
            if strict:
                raise e
            return None

    async def get_application__caching(self, name: str, strict: bool = True) -> Optional[resource.ResourceInstance]:
        """Get an application by name with caching.

        :param name: The name of the application.
        :param strict: Whether to raise an exception if the application is not found.
        :return: The application resource, or None if not found.
        :raises exceptions.NotFoundError: If the application is not found and strict is True.
        """
        return await self._get__caching(API_VERSION, KIND_APPLICATION, name, strict=strict)

    async def ensure_application(self, name: str, display_name: str) -> resource.ResourceInstance:
        application = self._new_application(name, display_name)
        return await self._create_or_patch(application)

    @staticmethod
    def _new_component(
        name: str,
        application: str,
        component_name: str,
        image_repo: Optional[str],
        source_url: Optional[str],
        revision: Optional[str],
    ) -> dict:
        obj = {
            "apiVersion": API_VERSION,
            "kind": KIND_COMPONENT,
            "metadata": {
                "name": name,
                "annotations": {
                    "build.appstudio.openshift.io/pipeline": '{"name":"docker-build-multi-platform-oci-ta","bundle":"latest"}',
                    # will raise PRs to upstream repos (openshift-priv) if this is not set to false
                    "build.appstudio.openshift.io/status": '{"pac":{"state":"disabled"}}',
                    # "build.appstudio.openshift.io/request": "configure-pac",
                    # https://gitlab.cee.redhat.com/konflux/docs/users/-/blob/main/topics/mintmaker/user.md#offboarding-a-repository
                    "mintmaker.appstudio.redhat.com/disabled": "true",
                },
            },
            "spec": {
                "application": application,
                "componentName": component_name,
                "source": {
                    "git": {
                        "url": source_url,
                        "revision": revision,
                    },
                },
            },
        }
        if image_repo:
            obj["spec"]["containerImage"] = image_repo
        if source_url:
            obj["spec"].setdefault("source", {}).setdefault("git", {})["url"] = source_url
        if revision:
            obj["spec"].setdefault("source", {}).setdefault("git", {})["revision"] = revision
        return obj

    async def get_component(self, name: str, strict: bool = True) -> Optional[resource.ResourceInstance]:
        """Get a component by name.

        :param name: The name of the component.
        :param strict: Whether to raise an exception if the component is not found.
        :return: The component resource, or None if not found.
        :raises exceptions.NotFoundError: If the component is not found and strict is True.
        """
        try:
            return await self._get(API_VERSION, KIND_COMPONENT, name, strict=strict)
        except exceptions.NotFoundError as e:
            if strict:
                raise e
            return None

    async def get_component__caching(self, name: str, strict: bool = True) -> Optional[resource.ResourceInstance]:
        """Get a component by name with caching.

        :param name: The name of the component.
        :param strict: Whether to raise an exception if the component is not found.
        :return: The component resource, or None if not found.
        :raises exceptions.NotFoundError: If the component is not found and strict is True.
        """
        return await self._get__caching(API_VERSION, KIND_COMPONENT, name, strict=strict)

    async def ensure_component(
        self,
        name: str,
        application: str,
        component_name: str,
        image_repo: Optional[str],
        source_url: Optional[str],
        revision: Optional[str],
    ) -> resource.ResourceInstance:
        component = self._new_component(name, application, component_name, image_repo, source_url, revision)
        return await self._create_or_replace(component)

    @alru_cache
    async def _get_pipelinerun_template(self, template_url: str):
        """Get a PipelineRun template.

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

    async def _new_pipelinerun_for_image_build(
        self,
        generate_name: str,
        namespace: Optional[str],
        application_name: str,
        component_name: str,
        git_url: str,
        commit_sha: str,
        target_branch: str,
        output_image: str,
        build_platforms: Sequence[str],
        prefetch: Optional[list] = None,
        git_auth_secret: str = "pipelines-as-code-secret",
        additional_tags: Optional[Sequence[str]] = None,
        skip_checks: bool = False,
        hermetic: Optional[bool] = None,
        sast: Optional[bool] = None,
        dockerfile: Optional[str] = None,
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL,
        annotations: Optional[dict[str, str]] = None,
        artifact_type: Optional[str] = None,
        service_account: Optional[str] = None,
        rebuild: Optional[bool] = None,
    ) -> dict:
        if additional_tags is None:
            additional_tags = []
        https_url = art_util.convert_remote_git_to_https(git_url)

        template = await self._get_pipelinerun_template(pipelinerun_template_url)
        rendered = template.render(
            {
                "source_url": https_url,
                "revision": commit_sha,
                "target_branch": target_branch,
                "git_auth_secret": git_auth_secret,
            }
        )
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
        if annotations:
            obj["metadata"]["annotations"].update(annotations)
        obj["metadata"]["labels"]["appstudio.openshift.io/application"] = application_name
        obj["metadata"]["labels"]["appstudio.openshift.io/component"] = component_name

        def _modify_param(params: List, name: str, value: Union[str, bool, list[str]]):
            """Modify a parameter in the params list. If the parameter does not exist, it is added.

            :param params: The list of parameters.
            :param name: The name of the parameter.
            :param value: The value of the parameter.
            """
            if isinstance(value, bool):
                # boolean value should be converted to string
                value = "true" if value else "false"
            for param in params:
                if param["name"] == name:
                    param["value"] = value
                    return
            params.append({"name": name, "value": value})

        # PipelineRun parameters to override in the template
        params = obj["spec"]["params"]
        _modify_param(params, "output-image", output_image)
        _modify_param(params, "skip-checks", skip_checks)
        _modify_param(
            params, "build-source-image", "true"
        )  # Have to be true always to satisfy Enterprise Contract Policy
        _modify_param(params, "build-platforms", list(build_platforms))
        if dockerfile:
            _modify_param(params, "dockerfile", dockerfile)

        if prefetch:
            _modify_param(params, "prefetch-input", prefetch)
        if hermetic is not None:
            _modify_param(params, "hermetic", hermetic)

        if rebuild is not None:
            _modify_param(params, "rebuild", rebuild)

        # See https://konflux-ci.dev/docs/how-tos/configuring/customizing-the-build/#configuring-timeouts
        obj["spec"]["timeouts"] = {"pipeline": "12h"}

        obj["spec"]["taskRunTemplate"]["serviceAccountName"] = service_account or f"build-pipeline-{component_name}"

        # Check if RPM lockfile prefetch is being used
        rpm_lockfile_prefetch_enabled = prefetch and any(item.get("type") == "rpm" for item in prefetch)

        # Task specific parameters to override in the template
        has_build_images_task = False
        has_sast_task = False
        for task in obj["spec"]["pipelineSpec"]["tasks"]:
            match task["name"]:
                case "build-images":
                    has_build_images_task = True
                    task["timeout"] = "12h"
                    _modify_param(task["params"], "SBOM_TYPE", "spdx")
                case "prefetch-dependencies":
                    _modify_param(task["params"], "sbom-type", "spdx")
                    if rpm_lockfile_prefetch_enabled:
                        _modify_param(task["params"], "dev-package-managers", "true")
                        _modify_param(task["params"], "log-level", "debug")
                case "apply-tags":
                    _modify_param(task["params"], "ADDITIONAL_TAGS", list(additional_tags))
                case "clone-repository":
                    _modify_param(
                        task["params"],
                        "refspec",
                        f"{commit_sha}:refs/remotes/origin/{target_branch} refs/tags/*:refs/tags/*",
                    )
                case "sast-snyk-check":
                    has_sast_task = True
                case "ecosystem-cert-preflight-checks":
                    if artifact_type:
                        _modify_param(task["params"], "artifact-type", artifact_type)

        if sast and not has_sast_task:
            raise IOError(
                "SAST task is enabled, but the template does not contain it. Please ensure the template is up-to-date."
            )
        if sast is False and has_sast_task:  # if SAST is explicitly disabled, remove SAST tasks
            tasks = []
            has_sast_task = False
            for task in obj["spec"]["pipelineSpec"]["tasks"]:
                task_name = task.get("name")
                if task_name in ("sast-unicode-check", "sast-shell-check"):
                    self._logger.info(f"Removing {task_name} tasks since SAST is disabled")
                    continue
                tasks.append(task)

            obj["spec"]["pipelineSpec"]["tasks"] = tasks

        # https://konflux.pages.redhat.com/docs/users/how-tos/configuring/overriding-compute-resources.html
        # ose-installer-artifacts fails with OOM with default values, hence bumping memory limit
        task_run_specs = []
        if has_build_images_task:
            task_run_specs += [
                {
                    "pipelineTaskName": "build-images",
                    "stepSpecs": [
                        {
                            "name": "sbom-syft-generate",
                            "computeResources": {
                                "requests": {
                                    "memory": "5Gi",
                                },
                                "limits": {
                                    "memory": "10Gi",
                                },
                            },
                        }
                    ],
                }
            ]
        if has_sast_task:
            task_run_specs += [
                {
                    "pipelineTaskName": "sast-shell-check",
                    "computeResources": {
                        "requests": {
                            "memory": "10Gi",
                        },
                        "limits": {
                            "memory": "10Gi",
                        },
                    },
                }
            ]

        obj["spec"]["taskRunSpecs"] = task_run_specs

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
        sast: Optional[bool] = None,
        git_auth_secret: str = "pipelines-as-code-secret",
        additional_tags: Sequence[str] = [],
        skip_checks: bool = False,
        hermetic: Optional[bool] = None,
        dockerfile: Optional[str] = None,
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL,
        annotations: Optional[dict[str, str]] = None,
        artifact_type: Optional[str] = None,
        service_account: Optional[str] = None,
        rebuild: Optional[bool] = None,
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
        :param output_image: The output image.
        :param vm_override: Override the default konflux VM flavor (in case we need more specs)
        :param building_arches: The architectures to build.
        :param prefetch: The param values for Konflux prefetch dependencies task
        :param sast: To enable the SAST task in PLR. If None, use the default value from the pipeline template.
        :param git_auth_secret: The git auth secret.
        :param additional_tags: Additional tags to apply to the image.
        :param skip_checks: Whether to skip checks.
        :param hermetic: Whether to build the image in a hermetic environment. If None, the default value is used.
        :param dockerfile: Optional Dockerfile name
        :param pipelinerun_template_url: The URL to the PipelineRun template.
        :param annotations: Optional PLR annotations
        :param artifact_type: The type of artifact artifact_type for ecosystem-cert-preflight-checks. Select from application, operatorbundle, or introspect.
        :param service_account: The service account to use for the PipelineRun.
        :param rebuild: Forces rebuild of the image, even if it already exists. If None, the default behavior is to not changed.
        :return: The PipelineRun resource.
        """
        unsupported_arches = set(building_arches) - set(self.SUPPORTED_ARCHES)
        if unsupported_arches:
            raise ValueError(f"Unsupported architectures: {unsupported_arches}")

        # If vm_override is not one and an override exists for a particular arch, use that. Otherwise, use the default
        build_platforms = [
            vm_override[arch] if vm_override and arch in vm_override else random.choice(self.SUPPORTED_ARCHES[arch])
            for arch in building_arches
        ]

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
            prefetch=prefetch,
            sast=sast,
            annotations=annotations,
            artifact_type=artifact_type,
            service_account=service_account,
            rebuild=rebuild,
        )
        if self.dry_run:
            fake_pipelinerun = resource.ResourceInstance(self.dyn_client, pipelinerun_manifest)
            fake_pipelinerun.metadata.name = f"{component_name}-dry-run"
            LOGGER.warning(f"[DRY RUN] Would have created PipelineRun: {fake_pipelinerun.metadata.name}")
            return fake_pipelinerun

        pipelinerun = await self._create(pipelinerun_manifest, async_req=True)
        LOGGER.debug(f"Created PipelineRun: {self.resource_url(pipelinerun)}")
        return pipelinerun

    @staticmethod
    def resource_url(resource_instance: resource.ResourceInstance) -> str:
        """Returns the URL to the Konflux UI for the given resource instance.
        :param resource_instance: The resource instance.
        :return: The URL.
        """
        kind = resource_instance.kind.lower()
        name = resource_instance.metadata.name
        namespace = resource_instance.metadata.namespace or constants.KONFLUX_DEFAULT_NAMESPACE
        application = resource_instance.metadata.labels.get("appstudio.openshift.io/application", "unknown-application")
        return f"{constants.KONFLUX_UI_HOST}/ns/{namespace}/applications/{application}/{kind}s/{name}"

    async def wait_for_pipelinerun(
        self,
        pipelinerun_name: str,
        namespace: Optional[str] = None,
        overall_timeout_timedelta: Optional[datetime.timedelta] = None,
        pending_timeout_timedelta: Optional[datetime.timedelta] = None,
    ) -> tuple[resource.ResourceInstance, list[Dict]]:
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
                "status": {"conditions": [{"status": "True", "type": "Succeeded"}]},
            }
            self._logger.warning(f"[DRY RUN] Would have waited for PipelineRun {pipelinerun_name} to complete")
            return resource.ResourceInstance(self.dyn_client, pipelinerun), resource.ResourceList(
                self.dyn_client, api_version="v1", kind="Pod"
            )

        api = await self._get_api("tekton.dev/v1", "PipelineRun")
        pod_resource = await self._get_api("v1", "Pod")
        corev1_client = await self._get_corev1()

        def _inner():
            watcher = watch.Watch()
            succeeded_status = "Not Found"
            succeeded_reason = "Not Found"
            timeout_datetime = datetime.datetime.now(tz=datetime.timezone.utc) + overall_timeout_timedelta

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
                        timeout_seconds=5 * 60,
                        _request_timeout=self.request_timeout,
                    ):
                        assert isinstance(event, Dict)
                        cancel_pipelinerun = (
                            False  # If set to true, an attempt will be made to cancel the pipelinerun within the loop
                        )
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
                        pods = pod_resource.get(
                            namespace=namespace,
                            label_selector=f"tekton.dev/pipeline={pipelinerun_name}",
                            _request_timeout=self.request_timeout,
                        )
                        current_time = datetime.datetime.now(tz=datetime.timezone.utc)
                        for pod_instance in pods.items:
                            pod_name = pod_instance.metadata.name
                            try:
                                # Convert to normal dict for pod_history
                                pod_history[pod_name] = pod_instance.to_dict()
                                pod_phase = pod_instance.status.phase
                                if pod_phase == 'Succeeded':
                                    # Cut down on log output. No need to see successful pods again and again.
                                    continue
                                # Calculate the pod age based on the creation timestamp
                                creation_time_str = pod_instance.metadata.get('creationTimestamp')
                                if creation_time_str:
                                    creation_time = datetime.datetime.strptime(
                                        creation_time_str, "%Y-%m-%dT%H:%M:%SZ"
                                    ).replace(tzinfo=datetime.timezone.utc)
                                else:
                                    creation_time = current_time
                                age = current_time - creation_time

                                if pod_phase == 'Pending' and age > pending_timeout_timedelta:
                                    self._logger.error(
                                        "PipelineRun %s pod %s pending beyond threshold %s; cancelling run",
                                        pipelinerun_name,
                                        pod_name,
                                        str(pending_timeout_timedelta),
                                    )
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

                        self._logger.info(
                            "PipelineRun %s [status=%s][reason=%s]; pods[total=%d][successful=%d][extant=%d]\n%s",
                            pipelinerun_name,
                            succeeded_status,
                            succeeded_reason,
                            len(pod_history),
                            successful_pods,
                            len(pods.items),
                            '\n'.join(pod_desc),
                        )

                        if succeeded_status not in ["Unknown", "Not Found"]:
                            # allow final pods to update their status if they can
                            time.sleep(5)
                            pods_instances = pod_resource.get(
                                namespace=namespace,
                                label_selector=f"tekton.dev/pipeline={pipelinerun_name}",
                                _request_timeout=self.request_timeout,
                            )
                            # We will convert ResourceInstances to Dicts so that they can be manipulated with
                            # extra information.
                            for pod_instance in pods_instances.items:
                                pod = (
                                    pod_instance.to_dict()
                                )  # Convert to normal dict so that we can store log_output later.
                                pod_name = pod.get('metadata').get('name')
                                pod_status = pod.get('status', {})
                                pod_phase = pod_status.get('phase')
                                # Update pod history with the final snapshot
                                pod_history[pod_name] = pod
                                if pod_phase != 'Succeeded':
                                    self._logger.warning(
                                        f'PipelineRun {pipelinerun_name} finished with pod {pod_name} in unexpected phase: {pod_phase}'
                                    )

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
                                                    container=container_name,
                                                    _request_timeout=self.request_timeout,
                                                )
                                                # stuff log information into the container_status, so that it can be
                                                # included in the bigquery database.
                                                container_status['log_output'] = log_response
                                                self._logger.warning(
                                                    f'Pod {pod_name} container {container_name} exited with {exit_code}; logs:\n------START LOGS {pod_name}:{container_name}------\n{log_response}\n------END LOGS {pod_name}:{container_name}------\n'
                                                )
                                            except:
                                                e_str = traceback.format_exc()
                                                self._logger.warning(
                                                    f'Failed to retrieve logs for pod {pod_name} container {container_name}: {e_str}'
                                                )

                            watcher.stop()
                            return obj, list(pod_history.values())

                        if datetime.datetime.now(tz=datetime.timezone.utc) > timeout_datetime:
                            self._logger.error(
                                "PipelineRun %s has run longer than timeout %s; cancelling run",
                                pipelinerun_name,
                                str(overall_timeout_timedelta),
                            )
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
                                            'status': 'Cancelled',
                                        },
                                    },
                                    content_type="application/merge-patch+json",
                                    _request_timeout=self.request_timeout,
                                )
                            except:
                                self._logger.error('Error trying to cancel PipelineRun %s', pipelinerun_name)
                                traceback.print_exc()

                    self._logger.info(
                        "No updates for PipelineRun %s during watch timeout period; requerying", pipelinerun_name
                    )
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

    async def wait_for_release(
        self,
        release_name: str,
        namespace: Optional[str] = None,
        overall_timeout_timedelta: Optional[datetime.timedelta] = None,
    ) -> resource.ResourceInstance:
        f"""
        Wait for a Release to complete.

        :param release_name: The name of the Release.
        :param namespace: The namespace of the Release.
        :param overall_timeout_timedelta: Maximum time to wait for release to complete before exiting (defaults to {DEFAULT_WAIT_HOURS_RELEASE}
        hour)
        :return: The Release ResourceInstance
        """
        if overall_timeout_timedelta is None:
            overall_timeout_timedelta = datetime.timedelta(hours=DEFAULT_WAIT_HOURS_RELEASE)

        namespace = namespace or self.default_namespace
        api = await self._get_api(API_VERSION, KIND_RELEASE)

        if self.dry_run:
            await asyncio.sleep(3)
            release = {
                "metadata": {"name": release_name, "namespace": namespace},
                "apiVersion": API_VERSION,
                "kind": KIND_RELEASE,
                "status": {"conditions": [{"type": "Released", "status": "True", "reason": "Succeeded"}]},
            }
            self._logger.info(f"[DRY RUN] Would have waited for Release {release_name} to complete")
            return resource.ResourceInstance(self.dyn_client, release)

        release_obj = await self._get(API_VERSION, KIND_RELEASE, release_name)
        url = self.resource_url(release_obj)
        self._logger.info("Found release at %s", url)

        def _inner():
            watcher = watch.Watch()
            released_status = "Not Found"
            released_reason = "Not Found"
            timeout_datetime = datetime.datetime.now(tz=datetime.timezone.utc) + overall_timeout_timedelta

            while True:
                try:
                    release_obj = watcher.stream(
                        api.get,
                        resource_version=0,
                        namespace=namespace,
                        serialize=False,
                        field_selector=f"metadata.name={release_name}",
                        timeout_seconds=60,
                        _request_timeout=self.request_timeout,
                    )
                    for event in release_obj:
                        assert isinstance(event, Dict)
                        obj = resource.ResourceInstance(api, event["object"])
                        # status takes some time to appear
                        try:
                            released_condition = art_util.KubeCondition.find_condition(obj, 'Released')
                            if released_condition:
                                released_status = released_condition.status
                                released_reason = released_condition.reason
                        except AttributeError:
                            pass

                        self._logger.info(
                            "Release %s [status=%s][reason=%s]", release_name, released_status, released_reason
                        )

                        if released_reason not in ["Unknown", "Not Found", "Progressing"]:
                            watcher.stop()
                            return obj

                        if datetime.datetime.now(tz=datetime.timezone.utc) > timeout_datetime:
                            self._logger.info("Timeout reached. Exiting..")
                            watcher.stop()
                            return obj

                    self._logger.info("No updates for Release %s during watch timeout period; requerying", release_name)
                except TimeoutError:
                    self._logger.error("Timeout waiting for Release %s to complete", release_name)
                    continue
                except exceptions.ApiException as e:
                    if e.status == 410:
                        # If the last result is too old, an `ApiException` exception will be thrown with
                        # `code` 410. In that case we have to recover by retrying without resource_version.
                        self._logger.debug("%s: Resource version is too old. Recovering...", release_name)
                        continue
                    raise

        return await exectools.to_thread(_inner)
