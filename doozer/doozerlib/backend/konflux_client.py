import asyncio
import logging
from typing import Dict, List, Optional, Sequence, Union, cast

import aiohttp
import jinja2
from artcommonlib import exectools
from artcommonlib import util as art_util
from async_lru import alru_cache
from kubernetes import config, watch
from kubernetes.client import ApiClient, Configuration
from kubernetes.dynamic import DynamicClient, exceptions, resource
from ruamel.yaml import YAML

from doozerlib import constants

from doozerlib.image import ImageMetadata

yaml = YAML(typ="safe")
LOGGER = logging.getLogger(__name__)


class KonfluxClient:
    """
    KonfluxClient is a client for interacting with the Konflux API.
    """
    # https://konflux.pages.redhat.com/docs/users/getting-started/multi-platform-builds.html
    # The arch to Konflux VM name mapping. The specs for each of the VMs can be seen in the doc link shared above.
    SUPPORTED_ARCHES = {
        "x86_64": "linux/x86_64",
        "s390x": "linux/s390x",
        "ppc64le": "linux/ppc64le",
        "aarch64": "linux/arm64",
    }

    def __init__(self, default_namespace: str, config: Configuration, dry_run: bool = False, logger: logging.Logger = LOGGER) -> None:
        self.api_client = ApiClient(configuration=config)
        self.dyn_client = DynamicClient(self.api_client)
        self.default_namespace = default_namespace
        self.dry_run = dry_run
        self._logger = logger

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

    async def _get(self, api_version: str, kind: str, name: str, namespace: str, strict: bool = True):
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
            resource = await exectools.to_thread(api.get, name=name, namespace=namespace)
        except exceptions.NotFoundError:
            if strict:
                raise
        return resource

    @alru_cache
    async def _get__caching(self, api_version: str, kind: str, name: str, namespace: str, strict: bool = True):
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
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "Application",
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
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "Component",
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
                                               build_platforms: Sequence[str], image_metadata: ImageMetadata, git_auth_secret: str = "pipelines-as-code-secret",
                                               additional_tags: Optional[Sequence[str]] = None, skip_checks: bool = False,
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
        _modify_param(params, "image-expires-after", "6w")
        _modify_param(params, "build-platforms", list(build_platforms))

        prefetch_params = []
        package_managers = image_metadata.config.get("content", {}).get("source", {}).get("pkg_managers", [])
        if "pip" in package_managers:
            pip_configs = image_metadata.config.get("cachito", {}).get("packages", {}).get("pip", [])
            if pip_configs:
                for entry in pip_configs:
                    pip_override = {"type": "pip"}

                    pip_path = entry.get("path")
                    if pip_path:
                        pip_override["path"] = pip_path

                    pip_requirements_files = entry.get("requirements_files", [])
                    pip_requirements_build_files = entry.get("requirements_build_files", [])
                    if pip_requirements_files or pip_requirements_build_files:
                        pip_override["requirements_files"] = pip_requirements_files + pip_requirements_build_files
                    prefetch_params.append(pip_override)
            else:
                prefetch_params.append({"type": "pip", "path": "."})

        if "gomod" in package_managers:
            gomod_configs = image_metadata.config.get("cachito", {}).get("packages", {}).get("gomod", [])
            if gomod_configs:
                for entry in gomod_configs:
                    gomod_override = {"type": "gomod"}

                    gomod_path = entry.get("path")
                    if gomod_path:
                        gomod_override["path"] = gomod_path

                    prefetch_params.append({"type": "gomod", "path": gomod_path})
            else:
                prefetch_params.append({"type": "gomod", "path": "."})

        if "npm" in package_managers:
            npm_configs = image_metadata.config.get("cachito", {}).get("packages", {}).get("npm", [])
            if npm_configs:
                for entry in npm_configs:
                    npm_override = {"type": "npm"}

                    npm_path = entry.get("path")
                    if npm_path:
                        npm_override["path"] = npm_path

                    prefetch_params.append({"type": "npm", "path": npm_path})
            else:
                prefetch_params.append({"type": "npm", "path": "."})

        if prefetch_params:
            _modify_param(params, "prefetch-input", prefetch_params)

        # See https://konflux-ci.dev/docs/how-tos/configuring/customizing-the-build/#configuring-timeouts
        obj["spec"]["timeouts"] = {"pipeline": "12h"}

        # Task specific parameters to override in the template
        has_build_images_task = False
        for task in obj["spec"]["pipelineSpec"]["tasks"]:
            match task["name"]:
                case "build-images":
                    has_build_images_task = True
                    task["timeout"] = "12h"
                case "apply-tags":
                    _modify_param(task["params"], "ADDITIONAL_TAGS", list(additional_tags))

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
        git_auth_secret: str = "pipelines-as-code-secret",
        additional_tags: Sequence[str] = [],
        skip_checks: bool = False,
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL,
        image_metadata: ImageMetadata = None
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
        :param image_metadata: Image metadata
        :return: The PipelineRun resource.
        """
        unsupported_arches = set(building_arches) - set(self.SUPPORTED_ARCHES)
        if unsupported_arches:
            raise ValueError(f"Unsupported architectures: {unsupported_arches}")

        # If vm_override is not one and an override exists for a particular arch, use that. Otherwise, use the default
        build_platforms = [vm_override[arch] if vm_override and arch in vm_override else self.SUPPORTED_ARCHES[arch] for arch in building_arches]

        pipelinerun_manifest = await self._new_pipelinerun_for_image_build(
            generate_name,
            namespace,
            application_name,
            component_name,
            git_url,
            commit_sha,
            target_branch,
            output_image,
            build_platforms,
            git_auth_secret=git_auth_secret,
            skip_checks=skip_checks,
            additional_tags=additional_tags,
            pipelinerun_template_url=pipelinerun_template_url,
            image_metadata=image_metadata
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

    async def wait_for_pipelinerun(self, pipelinerun_name: str, namespace: Optional[str] = None):
        """
        Wait for a PipelineRun to complete.

        :param pipelinerun_name: The name of the PipelineRun.
        :param namespace: The namespace of the PipelineRun.
        :return: The PipelineRun.
        """
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
            return resource.ResourceInstance(self.dyn_client, pipelinerun)

        api = await self._get_api("tekton.dev/v1", "PipelineRun")

        def _inner():
            watcher = watch.Watch()
            status = "Not Found"
            while True:
                try:
                    obj = api.get(name=pipelinerun_name, namespace=namespace)
                    resource_version = obj.metadata.resourceVersion
                    for event in watcher.stream(
                        api.get,
                        resource_version=resource_version,
                        namespace=namespace,
                        serialize=False,
                        field_selector=f"metadata.name={pipelinerun_name}"
                    ):
                        assert isinstance(event, Dict)
                        obj = resource.ResourceInstance(api, event["object"])
                        # status takes some time to appear
                        try:
                            status = obj.status.conditions[0].status
                        except AttributeError:
                            pass
                        self._logger.info("PipelineRun %s status: %s.", pipelinerun_name, status)
                        if status not in ["Unknown", "Not Found"]:
                            watcher.stop()
                            return obj
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
