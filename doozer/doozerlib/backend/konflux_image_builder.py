import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Sequence, cast

import jinja2
from artcommonlib import exectools
from artcommonlib import util as art_util
from dockerfile_parse import DockerfileParser
from importlib_resources import files
from kubernetes import client, config, watch
from kubernetes.client import Configuration
from kubernetes.dynamic import DynamicClient, exceptions, resource
from ruamel.yaml import YAML

from doozerlib import constants
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.image import ImageMetadata
from doozerlib.source_resolver import SourceResolution

yaml = YAML(typ="safe")
LOGGER = logging.getLogger(__name__)


class KonfluxImageBuildError(Exception):
    def __init__(self, message: str, pipelinerun_name: str, pipelinerun: Optional[resource.ResourceInstance]) -> None:
        super().__init__(message)
        self.pipelinerun_name = pipelinerun_name
        self.pipelinerun = pipelinerun


@dataclass
class KonfluxImageBuilderConfig:
    """ Options for the KonfluxImageBuilder class. """
    base_dir: Path
    group_name: str
    kubeconfig: Optional[str] = None
    context: Optional[str] = None
    namespace: Optional[str] = None
    image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO
    dry_run: bool = False


class KonfluxImageBuilder:
    """ This class is responsible for building container images with Konflux. """

    # https://gitlab.cee.redhat.com/konflux/docs/users/-/blob/main/topics/getting-started/multi-platform-builds.md
    SUPPORTED_ARCHES = {
        "x86_64": "linux/x86_64",
        "s390x": "linux/s390x",
        "ppc64le": "linux/ppc64le",
        "aarch64": "linux/arm64",
    }

    def __init__(self, config: KonfluxImageBuilderConfig, logger: Optional[logging.Logger] = None) -> None:
        self._config = config
        self._logger = logger or LOGGER

    async def build(self, metadata: ImageMetadata):
        """ Build a container image with Konflux. """
        metadata.build_status = False
        try:
            dest_dir = self._config.base_dir.joinpath(metadata.qualified_key)
            if dest_dir.exists():
                # Load exiting build source repository
                build_repo = await BuildRepo.from_local_dir(dest_dir, self._logger)
            else:
                # Clone the build source repository
                source = None
                if metadata.has_source():
                    self._logger.info(f"Resolving source for {metadata.qualified_key}")
                    source = cast(SourceResolution, await exectools.to_thread(metadata.runtime.source_resolver.resolve_source, metadata))
                else:
                    raise IOError(f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported.")
                dest_branch = "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format_map({
                    "group": metadata.runtime.group,
                    "assembly_name": metadata.runtime.assembly,
                    "distgit_key": metadata.distgit_key
                })
                build_repo = BuildRepo(url=source.url, branch=dest_branch, local_dir=dest_dir, logger=self._logger)
                await build_repo.ensure_source()

            # Load the Kubernetes configuration
            cfg = Configuration()
            config.load_kube_config(config_file=self._config.kubeconfig, context=self._config.context,
                                    persist_config=False, client_configuration=cfg)

            # Wait for parent members to be built
            parent_members = await self._wait_for_parent_members(metadata)
            failed_parents = [parent_member.distgit_key for parent_member in parent_members if parent_member is not None and not parent_member.build_status]
            if failed_parents:
                raise IOError(f"Couldn't build {metadata.distgit_key} because the following parent images failed to build: {', '.join(failed_parents)}")

            # Start the build
            self._logger.info("Starting Konflux image build for %s...", metadata.distgit_key)
            retries = 5
            error = None
            for attempt in range(retries):
                self._logger.info("[%s] Build attempt %s/%s", metadata.distgit_key, attempt + 1, retries)
                with client.ApiClient(configuration=cfg) as api_client:
                    dyn_client = DynamicClient(api_client)

                    pipelinerun = await self._start_build(metadata, build_repo, dyn_client)

                    pipelinerun_name = pipelinerun['metadata']['name']
                    self._logger.info("[%s] Waiting for PipelineRun %s to complete...", metadata.distgit_key, pipelinerun_name)
                    pipelinerun = await self._wait_for_pipelinerun(dyn_client, pipelinerun_name)
                    self._logger.info("[%s] PipelineRun %s completed", metadata.distgit_key, pipelinerun_name)
                    if pipelinerun.status.conditions[0].status != "True":
                        error = KonfluxImageBuildError(f"Konflux image build for {metadata.distgit_key} failed",
                                                       pipelinerun_name, pipelinerun)
                    else:
                        metadata.build_status = True
                        break
            if not metadata.build_status and error:
                raise error
        finally:
            metadata.build_event.set()
        return pipelinerun_name, pipelinerun

    async def _wait_for_parent_members(self, metadata: ImageMetadata):
        # If this image is FROM another group member, we need to wait on that group member to be built
        parent_members = list(metadata.get_parent_members().values())
        for parent_member in parent_members:
            if parent_member is None:
                continue  # Parent member is not included in the group; no need to wait
            # wait for parent member to be built
            while not parent_member.build_event.is_set():
                self._logger.info("[%s] Parent image %s is building; waiting...", metadata.distgit_key, parent_member.distgit_key)
                if await exectools.to_thread(parent_member.build_event.wait, timeout=20):
                    break
        return parent_members

    async def _start_build(self, metadata: ImageMetadata, build_repo: BuildRepo, dyn_client: DynamicClient):
        git_url = build_repo.https_url
        git_branch = build_repo.branch
        assert build_repo.commit_hash is not None, f"[{metadata.distgit_key}] git_commit is required for Konflux image build"
        git_commit = build_repo.commit_hash

        df_path = build_repo.local_dir.joinpath("Dockerfile")
        df = DockerfileParser(str(df_path))
        if "__doozer_uuid_tag" not in df.envs:
            raise ValueError(f"[{metadata.distgit_key}] Dockerfile must have a '__doozer_uuid_tag' environment variable; Did you forget to run 'doozer beta:images:konflux:rebase' first?")

        # Ensure the Application resource exists
        app_name = self._config.group_name.replace(".", "-")
        app_manifest = self._new_application(app_name, app_name)
        app = await self._create_or_patch(dyn_client, app_manifest)
        self._logger.info(f"[%s] Using application: {app['metadata']['name']}", metadata.distgit_key)

        # Ensure the component resource exists
        # Openshift doesn't allow dots in any of its fields, so we replace them with dashes
        component_name = f"{app_name}-{metadata.distgit_key}".replace(".", "-")
        dest_image_repo = self._config.image_repo
        dest_image_tag = df.envs["__doozer_uuid_tag"]
        default_revision = f"art-{self._config.group_name}-assembly-test-dgk-{metadata.distgit_key}"

        component_manifest = self._new_component(
            component_name,
            app_name,
            component_name,
            dest_image_repo,
            git_url,
            default_revision,
        )
        component = await self._create_or_patch(dyn_client, component_manifest)
        self._logger.info(f"[%s] Using component: {component['metadata']['name']}", metadata.distgit_key)

        # Create a PipelineRun
        arches = metadata.get_arches()
        unsupported_arches = set(arches) - set(KonfluxImageBuilder.SUPPORTED_ARCHES)
        if unsupported_arches:
            raise ValueError(f"[{metadata.distgit_key}] Unsupported arches: {', '.join(unsupported_arches)}")
        build_platforms = [self.SUPPORTED_ARCHES[arch] for arch in arches]
        pipelineruns_api = await self._get_pipelinerun_api(dyn_client)
        pipelinerun_manifest = self._new_pipelinerun(
            f"doozer-build-{component_name}-",
            app_name,
            component_name,
            git_url,
            git_commit,
            git_branch,
            f"{dest_image_repo}:{dest_image_tag}",
            build_platforms,
        )

        if self._config.dry_run:
            pipelinerun_manifest = resource.ResourceInstance(dyn_client, pipelinerun_manifest)
            pipelinerun_manifest.metadata.name = f"doozer-build-{component_name}-dry-run"
            self._logger.warning(f"[DRY RUN] [%s] Would have created PipelineRun: {pipelinerun_manifest.metadata.name}", metadata.distgit_key)
            return pipelinerun_manifest

        pipelinerun = await exectools.to_thread(
            pipelineruns_api.create,
            namespace=self._config.namespace,
            body=pipelinerun_manifest,
            async_req=True,
        )
        pipelinerun = cast(resource.ResourceInstance, pipelinerun)

        pipelinerun_name = pipelinerun['metadata']['name']
        self._logger.info(f"[%s] Created PipelineRun: {pipelinerun_name}", metadata.distgit_key)
        return pipelinerun

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

    @staticmethod
    async def _get_pipelinerun_api(dyn_client: DynamicClient):
        return await exectools.to_thread(
            dyn_client.resources.get,
            api_version="tekton.dev/v1",
            kind="PipelineRun",
        )

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

    @staticmethod
    def _new_pipelinerun(generate_name: str, application_name: str, component_name: str,
                         git_url: str, commit_sha: str, target_branch: str, output_image: str,
                         build_platforms: Sequence[str], git_auth_secret: str = "pipelines-as-code-secret") -> dict:
        https_url = art_util.convert_remote_git_to_https(git_url)
        # TODO: In the future the PipelineRun template should be loaded from a remote git repo.
        template_content = files("doozerlib").joinpath("backend").joinpath("konflux_image_build_pipelinerun.yaml").read_text()
        template = jinja2.Template(template_content, autoescape=True)
        rendered = template.render({
            "source_url": https_url,
            "revision": commit_sha,
            "target_branch": target_branch,
            "git_auth_secret": git_auth_secret,

        })
        obj = yaml.load(rendered)
        # Those fields in the template are specific to an image. They need to be removed.
        del obj["metadata"]["name"]
        del obj["metadata"]["namespace"]
        del obj["metadata"]["annotations"]["pipelinesascode.tekton.dev/on-cel-expression"]
        # Override the generated name with the provided one
        if generate_name:
            obj["metadata"]["generateName"] = generate_name
        # Set the application and component names
        obj["metadata"]["annotations"]["build.appstudio.openshift.io/repo"] = f"{https_url}?rev={commit_sha}"
        obj["metadata"]["labels"]["appstudio.openshift.io/application"] = application_name
        obj["metadata"]["labels"]["appstudio.openshift.io/component"] = component_name
        for param in obj["spec"]["params"]:
            if param["name"] == "output-image":
                param["value"] = output_image
            if param["name"] == "skip-checks":
                param["value"] = "true"
        obj["spec"]["params"].append({"name": "build-platforms", "value": list(build_platforms)})
        return obj

    async def _create_or_patch(self, dyn_client: DynamicClient, manifest: dict):
        name = manifest["metadata"]["name"]
        namespace = manifest["metadata"].get("namespace", self._config.namespace)
        api_version = manifest["apiVersion"]
        kind = manifest["kind"]
        api = await exectools.to_thread(
            dyn_client.resources.get,
            api_version=api_version,
            kind=kind,
        )
        found = True
        try:
            await exectools.to_thread(api.get, name=name, namespace=namespace)
        except exceptions.NotFoundError:
            found = False
        if self._config.dry_run:
            if found:
                self._logger.warning(f"[DRY RUN] Would have patched {api_version}/{kind} {namespace}/{name}")
            else:
                self._logger.warning(f"[DRY RUN] Would have created {api_version}/{kind} {namespace}/{name}")
            return resource.ResourceInstance(dyn_client, manifest)
        if found:
            self._logger.info(f"Patching {api_version}/{kind} {namespace}/{name}")
            new = await exectools.to_thread(
                api.patch,
                body=manifest,
                namespace=namespace,
                content_type="application/merge-patch+json",
            )
        else:
            self._logger.info(f"Creating {api_version}/{kind} {namespace}/{name}")
            new = await exectools.to_thread(
                api.create,
                namespace=namespace,
                body=manifest,
            )
        new = cast(resource.ResourceInstance, new)
        return new

    async def _wait_for_pipelinerun(self, dyn_client: DynamicClient, pipelinerun_name: str):
        if self._config.dry_run:
            await asyncio.sleep(3)
            pipelinerun = {
                "metadata": {"name": pipelinerun_name, "namespace": self._config.namespace},
                "apiVersion": "tekton.dev/v1",
                "kind": "PipelineRun",
                "status": {"conditions": [{"status": "True"}]}
            }
            self._logger.warning(f"[DRY RUN] Would have waited for PipelineRun {pipelinerun_name} to complete")
            return resource.ResourceInstance(dyn_client, pipelinerun)

        api = await self._get_pipelinerun_api(dyn_client)

        def _inner():
            watcher = watch.Watch()
            while True:
                try:
                    obj = api.get(name=pipelinerun_name, namespace=self._config.namespace)
                    resource_version = obj.metadata.resourceVersion
                    for event in watcher.stream(
                        api.get,
                        resource_version=resource_version,
                        namespace=self._config.namespace,
                        serialize=False,
                        field_selector=f"metadata.name={pipelinerun_name}"
                    ):
                        assert isinstance(event, Dict)
                        obj = resource.ResourceInstance(api, event["object"])
                        # status takes some time to appear
                        status = "Not Found"
                        try:
                            status = obj.status.conditions[0].status
                        except AttributeError:
                            pass
                        self._logger.info("PipelineRun %s status: %s", pipelinerun_name, status)
                        if status not in ["Unknown", "Not Found"]:
                            return obj
                except TimeoutError:
                    self._logger.error("Timeout waiting for PipelineRun %s to complete", pipelinerun_name)
                    continue
        return await exectools.to_thread(_inner)
