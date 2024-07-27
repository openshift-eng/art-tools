import logging
from pathlib import Path
import shutil
from typing import Dict, Optional, cast

# from kubernetes import client, config, dynamic, utils
from kubernetes_asyncio import client, config
from kubernetes_asyncio.dynamic import exceptions, resource, DynamicClient
from kubernetes_asyncio.client.configuration import Configuration
# from openshift.dynamic import DynamicClient, exceptions
from ruamel.yaml import YAML

from doozerlib.backend.build_repo import BuildRepo
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolution, SourceResolver

yaml = YAML(typ="safe")
LOGGER = logging.getLogger(__name__)


class KonfluxImageBuildError(Exception):
    def __init__(self, message: str, pipelinerun_name: str, pipelinerun: Optional[Dict]) -> None:
        super().__init__(message)
        self.pipelinerun_name = pipelinerun_name
        self.pipelinerun = pipelinerun


class KonfluxImageBuilder:
    """ This class is responsible for building container images with Konflux. """

    def __init__(self, runtime: Runtime, source_resolver: SourceResolver) -> None:
        self.upcycle = False
        self._konflux_url = ""
        self._konflux_namespace = "crt-nshift-art-pipeli-tenant"
        self._base_dir = Path(runtime.working_dir, "konflux_build_sources")
        self._runtime = runtime
        self._source_resolver = source_resolver
        self._logger = LOGGER
        pass

    async def build(self, metadata: ImageMetadata):
        """ Build a container image with Konflux. """
        cfg = Configuration()
        await config.load_kube_config(client_configuration=cfg)
        async with client.ApiClient(configuration=cfg) as api_client:
            dyn_client = await DynamicClient(api_client)

            # Ensure the Application resource exists
            app_manifest = self._new_application("test-ocp-4-17", "Test OCP 4.17 App")
            app = await self._create_or_patch(dyn_client, app_manifest)
            LOGGER.info(f"Created application: {app}")

            # Ensure the component resource exists
            # source = cast(SourceResolution, await exectools.to_thread(self._source_resolver.resolve_source, metadata))
            dest_dir = self._base_dir.joinpath(metadata.qualified_key)
            dest_branch = "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format_map({
                "group": self._runtime.group,
                "assembly_name": self._runtime.assembly,
                "distgit_key": metadata.distgit_key
            })
            source = self._source_resolver.resolve_source(metadata)
            build_repo = await self.setup_build_repo(source.url, dest_branch, dest_dir)

            component_manifest = self._new_component(
                f"test-4-17-{metadata.distgit_key}",
                "test-ocp-4-17",
                f"test-4-17-{metadata.distgit_key}",
                source.public_upstream_url
            )
            component = await self._create_or_patch(dyn_client, component_manifest)
            LOGGER.info(f"Created component: {component}")

            # Ensure the PipelineRun resource exists
            pipeline_runs_api = await dyn_client.resources.get(
                api_version="tekton.dev/v1",
                kind="PipelineRun",
            )
            pipeline_run_manifest = self._new_pipeline_run(
                f"test-4-17-{metadata.distgit_key}-build-",
                "test-ocp-4-17",
                f"test-4-17-{metadata.distgit_key}",
                source.public_upstream_url,
                dest_branch,
                "quay.io/redhat-user-workloads/crt-nshift-art-pipeli-tenant/test-app/test-ocp-build-data:test"
            )
            pipeline_run = await pipeline_runs_api.create(
                namespace=self._konflux_namespace,
                body=pipeline_run_manifest
            )
            pipeline_run_name = pipeline_run['metadata']['name']
            LOGGER.info(f"Created PipelineRun: {pipeline_run_name}")
            LOGGER.info("Waiting for PipelineRun %s to complete...", pipeline_run_name)
            pipeline_run = await self._wait_for_pipelinerun(pipeline_runs_api, pipeline_run_name)
            if pipeline_run.status.conditions[0].status != "True":
                raise KonfluxImageBuildError(f"Konflux image build for {metadata.distgit_key} failed", pipeline_run_name, pipeline_run)

        return pipeline_run_name, pipeline_run

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
    def _new_component(name: str, application: str, component_name: str, source_url: str,) -> dict:
        obj = {
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "Component",
            "metadata": {
                "name": name
            },
            "spec": {
                "application": application,
                "componentName": component_name,
                "source": {
                    "git": {
                        "url": source_url,
                    }
                }
            }
        }
        return obj

    def _new_pipeline_run(self, generate_name: str, application_name: str, component_name: str,
                          git_url: str, revision: str, output_image: str) -> dict:
        obj = {
            "apiVersion": "tekton.dev/v1",
            "kind": "PipelineRun",
            "metadata": {
                "generateName": generate_name,
                "labels": {
                    "appstudio.openshift.io/application": application_name,
                    "appstudio.openshift.io/component": component_name,
                }
            },
            "spec": {
                "params": [
                    {"name": "git-url", "value": git_url},
                    {"name": "output-image", "value": output_image},
                    {"name": "revision", "value": revision},
                    {"name": "dockerfile", "value": "/Dockerfile"},
                ],
                "pipelineRef": {
                    "resolver": "bundles",
                    "params": [
                        {
                            "name": "name",
                            "value": "docker-build"
                        },
                        {
                            "name": "bundle",
                            "value": "quay.io/konflux-ci/tekton-catalog/pipeline-docker-build:f3ac40bbc0230eccb8d98a4d54dabd55a4943c5d"
                        },
                        {
                            "name": "kind",
                            "value": "pipeline"
                        }
                    ]
                },
                "taskRunTemplate": {
                    "serviceAccountName": "appstudio-pipeline"
                },
                "timeouts": {
                    "pipeline": "1h0m0s"
                },
                "workspaces": [
                    {
                        "name": "workspace",
                        "volumeClaimTemplate": {
                            "spec": {
                                "accessModes": [
                                    "ReadWriteOnce"
                                ],
                                "resources": {
                                    "requests": {
                                        "storage": "1Gi"
                                    }
                                }
                            }
                        }
                    },
                    # {
                    #     "name": "git-auth",
                    #     "secret": {
                    #         "secretName": "git-auth"
                    #     }
                    # }
                ]

            }
        }
        return obj

    async def setup_build_repo(self, url: str, branch: str, local_dir: Path) -> BuildRepo:
        """ Clone the build repository and return a BuildRepo object. """
        build_repo = BuildRepo(url=url, branch=branch, local_dir=local_dir, logger=self._logger)
        # Clone the build repository
        needs_clone = True
        if build_repo.exists():
            if self.upcycle:
                self._logger.info("Upcycling existing build source repository at %s", local_dir)
                shutil.rmtree(local_dir)
            else:
                self._logger.info("Reusing existing build source repository at %s", local_dir)
                needs_clone = False
        if needs_clone:
            self._logger.info("Cloning build source repository %s on branch %s into %s...", build_repo.url, build_repo.branch, build_repo.local_dir)
            await build_repo.clone()
        return build_repo

    async def _create_or_patch(self, dyn_client: DynamicClient, manifest: dict):
        name = manifest["metadata"]["name"]
        namespace = manifest["metadata"].get("namespace", self._konflux_namespace)
        api_version = manifest["apiVersion"]
        kind = manifest["kind"]
        api = await dyn_client.resources.get(
            api_version=api_version,
            kind=kind,
        )
        found = False
        try:
            await api.get(name=name, namespace=namespace)
            found = True
        except exceptions.NotFoundError as e:
            pass
        if found:
            new = await api.patch(
                body=manifest,
                namespace=namespace,
                content_type="application/merge-patch+json",
            )
        else:
            new = await api.create(
                namespace=namespace,
                body=manifest
            )
        return new

    async def _wait_for_pipelinerun(self, api, pipeline_run_name: str):
        async for event in api.watch(namespace=self._konflux_namespace, name=pipeline_run_name):
            obj = cast(resource.ResourceInstance, event["object"])
            status = obj.status.conditions[0].status
            LOGGER.info("PipelineRun %s status: %s", pipeline_run_name, status)
            if status != "Unknown":
                return obj
