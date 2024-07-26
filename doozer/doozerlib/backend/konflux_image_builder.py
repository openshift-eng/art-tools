import logging
from pathlib import Path
import shutil
from typing import cast

from kubernetes import client, config, dynamic, utils
from openshift.dynamic import DynamicClient, exceptions
from ruamel.yaml import YAML

from doozerlib.backend.build_repo import BuildRepo
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime
from doozerlib.source_resolver import SourceResolution, SourceResolver

yaml = YAML(typ="safe")
LOGGER = logging.getLogger(__name__)


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

    async def build(self, metadata: ImageMetadata) -> None:
        # api_client = client.ApiClient()
        api_client = config.new_client_from_config()
        dyn_client = dynamic.DynamicClient(
            api_client
        )
        # v1 = client.CoreV1Api(api_client)

        # Ensure the Application resource exists

        apps_api = dyn_client.resources.get(
            api_version="appstudio.redhat.com/v1alpha1",
            kind="Application",
        )
        app_manifest = self._new_application("test-ocp-4-17", "Test OCP 4.17 App")
        try:
            app = apps_api.get(name="test-ocp-4-17", namespace=self._konflux_namespace)
            app = apps_api.patch(
                body=app_manifest,
                namespace=self._konflux_namespace,
                content_type="application/merge-patch+json",
            )
        except exceptions.NotFoundError as e:
            LOGGER.info(f"Application not found: {e}")
            app = apps_api.create(
                namespace=self._konflux_namespace,
                body=app_manifest
            )
        # app = utils.create_from_dict(dyn_client, app_manifest)
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

        components_api = dyn_client.resources.get(
            api_version="appstudio.redhat.com/v1alpha1",
            kind="Component",
        )
        component_manifest = self._new_component(
            f"test-4-17-{metadata.distgit_key}",
            "test-ocp-4-17",
            f"test-4-17-{metadata.distgit_key}",
            source.public_upstream_url
        )
        try:
            component = components_api.get(name=f"test-4-17-{metadata.distgit_key}", namespace=self._konflux_namespace)
            component = components_api.patch(
                body=component_manifest,
                namespace=self._konflux_namespace,
                content_type="application/merge-patch+json",
            )
        except exceptions.NotFoundError as e:
            LOGGER.info(f"Component not found: {e}")
            component = components_api.create(
                namespace=self._konflux_namespace,
                body=component_manifest
            )
        LOGGER.info(f"Created component: {component}")

        # Ensure the PipelineRun resource exists
        pipeline_runs_api = dyn_client.resources.get(
            api_version="tekton.dev/v1",
            kind="PipelineRun",
        )
        pipeline_run_manifest = self._new_pipeline_run(
            f"test-4-17-{metadata.distgit_key}-build-1",
            source.public_upstream_url,
            dest_branch,
            "quay.io/redhat-user-workloads/crt-nshift-art-pipeli-tenant/test-app/test-ocp-build-data:test"
        )
        try:
            pipeline_run = pipeline_runs_api.get(name=f"test-4-17-{metadata.distgit_key}-build-1", namespace=self._konflux_namespace)
            pipeline_run = pipeline_runs_api.patch(
                body=pipeline_run_manifest,
                namespace=self._konflux_namespace,
                content_type="application/merge-patch+json",
            )
        except exceptions.NotFoundError as e:
            LOGGER.info(f"PipelineRun not found: {e}")
            pipeline_run = pipeline_runs_api.create(
                namespace=self._konflux_namespace,
                body=pipeline_run_manifest
            )
        LOGGER.info(f"Created PipelineRun: {pipeline_run}")

        pass

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

    def _new_pipeline_run(self, name: str, git_url: str, revision: str, output_image: str) -> dict:
        obj = {
            "apiVersion": "tekton.dev/v1",
            "kind": "PipelineRun",
            "metadata": {
                "name": name
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
