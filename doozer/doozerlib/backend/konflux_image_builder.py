import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, cast

from artcommonlib import exectools
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.exectools import limit_concurrency
from artcommonlib.konflux.konflux_build_record import (ArtifactType, Engine,
                                                       KonfluxBuildOutcome,
                                                       KonfluxBuildRecord)
from artcommonlib.release_util import isolate_el_version_in_release
from dockerfile_parse import DockerfileParser
from kubernetes.dynamic import resource

from doozerlib import constants
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.image import ImageMetadata
from doozerlib.source_resolver import SourceResolution

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
    namespace: str
    kubeconfig: Optional[str] = None
    context: Optional[str] = None
    image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO
    skip_checks: bool = False
    dry_run: bool = False


class KonfluxImageBuilder:
    """ This class is responsible for building container images with Konflux. """

    # https://gitlab.cee.redhat.com/konflux/docs/users/-/blob/main/topics/getting-started/multi-platform-builds.md
    SUPPORTED_ARCHES = {
        # Only x86_64 is supported, until we are on the new cluster
        "x86_64": "linux/x86_64",
        "s390x": "linux/s390x",
        "ppc64le": "linux/ppc64le",
        "aarch64": "linux/arm64",
    }

    def __init__(self, config: KonfluxImageBuilderConfig, logger: Optional[logging.Logger] = None) -> None:
        self._config = config
        self._logger = logger or LOGGER
        self._konflux_client = KonfluxClient.from_kubeconfig(config.namespace, config.kubeconfig, config.context, config.dry_run)

    @limit_concurrency(limit=constants.MAX_KONFLUX_BUILD_QUEUE_SIZE)
    async def build(self, metadata: ImageMetadata):
        """ Build a container image with Konflux. """
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        metadata.build_status = False
        try:
            dest_dir = self._config.base_dir.joinpath(metadata.qualified_key)
            if dest_dir.exists():
                # Load exiting build source repository
                build_repo = await BuildRepo.from_local_dir(dest_dir, logger)
            else:
                # Clone the build source repository
                source = None
                if metadata.has_source():
                    logger.info(f"Resolving source for {metadata.qualified_key}")
                    source = cast(SourceResolution, await exectools.to_thread(metadata.runtime.source_resolver.resolve_source, metadata))
                else:
                    raise IOError(f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported.")
                dest_branch = "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format_map({
                    "group": metadata.runtime.group,
                    "assembly_name": metadata.runtime.assembly,
                    "distgit_key": metadata.distgit_key
                })
                build_repo = BuildRepo(url=source.url, branch=dest_branch, local_dir=dest_dir, logger=logger)
                await build_repo.ensure_source()

            # Wait for parent members to be built
            parent_members = await self._wait_for_parent_members(metadata)
            failed_parents = [parent_member.distgit_key for parent_member in parent_members if parent_member is not None and not parent_member.build_status]
            if failed_parents:
                raise IOError(f"Couldn't build {metadata.distgit_key} because the following parent images failed to build: {', '.join(failed_parents)}")

            # Start the build
            logger.info("Starting Konflux image build for %s...", metadata.distgit_key)
            retries = 3
            building_arches = metadata.get_arches()
            error = None
            for attempt in range(retries):
                logger.info("Build attempt %s/%s", attempt + 1, retries)
                pipelinerun = await self._start_build(metadata, build_repo, building_arches)
                await self.update_konflux_db(metadata, build_repo, pipelinerun, KonfluxBuildOutcome.PENDING, building_arches)

                pipelinerun_name = pipelinerun['metadata']['name']
                logger.info("Waiting for PipelineRun %s to complete...", pipelinerun_name)
                pipelinerun = await self._konflux_client.wait_for_pipelinerun(pipelinerun_name, namespace=self._config.namespace)
                logger.info("PipelineRun %s completed", pipelinerun_name)

                status = pipelinerun.status.conditions[0].status
                outcome = KonfluxBuildOutcome.SUCCESS if status == "True" else KonfluxBuildOutcome.FAILURE
                if self._config.dry_run:
                    logger.info("Dry run: Would have inserted build record in Konflux DB")
                else:
                    await self.update_konflux_db(metadata, build_repo, pipelinerun, outcome, building_arches)

                if status != "True":
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
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        parent_members = list(metadata.get_parent_members().values())
        for parent_member in parent_members:
            if parent_member is None:
                continue  # Parent member is not included in the group; no need to wait
            logger.info("Parent image %s is building; waiting...", parent_member.distgit_key)
            # wait for parent member to be built
            while not parent_member.build_event.is_set():
                # asyncio.sleep instead of Event.wait since it's less CPU intensive
                await asyncio.sleep(20)  # check every 20 seconds
        return parent_members

    async def _start_build(self, metadata: ImageMetadata, build_repo: BuildRepo, building_arches: list):
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        if not build_repo.commit_hash:
            raise IOError(f"The build branch {build_repo.branch} doesn't have any commits in the build repository {build_repo.https_url}")

        git_branch = build_repo.branch or build_repo.commit_hash
        git_url = build_repo.https_url
        git_commit = build_repo.commit_hash

        df_path = build_repo.local_dir.joinpath("Dockerfile")
        df = DockerfileParser(str(df_path))
        if "__doozer_uuid_tag" not in df.envs:
            raise ValueError(f"[{metadata.distgit_key}] Dockerfile must have a '__doozer_uuid_tag' environment variable; Did you forget to run 'doozer beta:images:konflux:rebase' first?")

        # Ensure the Application resource exists
        app_name = self._config.group_name.replace(".", "-")
        logger.info(f"Using application: {app_name}")
        await self._konflux_client.ensure_application(name=app_name, display_name=app_name)

        # Ensure the component resource exists
        # Openshift doesn't allow dots or underscores in any of its fields, so we replace them with dashes
        component_name = f"{app_name}-{metadata.distgit_key}".replace(".", "-").replace("_", "-")

        # 'openshift-4-18-ose-installer-terraform' -> 'ose-4-18-ose-installer-terraform'
        # A component resource name must start with a lower case letter and must be no more than 63 characters long.
        component_name = f"ose-{component_name.removeprefix('openshift-')}"

        dest_image_repo = self._config.image_repo
        dest_image_tag = df.envs["__doozer_uuid_tag"]
        version = df.labels.get("version")
        release = df.labels.get("release")
        if not version or not release:
            raise ValueError(f"[{metadata.distgit_key}] `version` and `release` labels are required.")
        additional_tags = [
            f"{metadata.image_name_short}-{version}-{release}"
        ]
        default_revision = f"art-{self._config.group_name}-assembly-test-dgk-{metadata.distgit_key}"
        logger.info(f"Using component: {component_name}")
        await self._konflux_client.ensure_component(
            name=component_name,
            application=app_name,
            component_name=component_name,
            image_repo=dest_image_repo,
            source_url=git_url,
            revision=default_revision,
        )

        # Start a PipelineRun
        pipelinerun = await self._konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{component_name}-",
            namespace=self._config.namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=git_url,
            commit_sha=git_commit,
            target_branch=git_branch,
            output_image=f"{dest_image_repo}:{dest_image_tag}",
            building_arches=building_arches,
            additional_tags=additional_tags,
            skip_checks=self._config.skip_checks,
            vm_override=metadata.config.get("konflux", {}).get("vm_override")
        )

        logger.info(f"Created PipelineRun: {self.build_pipeline_url(pipelinerun)}")
        return pipelinerun

    def build_pipeline_url(self, pipelinerun):
        return self._konflux_client.build_pipeline_url(pipelinerun)

    @staticmethod
    async def get_installed_packages(image_pullspec, arches) -> list:
        """
        Example sbom: https://gist.github.com/thegreyd/6718f4e4dae9253310c03b5d492fab68
        :return: Returns list of installed rpms for an image pullspec, assumes that the sbom exists in registry
        """
        async def _get_for_arch(arch):
            go_arch = go_arch_for_brew_arch(arch)

            cmd = [
                "cosign",
                "download",
                "sbom",
                image_pullspec,
                "--platform", f"linux/{go_arch}"
            ]
            _, stdout, _ = await exectools.cmd_gather_async(cmd)
            sbom_contents = json.loads(stdout)
            source_rpms = set()
            for x in sbom_contents["components"]:
                if x["bom-ref"].startswith("pkg:rpm"):
                    for i in x["properties"]:
                        if i["name"] == "syft:metadata:sourceRpm":
                            source_rpms.add(i["value"].rstrip(".src.rpm"))
                            break
            return source_rpms

        results = await asyncio.gather(*(_get_for_arch(arch) for arch in arches))
        for arch, result in zip(arches, results):
            if not result:
                raise ChildProcessError(f"Could not get rpms from SBOM for arch {arch}")
        installed_packages = set()
        for srpms in results:
            installed_packages.update(srpms)
        return sorted(installed_packages)

    async def update_konflux_db(self, metadata, build_repo, pipelinerun, outcome, building_arches):
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        if not metadata.runtime.konflux_db:
            logger.warning('Konflux DB connection is not initialized, not writing build record to the Konflux DB.')
            return

        try:
            rebase_repo_url = build_repo.https_url
            rebase_commit = build_repo.commit_hash

            df_path = build_repo.local_dir.joinpath("Dockerfile")
            df = DockerfileParser(str(df_path))

            source_repo = df.labels['io.openshift.build.source-location']
            commitish = df.labels['io.openshift.build.commit.id']

            component_name = df.labels['com.redhat.component']
            version = df.labels['version']
            release = df.labels['release']
            nvr = "-".join([component_name, version, release])

            pipelinerun_name = pipelinerun['metadata']['name']
            build_pipeline_url = self.build_pipeline_url(pipelinerun)

            build_record_params = {
                'name': metadata.distgit_key,
                'version': version,
                'release': release,
                'el_target': f'el{isolate_el_version_in_release(release)}',
                'arches': building_arches,
                'embargoed': 'p1' in release.split('.'),
                'start_time': datetime.now(tz=timezone.utc),
                'end_time': None,
                'nvr': nvr,
                'group': metadata.runtime.group,
                'assembly': metadata.runtime.assembly,
                'source_repo': source_repo,
                'commitish': commitish,
                'rebase_repo_url': rebase_repo_url,
                'rebase_commitish': rebase_commit,
                'artifact_type': ArtifactType.IMAGE,
                'engine': Engine.KONFLUX,
                'outcome': outcome,
                'parent_images': df.parent_images,
                'art_job_url': os.getenv('BUILD_URL', 'n/a'),
                'build_id': pipelinerun_name,
                'build_pipeline_url': build_pipeline_url,
                'pipeline_commit': 'n/a'  # TODO: populate this
            }

            if outcome == KonfluxBuildOutcome.SUCCESS:
                # results:
                # - name: IMAGE_URL
                #   value: quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:ose-network-metrics-daemon-rhel9-v4.18.0-20241001.151532
                # - name: IMAGE_DIGEST
                #   value: sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807

                image_pullspec = next((r['value'] for r in pipelinerun.status.results if r['name'] == 'IMAGE_URL'), None)
                image_digest = next((r['value'] for r in pipelinerun.status.results if r['name'] == 'IMAGE_DIGEST'), None)

                if not (image_pullspec and image_digest):
                    raise ValueError(f"[{metadata.distgit_key}] Could not find expected results in konflux "
                                     f"pipelinerun {pipelinerun_name}")

                start_time = pipelinerun.status.startTime
                end_time = pipelinerun.status.completionTime

                installed_packages = await self.get_installed_packages(image_pullspec, building_arches)

                build_record_params.update({
                    'image_pullspec': image_pullspec,
                    'installed_packages': installed_packages,
                    'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ'),
                    'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ'),
                    'image_tag': image_digest.split('sha256:')[-1],
                })
            elif outcome == KonfluxBuildOutcome.FAILURE:
                start_time = pipelinerun.status.startTime
                end_time = pipelinerun.status.completionTime
                build_record_params.update({
                    'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ'),
                    'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
                })

            build_record = KonfluxBuildRecord(**build_record_params)
            metadata.runtime.konflux_db.add_build(build_record)
            logger.info(f'Konflux build info stored successfully with status {outcome}')

        except Exception as err:
            logger.error('Failed writing record to the konflux DB: %s', err)
