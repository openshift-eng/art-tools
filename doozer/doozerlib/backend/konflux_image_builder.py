import asyncio
import json
import logging
import pprint
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional, cast, List

from artcommonlib import util as artlib_util
from artcommonlib import constants as artlib_constants
from artcommonlib import exectools, bigquery
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.exectools import limit_concurrency
from artcommonlib.konflux.konflux_build_record import (ArtifactType, Engine,
                                                       KonfluxBuildOutcome,
                                                       KonfluxBuildRecord)
from artcommonlib.model import Missing, ListModel
from artcommonlib.release_util import isolate_el_version_in_release
from dockerfile_parse import DockerfileParser
from kubernetes.dynamic import resource
from packageurl import PackageURL

from doozerlib import constants
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.build_visibility import is_release_embargoed
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
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
    plr_template: str
    kubeconfig: Optional[str] = None
    context: Optional[str] = None
    image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO
    skip_checks: bool = False
    dry_run: bool = False


class KonfluxImageBuilder:
    """ This class is responsible for building container images with Konflux. """

    def __init__(
        self,
        config: KonfluxImageBuilderConfig,
        logger: Optional[logging.Logger] = None,
        record_logger: Optional[RecordLogger] = None,
    ):
        """ Initialize the KonfluxImageBuilder.

        :param config: Options for the KonfluxImageBuilder.
        :param logger: Logger to use for logging. Defaults to the module logger.
        :param record_logger: Logger to use for logging build records. If None, no build records will be logged.
        """
        self._config = config
        self._logger = logger or LOGGER
        self._record_logger = record_logger
        self._konflux_client = KonfluxClient.from_kubeconfig(default_namespace=config.namespace, config_file=config.kubeconfig, context=config.context, dry_run=config.dry_run)

        if self._config.image_repo == constants.KONFLUX_DEFAULT_IMAGE_REPO:
            for secret in ["KONFLUX_ART_IMAGES_USERNAME", "KONFLUX_ART_IMAGES_PASSWORD"]:
                if secret not in os.environ:
                    raise EnvironmentError(f"Missing required environment variable {secret}")

    @limit_concurrency(limit=constants.MAX_KONFLUX_BUILD_QUEUE_SIZE)
    async def build(self, metadata: ImageMetadata):
        """ Build a container image with Konflux. """
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        metadata.build_status = False
        dest_dir = self._config.base_dir.joinpath(metadata.qualified_key)
        df_path = dest_dir.joinpath("Dockerfile")
        record = {
            "dir": str(dest_dir.absolute()),
            "dockerfile": str(df_path.absolute()),
            "name": metadata.distgit_key,
            "nvrs": "n/a",
            "message": "Unknown failure",
            "task_id": "n/a",
            "task_url": "n/a",
            "status": -1,  # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            "has_olm_bundle": 1 if metadata.is_olm_operator else 0,
        }
        try:
            if dest_dir.exists():
                # Load exiting build source repository
                build_repo = await BuildRepo.from_local_dir(dest_dir, logger)
            else:
                # Clone the build source repository
                source = None
                if metadata.has_source():
                    logger.info(f"Resolving source for {metadata.qualified_key}")
                    source = cast(SourceResolution, await exectools.to_thread(metadata.runtime.source_resolver.resolve_source, metadata, no_clone=True))
                else:
                    raise IOError(f"Image {metadata.qualified_key} doesn't have upstream source. This is no longer supported.")
                dest_branch = "art-{group}-assembly-{assembly_name}-dgk-{distgit_key}".format_map({
                    "group": metadata.runtime.group,
                    "assembly_name": metadata.runtime.assembly,
                    "distgit_key": metadata.distgit_key
                })
                build_repo = BuildRepo(url=source.url, branch=dest_branch, local_dir=dest_dir, logger=logger)
                await build_repo.ensure_source()

            # Parse Dockerfile
            uuid_tag, version, release = self._parse_dockerfile(metadata.distgit_key, df_path)
            nvr = f"{metadata.distgit_key}-{version}-{release}"

            # Sanity check to make sure a successful NVR build doesn't already exist in DB
            where = {"engine": Engine.KONFLUX.value}
            build_records = await metadata.runtime.konflux_db.get_build_records_by_nvrs([nvr],
                                                                                        outcome=KonfluxBuildOutcome.SUCCESS,
                                                                                        where=where,
                                                                                        strict=False)
            build_records = [b for b in build_records if b]
            if build_records:
                raise ValueError(f"Successful NVR build {nvr} already exists in DB! "
                                 f"pullspec: {build_records[0].image_pullspec}. "
                                 "To rebuild, please do another rebase")

            record["nvrs"] = nvr
            output_image = f"{self._config.image_repo}:{uuid_tag}"
            additional_tags = [
                f"{metadata.image_name_short}-{version}-{release}"
            ]

            # Wait for parent members to be built
            parent_members = await self._wait_for_parent_members(metadata)
            failed_parents = [parent_member.distgit_key for parent_member in parent_members if parent_member is not None and not parent_member.build_status]
            if failed_parents:
                raise IOError(f"Couldn't build {metadata.distgit_key} because the following parent images failed to build: {', '.join(failed_parents)}")

            # Start the build
            logger.info("Starting Konflux image build for %s...", metadata.distgit_key)
            retries = 3
            building_arches = metadata.get_arches()
            logger.info(f"Building for arches: {building_arches}")
            error = None
            for attempt in range(retries):
                logger.info("Build attempt %s/%s", attempt + 1, retries)
                pipelinerun = await self._start_build(metadata=metadata,
                                                      build_repo=build_repo,
                                                      building_arches=building_arches,
                                                      output_image=output_image,
                                                      additional_tags=additional_tags,
                                                      dest_dir=dest_dir)
                pipelinerun_name = pipelinerun['metadata']['name']
                record["task_id"] = pipelinerun_name
                record["task_url"] = self._konflux_client.build_pipeline_url(pipelinerun)
                await self.update_konflux_db(metadata, build_repo, pipelinerun, KonfluxBuildOutcome.PENDING, building_arches)

                logger.info("Waiting for PipelineRun %s to complete...", pipelinerun_name)
                timeout_timedelta = None
                if metadata.config.konflux.build_timeout:
                    timeout_timedelta = timedelta(minutes=int(metadata.config.konflux.build_timeout))

                pipelinerun, pod_list = await self._konflux_client.wait_for_pipelinerun(pipelinerun_name, namespace=self._config.namespace, overall_timeout_timedelta=timeout_timedelta)
                logger.info("PipelineRun %s completed", pipelinerun_name)

                succeeded_condition = artlib_util.KubeCondition.find_condition(pipelinerun, 'Succeeded')
                outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)

                if self._config.dry_run:
                    logger.info("Dry run: Would have inserted build record in Konflux DB")
                else:
                    await self.update_konflux_db(metadata, build_repo, pipelinerun, outcome, building_arches, pod_list)

                if outcome is not KonfluxBuildOutcome.SUCCESS:
                    error = KonfluxImageBuildError(f"Konflux image build for {metadata.distgit_key} failed with output={outcome}",
                                                   pipelinerun_name, pipelinerun)
                else:
                    metadata.build_status = True
                    record["message"] = "Success"
                    record["status"] = 0
                    break
            if not metadata.build_status and error:
                record["message"] = str(error)
                raise error
        finally:
            if self._record_logger:
                self._record_logger.add_record("image_build_konflux", **record)
            metadata.build_event.set()
        return pipelinerun_name, pipelinerun

    def _parse_dockerfile(self, distgit_key: str, df_path: Path):
        """ Parse the Dockerfile and return the UUID tag, version, and release.

        :param distgit_key: The distgit key of the image.
        :param df_path: The path to the Dockerfile.
        :return: A tuple containing the UUID tag, version, and release.
        :raises ValueError: If the Dockerfile is missing the required environment variables or labels.
        """
        df = DockerfileParser(str(df_path))
        uuid_tag = df.envs.get("__doozer_uuid_tag")
        if not uuid_tag:
            raise ValueError(f"[{distgit_key}] Dockerfile must have a '__doozer_uuid_tag' environment variable; Did you forget to run 'doozer beta:images:konflux:rebase' first?")
        version = df.labels.get("version")
        if not version:
            raise ValueError(f"[{distgit_key}] Dockerfile must have a 'version' label.")
        release = df.labels.get("release")
        if not release:
            raise ValueError(f"[{distgit_key}] Dockerfile must have a 'release' label.")
        return uuid_tag, version, release

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

    @staticmethod
    def get_application_name(group_name: str):
        # "openshift-4-18" -> "openshift-4-18"
        return group_name.replace(".", "-")

    @staticmethod
    def get_component_name(application_name: str, image_name: str):
        # Openshift doesn't allow dots or underscores in any of its fields, so we replace them with dashes
        name = f"{application_name}-{image_name}".replace(".", "-").replace("_", "-")
        # 'openshift-4-18-ose-installer-terraform' -> 'ose-4-18-ose-installer-terraform'
        # A component resource name must start with a lower case letter and must be no more than 63 characters long.
        name = f"ose-{name.removeprefix('openshift-')}"
        return name

    @staticmethod
    def _is_cachi2_enabled(metadata, logger=None):
        """
        Determine if cachi2 is enabled or not
        image config override > group config override > fallback to cachito config
        """
        logger = logger or LOGGER
        cachi2_config_override = metadata.config.konflux.cachi2.enabled
        cachi2_group_override = metadata.runtime.group_config.konflux.cachi2.enabled

        if cachi2_config_override not in [Missing, None]:
            # If cachi2 override is defined in image metadata
            cachi2_enabled = cachi2_config_override
            logger.info("cachi2 enabled from metadata config")
        elif cachi2_group_override not in [Missing, None]:
            # If cachi2 override is defined in group metadata
            cachi2_enabled = cachi2_group_override
            logger.info("cachi2 enabled from group config")
        else:
            # Enable cachi2 based on cachito config
            logger.info("cachi2 override not found. fallback to use cachito config")
            cachi2_enabled = artlib_util.is_cachito_enabled(metadata=metadata,
                                                            group_config=metadata.runtime.group_config, logger=logger)

        return cachi2_enabled

    def _prefetch(self, metadata: ImageMetadata, dest_dir: Optional[Path] = None) -> list:
        """
        To generate the param values for konflux's prefetch dependencies task which uses cachi2 (similar to cachito in
        brew) to fetch packages and make it available to the build task (which ideally will be hermetic)
        https://issues.redhat.com/browse/ART-11902
        """
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")

        cachi2_enabled = self._is_cachi2_enabled(metadata=metadata, logger=logger)

        if not cachi2_enabled:
            logger.info("Not setting pre-fetch since cachi2 not enabled")
            return []

        prefetch = []
        required_package_managers = metadata.config.content.source.pkg_managers

        if required_package_managers in [Missing, None]:
            # We assume that dest_dir is the current directory by default
            required_package_managers = artlib_util.detect_package_managers(metadata=metadata, dest_dir=dest_dir)

        if required_package_managers in [Missing, None]:
            raise ValueError(f"{required_package_managers} should not be empty if cachi2 is enabled")

        for package_manager in ["gomod", "npm", "pip", "yarn"]:
            if package_manager in required_package_managers:
                paths: dict = metadata.config.cachito.packages.get(package_manager, [])

                flag = False
                data = {"type": package_manager}
                for path in paths:
                    data = {"type": package_manager}
                    for entry, values in path.items():
                        if entry == "path":
                            data["path"] = values

                        if entry in ["requirements_files", "requirements_build_files"]:
                            if "requirements_files" not in data:
                                data["requirements_files"] = []
                            if entry == "requirements_files":
                                data["requirements_files"] = data["requirements_files"] + values
                            if entry == "requirements_build_files":
                                data["requirements_files"] = data["requirements_files"] + values
                        flag = True
                    prefetch.append(data)

                if not flag:
                    data["path"] = "."
                    prefetch.append(data)

        if prefetch:
            logger.info(f"Adding pre-fetch params: {prefetch}")

        return prefetch

    async def _start_build(self, metadata: ImageMetadata, build_repo: BuildRepo, building_arches: list[str],
                           output_image: str, additional_tags: list[str], dest_dir: Optional[Path] = None):
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        if not build_repo.commit_hash:
            raise IOError(f"The build branch {build_repo.branch} doesn't have any commits in the build repository {build_repo.https_url}")

        git_branch = build_repo.branch or build_repo.commit_hash
        git_url = build_repo.https_url
        git_commit = build_repo.commit_hash

        # Ensure the Application resource exists
        app_name = self.get_application_name(self._config.group_name)
        logger.info(f"Using application: {app_name}")
        await self._konflux_client.ensure_application(name=app_name, display_name=app_name)

        # Ensure the component resource exists
        component_name = self.get_component_name(app_name, metadata.distgit_key)
        default_revision = f"art-{self._config.group_name}-assembly-test-dgk-{metadata.distgit_key}"
        logger.info(f"Using component: {component_name}")
        await self._konflux_client.ensure_component(
            name=component_name,
            application=app_name,
            component_name=component_name,
            image_repo=output_image.split(":")[0],
            source_url=git_url,
            revision=default_revision,
        )

        # Start a PipelineRun
        # Check if hermetic builds need to be enabled
        hermetic = (metadata.get_konflux_network_mode() == "hermetic")

        prefetch = self._prefetch(metadata=metadata, dest_dir=dest_dir)

        # Check if SAST tasks needs to be enabled
        # Image config value overrides group config value
        group_config_sast_task = metadata.runtime.group_config.get("konflux", {}).get("sast", {}).get("enabled", False)
        image_config_sast_task = metadata.config.get("konflux", {}).get("sast", {}).get("enabled", Missing)
        sast = image_config_sast_task if image_config_sast_task is not Missing else group_config_sast_task

        pipelinerun = await self._konflux_client.start_pipeline_run_for_image_build(
            metadata=metadata,
            generate_name=f"{component_name}-",
            namespace=self._config.namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=git_url,
            commit_sha=git_commit,
            target_branch=git_branch,
            output_image=output_image,
            building_arches=building_arches,
            additional_tags=additional_tags,
            skip_checks=self._config.skip_checks,
            hermetic=hermetic,
            vm_override=metadata.config.get("konflux", {}).get("vm_override"),
            pipelinerun_template_url=self._config.plr_template,
            prefetch=prefetch,
            sast=sast
        )

        logger.info(f"Created PipelineRun: {self._konflux_client.build_pipeline_url(pipelinerun)}")
        return pipelinerun

    async def get_installed_packages(self, image_pullspec, arches, logger) -> list:
        """
        Example sbom: https://gist.github.com/thegreyd/6718f4e4dae9253310c03b5d492fab68
        :return: Returns list of installed rpms for an image pullspec, assumes that the sbom exists in registry
        """
        async def _get_for_arch(arch, logger):
            go_arch = go_arch_for_brew_arch(arch)

            cmd = [
                "cosign",
                "download",
                "sbom",
                image_pullspec,
                "--platform", f"linux/{go_arch}",
            ]

            if self._config.image_repo == constants.KONFLUX_DEFAULT_IMAGE_REPO:
                cmd += [
                    "--registry-username", f"{os.environ['KONFLUX_ART_IMAGES_USERNAME']}",
                    "--registry-password", f"{os.environ['KONFLUX_ART_IMAGES_PASSWORD']}",
                ]

            rc, stdout, _ = await exectools.cmd_gather_async(cmd)

            if rc != 0:
                raise ChildProcessError("cosign command failed to download SBOM")

            sbom_contents = json.loads(stdout)
            source_rpms = set()
            for x in sbom_contents["components"]:
                # konflux generates sbom in cyclonedx schema: https://cyclonedx.org
                # sbom uses purl or package-url convention https://github.com/package-url/purl-spec
                # example: pkg:rpm/rhel/coreutils-single@8.32-35.el9?arch=x86_64&upstream=coreutils-8.32-35.el9.src.rpm&distro=rhel-9.4
                # https://github.com/package-url/packageurl-python does not support purl schemes other than "pkg"
                # so filter them out
                if x.get("purl", '').startswith("pkg:"):
                    try:
                        purl = PackageURL.from_string(x["purl"])
                        # right now, we only care about rpms
                        if purl.type == "rpm":
                            # get the source rpm
                            source_rpm = purl.qualifiers.get("upstream", None)
                            if source_rpm:
                                source_rpms.add(source_rpm.rstrip(".src.rpm"))
                    except Exception as e:
                        logger.warning(f"Failed to parse purl: {x['purl']} {e}")
                        continue
            return source_rpms

        results = await asyncio.gather(*(_get_for_arch(arch, logger) for arch in arches))
        for arch, result in zip(arches, results):
            if not result:
                raise ChildProcessError(f"Could not get rpms from SBOM for arch {arch}")
        installed_packages = set()
        for srpms in results:
            installed_packages.update(srpms)
        return sorted(installed_packages)

    async def update_konflux_db(self, metadata, build_repo, pipelinerun, outcome, building_arches, pod_list: Optional[List[Dict]] = None) -> Optional[KonfluxBuildRecord]:
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        if not metadata.runtime.konflux_db:
            logger.warning('Konflux DB connection is not initialized, not writing build record to the Konflux DB.')
            return None

        rebase_repo_url = build_repo.https_url
        rebase_commit = build_repo.commit_hash

        df_path = build_repo.local_dir.joinpath("Dockerfile")
        df = DockerfileParser(str(df_path))

        source_repo = df.labels['io.openshift.build.source-location']
        commitish = df.labels['io.openshift.build.commit.id']

        version = df.labels['version']
        release = df.labels['release']
        nvr = "-".join([metadata.distgit_key, version, release])

        pipelinerun_name = pipelinerun['metadata']['name']
        # Pipelinerun names will eventually repeat over time, so also gather the pipelinerun uid
        pipelinerun_uid = pipelinerun['metadata']['uid']
        build_pipeline_url = self._konflux_client.build_pipeline_url(pipelinerun)

        build_record_params = {
            'name': metadata.distgit_key,
            'version': version,
            'release': release,
            'el_target': f'el{isolate_el_version_in_release(release)}',
            'arches': building_arches,
            'embargoed': is_release_embargoed(release, 'konflux'),
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
            'build_id': f'{pipelinerun_name}-{pipelinerun_uid}',
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

            installed_packages = await self.get_installed_packages(image_pullspec, building_arches, logger)

            build_record_params.update({
                'image_pullspec': f"{image_pullspec.split(':')[0]}@{image_digest}",
                'installed_packages': installed_packages,
                'image_tag': image_pullspec.split(':')[-1],
            })

        if pipelinerun.status:
            if pipelinerun.status.startTime:
                build_record_params['start_time'] = datetime.strptime(pipelinerun.status.startTime, '%Y-%m-%dT%H:%M:%SZ')
            if pipelinerun.status.completionTime:  # Pending will not have a completion time
                build_record_params['end_time'] = datetime.strptime(pipelinerun.status.completionTime, '%Y-%m-%dT%H:%M:%SZ')

        build_record = KonfluxBuildRecord(**build_record_params)
        metadata.runtime.konflux_db.add_build(build_record)
        logger.info(f'Konflux build info stored successfully with status {outcome}')

        try:
            taskrun_db_client = bigquery.BigQueryClient()
            taskrun_db_client.bind(artlib_constants.TASKRUN_TABLE_ID)
            if pod_list:
                rows = []
                for pod in pod_list:

                    def extract_datetime_from_pod_time(pod_time: Optional[str]):
                        if not pod_time:
                            return None
                        return datetime.fromisoformat(pod_time.rstrip("Z"))

                    pod_metadata = pod.get('metadata', {})
                    max_finished_time = None
                    all_containers_finished = True
                    exit_code_sum = 0
                    creation_timestamp = extract_datetime_from_pod_time(pod_metadata['creationTimestamp'])
                    pod_name = pod_metadata['name']
                    task_name = pod_metadata['labels']['tekton.dev/pipelineTask']  # e.g. "build-images"
                    task_run = pod_metadata['labels']['tekton.dev/taskRun']  # e.g. "ose-4-19-pf-status-relay-operator-h5pj5-build-images-1"
                    task_run_uid = pod_metadata['labels']['tekton.dev/taskRunUID']  # e.g. "58b6cdea-e72e-4c45-aac9-7010aa67fa28"
                    pipeline_run_name = pod_metadata['labels']['tekton.dev/pipelineRun']  # e.g. "ose-4-19-pf-status-relay-operator-h5pj5"
                    pipeline_run_uid = pod_metadata['labels']['tekton.dev/pipelineRunUID']  # e.g. "d288cbd8-de96-49d9-8294-b65246eff937"
                    pod_status = pod.get('status')

                    pod_start_time = None
                    if pod_status.get('startTime'):
                        pod_start_time = extract_datetime_from_pod_time(pod_status.get('startTime'))

                    pod_scheduled_condition = artlib_util.KubeCondition.find_condition(pod, 'PodScheduled')
                    pod_initialized_condition = artlib_util.KubeCondition.find_condition(pod, 'Initialized')

                    containers_info = []

                    def add_container_status(container_status: Dict, is_init_container: bool):
                        nonlocal max_finished_time, all_containers_finished, exit_code_sum
                        container_state = 'pending'  # when there is no pod state, assume pending
                        container_started_time = None
                        container_finished_time = None
                        state_reason = None
                        exit_code = None

                        container_state_obj = container_status.get('state', {})
                        if container_state_obj.get('waiting'):
                            container_state = 'waiting'
                            state_reason = container_state_obj.get('waiting', {}).get('reason')

                        if container_state_obj.get('running'):
                            container_state = 'running'
                            container_started_time = extract_datetime_from_pod_time(container_state_obj.get('running').get('startedAt'))
                            state_reason = container_state_obj.get('running', {}).get('reason')

                        if container_state_obj.get('terminated'):
                            terminated = container_state_obj.get('terminated', {})
                            container_state = 'terminated'
                            exit_code = terminated.get('exitCode')
                            exit_code_sum += exit_code
                            container_started_time = extract_datetime_from_pod_time(terminated.get('startedAt'))
                            container_finished_time = extract_datetime_from_pod_time(terminated.get('finishedAt'))
                            state_reason = terminated.get('reason')
                            if max_finished_time is None or container_finished_time > max_finished_time:
                                max_finished_time = container_finished_time
                        else:
                            all_containers_finished = False

                        container_info = {
                            'name': container_status.get('name'),
                            'is_init': is_init_container,
                            'image': container_status.get('image'),
                            'started_time': container_started_time.isoformat() if container_started_time else None,
                            'finished_time': container_finished_time.isoformat() if container_finished_time else None,
                            'state': container_state,
                            'exit_code': exit_code,
                            'reason': state_reason,
                            # log_output is not actually part of the Pod schema.
                            # The caller should capture logs they are interested
                            # in and stuff it into the associated containerStatus
                            # entry.
                            'log_output': container_status.get('log_output')
                        }
                        containers_info.append(container_info)

                    for container_status in pod_status['initContainerStatuses']:
                        add_container_status(container_status, is_init_container=True)

                    for container_status in pod_status['containerStatuses']:
                        add_container_status(container_status, is_init_container=False)

                    taskrun_record = {
                        'creation_time': creation_timestamp.isoformat(),
                        'task': task_name,
                        'task_run': task_run,
                        'task_run_uid': task_run_uid,
                        'pipeline_run': pipeline_run_name,
                        'pod_phase': pod_status.get('phase', 'Unknown'),
                        'scheduled_time': pod_scheduled_condition.last_transition_time.isoformat() if pod_scheduled_condition and pod_scheduled_condition.is_status_true() else None,
                        'initialized_time': pod_initialized_condition.last_transition_time.isoformat() if pod_initialized_condition and pod_initialized_condition.is_status_true() else None,
                        'start_time': pod_start_time.isoformat() if pod_start_time else None,
                        'containers': containers_info,
                        'capture_time': datetime.now(tz=timezone.utc).isoformat(),
                        'max_finished_time': max_finished_time.isoformat() if max_finished_time and all_containers_finished else None,
                        'build_id': build_record.build_id,
                        'pod_name': pod_name,
                        'pipeline_run_uid': pipeline_run_uid,
                        'success': all_containers_finished and exit_code_sum == 0,
                    }
                    rows.append(taskrun_record)

                try:
                    taskrun_db_client.client.insert_rows_json(f'{artlib_constants.GOOGLE_CLOUD_PROJECT}.{artlib_constants.DATASET_ID}.{artlib_constants.TASKRUN_TABLE_ID}', rows)
                except:
                    logger.warning('Error inserting taskrun information in bigquery')
                    pprint.pprint(rows)
                    raise
        except:
            logger.warning('Error recording taskrun information in bigquery')
            traceback.print_exc()

        return build_record
