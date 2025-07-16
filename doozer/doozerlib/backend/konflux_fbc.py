import asyncio
import itertools
import json
import logging
import os
import shutil
import ssl
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone
from io import StringIO
from os import PathLike
from pathlib import Path
from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import httpx
import jinja2
import truststore
from artcommonlib import util as artlib_util
from artcommonlib.konflux.konflux_build_record import (
    Engine,
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb
from async_lru import alru_cache
from dockerfile_parse import DockerfileParser
from doozerlib import constants, opm, util
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import KonfluxClient
from doozerlib.image import ImageMetadata
from doozerlib.record_logger import RecordLogger
from kubernetes.dynamic import resource
from tenacity import retry, stop_after_attempt, wait_fixed

LOGGER = logging.getLogger(__name__)
yaml = opm.yaml
ssl_ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

PRODUCTION_INDEX_PULLSPEC_FORMAT = "registry.redhat.io/redhat/redhat-operator-index:v{major}.{minor}"
BASE_IMAGE_RHEL9_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry-rhel9:v{major}.{minor}"
BASE_IMAGE_RHEL8_PULLSPEC_FORMAT = "registry.redhat.io/openshift4/ose-operator-registry:v{major}.{minor}"


class KonfluxFbcImporter:
    def __init__(
        self,
        base_dir: Path,
        group: str,
        assembly: str,
        ocp_version: Tuple[int, int],
        upcycle: bool,
        push: bool,
        commit_message: Optional[str] = None,
        fbc_repo: str = constants.ART_FBC_GIT_REPO,
        auth: Optional[opm.OpmRegistryAuth] = None,
        logger: logging.Logger | None = None,
    ):
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self.ocp_version = ocp_version
        self.upcycle = upcycle
        self.push = push
        self.commit_message = commit_message
        self.fbc_git_repo = fbc_repo
        self.auth = auth
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)

    async def import_from_index_image(self, metadata: ImageMetadata, index_image: str | None = None):
        """Create a file based catalog (FBC) by importing from an existing index image.

        :param metadata: The metadata of the operator image.
        :param index_image: The index image to import from. If not provided, a default index image is used.
        """
        # bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{metadata.distgit_key}]")
        repo_dir = self.base_dir.joinpath(metadata.distgit_key)
        if not index_image:
            index_image = PRODUCTION_INDEX_PULLSPEC_FORMAT.format(major=self.ocp_version[0], minor=self.ocp_version[1])
            logger.info(
                "Using default index image %s for OCP %s.%s", index_image, self.ocp_version[0], self.ocp_version[1]
            )

        # Clone the FBC repo
        build_branch = "art-{group}-assembly-{assembly_name}-fbc-{distgit_key}".format_map(
            {
                "group": self.group,
                "assembly_name": self.assembly,
                "distgit_key": metadata.distgit_key,
            }
        )
        logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_git_repo, build_branch, repo_dir)
        build_repo = BuildRepo(url=self.fbc_git_repo, branch=build_branch, local_dir=repo_dir, logger=logger)
        await build_repo.ensure_source(upcycle=self.upcycle, strict=False)

        # Update the FBC directory
        await self._update_dir(metadata, build_repo, index_image, logger)

        # Validate the catalog
        logger.info("Validating the catalog")
        await opm.validate(repo_dir.joinpath("catalog"))

        # Commit and push the changes
        message = self.commit_message or f"Imported catalog from index image {index_image} for {metadata.distgit_key}"
        await build_repo.commit(message, allow_empty=True)
        if self.push:
            await build_repo.push()
            logger.info("Changes pushed to remote repository %s branch %s", self.fbc_git_repo, build_branch)
        else:
            logger.info("Not pushing changes to remote repository")

    async def merge_fragments(self, fragments: Iterable[str]):
        logger = self._logger.getChild(f"merge_fragments")
        repo_dir = self.base_dir.joinpath("__merged")

        branch = f"art-{self.group}-fbc-stage"
        build_repo = BuildRepo(url=self.fbc_git_repo, branch=branch, local_dir=repo_dir, logger=logger)
        logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_git_repo, branch, repo_dir)
        await build_repo.ensure_source(upcycle=self.upcycle, strict=False)

        # Render the FBC fragments
        all_blobs = await asyncio.gather(
            *(self._render_index_image(fragment, migrate_level="none") for fragment in fragments),
        )
        catalog_blobs = list(itertools.chain.from_iterable(all_blobs))

        # Write the rendered blobs to a file
        catalog_dir = build_repo.local_dir.joinpath("catalog")
        catalog_dir.mkdir(parents=True, exist_ok=True)
        catalog_file_path = catalog_dir.joinpath("catalog.yaml")
        LOGGER.info("Writing catalog blobs to %s", catalog_file_path)
        with catalog_file_path.open('w') as f:
            yaml.dump_all(catalog_blobs, f)

        # Generate Dockerfile
        df_path = build_repo.local_dir.joinpath("catalog.Dockerfile")
        LOGGER.info("Generating Dockerfile %s", df_path)
        if df_path.exists():
            LOGGER.info("Removing existing Dockerfile %s", df_path)
            df_path.unlink()
        base_image_format = (
            BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if self.ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        )
        base_image = base_image_format.format(major=self.ocp_version[0], minor=self.ocp_version[1])
        await opm.generate_dockerfile(build_repo.local_dir, "catalog", base_image=base_image, builder_image=base_image)

        # Modify Dockerfile
        # dockerfile_path = repo_dir.joinpath("catalog.Dockerfile")
        # dfp = DockerfileParser(str(dockerfile_path))

        # Merge ImageDigestMirrorSets
        ims_path = build_repo.local_dir.joinpath(".tekton/images-mirror-set.yaml")
        ims_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Generating ImageDigestMirrorSet %s", ims_path)

        http_client = httpx.AsyncClient(verify=ssl_ctx)

        async def _get_image_digest_mirror_set(fragment: str):
            fragment_info = await util.oc_image_info_for_arch_async__caching(
                fragment,
                registry_config=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
            )
            commit = fragment_info["config"]["config"]["Labels"]["vcs-ref"]
            url = f"https://raw.githubusercontent.com/openshift-priv/art-fbc/{urllib.parse.quote(commit)}/.tekton/images-mirror-set.yaml"
            response = await http_client.get(url)
            response.raise_for_status()
            idms_data = yaml.load(StringIO(response.text))
            return idms_data

        idms_list = await asyncio.gather(
            *(_get_image_digest_mirror_set(fragment) for fragment in fragments),
        )
        mirrors_by_source = defaultdict(set)
        for ims in idms_list:
            for mirror in ims.get("spec", {}).get("imageDigestMirrors", []):
                source = mirror.get("source")
                mirrors = mirror.get("mirrors", [])
                if source and mirrors:
                    mirrors_by_source[source].update(mirrors)
        target_idms = {
            "apiVersion": "config.openshift.io/v1",
            "kind": "ImageDigestMirrorSet",
            "metadata": {
                "name": "art-stage-mirror-set",
                "namespace": "openshift-marketplace",
            },
            "spec": {
                "imageDigestMirrors": [
                    {
                        "source": source,
                        "mirrors": sorted(mirrors),
                    }
                    for source, mirrors in sorted(mirrors_by_source.items())
                ],
            },
        }
        with ims_path.open('w') as ims_file:
            yaml.dump(target_idms, ims_file)
        LOGGER.info("ImageDigestMirrorSet written to %s", ims_path)

        # Validate the catalog
        logger.info("Validating the catalog")
        await opm.validate(repo_dir.joinpath("catalog"))

        # Commit and push the changes
        message = self.commit_message or f"Merge FBC fragments\n\n{', '.join(fragments)}"
        await build_repo.commit(message, allow_empty=True)
        if self.push:
            await build_repo.push()
            logger.info("Changes pushed to remote repository %s branch %s", self.fbc_git_repo, branch)
        else:
            logger.info("Not pushing changes to remote repository")

    async def _update_dir(
        self, metadata: ImageMetadata, build_repo: BuildRepo, index_image: str, logger: logging.Logger
    ):
        """Update the FBC directory with the given operator image metadata and index image."""
        repo_dir = build_repo.local_dir
        # Get package name of the operator
        package_name = await self._get_package_name(metadata)

        # Render the catalog from the index image
        migrate_level = "none"
        if self.ocp_version >= (4, 17):
            migrate_level = "bundle-object-to-csv-metadata"
        catalog_blobs = await self._get_catalog_blobs_from_index_image(
            index_image, package_name, migrate_level=migrate_level
        )

        # Write catalog_blobs to catalog/<package>/catalog.json
        catalog_dir = repo_dir.joinpath("catalog", package_name)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        catalog_file_path = catalog_dir.joinpath("catalog.yaml")
        logger.info("Writing catalog blobs to %s", catalog_file_path)
        with catalog_file_path.open('w') as f:
            yaml.dump_all(catalog_blobs, f)

        # Generate Dockerfile
        df_path = repo_dir.joinpath("catalog.Dockerfile")
        logger.info("Generating Dockerfile %s", df_path)
        if df_path.exists():
            logger.info("Removing existing Dockerfile %s", df_path)
            df_path.unlink()
        base_image_format = (
            BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if self.ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        )
        base_image = base_image_format.format(major=self.ocp_version[0], minor=self.ocp_version[1])
        await opm.generate_dockerfile(repo_dir, "catalog", base_image=base_image, builder_image=base_image)

        logger.info("FBC directory updated")

    @alru_cache
    async def _render_index_image(self, index_image: str, migrate_level: str = "none") -> List[Dict]:
        blobs = await retry(reraise=True, stop=stop_after_attempt(3), wait=wait_fixed(5))(opm.render)(
            index_image,
            auth=self.auth,
            migrate_level=migrate_level,
        )
        return blobs

    def _filter_catalog_blobs(self, blobs: List[Dict], allowed_package_names: Set[str]):
        """Filter catalog blobs by package names.

        :param blobs: List of catalog blobs.
        :param allowed_package_names: Set of allowed package names.
        :return: Dict of filtered catalog blobs.
        """
        filtered: Dict[str, List[Dict[str, Any]]] = {}  # key is package name, value is blobs
        for blob in blobs:
            schema = blob["schema"]
            package_name = None
            match schema:
                case "olm.package":
                    package_name = blob["name"]
                case "olm.channel" | "olm.bundle" | "olm.deprecations":
                    package_name = blob["package"]
            if not package_name:
                raise IOError(f"Couldn't determine package name for unknown schema: {schema}")
            if package_name not in allowed_package_names:
                continue  # filtered out; skipping
            if package_name not in filtered:
                filtered[package_name] = []
            filtered[package_name].append(blob)
        return filtered

    async def _get_catalog_blobs_from_index_image(
        self, index_image: str, package_name: str | None, migrate_level: str = "none"
    ):
        blobs = await self._render_index_image(index_image, migrate_level=migrate_level)
        filtered_blobs = self._filter_catalog_blobs(blobs, {package_name})
        if package_name not in filtered_blobs:
            raise IOError(f"Package {package_name} not found in index image")
        return filtered_blobs[package_name]

    async def _get_package_name(self, metadata: ImageMetadata) -> str:
        """Get OLM package name of the given OLM operator
        This function loads OLM package name for the operator image
        by resolving sources and loading package.yaml files.

        :param metadata: operator image metadata.
        :return: OLM package name.
        """
        source_resolver = metadata.runtime.source_resolver
        assert source_resolver, "Source resolver is not initialized; Doozer bug?"
        source = await asyncio.to_thread(source_resolver.resolve_source, metadata)
        source_dir = source_resolver.get_source_dir(source, metadata)
        csv_config = metadata.config.get('update-csv')
        if not csv_config:
            raise ValueError(f"update-csv config not found for {metadata.distgit_key}")
        source_path = source_dir.joinpath(csv_config['manifests-dir'])
        package_yaml_file = next(source_path.glob('**/*package.yaml'))
        with package_yaml_file.open() as f:
            package_yaml = yaml.load(f)
            package_name = package_yaml.get('packageName')
            if not package_name:
                raise IOError(f"Package name not found in {package_yaml_file}")
            return str(package_name)


class KonfluxFbcFragmentMerger:
    def __init__(
        self,
        dry_run: bool,
        working_dir: Path,
        group: str,
        konflux_namespace: str,
        konflux_kubeconfig: str | None,
        konflux_context: str | None = None,
        upcycle: bool = False,
        commit_message: str | None = None,
        fbc_git_repo: str = constants.ART_FBC_GIT_REPO,
        fbc_branch: str | None = None,
        registry_auth: Optional[str] = None,
        skip_checks: bool = False,
        plr_template: Optional[str] = None,
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the KonfluxFbcFragmentMerger.
        """
        self.dry_run = dry_run
        self.working_dir = Path(working_dir)
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_namespace = konflux_namespace
        self.konflux_context = konflux_context
        # self.target_index = target_index
        # self.fragments = fragments
        # if not self.fragments:
        #     raise ValueError("At least one fragment must be provided.")
        self.group = group
        self.upcycle = upcycle
        self.commit_message = commit_message
        self.fbc_git_repo = fbc_git_repo
        self.fbc_branch = fbc_branch or f"art-{self.group}-fbc-stage"
        self.registry_auth = registry_auth
        self.skip_checks = skip_checks
        self.plr_template = plr_template or constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)
        # Default OCP version, can be changed based on the target index
        self.ocp_version: tuple[int, int] = (4, 18)

    async def run(self, fragments: Collection[str], target_index: str):
        if not fragments:
            raise ValueError("At least one fragment must be provided.")
        if not target_index:
            raise ValueError("Target index must be provided.")
        logger = self._logger
        # Clone the art-fbc repository into clone_dir
        # clone_dir = self.working_dir / "art-fbc"
        # if not clone_dir.exists():
        #     clone_dir.mkdir(parents=True)
        # # await self.clone_fbc_repo(clone_dir)
        # print(f"Cloned FBC fragments into {clone_dir}")
        repo_dir = self.working_dir
        build_repo = BuildRepo(url=self.fbc_git_repo, branch=self.fbc_branch, local_dir=repo_dir, logger=logger)
        logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_git_repo, self.fbc_branch, repo_dir)
        await build_repo.ensure_source(upcycle=self.upcycle, strict=False)

        # # Render the FBC fragments
        # opm_auth = opm.OpmRegistryAuth(path=self.registry_auth) if self.registry_auth else None
        # all_blobs = await asyncio.gather(
        #     *(opm.render(fragment, output_format="yaml", migrate_level="none", auth=opm_auth) for fragment in fragments)
        # )
        # catalog_blobs = list(itertools.chain.from_iterable(all_blobs))

        # # Write the rendered blobs to a file
        # catalog_dir = repo_dir.joinpath("catalog")
        # catalog_dir.mkdir(parents=True, exist_ok=True)
        # catalog_file_path = catalog_dir.joinpath("catalog.yaml")
        # LOGGER.info("Writing catalog blobs to %s", catalog_file_path)
        # with catalog_file_path.open('w') as f:
        #     yaml.dump_all(catalog_blobs, f)

        # # Generate Dockerfile
        # df_path = repo_dir.joinpath("catalog.Dockerfile")
        # LOGGER.info("Generating Dockerfile %s", df_path)
        # if df_path.exists():
        #     LOGGER.info("Removing existing Dockerfile %s", df_path)
        #     df_path.unlink()
        # base_image_format = (
        #     BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if self.ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
        # )
        # base_image = base_image_format.format(major=self.ocp_version[0], minor=self.ocp_version[1])
        # await opm.generate_dockerfile(repo_dir, "catalog", base_image=base_image, builder_image=base_image)

        # Modify Dockerfile
        # dockerfile_path = repo_dir.joinpath("catalog.Dockerfile")
        # dfp = DockerfileParser(str(dockerfile_path))

        # Merge ImageDigestMirrorSets
        idms_path = repo_dir.joinpath(".tekton/images-mirror-set.yaml")
        idms_path.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Generating ImageDigestMirrorSet %s", idms_path)

        async with httpx.AsyncClient(verify=ssl_ctx) as http_client:

            async def _get_fragment_idms(fragment: str) -> list[dict]:
                """
                Fetch the ImageDigestMirrorSet for a given fragment.
                """
                # _, out, _ = await self.cmd_assert(
                #     "oc",
                #     "image",
                #     "info",
                #     fragment,
                #     "--output=json",
                #     "--filter-by-os=linux/amd64",
                #     stdout=asyncio.subprocess.PIPE,
                #     stderr=asyncio.subprocess.PIPE,
                # )
                # fragment_info = json.loads(out)
                fragment_info = await util.oc_image_info_for_arch_async__caching(
                    fragment,
                    registry_config=self.registry_auth,
                )
                commit = fragment_info["config"]["config"]["Labels"]["vcs-ref"]
                url = (
                    f"https://raw.githubusercontent.com/openshift-priv/art-fbc/{commit}/.tekton/images-mirror-set.yaml"
                )

                response = await http_client.get(url)
                response.raise_for_status()
                idms_data = yaml.load(StringIO(response.text))
                return idms_data

            idms_list = await asyncio.gather(*(_get_fragment_idms(fragment) for fragment in fragments))

        mirrors_by_source = defaultdict(set)
        for idms in idms_list:
            for mirror in idms.get("spec", {}).get("imageDigestMirrors", []):
                source = mirror.get("source")
                mirrors = mirror.get("mirrors", [])
                if source and mirrors:
                    mirrors_by_source[source].update(mirrors)
        target_idms = {
            "apiVersion": "config.openshift.io/v1",
            "kind": "ImageDigestMirrorSet",
            "metadata": {
                "name": "art-stage-mirror-set",
                "namespace": "openshift-marketplace",
            },
            "spec": {
                "imageDigestMirrors": [
                    {
                        "source": source,
                        "mirrors": sorted(mirrors),
                    }
                    for source, mirrors in sorted(mirrors_by_source.items())
                ],
            },
        }
        with idms_path.open('w') as ims_file:
            yaml.dump(target_idms, ims_file)
        LOGGER.info("ImageDigestMirrorSet written to %s", idms_path)

        # Commit the changes to the git repository
        print("Committing changes to the git repository...")
        # # await self.cmd_assert("git", "add", ".", cwd=repo_dir)
        # # await self.cmd_assert("git", "commit", "-m", "Merge FBC fragments", cwd=clone_dir)
        # # _, out, _ = await self.cmd_assert("git", "rev-parse", "HEAD", cwd=clone_dir, stdout=asyncio.subprocess.PIPE)
        # # commit_sha = out.strip()
        # message = self.commit_message or f"Merge FBC fragments\n\n{', '.join(fragments)}"
        # await build_repo.commit(message, allow_empty=True)
        commit_sha = build_repo.commit_hash
        assert commit_sha, "Commit SHA should not be empty; Doozer bug?"
        if not self.dry_run:
            print("Pushing changes to the remote repository...")
            await build_repo.push()
            print(f"Changes pushed to remote repository {self.fbc_git_repo} branch {self.fbc_branch}")
        else:
            print("Dry run enabled, not pushing changes.")

        # Create application
        # API_VERSION = "appstudio.redhat.com/v1alpha1"
        # # KIND_SNAPSHOT = "Snapshot"
        # KIND_COMPONENT = "Component"
        # KIND_APPLICATION = "Application"
        app_name = f"fbc-openshift-{self.ocp_version[0]}-{self.ocp_version[1]}-stage"
        # app = {
        #     "apiVersion": API_VERSION,
        #     "kind": KIND_APPLICATION,
        #     "metadata": {
        #         "name": app_name,
        #         "namespace": self.konflux_namespace,
        #     },
        #     "spec": {
        #         "displayName": app_name,
        #         "appModelRepository": {"url": ""},
        #         "gitOpsRepository": {"url": ""},
        #     },
        # }
        # await self.cmd_assert("oc", "apply", "-f", "-", input=json.dumps(app))
        konflux_client = KonfluxClient.from_kubeconfig(
            config_file=self.konflux_kubeconfig,
            default_namespace=self.konflux_namespace,
            context=self.konflux_context,
            dry_run=self.dry_run,
            logger=logger,
        )
        await konflux_client.ensure_application(app_name, app_name)
        print(f"Created application {app_name}")

        # Create component
        comp_name = app_name + "-merged"
        # branch = f"art-openshift-{self.ocp_version[0]}-{self.ocp_version[1]}-fbc-stage"
        # comp = {
        #     "apiVersion": API_VERSION,
        #     "kind": KIND_COMPONENT,
        #     "metadata": {
        #         "name": comp_name,
        #         "namespace": self.konflux_namespace,
        #         "annotations": {
        #             "build.appstudio.openshift.io/pipeline": '{"name":"docker-build-multi-platform-oci-ta","bundle":"latest"}',
        #             # will raise PRs to upstream repos (openshift-priv) if this is not set to false
        #             "build.appstudio.openshift.io/status": '{"pac":{"state":"disabled"}}',
        #             # "build.appstudio.openshift.io/request": "configure-pac",
        #             # https://gitlab.cee.redhat.com/konflux/docs/users/-/blob/main/topics/mintmaker/user.md#offboarding-a-repository
        #             "mintmaker.appstudio.redhat.com/disabled": "true",
        #         },
        #     },
        #     "spec": {
        #         "application": app_name,
        #         "componentName": comp_name,
        #         "source": {
        #             "git": {
        #                 "url": "https://github.com/openshift-priv/art-fbc.git",
        #                 "revision": branch,
        #             },
        #         },
        #     },
        # }
        # await self.cmd_assert("oc", "apply", "-f", "-", input=json.dumps(comp))
        target_index_split = target_index.rsplit(":", maxsplit=1)
        output_image_repo = target_index_split[0]
        output_image_tag = (
            target_index_split[1]
            if len(target_index_split) > 1
            else f"art-openshift-{self.ocp_version[0]}-{self.ocp_version[1]}-fbc-stage"
        )
        await konflux_client.ensure_component(
            application=app_name,
            name=comp_name,
            component_name=comp_name,
            source_url=build_repo.https_url,
            revision=build_repo.branch,
            image_repo=output_image_repo,
        )
        print(f"Created component {comp_name} in application {app_name}")

        # Create pipelinerun
        # KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL = "https://github.com/openshift-priv/art-konflux-template/raw/refs/heads/main/.tekton/art-fbc-konflux-template-push.yaml"
        # plr_template_url = KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL
        # http_client = httpx.AsyncClient(verify=ctx)
        # response = await http_client.get(plr_template_url, follow_redirects=True)
        # response.raise_for_status()
        # template_text = response.text
        # plr_template = jinja2.Template(template_text, autoescape=True)
        # rendered = plr_template.render(
        #     {
        #         "source_url": "https://github.com/openshift-priv/art-fbc",
        #         "revision": commit_sha,
        #         "target_branch": branch,
        #         "git_auth_secret": "pipelines-as-code-secret",
        #     }
        # )
        # https_url = "https://github.com/openshift-priv/art-fbc"
        # plr = yaml.load(rendered)
        # # Those fields in the template are specific to an image. They need to be removed.
        # del plr["metadata"]["name"]
        # del plr["metadata"]["annotations"]["pipelinesascode.tekton.dev/on-cel-expression"]
        # # Override the generated name with the provided one
        # generate_name = f"fbc-openshift-{self.ocp_version[0]}-{self.ocp_version[1]}-stage-"
        # namespace = "ocp-art-tenant"
        # if generate_name:
        #     plr["metadata"]["generateName"] = generate_name
        # if namespace:
        #     plr["metadata"]["namespace"] = namespace
        # else:
        #     del plr["metadata"]["namespace"]
        # # Set the application and component names
        # plr["metadata"]["annotations"]["build.appstudio.openshift.io/repo"] = f"{https_url}?rev={commit_sha}"
        # plr["metadata"]["labels"]["appstudio.openshift.io/application"] = app_name
        # plr["metadata"]["labels"]["appstudio.openshift.io/component"] = comp_name

        # def _modify_param(params: List, name: str, value: Union[str, bool, list[str]]):
        #     """Modify a parameter in the params list. If the parameter does not exist, it is added.

        #     :param params: The list of parameters.
        #     :param name: The name of the parameter.
        #     :param value: The value of the parameter.
        #     """
        #     if isinstance(value, bool):
        #         # boolean value should be converted to string
        #         value = "true" if value else "false"
        #     for param in params:
        #         if param["name"] == name:
        #             param["value"] = value
        #             return
        #     params.append({"name": name, "value": value})

        # # Modify the parameters in the Pipelinerun
        # SUPPORTED_ARCHES = {
        #     "x86_64": ["linux/x86_64"],
        #     "s390x": ["linux/s390x"],
        #     "ppc64le": ["linux/ppc64le"],
        #     "aarch64": ["linux/arm64"],
        # }
        # build_platforms = [platform for platforms in SUPPORTED_ARCHES.values() if platforms for platform in platforms]
        # KONFLUX_DEFAULT_FBC_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"
        # output_image = (
        #     f"{KONFLUX_DEFAULT_FBC_REPO}:stage-{self.ocp_version[0]}-{self.ocp_version[1]}-fbc-{commit_sha[:7]}"
        # )
        # params = plr["spec"]["params"]
        # _modify_param(params, "dockerfile", "catalog.Dockerfile")
        # _modify_param(params, "build-platforms", build_platforms)
        # _modify_param(params, "output-image", output_image)

        # rc, out, err = await self.cmd_assert(
        #     "oc",
        #     "create",
        #     "-f",
        #     "-",
        #     "-o",
        #     "json",
        #     input=json.dumps(plr),
        #     stdout=asyncio.subprocess.PIPE,
        #     stderr=asyncio.subprocess.PIPE,
        # )
        # created_plr = json.loads(out)
        arches = list(KonfluxClient.SUPPORTED_ARCHES.keys())
        created_plr = await konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{comp_name}-",
            namespace=self.konflux_namespace,
            application_name=app_name,
            component_name=comp_name,
            git_url=build_repo.https_url,
            commit_sha=commit_sha,
            target_branch=self.fbc_branch or commit_sha,
            output_image=f"{output_image_repo}:{output_image_tag}",
            additional_tags=[],
            vm_override={},
            building_arches=arches,  # FBC should be built for all supported arches
            hermetic=True,  # FBC should be built in hermetic mode
            dockerfile="catalog.Dockerfile",
            skip_checks=self.skip_checks,
            pipelinerun_template_url=self.plr_template,
        )
        plr_name = created_plr["metadata"]["name"]
        # plr_url = f"https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com/ns/{self.konflux_namespace}/applications/{app_name}/pipelineruns/{plr_name}"
        plr_url = konflux_client.build_pipeline_url(created_plr)
        print(f"Created Pipelinerun {plr_name}: {plr_url}")

        # Wait for the Pipelinerun to complete
        # print("Waiting for the Pipelinerun to complete...")
        # while True:
        #     rc, out, err = await self.cmd_assert(
        #         "oc",
        #         "get",
        #         "pipelinerun",
        #         plr_name,
        #         "-o",
        #         "json",
        #         stdout=asyncio.subprocess.PIPE,
        #         stderr=asyncio.subprocess.PIPE,
        #     )
        #     plr_info = json.loads(out)
        #     conditions = plr_info.get("status", {}).get("conditions", [])
        #     succeeded_cond = next((c for c in conditions if c.get("type") == "Succeeded"), None)
        #     if not succeeded_cond:
        #         print("Pipelinerun is still pending")
        #         await asyncio.sleep(10)
        #         continue
        #     status = succeeded_cond.get("status")
        #     if status == "True":
        #         print("Pipelinerun completed successfully.")
        #         break
        #     elif status == "False":
        #         print(f"Pipelinerun failed: {succeeded_cond.get('message', 'No message provided')}")
        #         raise ChildProcessError(
        #             f"Pipelinerun {plr_name} failed: {succeeded_cond.get('message', 'No message provided')}"
        #         )
        #     else:
        #         print("Pipelinerun is still running...")
        #         await asyncio.sleep(10)
        plr, _ = await konflux_client.wait_for_pipelinerun(plr_name, self.konflux_namespace)
        succeeded_condition = artlib_util.KubeCondition.find_condition(plr, 'Succeeded')
        outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)
        logger.info(
            "Pipelinerun %s completed with outcome: %s",
            plr_name,
            outcome,
        )
        if outcome is not KonfluxBuildOutcome.SUCCESS:
            raise IOError(
                f"Pipelinerun {plr_name} failed with outcome: {outcome.name} - {succeeded_condition.message or 'No message provided'}"
            )

    async def cmd_assert(self, program, *args, input=None, check=True, **kwargs):
        """
        Run a command asynchronously and return the result.
        To capture stdout and stderr, pass `stdout=asyncio.subprocess.PIPE` and/or `stderr=asyncio.subprocess.PIPE` in kwargs.
        If `check` is True and the command fails, raise a ChildProcessError with the error message.

        Args:
            program (str): The command to run.
            check (bool): Whether to raise an error if the command fails.
            input (str | bytes | None): Input to send to the command's stdin.
            *args: Additional arguments to pass to the command.
            **kwargs: Additional keyword arguments for subprocess creation.
        Returns:
            tuple: A tuple containing the return code, stdout, and stderr.
        """
        print(f"Running command: {program} {' '.join(args)}")
        stdout = kwargs.pop('stdout', None)
        stderr = kwargs.pop('stderr', None)
        stdin = kwargs.pop('stdin', None)
        if input is not None:
            stdin = asyncio.subprocess.PIPE
        env = kwargs.pop('env', None)
        if env is None:
            env = os.environ.copy()
        proc = await asyncio.create_subprocess_exec(
            program,
            *args,
            **kwargs,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            env=env,
        )
        stdout, stderr = await proc.communicate(input=input.encode() if isinstance(input, str) else input)
        out = stdout.decode() if stdout is not None else ""
        err = stderr.decode() if stderr is not None else ""
        if check and proc.returncode != 0:
            raise ChildProcessError(f"Command failed with exit code {proc.returncode}: {err}")
        return proc.returncode, out, err

    async def clone_fbc_repo(self, clone_dir: Path):
        # This is a placeholder for the actual cloning logic
        # In a real implementation, you would clone the repository here
        print(f"Cloning FBC fragments into {clone_dir}...")
        shutil.rmtree(clone_dir, ignore_errors=True)  # Clean up if it exists
        git_url = "git@github.com:openshift-priv/art-fbc.git"
        branch = f"art-openshift-{self.ocp_version[0]}-{self.ocp_version[1]}-fbc-stage"
        # _, _, err = await self.cmd_assert("git", "clone", "--depth", "1", "--branch", branch, git_url, str(clone_dir), stderr=asyncio.subprocess.PIPE)
        # run git init
        await self.cmd_assert("git", "init", str(clone_dir))
        # run git remote add origin
        await self.cmd_assert("git", "remote", "add", "origin", git_url, cwd=clone_dir)
        # run git fetch. if the branch does not exist, create it
        try:
            await self.cmd_assert("git", "fetch", "origin", branch, cwd=clone_dir, stderr=asyncio.subprocess.PIPE)
        except ChildProcessError as e:
            if "couldn't find remote ref" in str(e):
                print(f"Branch {branch} not found, creating it.")
                await self.cmd_assert("git", "checkout", "--orphan", branch, cwd=clone_dir)
            else:
                raise e
        else:
            await self.cmd_assert("git", "checkout", branch, cwd=clone_dir)

    async def opm_render(
        self, *input: str, output_format: str = "yaml", migrate_level: str = "none", auth_file: str | None = None
    ) -> list[dict]:
        """
            Run `opm render` on the given input and return the parsed file-based catalog blobs.

            :param input: The catalog images, file-based catalog directories, bundle images, and sqlite
        database files to render.
            :param output_format: The output format to use when rendering the catalog. One of "yaml" or "json".
            :param migrate_level: The migration level to use when rendering the catalog. One of "none" or "bundle-object-to-csv-metadata".
            :param auth: The registry authentication information to use.
            :return: The parsed file-based catalog blobs.
        """
        if not input:
            raise ValueError("input must not be empty.")
        if output_format not in ["yaml", "json"]:
            raise ValueError(f"Invalid output format: {output_format}")
        # LOGGER.debug(f"Rendering FBC for {', '.join(input)}")
        _, out, _ = await self.cmd_assert(
            "opm",
            "render",
            "--migrate-level",
            migrate_level,
            "-o",
            output_format,
            "--",
            *input,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        blobs = yaml.load_all(StringIO(out))
        return list(blobs)

    async def generate_dockerfile(self, dest_dir: Path, dc_dir_name: str, base_image: str, builder_image: str):
        """
        Generate a Dockerfile for for a file-based catalog.

        :param dest_dir: The directory to generate the Dockerfile in.
        :param dc_dir_name: The directory containing the file-based catalog.
        :param base_image: Image base to use to build catalog.
        """
        options = [
            "--builder-image",
            builder_image,
            "--base-image",
            base_image,
        ]
        LOGGER.debug(f"Generating FBC Dockerfile in {dest_dir}")
        await self.cmd_assert("opm", "generate", "dockerfile", *options, "--", dc_dir_name, cwd=dest_dir)


class KonfluxFbcRebaser:
    def __init__(
        self,
        base_dir: str | PathLike,
        group: str,
        assembly: str,
        version: str,
        release: str,
        commit_message: str,
        push: bool,
        fbc_repo: str,
        upcycle: bool,
        record_logger: Optional[RecordLogger] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.group = group
        self.assembly = assembly
        self.version = version
        self.release = release
        self.commit_message = commit_message
        self.push = push
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.upcycle = upcycle
        self._record_logger = record_logger
        self._logger = logger or LOGGER.getChild(self.__class__.__name__)

    @staticmethod
    def get_fbc_name(image_name: str):
        return f"{image_name}-fbc"

    async def rebase(self, metadata: ImageMetadata, bundle_build: KonfluxBundleBuildRecord, version: str, release: str):
        bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{bundle_short_name}]")
        repo_dir = self.base_dir.joinpath(metadata.distgit_key)

        name = self.get_fbc_name(metadata.distgit_key)
        nvr = f"{name}-{version}-{release}"
        record = {
            # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            'status': -1,
            "name": metadata.distgit_key,
            "message": "Unknown failure",
            "fbc_nvr": nvr,
            "bundle_nvrs": ','.join(
                [str(bundle_build.nvr)]
            ),  # Currently we only support rebasing for one bundle at a time
        }

        try:
            # Clone the FBC repo
            fbc_build_branch = "art-{group}-assembly-{assembly_name}-fbc-{distgit_key}".format_map(
                {
                    "group": self.group,
                    "assembly_name": self.assembly,
                    "distgit_key": metadata.distgit_key,
                }
            )
            logger.info("Cloning FBC repo %s branch %s into %s", self.fbc_repo, fbc_build_branch, repo_dir)
            build_repo = BuildRepo(url=self.fbc_repo, branch=fbc_build_branch, local_dir=repo_dir, logger=logger)
            await build_repo.ensure_source(
                upcycle=self.upcycle,
                strict=False,
            )

            # Update the FBC repo
            await self._rebase_dir(metadata, build_repo, bundle_build, version, release, logger)

            # Validate the updated catalog
            logger.info("Validating the updated catalog")
            await opm.validate(build_repo.local_dir.joinpath("catalog"))

            # Commit and push the changes
            addtional_message = (
                f"\n\n---\noperator: {bundle_build.operator_nvr}\noperands: {','.join(bundle_build.operand_nvrs)}"
            )
            await build_repo.commit(self.commit_message + addtional_message, allow_empty=True)
            if self.push:
                await build_repo.push()
            else:
                logger.info("Not pushing changes to remote repository")

            record["message"] = "Success"
            record["status"] = 0
            logger.info("rebase complete")
        except Exception as error:
            record['message'] = str(error)
            raise
        finally:
            if self._record_logger:
                self._record_logger.add_record("rebase_fbc_konflux", **record)

    async def _get_referenced_images(self, konflux_db: KonfluxDb, bundle_build: KonfluxBundleBuildRecord):
        assert bundle_build.operator_nvr, "operator_nvr is empty; doozer bug?"
        nvrs = {bundle_build.operator_nvr}
        if bundle_build.operand_nvrs:
            nvrs |= set(bundle_build.operand_nvrs)
        assert konflux_db.record_cls is KonfluxBuildRecord, "konflux_db is not bound to KonfluxBuildRecord. Doozer bug?"
        ref_builds = await konflux_db.get_build_records_by_nvrs(list(nvrs))
        return ref_builds

    async def _rebase_dir(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        bundle_build: KonfluxBundleBuildRecord,
        version: str,
        release: str,
        logger: logging.Logger,
    ):
        logger.info("Rebasing dir %s", build_repo.local_dir)

        # This will raise an ValueError if the bundle delivery repo name is not set in the metadata config.
        delivery_repo_name = metadata.get_olm_bundle_delivery_repo_name()

        # Fetch bundle image info and blob
        logger.info("Fetching OLM bundle image %s from %s", bundle_build.nvr, bundle_build.image_pullspec)
        olm_bundle_image_info = await self._fetch_olm_bundle_image_info(bundle_build)
        labels = olm_bundle_image_info["config"]["config"]["Labels"]
        image_name = labels.get("name")
        if not image_name:
            raise IOError("Image name not found in bundle image")
        channel_names = labels.get("operators.operatorframework.io.bundle.channels.v1")
        if not channel_names:
            raise IOError("Channel name not found in bundle image")
        channel_names = channel_names.split(",")
        default_channel_name = labels.get("operators.operatorframework.io.bundle.channel.default.v1")
        olm_bundle_name, olm_package, olm_bundle_blob = await self._fetch_olm_bundle_blob(bundle_build)
        if olm_package != labels.get("operators.operatorframework.io.bundle.package.v1"):
            raise IOError(
                f"Package name mismatch: {olm_package} != {labels.get('operators.operatorframework.io.bundle.package.v1')}"
            )
        olm_csv_metadata = next(
            (entry for entry in olm_bundle_blob["properties"] if entry["type"] == "olm.csv.metadata"), None
        )
        if not olm_csv_metadata:
            raise IOError(f"CSV metadata not found in bundle {olm_bundle_name}")
        olm_skip_range = olm_csv_metadata["value"]["annotations"].get("olm.skipRange", None)

        # Load referenced images
        konflux_db: KonfluxDb = metadata.runtime.konflux_db
        konflux_db.bind(KonfluxBuildRecord)
        ref_builds = await self._get_referenced_images(konflux_db, bundle_build)
        ref_builds.append(bundle_build)  # Include the bundle build itself
        ref_pullspecs = {
            b.image_pullspec.replace(constants.REGISTRY_PROXY_BASE_URL, constants.BREW_REGISTRY_BASE_URL)
            for b in ref_builds
        }

        # Load current catalog
        catalog_dir = build_repo.local_dir.joinpath("catalog", olm_package)
        catalog_file_path = catalog_dir.joinpath("catalog.yaml")
        if catalog_file_path.is_file():
            logger.info("Catalog file %s already exists, loading it", catalog_file_path)
            with catalog_file_path.open() as f:
                catalog_blobs = list(yaml.load_all(f))
        else:
            logger.info("Catalog file %s does not exist, bootstrap a new one", catalog_file_path)
            catalog_blobs = self._bootstrap_catalog(olm_package, default_channel_name or "stable")

        categorized_catalog_blobs = self._catagorize_catalog_blobs(catalog_blobs)
        if olm_package not in categorized_catalog_blobs:
            raise IOError(f"Package {olm_package} not found in catalog. The FBC repo is not properly initialized.")
        if len(categorized_catalog_blobs) > 1:
            logger.warning(
                f"The catalog file {catalog_file_path} has multiple packages: {','.join(categorized_catalog_blobs.keys())}"
            )

        # Update the catalog
        def _update_channel(channel: Dict):
            # Update "skips" in the channel
            # FIXME: We try to mimic how `skips` field is updated by the old ET centric process.
            # We should verify if this is correct.
            skips = None
            bundle_with_skips = next(
                (it for it in channel['entries'] if it.get('skips')), None
            )  # Find which bundle has the skips field
            if bundle_with_skips:
                # Then we move the skips field to the new bundle
                # and add the bundle name of bundle_with_skips to the skips field
                skips = set(bundle_with_skips.pop('skips'))
                skips = (skips | {bundle_with_skips['name']}) - {olm_bundle_name}

            # Add the current bundle to the specified channel in the catalog
            entry = next((entry for entry in channel['entries'] if entry['name'] == olm_bundle_name), None)
            if not entry:
                logger.info("Adding bundle %s to channel %s", olm_bundle_name, channel['name'])
                entry = {"name": olm_bundle_name}
                channel['entries'].append(entry)
            else:
                logger.warning("Bundle %s already exists in channel %s. Replacing...", olm_bundle_name, channel['name'])
                entry.clear()
                entry["name"] = olm_bundle_name
            if olm_skip_range:
                entry["skipRange"] = olm_skip_range
            if skips:
                entry["skips"] = sorted(skips)

        for channel_name in channel_names:
            logger.info("Updating channel %s", channel_name)
            channel = categorized_catalog_blobs[olm_package]["olm.channel"].get(channel_name, None)
            if not channel:
                raise IOError(
                    f"Channel {channel_name} not found in package {olm_package}. The FBC repo is not properly initialized."
                )
            _update_channel(channel)

        # Set default channel
        if default_channel_name:
            package_blob = categorized_catalog_blobs[olm_package]["olm.package"][olm_package]
            if package_blob.get("defaultChannel") != default_channel_name:
                logger.info("Setting default channel to %s", default_channel_name)
                package_blob["defaultChannel"] = default_channel_name

        # Replace pullspecs to use the prod registry
        digest = bundle_build.image_pullspec.split('@', 1)[-1]
        bundle_prod_pullspec = f"{constants.DELIVERY_IMAGE_REGISTRY}/{delivery_repo_name}@{digest}"
        olm_bundle_blob["image"] = bundle_prod_pullspec
        related_images = olm_bundle_blob.get("relatedImages", [])
        if related_images:
            target_entry = next((it for it in related_images if it['name'] == ""), None)
            if target_entry:
                logger.info("Replacing image reference %s with %s", target_entry['image'], bundle_prod_pullspec)
                target_entry['image'] = bundle_prod_pullspec

        # Add the new bundle blob to the catalog
        if olm_bundle_name not in categorized_catalog_blobs[olm_package].setdefault("olm.bundle", {}):
            logger.info("Adding bundle %s to package %s", olm_bundle_name, olm_package)
            categorized_catalog_blobs[olm_package]["olm.bundle"][olm_bundle_name] = olm_bundle_blob
            catalog_blobs.append(olm_bundle_blob)
        else:  # Update the existing bundle blob
            logger.warning("Bundle %s already exists in package %s. Replacing...", olm_bundle_name, olm_package)
            target_entry = categorized_catalog_blobs[olm_package]["olm.bundle"][olm_bundle_name]
            target_entry.clear()
            target_entry.update(olm_bundle_blob)

        # Write the updated catalog back to the file
        logger.info("Writing updated catalog to %s", catalog_file_path)
        catalog_dir.mkdir(parents=True, exist_ok=True)
        with catalog_file_path.open("w") as f:
            yaml.dump_all(catalog_blobs, f)

        # Add ImageDigestMirrorSet .tekton/images-mirror-set.yaml to the build repo to make Enterprise Contract happy
        image_digest_mirror_set = self._generate_image_digest_mirror_set(
            categorized_catalog_blobs[olm_package]["olm.bundle"].values(),
            ref_pullspecs,
        )
        dot_tekton_dir = build_repo.local_dir.joinpath(".tekton")
        images_mirror_set_file_path = dot_tekton_dir.joinpath("images-mirror-set.yaml")
        if not image_digest_mirror_set:
            logger.info("No related images found, deleting existing images-mirror-set.yaml")
            images_mirror_set_file_path.unlink(missing_ok=True)
        else:
            logger.info("Adding ImageDigestMirrorSet to build repo")
            dot_tekton_dir.mkdir(exist_ok=True)
            with images_mirror_set_file_path.open('w') as f:
                yaml.dump(image_digest_mirror_set, f)

        # Update Dockerfile
        dockerfile_path = build_repo.local_dir.joinpath("catalog.Dockerfile")
        if not dockerfile_path.is_file():
            logger.info("Dockerfile %s does not exist, creating a new one", dockerfile_path)
            group_config = metadata.runtime.group_config
            ocp_version = int(group_config.vars.MAJOR), int(group_config.vars.MINOR)
            base_image_format = (
                BASE_IMAGE_RHEL9_PULLSPEC_FORMAT if ocp_version >= (4, 15) else BASE_IMAGE_RHEL8_PULLSPEC_FORMAT
            )
            base_image = base_image_format.format(major=ocp_version[0], minor=ocp_version[1])
            await opm.generate_dockerfile(
                build_repo.local_dir, "catalog", base_image=base_image, builder_image=base_image
            )

        logger.info("Updating Dockerfile %s", dockerfile_path)
        dfp = DockerfileParser(str(dockerfile_path))
        metadata_envs: Dict[str, str] = {
            '__doozer_group': self.group,
            '__doozer_key': metadata.distgit_key,
            '__doozer_version': version,
            '__doozer_release': release,
            '__doozer_bundle_nvrs': ','.join([str(bundle_build.nvr)]),
        }
        for key, value in metadata_envs.items():
            if dfp.envs.get(key) != value:
                logger.info("Setting %s=%s", key, value)
                dfp.envs[key] = value

        dfp.labels['io.openshift.build.source-location'] = bundle_build.source_repo
        dfp.labels['io.openshift.build.commit.id'] = bundle_build.commitish

        # The following label is used internally by ART's shipment pipeline
        name = self.get_fbc_name(metadata.distgit_key)
        dfp.labels['com.redhat.art.nvr'] = f'{name}-{version}-{release}'

    def _bootstrap_catalog(self, package_name: str, default_channel: str = 'stable') -> List[Dict[str, Any]]:
        """Bootstrap a new catalog for the given package name.
        :param package_name: The name of the package to bootstrap.
        :return: A dictionary representing the catalog.
        """
        # Following https://github.com/konflux-ci/olm-operator-konflux-sample/blob/main/v4.13/catalog-template.json
        package_blob = {
            "defaultChannel": default_channel,
            "icon": {
                "base64data": "PHN2ZyBpZD0iZjc0ZTM5ZDEtODA2Yy00M2E0LTgyZGQtZjM3ZjM1NWQ4YWYzIiBkYXRhLW5hbWU9Ikljb24iIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDM2IDM2Ij4KICA8ZGVmcz4KICAgIDxzdHlsZT4KICAgICAgLmE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCB7CiAgICAgICAgZmlsbDogI2UwMDsKICAgICAgfQogICAgPC9zdHlsZT4KICA8L2RlZnM+CiAgPGc+CiAgICA8cGF0aCBjbGFzcz0iYTQxYzUyMzQtYTE0YS00ZjNkLTkxNjAtNDQ3MmI3NmQwMDQwIiBkPSJNMjUsMTcuMzhIMjMuMjNhNS4yNyw1LjI3LDAsMCwwLTEuMDktMi42NGwxLjI1LTEuMjVhLjYyLjYyLDAsMSwwLS44OC0uODhsLTEuMjUsMS4yNWE1LjI3LDUuMjcsMCwwLDAtMi42NC0xLjA5VjExYS42Mi42MiwwLDEsMC0xLjI0LDB2MS43N2E1LjI3LDUuMjcsMCwwLDAtMi42NCwxLjA5bC0xLjI1LTEuMjVhLjYyLjYyLDAsMCwwLS44OC44OGwxLjI1LDEuMjVhNS4yNyw1LjI3LDAsMCwwLTEuMDksMi42NEgxMWEuNjIuNjIsMCwwLDAsMCwxLjI0aDEuNzdhNS4yNyw1LjI3LDAsMCwwLDEuMDksMi42NGwtMS4yNSwxLjI1YS42MS42MSwwLDAsMCwwLC44OC42My42MywwLDAsMCwuODgsMGwxLjI1LTEuMjVhNS4yNyw1LjI3LDAsMCwwLDIuNjQsMS4wOVYyNWEuNjIuNjIsMCwwLDAsMS4yNCwwVjIzLjIzYTUuMjcsNS4yNywwLDAsMCwyLjY0LTEuMDlsMS4yNSwxLjI1YS42My42MywwLDAsMCwuODgsMCwuNjEuNjEsMCwwLDAsMC0uODhsLTEuMjUtMS4yNWE1LjI3LDUuMjcsMCwwLDAsMS4wOS0yLjY0SDI1YS42Mi42MiwwLDAsMCwwLTEuMjRabS03LDQuNjhBNC4wNiw0LjA2LDAsMSwxLDIyLjA2LDE4LDQuMDYsNC4wNiwwLDAsMSwxOCwyMi4wNloiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik0yNy45LDI4LjUyYS42Mi42MiwwLDAsMS0uNDQtLjE4LjYxLjYxLDAsMCwxLDAtLjg4LDEzLjQyLDEzLjQyLDAsMCwwLDIuNjMtMTUuMTkuNjEuNjEsMCwwLDEsLjMtLjgzLjYyLjYyLDAsMCwxLC44My4yOSwxNC42NywxNC42NywwLDAsMS0yLjg4LDE2LjYxQS42Mi42MiwwLDAsMSwyNy45LDI4LjUyWiIvPgogICAgPHBhdGggY2xhc3M9ImE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCIgZD0iTTI3LjksOC43M2EuNjMuNjMsMCwwLDEtLjQ0LS4xOUExMy40LDEzLjQsMCwwLDAsMTIuMjcsNS45MWEuNjEuNjEsMCwwLDEtLjgzLS4zLjYyLjYyLDAsMCwxLC4yOS0uODNBMTQuNjcsMTQuNjcsMCwwLDEsMjguMzQsNy42NmEuNjMuNjMsMCwwLDEtLjQ0LDEuMDdaIi8+CiAgICA8cGF0aCBjbGFzcz0iYTQxYzUyMzQtYTE0YS00ZjNkLTkxNjAtNDQ3MmI3NmQwMDQwIiBkPSJNNS4zNSwyNC42MmEuNjMuNjMsMCwwLDEtLjU3LS4zNUExNC42NywxNC42NywwLDAsMSw3LjY2LDcuNjZhLjYyLjYyLDAsMCwxLC44OC44OEExMy40MiwxMy40MiwwLDAsMCw1LjkxLDIzLjczYS42MS42MSwwLDAsMS0uMy44M0EuNDguNDgsMCwwLDEsNS4zNSwyNC42MloiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik0xOCwzMi42MkExNC42NCwxNC42NCwwLDAsMSw3LjY2LDI4LjM0YS42My42MywwLDAsMSwwLS44OC42MS42MSwwLDAsMSwuODgsMCwxMy40MiwxMy40MiwwLDAsMCwxNS4xOSwyLjYzLjYxLjYxLDAsMCwxLC44My4zLjYyLjYyLDAsMCwxLS4yOS44M0ExNC42NywxNC42NywwLDAsMSwxOCwzMi42MloiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik0zMCwyOS42MkgyN2EuNjIuNjIsMCwwLDEtLjYyLS42MlYyNmEuNjIuNjIsMCwwLDEsMS4yNCwwdjIuMzhIMzBhLjYyLjYyLDAsMCwxLDAsMS4yNFoiLz4KICAgIDxwYXRoIGNsYXNzPSJhNDFjNTIzNC1hMTRhLTRmM2QtOTE2MC00NDcyYjc2ZDAwNDAiIGQ9Ik03LDMwLjYyQS42Mi42MiwwLDAsMSw2LjM4LDMwVjI3QS42Mi42MiwwLDAsMSw3LDI2LjM4aDNhLjYyLjYyLDAsMCwxLDAsMS4yNEg3LjYyVjMwQS42Mi42MiwwLDAsMSw3LDMwLjYyWiIvPgogICAgPHBhdGggY2xhc3M9ImE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCIgZD0iTTI5LDkuNjJIMjZhLjYyLjYyLDAsMCwxLDAtMS4yNGgyLjM4VjZhLjYyLjYyLDAsMCwxLDEuMjQsMFY5QS42Mi42MiwwLDAsMSwyOSw5LjYyWiIvPgogICAgPHBhdGggY2xhc3M9ImE0MWM1MjM0LWExNGEtNGYzZC05MTYwLTQ0NzJiNzZkMDA0MCIgZD0iTTksMTAuNjJBLjYyLjYyLDAsMCwxLDguMzgsMTBWNy42Mkg2QS42Mi42MiwwLDAsMSw2LDYuMzhIOUEuNjIuNjIsMCwwLDEsOS42Miw3djNBLjYyLjYyLDAsMCwxLDksMTAuNjJaIi8+CiAgPC9nPgo8L3N2Zz4K",
                "mediatype": "image/svg+xml",
            },
            "name": package_name,
            "schema": "olm.package",
        }
        channel_blob = {
            "entries": [],
            "name": default_channel,
            "package": package_name,
            "schema": "olm.channel",
        }
        return [
            package_blob,
            channel_blob,
        ]

    def _generate_image_digest_mirror_set(self, olm_bundle_blobs: Iterable[Dict], ref_pullspecs: Iterable[str]):
        mirrored_repos = {
            p_split[1]: p_split[0]
            for bundle_blob in olm_bundle_blobs
            for related_image in bundle_blob.get("relatedImages", [])
            if (p_split := related_image["image"].split('@', 1))
        }
        source_repos = {p_split[1]: p_split[0] for pullspec in ref_pullspecs if (p_split := pullspec.split('@', 1))}
        if not mirrored_repos:
            return None
        image_digest_mirror_set = {
            "apiVersion": "config.openshift.io/v1",
            "kind": "ImageDigestMirrorSet",
            "metadata": {
                "name": "art-images-mirror-set",
                "namespace": "openshift-marketplace",
            },
            "spec": {
                "imageDigestMirrors": [
                    {
                        "source": dest,
                        "mirrors": [
                            mirrored_repos[sha],
                        ],
                    }
                    for sha, dest in source_repos.items()
                ],
            },
        }
        return image_digest_mirror_set

    async def _fetch_olm_bundle_image_info(self, bundle_build: KonfluxBundleBuildRecord):
        return await util.oc_image_info_for_arch_async__caching(
            bundle_build.image_pullspec,
            registry_config=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
        )

    async def _fetch_olm_bundle_blob(self, bundle_build: KonfluxBundleBuildRecord):
        """Fetch the olm.bundle blob for the given bundle build.

        :param bundle_build: The bundle build record.
        :return: A tuple of (bundle name, package name, bundle blob).
        """
        registry_auth = opm.OpmRegistryAuth(
            path=os.environ.get("KONFLUX_ART_IMAGES_AUTH_FILE"),
        )
        rendered_blobs = await opm.render(bundle_build.image_pullspec, migrate=True, auth=registry_auth)
        if not isinstance(rendered_blobs, list) or len(rendered_blobs) != 1:
            raise IOError(f"Expected exactly one rendered blob, but got {len(rendered_blobs)}")
        olm_bundle_blob = rendered_blobs[0]
        if olm_bundle_blob.get('schema') != 'olm.bundle':
            raise IOError(f"Bundle blob has invalid schema: {olm_bundle_blob.get('schema')}")
        olm_package = olm_bundle_blob["package"]
        if not olm_package:
            raise IOError("Package not found in bundle blob")
        assert isinstance(olm_package, str), f"Expected package name to be a string, but got {type(olm_package)}"
        olm_bundle_name = olm_bundle_blob["name"]
        if not olm_bundle_name:
            raise IOError("Name not found in bundle blob")
        assert isinstance(olm_bundle_name, str), f"Expected bundle name to be a string, but got {type(olm_bundle_name)}"
        return olm_bundle_name, olm_package, olm_bundle_blob

    def _catagorize_catalog_blobs(self, blobs: List[Dict]):
        """Given a list of catalog blobs, categorize them by schema and package name.

        :param blobs: A list of catalog blobs.
        :return: a dictionary of the form {package_name: {schema: {blob_name: blob}}}
        """
        categorized_blobs = {}
        for blob in blobs:
            schema = blob["schema"]
            package_name = None
            match schema:
                case "olm.package":
                    package_name = blob["name"]
                case "olm.channel" | "olm.bundle" | "olm.deprecations":
                    package_name = blob["package"]
                case _:  # Unknown schema
                    raise IOError(f"Found unsupported schema: {schema}")
            if not package_name:
                raise IOError(f"Couldn't determine package name for unknown schema: {schema}")
            categorized_blobs.setdefault(package_name, {}).setdefault(schema, {})[blob["name"]] = blob
        return categorized_blobs


class KonfluxFbcBuildError(Exception):
    def __init__(self, message: str, pipelinerun_name: str, pipelinerun: Optional[resource.ResourceInstance]) -> None:
        super().__init__(message)
        self.pipelinerun_name = pipelinerun_name
        self.pipelinerun = pipelinerun


class KonfluxFbcBuilder:
    def __init__(
        self,
        base_dir: Path,
        group: str,
        assembly: str,
        db: KonfluxDb,
        fbc_repo: str,
        konflux_namespace: str,
        konflux_kubeconfig: Optional[str] = None,
        konflux_context: Optional[str] = None,
        image_repo: str = constants.KONFLUX_DEFAULT_IMAGE_REPO,
        skip_checks: bool = False,
        pipelinerun_template_url: str = constants.KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL,
        dry_run: bool = False,
        record_logger: Optional[RecordLogger] = None,
        logger: logging.Logger = LOGGER,
    ):
        self.base_dir = base_dir
        self.group = group
        self.assembly = assembly
        self._db = db
        self.fbc_repo = fbc_repo or constants.ART_FBC_GIT_REPO
        self.konflux_namespace = konflux_namespace
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.image_repo = image_repo
        self.skip_checks = skip_checks
        self.pipelinerun_template_url = pipelinerun_template_url
        self.dry_run = dry_run
        self._record_logger = record_logger
        self._logger = logger.getChild(self.__class__.__name__)
        self._konflux_client = KonfluxClient.from_kubeconfig(
            konflux_namespace, konflux_kubeconfig, konflux_context, dry_run=self.dry_run
        )

    @staticmethod
    def get_application_name(group_name: str, image_name: str):
        # Note: for now, we use a different application for each image
        # In future, we might change it to one application per group for all images
        return f"fbc-{group_name.replace('openshift-', 'ocp-')}-{image_name}".replace(".", "-").replace("_", "-")

    @staticmethod
    def get_component_name(group_name: str, image_name: str):
        # Note: since application name is per image, we use the same name for the application and the component
        return KonfluxFbcBuilder.get_application_name(group_name, image_name)

    async def build(self, metadata: ImageMetadata):
        bundle_short_name = metadata.get_olm_bundle_short_name()
        logger = self._logger.getChild(f"[{bundle_short_name}]")
        logger.info("Building FBC for %s", metadata.distgit_key)

        record = {
            # Status defaults to failure until explicitly set by success. This handles raised exceptions.
            'status': -1,
            "name": metadata.distgit_key,
            "message": "Unknown failure",
            "task_id": "n/a",
            "task_url": "n/a",
            "fbc_nvr": "n/a",
            "bundle_nvrs": "n/a",
        }

        try:
            # Clone or load the FBC repository
            repo_dir = self.base_dir.joinpath(metadata.distgit_key)
            build_repo = None
            if repo_dir.exists():
                logger.info("Loading existing FBC repository...")
                build_repo = await BuildRepo.from_local_dir(repo_dir, logger)
                logger.info("FBC repository loaded from %s", repo_dir)
            else:
                build_branch = "art-{group}-assembly-{assembly_name}-fbc-{distgit_key}".format_map(
                    {
                        "group": self.group,
                        "assembly_name": self.assembly,
                        "distgit_key": metadata.distgit_key,
                    }
                )
                logger.info("Cloning bundle repository %s branch %s into %s", self.fbc_repo, build_branch, repo_dir)
                build_repo = BuildRepo(
                    url=self.fbc_repo,
                    branch=build_branch,
                    local_dir=repo_dir,
                    logger=logger,
                )
                await build_repo.ensure_source(strict=True)
                logger.info("FBC repository cloned to %s", repo_dir)

            # Parse catalog.Dockerfile
            dfp = DockerfileParser(str(repo_dir.joinpath("catalog.Dockerfile")))
            version = dfp.envs.get("__doozer_version")
            release = dfp.envs.get("__doozer_release")
            if not version or not release:
                raise ValueError("Version and release not found in the catalog.Dockerfile. Did you rebase?")
            logger.info("Version: %s, Release: %s", version, release)
            bundle_nvrs = dfp.envs.get("__doozer_bundle_nvrs")
            if bundle_nvrs:
                record["bundle_nvrs"] = bundle_nvrs

            # Start FBC build
            logger.info("Starting FBC build...")
            retries = 3
            name = KonfluxFbcRebaser.get_fbc_name(metadata.distgit_key)
            nvr = f"{name}-{version}-{release}"
            record["fbc_nvr"] = nvr
            output_image = f"{self.image_repo}:{nvr}"

            # FBC needs to be built for all supported arches.
            arches = list(KonfluxClient.SUPPORTED_ARCHES.keys())
            for attempt in range(1, retries + 1):
                logger.info("Build attempt %d/%d", attempt, retries)
                pipelinerun, url = await self._start_build(
                    metadata=metadata,
                    build_repo=build_repo,
                    output_image=output_image,
                    arches=arches,
                    logger=logger,
                )
                pipelinerun_name = pipelinerun.metadata.name
                record["task_id"] = pipelinerun_name
                record["task_url"] = url
                if not self.dry_run:
                    await self._update_konflux_db(
                        metadata, build_repo, pipelinerun, KonfluxBuildOutcome.PENDING, arches, logger=logger
                    )
                else:
                    logger.info("Dry run: Would have inserted build record in Konflux DB")

                logger.info("Waiting for PipelineRun %s to complete...", pipelinerun_name)
                pipelinerun, _ = await self._konflux_client.wait_for_pipelinerun(
                    pipelinerun_name, namespace=self.konflux_namespace
                )
                logger.info("PipelineRun %s completed", pipelinerun_name)

                succeeded_condition = artlib_util.KubeCondition.find_condition(pipelinerun, 'Succeeded')
                outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)

                if self.dry_run:
                    logger.info("Dry run: Would have inserted build record in Konflux DB")
                else:
                    await self._update_konflux_db(metadata, build_repo, pipelinerun, outcome, arches, logger=logger)

                if outcome is not KonfluxBuildOutcome.SUCCESS:
                    error = KonfluxFbcBuildError(
                        f"Konflux image build for {metadata.distgit_key} failed", pipelinerun_name, pipelinerun
                    )
                else:
                    error = None
                    metadata.build_status = True
                    record["message"] = "Success"
                    record["status"] = 0
                    break
            if error:
                record['message'] = str(error)
                raise error
        finally:
            if self._record_logger:
                self._record_logger.add_record("build_fbc_konflux", **record)
        return pipelinerun_name, pipelinerun

    async def _start_build(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        output_image: str,
        arches: Sequence[str],
        logger: logging.Logger,
    ):
        """Start a build with Konflux."""
        if not build_repo.commit_hash:
            raise IOError("Bundle repository must have a commit to build. Did you rebase?")
        # Ensure the Application resource exists
        # Openshift doesn't allow dots or underscores in any of its fields, so we replace them with dashes
        app_name = self.get_application_name(self.group, metadata.distgit_key)
        logger.info(f"Using Konflux application: {app_name}")
        konflux_client = self._konflux_client
        await konflux_client.ensure_application(name=app_name, display_name=app_name)
        logger.info(f"Konflux application {app_name} created")
        # Ensure the Component resource exists
        component_name = self.get_component_name(self.group, metadata.distgit_key)
        logger.info(f"Creating Konflux component: {component_name}")
        dest_image_repo = output_image.split(":")[0]
        await konflux_client.ensure_component(
            name=component_name,
            application=app_name,
            component_name=component_name,
            image_repo=dest_image_repo,
            source_url=build_repo.https_url,
            revision=build_repo.branch,
        )
        logger.info(f"Konflux component {component_name} created")
        # Create a new pipeline run
        logger.info("Starting Konflux pipeline run...")
        additional_tags = []
        pipelinerun = await konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{component_name}-",
            namespace=self.konflux_namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=build_repo.https_url,
            commit_sha=build_repo.commit_hash,
            target_branch=build_repo.branch or build_repo.commit_hash,
            output_image=output_image,
            vm_override={},
            building_arches=arches,  # FBC should be built for all supported arches
            additional_tags=list(additional_tags),
            skip_checks=self.skip_checks,
            hermetic=True,
            dockerfile="catalog.Dockerfile",
            pipelinerun_template_url=self.pipelinerun_template_url,
        )
        url = konflux_client.build_pipeline_url(pipelinerun)
        logger.info(f"PipelineRun {pipelinerun.metadata.name} created: {url}")
        return pipelinerun, url

    async def _update_konflux_db(
        self,
        metadata: ImageMetadata,
        build_repo: BuildRepo,
        pipelinerun: resource.ResourceInstance,
        outcome: KonfluxBuildOutcome,
        arches: Sequence[str],
        logger: Optional[logging.Logger] = None,
    ):
        logger = logger or self._logger.getChild(f"[{metadata.distgit_key}]")
        db = self._db
        if not db or db.record_cls != KonfluxFbcBuildRecord:
            logger.warning('Konflux DB connection is not initialized, not writing build record to the Konflux DB.')
            return
        try:
            rebase_repo_url = build_repo.https_url
            rebase_commit = build_repo.commit_hash

            df_path = build_repo.local_dir.joinpath("catalog.Dockerfile")
            dfp = DockerfileParser(str(df_path))

            name = self.get_fbc_name(metadata.distgit_key)
            version = dfp.envs.get("__doozer_version")
            release = dfp.envs.get("__doozer_release")
            assert version and release, "Version and release not found in the catalog.Dockerfile. Did you rebase?"

            bundle_nvrs = dfp.envs.get("__doozer_bundle_nvrs", "").split(",")
            source_repo = dfp.labels.get('io.openshift.build.source-location')
            commitish = dfp.labels.get('io.openshift.build.commit.id')

            nvr = "-".join([name, version, release])

            pipelinerun_name = pipelinerun.metadata.name
            build_pipeline_url = KonfluxClient.build_pipeline_url(pipelinerun)

            build_record_params = {
                'name': name,
                'version': version,
                'release': release,
                'start_time': datetime.now(tz=timezone.utc),
                'end_time': None,
                'nvr': nvr,
                'group': metadata.runtime.group,
                'assembly': metadata.runtime.assembly,
                'source_repo': source_repo or "n/a",
                'commitish': commitish or "n/a",
                'rebase_repo_url': rebase_repo_url,
                'rebase_commitish': rebase_commit,
                'engine': Engine.KONFLUX,
                'outcome': str(outcome),
                'art_job_url': os.getenv('BUILD_URL', 'n/a'),
                'build_id': pipelinerun_name,
                'build_pipeline_url': build_pipeline_url,
                'pipeline_commit': 'n/a',  # TODO: populate this
                'bundle_nvrs': bundle_nvrs,
                'arches': arches,
            }

            match outcome:
                case KonfluxBuildOutcome.SUCCESS:
                    # results:
                    # - name: IMAGE_URL
                    #   value: quay.io/openshift-release-dev/ocp-v4.0-art-dev-test:ose-network-metrics-daemon-rhel9-v4.18.0-20241001.151532
                    # - name: IMAGE_DIGEST
                    #   value: sha256:49d65afba393950a93517f09385e1b441d1735e0071678edf6fc0fc1fe501807

                    image_pullspec = next(
                        (r['value'] for r in pipelinerun.status.results or [] if r['name'] == 'IMAGE_URL'), None
                    )
                    image_digest = next(
                        (r['value'] for r in pipelinerun.status.results or [] if r['name'] == 'IMAGE_DIGEST'), None
                    )

                    if not (image_pullspec and image_digest):
                        raise ValueError(
                            f"[{metadata.distgit_key}] Could not find expected results in konflux "
                            f"pipelinerun {pipelinerun_name}"
                        )

                    start_time = pipelinerun.status.startTime
                    end_time = pipelinerun.status.completionTime

                    build_record_params.update(
                        {
                            'image_pullspec': f"{image_pullspec.split(':')[0]}@{image_digest}",
                            'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                                tzinfo=timezone.utc
                            ),
                            'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
                            'image_tag': image_pullspec.split(':')[-1],
                        }
                    )
                case KonfluxBuildOutcome.FAILURE:
                    start_time = pipelinerun.status.startTime
                    end_time = pipelinerun.status.completionTime
                    build_record_params.update(
                        {
                            'start_time': datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ').replace(
                                tzinfo=timezone.utc
                            ),
                            'end_time': datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc),
                        }
                    )

            build_record = KonfluxFbcBuildRecord(**build_record_params)
            db.add_build(build_record)
            logger.info(f'Konflux build info stored successfully with status {outcome}')

        except Exception as err:
            logger.error('Failed writing record to the konflux DB: %s', err)
            raise
