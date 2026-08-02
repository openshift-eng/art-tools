import asyncio
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import click
import yaml
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import ArtifactType, Engine, KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.util import oc_image_info_async, sync_to_quay
from semver import Version

from doozerlib import constants, release_inspector
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.backend.konflux_client import ImageBuildParams, KonfluxClient
from doozerlib.backend.rebaser import KonfluxRebaser
from doozerlib.cli import cli, click_coroutine, pass_runtime, validate_semver_major_minor_patch
from doozerlib.cli.release_gen_payload import (
    assembly_imagestream_base_name_generic,
    default_imagestream_namespace_base_name,
    payload_imagestream_namespace_and_name,
)
from doozerlib.exceptions import DoozerFatalError
from doozerlib.runtime import Runtime
from doozerlib.util import find_manifest_list_sha, get_release_name_for_assembly

LOGGER = logging.getLogger(__name__)

# The name of the manifest in the output of `oc adm release new --to-dir` that describes
# the images referenced by the release payload, including the cluster-version-operator tag.
IMAGE_REFERENCES_FILENAME = "image-references"
RELEASE_MANIFESTS_SUBDIR = "release-manifests"

# All release payload builds, across every group, share a single Konflux Application.
# Each group gets its own Component underneath it (see get_component_name()).
KONFLUX_RELEASE_PAYLOAD_APPLICATION_NAME = "release-payloads"

# Release payloads are release-critical and gate GA/candidate promotion, so they always
# build at Kueue's highest priority -- unlike ordinary image builds, which default to
# "auto" (see doozerlib.util.get_konflux_build_priority). Mirrors FBC_BUILD_PRIORITY /
# BUNDLE_BUILD_PRIORITY in doozerlib.backend.konflux_fbc / konflux_olm_bundler.
RELEASE_PAYLOAD_BUILD_PRIORITY = "1"

# Stable build-record name for every release payload build, regardless of group. Combined
# with group/version/release this gives an NVR of `release-payload-<version>-<release>`,
# which --nvr looks up later to sync an already-built payload without rebuilding it.
RELEASE_PAYLOAD_BUILD_RECORD_NAME = "release-payload"


class ReleasePayloadRebaseAndBuildCli:
    """Implements ART-21775: generate release payload manifests, push them to
    openshift-priv/ocp-release-payloads, and build a Konflux release payload image.

    Unlike a normal doozer image, the release payload has no upstream source repository:
    its "rebase" step runs `oc adm release new --to-dir` to snapshot the manifests already
    populated in the group's imagestream (by build-sync) and writes a minimal Dockerfile
    that layers those manifests onto the cluster-version-operator image. Because Konflux
    builds a single multi-arch image (a manifest list) from one Dockerfile, this command
    is invoked once per group/assembly rather than once per architecture.
    """

    def __init__(
        self,
        runtime: Runtime,
        version: Optional[str],
        release: Optional[str],
        arch: str,
        payload_repo: str,
        image_repo: str,
        konflux_kubeconfig: Optional[str],
        konflux_context: Optional[str],
        konflux_namespace: str,
        from_release: Optional[str] = None,
        commit_message: Optional[str] = None,
        registry_config: Optional[str] = None,
        skip_checks: bool = False,
        skip_tasks: Sequence[str] = (),
        plr_template: str = constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL,
        push: bool = False,
        sync: bool = False,
        release_image_repo: str = constants.RELEASE_PAYLOAD_DEST_REPO,
        dry_run: bool = False,
        nvr: Optional[str] = None,
    ):
        self.runtime = runtime
        self.version = version
        self.release = release
        self.arch = arch
        self.payload_repo = payload_repo or constants.ART_RELEASE_PAYLOAD_GIT_REPO
        self.image_repo = image_repo
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_context = konflux_context
        self.konflux_namespace = konflux_namespace
        self.from_release = from_release
        self.commit_message = commit_message
        self.registry_config = registry_config
        self.skip_checks = skip_checks
        self.skip_tasks = tuple(skip_tasks)
        self.plr_template = plr_template
        self.push = push
        self.sync = sync
        self.release_image_repo = release_image_repo or constants.RELEASE_PAYLOAD_DEST_REPO
        self.dry_run = dry_run
        self.nvr = nvr
        self._logger = LOGGER

    @staticmethod
    def get_application_name() -> str:
        """Konflux Application name shared by release payload builds for every group.

        e.g. `release-payloads`.
        """
        return KONFLUX_RELEASE_PAYLOAD_APPLICATION_NAME

    @staticmethod
    def get_component_name(group: str) -> str:
        """Konflux Component name for a group's release payload builds.

        There is a single Component per group underneath the shared `release-payloads`
        Application (Konflux builds all architectures as one multi-arch manifest list from
        a single PipelineRun per group/assembly), e.g. `release-payload-openshift-4-21`.
        """
        return f"release-payload-{group}".replace(".", "-").replace("_", "-")

    def _resolve_imagestream(self) -> Tuple[str, str]:
        """Derive the (namespace, name) of the build-sync imagestream to source manifests from.

        This mirrors the naming used by build-sync, e.g. for `--group openshift-5.0
        --assembly ec.5` this resolves to `5.0-art-assembly-ec.5` in namespace `ocp`.
        """
        runtime = self.runtime
        version = runtime.get_minor_version()
        base_name = assembly_imagestream_base_name_generic(
            version, runtime.assembly, runtime.assembly_type, build_system='konflux'
        )
        base_namespace = default_imagestream_namespace_base_name()
        namespace, name = payload_imagestream_namespace_and_name(base_namespace, base_name, self.arch, private=False)
        return namespace, name

    async def _generate_manifests(self, manifests_dir: Path) -> str:
        """Run `oc adm release new --to-dir` and return the cluster-version-operator pullspec.

        :param manifests_dir: Directory to write the release manifests into.
        :return: The pullspec for the cluster-version-operator image referenced by the manifests.
        """
        await exectools.to_thread(manifests_dir.mkdir, parents=True, exist_ok=True)
        semver = self.version.lstrip("v")
        release_name = f"{semver}-{self.release}"
        cmd = [
            "oc",
            "adm",
            "release",
            "new",
            f"--name={release_name}",
            f"--to-dir={manifests_dir}",
        ]
        if self.from_release:
            cmd.append(f"--from-release={self.from_release}")
        else:
            namespace, imagestream_name = self._resolve_imagestream()
            cmd.extend(["-n", namespace, f"--from-image-stream={imagestream_name}", "--reference-mode=source"])
        if self.registry_config:
            cmd.append(f"--registry-config={self.registry_config}")

        self._logger.info("Generating release manifests: %s", " ".join(cmd))
        env = os.environ.copy()
        env["GOTRACEBACK"] = "all"
        await exectools.cmd_assert_async(cmd, env=env)

        image_references_path = manifests_dir / IMAGE_REFERENCES_FILENAME
        if not image_references_path.is_file():
            raise DoozerFatalError(
                f"{IMAGE_REFERENCES_FILENAME} manifest not found at {image_references_path} "
                "after running `oc adm release new`"
            )
        content = await exectools.to_thread(image_references_path.read_text)
        image_references = yaml.safe_load(content)
        tags = (image_references or {}).get("spec", {}).get("tags") or []
        cvo_tag = next((tag for tag in tags if tag.get("name") == "cluster-version-operator"), None)
        if not cvo_tag:
            raise DoozerFatalError(
                f"cluster-version-operator tag not found in generated {IMAGE_REFERENCES_FILENAME} manifest"
            )
        cvo_pullspec = cvo_tag.get("from", {}).get("name")
        if not cvo_pullspec:
            raise DoozerFatalError(
                f"cluster-version-operator tag has no pullspec in generated {IMAGE_REFERENCES_FILENAME} manifest"
            )
        self._logger.info("Resolved cluster-version-operator pullspec: %s", cvo_pullspec)
        return cvo_pullspec

    async def _resolve_art_images_pullspec(self, imagestream_pullspec: str) -> str:
        """Resolve a quay art-dev pullspec from the imagestream to the original Konflux build output.

        The imagestream contains single-arch pullspecs mirrored by build-sync. The Konflux build
        needs the multi-arch manifest list from art-images. We extract the NVR from the image
        labels, query the Konflux DB by NVR, and return the build record's image_pullspec.

        :param imagestream_pullspec: The pullspec from the imagestream (e.g., quay.io/openshift-release-dev/...)
        :return: The art-images pullspec for the Konflux build output.
        """
        if not self.runtime.konflux_db:
            raise DoozerFatalError("Konflux DB is not available; cannot resolve CVO pullspec to art-images pullspec")

        self.runtime.konflux_db.bind(KonfluxBuildRecord)

        name, version, release_str = await release_inspector.extract_nvr_from_pullspec(
            imagestream_pullspec, registry_config=self.registry_config
        )
        cvo_nvr = f"{name}-{version}-{release_str}"
        self._logger.info("CVO NVR resolved from imagestream pullspec: %s", cvo_nvr)

        build_record = await self.runtime.konflux_db.get_build_record_by_nvr(
            nvr=cvo_nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            exclude_large_columns=True,
        )
        if not build_record:
            raise DoozerFatalError(f"No successful Konflux build record found for CVO NVR {cvo_nvr}")
        if not build_record.image_pullspec:
            raise DoozerFatalError(f"Konflux build record for CVO NVR {cvo_nvr} has no image_pullspec")

        self._logger.info("Resolved CVO to art-images pullspec: %s", build_record.image_pullspec)
        return build_record.image_pullspec

    async def _rebase(self) -> Tuple[BuildRepo, str, str]:
        """Generate manifests, write the Dockerfile, and commit the result to a local clone.

        :return: A tuple of (build_repo, cvo_pullspec, branch).
        """
        runtime = self.runtime
        repo_dir = Path(runtime.working_dir, constants.WORKING_SUBDIR_RELEASE_PAYLOAD_SOURCES, runtime.group)
        branch = KonfluxRebaser.construct_dest_branch(runtime.group, runtime.assembly, "release-payload")

        self._logger.info("Preparing release payload source repository at %s on branch %s...", repo_dir, branch)
        build_repo = BuildRepo(url=self.payload_repo, branch=branch, local_dir=repo_dir, logger=self._logger)
        await build_repo.ensure_source(upcycle=runtime.upcycle, strict=False)

        # Clear out any stale content from a previous rebase before regenerating.
        await build_repo.delete_all_files()
        manifests_dir = repo_dir / RELEASE_MANIFESTS_SUBDIR
        if manifests_dir.exists():
            await exectools.to_thread(shutil.rmtree, manifests_dir)

        cvo_pullspec = await self._generate_manifests(manifests_dir)

        # The imagestream pullspec is a single-arch quay art-dev image. Resolve it to the
        # original Konflux build output (image_pullspec in art-images) so the Dockerfile FROM
        # is a multi-arch manifest list natively accessible from within the Konflux build environment.
        from_pullspec = await self._resolve_art_images_pullspec(cvo_pullspec)

        if "@" not in from_pullspec:
            raise DoozerFatalError(f"Expected digest-based art-images pullspec but got: {from_pullspec}")
        cvo_image_digest = from_pullspec.split("@", 1)[1]
        dockerfile_content = (
            f"FROM {from_pullspec} AS cvo\n"
            f"\n"
            f"FROM scratch\n"
            f"COPY --from=cvo / /\n"
            f'LABEL io.openshift.release="{self._get_release_label()}" \\\n'
            f'      io.openshift.release.base-image-digest="{cvo_image_digest}"\n'
            f"COPY {RELEASE_MANIFESTS_SUBDIR}/ /{RELEASE_MANIFESTS_SUBDIR}/\n"
        )
        dockerfile_path = repo_dir / "Dockerfile"
        await exectools.to_thread(dockerfile_path.write_text, dockerfile_content)

        message = self.commit_message or (
            f"Rebase release payload manifests for {runtime.group} assembly {runtime.assembly}\n\n"
            f"version: {self.version}\nrelease: {self.release}"
        )
        await build_repo.commit(message, allow_empty=True, force=True)
        return build_repo, cvo_pullspec, branch

    async def _build(self, build_repo: BuildRepo, arches: Sequence[str]) -> Dict:
        """Ensure the Konflux Application/Component exist and start (and wait for) the build.

        :param build_repo: The rebased release payload source repo (must have a commit).
        :param arches: The architectures Konflux should build for this release payload.
        :return: A dict with the output image, PipelineRun name/URL, and build outcome.
        """
        if not build_repo.commit_hash:
            raise IOError("Release payload repository must have a commit to build. Did you rebase?")

        start_time = datetime.now(tz=timezone.utc)
        runtime = self.runtime
        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_namespace,
            config_file=self.konflux_kubeconfig,
            context=self.konflux_context,
            dry_run=self.dry_run,
        )
        app_name = self.get_application_name()
        component_name = self.get_component_name(runtime.group)
        self._logger.info("Using Konflux application %s, component %s", app_name, component_name)
        await konflux_client.ensure_application(name=app_name, display_name=app_name)
        await konflux_client.ensure_component(
            name=component_name,
            application=app_name,
            component_name=component_name,
            image_repo=self.image_repo,
            source_url=build_repo.https_url,
            revision=build_repo.branch,
        )

        git_auth_secret = await konflux_client.ensure_git_auth_secret(namespace=self.konflux_namespace)
        refresh_task = asyncio.create_task(konflux_client.token_refresh_loop(namespace=self.konflux_namespace))

        output_image = f"{self.image_repo}:{self.version}-{self.release}"
        # The Component (and its branch) is shared by every assembly of the group, so fold the
        # assembly into the generateName prefix -- otherwise builds for different assemblies are
        # indistinguishable in the Konflux UI's PipelineRun list. The group is omitted here: it's
        # redundant with the assembly (e.g. group `openshift-4.21` + assembly `4.21.1`), and
        # dropping it keeps the generated name shorter.
        assembly_slug = str(runtime.assembly).replace(".", "-").replace("_", "-").lower()
        try:
            pipelinerun_info = await konflux_client.start_pipeline_run_for_image_build(
                generate_name=f"release-payload-{assembly_slug}-",
                namespace=self.konflux_namespace,
                application_name=app_name,
                component_name=component_name,
                git_url=build_repo.https_url,
                commit_sha=build_repo.commit_hash,
                target_branch=build_repo.branch or build_repo.commit_hash,
                output_image=output_image,
                building_arches=arches,
                git_auth_secret=git_auth_secret,
                pipelinerun_template_url=self.plr_template,
                build_params=ImageBuildParams(
                    skip_checks=self.skip_checks,
                    skip_tasks=self.skip_tasks,
                    hermetic=True,
                    fetch_tags=False,
                    build_priority=RELEASE_PAYLOAD_BUILD_PRIORITY,
                ),
            )
            url = konflux_client.resource_url(pipelinerun_info.to_dict())
            self._logger.info("PipelineRun %s created: %s", pipelinerun_info.name, url)

            self._logger.info("Waiting for PipelineRun %s to complete...", pipelinerun_info.name)
            pipelinerun_info = await konflux_client.wait_for_pipelinerun(
                pipelinerun_info.name, namespace=self.konflux_namespace
            )
            succeeded_condition = pipelinerun_info.find_condition('Succeeded')
            outcome = KonfluxBuildOutcome.extract_from_pipelinerun_succeeded_condition(succeeded_condition)
            self._logger.info("PipelineRun %s completed with outcome %s", pipelinerun_info.name, outcome)

            await self._record_build(
                build_repo=build_repo,
                component_name=component_name,
                arches=arches,
                output_image=output_image,
                pipelinerun_url=url,
                outcome=outcome,
                start_time=start_time,
            )
        finally:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
            try:
                await konflux_client.delete_git_auth_secret(namespace=self.konflux_namespace)
                await konflux_client.cleanup_stale_git_auth_secrets(namespace=self.konflux_namespace)
            except Exception as e:
                self._logger.warning("Failed to cleanup git-auth secrets: %s", e)

        return {
            "output_image": output_image,
            "pipelinerun_name": pipelinerun_info.name,
            "pipelinerun_url": url,
            "outcome": str(outcome),
        }

    async def _record_build(
        self,
        build_repo: BuildRepo,
        component_name: str,
        arches: Sequence[str],
        output_image: str,
        pipelinerun_url: str,
        outcome: KonfluxBuildOutcome,
        start_time: datetime,
    ) -> None:
        """Persist a KonfluxBuildRecord for this release payload build so it (and every other
        release payload build for the group) can be looked up later, e.g. via --nvr.

        There is no dedicated "release payload" artifact type -- this reuses the same
        ``builds`` table and ``artifact_type=IMAGE`` as every other Konflux build. Records are
        distinguished by their stable name (``RELEASE_PAYLOAD_BUILD_RECORD_NAME``) plus
        group/version/release, giving an NVR of ``release-payload-<version>-<release>``.

        Failures to record are logged and swallowed: a missing DB row shouldn't fail an
        otherwise-successful (or already-failed) build.
        """
        if self.dry_run:
            self._logger.info("[DRY RUN] Would have recorded release payload build in Konflux DB")
            return

        runtime = self.runtime
        if not runtime.konflux_db:
            self._logger.warning("Konflux DB is not available; not recording release payload build.")
            return

        try:
            runtime.konflux_db.bind(KonfluxBuildRecord)
            record = KonfluxBuildRecord(
                name=RELEASE_PAYLOAD_BUILD_RECORD_NAME,
                group=runtime.group,
                version=self.version.lstrip("v"),
                release=self.release,
                assembly=str(runtime.assembly),
                arches=list(arches),
                # Release payloads are always public GA/candidate-track artifacts; embargo
                # (private-fix / .p3) support doesn't apply to this component.
                embargoed=False,
                hermetic=True,
                source_repo=self.payload_repo,
                rebase_repo_url=build_repo.https_url,
                rebase_commitish=build_repo.commit_hash,
                start_time=start_time,
                end_time=datetime.now(tz=timezone.utc),
                artifact_type=ArtifactType.IMAGE,
                engine=Engine.KONFLUX,
                image_pullspec=output_image,
                outcome=outcome,
                art_job_url=os.getenv("BUILD_URL", "n/a"),
                build_pipeline_url=pipelinerun_url,
                build_component=component_name,
                build_priority=int(RELEASE_PAYLOAD_BUILD_PRIORITY),
            )
            runtime.konflux_db.add_build(record)
            self._logger.info("Recorded release payload build %s in Konflux DB", record.nvr)
        except Exception as e:
            self._logger.warning("Failed to record release payload build in Konflux DB: %s", e)

    async def _sync(self, source_pullspec: str, arches: Sequence[str] = ()) -> Dict:
        """Mirror the built release payload -- the manifest list and every per-arch member --
        to the release registry.

        Konflux publishes the payload as a manifest list tagged in art-images
        (``source_pullspec``). This resolves that tag to its manifest-list digest as well as
        the digest of each per-arch member, and mirrors each of them individually into
        ``self.release_image_repo``. ``sync_to_quay`` tags each with its own ``sha256-<digest>``
        tag so quay does not garbage-collect it.

        :param source_pullspec: The pullspec to sync, tagged or digest-based (e.g.
            ``repo:1.2.3-1`` from a fresh ``_build``, or a build record's ``image_pullspec``
            when syncing an already-built payload by NVR).
        :param arches: The arches Konflux built for this payload. Only used by the
            (currently disabled) promote-style tagging below.
        :return: A dict describing what was synced.
        """
        source_repo = source_pullspec.split("@", 1)[0] if "@" in source_pullspec else source_pullspec.rsplit(":", 1)[0]

        # Fall back to QUAY_AUTH_FILE env var (set by Jenkins) when --registry-config
        # wasn't passed, matching the pattern used by sync_to_quay().
        registry_config = self.registry_config or os.environ.get("QUAY_AUTH_FILE")

        self._logger.info("Resolving manifest-list digest for %s...", source_pullspec)
        list_digest = await find_manifest_list_sha(source_pullspec, registry_config=registry_config)
        list_pullspec = f"{source_repo}@{list_digest}"

        self._logger.info("Resolving per-arch digests for %s...", source_pullspec)
        arch_infos = await oc_image_info_async(
            source_pullspec, '--show-multiarch', registry_config=registry_config
        )
        arch_digests = [info["digest"] for info in arch_infos if info.get("digest")]
        arch_pullspecs = [f"{source_repo}@{digest}" for digest in arch_digests]

        if self.dry_run:
            self._logger.warning(
                "[DRY RUN] Would have synced %s (list) and %s (arches) to %s",
                list_pullspec,
                arch_pullspecs,
                self.release_image_repo,
            )
            return {
                "synced": False,
                "release_repo": self.release_image_repo,
                "release_pullspec": list_pullspec,
                "arch_pullspecs": arch_pullspecs,
            }

        self._logger.info("Syncing release payload manifest list %s to %s...", list_pullspec, self.release_image_repo)
        await sync_to_quay(list_pullspec, self.release_image_repo)
        for arch_pullspec in arch_pullspecs:
            self._logger.info("Syncing release payload arch image %s to %s...", arch_pullspec, self.release_image_repo)
            await sync_to_quay(arch_pullspec, self.release_image_repo)

        # TODO(2026-08-14): The real "promote" pyartcd pipeline (pyartcd/pipelines/promote.py)
        # is the system of record for publishing release payloads to
        # quay.io/openshift-release-dev/ocp-release under the human-readable `<release>-<arch>`
        # and `<release>-multi` tags. The manifest list and every per-arch image are already
        # synced above (each pinned by its own sha256-<digest> tag); what stays disabled here
        # is writing to those SAME tags promote owns, since that would race with (and could
        # overwrite, or be overwritten by) promote's output. Until this command is adopted as
        # the source for promote to consume, only the digest-pinned sha256 tags are published.
        # Once that hand-off is decided, enable the block below (which mirrors the pattern used
        # by PromotePipeline._promote_heterogeneous_payload / PromotePipeline.build_release_image)
        # to also publish the human-readable tags:
        #
        # release_name = f"{self.version.lstrip('v')}-{self.release}"
        # multi_dest = f"{self.release_image_repo}:{release_name}-multi"
        # await _oc_image_mirror(
        #     ["oc", "image", "mirror", "--keep-manifest-list", list_pullspec, multi_dest],
        #     f"tagging {list_pullspec} as {multi_dest}",
        # )
        # for brew_arch in arches:
        #     go_arch = go_arch_for_brew_arch(brew_arch)  # artcommonlib.arch_util.go_arch_for_brew_arch
        #     arch_dest = f"{self.release_image_repo}:{release_name}-{brew_arch}"
        #     await _oc_image_mirror(
        #         ["oc", "image", "mirror", f"--filter-by-os=linux/{go_arch}", list_pullspec, arch_dest],
        #         f"tagging {list_pullspec} as {arch_dest}",
        #     )

        self._logger.info("Synced release payload %s to %s", list_pullspec, self.release_image_repo)
        return {
            "synced": True,
            "release_repo": self.release_image_repo,
            "release_pullspec": list_pullspec,
            "arch_pullspecs": arch_pullspecs,
        }

    def _get_release_label(self) -> str:
        """Return the value for the io.openshift.release Dockerfile label.

        For STANDARD assemblies (GA) this matches the bare version (e.g. "4.21.1").
        For PREVIEW/CANDIDATE assemblies this includes the prerelease suffix (e.g. "5.0.0-ec.6"),
        which is required so the label accurately identifies the pre-GA release.
        """
        runtime = self.runtime
        try:
            return get_release_name_for_assembly(runtime.group, runtime.get_releases_config(), runtime.assembly)
        except ValueError:
            return self.version.lstrip("v")

    async def _resolve_version(self) -> str:
        """Derive --version from --group/--assembly when it wasn't explicitly supplied.

        Mirrors how the promote pyartcd pipeline derives its release name (via
        ``get_release_name_for_assembly``) instead of taking a version string as a
        parameter. That helper returns the full release name -- e.g. ``"4.12.95"`` for a
        STANDARD/GA assembly, or ``"5.0.0-ec.5"`` for a CANDIDATE/PREVIEW pre-GA assembly
        (major.minor from the group, assembly name as the prerelease suffix). This command's
        ``--version`` must be bare major.minor.patch (enforced by
        ``validate_semver_major_minor_patch`` when supplied explicitly), so any prerelease
        suffix is stripped here.

        :return: A "v"-prefixed major.minor.patch version string, e.g. ``"v4.12.95"``.
        """
        runtime = self.runtime
        try:
            release_name = get_release_name_for_assembly(runtime.group, runtime.get_releases_config(), runtime.assembly)
            parsed = Version.parse(release_name)
        except ValueError as e:
            raise DoozerFatalError(
                f"Could not derive a version from assembly {runtime.assembly!r} in group {runtime.group!r}: {e}"
            ) from e
        derived = f"v{parsed.major}.{parsed.minor}.{parsed.patch}"
        self._logger.info(
            "Derived --version=%s from assembly %s (release name %s)", derived, runtime.assembly, release_name
        )
        return derived

    async def _sync_nvr(self) -> Dict:
        """Look up an already-built release payload by NVR and mirror it out, skipping
        rebase/build entirely.

        This is the counterpart to ``_record_build``: it looks up the build record written
        for a prior (possibly unrelated, e.g. a different CI run) invocation of this command
        by NVR, and syncs its ``image_pullspec`` the same way a fresh build's output would be.

        :return: A dict describing the resolved NVR and the sync result.
        """
        runtime = self.runtime
        if not runtime.konflux_db:
            raise DoozerFatalError("Konflux DB is not available; cannot resolve --nvr to a build record")

        runtime.konflux_db.bind(KonfluxBuildRecord)
        build_record = await runtime.konflux_db.get_build_record_by_nvr(
            nvr=self.nvr,
            outcome=KonfluxBuildOutcome.SUCCESS,
            exclude_large_columns=True,
        )
        if not build_record:
            raise DoozerFatalError(f"No successful Konflux build record found for release payload NVR {self.nvr}")
        if not build_record.image_pullspec:
            raise DoozerFatalError(f"Konflux build record for release payload NVR {self.nvr} has no image_pullspec")

        self._logger.info(
            "Resolved release payload NVR %s to pullspec %s; syncing to %s",
            self.nvr,
            build_record.image_pullspec,
            self.release_image_repo,
        )
        result: Dict = {
            "nvr": self.nvr,
            "group": build_record.group,
            "assembly": build_record.assembly,
            "version": build_record.version,
            "release": build_record.release,
            "source_pullspec": build_record.image_pullspec,
            "synced": False,
        }
        sync_result = await self._sync(build_record.image_pullspec, build_record.arches or [])
        result.update(sync_result)
        return result

    async def run(self) -> Dict:
        """Rebase release payload manifests and, if --push was given, build them in Konflux.

        If --nvr was given, skip rebase/build entirely and only sync the already-built
        release payload with that NVR out to --release-image-repo.
        """
        runtime = self.runtime
        runtime.initialize(config_only=True)

        if self.nvr:
            return await self._sync_nvr()

        if runtime.assembly is None:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        assert runtime.group_config is not None, "group_config is not loaded; Doozer bug?"

        if not self.release:
            raise DoozerFatalError("--release is required unless --nvr is given.")

        arches: List[str] = runtime.get_global_konflux_arches()
        if not arches:
            raise DoozerFatalError(f"No architectures found in group config for {runtime.group}")

        if self.version is None or self.version == "auto":
            self.version = await self._resolve_version()

        self._logger.info(
            "Rebasing release payload for %s assembly %s (arches: %s)...", runtime.group, runtime.assembly, arches
        )
        build_repo, cvo_pullspec, branch = await self._rebase()

        result: Dict = {
            "group": runtime.group,
            "assembly": str(runtime.assembly),
            "version": self.version,
            "release": self.release,
            "arch": self.arch,
            "building_arches": arches,
            "payload_repo": build_repo.https_url,
            "branch": branch,
            "commit_sha": build_repo.commit_hash,
            "cvo_pullspec": cvo_pullspec,
            "pushed": False,
            "output_image": None,
            "pipelinerun_name": None,
            "pipelinerun_url": None,
            "outcome": None,
            "synced": False,
        }

        if not self.push:
            self._logger.info(
                "--push not set; skipping git push and Konflux build. Rebased content is available locally at %s",
                build_repo.local_dir,
            )
            return result

        if self.dry_run:
            self._logger.warning("[DRY RUN] Would have pushed branch %s to %s", branch, build_repo.https_url)
        else:
            self._logger.info("Pushing branch %s to %s...", branch, build_repo.https_url)
            await build_repo.push(force=True)
            result["pushed"] = True

        build_result = await self._build(build_repo, arches)
        result.update(build_result)

        if result["outcome"] != str(KonfluxBuildOutcome.SUCCESS):
            raise DoozerFatalError(
                f"Release payload build did not succeed for {runtime.group} assembly {runtime.assembly}: "
                f"{result['outcome']} ({result.get('pipelinerun_url')})"
            )

        if self.sync:
            sync_result = await self._sync(result["output_image"], arches)
            result.update(sync_result)

        return result


def _validate_optional_version(ctx, param, version):
    """Like validate_semver_major_minor_patch, but tolerates None/"auto" (both mean
    "derive the version from --group/--assembly"), matching the convention used by
    images:konflux:rebase's _validate_version.
    """
    if version is None or version == "auto":
        return version
    return validate_semver_major_minor_patch(ctx, param, version)


@cli.command(
    "beta:release-payload:rebase-and-build",
    short_help="Generate release payload manifests and build the release payload image in Konflux",
)
@click.option(
    "--version",
    metavar='VERSION',
    default=None,
    callback=_validate_optional_version,
    help="Version string for the release payload NVR. If omitted (or \"auto\"), derived from"
    " --group/--assembly, the same convention used by the promote pipeline.",
)
@click.option(
    "--release",
    metavar='RELEASE',
    default=None,
    help="Release string for the release payload NVR. Required unless --nvr is given.",
)
@click.option(
    "--nvr",
    metavar='NVR',
    default=None,
    help="Skip rebase and build; only sync the already-built release payload with this NVR"
    " out to --release-image-repo. Looked up in the Konflux DB. When given, --release and"
    " every rebase/build option are ignored.",
)
@click.option(
    "--arch",
    metavar='ARCH',
    default='x86_64',
    help="Brew arch of the build-sync imagestream to source release manifests from."
    " Does not limit which arches Konflux builds; Konflux always builds a multi-arch manifest list.",
)
@click.option(
    "--from-release",
    metavar='PULLSPEC',
    default=None,
    help="Use an existing release image pullspec as the manifest source instead of the derived imagestream.",
)
@click.option(
    "--message",
    "-m",
    metavar='MSG',
    default=None,
    help="Commit message. If not provided, a default generated message will be used.",
)
@click.option(
    "--payload-repo",
    metavar='URL',
    default=constants.ART_RELEASE_PAYLOAD_GIT_REPO,
    help="The git repository to push the rebased release payload source to.",
)
@click.option(
    "--image-repo",
    default=constants.KONFLUX_DEFAULT_IMAGE_REPO,
    help="Push the built release payload image to the specified repo.",
)
@click.option(
    '--konflux-kubeconfig', metavar='PATH', help='Path to the kubeconfig file to use for Konflux cluster connections.'
)
@click.option(
    '--konflux-context',
    metavar='CONTEXT',
    help='The name of the kubeconfig context to use for Konflux cluster connections.',
)
@click.option(
    '--konflux-namespace',
    metavar='NAMESPACE',
    default=KONFLUX_DEFAULT_NAMESPACE,
    help='The namespace to use for Konflux cluster connections.',
)
@click.option(
    "--registry-config",
    metavar='PATH',
    default=None,
    help="Path to a registry auth file to use when reading operator images while generating manifests.",
)
@click.option('--skip-checks', default=False, is_flag=True, help='Skip all post build checks')
@click.option(
    '--skip-task',
    'skip_tasks',
    multiple=True,
    help='Remove a named Tekton task from the PipelineRun. Repeatable (e.g. --skip-task clair-scan --skip-task sast-snyk-check).',
)
@click.option(
    '--plr-template',
    required=False,
    default=constants.KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL,
    help='Use a custom PipelineRun template to build the release payload image.'
    ' Overrides the default template from openshift-priv/art-konflux-template',
)
@click.option(
    '--push',
    is_flag=True,
    default=False,
    help='Push the rebased content to git and trigger the Konflux build. Without it, manifests are only'
    ' generated and committed locally.',
)
@click.option(
    '--sync',
    is_flag=True,
    default=False,
    help='After a successful build, mirror the built release payload manifest list and every'
    ' per-arch image to --release-image-repo, each pinned by its own sha256-<digest> tag to'
    ' prevent quay garbage collection. Requires --push. Uses the QUAY_AUTH_FILE environment'
    ' variable for push credentials.',
)
@click.option(
    '--release-image-repo',
    metavar='REPO',
    default=constants.RELEASE_PAYLOAD_DEST_REPO,
    help='Quay repo to sync the built release payload to when --sync is set.',
)
@click.option(
    '--dry-run',
    is_flag=True,
    default=False,
    help='Do not push to git or call the Konflux API; only log what would happen.',
)
@click.option(
    '--output',
    '-o',
    type=click.Choice(['json'], case_sensitive=False),
    default=None,
    help='Output the result in the specified machine-parseable format.',
)
@pass_runtime
@click_coroutine
async def release_payload_rebase_and_build(
    runtime: Runtime,
    version: Optional[str],
    release: Optional[str],
    nvr: Optional[str],
    arch: str,
    from_release: Optional[str],
    message: Optional[str],
    payload_repo: str,
    image_repo: str,
    konflux_kubeconfig: Optional[str],
    konflux_context: Optional[str],
    konflux_namespace: str,
    registry_config: Optional[str],
    skip_checks: bool,
    skip_tasks: Tuple[str, ...],
    plr_template: str,
    push: bool,
    sync: bool,
    release_image_repo: str,
    dry_run: bool,
    output: Optional[str],
):
    """
    Generate release payload manifests and build the release payload image in Konflux.

    This command runs `oc adm release new --to-dir` to snapshot the manifests already present
    in the group's build-sync imagestream, writes a Dockerfile that layers those manifests onto
    the cluster-version-operator image, pushes the result to openshift-priv/ocp-release-payloads,
    and (with --push) triggers a Konflux build of the release payload image.

    Example usage:

    doozer --group=openshift-4.21 --assembly=4.21.1 beta:release-payload:rebase-and-build \\
        --release=202608011200.p2 --push --sync

    --version is optional: when omitted (or set to "auto") it is derived from
    --group/--assembly, the same convention used by the promote pipeline.

    Pass --nvr to skip rebase/build entirely and just sync an already-built release payload
    (looked up in the Konflux DB) out to --release-image-repo:

    doozer --group=openshift-4.21 --assembly=4.21.1 beta:release-payload:rebase-and-build \\
        --nvr=release-payload-4.21.1-202608011200.p2
    """
    if not konflux_kubeconfig:
        konflux_kubeconfig = os.environ.get('KONFLUX_SA_KUBECONFIG')
    if not konflux_kubeconfig:
        LOGGER.info(
            "--konflux-kubeconfig and KONFLUX_SA_KUBECONFIG env var are not set. Will rely on oc being logged in"
        )

    cli_obj = ReleasePayloadRebaseAndBuildCli(
        runtime=runtime,
        version=version,
        release=release,
        nvr=nvr,
        arch=arch,
        payload_repo=payload_repo,
        image_repo=image_repo,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_context=konflux_context,
        konflux_namespace=konflux_namespace,
        from_release=from_release,
        commit_message=message,
        registry_config=registry_config,
        skip_checks=skip_checks,
        skip_tasks=skip_tasks,
        plr_template=plr_template,
        push=push,
        sync=sync,
        release_image_repo=release_image_repo,
        dry_run=dry_run,
    )
    try:
        result = await cli_obj.run()
    except Exception as e:
        if output == 'json':
            click.echo(json.dumps({"error": str(e)}, indent=2))
            sys.exit(1)
        raise

    if output == 'json':
        click.echo(json.dumps(result, indent=2))
    else:
        LOGGER.info("Release payload rebase and build complete:\n%s", json.dumps(result, indent=2))
