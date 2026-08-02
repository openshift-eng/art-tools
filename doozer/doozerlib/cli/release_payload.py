import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import click
import yaml
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome

from doozerlib import constants
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

LOGGER = logging.getLogger(__name__)

# The name of the manifest in the output of `oc adm release new --to-dir` that describes
# the images referenced by the release payload, including the cluster-version-operator tag.
IMAGE_REFERENCES_FILENAME = "image-references"
RELEASE_MANIFESTS_SUBDIR = "release-manifests"


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
        version: str,
        release: str,
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
        dry_run: bool = False,
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
        self.dry_run = dry_run
        self._logger = LOGGER

    @staticmethod
    def get_application_name(group: str) -> str:
        """Konflux Application name for a group's release payload builds."""
        return f"release-payload-{group}".replace(".", "-").replace("_", "-")

    @staticmethod
    def get_component_name(group: str) -> str:
        """Konflux Component name for a group's release payload builds.

        There is a single Component per group (Konflux builds all architectures as one
        multi-arch manifest list from a single PipelineRun), so this is the same as the
        Application name.
        """
        return ReleasePayloadRebaseAndBuildCli.get_application_name(group)

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
        release_name = f"{self.version}-{self.release}"
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

        dockerfile_content = f"FROM {cvo_pullspec}\nCOPY {RELEASE_MANIFESTS_SUBDIR}/ /{RELEASE_MANIFESTS_SUBDIR}/\n"
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

        runtime = self.runtime
        konflux_client = KonfluxClient.from_kubeconfig(
            default_namespace=self.konflux_namespace,
            config_file=self.konflux_kubeconfig,
            context=self.konflux_context,
            dry_run=self.dry_run,
        )
        app_name = self.get_application_name(runtime.group)
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

        output_image = f"{self.image_repo}:{self.version}-{self.release}"
        pipelinerun_info = await konflux_client.start_pipeline_run_for_image_build(
            generate_name=f"{component_name}-",
            namespace=self.konflux_namespace,
            application_name=app_name,
            component_name=component_name,
            git_url=build_repo.https_url,
            commit_sha=build_repo.commit_hash,
            target_branch=build_repo.branch or build_repo.commit_hash,
            output_image=output_image,
            building_arches=arches,
            pipelinerun_template_url=self.plr_template,
            build_params=ImageBuildParams(
                skip_checks=self.skip_checks,
                skip_tasks=self.skip_tasks,
                hermetic=True,
                fetch_tags=False,
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

        return {
            "output_image": output_image,
            "pipelinerun_name": pipelinerun_info.name,
            "pipelinerun_url": url,
            "outcome": str(outcome),
        }

    async def run(self) -> Dict:
        """Rebase release payload manifests and, if --push was given, build them in Konflux."""
        runtime = self.runtime
        runtime.initialize(config_only=True)
        if runtime.assembly is None:
            raise ValueError("Assemblies feature is disabled for this group. This is no longer supported.")
        assert runtime.group_config is not None, "group_config is not loaded; Doozer bug?"

        arches: List[str] = runtime.get_global_konflux_arches()
        if not arches:
            raise DoozerFatalError(f"No architectures found in group config for {runtime.group}")

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
        return result


@cli.command(
    "beta:release-payload:rebase-and-build",
    short_help="Generate release payload manifests and build the release payload image in Konflux",
)
@click.option(
    "--version",
    metavar='VERSION',
    required=True,
    callback=validate_semver_major_minor_patch,
    help="Version string for the release payload NVR.",
)
@click.option("--release", metavar='RELEASE', required=True, help="Release string for the release payload NVR.")
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
    version: str,
    release: str,
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
        --version=4.21.1 --release=202608011200.p0 --push
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
