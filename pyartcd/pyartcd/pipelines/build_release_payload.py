import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import click
from artcommonlib import exectools
from artcommonlib.constants import KONFLUX_DEFAULT_NAMESPACE

from pyartcd import constants
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.signatory import SigstoreSignatory

CONCURRENCY_LIMIT = 100


def _default_release() -> str:
    """Generate a release string in the same format as the Jenkins job (yyyyMMddHHmm.p2)."""
    return datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M") + ".p2"


class BuildReleasePayloadPipeline:
    """Orchestrates building a release payload image via the doozer
    beta:release-payload:rebase-and-build command and cosigning the result.

    Follows the same pattern as BuildFbcPipeline: calls a doozer command for
    the build/sync step, then uses SigstoreSignatory for cosigning (since
    doozer cannot import from pyartcd).
    """

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        nvr: Optional[str],
        release: Optional[str],
        version: Optional[str],
        arch: str,
        sync: bool,
        konflux_kubeconfig: Optional[str],
        konflux_namespace: str,
        release_image_repo: str,
        data_path: Optional[str],
        registry_config: Optional[str],
        skip_cosign: bool,
        skip_checks: bool,
        dry_run: bool,
    ):
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.nvr = nvr
        self.release = release
        self.version = version
        self.arch = arch
        self.sync = sync
        self.konflux_kubeconfig = konflux_kubeconfig
        self.konflux_namespace = konflux_namespace
        self.release_image_repo = release_image_repo
        self.data_path = data_path
        self.registry_config = registry_config
        self.skip_cosign = skip_cosign
        self.skip_checks = skip_checks
        self.dry_run = dry_run
        self._logger = runtime.logger

    def _check_environment_variables(self):
        for var in ["KMS_CRED_FILE", "KMS_KEY_ID"]:
            if not os.environ.get(var):
                msg = f"Environment variable {var} is not set."
                if self.dry_run:
                    self._logger.warning(msg)
                else:
                    raise ValueError(msg)

    async def _run_doozer(self) -> Dict:
        """Call the doozer beta:release-payload:rebase-and-build command and return its JSON output."""
        cmd = [
            "doozer",
            f"--group={self.group}",
            f"--assembly={self.assembly}",
            f"--working-dir={self.runtime.doozer_working}",
        ]
        if self.data_path:
            cmd.append(f"--data-path={self.data_path}")

        cmd += [
            "beta:release-payload:rebase-and-build",
            f"--arch={self.arch}",
            f"--release-image-repo={self.release_image_repo}",
            "--output=json",
        ]

        if self.nvr:
            # Skip rebase/build; doozer will look up the NVR and sync it.
            cmd.append(f"--nvr={self.nvr}")
        else:
            release = self.release or _default_release()
            cmd.append(f"--release={release}")
            if self.version:
                cmd.append(f"--version={self.version}")
            if not self.dry_run:
                # Matches Jenkinsfile: --push always in non-dry-run, --sync is opt-in.
                cmd.append("--push")
                if self.sync:
                    cmd.append("--sync")

        if self.konflux_kubeconfig:
            cmd.append(f"--konflux-kubeconfig={self.konflux_kubeconfig}")
        if self.konflux_namespace:
            cmd.append(f"--konflux-namespace={self.konflux_namespace}")
        if self.registry_config:
            cmd.append(f"--registry-config={self.registry_config}")
        if self.skip_checks:
            cmd.append("--skip-checks")
        if self.dry_run:
            cmd.append("--dry-run")

        self._logger.info("Running doozer command: %s", " ".join(cmd))
        await exectools.cmd_assert_async(cmd)

        result_path = Path(self.runtime.doozer_working, 'release-payload-result.json')
        if not result_path.exists():
            raise RuntimeError(f"doozer did not produce result file at {result_path}")
        try:
            return json.loads(result_path.read_text())
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Could not parse doozer result file {result_path}: {e}") from e

    async def _cosign(self, doozer_result: Dict) -> None:
        """Cosign the synced release payload images.

        Checks each SHA for an existing cosign signature before signing to
        make this step idempotent (safe to re-run). Only the manifest list
        and per-arch digests are signed -- no canonical tag signing, since
        this command only publishes digest-pinned copies to the release repo.
        """
        release_pullspec: Optional[str] = doozer_result.get("release_pullspec")
        arch_pullspecs: List[str] = doozer_result.get("arch_pullspecs") or []

        if not release_pullspec:
            raise RuntimeError("doozer result missing release_pullspec; cannot cosign")

        # The doozer command syncs from art-images into release_image_repo, tagging each image
        # by its sha256-<digest> tag. The release_pullspec / arch_pullspecs returned are in the
        # *source* repo (art-images). We need the corresponding *destination* pullspecs in
        # release_image_repo so we sign what was actually published.
        all_source_pullspecs = [release_pullspec] + list(arch_pullspecs)
        dest_pullspecs = []
        for ps in all_source_pullspecs:
            if "@" not in ps:
                self._logger.warning("Skipping non-digest pullspec during cosign: %s", ps)
                continue
            digest = ps.split("@", 1)[1]
            dest_pullspecs.append(f"{self.release_image_repo}@{digest}")

        if not dest_pullspecs:
            self._logger.warning("No digest-based pullspecs to cosign; skipping")
            return

        signatory = SigstoreSignatory(
            logger=self._logger,
            dry_run=self.dry_run,
            signing_creds=os.environ.get("KMS_CRED_FILE", "dummy-file"),
            signing_key_ids=os.environ.get("KMS_KEY_ID", "dummy-key").strip().split(","),
            rekor_url=os.environ.get("REKOR_URL", ""),
            concurrency_limit=CONCURRENCY_LIMIT,
        )

        unsigned: List[str] = []
        for ps in dest_pullspecs:
            if await signatory.has_cosign_signature(ps):
                self._logger.info("Already signed, skipping: %s", ps)
            else:
                self._logger.info("Not yet signed, will sign: %s", ps)
                unsigned.append(ps)

        if not unsigned:
            self._logger.info("All %d pullspecs already have cosign signatures; nothing to sign", len(dest_pullspecs))
            return

        self._logger.info("Signing %d/%d pullspecs (digest identity only)", len(unsigned), len(dest_pullspecs))
        errors = await signatory.sign_component_images(unsigned)
        if errors:
            raise RuntimeError(f"Cosign signing failed for {len(errors)} pullspec(s): {errors}")

        self._logger.info("Successfully cosigned %d release payload image(s)", len(unsigned))

    async def run(self) -> None:
        if not self.skip_cosign:
            self._check_environment_variables()

        doozer_result = await self._run_doozer()

        if self.skip_cosign:
            self._logger.info("--skip-cosign set; skipping cosigning")
            return

        if not doozer_result.get("synced"):
            self._logger.warning("Doozer reported synced=False; skipping cosign (images not in release repo)")
            return

        await self._cosign(doozer_result)


@cli.command("build-release-payload")
@click.option(
    "-g",
    "--group",
    metavar="NAME",
    required=True,
    help="The doozer group (e.g. openshift-4.21).",
)
@click.option(
    "-a",
    "--assembly",
    metavar="ASSEMBLY_NAME",
    required=True,
    help="The assembly name (e.g. 4.21.1).",
)
@click.option(
    "--nvr",
    metavar="NVR",
    default=None,
    help="If set, skip rebase and build — only sync this already-built payload NVR and cosign it. "
    "When given, --release, --version, --push, --sync, and --skip-checks are ignored.",
)
@click.option(
    "--release",
    metavar="RELEASE",
    default=None,
    help="Release string for the payload NVR (e.g. 202608011200.p2). "
    "If omitted, auto-generated as a UTC timestamp (yyyyMMddHHmm.p2). Ignored when --nvr is set.",
)
@click.option(
    "--version",
    metavar="VERSION",
    default=None,
    help="Version string (e.g. v4.21.1). If omitted, derived from --group/--assembly. Ignored when --nvr is set.",
)
@click.option(
    "--arch",
    metavar="ARCH",
    default="x86_64",
    help="Brew arch of the build-sync imagestream to source release manifests from.",
)
@click.option(
    "--sync",
    is_flag=True,
    default=False,
    help="After a successful build, mirror the release payload to --release-image-repo. "
    "In non-dry-run mode --push is always set; this flag additionally enables --sync. "
    "Ignored when --nvr is set (NVR path always syncs).",
)
@click.option(
    "--konflux-kubeconfig",
    metavar="PATH",
    default=None,
    envvar="KONFLUX_SA_KUBECONFIG",
    help="Path to the kubeconfig file for Konflux cluster connections.",
)
@click.option(
    "--konflux-namespace",
    metavar="NAMESPACE",
    default=KONFLUX_DEFAULT_NAMESPACE,
    help="Namespace to use for Konflux cluster connections.",
)
@click.option(
    "--release-image-repo",
    metavar="REPO",
    default=constants.RELEASE_IMAGE_REPO,
    help="Quay repo to sync and sign the built release payload in.",
)
@click.option(
    "--data-path",
    metavar="PATH",
    default=None,
    envvar="DOOZER_DATA_PATH",
    help="Path or URL to ocp-build-data (passed to doozer).",
)
@click.option(
    "--registry-config",
    metavar="PATH",
    default=None,
    help="Path to a registry auth file (passed to doozer for reading operator images).",
)
@click.option(
    "--skip-cosign",
    is_flag=True,
    default=False,
    help="Skip cosigning the synced release payload images.",
)
@click.option(
    "--skip-checks",
    is_flag=True,
    default=False,
    help="Pass --skip-checks to doozer (skip Konflux post-build checks). Ignored when --nvr is set.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Do not push, build, sync, or sign; only log what would happen.",
)
@pass_runtime
@click_coroutine
async def build_release_payload(
    runtime: Runtime,
    group: str,
    assembly: str,
    nvr: Optional[str],
    release: Optional[str],
    version: Optional[str],
    arch: str,
    sync: bool,
    konflux_kubeconfig: Optional[str],
    konflux_namespace: str,
    release_image_repo: str,
    data_path: Optional[str],
    registry_config: Optional[str],
    skip_cosign: bool,
    skip_checks: bool,
    dry_run: bool,
):
    """Build and cosign a release payload image for a given assembly.

    In the normal path, calls `doozer beta:release-payload:rebase-and-build --push [--sync]`
    to rebase, build, and optionally mirror the release payload, then cosigns each synced
    digest using sigstore/cosign.

    With --nvr, skips rebase and build entirely: looks up the already-built payload by NVR,
    syncs it to --release-image-repo, and cosigns it. This is safe to re-run — existing
    cosign signatures are detected and skipped.

    Environment variables required for signing (unless --skip-cosign):
      KMS_CRED_FILE  -- path to AWS credentials for KMS signing
      KMS_KEY_ID     -- AWS KMS key ID (comma-separated for multiple keys)
      REKOR_URL      -- Rekor transparency log URL (optional)
      QUAY_AUTH_FILE -- registry auth file for quay.io
    """
    pipeline = BuildReleasePayloadPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        nvr=nvr,
        release=release,
        version=version,
        arch=arch,
        sync=sync,
        konflux_kubeconfig=konflux_kubeconfig,
        konflux_namespace=konflux_namespace,
        release_image_repo=release_image_repo,
        data_path=data_path,
        registry_config=registry_config,
        skip_cosign=skip_cosign,
        skip_checks=skip_checks,
        dry_run=dry_run,
    )
    await pipeline.run()
