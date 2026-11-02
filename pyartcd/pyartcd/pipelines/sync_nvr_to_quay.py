"""
sync-nvr-to-quay pipeline: Sync a Konflux-built image (identified by its NVR)
into an approved quay repo, tagging it so quay does not garbage-collect it.

Modeled on mirror_images_to_ci.py (Pipeline class + @cli.command + click options
+ registration in pipelines/__init__.py).

Embargoed NVRs are NEVER synced to the target repo.
"""

import logging

import click
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.constants import KONFLUX_IMAGE_TEST_REPO
from artcommonlib.konflux.konflux_build_record import KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.util import sync_to_quay

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

# Allowlist of quay repos this pipeline is permitted to sync into. The Jenkins job
# presents TARGET_REPO as a dropdown, but the allowlist is still enforced here so
# the pipeline cannot be driven to push to an arbitrary repo.
ALLOWED_TARGET_REPOS = frozenset(
    {
        KONFLUX_IMAGE_TEST_REPO,
    }
)


async def sync_image_to_quay(
    image_pullspec: str,
    target_repo: str,
    tags: list,
    dry_run: bool,
    logger: logging.Logger = LOGGER,
) -> None:
    """
    Shared helper: sync a single image pullspec to a quay target repo with the
    given tags, honoring dry-run.

    sync_to_quay already creates a ``sha256-<digest>`` tag to prevent GC; ``tags``
    are additional human-friendly tags. Empty/falsy tags are dropped.
    """
    filtered_tags = [t for t in tags if t]
    if dry_run:
        logger.info("[DRY RUN] Would sync %s to %s with additional tags %s", image_pullspec, target_repo, filtered_tags)
        return
    logger.info("Syncing %s to %s with additional tags %s", image_pullspec, target_repo, filtered_tags)
    await sync_to_quay(image_pullspec, target_repo, filtered_tags)


class SyncNvrToQuayPipeline:
    """Sync one or more Konflux-built images (by NVR) into an approved quay repo."""

    def __init__(
        self,
        runtime: Runtime,
        nvrs: list,
        target_repo: str = KONFLUX_IMAGE_TEST_REPO,
    ) -> None:
        self.runtime = runtime
        self._logger = runtime.logger
        self.nvrs = nvrs
        self.target_repo = target_repo

        self._validate_parameters()

    def _validate_parameters(self) -> None:
        """Validate the NVR list and enforce the target-repo allowlist."""
        if not self.nvrs:
            raise ValueError("At least one NVR is required")

        if self.target_repo not in ALLOWED_TARGET_REPOS:
            raise ValueError(
                f"Target repo {self.target_repo!r} is not allowed. Allowed repos: {sorted(ALLOWED_TARGET_REPOS)}"
            )

    async def _sync_nvr(self, db: KonfluxDb, nvr: str) -> None:
        """Resolve a single NVR's build record, apply the embargo gate, and sync."""
        self._logger.info("Processing NVR %s", nvr)
        record: KonfluxBuildRecord = await db.get_build_record_by_nvr(nvr)

        # EMBARGO GATE: embargoed builds must NEVER be synced to this repo.
        if is_nvr_embargoed(nvr) or getattr(record, "embargoed", False):
            self._logger.warning("Skipping %s: NVR is embargoed and must not be synced", nvr)
            return

        # X.Y version derived from the record version (e.g. "4.18.0" -> "4.18").
        major_minor = ".".join((record.version or "").split(".")[:2])
        # Human-friendly tags mirroring the old mirror_images pattern.
        tags = [record.image_tag, f"{record.name}-{major_minor}"]

        await sync_image_to_quay(
            record.image_pullspec,
            self.target_repo,
            tags,
            dry_run=self.runtime.dry_run,
            logger=self._logger,
        )
        self._logger.info("Done syncing %s", nvr)

    async def run(self) -> None:
        self._logger.info("Starting sync-nvr-to-quay for %s NVR(s) -> %s", len(self.nvrs), self.target_repo)

        db = KonfluxDb()
        db.bind(KonfluxBuildRecord)

        for nvr in self.nvrs:
            await self._sync_nvr(db, nvr)

        self._logger.info("sync-nvr-to-quay completed")


@cli.command(
    "sync-nvr-to-quay",
    help="Sync Konflux-built image(s) identified by NVR into an approved quay repo, "
    "tagging them so quay does not garbage-collect them. Embargoed NVRs are never synced.",
)
@click.option(
    "--nvr",
    "nvr",
    required=True,
    help="Comma-separated list of one or more NVRs to sync.",
)
@click.option(
    "--target-repo",
    default=KONFLUX_IMAGE_TEST_REPO,
    show_default=True,
    help="Target quay repo to sync into. Must be on the server-side allowlist.",
)
@pass_runtime
@click_coroutine
async def sync_nvr_to_quay_cli(runtime: Runtime, nvr: str, target_repo: str):
    """CLI entrypoint for the sync-nvr-to-quay pipeline."""
    nvrs = [n.strip() for n in nvr.split(",") if n.strip()]
    pipeline = SyncNvrToQuayPipeline(runtime, nvrs=nvrs, target_repo=target_repo)
    await pipeline.run()
