"""
sync-nvr-to-quay pipeline: Sync a Konflux-built image (identified by its NVR)
into an approved quay repo, tagging it so quay does not garbage-collect it.

Modeled on mirror_images_to_ci.py (Pipeline class + @cli.command + click options
+ registration in pipelines/__init__.py).

Embargoed NVRs are NEVER synced to the target repo.
"""

import logging
import os
import re

import click
from artcommonlib.build_visibility import is_nvr_embargoed
from artcommonlib.constants import KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_IMAGE_TEST_REPO, RHCOS_IMAGE_REPO
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome, KonfluxBuildRecord
from artcommonlib.konflux.konflux_db import KonfluxDb
from artcommonlib.registry_config import RegistryConfig
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

# Input allow-list patterns, validated before any DB query so untrusted operator input
# cannot reach the query layer. The NVR pattern is permissive enough for real NVRs
# (e.g. "rhcos-node-image-nvidia-container-v5.0.0-202610261547.p2.gb4b1579.assembly.stream.el10")
# while still rejecting whitespace and shell/SQL metacharacters.
# NOTE: the patterns are intentionally un-anchored and matched with re.fullmatch (not re.match
# with '$'), because in Python '$' also matches just before a trailing newline, so e.g. "abc\n"
# would otherwise sneak through. re.fullmatch requires the ENTIRE input to match.
NVR_PATTERN = re.compile(r"[A-Za-z0-9._+~-]{1,256}")
RECORD_ID_PATTERN = re.compile(r"[A-Za-z0-9-]{1,64}")


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
        allow_failed: bool = False,
        record_id: str = "",
    ) -> None:
        """Initialize the pipeline and validate its parameters.

        :param runtime: PyARTCD runtime instance.
        :param nvrs: List of one or more NVRs to sync.
        :param target_repo: Target quay repo; must be on the server-side allowlist.
        :param allow_failed: Also consider terminal failed builds (e.g. its_error).
        :param record_id: Konflux build record_id to disambiguate multiple records for one NVR.
        :raises ValueError: If any parameter fails validation.
        """
        self.runtime = runtime
        self._logger = runtime.logger
        self.nvrs = nvrs
        self.target_repo = target_repo
        self.allow_failed = allow_failed
        self.record_id = record_id

        self._validate_parameters()

    def _validate_parameters(self) -> None:
        """Validate inputs (allowlist + format) before any DB query is issued."""
        if not self.nvrs:
            raise ValueError("At least one NVR is required")

        if self.target_repo not in ALLOWED_TARGET_REPOS:
            raise ValueError(
                f"Target repo {self.target_repo!r} is not allowed. Allowed repos: {sorted(ALLOWED_TARGET_REPOS)}"
            )

        # Allow-list each NVR's format before it can reach the DB query layer.
        for nvr in self.nvrs:
            if not NVR_PATTERN.fullmatch(nvr):
                raise ValueError(
                    f"Invalid NVR {nvr!r}: must match {NVR_PATTERN.pattern} (letters, digits, '.', '-', '_', '+', '~')."
                )

        # Allow-list the record_id's format before it can reach the DB query layer.
        if self.record_id and not RECORD_ID_PATTERN.fullmatch(self.record_id):
            raise ValueError(
                f"Invalid record_id {self.record_id!r}: must match {RECORD_ID_PATTERN.pattern} "
                "(letters, digits, and '-')."
            )

        # --record-id disambiguates a single NVR; it is meaningless for several NVRs at once.
        if self.record_id and len(self.nvrs) > 1:
            raise ValueError(
                "--record-id is only meaningful for a single NVR; "
                f"got {len(self.nvrs)} NVRs. Re-run with one NVR at a time when using --record-id."
            )

    async def _resolve_record(self, db: KonfluxDb, nvr: str) -> KonfluxBuildRecord:
        """
        List matching build records for an NVR and select exactly one.

        When allow_failed is False, only outcome='success' records are eligible.
        When allow_failed is True, the search default (terminal success + failure
        outcomes, including its_error) applies. record_id disambiguates when more
        than one record exists for the NVR.
        """
        where = {"nvr": nvr}
        if self.record_id:
            where["record_id"] = self.record_id
        if not self.allow_failed:
            where["outcome"] = str(KonfluxBuildOutcome.SUCCESS)

        records = [record async for record in db.search_builds_by_fields(where=where)]

        if self.record_id:
            if not records:
                raise ValueError(
                    f"No build record found for NVR {nvr!r} with record_id {self.record_id!r} "
                    f"(allow_failed={self.allow_failed})."
                )
            if len(records) > 1:
                raise ValueError(
                    f"Multiple build records matched NVR {nvr!r} with record_id {self.record_id!r}; "
                    "record_id should uniquely identify a record. Please verify the record_id."
                )
            return records[0]

        if not records:
            hint = ""
            if not self.allow_failed:
                hint = (
                    " If the build failed ITS/conforma, re-run with --allow-failed to include terminal failed builds."
                )
            raise ValueError(f"No build record found for NVR {nvr!r} (allow_failed={self.allow_failed}).{hint}")

        if len(records) > 1:
            candidates = "\n".join(
                f"  - record_id={record.record_id} outcome={record.outcome} "
                f"start_time={record.start_time} image_pullspec={record.image_pullspec}"
                for record in records
            )
            raise ValueError(
                f"Multiple build records found for NVR {nvr!r}:\n{candidates}\n"
                "Re-run with --record-id (RECORD_ID in Jenkins) set to the desired record. "
                "Refusing to pick one automatically."
            )

        return records[0]

    async def _sync_nvr(self, db: KonfluxDb, nvr: str) -> None:
        """Resolve a single NVR's build record, apply the embargo gate, and sync."""
        self._logger.info("Processing NVR %s", nvr)
        record = await self._resolve_record(db, nvr)

        # EMBARGO GATE: embargoed builds must NEVER be synced to this repo, regardless of allow_failed.
        if is_nvr_embargoed(nvr) or getattr(record, "embargoed", False):
            self._logger.warning("Skipping %s: NVR is embargoed and must not be synced", nvr)
            return

        # SAFETY GUARD: a failed build (e.g. build_error) may have no image to mirror.
        if not record.image_pullspec:
            raise ValueError(f"record {record.record_id} for {nvr} has no image_pullspec; cannot sync")

        # Only apply the build-specific image_tag (empty/omitted when the record has none).
        # sync_to_quay additionally adds the sha256-<digest> GC tag internally. We deliberately
        # do NOT apply a floating "<name>-<X.Y>" tag: it would be overwritten by any later build
        # of the same component, which is misleading in a pull-for-testing repo.
        tags = [record.image_tag]

        await sync_image_to_quay(
            record.image_pullspec,
            self.target_repo,
            tags,
            dry_run=self.runtime.dry_run,
            logger=self._logger,
        )
        self._logger.info("Done syncing %s", nvr)

    async def run(self) -> None:
        """Bind the Konflux DB and sync each configured NVR in turn.

        The source image may live in the RHCOS repo (art-rhcos-images), which uses a
        separate quay credential (RHCOS_QUAY_AUTH_FILE) than the default konflux creds
        (QUAY_AUTH_FILE). sync_to_quay runs ``oc image mirror --registry-config=$QUAY_AUTH_FILE``,
        so we build a merged auth file that covers both the source (RHCOS or konflux) and
        the destination (art-images-test), and point QUAY_AUTH_FILE at it for the sync calls.
        """
        self._logger.info("Starting sync-nvr-to-quay for %s NVR(s) -> %s", len(self.nvrs), self.target_repo)

        db = KonfluxDb()
        db.bind(KonfluxBuildRecord)

        quay_auth_file = os.environ.get("QUAY_AUTH_FILE")
        if not quay_auth_file:
            # No auth file configured (e.g. a local/dev invocation); keep legacy behavior and
            # do not attempt to build a merged registry config.
            for nvr in self.nvrs:
                await self._sync_nvr(db, nvr)
            self._logger.info("sync-nvr-to-quay completed")
            return

        # Merge the default konflux creds with the RHCOS creds (when available) so the mirror
        # can read an RHCOS source as well as a konflux source, and write to art-images-test.
        rhcos_quay_auth_file = os.environ.get("RHCOS_QUAY_AUTH_FILE")
        source_files = [quay_auth_file]
        registries = [KONFLUX_DEFAULT_IMAGE_REPO, KONFLUX_IMAGE_TEST_REPO]
        if rhcos_quay_auth_file and os.path.exists(rhcos_quay_auth_file):
            source_files.append(rhcos_quay_auth_file)
            registries.append(RHCOS_IMAGE_REPO)

        with RegistryConfig(source_files=source_files, registries=registries) as merged_auth_file:
            previous_auth_file = os.environ.get("QUAY_AUTH_FILE")
            os.environ["QUAY_AUTH_FILE"] = merged_auth_file
            try:
                for nvr in self.nvrs:
                    await self._sync_nvr(db, nvr)
            finally:
                if previous_auth_file is None:
                    os.environ.pop("QUAY_AUTH_FILE", None)
                else:
                    os.environ["QUAY_AUTH_FILE"] = previous_auth_file

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
@click.option(
    "--allow-failed",
    is_flag=True,
    default=False,
    help="Also consider terminal failed builds (e.g. its_error) in addition to successful ones. "
    "Embargoed builds are still never synced.",
)
@click.option(
    "--record-id",
    default="",
    help="Konflux build record_id, used to disambiguate when multiple records exist for one NVR.",
)
@pass_runtime
@click_coroutine
async def sync_nvr_to_quay_cli(runtime: Runtime, nvr: str, target_repo: str, allow_failed: bool, record_id: str):
    """CLI entrypoint for the sync-nvr-to-quay pipeline."""
    nvrs = [n.strip() for n in nvr.split(",") if n.strip()]
    pipeline = SyncNvrToQuayPipeline(
        runtime,
        nvrs=nvrs,
        target_repo=target_repo,
        allow_failed=allow_failed,
        record_id=record_id,
    )
    await pipeline.run()
