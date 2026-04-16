"""Validate Konflux OLM operator builds the same way bundle rebasing resolves manifest image refs."""

from __future__ import annotations

import datetime
import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, List, Tuple, cast

from artcommonlib import exectools
from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildRecord
from artcommonlib.olm import operator_image_refs as olm_image_refs
from doozerlib import constants as doozer_constants
from doozerlib.backend.build_repo import BuildRepo
from doozerlib.source_resolver import SourceResolution, SourceResolver

if TYPE_CHECKING:
    from elliottlib.imagecfg import ImageMetadata
    from elliottlib.runtime import Runtime

LOGGER = logging.getLogger(__name__)

OLM_FIND_BUILDS_CANDIDATE_LIMIT = 25


def _image_has_git_source(metadata: ImageMetadata) -> bool:
    try:
        return bool(metadata.config.content.source.git)
    except Exception:
        return False


def _source_resolver_for(runtime: Runtime) -> SourceResolver:
    return SourceResolver(
        sources_base_dir=str(runtime.working_dir),
        cache_dir=getattr(runtime, 'cache_dir', None),
        group_config=runtime.group_config,
        local=False,
        upcycle=False,
        stage=False,
    )


def _bundle_dir_yaml_files(operator_dir: Path, update_csv: dict) -> List[Tuple[str, str]]:
    manifests_dir = operator_dir.joinpath(str(update_csv['manifests-dir']))
    bundle_dir = manifests_dir.joinpath(str(update_csv['bundle-dir']))
    out: List[Tuple[str, str]] = []
    if not bundle_dir.is_dir():
        return out
    for src in sorted(bundle_dir.iterdir()):
        if src.name == 'image-references':
            continue
        if src.suffix.lower() not in ('.yaml', '.yml'):
            continue
        out.append((str(src.relative_to(operator_dir)), src.read_text(encoding='utf-8', errors='replace')))
    return out


async def validate_konflux_operator_build_manifest_refs(
    runtime: Runtime,
    image_meta: ImageMetadata,
    operator_build: KonfluxBuildRecord,
    *,
    image_repo: str | None = None,
    logger: logging.Logger | None = None,
) -> Tuple[bool, str | None]:
    """
    Return (True, None) if operator manifests at this build's ref are resolvable like bundle rebasing.

    Non-Konflux engine records are accepted without cloning (no validation).
    """
    log = logger or LOGGER
    if operator_build.engine is not Engine.KONFLUX:
        return True, None

    if not _image_has_git_source(image_meta):
        return False, 'no git source in image metadata'

    update_csv = image_meta.config.get('update-csv')
    if not update_csv or not update_csv.get('manifests-dir') or not update_csv.get('bundle-dir'):
        return False, 'incomplete update-csv config (manifests-dir / bundle-dir)'

    resolver = _source_resolver_for(runtime)
    source = cast(
        SourceResolution,
        await exectools.to_thread(resolver.resolve_source, image_meta, no_clone=True),
    )

    tmp_root = tempfile.mkdtemp(dir=str(runtime.working_dir))
    operator_dir = Path(tmp_root) / 'operator-checkout'
    try:
        repo = BuildRepo(
            url=source.url,
            branch=None,
            local_dir=operator_dir,
            logger=log,
        )
        await repo.ensure_source(upcycle=False)
        refspec = operator_build.rebase_commitish
        if not refspec:
            return False, 'operator build has empty rebase_commitish'
        await repo.fetch(refspec, strict=True)
        await repo.switch('FETCH_HEAD', detach=True)

        files = _bundle_dir_yaml_files(operator_dir, update_csv)
        if not files:
            return False, f'no YAML files under bundle-dir {update_csv.get("bundle-dir")!r}'

        repo_url = image_repo or doozer_constants.KONFLUX_DEFAULT_IMAGE_REPO
        ok, err = await olm_image_refs.validate_olm_bundle_dir_yaml_files(
            files,
            str(update_csv['registry']),
            Engine.KONFLUX,
            image_meta,
            runtime.group_config,
            repo_url,
            logger=log,
        )
        return ok, err
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def _konflux_completed_before(runtime: Runtime) -> datetime.datetime | None:
    be = runtime.assembly_basis_event
    if isinstance(be, datetime.datetime):
        return be
    return None


async def select_konflux_olm_operator_build_for_find_builds(
    runtime: Runtime,
    image_meta: ImageMetadata,
) -> Tuple[KonfluxBuildRecord, List[dict]]:
    """
    Pick the newest Konflux operator build whose bundle-dir manifests pass reference validation.

    Pinned ``is:`` components are never replaced with an older build; validation failure raises.

    Returns (chosen_build, skipped_invalid) where skipped_invalid entries are
    ``{'nvr': ..., 'reason': ...}`` for logging / JSON.
    """
    from elliottlib.exceptions import ElliottFatalError

    completed_before = _konflux_completed_before(runtime)
    candidates = await image_meta.list_konflux_success_build_candidates(
        el_target=image_meta.branch_el_target(),
        exclude_large_columns=True,
        limit=OLM_FIND_BUILDS_CANDIDATE_LIMIT,
        completed_before=completed_before,
    )
    if not candidates:
        raise ElliottFatalError(f"No Konflux builds found for OLM operator {image_meta.distgit_key}")

    skipped: List[dict] = []
    is_pinned = bool(image_meta.config.get('is'))

    if is_pinned:
        cand = candidates[0]
        ok, err = await validate_konflux_operator_build_manifest_refs(runtime, image_meta, cand, logger=LOGGER)
        if not ok:
            raise ElliottFatalError(
                f"Pinned OLM operator image {image_meta.distgit_key} build {cand.nvr} has invalid "
                f"manifest image references: {err}"
            )
        return cand, skipped

    for cand in candidates:
        ok, err = await validate_konflux_operator_build_manifest_refs(runtime, image_meta, cand, logger=LOGGER)
        if ok:
            if skipped:
                LOGGER.warning(
                    "OLM operator %s: using %s after skipping %d build(s) with invalid manifest references",
                    image_meta.distgit_key,
                    cand.nvr,
                    len(skipped),
                )
            return cand, skipped
        skipped.append({'nvr': cand.nvr, 'reason': err or 'unknown'})
        LOGGER.warning(
            "Skipping OLM operator build %s for %s: %s",
            cand.nvr,
            image_meta.distgit_key,
            err,
        )

    raise ElliottFatalError(
        f"No OLM operator build with resolvable manifest references for {image_meta.distgit_key} "
        f"within {len(candidates)} candidate(s)"
    )
