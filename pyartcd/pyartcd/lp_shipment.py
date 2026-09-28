"""Helpers for safely creating and reusing layered-product shipment merge requests.

This module owns the ``assembly.group.shipment.mr`` pointer in ``releases.yml``
and reconciles generated shipment files with an existing GitLab merge request.
On reuse, the previous layered-product shipment files are discarded and rebuilt
from the current release inputs.
"""

import asyncio
import logging
import re
from collections import Counter
from io import StringIO
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

from artcommonlib import exectools
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import new_roundtrip_yaml_handler
from elliottlib.shipment_model import ShipmentConfig
from elliottlib.shipment_utils import ShipmentMRCIState, inspect_shipment_mr_ci_state
from gitlab.exceptions import GitlabCreateError

from pyartcd.fbc_util import extract_ocp_version_from_nvr
from pyartcd.git import GitRepository

LOGGER = logging.getLogger(__name__)
YAML = new_roundtrip_yaml_handler()
_TIMESTAMP_RE = re.compile(r"(\d{14})$")
_PROD_RELEASE_LABEL_PREFIX = "prod-release"
_STAGE_RELEASE_SUCCESS_LABEL = "stage-release-success"
_MR_CREATION_ATTEMPTS = 3
_MR_CREATION_RETRY_DELAY_SECONDS = 5


class ShipmentMRValidationError(ValueError):
    """Indicate that a configured shipment MR is invalid or unrelated."""


class ShipmentMRScopeError(ShipmentMRValidationError):
    """Indicate that a shipment MR does not belong to the expected release scope."""


class ShipmentMRProductionError(ValueError):
    """Indicate that production history makes automated MR replacement unsafe."""


class ShipmentMRActiveStageError(ValueError):
    """Indicate that active stage work makes in-place MR reuse unsafe."""


async def create_shipment_mr_with_retry(source_project, attributes: dict, logger: logging.Logger) -> object:
    """Create a shipment MR after GitLab recognizes its newly pushed branch.

    GitLab can briefly return ``source_branch: does not exist`` immediately
    after accepting a new branch push. Retry only that known transient error;
    all other MR creation failures remain immediate and unchanged.

    Args:
        source_project: Python-gitlab project that owns the source branch.
        attributes: Merge request attributes passed to python-gitlab.
        logger: Logger used to report a transient retry.

    Returns:
        The newly created python-gitlab merge request object.

    Raises:
        GitlabCreateError: If MR creation fails for another reason or the
            source branch remains unavailable after all attempts.
    """
    source_branch = attributes.get('source_branch', '<unknown>')
    for attempt in range(1, _MR_CREATION_ATTEMPTS + 1):
        try:
            return source_project.mergerequests.create(attributes)
        except GitlabCreateError as exc:
            errors = exc.error_message if isinstance(exc.error_message, dict) else {}
            source_errors = errors.get('source_branch', [])
            missing_source_branch = exc.response_code == 400 and any(
                'does not exist' in str(error).lower() for error in source_errors
            )
            if not missing_source_branch or attempt == _MR_CREATION_ATTEMPTS:
                raise
            logger.warning(
                "GitLab has not recognized newly pushed branch %s yet; retrying MR creation (%d/%d)",
                source_branch,
                attempt + 1,
                _MR_CREATION_ATTEMPTS,
            )
            await asyncio.sleep(_MR_CREATION_RETRY_DELAY_SECONDS)

    raise AssertionError("Unreachable")


def close_superseded_shipment_mr(mr, dry_run: bool) -> None:
    """Close an open shipment MR before creating its forced replacement.

    Args:
        mr: Superseded GitLab merge request object.
        dry_run: Report the transition without saving it remotely.
    """
    if getattr(mr, 'state', None) != 'opened':
        return
    if dry_run:
        LOGGER.info("[DRY-RUN] Would close superseded shipment MR: %s", getattr(mr, 'web_url', 'unknown'))
        return
    mr.state_event = 'close'
    mr.save()
    mr.state = 'closed'


def add_superseded_mr_comment(
    gitlab_client,
    superseded_mr_url: str,
    replacement_mr_url: str,
    operation: str,
    run_url: str | None = None,
) -> None:
    """Record a successful forced replacement on the superseded MR.

    Comment failure is intentionally non-fatal because the authoritative
    replacement pointer has already been persisted in ``releases.yml``.

    Args:
        gitlab_client: Authenticated ART GitLab client.
        superseded_mr_url: Shipment MR that was replaced.
        replacement_mr_url: New authoritative shipment MR.
        operation: Release operation that performed the replacement.
        run_url: Optional Jenkins build or Tekton PipelineRun URL.
    """
    replacement_iid = replacement_mr_url.rstrip('/').rsplit('/', 1)[-1]
    replacement_label = f"shipment MR !{replacement_iid}" if replacement_iid.isdigit() else "replacement shipment MR"
    body = (
        "**DO NOT USE THIS MR FOR RELEASE.**\n\n"
        f"This shipment was superseded by [{replacement_label}]({replacement_mr_url}) during a forced replacement"
    )
    if run_url:
        body += f" via the [{operation} run]({run_url})."
    else:
        body += f" via `{operation}`."
    try:
        gitlab_client.add_mr_comment(superseded_mr_url, body)
    except Exception as exc:
        LOGGER.warning(
            "Failed to comment on superseded shipment MR %s: %s",
            superseded_mr_url,
            exc,
        )


def _project_path(url: str) -> str:
    """Extract a GitLab project path from a repository URL.

    Args:
        url: HTTPS or Git repository URL.

    Returns:
        The normalized ``namespace/project`` path.
    """
    return urlparse(url).path.strip('/').removesuffix('.git')


def get_shipment_mr_url(releases_config: dict, assembly: str) -> str | None:
    """Read the layered-product shipment MR pointer for an assembly.

    Args:
        releases_config: Parsed contents of ``releases.yml``.
        assembly: Assembly name to inspect.

    Returns:
        The configured shipment MR URL, or ``None`` when it is not present.
    """
    return (
        (releases_config or {})
        .get('releases', {})
        .get(assembly, {})
        .get('assembly', {})
        .get('group', {})
        .get('shipment', {})
        .get('mr')
    )


async def update_shipment_mr_url(
    repo: GitRepository,
    group: str,
    assembly: str,
    mr_url: str,
    expected_mr_url: str | None,
    *,
    create_as_stream: bool,
) -> bool:
    """Persist a layered-product shipment MR pointer safely.

    The group branch is fetched immediately before editing. The write proceeds
    only when the current pointer still equals ``expected_mr_url`` so that a
    concurrent release cannot be overwritten.

    Args:
        repo: Initialized ocp-build-data Git repository.
        group: Group branch containing ``releases.yml``.
        assembly: Assembly whose shipment pointer should be updated.
        mr_url: Shipment MR URL to store.
        expected_mr_url: Pointer value observed before the shipment work began.
        create_as_stream: Create a missing assembly as an explicit stream
            assembly. When false, the assembly must already exist.

    Returns:
        Whether the commit was created and pushed.

    Raises:
        RuntimeError: If the pointer changed concurrently or a required assembly
            disappeared.
    """
    await repo.fetch_switch_branch(group, remote="origin")
    releases_path = repo._directory / "releases.yml"
    releases_config = YAML.load(releases_path) if releases_path.exists() else None
    releases_config = releases_config or {}

    current_mr_url = get_shipment_mr_url(releases_config, assembly)
    if current_mr_url != expected_mr_url:
        raise RuntimeError(
            f"Shipment MR pointer changed concurrently from {expected_mr_url!r} to {current_mr_url!r}; "
            "refusing to overwrite it"
        )

    releases = releases_config.setdefault('releases', {})
    if not create_as_stream and assembly not in releases:
        raise RuntimeError(f"Assembly {assembly} disappeared before the shipment MR pointer could be stored")
    assembly_entry = releases.setdefault(assembly, {})
    assembly_def = assembly_entry.setdefault('assembly', {})
    if create_as_stream and not assembly_def:
        assembly_def['type'] = 'stream'
    shipment = assembly_def.setdefault('group', {}).setdefault('shipment', {})
    shipment['mr'] = mr_url
    YAML.dump(releases_config, releases_path)
    return await repo.commit_push(f"Update assembly {assembly}: add shipment MR URL", safe=True)


async def verify_shipment_mr_url(repo: GitRepository, group: str, assembly: str, expected_mr_url: str) -> None:
    """Confirm that ``releases.yml`` still points at the selected MR.

    Args:
        repo: Initialized ocp-build-data Git repository.
        group: Group branch containing ``releases.yml``.
        assembly: Assembly whose pointer should be checked.
        expected_mr_url: MR URL selected earlier in the release run.

    Raises:
        RuntimeError: If another process changed the pointer.
    """
    await repo.fetch_switch_branch(group, remote="origin")
    releases_path = repo._directory / "releases.yml"
    releases_config = YAML.load(releases_path) if releases_path.exists() else None
    current_mr_url = get_shipment_mr_url(releases_config or {}, assembly)
    if current_mr_url != expected_mr_url:
        raise RuntimeError(
            f"Shipment MR pointer changed concurrently from {expected_mr_url!r} to {current_mr_url!r}; "
            "refusing to update the old MR"
        )


def validate_shipment_mr_ci_state(
    gitlab_client,
    mr_url: str,
    mr,
    *,
    allow_active_stage: bool,
) -> ShipmentMRCIState:
    """Validate whether Shipment CI state permits reuse or replacement.

    Args:
        gitlab_client: Authenticated ART GitLab client.
        mr_url: URL of the shipment merge request.
        mr: Python-gitlab merge request object.
        allow_active_stage: Permit stage-only activity for ``--force``
            replacement. Normal in-place reuse must pass ``False``.

    Returns:
        The fully inspected Shipment CI state.

    Raises:
        ShipmentMRProductionError: If production was attempted.
        ShipmentMRActiveStageError: If stage is active during normal reuse.
        RuntimeError: If CI state cannot be determined reliably.
    """
    state = inspect_shipment_mr_ci_state(gitlab_client, mr_url, mr)
    if state.prod_attempts:
        raise ShipmentMRProductionError(
            "Shipment MR has production pipeline history and cannot be reused or automatically replaced: "
            f"{'; '.join(state.prod_attempts)}. Manual release recovery is required."
        )
    if state.active_stage and not allow_active_stage:
        raise ShipmentMRActiveStageError(
            "Shipment MR still has active stage work and cannot be reused in place: "
            f"{'; '.join(state.active_stage)}. Wait for stage to finish or use --force to create a replacement MR."
        )
    return state


def validate_shipment_mr(
    gitlab_client,
    mr_url: str,
    pull_url: str,
    push_url: str,
    *,
    allowed_states: tuple[str, ...] = ('opened',),
):
    """Validate a shipment MR referenced by ``releases.yml``.

    Args:
        gitlab_client: Authenticated ART GitLab client.
        mr_url: Referenced shipment MR URL.
        pull_url: Configured canonical shipment-data repository URL.
        push_url: Configured shipment-data push repository URL.
        allowed_states: MR states accepted by the requested operation. Normal
            reuse accepts only ``opened``; replacement inspection also accepts
            ``closed``.

    Returns:
        The GitLab merge request object when it satisfies the requested state
        and repository constraints.

    Raises:
        ShipmentMRValidationError: If the MR is missing, has a disallowed state,
            points to the wrong project or target branch, or originates from the
            wrong push repository.
        ShipmentMRProductionError: If the MR is merged or has a production
            release label.
    """
    if urlparse(mr_url).netloc != urlparse(pull_url).netloc:
        raise ShipmentMRValidationError(
            f"Shipment MR host {urlparse(mr_url).netloc} does not match {urlparse(pull_url).netloc}. "
            "Use --force to create a replacement MR."
        )
    target_project_path, _ = gitlab_client._parse_mr_url(mr_url)
    mr = gitlab_client.get_mr_from_url(mr_url)
    if not mr:
        raise ShipmentMRValidationError(f"Shipment MR {mr_url} was not found. Use --force to create a replacement MR.")
    if mr.state == 'merged':
        raise ShipmentMRProductionError(
            f"Shipment MR {mr_url} is merged and cannot be reused or automatically replaced. Manual recovery is required."
        )
    if mr.state not in allowed_states:
        raise ShipmentMRValidationError(
            f"Shipment MR state is {mr.state}, not one of {allowed_states}. Use --force to create a replacement MR."
        )
    if target_project_path != _project_path(pull_url):
        raise ShipmentMRValidationError(
            f"Shipment MR target project {target_project_path} does not match {_project_path(pull_url)}. "
            "Use --force to create a replacement MR."
        )
    source_project_path = gitlab_client.get_project(mr.source_project_id).path_with_namespace
    if source_project_path != _project_path(push_url):
        raise ShipmentMRValidationError(
            f"Shipment MR source project {source_project_path} does not match {_project_path(push_url)}. "
            "Use --force to create a replacement MR."
        )
    if mr.target_branch != "main":
        raise ShipmentMRValidationError(
            f"Shipment MR target branch is {mr.target_branch}, not main. Use --force to create a replacement MR."
        )
    prod_labels = sorted(
        label for label in (getattr(mr, 'labels', None) or []) if label.lower().startswith(_PROD_RELEASE_LABEL_PREFIX)
    )
    if prod_labels:
        raise ShipmentMRProductionError(
            f"Shipment MR has production release label(s) {prod_labels} and must not be modified. "
            "Automated replacement is disabled after a production attempt; manual recovery is required."
        )
    return mr


def set_shipment_mr_draft(mr, dry_run: bool) -> None:
    """Reset stage status and mark a reused MR as draft.

    For normal reuse, the success label describes the previous shipment files
    and is removed before those files are replaced.

    Args:
        mr: GitLab merge request object to update.
        dry_run: Logically perform the transition without saving it remotely.
    """
    changed = False
    if not mr.title.startswith("Draft:"):
        mr.title = f"Draft: {mr.title}"
        changed = True
    labels = list(getattr(mr, 'labels', None) or [])
    if _STAGE_RELEASE_SUCCESS_LABEL in labels:
        labels.remove(_STAGE_RELEASE_SUCCESS_LABEL)
        mr.labels = labels
        changed = True
    if changed and not dry_run:
        mr.save()


def _to_dict(config: ShipmentConfig) -> dict:
    """Convert a shipment model to the mapping written to YAML."""
    return config.model_dump(exclude_unset=True, exclude_none=True)


def _identity(config: dict) -> tuple:
    """Build the stable semantic identity for a shipment configuration.

    FBC identities include the component and target OCP version so multiple
    operators and multiple OCP targets remain independently addressable.

    Args:
        config: Parsed shipment configuration.

    Returns:
        A tuple identifying the logical shipment independently of its filename.

    Raises:
        ValueError: If an FBC shipment does not contain exactly one FBC NVR.
    """
    shipment = config.get('shipment', {})
    metadata = shipment.get('metadata', {})
    base = (
        metadata.get('product'),
        metadata.get('group'),
        metadata.get('assembly'),
        metadata.get('application'),
        bool(metadata.get('fbc', False)),
    )
    if not base[-1]:
        return base

    nvrs = shipment.get('snapshot', {}).get('nvrs', [])
    if len(nvrs) != 1:
        raise ValueError(f"Expected one NVR in an FBC shipment, found {len(nvrs)}")
    nvr = nvrs[0]
    component = parse_nvr(nvr)['name']
    return (*base, component, extract_ocp_version_from_nvr(nvr))


def _identity_sort_key(item: tuple) -> tuple[str, ...]:
    """Return a deterministic ordering key for a shipment identity item."""
    return tuple("" if value is None else str(value) for value in item[0])


def _shipment_path_matches(
    path: str,
    group: str,
    assembly: str,
    product: str | None = None,
) -> bool:
    """Determine whether a shipment path belongs to a release scope.

    Path-based ownership lets a rerun remove a previously generated file even
    when its YAML metadata was edited or damaged manually.

    Args:
        path: Repository-relative shipment file path.
        group: Expected layered-product group.
        assembly: Expected layered-product assembly.
        product: Optional expected shipment product.

    Returns:
        Whether the path belongs to the requested release scope.
    """
    parts = Path(path).parts
    if len(parts) < 6 or parts[0] != 'shipment':
        return False
    if product is not None and parts[1] != product:
        return False
    return parts[2] == group and parts[-1].startswith(f"{assembly}.") and parts[-1].endswith(('.yaml', '.yml'))


async def validate_shipment_mr_reuse_state(
    repo: GitRepository,
    mr,
    product: str,
    group: str,
    assembly: str,
) -> None:
    """Validate the release scope and reject production-completed MR reuse.

    The GitLab success label is checked by :func:`validate_shipment_mr`. This
    additional content check protects against a missing label or a partially
    completed labeling job by inspecting both image advisory information and
    FBC pipeline results.

    Args:
        repo: Initialized shipment-data repository.
        mr: Open GitLab merge request proposed for reuse.
        product: Layered product expected in the shipment files.
        group: Layered-product group expected in the shipment files.
        assembly: Layered-product assembly expected in the shipment files.

    Raises:
        ValueError: If the MR does not contain the expected release scope or a
            matching shipment file records production release data.
        RuntimeError: If GitLab truncates the MR change list.
    """
    await repo.fetch_switch_branch(mr.source_branch, remote="origin")
    change_data = mr.changes()
    if change_data.get('overflow'):
        raise RuntimeError("GitLab truncated the shipment MR change list; refusing an incomplete validation")

    matching_files = []
    shipment_paths = []
    for change in change_data.get('changes', []):
        path = change['new_path']
        if _shipment_path_matches(path, group, assembly):
            shipment_paths.append(path)
        if not _shipment_path_matches(path, group, assembly, product=product):
            continue
        absolute_path = repo._directory / path
        if not absolute_path.exists():
            continue
        matching_files.append(path)
        config = YAML.load(absolute_path)
        if not isinstance(config, dict) or 'shipment' not in config:
            raise ValueError(f"Cannot safely determine production release state from malformed shipment file {path}")
        shipment = config['shipment']
        prod = shipment.get('environments', {}).get('prod', {}) or {}
        advisory = prod.get('advisory')
        pipeline = (prod.get('result') or {}).get('pipeline')
        if advisory or pipeline:
            markers = []
            if advisory:
                markers.append('prod advisory')
            if pipeline:
                markers.append('prod pipeline result')
            raise ShipmentMRProductionError(
                f"Shipment MR file {path} contains {' and '.join(markers)} and must not be modified. "
                "Automated replacement is disabled after a production attempt; manual recovery is required."
            )

    if not matching_files:
        found = f" Found candidate files: {sorted(shipment_paths)}." if shipment_paths else ""
        raise ShipmentMRScopeError(
            f"Shipment MR does not contain shipment files for product {product!r}, group {group!r}, "
            f"and assembly {assembly!r}; refusing to modify an unrelated MR.{found} "
            "Correct the assembly shipment.mr pointer or use --force to create a replacement MR."
        )


async def validate_shipment_mr_for_operation(
    gitlab_client,
    repo: GitRepository,
    mr_url: str,
    pull_url: str,
    push_url: str,
    product: str,
    group: str,
    assembly: str,
    *,
    allowed_states: tuple[str, ...],
    allow_active_stage: bool,
) -> tuple[object, ShipmentMRCIState]:
    """Validate a layered-product shipment MR and its complete safety state.

    Args:
        gitlab_client: Authenticated ART GitLab client.
        repo: Initialized shipment-data repository.
        mr_url: URL referenced by the assembly in ``releases.yml``.
        pull_url: Configured canonical shipment-data repository URL.
        push_url: Configured shipment-data push repository URL.
        product: Expected layered-product name.
        group: Expected layered-product group.
        assembly: Expected layered-product assembly.
        allowed_states: MR states accepted by the requested operation.
        allow_active_stage: Permit an isolated ``--force`` replacement while
            stage work on the previous MR continues.

    Returns:
        The validated MR and its inspected Shipment CI state.

    Raises:
        ShipmentMRValidationError: If the MR is invalid or unrelated.
        ShipmentMRProductionError: If the MR is merged or production was
            attempted.
        ShipmentMRActiveStageError: If normal reuse encounters active stage
            work.
        RuntimeError: If GitLab state cannot be determined reliably.
    """
    mr = validate_shipment_mr(
        gitlab_client,
        mr_url,
        pull_url,
        push_url,
        allowed_states=allowed_states,
    )
    await validate_shipment_mr_reuse_state(repo, mr, product, group, assembly)
    state = validate_shipment_mr_ci_state(
        gitlab_client,
        mr_url,
        mr,
        allow_active_stage=allow_active_stage,
    )
    return mr, state


async def _restore_from_main(repo: GitRepository, path: str) -> None:
    """Restore a stale MR file to its content on the target branch.

    Args:
        repo: Checked-out shipment-data repository.
        path: Repository-relative shipment file path.

    Raises:
        RuntimeError: If the file cannot be read from ``main``.
    """
    rc, content, error = await exectools.cmd_gather_async(
        ["git", "-C", str(repo._directory), "show", f"main:{path}"], env=repo._local_env()
    )
    if rc:
        raise RuntimeError(f"Unable to restore {path} from main: {error}")
    await repo.write_file(path, content)


async def reconcile_shipment_mr(
    repo: GitRepository,
    mr,
    shipments_by_kind: Dict[str, ShipmentConfig],
    *,
    include_fbc_ocp_version: bool,
    dry_run: bool,
) -> bool:
    """Replace a reusable MR's layered-product shipment files from scratch.

    Every MR-owned file for the generated product, group, and assembly is
    removed (or restored from ``main``), then the current shipment set is
    written with deterministic filenames. No content or CI mutation from the
    previous files is carried into the replacement.

    Args:
        repo: Initialized shipment-data repository.
        mr: Validated GitLab merge request to update.
        shipments_by_kind: Current generated shipment models keyed by kind.
        include_fbc_ocp_version: Include the target OCP version in new FBC
            filenames.
        dry_run: Prepare and display changes without committing or pushing.

    Returns:
        Whether reconciliation produced content changes. A no-change rerun
        returns ``False`` and is considered successful.

    Raises:
        ValueError: If the MR branch lacks a timestamp or desired files contain
            duplicate semantic identities or scopes.
        RuntimeError: If GitLab truncates the MR change list or a stale file
            cannot be restored safely.
    """
    timestamp_match = _TIMESTAMP_RE.search(mr.source_branch)
    if not timestamp_match:
        raise ValueError(f"Cannot determine shipment timestamp from MR branch {mr.source_branch}")
    timestamp = timestamp_match.group(1)

    desired_items = [(kind, _to_dict(config)) for kind, config in shipments_by_kind.items()]
    desired_identities = [_identity(config) for _, config in desired_items]
    duplicates = [identity for identity, count in Counter(desired_identities).items() if count > 1]
    if duplicates:
        raise ValueError(f"Generated shipment configurations contain duplicate identities: {duplicates}")
    desired_by_identity = {identity: item for identity, item in zip(desired_identities, desired_items)}
    desired_scopes = {identity[:3] for identity in desired_identities}
    if len(desired_scopes) != 1:
        raise ValueError(f"Generated shipment configurations span multiple scopes: {sorted(desired_scopes)}")
    desired_scope = next(iter(desired_scopes))

    await repo.fetch_switch_branch(mr.source_branch, remote="origin")
    change_data = mr.changes()
    if change_data.get('overflow'):
        raise RuntimeError("GitLab truncated the shipment MR change list; refusing an incomplete reconciliation")
    changes = change_data.get('changes', [])
    existing_files: list[tuple[str, bool]] = []
    for change in changes:
        path = change['new_path']
        if not _shipment_path_matches(path, desired_scope[1], desired_scope[2], product=desired_scope[0]):
            continue
        absolute_path = repo._directory / path
        if not absolute_path.exists():
            continue
        is_new = change.get('new_file') is True or change.get('new_file') == 'true'
        existing_files.append((path, is_new))

    for path, is_new in existing_files:
        if is_new:
            (repo._directory / path).unlink()
        else:
            await _restore_from_main(repo, path)

    next_counter = 1
    for identity, (kind, desired) in sorted(desired_by_identity.items(), key=_identity_sort_key):
        metadata = desired['shipment']['metadata']
        target_dir = Path('shipment') / metadata['product'] / metadata['group'] / metadata['application'] / 'prod'
        if metadata.get('fbc'):
            ocp_part = f".ocp{identity[-1]}" if include_fbc_ocp_version and identity[-1] else ""
            filename = f"{metadata['assembly']}.fbc{ocp_part}.{timestamp}{next_counter:02d}.yaml"
            next_counter += 1
        else:
            filename = f"{metadata['assembly']}.{kind.rstrip('0123456789')}.{timestamp}.yaml"
        out = StringIO()
        YAML.dump(desired, out)
        path = target_dir / filename
        (repo._directory / path).parent.mkdir(parents=True, exist_ok=True)
        await repo.write_file(path, out.getvalue())

    await repo.log_diff()
    if dry_run:
        return True
    return await repo.commit_push(f"Update shipment configurations for {desired_identities[0][2]}", safe=True)
