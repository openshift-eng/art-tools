import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

from artcommonlib.assembly import assembly_config_struct
from artcommonlib.constants import SHIPMENT_CONFIG_KINDS
from artcommonlib.gitlab import GitLabClient
from artcommonlib.jira_config import JIRA_DOMAIN_NAME
from artcommonlib.model import Model
from artcommonlib.util import new_roundtrip_yaml_handler
from errata_tool import Erratum

from elliottlib.shipment_model import Issue, Issues, ReleaseNotes, ShipmentConfig

logger = logging.getLogger(__name__)

yaml = new_roundtrip_yaml_handler()

_ACTIVE_CI_STATUSES = frozenset(
    {
        'created',
        'waiting_for_resource',
        'preparing',
        'pending',
        'running',
        'scheduled',
        'canceling',
    }
)
_TERMINAL_CI_STATUSES = frozenset({'success', 'failed', 'canceled', 'skipped', 'manual'})
_UNTOUCHED_PROD_STATUSES = frozenset({'created', 'manual', 'skipped'})


@dataclass(frozen=True)
class ShipmentMRCIState:
    """Summarize Shipment CI state relevant to release safety.

    Attributes:
        active_stage: Descriptions of active MR, stage bridge, or downstream
            stage jobs.
        prod_attempts: Descriptions proving that production was attempted.
        active_prod: Descriptions of active production bridges, downstream
            pipelines, or downstream jobs.
    """

    active_stage: tuple[str, ...]
    prod_attempts: tuple[str, ...]
    active_prod: tuple[str, ...] = ()


@dataclass(frozen=True)
class ShipmentConfigRecord:
    """A validated shipment configuration and its repository path.

    Attributes:
        path: Path of the shipment YAML file in its repository.
        config: Validated shipment configuration.
    """

    path: str
    config: ShipmentConfig


def _object_value(item, name: str, default=None):
    """Read a field from a python-gitlab object or API response mapping.

    Args:
        item: Python object or mapping returned by the GitLab API.
        name: Field name to read.
        default: Value returned when the field is absent.

    Returns:
        The field value, or ``default`` when it is absent.
    """
    if isinstance(item, dict):
        return item.get(name, default)
    return getattr(item, name, default)


def _checked_ci_status(item, context: str) -> str:
    """Return a recognized GitLab CI status or fail closed.

    Args:
        item: Python-gitlab object or response mapping containing ``status``.
        context: Human-readable pipeline or job description.

    Returns:
        The normalized GitLab CI status.

    Raises:
        RuntimeError: If the status is absent or unknown.
    """
    status = _object_value(item, 'status')
    if status not in _ACTIVE_CI_STATUSES | _TERMINAL_CI_STATUSES:
        raise RuntimeError(f"Cannot safely classify {context}: unknown GitLab CI status {status!r}")
    return status


def inspect_shipment_mr_ci_state(gitlab_client, mr_url: str, mr) -> ShipmentMRCIState:
    """Inspect all Shipment CI pipelines belonging to a merge request.

    Parent pipeline state, stage and production trigger bridges, and their
    downstream jobs are inspected with pagination enabled. A production bridge
    is considered attempted once it leaves the untouched ``manual`` or
    ``skipped`` states, or as soon as GitLab associates a downstream pipeline.

    Args:
        gitlab_client: Authenticated ART GitLab client.
        mr_url: URL of the shipment merge request.
        mr: Python-gitlab merge request object.

    Returns:
        Active stage work, production attempts, and active production work.

    Raises:
        RuntimeError: If GitLab returns incomplete or unrecognized pipeline
            state. Callers must fail closed.
    """
    project_path, _ = gitlab_client._parse_mr_url(mr_url)
    project = gitlab_client.get_project(project_path)
    active_stage = []
    prod_attempts = []
    active_prod = []

    mr_pipelines = mr.pipelines.list(get_all=True)
    for mr_pipeline in mr_pipelines:
        pipeline_id = _object_value(mr_pipeline, 'id')
        if pipeline_id is None:
            raise RuntimeError("Cannot safely inspect shipment MR CI state: a pipeline has no ID")
        pipeline = project.pipelines.get(pipeline_id)
        pipeline_url = _object_value(pipeline, 'web_url', f'pipeline {pipeline_id}')
        pipeline_status = _checked_ci_status(pipeline, f'MR pipeline {pipeline_url}')
        bridges = pipeline.bridges.list(get_all=True)
        stage_bridge_found = False

        for bridge in bridges:
            bridge_name = _object_value(bridge, 'name')
            if bridge_name not in {'stage-job', 'prod-job'}:
                continue
            bridge_status = _checked_ci_status(bridge, f'{bridge_name} in {pipeline_url}')
            downstream = _object_value(bridge, 'downstream_pipeline')
            is_prod = bridge_name == 'prod-job'

            if is_prod:
                if bridge_status not in _UNTOUCHED_PROD_STATUSES or downstream:
                    prod_attempts.append(f"{pipeline_url} prod-job is {bridge_status}")
                if bridge_status in _ACTIVE_CI_STATUSES:
                    active_prod.append(f"{pipeline_url} prod-job is {bridge_status}")
            else:
                stage_bridge_found = True
                if bridge_status in _ACTIVE_CI_STATUSES:
                    active_stage.append(f"{pipeline_url} stage-job is {bridge_status}")

            if not downstream:
                continue
            downstream_id = _object_value(downstream, 'id')
            downstream_project_id = _object_value(downstream, 'project_id', _object_value(project, 'id'))
            if downstream_id is None or downstream_project_id is None:
                raise RuntimeError(
                    f"Cannot safely inspect {pipeline_url} {bridge_name}: downstream pipeline identification is incomplete"
                )
            downstream_project = (
                project
                if downstream_project_id == _object_value(project, 'id')
                else gitlab_client.get_project(downstream_project_id)
            )
            downstream_pipeline = downstream_project.pipelines.get(downstream_id)
            downstream_url = _object_value(downstream_pipeline, 'web_url', f'pipeline {downstream_id}')
            environment = 'production' if is_prod else 'stage'
            downstream_status = _checked_ci_status(
                downstream_pipeline, f'downstream {environment} pipeline {downstream_url}'
            )
            active_descriptions = active_prod if is_prod else active_stage
            if downstream_status in _ACTIVE_CI_STATUSES:
                active_descriptions.append(f"downstream {environment} pipeline {downstream_url} is {downstream_status}")

            for job in downstream_pipeline.jobs.list(get_all=True, include_retried=True):
                job_name = _object_value(job, 'name', 'unknown job')
                job_status = _checked_ci_status(job, f'{job_name} in {downstream_url}')
                if job_status in _ACTIVE_CI_STATUSES:
                    active_descriptions.append(f"{downstream_url} job {job_name!r} is {job_status}")

        # A ready MR pipeline can still be validating or generating its dynamic
        # configuration before GitLab exposes the stage trigger bridge.
        if pipeline_status in _ACTIVE_CI_STATUSES and not stage_bridge_found:
            active_stage.append(f"{pipeline_url} is {pipeline_status} before its stage job is available")

    return ShipmentMRCIState(
        active_stage=tuple(sorted(set(active_stage))),
        prod_attempts=tuple(sorted(set(prod_attempts))),
        active_prod=tuple(sorted(set(active_prod))),
    )


# Single source of truth for the public errata URL.
# verify_docs_approval.py defines the same constant; import from here once that module lands.
PUBLIC_ERRATA_URL = "https://access.redhat.com/errata"

# ---------------------------------------------------------------------------
# Skip-stage sentinel (EXTRAORDINARY USE ONLY)
# ---------------------------------------------------------------------------
# ``releasePlan: Skipped`` (any ASCII case) on a *stage* environment bypasses
# Konflux stage release and stage CDN publish. Use only with explicit team
# approval. When setting this in ocp-shipment-data config.yaml, leave a caution
# comment at the call site. Prod must never use this sentinel.
#
# The product (shipment.metadata.product / ocp-build-data group ``product``)
# must also appear in SKIP_STAGE_ALLOWED_PRODUCTS — config comments alone are
# not sufficient.
SKIPPED_RELEASE_PLAN = "Skipped"
STAGE_RELEASE_SKIPPED_LABEL = "stage-release-skipped"
# Orange / apricot — distinct from the blue *-release-success project labels.
STAGE_RELEASE_SKIPPED_LABEL_COLOR = "#ED9121"

# Products allowed to use stage releasePlan: Skipped. Keep in sync with the
# duplicate list in shipment-ci pipelines/generate_ci_files.py.
SKIP_STAGE_ALLOWED_PRODUCTS = frozenset(
    {
        "openshift_agent_installer",  # OVE ISO (installer-ove-ui)
    }
)


def is_release_plan_skipped(release_plan: str | None) -> bool:
    """Return True if releasePlan is the skip-stage sentinel (case-insensitive 'skipped').

    EXTRAORDINARY USE ONLY. Skipping stage bypasses Konflux stage validation and CDN
    stage publish. Use only with explicit team approval. When setting this in
    ocp-shipment-data config.yaml, leave a caution comment at the call site.

    Prod releasePlan must never be this sentinel; callers must reject that case.
    The product must also be listed in SKIP_STAGE_ALLOWED_PRODUCTS — see
    ``assert_product_may_skip_stage``.
    """
    return bool(release_plan) and release_plan.strip().casefold() == "skipped"


def assert_product_may_skip_stage(product: str | None) -> None:
    """Raise ValueError unless product is whitelisted for skip-stage.

    EXTRAORDINARY USE ONLY. Call this whenever stage releasePlan is the skip
    sentinel so non-approved products cannot opt out of stage via config alone.
    """
    if product in SKIP_STAGE_ALLOWED_PRODUCTS:
        return
    allowed = ", ".join(sorted(SKIP_STAGE_ALLOWED_PRODUCTS)) or "(none)"
    raise ValueError(
        f"Product {product!r} is not allowed to use stage releasePlan: {SKIPPED_RELEASE_PLAN}. "
        f"Only whitelisted products may skip stage: {allowed}. "
        f"Add the product to SKIP_STAGE_ALLOWED_PRODUCTS in elliottlib.shipment_utils "
        f"(and the duplicate list in shipment-ci) after team approval."
    )


def strip_advisory_cross_reference(text: str, rpm_name: str) -> str:
    """
    Remove the RPM advisory cross-reference block from advisory or shipment YAML text.

    Strips the lead-in sentence ("See the following advisory for the RPM packages...")
    together with the following URL paragraph from the text. Collapses any triple-newlines
    left behind into a single blank line.

    Handles two layouts:
    - Sentence appended to a prior paragraph: "prior text. See the following...\n\nhttps://..."
    - Sentence as its own paragraph: "prior text\n\nSee the following...\n\nhttps://..."

    Arg(s):
        text (str): freeform advisory/release-notes text to process.
        rpm_name (str): the RPM advisory's full display name, e.g. "RHBA-2026:44227".
    Return Value(s):
        str: text with the cross-reference block removed.
    """
    escaped_url = re.escape(f"{PUBLIC_ERRATA_URL}/{rpm_name}")
    # Strip "See the following advisory for RPM packages..." + blank line + URL line.
    # " ?" matches the leading space when the sentence is attached to a prior sentence
    # (" See the following...") without consuming the preceding period.
    # "[ \t]*" before the URL handles YAML literal-block indentation (the URL line
    # is indented when the text lives inside a YAML file rather than an ET advisory).
    pattern = (
        r" ?See the following advisory for (?:the )?RPM packages[^\n]*\n"
        r"[ \t]*\n"
        rf"[ \t]*{escaped_url}[^\n]*"
    )
    new_text = re.sub(pattern, "", text)
    # Fallback: if the URL is still present (unusual format), strip just the URL paragraph.
    if f"{PUBLIC_ERRATA_URL}/{rpm_name}" in new_text:
        new_text = re.sub(rf"\n[ \t]*\n[ \t]*{escaped_url}[^\n]*", "", new_text)
    # Collapse triple-newlines produced by removal.
    new_text = re.sub(r"\n{3,}", "\n\n", new_text)
    return new_text


def strip_et_advisory_rpm_reference(advisory_num: int, rpm_name: str, dry_run: bool = False) -> bool:
    """
    Load an ET advisory, strip the RPM cross-reference from description/solution, and commit.

    Soft-fails on Erratum load/commit errors (logs a warning, returns False).

    Arg(s):
        advisory_num (int): Numeric Errata Tool advisory ID.
        rpm_name (str): the RPM advisory's full display name, e.g. "RHBA-2026:44227".
        dry_run (bool): When True, log what would change but do not commit.
    Return Value(s):
        bool: True if the advisory text was (or would have been) changed.
    """
    try:
        advisory = Erratum(errata_id=advisory_num)
    except Exception as ex:
        logger.warning("Failed to load ET advisory %s: %s", advisory_num, ex)
        return False

    updates = {}
    for field in ("description", "solution"):
        text = getattr(advisory, field, None)
        if not text:
            continue
        new_text = strip_advisory_cross_reference(text, rpm_name)
        if new_text != text:
            updates[field] = new_text

    if not updates:
        return False
    if dry_run:
        logger.info("[DRY-RUN] Would strip RPM reference from ET advisory %s: %s", advisory_num, list(updates))
        return True
    try:
        advisory.update(**updates)
        advisory.commit()
        logger.info("Stripped RPM reference from ET advisory %s: %s", advisory_num, list(updates))
        return True
    except Exception as ex:
        logger.warning("Failed to commit ET advisory %s after stripping RPM reference: %s", advisory_num, ex)
        return False


def patch_et_advisory_text(
    advisory_num: int,
    format_dict: dict[str, str],
    dry_run: bool = False,
    validate_targets: tuple[str, ...] = (),
) -> list[str]:
    """
    Load an Errata Tool advisory, substitute placeholders in description/solution, and commit.

    Soft-fails on Erratum update/commit errors: logs a warning and returns without raising.
    If the advisory cannot be loaded at all, validation is also skipped (the text is
    unreadable) — callers that need guaranteed validation should treat an all-empty return
    from a known-placeholder advisory as suspicious when ET errors are present.

    Arg(s):
        advisory_num (int): Numeric Errata Tool advisory ID.
        format_dict (dict[str, str]): Mapping of placeholder name → replacement value, e.g.
            ``{"IMAGE_ADVISORY": "RHBA-2025:13660"}``.  Only entries with non-empty values
            should be included; the caller is responsible for filtering.
        dry_run (bool): When True, log what would change but do not call update/commit.
        validate_targets (tuple[str, ...]): Placeholder names to scan for in the advisory
            text *before* substitution.  Any that appear in the text but have no corresponding
            entry in ``format_dict`` are returned as human-readable strings so the caller can
            raise or warn.  Pass ``()`` to skip validation (default).
    Return Value(s):
        list[str]: Descriptions of placeholders from ``validate_targets`` that were found in
            the advisory text but could not be substituted (no value in ``format_dict``).
            Empty when ``validate_targets`` is ``()`` or all targets were resolved.
    """
    unresolved: list[str] = []
    try:
        advisory = Erratum(errata_id=advisory_num)
    except Exception as ex:
        # If the advisory cannot be loaded at all, neither substitution nor placeholder
        # validation is possible. Log and return empty — caller is responsible for deciding
        # whether this is fatal.
        logger.warning("Failed to load ET advisory %s: %s", advisory_num, ex)
        return unresolved

    updates = {}
    for field in ("description", "solution"):
        text = getattr(advisory, field, None)
        if not text:
            continue
        new_text = text
        for var_name, value in format_dict.items():
            new_text = new_text.replace(f"{{{var_name}}}", value)
        if new_text != text:
            updates[field] = new_text
        for target in validate_targets:
            if f"{{{target}}}" in text and target not in format_dict:
                unresolved.append(f"ET advisory {advisory_num} ({field}): {{{target}}}")

    if not updates:
        return unresolved
    if dry_run:
        logger.info("[DRY-RUN] Would patch ET advisory %s: %s", advisory_num, list(updates))
        return unresolved
    try:
        advisory.update(**updates)
        advisory.commit()
        logger.info("Patched ET advisory %s: %s", advisory_num, list(updates))
    except Exception as ex:
        logger.warning("Failed to commit ET advisory %s: %s", advisory_num, ex)
    return unresolved


def get_shipment_config_records_from_mr(
    mr_url: str,
    kinds: Tuple[str, ...] | None = SHIPMENT_CONFIG_KINDS,
    group: str | None = None,
    product: str | None = None,
    environment: str | None = None,
) -> list[ShipmentConfigRecord]:
    """Fetch validated shipment configuration records from a merge request.

    Path filters are applied before file contents are fetched. When ``product``
    is supplied, the path and parsed metadata must agree so callers cannot
    mistake another product's shipment for the requested one.

    Args:
        mr_url: URL of the merge request.
        kinds: Shipment kinds to include. ``None`` includes every shipment
            YAML path, including binary and product-specific kinds.
        group: Optional exact group path segment.
        product: Optional exact product path segment and metadata value.
        environment: Optional environment path segment, such as ``prod``.

    Returns:
        Matching path-aware shipment configuration records.

    Raises:
        ValueError: If a matching path disagrees with parsed shipment metadata.
    """
    records: list[ShipmentConfigRecord] = []
    gl = GitLabClient.from_url(mr_url)
    mr = gl.get_mr_from_url(mr_url)
    source_project = gl.get_project(mr.source_project_id)

    diff_versions = mr.diffs.list(all=True)
    if not diff_versions:
        return records
    diff = mr.diffs.get(diff_versions[0].id)
    for file_diff in diff.diffs:
        file_path = file_diff.get('new_path') or file_diff.get('old_path')
        if not file_path or not file_path.endswith(('.yaml', '.yml')):
            continue

        path_parts = file_path.split('/')
        if product or group or environment:
            if len(path_parts) < 4 or path_parts[0] != "shipment":
                continue
            if product and path_parts[1] != product:
                continue
            if group and path_parts[2] != group:
                continue
            if environment and environment not in path_parts[3:]:
                continue

        filename = path_parts[-1]
        parts = filename.replace('.yaml', '').replace('.yml', '')
        if kinds is not None and not any(kind in parts for kind in kinds):
            continue

        file_content = source_project.files.get(file_path, mr.source_branch)
        content = file_content.decode().decode('utf-8')
        yaml_data = Model(yaml.load(content)).primitive()
        shipment_config = ShipmentConfig(**yaml_data)
        if product and shipment_config.shipment.metadata.product != product:
            raise ValueError(
                f"Shipment path {file_path} belongs to product {product!r}, but metadata declares "
                f"{shipment_config.shipment.metadata.product!r}"
            )
        records.append(ShipmentConfigRecord(path=file_path, config=shipment_config))
    return records


def get_shipment_configs_from_mr(
    mr_url: str,
    kinds: Tuple[str, ...] = SHIPMENT_CONFIG_KINDS,
    group: str | None = None,
) -> Dict[str, ShipmentConfig]:
    """
    Fetch shipment configs from a merge request URL.

    Arg(s):
        mr_url (str): URL of the merge request.
        kinds (Tuple[str, ...]): Possible advisory kinds to fetch shipment configs for.
        group (str | None): When provided, only files whose path has this group
            at position 2 (shipment/{product}/{group}/...) are parsed. Skips
            non-matching files entirely, avoiding parse errors on unrelated products.

    Return Value(s):
        Dict[str, ShipmentConfig]: Dict of {kind: ShipmentConfig}.
    """

    shipment_configs: Dict[str, ShipmentConfig] = {}
    for record in get_shipment_config_records_from_mr(mr_url, kinds=kinds, group=group):
        filename = record.path.split('/')[-1]
        parts = filename.replace('.yaml', '').replace('.yml', '')
        kind = next((k for k in kinds if k in parts), None)
        assert kind is not None
        if kind in shipment_configs:
            raise ValueError(f"Multiple shipment configs found for {kind}")
        shipment_configs[kind] = record.config

    return shipment_configs


def get_shipment_config_from_mr(mr_url: str, kind: str) -> ShipmentConfig | None:
    """Fetch a specific shipment config from a merge request URL."""
    shipment_configs = get_shipment_configs_from_mr(mr_url)
    return shipment_configs.get(kind)


def get_builds_from_mr(mr_url: str) -> Dict[str, List[str]]:
    """Fetch builds from a merge request URL."""

    builds_by_kind = {}
    shipment_configs = get_shipment_configs_from_mr(mr_url)
    for kind, shipment_config in shipment_configs.items():
        nvrs = []
        if shipment_config.shipment.snapshot:
            nvrs = shipment_config.shipment.snapshot.nvrs
            logger.info(f"Found {len(nvrs)} builds for {kind}")
        builds_by_kind[kind] = nvrs

    return builds_by_kind


def set_bugzilla_bug_ids(release_notes: ReleaseNotes, bug_ids: Iterable[int | str]):
    if not all(isinstance(bug_id, int) or bug_id.isdigit() for bug_id in bug_ids):
        raise ValueError("All bug IDs must be integers")

    non_bugzilla_issues = (
        [b for b in release_notes.issues.fixed if b.source != "bugzilla.redhat.com"] if release_notes.issues else []
    )
    fixed = non_bugzilla_issues + [
        Issue(id=str(issue_id), source="bugzilla.redhat.com") for issue_id in sorted(set(bug_ids))
    ]
    fixed.sort(key=lambda x: x.id)
    if not fixed:
        release_notes.issues = None
    else:
        release_notes.issues = Issues(fixed=fixed)


def set_jira_bug_ids(release_notes: ReleaseNotes, bug_ids: Iterable[str]):
    non_jira_issues = (
        [b for b in release_notes.issues.fixed if b.source != JIRA_DOMAIN_NAME] if release_notes.issues else []
    )
    fixed = non_jira_issues + [Issue(id=str(issue_id), source=JIRA_DOMAIN_NAME) for issue_id in sorted(set(bug_ids))]
    fixed.sort(key=lambda x: x.id)
    if not fixed:
        release_notes.issues = None
    else:
        release_notes.issues = Issues(fixed=fixed)


def get_full_advisory_id_from_shipment(shipment_config: ShipmentConfig) -> str:
    """
    Build the full advisory display id from a shipment config's live ID, e.g. "RHBA-2025:13660".

    Arg(s):
        shipment_config (ShipmentConfig): Shipment config containing releaseNotes.type and
            releaseNotes.live_id.
    Return Value(s):
        str: The formatted advisory id, e.g. "RHBA-2025:13660".
    """
    release_notes = shipment_config.shipment.data.releaseNotes
    live_id = release_notes.live_id
    if not live_id:
        raise ValueError("Could not find live ID in image shipment config!")
    year = datetime.now().strftime("%Y")
    return f"{release_notes.type.upper()}-{year}:{live_id:04}"


def get_bug_ids_from_open_shipment_mrs(
    shipment_data_url: str,
    group: str,
    releases_config: Model,
    current_assembly: str,
) -> set[str]:
    """
    Collect bug IDs from all open shipment MRs in ocp-shipment-data that match
    the given group, excluding the current assembly's own MR.

    Only considers MRs whose assembly is defined in releases_config and whose
    MR URL matches the shipment URL configured for that assembly. This filters
    out test/incomplete MRs.

    Arg(s):
        shipment_data_url (str): GitLab URL for ocp-shipment-data repo
        group (str): OCP group to match (e.g., 'openshift-4.18')
        releases_config (Model): Parsed releases.yml config for assembly validation
        current_assembly (str): Assembly being prepared (its own MR is excluded)
    Return Value(s):
        set[str]: Set of bug IDs already attached to open shipment MRs
    """
    parsed_url = urlparse(shipment_data_url)
    project_path = parsed_url.path.strip("/")

    gl = GitLabClient.from_url(shipment_data_url)
    open_mrs = gl.list_merge_requests(project_path, state="opened")

    bug_ids: set[str] = set()
    for mr in open_mrs:
        try:
            shipment_configs = get_shipment_configs_from_mr(mr.web_url, group=group)
        except (ValueError, TypeError, KeyError):
            logger.warning("Failed to parse shipment configs from MR %s, skipping", mr.web_url, exc_info=True)
            continue

        for config in shipment_configs.values():
            metadata = config.shipment.metadata
            if metadata.group != group:
                continue
            if metadata.assembly == current_assembly:
                continue
            if not _is_assembly_shipment_valid(releases_config, metadata.assembly, mr.web_url):
                continue
            if metadata.fbc:
                continue
            if not config.shipment.data or not config.shipment.data.releaseNotes:
                continue
            issues = config.shipment.data.releaseNotes.issues
            if not issues or not issues.fixed:
                continue
            for issue in issues.fixed:
                bug_ids.add(issue.id)

    if bug_ids:
        logger.info("Found %d bugs attached to open shipment MRs: %s", len(bug_ids), sorted(bug_ids))

    return bug_ids


def _is_assembly_shipment_valid(releases_config: Model, assembly: str, mr_url: str) -> bool:
    """
    Check that an assembly exists in releases_config and that its configured
    shipment URL matches the given MR URL.

    Arg(s):
        releases_config (Model): Parsed releases.yml
        assembly (str): Assembly name from shipment metadata
        mr_url (str): MR web URL to validate against
    Return Value(s):
        bool: True if assembly is defined and its shipment URL matches mr_url
    """
    if not releases_config.releases[assembly]:
        logger.debug("Assembly %s not found in releases_config, skipping MR %s", assembly, mr_url)
        return False

    assembly_group_config = assembly_config_struct(releases_config, assembly, "group", {})
    shipment = assembly_group_config.get("shipment") or {}
    configured_url = shipment.get("url") if isinstance(shipment, dict) else getattr(shipment, "url", None)
    if not configured_url:
        logger.debug("No shipment URL configured for assembly %s, skipping MR %s", assembly, mr_url)
        return False

    if configured_url != mr_url:
        logger.debug(
            "MR URL %s does not match configured shipment URL %s for assembly %s, skipping",
            mr_url,
            configured_url,
            assembly,
        )
        return False

    return True
