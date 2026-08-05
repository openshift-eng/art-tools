import logging
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

    gl = GitLabClient.from_url(mr_url)

    mr = gl.get_mr_from_url(mr_url)
    source_project = gl.get_project(mr.source_project_id)

    diff_info = mr.diffs.list(all=True)[0]
    diff = mr.diffs.get(diff_info.id)
    for file_diff in diff.diffs:
        file_path = file_diff.get('new_path') or file_diff.get('old_path')
        if not file_path or not file_path.endswith(('.yaml', '.yml')):
            continue

        path_parts = file_path.split('/')
        if group and (len(path_parts) < 4 or path_parts[0] != "shipment" or path_parts[2] != group):
            continue

        filename = file_path.split('/')[-1]
        parts = filename.replace('.yaml', '').replace('.yml', '')
        kind = next((k for k in kinds if k in parts), None)
        if not kind:
            continue

        file_content = source_project.files.get(file_path, mr.source_branch)
        content = file_content.decode().decode('utf-8')

        # Convert CommentedMap to regular Python objects before creating Pydantic model
        yaml_data = Model(yaml.load(content)).primitive()
        shipment_data = ShipmentConfig(**yaml_data)
        if kind in shipment_configs:
            raise ValueError(f"Multiple shipment configs found for {kind}")
        shipment_configs[kind] = shipment_data

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
