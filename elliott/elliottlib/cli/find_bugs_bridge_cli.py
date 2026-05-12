"""Mirror eligible basis-group JIRA bugs into a bridge release."""

import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import click
import yaml
from artcommonlib.model import Model
from jira import JIRAError

from elliottlib import Runtime
from elliottlib.bzutil import JIRABug, JIRABugTracker
from elliottlib.cli import common
from elliottlib.cli.common import click_coroutine

LOGGER = logging.getLogger(__name__)

BRIDGE_LABEL = "art:bridge-bug"
WONT_FIX_RESOLUTIONS = {"won't fix", "wontfix", "wont fix"}
REQUIRED_LINK_TYPES = {"depends_on", "blocked_by", "clones"}


@dataclass(frozen=True)
class BridgeBugMirroringConfig:
    """Bridge bug mirroring settings derived from group config."""

    basis_group: str
    enabled: bool


@common.cli.command("find-bugs:bridge-mirror", short_help="Mirror basis-group bugs into a bridge release")
@click.option("--noop", "--dry-run", is_flag=True, default=False, help="Don't change anything")
@click.pass_obj
@click_coroutine
async def find_bugs_bridge_cli(runtime: Runtime, noop: bool):
    """Mirror eligible basis-group bugs into the current bridge release.

    Args:
        runtime: Elliott runtime for the bridge target group.
        noop: If `True`, log intended changes without creating or updating issues.

    Returns:
        None.
    """
    cli = FindBugsBridgeCli(runtime=runtime, noop=noop)
    await cli.run()


class FindBugsBridgeCli:
    """Synchronize bridge-release mirrors for bugs from a basis group."""

    def __init__(self, runtime: Runtime, noop: bool):
        self.runtime = runtime
        self.noop = noop
        self.bridge_config: Optional[BridgeBugMirroringConfig] = None
        self.source_runtime: Optional[Runtime] = None
        self.source_tracker: Optional[JIRABugTracker] = None
        self.target_tracker: Optional[JIRABugTracker] = None
        self.target_release: Optional[str] = None
        self.images_by_component: Dict[str, object] = {}
        self.images_by_jira_component: Dict[str, object] = {}
        self._product_config: Optional[Model] = None
        self.existing_mirrors_by_source: Dict[str, List[JIRABug]] = {}
        self.invalid_bugs: Dict[str, List[str]] = {}
        self.existing_mirror_count = 0
        self.created_mirror_count = 0
        self.updated_mirror_count = 0

    async def run(self):
        """Create or update bridge mirrors for all eligible source bugs.

        Returns:
            None.

        Raises:
            ValueError: If the target group has no configured JIRA target release,
                or if invalid existing mirror states are detected.
        """
        self.runtime.initialize(mode="images")
        major, minor = self.runtime.get_major_minor_fields()
        self.major_minor = f"{major}.{minor}"
        self.bridge_config = self._load_bridge_config(self.runtime)
        if not self.bridge_config.enabled:
            LOGGER.info("Bridge bug mirroring is disabled for %s", self.runtime.group)
            return

        self.source_runtime = self._build_source_runtime(self.bridge_config.basis_group)
        self.source_runtime.initialize(mode="none")
        self.source_tracker = self.source_runtime.get_bug_tracker("jira")
        self.target_tracker = self.runtime.get_bug_tracker("jira")

        target_releases = self.target_tracker.target_release()
        if not target_releases:
            raise ValueError(f"No Jira target release configured for {self.runtime.group}")
        self.target_release = target_releases[0]

        self._build_target_image_maps()
        candidates = self._get_candidate_bugs()
        self.existing_mirrors_by_source = self._get_existing_mirrors_by_source([bug.id for bug, _ in candidates])
        self.existing_mirror_count = len(
            {mirror.id for mirrors in self.existing_mirrors_by_source.values() for mirror in mirrors}
        )
        self.created_mirror_count = 0
        self.updated_mirror_count = 0
        LOGGER.info("Found %d existing bridge mirrors for %s", self.existing_mirror_count, self.runtime.group)
        skipped = 0
        for bug, image_meta in candidates:
            if await self._sync_mirror(bug, image_meta):
                continue
            else:
                skipped += 1

        if self.invalid_bugs:
            self._report_invalid_bugs()
            raise ValueError(
                f"Found invalid bridge bug cases for {len(self.invalid_bugs)} source bug(s); see log for details"
            )

        LOGGER.info(
            "Bridge bug mirroring complete for %s: existing=%d, created=%d, updated=%d, skipped=%d",
            self.runtime.group,
            self.existing_mirror_count,
            self.created_mirror_count,
            self.updated_mirror_count,
            skipped,
        )

    @staticmethod
    def _load_bridge_config(runtime: Runtime) -> BridgeBugMirroringConfig:
        """Read bridge bug mirroring settings from the target runtime.

        Args:
            runtime: Runtime for the bridge target group.

        Returns:
            BridgeBugMirroringConfig: Parsed bridge mirroring settings for the
            target group.

        Raises:
            ValueError: If mirroring is enabled but `bridge_release.basis_group`
                is missing.
        """
        bridge_release = runtime.group_config.get("bridge_release", {}) or {}
        basis_group = bridge_release.get("basis_group")
        bug_mirroring = bridge_release.get("bug_mirroring", {}) or {}
        enabled = bool(bug_mirroring.get("enabled", False))
        if enabled and not basis_group:
            raise ValueError(f"bridge_release.basis_group must be set for {runtime.group}")
        return BridgeBugMirroringConfig(basis_group=basis_group or "", enabled=enabled)

    def _build_source_runtime(self, group: str) -> Runtime:
        """Build a runtime for querying bugs in the basis group.

        Args:
            group: Basis group name to query for source bugs.

        Returns:
            Runtime: A runtime configured to read the basis group from the same
            build-data source and assembly as the target runtime.
        """
        config = self.runtime.cfg_obj.to_dict()
        config.update(
            {
                "group": group,
                "assembly": self.runtime.assembly,
                "data_path": self.runtime.data_path,
                "build_system": self.runtime.build_system,
                "working_dir": os.path.join(self.runtime.working_dir, f"bridge-source-{group}"),
            }
        )
        return Runtime(cfg_obj=self.runtime.cfg_obj, **config)

    def _build_target_image_maps(self):
        """Map target images by component name and JIRA component.

        Returns:
            None. This method repopulates `self.images_by_component` and
            `self.images_by_jira_component` from runtime image metadata and
            `product.yml` component mappings.
        """
        self.images_by_component = {}
        self.images_by_jira_component = {}
        for image_meta in self.runtime.image_metas():
            if not image_meta.bridge_bug_mirroring_enabled:
                continue
            self.images_by_component[image_meta.get_component_name()] = image_meta
        component_mapping = self._get_product_config().bug_mapping.components
        for package_name, component_entry in component_mapping.items():
            image_meta = self.images_by_component.get(package_name)
            if not image_meta:
                continue
            issue_component = (
                component_entry.get("issue_component")
                if isinstance(component_entry, dict)
                else component_entry.issue_component
            )
            if issue_component:
                self.images_by_jira_component[issue_component] = image_meta

    def _get_candidate_bugs(self) -> List[Tuple[JIRABug, object]]:
        """Return source bugs that should be mirrored into the target release.

        Returns:
            list[tuple[JIRABug, object]]: `(source_bug, image_meta)` pairs for
            open, non-tracker bugs whose JIRA component maps to a bridge-enabled
            target image.
        """
        assert self.source_tracker is not None
        bugs = self.source_tracker.search_bugs(
            search_filter="default", custom_query=' and status != "CLOSED"', verbose=self.runtime.debug
        )
        candidates: List[Tuple[JIRABug, object]] = []
        tracker_filtered = 0
        unmapped_filtered = 0
        for bug in sorted(bugs, key=lambda item: item.id):
            if bug.is_tracker_bug():
                tracker_filtered += 1
                LOGGER.debug("Skipping tracker bug %s", bug.id)
                continue
            image_meta = self._resolve_bug_image(bug)
            if not image_meta:
                unmapped_filtered += 1
                LOGGER.debug("Skipping %s because it does not map to a target image", bug.id)
                continue
            candidates.append((bug, image_meta))
        total_filtered = tracker_filtered + unmapped_filtered
        LOGGER.info(
            "Filtered %d bugs: tracker/CVE=%d, unmapped/non-ART/non-payload=%d",
            total_filtered,
            tracker_filtered,
            unmapped_filtered,
        )
        LOGGER.info("Found %d candidate bugs in basis group %s", len(candidates), self.bridge_config.basis_group)
        return candidates

    def _resolve_bug_image(self, bug: JIRABug):
        """Resolve the target image metadata for a source bug.

        Args:
            bug: Source JIRA bug from the basis group.

        Returns:
            The target image metadata object for the bug's JIRA component, or
            `None` if the bug has no component or maps to no bridge-enabled image.
        """
        try:
            jira_component = bug.component
        except IndexError:
            LOGGER.debug("Skipping %s because it does not have a component", bug.id)
            return None
        return self.images_by_jira_component.get(jira_component)

    def _get_product_config(self) -> Model:
        """Load and cache `product.yml` from the build-data main branch.

        Returns:
            Model: Parsed `product.yml` content used to map package names to JIRA
            components.

        Raises:
            IOError: If `product.yml` is missing or invalid in the build-data
                repository.
        """
        if self._product_config is not None:
            return self._product_config

        product_yml = self.runtime.get_file_from_branch("main", "product.yml")
        if not product_yml.strip():
            raise IOError("product.yml was not found in the current build-data repo main branch")
        product_config = yaml.safe_load(product_yml)
        if not isinstance(product_config, dict):
            raise IOError("product.yml from the current build-data repo main branch is missing or invalid")
        self._product_config = Model(dict_to_model=product_config)
        return self._product_config

    async def _sync_mirror(self, source_bug: JIRABug, image_meta) -> bool:
        """Create or update the bridge mirror for a source bug.

        Args:
            source_bug: Source JIRA bug from the basis group.
            image_meta: Target image metadata matched to the source bug.

        Returns:
            bool: `True` if the bug was handled by creating or updating a mirror,
            or by reusing a valid existing mirror. `False` if the bug was skipped
            because an invalid state or a closed Won't Fix mirror was found.
        """
        assert self.target_tracker is not None
        if source_bug.id in self.invalid_bugs:
            return False
        existing = self.existing_mirrors_by_source.get(source_bug.id, [])
        if existing:
            if any(self._is_closed_wont_fix(mirror) for mirror in existing):
                LOGGER.info("Skipping %s because an existing mirror is closed as Won't Fix", source_bug.id)
                return False
            if all(mirror.status.lower() == "closed" for mirror in existing):
                self._record_invalid_bug(
                    source_bug.id,
                    "Existing bridge mirror is closed without Won't Fix; manual investigation required",
                )
                return False
            fields = self._build_issue_fields(source_bug, image_meta)
            mirror = existing[0]
            if mirror.status.lower() != "closed":
                self._update_issue(mirror, fields)
                self._ensure_issue_links(mirror.id, source_bug.id)
                self.updated_mirror_count += 1
            return True

        fields = self._build_issue_fields(source_bug, image_meta)
        if self.noop:
            LOGGER.info("[DRY RUN] Would create bridge mirror for %s", source_bug.id)
            self.created_mirror_count += 1
            return True

        issue = self.target_tracker.create_issue(fields)
        self._ensure_issue_links(issue.id, source_bug.id)
        LOGGER.info("Created bridge mirror %s for %s", issue.id, source_bug.id)
        self.created_mirror_count += 1
        return True

    def _build_issue_fields(self, source_bug: JIRABug, image_meta) -> dict:
        """Build the field payload for a bridge mirror issue.

        Args:
            source_bug: Source JIRA bug being mirrored.
            image_meta: Target image metadata matched to the source bug.

        Returns:
            dict: JIRA issue fields for the mirror, including summary,
            description, issue type, components, labels, and selected copied
            metadata such as priority and security level.
        """
        assert self.target_tracker is not None
        summary = f"{source_bug.summary} [bridge to {self.target_release}]"
        description = self._build_description(source_bug, image_meta)
        issue_type = getattr(source_bug.bug.fields.issuetype, "name", "Bug")
        components = [{"name": component.name} for component in getattr(source_bug.bug.fields, "components", [])]
        fields = {
            "summary": summary,
            "description": description,
            "issuetype": {"name": issue_type},
            "components": components,
            "labels": [BRIDGE_LABEL],
        }
        priority = getattr(getattr(source_bug.bug.fields, "priority", None), "name", None)
        if priority:
            fields["priority"] = {"name": priority}
        security = getattr(source_bug.bug.fields, "security", None)
        if security and getattr(security, "name", None):
            fields["security"] = {"name": security.name}
        return fields

    def _build_description(self, source_bug: JIRABug, image_meta) -> str:
        """Build the bridge-specific description for a mirrored issue.

        Args:
            source_bug: Source JIRA bug being mirrored.
            image_meta: Target image metadata matched to the source bug.

        Returns:
            str: A bridge-specific description that explains why the mirror
            exists and appends the original source issue description.
        """
        source_description = getattr(source_bug.bug.fields, "description", "") or "No source description provided."
        return (
            f"This issue was automatically created by ART as a bridge-release mirror of {source_bug.id} "
            f"from {self.bridge_config.basis_group} for {self.major_minor}.\n\n"
            "Why this issue exists:\n"
            f"- The source issue is still open in {self.bridge_config.basis_group} and maps to image "
            f"{image_meta.distgit_key} for this bridge release.\n"
            f"- This mirror keeps the issue visible for {self.major_minor} advisory workflows until a product "
            "engineer decides otherwise.\n\n"
            f"How to disposition it for {self.major_minor}:\n"
            f"- You do not need to directly manage this issue's state unless you want to remove it from {self.major_minor} advisories.\n"
            f"- If the issue should not appear in {self.major_minor} advisories, close this mirror with \"Won't Fix\".\n"
            "- Otherwise, leave it open and ART automation will continue to manage its presence.\n\n"
            "Original description:\n"
            "----\n"
            f"{source_description}"
        )

    def _get_existing_mirrors_by_source(self, source_bug_ids: List[str]) -> Dict[str, List[JIRABug]]:
        """Return existing mirror issues keyed by source bug id.

        Args:
            source_bug_ids: Source JIRA issue keys to look up in existing bridge
                mirrors.

        Returns:
            dict[str, list[JIRABug]]: A mapping for every requested source bug id.
            Lists are empty when no mirror is found and can contain multiple
            entries when duplicate mirrors exist.
        """
        assert self.target_tracker is not None
        if not source_bug_ids:
            return {}

        mirrors = self.target_tracker.search_bugs(
            include_labels=[BRIDGE_LABEL],
            with_target_release=True,
            custom_query=" order by created DESC",
            verbose=self.runtime.debug,
        )
        source_bug_ids = set(source_bug_ids)
        mirrors_by_source: Dict[str, List[JIRABug]] = {bug_id: [] for bug_id in source_bug_ids}
        for mirror in mirrors:
            linked_sources = self._get_linked_sources(mirror, source_bug_ids)
            for linked_source in linked_sources:
                mirrors_by_source[linked_source].append(mirror)
        for source_bug_id, linked_mirrors in mirrors_by_source.items():
            if len(linked_mirrors) > 1:
                self._record_invalid_bug(
                    source_bug_id,
                    f"Expected exactly one bridge mirror, found {len(linked_mirrors)}: "
                    f"{[mirror.id for mirror in linked_mirrors]}",
                )
            elif linked_mirrors:
                self._validate_required_links(linked_mirrors[0], source_bug_id)
        return mirrors_by_source

    def _update_issue(self, mirror_bug: JIRABug, fields: dict):
        """Update an existing bridge mirror with the latest source fields.

        Args:
            mirror_bug: Existing bridge mirror to update.
            fields: JIRA update payload derived from the current source bug state.

        Returns:
            None.
        """
        if self.noop:
            LOGGER.info("[DRY RUN] Would update bridge mirror %s with fields=%s", mirror_bug.id, fields)
            return
        merged_labels = sorted(set(mirror_bug.keywords) | set(fields["labels"]))
        update_fields = fields.copy()
        update_fields["labels"] = merged_labels
        mirror_bug.bug.update(fields=update_fields)
        LOGGER.info("Updated bridge mirror %s", mirror_bug.id)

    def _ensure_issue_links(self, mirror_issue_key: str, source_issue_key: str):
        """Ensure the required links exist between the source and mirror issues.

        Args:
            mirror_issue_key: JIRA key for the bridge mirror issue.
            source_issue_key: JIRA key for the basis-group source issue.

        Returns:
            None.

        Raises:
            JIRAError: If link creation fails for a reason other than the link
                already existing.
        """
        assert self.target_tracker is not None
        if self.noop:
            LOGGER.info("[DRY RUN] Would add required links between %s and %s", mirror_issue_key, source_issue_key)
            return
        link_specs = [
            ("Blocks", source_issue_key, mirror_issue_key),
            ("Depend", mirror_issue_key, source_issue_key),
            ("Cloners", mirror_issue_key, source_issue_key),
        ]
        for link_name, inward_issue, outward_issue in link_specs:
            try:
                self.target_tracker.create_issue_link(link_name, inward_issue, outward_issue)
            except JIRAError as err:
                if "already exists" not in str(err).lower():
                    raise

    @staticmethod
    def _get_linked_sources(mirror_bug: JIRABug, source_bug_ids: set[str]) -> set[str]:
        """Return source bug ids linked to a mirror through required link types.

        Args:
            mirror_bug: Bridge mirror issue to inspect.
            source_bug_ids: Source issue keys that are valid candidates for
                linkage.

        Returns:
            set[str]: Source issue keys linked to the mirror through any of the
            required bridge mirror link types.
        """
        linked_sources = set()
        for link in getattr(mirror_bug.bug.fields, "issuelinks", []):
            if link.type.name == "Blocks" and hasattr(link, "inwardIssue"):
                key = link.inwardIssue.key
                if key in source_bug_ids:
                    linked_sources.add(key)
            if link.type.name == "Depend" and hasattr(link, "outwardIssue"):
                key = link.outwardIssue.key
                if key in source_bug_ids:
                    linked_sources.add(key)
            if link.type.name == "Cloners":
                if hasattr(link, "outwardIssue") and link.outwardIssue.key in source_bug_ids:
                    linked_sources.add(link.outwardIssue.key)
                if hasattr(link, "inwardIssue") and link.inwardIssue.key in source_bug_ids:
                    linked_sources.add(link.inwardIssue.key)
        return linked_sources

    @staticmethod
    def _required_link_types_for_source(mirror_bug: JIRABug, source_bug_id: str) -> set[str]:
        """Return required link types already present for a source bug.

        Args:
            mirror_bug: Bridge mirror issue to inspect.
            source_bug_id: Source issue key whose links should be validated.

        Returns:
            set[str]: Normalized required link markers already present between
            the mirror and the source issue.
        """
        found = set()
        for link in getattr(mirror_bug.bug.fields, "issuelinks", []):
            if link.type.name == "Blocks" and hasattr(link, "inwardIssue") and link.inwardIssue.key == source_bug_id:
                found.add("blocked_by")
            if link.type.name == "Depend" and hasattr(link, "outwardIssue") and link.outwardIssue.key == source_bug_id:
                found.add("depends_on")
            if link.type.name == "Cloners":
                if hasattr(link, "outwardIssue") and link.outwardIssue.key == source_bug_id:
                    found.add("clones")
                if hasattr(link, "inwardIssue") and link.inwardIssue.key == source_bug_id:
                    found.add("clones")
        return found

    def _validate_required_links(self, mirror_bug: JIRABug, source_bug_id: str):
        """Record an invalid case when a mirror is missing required links.

        Args:
            mirror_bug: Bridge mirror issue to validate.
            source_bug_id: Source issue key that should be linked to the mirror.

        Returns:
            None.
        """
        found = self._required_link_types_for_source(mirror_bug, source_bug_id)
        missing = REQUIRED_LINK_TYPES - found
        if missing:
            self._record_invalid_bug(
                source_bug_id,
                f"Bridge mirror {mirror_bug.id} is missing required links: {sorted(missing)}",
            )

    def _record_invalid_bug(self, source_bug_id: str, message: str):
        """Record an invalid bridge mirroring case for later reporting."""
        self.invalid_bugs.setdefault(source_bug_id, []).append(message)

    def _report_invalid_bugs(self):
        """Log every invalid bridge mirroring case collected during the run."""
        for source_bug_id in sorted(self.invalid_bugs):
            for message in self.invalid_bugs[source_bug_id]:
                LOGGER.error("Invalid bridge bug case for %s: %s", source_bug_id, message)

    @staticmethod
    def _is_closed_wont_fix(mirror_bug: JIRABug) -> bool:
        """Return whether a mirror is closed with a Won't Fix resolution.

        Args:
            mirror_bug: Bridge mirror issue to inspect.

        Returns:
            bool: `True` when the mirror is closed with a Won't Fix style
            resolution, otherwise `False`.
        """
        if mirror_bug.status.lower() != "closed":
            return False
        resolution = (mirror_bug.resolution or "").replace("’", "'").lower()
        return resolution in WONT_FIX_RESOLUTIONS
