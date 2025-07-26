import logging
import os
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

import gitlab
from artcommonlib.util import new_roundtrip_yaml_handler

from elliottlib.shipment_model import Issue, Issues, ReleaseNotes, ShipmentConfig

logger = logging.getLogger(__name__)

yaml = new_roundtrip_yaml_handler()


def get_shipment_configs_from_mr(
    mr_url: str, kinds: Tuple[str, ...] = ("image", "extras", "metadata", "fbc", "microshift-bootc")
) -> Dict[str, ShipmentConfig]:
    """Fetch shipment configs from a merge request URL.
    :param mr_url: URL of the merge request
    :param kinds: List of all possible advisory kinds to fetch shipment configs for
    :return: Dict of {kind: ShipmentConfig}
    """

    shipment_configs: Dict[str, ShipmentConfig] = {}

    gitlab_token = os.getenv("GITLAB_TOKEN")
    if not gitlab_token:
        raise ValueError("GITLAB_TOKEN environment variable is required for Konflux operations")

    parsed_url = urlparse(mr_url)
    project_path = parsed_url.path.strip('/').split('/-/merge_requests')[0]
    mr_id = parsed_url.path.split('/')[-1]
    gitlab_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

    gl = gitlab.Gitlab(gitlab_url, private_token=gitlab_token)
    gl.auth()

    project = gl.projects.get(project_path)
    mr = project.mergerequests.get(mr_id)
    source_project = gl.projects.get(mr.source_project_id)

    diff_info = mr.diffs.list(all=True)[0]
    diff = mr.diffs.get(diff_info.id)
    for file_diff in diff.diffs:
        file_path = file_diff.get('new_path') or file_diff.get('old_path')
        if not file_path or not file_path.endswith(('.yaml', '.yml')):
            continue

        filename = file_path.split('/')[-1]
        parts = filename.replace('.yaml', '').replace('.yml', '')
        kind = next((k for k in kinds if k in parts), None)
        if not kind:
            continue

        file_content = source_project.files.get(file_path, mr.source_branch)
        content = file_content.decode().decode('utf-8')

        shipment_data = ShipmentConfig(**yaml.load(content))
        if kind in shipment_configs:
            raise ValueError(f"Multiple shipment configs found for {kind}")
        shipment_configs[kind] = shipment_data

    return shipment_configs


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


def add_bug_ids_to_release_notes(release_notes: ReleaseNotes, bug_ids: Iterable[int | str]):
    if not release_notes.issues:
        release_notes.issues = Issues(fixed=[])
    existing_issues_ids = [b.id for b in release_notes.issues.fixed]
    for issue_id in sorted(set(bug_ids)):
        if issue_id not in existing_issues_ids:
            source = "bugzilla.redhat.com" if issue_id.isdigit() else "issues.redhat.com"
            release_notes.issues.fixed.append(Issue(id=issue_id, source=source))
