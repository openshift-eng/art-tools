import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

import click
import yaml
from artcommonlib.assembly import assembly_resolved
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.gitlab import GitLabClient
from artcommonlib.model import Missing, Model

from elliottlib.cli.common import cli, click_coroutine, pass_runtime
from elliottlib.errata import get_bug_ids, get_raw_erratum

LOGGER = logging.getLogger(__name__)

ADVISORY_CVE_KINDS = ("rpm", "rhcos")
SKIPPED_ADVISORY_IMPETUSES = frozenset({"microshift"})
DROPPED_ADVISORY_STATUS = "DROPPED_NO_SHIP"
JIRA_ISSUE_SOURCES = frozenset({"redhat.atlassian.net", "issues.redhat.com"})
OCP_BUILD_DATA_URL = "https://github.com/openshift-eng/ocp-build-data"


@dataclass
class VerifyCveTrackersResult:
    group: str
    assembly: str
    shipment_mr_url: Optional[str]
    shipment_files: list[str] = field(default_factory=list)
    missing_advisory_trackers: list[str] = field(default_factory=list)
    missing_shipment_trackers: list[str] = field(default_factory=list)
    advisory_elliott_trackers: list[str] = field(default_factory=list)
    shipment_elliott_trackers: list[str] = field(default_factory=list)

    @property
    def missing_trackers(self) -> list[str]:
        return sorted(set(self.missing_advisory_trackers + self.missing_shipment_trackers))

    @property
    def passed(self) -> bool:
        return not self.missing_trackers


def get_jira_issues_from_shipment_data(data: dict) -> set[str]:
    shipment = data.get("shipment") or {}
    release_notes = ((shipment.get("data") or {}).get("releaseNotes")) or {}
    issues = release_notes.get("issues") or {}
    fixed = issues.get("fixed") or []
    jira_ids = set()
    for issue in fixed:
        if not isinstance(issue, dict):
            continue
        issue_id = issue.get("id")
        source = issue.get("source", "")
        if not issue_id:
            continue
        if source in JIRA_ISSUE_SOURCES or str(issue_id).startswith("OCPBUGS-"):
            jira_ids.add(str(issue_id))
    return jira_ids


def parse_shipment_metadata_from_data(data: dict) -> tuple[str, str]:
    shipment = data.get("shipment") or {}
    metadata = shipment.get("metadata") or {}
    group = metadata.get("group")
    assembly = metadata.get("assembly")
    if not group or not assembly:
        raise ValueError("Shipment YAML must define shipment.metadata.group and shipment.metadata.assembly")
    return group, assembly


def get_jira_issues_from_shipment_mr(
    mr_url: str,
) -> tuple[set[str], list[str], tuple[str, str]]:
    gl = GitLabClient.from_url(mr_url)
    mr = gl.get_mr_from_url(mr_url)
    source_project = gl.get_project(mr.source_project_id)

    diff_info = mr.diffs.list(all=True)[0]
    diff = mr.diffs.get(diff_info.id)
    yaml_files = [
        file_diff.get("new_path") or file_diff.get("old_path")
        for file_diff in diff.diffs
        if (file_diff.get("new_path") or file_diff.get("old_path", "")).lower().endswith((".yaml", ".yml"))
    ]

    if not yaml_files:
        raise ValueError(f"No shipment YAML files found in MR {mr_url}")

    jira_issues: set[str] = set()
    processed_files: list[str] = []
    group = None
    assembly = None

    for file_path in yaml_files:
        try:
            file_content = source_project.files.get(file_path, mr.source_branch)
            content = file_content.decode().decode("utf-8")
            data = yaml.safe_load(content)
            jira_issues.update(get_jira_issues_from_shipment_data(data))

            file_group, file_assembly = parse_shipment_metadata_from_data(data)
            processed_files.append(file_path)
            if group is None:
                group, assembly = file_group, file_assembly
            elif (file_group, file_assembly) != (group, assembly):
                raise ValueError(
                    f"Shipment MR contains mixed assemblies: expected {group}/{assembly}, "
                    f"found {file_group}/{file_assembly} in {file_path}"
                )
        except ValueError:
            raise
        except Exception:
            LOGGER.warning(
                "Failed to process shipment file %s in MR %s",
                file_path,
                mr_url,
                exc_info=True,
            )
            continue

    if group is None or assembly is None:
        raise ValueError(f"No valid shipment YAML files found in MR {mr_url}")

    return jira_issues, processed_files, (group, assembly)


def _get_shipment_config(assembly_group: dict) -> dict:
    for key in ("shipment", "shipment!"):
        value = assembly_group.get(key)
        if isinstance(value, dict):
            return value
    return {}


def get_shipment_mr_url(assembly_group: dict) -> str:
    shipment_url = _get_shipment_config(assembly_group).get("url")
    if not shipment_url:
        raise RuntimeError("shipment.url not found in assembly group config")
    return shipment_url


def _load_releases_yaml_sync(group: str, data_path: str) -> Optional[dict]:
    if not data_path.startswith("https://"):
        releases_path = Path(data_path) / "releases.yml"
        if not releases_path.is_file():
            return None
        return yaml.safe_load(releases_path.read_text(encoding="utf-8"))

    parts = data_path.rstrip("/").removesuffix(".git").split("/")
    if len(parts) < 2:
        return None
    owner, repo_name = parts[-2], parts[-1]
    repo = get_github_client_for_org(owner).get_repo(f"{owner}/{repo_name}")
    content = repo.get_contents("releases.yml", ref=group)
    return yaml.safe_load(content.decoded_content)


async def _load_releases_yaml(group: str, data_path: str) -> Optional[dict]:
    return await asyncio.to_thread(_load_releases_yaml_sync, group, data_path)


async def load_assembly_group(group: str, assembly: str, data_path: str) -> Optional[dict]:
    releases = await _load_releases_yaml(group, data_path)
    if not releases or assembly not in releases.get("releases", {}):
        return None

    releases_config = Model(dict_to_model=releases)
    group_config = assembly_resolved(releases_config, assembly).group
    if group_config is Missing:
        return None
    return group_config.primitive()


def flatten_elliott_cve_trackers(elliott_output: dict, kinds: Optional[Iterable[str]] = None) -> list[str]:
    trackers: set[str] = set()
    if kinds is None:
        values = elliott_output.values()
    else:
        values = (elliott_output.get(kind) for kind in kinds)

    for value in values:
        if isinstance(value, list):
            trackers.update(str(item) for item in value)
    return sorted(trackers)


def find_missing_cve_trackers(elliott_trackers: Iterable[str], attached_jira_issues: Iterable[str]) -> list[str]:
    attached = set(attached_jira_issues)
    return sorted(tracker for tracker in elliott_trackers if tracker not in attached)


def get_rhsa_jira_issues(advisories: dict) -> set[str]:
    jira_issues: set[str] = set()

    for impetus, advisory_id in advisories.items():
        if impetus in SKIPPED_ADVISORY_IMPETUSES:
            continue
        if advisory_id is None or int(advisory_id) <= 0:
            continue

        raw_erratum = get_raw_erratum(advisory_id)
        erratum = raw_erratum.get("errata") or {}
        if erratum.get("status") == DROPPED_ADVISORY_STATUS:
            continue
        if erratum.get("type") != "RHSA":
            continue

        jira_issues.update(str(issue_id) for issue_id in get_bug_ids(advisory_id).get("jira", []))

    return jira_issues


async def verify_cve_trackers(
    group: str,
    assembly: str,
    data_path: str,
    advisory_trackers: dict,
    shipment_trackers: dict,
) -> VerifyCveTrackersResult:
    assembly_group = await load_assembly_group(group, assembly, data_path)
    if not assembly_group:
        raise RuntimeError(f"Failed to load assembly group config for {group} {assembly}")

    shipment_mr_url = get_shipment_mr_url(assembly_group)
    result = VerifyCveTrackersResult(
        group=group,
        assembly=assembly,
        shipment_mr_url=shipment_mr_url,
    )

    # Check RHSA advisories
    rhsa_jira_issues = await asyncio.to_thread(
        get_rhsa_jira_issues,
        assembly_group.get("advisories", {}),
    )
    LOGGER.info("Found %s Jira issues on RHSA advisories", len(rhsa_jira_issues))

    result.advisory_elliott_trackers = flatten_elliott_cve_trackers(advisory_trackers, kinds=ADVISORY_CVE_KINDS)
    result.missing_advisory_trackers = find_missing_cve_trackers(
        result.advisory_elliott_trackers,
        rhsa_jira_issues,
    )

    # Check shipment MR
    shipment_jira_issues, shipment_files, (mr_group, mr_assembly) = await asyncio.to_thread(
        get_jira_issues_from_shipment_mr,
        shipment_mr_url,
    )
    result.shipment_files = shipment_files

    if (mr_group, mr_assembly) != (group, assembly):
        raise ValueError(
            f"Shipment MR metadata ({mr_group}, {mr_assembly}) does not match "
            f"requested group/assembly ({group}, {assembly})"
        )

    LOGGER.info(
        "Found %s Jira issues across %s shipment files in MR",
        len(shipment_jira_issues),
        len(shipment_files),
    )

    result.shipment_elliott_trackers = flatten_elliott_cve_trackers(shipment_trackers)
    result.missing_shipment_trackers = find_missing_cve_trackers(
        result.shipment_elliott_trackers,
        shipment_jira_issues,
    )

    return result


def render_result(result: VerifyCveTrackersResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "group": result.group,
                "assembly": result.assembly,
                "shipment_mr_url": result.shipment_mr_url,
                "shipment_files": result.shipment_files,
                "passed": result.passed,
                "missing_trackers": result.missing_trackers,
                "missing_advisory_trackers": result.missing_advisory_trackers,
                "missing_shipment_trackers": result.missing_shipment_trackers,
                "advisory_elliott_trackers": result.advisory_elliott_trackers,
                "shipment_elliott_trackers": result.shipment_elliott_trackers,
            },
            indent=2,
        )

    lines = [f"CVE tracker verification for {result.group} assembly {result.assembly}"]
    if result.shipment_mr_url:
        lines.append(f"Shipment MR: {result.shipment_mr_url}")
        if result.shipment_files:
            lines.append(f"Shipment files checked: {len(result.shipment_files)}")

    if result.missing_advisory_trackers:
        lines.append("")
        lines.append("MISSING CVE TRACKER BUGS IN RHSA ADVISORIES:")
        for tracker in result.missing_advisory_trackers:
            lines.append(f"  - {tracker}")

    if result.missing_shipment_trackers:
        lines.append("")
        lines.append("MISSING CVE TRACKER BUGS IN SHIPMENT MR:")
        for tracker in result.missing_shipment_trackers:
            lines.append(f"  - {tracker}")

    if result.passed:
        lines.append("")
        lines.append("All CVE tracker bugs are covered.")

    lines.append("")
    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


@cli.command("verify-cve-trackers", short_help="Verify CVE tracker bugs are covered in advisories and shipment MR")
@click.option(
    "--advisory-trackers-file",
    required=True,
    type=click.Path(exists=True),
    help="JSON file with elliott find-bugs --cve-only output for Brew builds",
)
@click.option(
    "--shipment-trackers-file",
    required=True,
    type=click.Path(exists=True),
    help="JSON file with elliott find-bugs --cve-only output for Konflux builds",
)
@click.option(
    "-o",
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)
@pass_runtime
@click_coroutine
async def verify_cve_trackers_cli(
    runtime,
    advisory_trackers_file: str,
    shipment_trackers_file: str,
    output: str,
):
    """Verify CVE tracker bugs are covered in RHSA advisories and shipment MR.

    Requires --group and --assembly global options.

    Expects JSON output from `elliott find-bugs --cve-only` as input files:
    one for Brew builds (advisory trackers) and one for Konflux builds
    (shipment trackers). These are typically produced by the artcd
    ert-verification pipeline.

    Example:
        elliott -g openshift-4.18 --assembly 4.18.51 verify-cve-trackers \\
            --advisory-trackers-file brew.json --shipment-trackers-file konflux.json
    """
    with open(advisory_trackers_file) as f:
        advisory_trackers = json.load(f)
    with open(shipment_trackers_file) as f:
        shipment_trackers = json.load(f)

    result = await verify_cve_trackers(
        group=runtime.group,
        assembly=runtime.assembly,
        data_path=runtime.data_path,
        advisory_trackers=advisory_trackers,
        shipment_trackers=shipment_trackers,
    )
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
