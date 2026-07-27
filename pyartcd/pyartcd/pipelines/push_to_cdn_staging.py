import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import click
import yaml as pyyaml
from artcommonlib.assembly import assembly_resolved
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.model import Missing, Model
from elliottlib.errata import ErrataConnector, get_cdn_push_status, get_raw_erratum

from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.constants import OCP_BUILD_DATA_URL
from pyartcd.runtime import Runtime

LOGGER = logging.getLogger(__name__)

SKIPPED_ADVISORY_IMPETUSES = frozenset({"microshift"})
DROPPED_ADVISORY_STATUS = "DROPPED_NO_SHIP"


@dataclass
class AdvisoryPushResult:
    errata_id: int
    impetus: str
    status: str  # "triggered", "complete", "in_progress", "dependency_blocked", "failed", "skipped"
    push_jobs: list[dict] = field(default_factory=list)
    error: str = ""


@dataclass
class PushToCdnStagingResult:
    group: str
    assembly: str
    advisories: list[AdvisoryPushResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors and all(a.status in ("complete", "skipped") for a in self.advisories)


def _load_releases_yaml_sync(group: str, data_path: str) -> Optional[dict]:
    if not data_path.startswith("https://"):
        releases_path = Path(data_path) / "releases.yml"
        if not releases_path.is_file():
            return None
        return pyyaml.safe_load(releases_path.read_text(encoding="utf-8"))

    parts = data_path.rstrip("/").removesuffix(".git").split("/")
    if len(parts) < 2:
        return None
    owner, repo_name = parts[-2], parts[-1]
    repo = get_github_client_for_org(owner).get_repo(f"{owner}/{repo_name}")
    content = repo.get_contents("releases.yml", ref=group)
    return pyyaml.safe_load(content.decoded_content)


async def load_assembly_group(group: str, assembly: str, data_path: str) -> Optional[dict]:
    releases = await asyncio.to_thread(_load_releases_yaml_sync, group, data_path)
    if not releases or assembly not in releases.get("releases", {}):
        return None

    releases_config = Model(dict_to_model=releases)
    group_config = assembly_resolved(releases_config, assembly).group
    if group_config is Missing:
        return None
    return group_config.primitive()


def _get_advisory_status(advisory_id: int) -> str:
    erratum = get_raw_erratum(advisory_id)["errata"]
    advisory_type_key = list(erratum.keys())[0]
    return erratum[advisory_type_key]["status"]


def _get_latest_jobs_by_target(push_jobs: list[dict]) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for job in push_jobs:
        target = job.get("target", {}).get("name", "unknown")
        if target not in latest or job.get("id", 0) > latest[target].get("id", 0):
            latest[target] = job
    return latest


def _classify_push_jobs(push_jobs: list[dict]) -> str:
    """Classify push job state: 'complete', 'in_progress', 'failed', or 'none'."""
    if not push_jobs:
        return "none"
    latest = _get_latest_jobs_by_target(push_jobs)
    statuses = {j.get("status") for j in latest.values()}
    if all(s == "COMPLETE" for s in statuses):
        return "complete"
    if "FAILED" in statuses:
        return "failed"
    return "in_progress"


def _push_advisory(advisory_id: int, impetus: str, dry_run: bool, logger: logging.Logger) -> AdvisoryPushResult:
    try:
        advisory_status = _get_advisory_status(advisory_id)
    except Exception as e:
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="failed",
            error=f"Failed to get advisory status: {e}",
        )

    if advisory_status == DROPPED_ADVISORY_STATUS:
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="skipped",
            error="Advisory is DROPPED_NO_SHIP",
        )

    try:
        push_jobs = get_cdn_push_status(advisory_id)
    except Exception as e:
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="failed",
            error=f"Failed to get push status: {e}",
        )

    job_state = _classify_push_jobs(push_jobs)

    if job_state == "complete":
        logger.info("Advisory %s (%s): push jobs already complete", advisory_id, impetus)
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="complete",
            push_jobs=push_jobs,
        )

    if job_state == "in_progress":
        logger.info("Advisory %s (%s): push jobs in progress", advisory_id, impetus)
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="in_progress",
            push_jobs=push_jobs,
        )

    # job_state is "none" or "failed" — trigger push
    if dry_run:
        logger.info("[DRY-RUN] Would trigger CDN stage push for advisory %s (%s)", advisory_id, impetus)
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="triggered",
        )

    try:
        response = ErrataConnector()._post(f'/api/v1/erratum/{advisory_id}/push?defaults=stage')
    except Exception as e:
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="failed",
            error=f"Push trigger failed: {e}",
        )

    if response.status_code == 400 and "dependencies" in response.text:
        logger.warning("Advisory %s (%s): blocked by push dependencies", advisory_id, impetus)
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="dependency_blocked",
            error="Advisory has unresolved push dependencies",
        )

    if not response.ok:
        return AdvisoryPushResult(
            errata_id=advisory_id,
            impetus=impetus,
            status="failed",
            error=f"Push trigger returned HTTP {response.status_code}: {response.text}",
        )

    push_jobs = response.json()
    logger.info("Advisory %s (%s): CDN stage push triggered", advisory_id, impetus)
    return AdvisoryPushResult(
        errata_id=advisory_id,
        impetus=impetus,
        status="triggered",
        push_jobs=push_jobs if isinstance(push_jobs, list) else [],
    )


class PushToCdnStagingPipeline:
    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        data_path: str,
    ) -> None:
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.data_path = data_path
        self.logger = runtime.logger

    async def run(self) -> PushToCdnStagingResult:
        assembly_group = await load_assembly_group(self.group, self.assembly, self.data_path)
        if not assembly_group:
            raise RuntimeError(f"Failed to load assembly group config for {self.group} {self.assembly}")

        advisories = assembly_group.get("advisories", {})
        if not advisories:
            raise RuntimeError(f"No advisories found for {self.group} {self.assembly}")

        result = PushToCdnStagingResult(group=self.group, assembly=self.assembly)

        for impetus, advisory_id in advisories.items():
            if impetus in SKIPPED_ADVISORY_IMPETUSES:
                self.logger.info("Skipping %s advisory %s", impetus, advisory_id)
                continue
            if not isinstance(advisory_id, int) or advisory_id <= 0:
                self.logger.warning("Skipping %s: invalid advisory ID %s", impetus, advisory_id)
                continue

            advisory_result = await asyncio.to_thread(
                _push_advisory,
                advisory_id,
                impetus,
                self.runtime.dry_run,
                self.logger,
            )
            result.advisories.append(advisory_result)

            if advisory_result.status in ("failed", "dependency_blocked"):
                result.errors.append(f"{impetus} ({advisory_id}): {advisory_result.error}")

        return result


def render_result(result: PushToCdnStagingResult, output: str) -> str:
    if output == "json":
        return json.dumps(
            {
                "group": result.group,
                "assembly": result.assembly,
                "passed": result.passed,
                "advisories": [
                    {
                        "errata_id": a.errata_id,
                        "impetus": a.impetus,
                        "status": a.status,
                        "error": a.error,
                    }
                    for a in result.advisories
                ],
                "errors": result.errors,
            },
            indent=2,
        )

    lines = [f"Push to CDN staging for {result.group} assembly {result.assembly}"]
    lines.append("")

    for a in result.advisories:
        status_icon = {
            "triggered": "[TRIGGERED]",
            "complete": "[COMPLETE]",
            "in_progress": "[IN PROGRESS]",
            "dependency_blocked": "[BLOCKED]",
            "failed": "[FAILED]",
            "skipped": "[SKIPPED]",
        }.get(a.status, f"[{a.status.upper()}]")
        line = f"  {status_icon} {a.impetus} (advisory {a.errata_id})"
        if a.error:
            line += f" - {a.error}"
        lines.append(line)

    lines.append("")
    in_progress = any(a.status == "in_progress" for a in result.advisories)
    if result.passed:
        lines.append("All advisory pushes triggered or already complete.")
    elif in_progress and not result.errors:
        lines.append("IN PROGRESS: Some advisory pushes are still running.")
    else:
        lines.append("FAILED: Some advisories could not be pushed.")
        for error in result.errors:
            lines.append(f"  - {error}")

    return "\n".join(lines)


@cli.command("push-to-cdn-staging")
@click.option(
    "-g",
    "--group",
    metavar="GROUP",
    required=True,
    help="The group of components on which to operate. e.g. openshift-4.20",
)
@click.option(
    "--assembly",
    metavar="ASSEMBLY",
    required=True,
    help="Assembly name e.g. 4.20.1",
)
@click.option(
    "--data-path",
    required=False,
    default=OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use",
)
@click.option(
    "--output",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
)
@pass_runtime
@click_coroutine
async def push_to_cdn_staging(
    runtime: Runtime,
    group: str,
    assembly: str,
    data_path: str,
    output: str,
):
    """Push advisory artifacts to CDN staging via Errata Tool."""
    pipeline = PushToCdnStagingPipeline(
        runtime=runtime,
        group=group,
        assembly=assembly,
        data_path=data_path,
    )
    result = await pipeline.run()
    click.echo(render_result(result, output))

    if not result.passed:
        raise SystemExit(1)
