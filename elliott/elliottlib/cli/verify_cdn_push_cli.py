import logging
from dataclasses import dataclass, field
from typing import Optional

import click

from elliottlib.cli.common import cli, click_coroutine
from elliottlib.errata_async import AsyncErrataAPI
from elliottlib.verify_common import (
    VerifyResultBase,
    get_assembly_advisory_ids,
    handle_verify_result,
    verify_output_option,
)

LOGGER = logging.getLogger(__name__)

PUSH_STATUS_COMPLETE = "COMPLETE"
PUSH_STATUS_FAILED = "FAILED"

CDN_PUSH_ADVISORY_TYPES = ("rpm", "rhcos")


@dataclass
class PushJobInfo:
    target: str
    job_id: int
    status: str

    @property
    def complete(self) -> bool:
        return self.status == PUSH_STATUS_COMPLETE

    @property
    def failed(self) -> bool:
        return self.status == PUSH_STATUS_FAILED


@dataclass
class AdvisoryPushResult:
    advisory_id: int
    impetus: str
    push_jobs: list[PushJobInfo] = field(default_factory=list)
    push_triggered: bool = False
    error: Optional[str] = None

    @property
    def complete(self) -> bool:
        return bool(self.push_jobs) and all(j.complete for j in self.push_jobs) and not self.error

    @property
    def failed(self) -> bool:
        return bool(self.error) or any(j.failed for j in self.push_jobs)

    @property
    def pending(self) -> bool:
        return not self.complete and not self.failed


@dataclass
class VerifyCdnPushResult(VerifyResultBase):
    advisories: list[AdvisoryPushResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return bool(self.advisories) and all(a.complete for a in self.advisories)

    @property
    def failed(self) -> bool:
        return any(a.failed for a in self.advisories)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "failed": self.failed,
            "advisories": [
                {
                    "advisory_id": a.advisory_id,
                    "impetus": a.impetus,
                    "complete": a.complete,
                    "failed": a.failed,
                    "push_triggered": a.push_triggered,
                    "error": a.error,
                    "push_jobs": [{"target": j.target, "job_id": j.job_id, "status": j.status} for j in a.push_jobs],
                }
                for a in self.advisories
            ],
        }

    def render_text(self) -> str:
        lines = ["CDN staging push status", ""]
        for a in self.advisories:
            status = "COMPLETE" if a.complete else ("FAIL" if a.failed else "PENDING")
            lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): {status}")
            if a.push_triggered:
                lines.append("    Push re-triggered")
            if a.error:
                lines.append(f"    Error: {a.error}")
            for j in a.push_jobs:
                lines.append(f"    {j.target}: {j.status} (job {j.job_id})")
        lines.append("")
        overall = "COMPLETE" if self.passed else ("FAIL" if self.failed else "PENDING")
        lines.append(f"Overall: {overall}")
        return "\n".join(lines)


def parse_push_jobs(raw_jobs: list) -> list[PushJobInfo]:
    latest_by_target: dict[str, PushJobInfo] = {}
    for job in raw_jobs:
        job_id = job["id"]
        status = job["status"]
        target = job["target"]["name"]
        if target not in latest_by_target or job_id > latest_by_target[target].job_id:
            latest_by_target[target] = PushJobInfo(target=target, job_id=job_id, status=status)
    return list(latest_by_target.values())


async def check_advisory_push(api: AsyncErrataAPI, advisory_id: int, impetus: str, do_push: bool) -> AdvisoryPushResult:
    result = AdvisoryPushResult(advisory_id=advisory_id, impetus=impetus)
    try:
        raw_jobs = await api.get_push_jobs(advisory_id)
        result.push_jobs = parse_push_jobs(raw_jobs)

        all_complete = bool(result.push_jobs) and all(j.complete for j in result.push_jobs)
        if all_complete:
            LOGGER.info("Advisory %s (%s): all push jobs complete", advisory_id, impetus)
            return result

        has_failed = any(j.failed for j in result.push_jobs)
        no_jobs = not result.push_jobs

        if do_push and (has_failed or no_jobs):
            reason = "failed jobs" if has_failed else "no push jobs found"
            LOGGER.info("Advisory %s (%s): %s, triggering CDN stage push", advisory_id, impetus, reason)
            push_response = await api.push_cdn_stage(advisory_id)
            if push_response is None:
                LOGGER.warning("Advisory %s (%s): push rejected (unmet dependencies)", advisory_id, impetus)
                result.error = "push rejected due to unmet dependencies"
            else:
                result.push_triggered = True
                raw_jobs = await api.get_push_jobs(advisory_id)
                result.push_jobs = parse_push_jobs(raw_jobs)
        else:
            for j in result.push_jobs:
                LOGGER.info("Advisory %s (%s): target %s status %s", advisory_id, impetus, j.target, j.status)

    except Exception as e:
        LOGGER.error("Advisory %s (%s): error checking push status: %s", advisory_id, impetus, e)
        result.error = str(e)

    return result


async def check_blocking_advisories(api: AsyncErrataAPI, advisory_id: int, do_push: bool) -> list[AdvisoryPushResult]:
    try:
        raw = await api.get_advisory(advisory_id)
        erratum_data = next(iter(raw.get("errata", {}).values()), {})
        blocking_ids = erratum_data.get("blocking_advisories", [])
    except Exception as e:
        LOGGER.warning("Failed to get blocking advisories for %s: %s", advisory_id, e)
        return [
            AdvisoryPushResult(
                advisory_id=advisory_id,
                impetus=f"blocking-lookup-{advisory_id}",
                error=f"failed to check blocking advisories: {e}",
            )
        ]

    results = []
    for blocking_id in blocking_ids:
        LOGGER.info("Advisory %s has blocking advisory %s, checking it first", advisory_id, blocking_id)
        result = await check_advisory_push(api, blocking_id, f"blocking-{blocking_id}", do_push)
        results.append(result)
    return results


async def verify_cdn_push(advisories: dict[str, int], do_push: bool) -> VerifyCdnPushResult:
    result = VerifyCdnPushResult()

    async with AsyncErrataAPI() as api:
        for impetus, advisory_id in advisories.items():
            blocking_results = await check_blocking_advisories(api, advisory_id, do_push)
            result.advisories.extend(blocking_results)

            blocking_incomplete = any(not r.complete for r in blocking_results)
            if blocking_incomplete:
                LOGGER.warning(
                    "Advisory %s (%s): blocking advisories not yet complete, skipping",
                    advisory_id,
                    impetus,
                )
                result.advisories.append(
                    AdvisoryPushResult(
                        advisory_id=advisory_id,
                        impetus=impetus,
                        error="blocking advisories not yet complete",
                    )
                )
                continue

            ar = await check_advisory_push(api, advisory_id, impetus, do_push)
            result.advisories.append(ar)

    return result


@cli.command("verify-cdn-push", short_help="Verify CDN staging push jobs for advisories")
@click.option(
    "--push/--no-push",
    default=True,
    show_default=True,
    help="Re-trigger CDN push for advisories with failed or missing push jobs.",
)
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_cdn_push_cli(runtime, push, output):
    """Verify CDN staging push jobs have completed for release advisories.

    Checks push job status for CDN push advisory types (rpm, rhcos) via the Errata API.
    With --push (default), re-triggers push for advisories with failed or
    missing push jobs. Handles advisory push dependencies automatically.

    Requires --group and --assembly global options. Advisory IDs are
    resolved from the assembly config in releases.yml.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.51 verify-cdn-push
    """
    runtime.initialize()
    advisories = get_assembly_advisory_ids(runtime, include_types=CDN_PUSH_ADVISORY_TYPES)
    if not advisories:
        raise click.UsageError(f"No advisory IDs found for {CDN_PUSH_ADVISORY_TYPES} in assembly config.")

    LOGGER.info("Checking CDN push status for advisories: %s", advisories)
    result = await verify_cdn_push(advisories=advisories, do_push=push)
    handle_verify_result(result, output)
