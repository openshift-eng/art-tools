import asyncio
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


@dataclass
class AdvisoryAlertResult:
    advisory_id: int
    impetus: str
    errata_type: str = ""
    blocking: bool = False
    skipped: bool = False
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return not self.blocking and not self.error

    @property
    def failed(self) -> bool:
        return self.blocking or bool(self.error)


@dataclass
class VerifySecurityAlertsResult(VerifyResultBase):
    advisories: list[AdvisoryAlertResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(a.ok for a in self.advisories)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "failed": self.failed,
            "advisories": [
                {
                    "advisory_id": a.advisory_id,
                    "impetus": a.impetus,
                    "errata_type": a.errata_type,
                    "blocking": a.blocking,
                    "skipped": a.skipped,
                    "error": a.error,
                }
                for a in self.advisories
            ],
        }

    def render_text(self) -> str:
        lines = ["Security alerts check", ""]
        for a in self.advisories:
            if a.skipped:
                lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): SKIPPED ({a.errata_type.upper()})")
            elif a.blocking:
                lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): BLOCKING")
            elif a.error:
                lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): ERROR")
                lines.append(f"    {a.error}")
            else:
                lines.append(f"  Advisory {a.advisory_id} ({a.impetus}): OK")
        lines.append("")
        overall = "OK" if self.passed else "FAIL"
        lines.append(f"Overall: {overall}")
        return "\n".join(lines)


def get_errata_type(advisory_data: dict) -> str:
    errata = advisory_data.get("errata", {})
    return next(iter(errata.keys()), "")


async def check_advisory_security_alerts(api: AsyncErrataAPI, advisory_id: int, impetus: str) -> AdvisoryAlertResult:
    result = AdvisoryAlertResult(advisory_id=advisory_id, impetus=impetus)
    try:
        advisory_data = await api.get_advisory(advisory_id)
        result.errata_type = get_errata_type(advisory_data)

        if not result.errata_type:
            result.error = "unable to determine advisory type"
            LOGGER.error("Advisory %s (%s): %s", advisory_id, impetus, result.error)
            return result

        if result.errata_type != "rhsa":
            LOGGER.info(
                "Advisory %s (%s): type %s, skipping security alerts check",
                advisory_id,
                impetus,
                result.errata_type.upper(),
            )
            result.skipped = True
            return result

        response = await api.refresh_security_alerts(advisory_id)
        alerts = response.get("alerts", {})
        LOGGER.debug(
            "Advisory %s (%s): blocking=%s, alert_count=%d",
            advisory_id,
            impetus,
            alerts.get("blocking", False),
            len(alerts.get("alerts", [])),
        )
        result.blocking = bool(alerts.get("blocking", False))

        if result.blocking:
            LOGGER.error("Advisory %s (%s): has BLOCKING security alerts", advisory_id, impetus)
        else:
            LOGGER.info("Advisory %s (%s): no blocking security alerts", advisory_id, impetus)

    except Exception as e:
        LOGGER.error("Advisory %s (%s): error checking security alerts: %s", advisory_id, impetus, e)
        result.error = str(e)

    return result


async def verify_security_alerts(advisories: dict[str, int]) -> VerifySecurityAlertsResult:
    result = VerifySecurityAlertsResult()

    async with AsyncErrataAPI() as api:
        tasks = [
            check_advisory_security_alerts(api, advisory_id, impetus) for impetus, advisory_id in advisories.items()
        ]
        results = await asyncio.gather(*tasks)
        result.advisories.extend(results)

    return result


# microshift advisories are managed separately and don't go through ProdSec alert flow
SKIPPED_IMPETUS = ("microshift",)


@cli.command("verify-security-alerts", short_help="Check RHSA advisories for blocking security alerts")
@verify_output_option
@click.pass_obj
@click_coroutine
async def verify_security_alerts_cli(runtime, output):
    """Check RHSA advisories for blocking security alerts.

    Refreshes security alert data from ProdSec and checks for blocking
    alerts on all RHSA advisories in the assembly. Non-RHSA advisories
    are skipped. Exits with code 1 if any blocking alert is found or
    if any advisory check fails with an error.

    Requires --group and --assembly global options. Advisory IDs are
    resolved from the assembly config in releases.yml.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.51 verify-security-alerts
    """
    runtime.initialize()
    advisories = get_assembly_advisory_ids(runtime, exclude_types=SKIPPED_IMPETUS)
    if not advisories:
        raise click.UsageError("No advisory IDs found in assembly config.")

    LOGGER.info("Checking security alerts for advisories: %s", advisories)
    result = await verify_security_alerts(advisories=advisories)
    handle_verify_result(result, output)
