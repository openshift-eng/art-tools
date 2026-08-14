import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import click
from artcommonlib.arch_util import go_arch_for_brew_arch
from artcommonlib.assembly import assembly_basis

from elliottlib.cli.common import cli, click_coroutine

LOGGER = logging.getLogger(__name__)

RELEASE_CONTROLLER_URL = "https://{go_arch}.ocp.releases.ci.openshift.org"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)


@dataclass
class QualifierCheckResult:
    release_tag: str
    arch: str
    badge_earned: Optional[bool] = None
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.badge_earned is True


@dataclass
class VerifyQeQualifierResult:
    assembly: str
    stable_results: list[QualifierCheckResult] = field(default_factory=list)
    nightly_results: list[QualifierCheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        all_results = self.stable_results + self.nightly_results
        return bool(all_results) and all(r.passed for r in all_results)


async def check_qe_qualifier(release_tag: str, go_arch: str, session: aiohttp.ClientSession) -> QualifierCheckResult:
    url = RELEASE_CONTROLLER_URL.format(go_arch=go_arch)
    api_url = f"{url}/api/v1/releasetag/{release_tag}/qualifiers"
    result = QualifierCheckResult(release_tag=release_tag, arch=go_arch)

    try:
        async with session.get(api_url) as response:
            if response.status == 404:
                result.error = f"Release tag {release_tag} not found on {go_arch} release controller"
                return result
            response.raise_for_status()
            data = await response.json()
    except Exception as e:
        result.error = f"Failed to query release controller for {release_tag} ({go_arch}): {e}"
        return result

    qe = data.get("qualifiers", {}).get("qe", {})
    result.badge_earned = qe.get("badgeEarned", False)
    LOGGER.info("%s (%s): QE badge %s", release_tag, go_arch, "earned" if result.badge_earned else "not earned")
    return result


async def verify_qe_qualifier(
    assembly: str,
    arches: list[str],
    nightly_tags: dict[str, str],
    check_stable: bool = True,
    check_nightly: bool = True,
) -> VerifyQeQualifierResult:
    result = VerifyQeQualifierResult(assembly=assembly)

    async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
        tasks = []
        for arch in arches:
            go_arch = go_arch_for_brew_arch(arch)
            if check_stable:
                tasks.append(("stable", check_qe_qualifier(assembly, go_arch, session)))

            nightly_tag = nightly_tags.get(arch)
            if check_nightly and nightly_tag:
                tasks.append(("nightly", check_qe_qualifier(nightly_tag, go_arch, session)))

        results = await asyncio.gather(*[t[1] for t in tasks])

    for (kind, _), check in zip(tasks, results):
        if kind == "stable":
            result.stable_results.append(check)
        else:
            result.nightly_results.append(check)

    return result


def render_result(result: VerifyQeQualifierResult, output: str) -> str:
    if output == "json":
        data = {
            "assembly": result.assembly,
            "passed": result.passed,
            "stable": [
                {
                    "release_tag": r.release_tag,
                    "arch": r.arch,
                    "badge_earned": r.badge_earned,
                    "passed": r.passed,
                    "error": r.error,
                }
                for r in result.stable_results
            ],
            "nightly": [
                {
                    "release_tag": r.release_tag,
                    "arch": r.arch,
                    "badge_earned": r.badge_earned,
                    "passed": r.passed,
                    "error": r.error,
                }
                for r in result.nightly_results
            ],
        }
        return json.dumps(data, indent=2)

    lines = [f"Assembly: {result.assembly}", ""]

    if result.stable_results:
        lines.append("Stable:")
        for r in result.stable_results:
            status = "PASS" if r.passed else "FAIL"
            if r.error:
                lines.append(f"  {r.arch}: ERROR - {r.error}")
            else:
                lines.append(f"  {r.arch}: {status} (tag: {r.release_tag})")
        lines.append("")

    if result.nightly_results:
        lines.append("Nightly:")
        for r in result.nightly_results:
            status = "PASS" if r.passed else "FAIL"
            if r.error:
                lines.append(f"  {r.arch}: ERROR - {r.error}")
            else:
                lines.append(f"  {r.arch}: {status} (tag: {r.release_tag})")
        lines.append("")

    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


@cli.command("verify-qe-qualifier", short_help="Check release controller QE qualifier for stable and nightly builds")
@click.option(
    "-o", "--output", type=click.Choice(["text", "json"]), default="text", show_default=True, help="Output format."
)
@click.option("--stable/--no-stable", default=True, show_default=True, help="Check stable build QE qualifier.")
@click.option("--nightly/--no-nightly", default=True, show_default=True, help="Check nightly build QE qualifier.")
@click.pass_obj
@click_coroutine
async def verify_qe_qualifier_cli(runtime, output, stable, nightly):
    """Check release controller QE qualifier for stable and nightly builds.

    Checks only amd64 (x86_64) as QE qualifiers are only set on the amd64
    release controller.

    Requires --group and --assembly global options. Reads the assembly's
    reference_releases from ocp-build-data to determine the nightly tag.

    Returns exit 0 if checked builds have earned the QE badge, exit 1 otherwise.

    Example:
        elliott --data-path https://github.com/openshift-eng/ocp-build-data --group openshift-4.22 --assembly 4.22.9 verify-qe-qualifier
        elliott ... verify-qe-qualifier --no-nightly
        elliott ... verify-qe-qualifier --no-stable
    """
    if not stable and not nightly:
        raise click.UsageError("At least one of --stable or --nightly must be enabled.")

    runtime.initialize()

    nightly_tags = {}
    if nightly:
        releases_config = runtime.get_releases_config()
        basis = assembly_basis(releases_config, runtime.assembly)

        reference_releases = basis.get("reference_releases", {})
        if not reference_releases:
            raise click.UsageError(f"Assembly {runtime.assembly} has no reference_releases in its basis config.")

        if "x86_64" not in reference_releases:
            raise click.UsageError(f"Assembly {runtime.assembly} has no x86_64 reference release.")

        nightly_tags = {"x86_64": reference_releases["x86_64"]}

    result = await verify_qe_qualifier(
        assembly=runtime.assembly,
        arches=["x86_64"],
        nightly_tags=nightly_tags,
        check_stable=stable,
        check_nightly=nightly,
    )
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
