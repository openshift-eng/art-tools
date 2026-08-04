import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
import click
from artcommonlib import exectools

from elliottlib.cli.common import cli, click_coroutine

LOGGER = logging.getLogger(__name__)

SIGNATURE_MIRROR_BASE = "https://mirror.openshift.com/pub/openshift-v4/signatures"
DEV_MIRROR_PATH = "openshift-release-dev/ocp-release"
PROD_MIRROR_PATH = "openshift/release"
RELEASE_IMAGE_REPO = "quay.io/openshift-release-dev/ocp-release"
MAX_SIGNATURE_PROBES = 100


@dataclass
class SignatureCheckResult:
    arch: str
    pullspec: str
    digest: str
    dev_mirror: Optional[bool] = None
    prod_mirror: Optional[bool] = None

    @property
    def passed(self) -> bool:
        results = []
        if self.dev_mirror is not None:
            results.append(self.dev_mirror)
        if self.prod_mirror is not None:
            results.append(self.prod_mirror)
        return bool(results) and all(results)


@dataclass
class VerifySignaturesResult:
    release_name: str
    arch_results: list[SignatureCheckResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        if self.errors:
            return False
        return bool(self.arch_results) and all(r.passed for r in self.arch_results)


async def get_release_image_digest(pullspec: str) -> str:
    cmd = ["oc", "image", "info", "-o", "json", pullspec]
    rc, out, err = await exectools.cmd_gather_async(cmd)
    if rc:
        raise RuntimeError(f"oc image info failed for {pullspec}: {err.strip()}")
    data = json.loads(out)
    if isinstance(data, list):
        data = data[0] if data else {}
    digest = data.get("digest")
    if not digest:
        raise RuntimeError(f"No digest found for {pullspec}")
    return digest


async def check_signature_on_mirror(sha: str, mirror_path: str) -> bool:
    base_url = f"{SIGNATURE_MIRROR_BASE}/{mirror_path}/sha256={sha}"
    async with aiohttp.ClientSession() as session:
        for sig_num in range(1, MAX_SIGNATURE_PROBES + 1):
            url = f"{base_url}/signature-{sig_num}"
            async with session.get(url) as response:
                if response.status == 200:
                    LOGGER.info("Found signature at %s", url)
                    return True
                if response.status == 404:
                    return False
                raise aiohttp.ClientResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=f"Unexpected status {response.status} from {url}",
                )
    LOGGER.warning("Reached max probes (%d) for %s — treating as unsigned", MAX_SIGNATURE_PROBES, sha)
    return False


async def verify_release_signatures(release_name, arches, check_dev, check_prod) -> VerifySignaturesResult:
    result = VerifySignaturesResult(release_name=release_name)

    for arch in arches:
        pullspec = f"{RELEASE_IMAGE_REPO}:{release_name}-{arch}"
        LOGGER.info("Checking signatures for %s", pullspec)

        try:
            digest = await get_release_image_digest(pullspec)
        except Exception as e:
            result.errors.append(f"{arch}: failed to get digest for {pullspec}: {e}")
            continue

        sha = digest.removeprefix("sha256:")
        check = SignatureCheckResult(arch=arch, pullspec=pullspec, digest=digest)

        if check_dev:
            try:
                check.dev_mirror = await check_signature_on_mirror(sha, DEV_MIRROR_PATH)
            except Exception as e:
                result.errors.append(f"{arch}: dev mirror check failed: {e}")
            else:
                status = "OK" if check.dev_mirror else "MISSING"
                LOGGER.info("%s: dev mirror signature %s", arch, status)

        if check_prod:
            try:
                check.prod_mirror = await check_signature_on_mirror(sha, PROD_MIRROR_PATH)
            except Exception as e:
                result.errors.append(f"{arch}: prod mirror check failed: {e}")
            else:
                status = "OK" if check.prod_mirror else "MISSING"
                LOGGER.info("%s: prod mirror signature %s", arch, status)

        result.arch_results.append(check)

    return result


def render_result(result: VerifySignaturesResult, output: str) -> str:
    if output == "json":
        data = {
            "release": result.release_name,
            "passed": result.passed,
            "arch_results": [
                {
                    "arch": r.arch,
                    "pullspec": r.pullspec,
                    "digest": r.digest,
                    "dev_mirror": r.dev_mirror,
                    "prod_mirror": r.prod_mirror,
                    "passed": r.passed,
                }
                for r in result.arch_results
            ],
            "errors": result.errors,
        }
        return json.dumps(data, indent=2)

    lines = [f"Release: {result.release_name}", ""]
    for r in result.arch_results:
        status = "PASS" if r.passed else "FAIL"
        lines.append(f"  {r.arch}: {status}")
        lines.append(f"    pullspec: {r.pullspec}")
        lines.append(f"    digest:   {r.digest}")
        if r.dev_mirror is not None:
            lines.append(f"    dev mirror:  {'OK' if r.dev_mirror else 'MISSING'}")
        if r.prod_mirror is not None:
            lines.append(f"    prod mirror: {'OK' if r.prod_mirror else 'MISSING'}")
        lines.append("")

    if result.errors:
        lines.append("Errors:")
        for err in result.errors:
            lines.append(f"  - {err}")
        lines.append("")

    overall = "PASS" if result.passed else "FAIL"
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)


@cli.command("verify-signatures", short_help="Verify release image signatures on mirror")
@click.option(
    "--arch",
    "arches",
    multiple=True,
    help="Architecture(s) to check. Can be specified multiple times. Defaults to group arches from ocp-build-data.",
)
@click.option(
    "--check-dev-mirror/--no-check-dev-mirror",
    default=True,
    show_default=True,
    help="Check signatures on dev mirror (openshift-release-dev/ocp-release).",
)
@click.option(
    "--check-prod-mirror/--no-check-prod-mirror",
    default=True,
    show_default=True,
    help="Check signatures on prod mirror (openshift/release).",
)
@click.option(
    "-o", "--output", type=click.Choice(["text", "json"]), default="text", show_default=True, help="Output format."
)
@click.pass_obj
@click_coroutine
async def verify_signatures_cli(runtime, arches, check_dev_mirror, check_prod_mirror, output):
    """Verify release image signatures on mirror.

    Requires --group and --assembly global options. Uses the assembly name
    as the release name and group arches as default architectures.

    Example:
        elliott --group openshift-4.18 --assembly 4.18.51 verify-signatures
    """
    runtime.initialize(config_only=True)
    if not check_dev_mirror and not check_prod_mirror:
        raise click.UsageError("At least one of --check-dev-mirror or --check-prod-mirror must be enabled.")

    release_name = runtime.assembly
    if not arches:
        arches = runtime.group_config.get('arches', ['x86_64'])

    result = await verify_release_signatures(
        release_name=release_name,
        arches=arches,
        check_dev=check_dev_mirror,
        check_prod=check_prod_mirror,
    )
    click.echo(render_result(result, output))
    if not result.passed:
        raise SystemExit(1)
