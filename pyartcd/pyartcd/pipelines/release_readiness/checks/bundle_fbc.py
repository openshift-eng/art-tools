"""
Bundle/FBC coverage check — verifies OLM operators have bundles and FBCs built.
"""

import asyncio
import logging

from artcommonlib import exectools
from artcommonlib.konflux.konflux_build_record import (
    KonfluxBuildOutcome,
    KonfluxBuildRecord,
    KonfluxBundleBuildRecord,
    KonfluxFbcBuildRecord,
)
from artcommonlib.konflux.konflux_db import KonfluxDb

from pyartcd.pipelines.release_readiness.helpers import error_result
from pyartcd.pipelines.release_readiness.models import CheckResult, Status

_LOGGER = logging.getLogger(__name__)


async def check_bundle_fbc_coverage(group: str, build_system: str, doozer_working: str) -> CheckResult:
    """
    Check that all OLM operators have bundles and FBCs built.

    Arg(s):
        group (str): OCP group (e.g. "openshift-4.21").
        build_system (str): Build system ("brew" or "konflux").
        doozer_working (str): Working directory for doozer.

    Return Value(s):
        CheckResult: Bundle/FBC coverage check result.
    """

    _LOGGER.info("Checking bundle/FBC coverage for %s", group)

    if build_system != "konflux":
        return CheckResult(name="bundle_fbc_coverage", status=Status.GREEN, summary="Skipped (Brew build system) ⏭️")

    try:
        operator_names = await _load_operator_names(group, build_system, doozer_working)
        if not operator_names:
            return CheckResult(name="bundle_fbc_coverage", status=Status.GREEN, summary="No OLM operators in group ✅")

        missing_builds, missing_bundles, missing_fbcs = await _scan_operator_coverage(group, operator_names)

    except Exception as e:
        _LOGGER.warning("Error checking bundle/FBC coverage for %s: %s", group, e)
        return error_result("bundle_fbc_coverage", Status.YELLOW, "Could not check bundle/FBC coverage", e)

    return _format_bundle_fbc_result(
        len(operator_names),
        missing_builds,
        missing_bundles,
        missing_fbcs,
    )


async def _load_operator_names(group: str, build_system: str, doozer_working: str) -> list[str]:
    """
    Load OLM operator distgit keys from ocp-build-data.
    """

    cmd = [
        "doozer",
        "--group",
        group,
        "--assembly",
        "stream",
        "--build-system",
        build_system,
        f"--working-dir={doozer_working}",
        "olm-bundle:list-olm-operators",
        "--output-format",
        "distgit-key",
    ]
    _, stdout, _ = await exectools.cmd_gather_async(cmd, stderr=None)
    return stdout.strip().split("\n") if stdout.strip() else []


async def _scan_operator_coverage(
    group: str,
    operator_names: list[str],
) -> tuple[list[str], list[str], list[str]]:
    """
    Query KonfluxDb for operator -> bundle -> FBC chain coverage.

    Return Value(s):
        tuple: (missing_builds, missing_bundles, missing_fbcs) name lists.
    """

    operator_db = KonfluxDb()
    operator_db.bind(KonfluxBuildRecord)
    bundle_db = KonfluxDb()
    bundle_db.bind(KonfluxBundleBuildRecord)
    fbc_db = KonfluxDb()
    fbc_db.bind(KonfluxFbcBuildRecord)

    operator_builds = await asyncio.gather(
        *[
            operator_db.get_latest_build(
                name=name,
                group=group,
                assembly="stream",
                outcome=KonfluxBuildOutcome.SUCCESS,
                exclude_large_columns=True,
            )
            for name in operator_names
        ]
    )

    missing_builds = [name for name, build in zip(operator_names, operator_builds) if not build]
    operators_with_builds = [b for b in operator_builds if b]

    bundle_checks = await asyncio.gather(
        *[
            bundle_db.get_latest_build(
                name=f"{op.name}-bundle",
                group=group,
                outcome=KonfluxBuildOutcome.SUCCESS,
                assembly="stream",
                extra_patterns={"operator_nvr": op.nvr},
            )
            for op in operators_with_builds
        ]
    )

    missing_bundles: list[str] = []
    fbc_tasks: list[tuple[str, asyncio.Task]] = []
    for op, bundle in zip(operators_with_builds, bundle_checks):
        if not bundle:
            missing_bundles.append(op.name)
        else:
            fbc_tasks.append((op.name, _check_fbc_exists(fbc_db, group, op, bundle)))

    missing_fbcs: list[str] = []
    if fbc_tasks:
        names, tasks = zip(*fbc_tasks)
        results = await asyncio.gather(*tasks)
        missing_fbcs = [name for name, fbc in zip(names, results) if not fbc]

    return missing_builds, missing_bundles, missing_fbcs


async def _check_fbc_exists(
    fbc_db: KonfluxDb,
    group: str,
    operator: KonfluxBuildRecord,
    bundle: KonfluxBundleBuildRecord,
) -> KonfluxFbcBuildRecord | None:
    """
    Check if a successful FBC build exists containing this bundle.
    """

    async for fbc in fbc_db.search_builds_by_fields(
        where={
            "name": f"{operator.name}-fbc",
            "group": group,
            "outcome": KonfluxBuildOutcome.SUCCESS,
            "assembly": "stream",
        },
        array_contains={"bundle_nvrs": bundle.nvr},
        limit=1,
        order_by="start_time",
        sorting="DESC",
    ):
        return fbc
    return None


def _format_bundle_fbc_result(
    total: int,
    missing_builds: list[str],
    missing_bundles: list[str],
    missing_fbcs: list[str],
) -> CheckResult:
    """
    Format bundle/FBC coverage into a CheckResult.
    """

    bundle_count = total - len(missing_builds) - len(missing_bundles)
    fbc_count = bundle_count - len(missing_fbcs)

    details = [
        f"  Operators: {total}",
        f"  Bundle exists: {bundle_count}/{total} "
        f"{Status.GREEN.emoji if not missing_bundles and not missing_builds else Status.RED.emoji}",
        f"  FBC exists: {fbc_count}/{total} {Status.GREEN.emoji if not missing_fbcs else Status.YELLOW.emoji}",
    ]

    for label, names in [
        ("No image build", missing_builds),
        ("Missing bundle", missing_bundles),
        ("Missing FBC", missing_fbcs),
    ]:
        if names:
            details.append(f"    {label}: {', '.join(names[:3])}")

    if missing_bundles or missing_builds:
        status = Status.RED
    elif missing_fbcs:
        status = Status.YELLOW
    else:
        status = Status.GREEN

    total_missing = len(missing_builds) + len(missing_bundles) + len(missing_fbcs)
    summary = f"{total - total_missing}/{total} operators fully covered"
    if total_missing:
        issue_word = "issue" if total_missing == 1 else "issues"
        summary += f" ({total_missing} {issue_word}) {status.emoji}"
    else:
        summary += f" {status.emoji}"

    return CheckResult(name="bundle_fbc_coverage", status=status, summary=summary, details=details)
