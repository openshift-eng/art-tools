"""Shared orchestration for synchronous layered-product FBC builds."""

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional, Sequence

from artcommonlib import exectools
from artcommonlib.util import resolve_konflux_kubeconfig_by_product, resolve_konflux_namespace_by_product
from artcommonlib.variants import get_build_variant_for_product

from pyartcd import constants, jenkins, locks
from pyartcd.fbc_util import extract_ocp_version_from_nvr

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class FbcBuildResult:
    """Results from one target-specific FBC build request.

    Attributes:
        target_version: Target OCP major/minor version, or ``None`` for the
            legacy single-build fallback.
        operator_nvrs: Exact operator NVRs supplied to Doozer.
        nvrs: FBC NVRs produced by Doozer.
        pullspecs: FBC pullspecs produced by Doozer.
    """

    target_version: Optional[str]
    operator_nvrs: tuple[str, ...]
    nvrs: tuple[str, ...]
    pullspecs: tuple[str, ...]


def generate_fbc_lock_name(
    group: str,
    target_version: Optional[str] = None,
    allow_missing_target: bool = False,
) -> str:
    """Return the standard Redis lock name for an FBC build.

    Args:
        group: Doozer group name.
        target_version: Target OCP major/minor version. This is required for
            non-OpenShift groups using the normal multi-target workflow.
        allow_missing_target: Permit the legacy single-build fallback used
            when a group has no ``OCP_TARGET_VERSIONS`` configuration.

    Returns:
        The Redis resource name used by ``artcd build-fbc``.

    Raises:
        ValueError: If a non-OpenShift group omits its target version outside
            the legacy fallback.
    """
    if group.startswith('openshift-'):
        return f"lock:fbc-build:{group}"
    if not target_version:
        if allow_missing_target:
            return f"lock:fbc-build:{group}"
        raise ValueError(f"--major-minor is required for non-OpenShift group '{group}'")
    return f"lock:fbc-build:{group}-ocp-{target_version}"


def _validate_fbc_build_output(
    target_version: Optional[str], operator_nvrs: Sequence[str], return_code: int, stdout: str, stderr: str
) -> FbcBuildResult:
    """Parse and validate one Doozer JSON result.

    Args:
        target_version: Target OCP major/minor version.
        operator_nvrs: Exact operator NVRs requested.
        return_code: Doozer process return code.
        stdout: Doozer standard output containing JSON.
        stderr: Doozer standard error for diagnostics.

    Returns:
        A complete target-specific build result.

    Raises:
        RuntimeError: If Doozer failed or returned an incomplete or malformed
            result set.
    """
    target = target_version or "default"
    try:
        output = json.loads(stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RuntimeError(
            f"FBC build for OCP {target} failed with rc={return_code} and did not produce valid JSON output: "
            f"{(stderr or '')[-1000:]}"
        ) from exc
    if not isinstance(output, dict):
        raise RuntimeError(f"FBC build for OCP {target} returned JSON {type(output).__name__}, expected an object")

    errors = output.get("errors", [])
    failed_count = output.get("failed_count", 0)
    nvrs = output.get("nvrs", [])
    pullspecs = output.get("pullspecs", [])
    expected_count = len(set(operator_nvrs))

    if return_code != 0 or errors or failed_count:
        raise RuntimeError(
            f"FBC build for OCP {target} failed: rc={return_code}, failed_count={failed_count}, errors={errors}"
        )
    if len(nvrs) != expected_count or len(pullspecs) != expected_count:
        raise RuntimeError(
            f"FBC build for OCP {target} returned {len(nvrs)} NVRs and {len(pullspecs)} pullspecs for "
            f"{expected_count} operators"
        )
    if len(nvrs) != len(pullspecs):
        raise RuntimeError(
            f"FBC build for OCP {target} returned mismatched NVR and pullspec counts: {len(nvrs)} != {len(pullspecs)}"
        )
    if target_version:
        unexpected_targets = [nvr for nvr in nvrs if extract_ocp_version_from_nvr(nvr) != target_version]
        if unexpected_targets:
            raise RuntimeError(
                f"FBC build for OCP {target_version} returned NVRs for another target: {unexpected_targets}"
            )

    return FbcBuildResult(
        target_version=target_version,
        operator_nvrs=tuple(operator_nvrs),
        nvrs=tuple(nvrs),
        pullspecs=tuple(pullspecs),
    )


async def build_fbc_catalogs(
    runtime,
    group: str,
    assembly: str,
    version: str,
    product: str,
    operator_nvrs_by_target: Mapping[Optional[str], Sequence[str]],
    data_path: str = constants.OCP_BUILD_DATA_URL,
    data_gitref: Optional[str] = None,
    force: bool = False,
    reset_to_prod: bool = False,
) -> list[FbcBuildResult]:
    """Build FBC catalogs and wait for every target to finish.

    Target builds run concurrently, but each uses an isolated Doozer working
    directory and the same target-specific Redis lock as ``artcd build-fbc``.

    Args:
        runtime: pyartcd runtime providing the working directory and dry-run
            setting.
        group: Doozer group name.
        assembly: Assembly name.
        version: Version to assign to the generated FBC NVRs.
        product: Product name used to select the Konflux tenant.
        operator_nvrs_by_target: Mapping from OCP target version to exact
            operator NVRs.
        data_path: ocp-build-data repository URL.
        data_gitref: Optional ocp-build-data Git ref.
        force: Force a new build even if equivalent FBC content exists.
        reset_to_prod: Import the latest production catalog before rebasing.

    Returns:
        Results in the same order as ``operator_nvrs_by_target``.

    Raises:
        ValueError: If a build request has no operator NVRs.
        RuntimeError: If any target fails or returns an incomplete result.
    """
    release = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    message = f"Rebase FBC segment with release {release}"
    namespace = resolve_konflux_namespace_by_product(product or "ocp")
    kubeconfig = resolve_konflux_kubeconfig_by_product(product or "ocp")
    build_variant = get_build_variant_for_product(product or "ocp")
    registry_auth = os.getenv("KONFLUX_OPERATOR_INDEX_AUTH_FILE") or os.getenv("QUAY_AUTH_FILE")
    lock_identifier = jenkins.get_build_path_or_random()

    async def _build_one(target_version: Optional[str], requested_nvrs: Sequence[str]) -> FbcBuildResult:
        operator_nvrs = tuple(dict.fromkeys(requested_nvrs))
        target = target_version or "default"
        if not operator_nvrs:
            raise ValueError(f"No operator NVRs supplied for FBC target {target}")

        working_dir = Path(runtime.working_dir).absolute() / f"doozer-working-fbc-{target.replace('.', '-')}"
        group_arg = f"--group={group}"
        if data_gitref:
            group_arg += f"@{data_gitref}"
        cmd = [
            "doozer",
            group_arg,
            f"--assembly={assembly}",
            "--build-system=konflux",
            f"--working-dir={working_dir}",
            f"--data-path={data_path}",
            f"--variant={build_variant.value}",
            "beta:fbc:rebase-and-build",
            f"--konflux-namespace={namespace}",
            f"--version={version}",
            f"--release={release}",
            f"--message={message}",
            "--output=json",
        ]
        if kubeconfig:
            cmd.append(f"--konflux-kubeconfig={kubeconfig}")
        if reset_to_prod:
            cmd.append("--reset-to-prod")
            if registry_auth:
                cmd.append(f"--prod-registry-auth={registry_auth}")
        else:
            cmd.append("--no-reset-to-prod")
        if force:
            cmd.append("--force")
        if target_version:
            cmd.append(f"--major-minor={target_version}")
        cmd.extend(["--", *operator_nvrs])

        LOGGER.info(
            "Building FBC catalogs for OCP %s from operator NVRs: %s",
            target,
            list(operator_nvrs),
        )
        if runtime.dry_run:
            LOGGER.info("[DRY-RUN] Would run: %s", " ".join(cmd))
            return FbcBuildResult(target_version, operator_nvrs, (), ())

        async def _run():
            return_code, stdout, stderr = await exectools.cmd_gather_async(cmd, check=False)
            return _validate_fbc_build_output(
                target_version,
                operator_nvrs,
                return_code,
                stdout,
                stderr,
            )

        lock_name = generate_fbc_lock_name(group, target_version, allow_missing_target=True)
        return await locks.run_with_lock(
            coro=_run(),
            lock=locks.Lock.FBC_BUILD,
            lock_name=lock_name,
            lock_id=lock_identifier,
        )

    results = await asyncio.gather(
        *(
            _build_one(target_version, operator_nvrs)
            for target_version, operator_nvrs in operator_nvrs_by_target.items()
        ),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, Exception)]
    if failures:
        if len(failures) == 1:
            raise failures[0]
        raise RuntimeError(
            f"{len(failures)} FBC target builds failed: {'; '.join(str(failure) for failure in failures)}"
        )
    return [result for result in results if isinstance(result, FbcBuildResult)]
