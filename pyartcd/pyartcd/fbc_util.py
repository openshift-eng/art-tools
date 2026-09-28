"""
Shared utilities for FBC (File-Based Catalog) image validation.

Used by release_from_fbc, gen_assembly_lp, and prepare_release_lp pipelines
to validate consistency of related images across FBC builds targeting
different OCP versions.
"""

import asyncio
import json
import logging
import os
import re
from collections import defaultdict
from typing import Dict, List, Optional

from artcommonlib import exectools
from artcommonlib.rpm_utils import parse_nvr
from artcommonlib.util import extract_ocp_version_from_fbc_nvr
from elliottlib.util import extract_nvrs_from_fbc
from tenacity import retry, stop_after_attempt

logger = logging.getLogger(__name__)

_LP_GROUP_VERSION_RE = re.compile(r'-(\d+)\.(\d+)$')
_LP_ASSEMBLY_VERSION_RE = re.compile(r'^v?(\d+)\.(\d+)(?:\.(\d+))?$')


def extract_ocp_version_from_nvr(nvr: str) -> Optional[str]:
    """Extract the target OCP version from a layered-product FBC NVR.

    Args:
        nvr: Layered-product FBC NVR.

    Returns:
        The ``major.minor`` OCP version, or ``None`` when absent.
    """
    return extract_ocp_version_from_fbc_nvr(nvr)


def validate_layered_product_group_assembly(group: str, assembly: str) -> None:
    """Validate that a layered-product assembly belongs to its group.

    Args:
        group: Layered-product group name ending in its major and minor
            version, such as ``logging-6.2``.
        assembly: Layered-product assembly version, such as ``6.2.13``.

    Raises:
        ValueError: If either version cannot be determined or their major and
            minor versions differ.
    """
    group_match = _LP_GROUP_VERSION_RE.search(group)
    if not group_match:
        raise ValueError(
            f"Cannot determine the layered-product version from group '{group}'; expected a name ending in "
            "'<major>.<minor>'"
        )

    assembly_match = _LP_ASSEMBLY_VERSION_RE.fullmatch(assembly)
    if not assembly_match:
        raise ValueError(
            f"Cannot determine the layered-product version from assembly '{assembly}'; expected '<major>.<minor>' "
            "or '<major>.<minor>.<patch>'"
        )

    group_version = group_match.group(1, 2)
    assembly_version = assembly_match.group(1, 2)
    if group_version != assembly_version:
        expected = '.'.join(group_version)
        actual = '.'.join(assembly_version)
        raise ValueError(
            f"Assembly '{assembly}' does not belong to group '{group}': group release train is {expected}, "
            f"but assembly release train is {actual}. No shipment data was changed."
        )


def validate_layered_product_fbc_nvrs(assembly: str, fbc_nvrs: List[str]) -> None:
    """Validate that layered-product FBC NVRs belong to an assembly.

    A three-component assembly requires an exact major, minor, and patch
    match. A two-component assembly requires the same major and minor. The
    NVR release field, including its target OCP version, is intentionally not
    considered.

    Args:
        assembly: Layered-product assembly version.
        fbc_nvrs: FBC NVRs to validate.

    Raises:
        ValueError: If the assembly or an FBC NVR has an unsupported version,
            or if an FBC belongs to a different assembly.
    """
    assembly_match = _LP_ASSEMBLY_VERSION_RE.fullmatch(assembly)
    if not assembly_match:
        raise ValueError(
            f"Cannot validate FBC NVRs for assembly '{assembly}'; expected '<major>.<minor>' or "
            "'<major>.<minor>.<patch>'"
        )
    assembly_parts = tuple(part for part in assembly_match.groups() if part is not None)

    mismatches = []
    for nvr in fbc_nvrs:
        try:
            parsed_nvr = parse_nvr(nvr)
        except ValueError as exc:
            raise ValueError(f"Cannot determine the version of FBC NVR '{nvr}': {exc}") from exc

        nvr_version = parsed_nvr.get('version', '').lstrip('v')
        nvr_match = _LP_ASSEMBLY_VERSION_RE.fullmatch(nvr_version)
        if not nvr_match:
            raise ValueError(f"Cannot determine the layered-product version of FBC NVR '{nvr}'")
        nvr_parts = tuple(part for part in nvr_match.groups() if part is not None)
        if nvr_parts[: len(assembly_parts)] != assembly_parts:
            mismatches.append(f"{nvr} (version {nvr_version})")

    if mismatches:
        raise ValueError(
            f"FBC NVRs do not match assembly '{assembly}': {', '.join(mismatches)}. No shipment data was changed."
        )


async def extract_fbc_labels(fbc_pullspec: str) -> Dict[str, Optional[str]]:
    """
    Extract the NVR and __doozer_key labels from an FBC image.

    Returns a dict with 'nvr' and 'doozer_key' keys. On failure, returns
    None values rather than raising.
    """
    logger.info("Extracting FBC labels from FBC pullspec: %s", fbc_pullspec)

    @retry(reraise=True, stop=stop_after_attempt(3))
    async def _get_image_info():
        oc_cmd = ['oc', 'image', 'info', fbc_pullspec, '--filter-by-os', 'amd64', '-o', 'json']

        quay_auth_file = os.getenv("QUAY_AUTH_FILE")
        if quay_auth_file:
            oc_cmd.extend(['--registry-config', quay_auth_file])

        rc, stdout, stderr = await exectools.cmd_gather_async(oc_cmd, check=False)
        if rc != 0:
            raise RuntimeError(f"Failed to get image info for FBC {fbc_pullspec}: {stderr}")
        return stdout

    try:
        image_info_output = await _get_image_info()
        image_info = json.loads(image_info_output)

        labels = image_info.get('config', {}).get('config', {}).get('Labels', {})
        nvr = labels.get('com.redhat.art.nvr')
        doozer_key = labels.get('__doozer_key')

        logger.info("Extracted FBC labels - nvr: %s, doozer_key: %s", nvr, doozer_key)
        return {'nvr': nvr, 'doozer_key': doozer_key}

    except Exception as e:
        logger.exception("Failed to extract labels from FBC %s: %s", fbc_pullspec, e)
        return {'nvr': None, 'doozer_key': None}


async def validate_fbc_related_images(
    fbc_pullspecs: List[str], product: str, operator_groups_out: Optional[Dict[str, List[str]]] = None
) -> List[str]:
    """
    Validate that FBC pullspecs from the same operator have the same related images.

    Different operators are allowed to have different related images, but
    multiple FBC builds for the *same* operator (targeting different OCP versions)
    must contain identical related-image NVR sets.

    If `operator_groups_out` is provided, it is populated with the operator-to-pullspecs
    grouping computed internally, so callers can reuse it (e.g. to resolve mr_approvers)
    without re-extracting FBC labels.

    Returns the combined list of all unique related images across all operators.
    Raises RuntimeError if any same-operator FBCs have mismatched related images.
    """
    logger.info("Validating related images for %d FBC builds (grouped by operator)...", len(fbc_pullspecs))

    # Step 1: Extract operator identifiers from all FBCs in parallel
    logger.info("Extracting operator identifiers from FBC images...")
    label_tasks = [extract_fbc_labels(fbc) for fbc in fbc_pullspecs]
    fbc_labels_list = await asyncio.gather(*label_tasks)

    # Step 2: Map FBCs to operators
    fbc_to_operator = {}
    for fbc_pullspec, labels in zip(fbc_pullspecs, fbc_labels_list):
        operator = labels.get('doozer_key')

        if not operator:
            nvr = labels.get('nvr', '')
            if nvr and '-fbc-' in nvr:
                # NVR format: "cluster-logging-operator-fbc-6.4.5-20260529.ocp4.18"
                operator = nvr.split('-fbc-')[0]
                logger.warning(
                    "Missing __doozer_key label for %s, extracted operator from NVR: %s",
                    fbc_pullspec,
                    operator,
                )
            else:
                try:
                    tag = fbc_pullspec.split(':')[-1]
                    if '-fbc-' in tag:
                        operator = tag.split('-fbc-')[0]
                        logger.warning(
                            "Missing __doozer_key label for %s, extracted operator from tag: %s",
                            fbc_pullspec,
                            operator,
                        )
                    else:
                        raise RuntimeError(
                            f"Could not determine operator for FBC {fbc_pullspec} (nvr={nvr}). "
                            f"Cannot validate related-image consistency without operator identity."
                        )
                except RuntimeError:
                    raise
                except Exception as e:
                    raise RuntimeError(f"Failed to extract operator from {fbc_pullspec}: {e}") from e

        fbc_to_operator[fbc_pullspec] = operator

    # Step 3: Group FBCs by operator
    operator_groups = defaultdict(list)
    for fbc, operator in fbc_to_operator.items():
        operator_groups[operator].append(fbc)

    logger.info("Found %d operator group(s):", len(operator_groups))
    for operator, fbcs in operator_groups.items():
        logger.info("  %s: %d FBC(s)", operator, len(fbcs))

    if operator_groups_out is not None:
        operator_groups_out.update(operator_groups)

    # Step 4: Extract related images from each FBC sequentially to avoid temp directory conflicts
    logger.info("Extracting related images from all FBCs...")
    fbc_related_images = {}
    for fbc_pullspec in fbc_pullspecs:
        related_nvrs = await extract_nvrs_from_fbc(fbc_pullspec, product)
        fbc_related_images[fbc_pullspec] = sorted(related_nvrs)

    # Step 5: Validate related images within each operator group
    all_related_images = set()
    validation_errors = []

    for operator, fbcs_in_group in operator_groups.items():
        if len(fbcs_in_group) == 1:
            logger.info("Operator '%s': single FBC, no validation needed", operator)
            all_related_images.update(fbc_related_images[fbcs_in_group[0]])
            continue

        logger.info("Validating %d FBCs for operator '%s'...", len(fbcs_in_group), operator)
        reference_fbc = fbcs_in_group[0]
        reference_images = fbc_related_images[reference_fbc]

        mismatches = []
        for fbc_pullspec in fbcs_in_group[1:]:
            current_images = fbc_related_images[fbc_pullspec]
            if current_images != reference_images:
                only_in_reference = set(reference_images) - set(current_images)
                only_in_current = set(current_images) - set(reference_images)
                mismatches.append(
                    {
                        'fbc': fbc_pullspec,
                        'only_in_reference': sorted(only_in_reference),
                        'only_in_current': sorted(only_in_current),
                    }
                )

        if mismatches:
            error_msg = f"Operator '{operator}': FBC builds have mismatched related images.\n"
            error_msg += f"Reference FBC: {reference_fbc}\n"
            for mismatch in mismatches:
                error_msg += f"\nMismatch with: {mismatch['fbc']}\n"
                if mismatch['only_in_reference']:
                    error_msg += f"  Only in reference: {mismatch['only_in_reference']}\n"
                if mismatch['only_in_current']:
                    error_msg += f"  Only in current: {mismatch['only_in_current']}\n"
            validation_errors.append(error_msg)
        else:
            logger.info(
                "Operator '%s': all %d FBCs have matching related images (%d images)",
                operator,
                len(fbcs_in_group),
                len(reference_images),
            )
            all_related_images.update(reference_images)

    # Step 6: Report validation results
    if validation_errors:
        full_error = "\n".join(validation_errors)
        logger.error("Validation failed:\n%s", full_error)
        raise RuntimeError(full_error)

    unique_images = sorted(all_related_images)
    logger.info(
        "Validation passed: %d total unique related images across %d operator(s)",
        len(unique_images),
        len(operator_groups),
    )
    return unique_images
