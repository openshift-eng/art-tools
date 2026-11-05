"""Stage registry delivery checks shared by scan-operator and layered-products-scan."""

import logging
import os
from typing import Optional

from artcommonlib import exectools
from artcommonlib.util import product_version_from_group_name, resolve_konflux_fbc_stage_release_plan

# The registry targeted by advisory-stage ReleasePlanAdmissions.
STAGE_REGISTRY = "registry.stage.redhat.io"

LOGGER = logging.getLogger(__name__)


def resolve_stage_release_plan(group: str, group_config: dict, logger: logging.Logger = LOGGER) -> Optional[str]:
    """Return the FBC advisory-stage ReleasePlan for a group, or None when the group has none.

    The product version comes from the group name when it carries one, then from
    ``version`` and finally from ``vars.MAJOR``/``vars.MINOR`` in the group config.

    :param group: Group name, e.g. 'openshift-4.18' or 'acm-5.0'
    :param group_config: Group config as returned by load_group_config()
    :param logger: Logger for the decision message
    """
    product = group_config.get('product') or 'ocp'

    group_version = product_version_from_group_name(group)
    if group_version:
        product_major, product_minor = group_version
    else:
        version_str = group_config.get('version')
        if version_str:
            parts = str(version_str).split('.')
            product_major, product_minor = int(parts[0]), int(parts[1])
        else:
            vars_section = group_config.get('vars', {})
            product_major = int(vars_section.get('MAJOR', 0))
            product_minor = int(vars_section.get('MINOR', 0))

    plan = resolve_konflux_fbc_stage_release_plan(product, product_major, product_minor)

    if not plan:
        logger.info(
            "No stage release plan configured for product '%s' (%d.%d); skipping stage release checks",
            product,
            product_major,
            product_minor,
        )
        return None

    logger.info(
        "Stage release plan '%s' found for product '%s' (%d.%d); will check stage registry for bundle delivery",
        plan,
        product,
        product_major,
        product_minor,
    )
    return plan


def stage_registry_auth_file() -> Optional[str]:
    """Return the registry auth file for stage lookups, or None to use skopeo's defaults.

    Jenkins scan-operator passes the quay-auth-file secret as REGISTRY_AUTH_FILE; the
    layered-products scan (Jenkins and Tekton) passes the same secret as QUAY_AUTH_FILE.
    """
    return os.environ.get('REGISTRY_AUTH_FILE') or os.environ.get('QUAY_AUTH_FILE') or None


async def check_bundle_in_stage_registry(
    delivery_repo: Optional[str],
    bundle_pullspec: Optional[str],
    operator_nvr: str,
    logger: logging.Logger = LOGGER,
) -> Optional[bool]:
    """Check whether a bundle image was delivered to the stage registry.

    Looks the bundle up by digest, so the check works for every advisory-stage
    ReleasePlanAdmission no matter which tags it pushes (the ACM and MCE ones push
    no ``{{ digest_sha }}`` tag).

    :param delivery_repo: Bundle delivery repo, e.g. 'rhacm2/acm-operator-bundle'
    :param bundle_pullspec: Bundle build pullspec carrying a sha256 digest
    :param operator_nvr: Operator NVR, used in log messages
    :param logger: Logger for the decision message
    :return: True if the image exists, False if confirmed missing, None if inconclusive
    """
    if not delivery_repo:
        logger.warning('  No delivery repo configured for %s; skipping stage release check', operator_nvr)
        return None

    if not bundle_pullspec or '@' not in bundle_pullspec:
        logger.warning('  No image digest in bundle pullspec for %s; skipping stage release check', operator_nvr)
        return None

    digest = bundle_pullspec.split('@', 1)[-1]
    if not digest.startswith('sha256:'):
        logger.warning('  Invalid bundle digest for %s: %s', operator_nvr, digest)
        return None

    stage_pullspec = f"docker://{STAGE_REGISTRY}/{delivery_repo}@{digest}"
    cmd = ['skopeo', 'inspect', '--raw']
    if auth_file := stage_registry_auth_file():
        cmd.extend(['--authfile', auth_file])
    cmd.append(stage_pullspec)

    try:
        rc, _, err = await exectools.cmd_gather_async(cmd, check=False)
    except Exception as e:
        logger.warning('  Failed to check stage registry for %s: %s', operator_nvr, e)
        return None

    if rc == 0:
        logger.info('  Stage release for %s exists: %s', operator_nvr, stage_pullspec)
        return True
    if 'manifest unknown' in (err or '').lower():
        logger.info('  Stage release MISSING for %s: %s', operator_nvr, stage_pullspec)
        return False
    logger.warning('  Stage registry check inconclusive for %s (rc=%s): %s', operator_nvr, rc, err)
    return None
