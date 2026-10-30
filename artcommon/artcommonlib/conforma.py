"""Shared Conforma pipeline defaults and stage-policy selection, independent of eligibility."""

from typing import Literal

from artcommonlib.product_catalog import find_product_config
from artcommonlib.release_util import SoftwareLifecyclePhase

KONFLUX_EC_PIPELINE_GIT_URL = "https://github.com/conforma/cli"
KONFLUX_EC_PIPELINE_REVISION = "main"
KONFLUX_EC_PIPELINE_PATH = "pipelines/enterprise-contract/0.1/enterprise-contract.yaml"


def resolve_stage_policy(
    product: str | None,
    *,
    lifecycle_phase: str | None = None,
    assembly: str = "stream",
    build_type: Literal["image", "bundle", "fbc"] = "image",
    ec_policy: str | None = None,
    fbc_ec_policy: str | None = None,
) -> str | None:
    """Resolve an override or catalog policy for an image, bundle or FBC.

    FBC overrides precede generic overrides. Unknown products and products without
    a policy return None unless explicitly overridden. Missing/unknown lifecycle
    phases use the normal policy, matching scheduled release-stage verification.
    Only lifecycle, not assembly type (including preview), selects pre-release.
    OCP test assemblies retain transitional policies because their base images
    are not released and the art-images base registry rule must be excluded.
    """
    if build_type not in ("image", "bundle", "fbc"):
        raise ValueError(f"Unsupported Conforma build type '{build_type}'")
    if build_type == "fbc" and fbc_ec_policy:
        return fbc_ec_policy
    if ec_policy:
        return ec_policy
    config = find_product_config(product) if product else None
    if config is None or config.conforma_stage_policies is None:
        return None
    policies = config.conforma_stage_policies
    if build_type == "fbc":
        return policies.fbc_policy
    try:
        phase = SoftwareLifecyclePhase.from_name(lifecycle_phase) if lifecycle_phase else None
    except ValueError:
        phase = None
    pre_release = phase == SoftwareLifecyclePhase.PRE_RELEASE
    if assembly == "test" and policies.test_image_policy:
        return (
            policies.test_pre_release_image_policy
            if pre_release and policies.test_pre_release_image_policy
            else policies.test_image_policy
        )
    return (
        policies.pre_release_image_policy
        if pre_release and policies.pre_release_image_policy
        else policies.image_policy
    )
