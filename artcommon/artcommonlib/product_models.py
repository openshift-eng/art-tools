"""
Pydantic models for product configuration.
"""

from collections.abc import Mapping
from types import MappingProxyType

from artcommonlib.product_ids import ProductId
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReleaseTarget(BaseModel):
    """
    Konflux ReleasePlan and Application pair.

    Arg(s):
        release_plan: Konflux ReleasePlan name.
        application: Konflux Application name.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    release_plan: str
    application: str


class ConformaPolicies(BaseModel):
    """
    Conforma stage policies for image and optional FBC verification.

    Arg(s):
        image_policy: Image policy resource.
        fbc_policy: FBC policy resource, if configured.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    image_policy: str
    fbc_policy: str | None = None


class ProductConfig(BaseModel):
    """
    Configuration associated with one canonical product name.

    Arg(s):
        product_id: Canonical product identifier.
        aliases: Legacy product names accepted by the lookup.
        namespace: Konflux namespace for the product, if configured.
        kubeconfig_env: Environment variable containing the product kubeconfig.
        base_image_release: Production base-image ReleasePlan and application.
        ec_base_image_release: Pre-release base-image ReleasePlan and application.
        conforma_stage_policies: Image and optional FBC Conforma stage policies.
        fbc_stage_release_plans: Product-version to FBC ReleasePlan mapping.
        cpe_product_name: Product name used in generated CPE labels.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    product_id: ProductId
    aliases: tuple[str, ...] = ()
    namespace: str | None = None
    kubeconfig_env: str | None = None
    base_image_release: ReleaseTarget | None = None
    ec_base_image_release: ReleaseTarget | None = None
    conforma_stage_policies: ConformaPolicies | None = None
    fbc_stage_release_plans: Mapping[tuple[int, int], str] = Field(default_factory=dict)
    cpe_product_name: str | None = None

    @field_validator("fbc_stage_release_plans", mode="after")
    @classmethod
    def _freeze_fbc_stage_release_plans(cls, plans: Mapping[tuple[int, int], str]) -> Mapping[tuple[int, int], str]:
        """Prevent callers from mutating a validated product's release plans."""
        return MappingProxyType(dict(plans))

    @property
    def cpe_name(self) -> str:
        """
        Return the configured CPE product name or the canonical product name.

        Return Value(s):
            str: Product name to use in a CPE label.
        """
        return self.cpe_product_name or self.product_id.value

    @property
    def product_name(self) -> str:
        """
        Return the canonical product name represented by the identifier.

        Return Value(s):
            str: Canonical product name.
        """
        return self.product_id.value
