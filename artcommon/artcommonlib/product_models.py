"""
Immutable dataclasses for product configuration.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from artcommonlib.product_ids import ProductId


@dataclass(frozen=True, slots=True)
class ReleaseTarget:
    """
    Konflux ReleasePlan and Application pair.

    Arg(s):
        release_plan: Konflux ReleasePlan name.
        application: Konflux Application name.
    """

    release_plan: str
    application: str


@dataclass(frozen=True, slots=True)
class ConformaPolicies:
    """
    Conforma stage policies for image and optional FBC verification.

    Arg(s):
        image_policy: Image policy resource.
        fbc_policy: FBC policy resource, if configured.
    """

    image_policy: str
    fbc_policy: str | None = None


@dataclass(frozen=True, slots=True)
class ProductConfig:
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

    product_id: ProductId
    aliases: tuple[str, ...] = ()
    namespace: str | None = None
    kubeconfig_env: str | None = None
    base_image_release: ReleaseTarget | None = None
    ec_base_image_release: ReleaseTarget | None = None
    conforma_stage_policies: ConformaPolicies | None = None
    fbc_stage_release_plans: Mapping[tuple[int, int], str] = field(default_factory=dict)
    cpe_product_name: str | None = None

    def __post_init__(self) -> None:
        """
        Prevent callers from mutating a product's release plans.
        """
        object.__setattr__(self, "fbc_stage_release_plans", MappingProxyType(dict(self.fbc_stage_release_plans)))

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
