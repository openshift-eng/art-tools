"""
Canonical product configuration data and lookup functions.

This module contains one explicit ProductConfig definition per product and the
small lookup API used by ART tooling.
"""

from artcommonlib.product_ids import ProductId
from artcommonlib.product_models import ConformaPolicies, ProductConfig, ReleaseTarget

PRODUCT_CATALOG: tuple[ProductConfig, ...] = (
    ProductConfig(
        product_id=ProductId.OCP,
        namespace="ocp-art-tenant",
        kubeconfig_env="KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="ocp-art-images-base-silent",
            application="art-images-base",
        ),
        ec_base_image_release=ReleaseTarget(
            release_plan="ocp-art-images-base-silent-ec",
            application="art-images-base",
        ),
        fbc_stage_release_plans={
            (4, 12): "ocp-art-advisory-stage-auto-4-12",
            (4, 13): "ocp-art-advisory-stage-auto-4-13",
            (4, 14): "ocp-art-advisory-stage-auto-4-14",
            (4, 15): "ocp-art-advisory-stage-auto-4-15",
            (4, 16): "ocp-art-advisory-stage-auto-4-16",
            (4, 17): "ocp-art-advisory-stage-auto-4-17",
            (4, 18): "ocp-art-advisory-stage-auto-4-18",
            (4, 19): "ocp-art-advisory-stage-auto-4-19",
            (4, 20): "ocp-art-advisory-stage-auto-4-20",
            (4, 21): "ocp-art-advisory-stage-auto-4-21",
            (4, 22): "ocp-art-advisory-stage-auto-4-22",
            (4, 23): "ocp-art-advisory-stage-auto-4-23",
            (5, 0): "ocp-art-advisory-stage-auto-5-0",
            (5, 1): "ocp-art-advisory-stage-auto-5-1",
        },
    ),
    ProductConfig(product_id=ProductId.MICROSHIFT),
    ProductConfig(product_id=ProductId.OKD),
    ProductConfig(
        product_id=ProductId.ACM,
        namespace="art-acm-tenant",
        kubeconfig_env="ACM_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="acm-images-base-silent",
            application="acm-images-base",
        ),
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-art-acm-stage",
            fbc_policy="rhtap-releng-tenant/fbc-art-acm-stage",
        ),
        fbc_stage_release_plans={
            (2, 16): "acm-advisory-stage-2-16",
            (5, 0): "acm-advisory-stage-5-0",
        },
        cpe_product_name="acm",
    ),
    ProductConfig(
        product_id=ProductId.CERT_MANAGER,
        namespace="art-oap-tenant",
        kubeconfig_env="OAP_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="oap-cm-images-base-silent",
            application="oap-images-base",
        ),
        fbc_stage_release_plans={(1, 19): "cm-advisory-stage-auto-1-19"},
        cpe_product_name="cert_manager",
    ),
    ProductConfig(
        product_id=ProductId.COO,
        namespace="art-coo-tenant",
        kubeconfig_env="COO_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="coo-images-base-silent",
            application="coo-images-base",
        ),
    ),
    ProductConfig(
        product_id=ProductId.EXTERNAL_SECRETS,
        namespace="art-oap-tenant",
        kubeconfig_env="OAP_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="oap-eso-images-base-silent",
            application="oap-images-base",
        ),
        fbc_stage_release_plans={(1, 1): "eso-advisory-stage-auto-1-1"},
        cpe_product_name="external_secrets_operator",
    ),
    ProductConfig(
        product_id=ProductId.MCE,
        namespace="art-acm-tenant",
        kubeconfig_env="ACM_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="acm-images-base-silent",
            application="acm-images-base",
        ),
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-art-acm-stage",
            fbc_policy="rhtap-releng-tenant/fbc-art-acm-stage",
        ),
        fbc_stage_release_plans={
            (2, 11): "mce-advisory-stage-2-11",
            (5, 0): "mce-advisory-stage-5-0",
        },
        cpe_product_name="multicluster_engine",
    ),
    ProductConfig(
        aliases=("logging",),
        product_id=ProductId.LOGGING,
        namespace="art-logging-tenant",
        kubeconfig_env="LOGGING_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="logging-images-base-silent",
            application="logging-images-base",
        ),
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-art-logging-stage",
            fbc_policy="rhtap-releng-tenant/fbc-stage",
        ),
        fbc_stage_release_plans={
            (6, 0): "logging-advisory-stage-auto-6-0",
            (6, 2): "logging-advisory-stage-auto-6-2",
            (6, 3): "logging-advisory-stage-auto-6-3",
            (6, 4): "logging-advisory-stage-auto-6-4",
            (6, 5): "logging-advisory-stage-auto-6-5",
            (6, 6): "logging-advisory-stage-auto-6-6",
            (6, 7): "logging-advisory-stage-auto-6-7",
        },
        cpe_product_name="logging",
    ),
    ProductConfig(
        product_id=ProductId.MTA,
        namespace="art-mta-tenant",
        kubeconfig_env="MTA_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="mta-images-base-silent",
            application="mta-images-base",
        ),
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-art-mta-stage",
            fbc_policy="rhtap-releng-tenant/fbc-stage",
        ),
        fbc_stage_release_plans={
            (8, 0): "mta-advisory-stage-8-0",
            (8, 1): "mta-advisory-stage-8-1",
            (8, 2): "mta-advisory-stage-8-2",
            (8, 3): "mta-advisory-stage-8-3",
        },
        cpe_product_name="migration_toolkit_applications",
    ),
    ProductConfig(
        product_id=ProductId.QUAY,
        namespace="art-quay-tenant",
        kubeconfig_env="QUAY_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="quay-images-base-silent",
            application="quay-images-base",
        ),
        fbc_stage_release_plans={
            (3, 17): "quay-advisory-stage-3-17",
            (3, 18): "quay-advisory-stage-3-18",
        },
    ),
    ProductConfig(
        product_id=ProductId.MTC,
        namespace="art-mtc-tenant",
        kubeconfig_env="MTC_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="mtc-images-base-silent",
            application="mtc-images-base",
        ),
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-art-mtc-stage",
            fbc_policy="rhtap-releng-tenant/fbc-stage",
        ),
        fbc_stage_release_plans={(1, 8): "mtc-advisory-stage-1-8"},
        cpe_product_name="rhmt",
    ),
    ProductConfig(
        product_id=ProductId.OADP,
        namespace="art-oadp-tenant",
        kubeconfig_env="OADP_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="oadp-images-base-silent",
            application="oadp-images-base",
        ),
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-art-oadp-stage",
            fbc_policy="rhtap-releng-tenant/fbc-art-oadp-stage",
        ),
        fbc_stage_release_plans={
            (1, 3): "oadp-advisory-stage-1-3",
            (1, 4): "oadp-advisory-stage-1-4",
            (1, 5): "oadp-advisory-stage-1-5",
            (1, 6): "oadp-advisory-stage-1-6",
        },
        cpe_product_name="openshift_api_data_protection",
    ),
    ProductConfig(
        product_id=ProductId.OC_MIRROR,
        namespace="ocp-art-tenant",
        kubeconfig_env="KONFLUX_SA_KUBECONFIG",
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-standard",
        ),
    ),
    ProductConfig(
        product_id=ProductId.MIRROR_GUI,
        aliases=("mirror-gui",),
        namespace="ocp-art-tenant",
        kubeconfig_env="KONFLUX_SA_KUBECONFIG",
        conforma_stage_policies=ConformaPolicies(
            image_policy="rhtap-releng-tenant/registry-standard",
        ),
    ),
    ProductConfig(
        product_id=ProductId.RHOSDT,
        namespace="art-rhosdt-tenant",
        kubeconfig_env="RHOSDT_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="rhosdt-images-base-silent",
            application="rhosdt-images-base",
        ),
        fbc_stage_release_plans={(3, 11): "rhosdt-advisory-stage-auto-3-11"},
        cpe_product_name="openshift_distributed_tracing",
    ),
    ProductConfig(
        product_id=ProductId.ZERO_TRUST,
        namespace="art-oap-tenant",
        kubeconfig_env="OAP_KONFLUX_SA_KUBECONFIG",
        base_image_release=ReleaseTarget(
            release_plan="oap-zt-images-base-silent",
            application="oap-images-base",
        ),
        fbc_stage_release_plans={
            (1, 0): "zt-advisory-stage-auto-1-0",
            (1, 1): "zt-advisory-stage-auto-1-1",
        },
        cpe_product_name="zero_trust_workload_identity_manager",
    ),
    ProductConfig(
        product_id=ProductId.AGENT_INSTALLER,
        namespace="art-installer-agent-tenant",
        kubeconfig_env="ASSISTED_INSTALLER_SA_KUBECONFIG",
    ),
    ProductConfig(
        product_id=ProductId.SUPPLEMENTAL_TOOLS,
        namespace="ocp-art-tenant",
        kubeconfig_env="KONFLUX_SA_KUBECONFIG",
    ),
)


def _normalize_product_name(product: str) -> str:
    """
    Normalize a product name for catalog lookup.

    Arg(s):
        product: Product name to normalize.
    Return Value(s):
        str: Lowercase product name without surrounding whitespace.
    """
    return product.strip().lower()


def _product_accepts_name(config: ProductConfig, product: str) -> bool:
    """
    Return whether a product configuration owns a canonical name or alias.

    Arg(s):
        config: Product configuration to inspect.
        product: Normalized product name.
    Return Value(s):
        bool: True when the product matches the configuration.
    """
    names = (config.product_name, *config.aliases)
    return any(_normalize_product_name(name) == product for name in names)


def find_product_config(product: str) -> ProductConfig | None:
    """
    Find configuration for a canonical product name or alias.

    Arg(s):
        product: Product name to normalize and resolve.
    Return Value(s):
        ProductConfig | None: Matching configuration, or None if unknown.
    """
    normalized_product = _normalize_product_name(product)
    return next(
        (config for config in PRODUCT_CATALOG if _product_accepts_name(config, normalized_product)),
        None,
    )


def get_product_config(product: str) -> ProductConfig:
    """
    Return configuration for a canonical product name or alias.

    Arg(s):
        product: Product name to normalize and resolve.
    Return Value(s):
        ProductConfig: Matching configuration.
    Raises:
        ValueError: If the product name is unknown.
    """
    config = find_product_config(product)
    if config is None:
        known_products = ", ".join(get_product_names())
        raise ValueError(f"Unknown product '{product}'. Known products: {known_products}")
    return config


def get_product_id_for_product(product: str) -> ProductId:
    """
    Return the product identifier for a canonical product name or alias.

    Arg(s):
        product: Product name to normalize and resolve.
    Return Value(s):
        ProductId: Matching product identifier.
    Raises:
        ValueError: If the product name is unknown.
    """
    return get_product_config(product).product_id


def get_product_names() -> tuple[str, ...]:
    """
    Return canonical product names in catalog declaration order.

    Return Value(s):
        tuple[str, ...]: Canonical product names.
    """
    return tuple(config.product_name for config in PRODUCT_CATALOG)


def get_kubeconfig_env_vars() -> tuple[str, ...]:
    """
    Return unique configured kubeconfig environment variables.

    Return Value(s):
        tuple[str, ...]: Environment variable names in catalog declaration order.
    """
    return tuple(
        dict.fromkeys(config.kubeconfig_env for config in PRODUCT_CATALOG if config.kubeconfig_env is not None)
    )
