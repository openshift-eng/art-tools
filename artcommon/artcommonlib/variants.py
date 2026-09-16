from enum import Enum


class BuildVariant(Enum):
    OCP = "ocp"
    # MicroShift is derived from OCP but uses a distinct build variant.
    MICROSHIFT = "microshift"
    OKD = "okd"

    # Layered product variants.
    ACM = "rhacm2"
    CERT_MANAGER = "cert-manager"
    COO = "cluster-observability-operator"
    EXTERNAL_SECRETS = "external-secrets-operator"
    MCE = "multicluster-engine"
    LOGGING = "openshift-logging"
    MTA = "mta"
    QUAY = "quay"
    MTC = "rhmtc"
    OADP = "oadp"
    OC_MIRROR = "oc-mirror"
    MIRROR_GUI = "mirror-gui"
    ZERO_TRUST = "zero-trust-workload-identity-manager"


def get_build_variant_for_product(product: str) -> BuildVariant | None:
    """
    Resolve a build-data product name to its build variant.

    Args:
        product: Product name from the build-data group configuration.

    Returns:
        The build variant associated with the product, or None when the product
        does not have a supported build variant yet.
    """
    normalized_product = product.strip().lower()
    try:
        return BuildVariant(normalized_product)
    except ValueError:
        return None
