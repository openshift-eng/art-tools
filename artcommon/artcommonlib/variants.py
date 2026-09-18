import logging
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
    RHOSDT = "openshift-opentelemetry-operator"
    ZERO_TRUST = "zero-trust-workload-identity-manager"


logger = logging.getLogger(__name__)


def get_build_variant_for_product(product: str) -> BuildVariant:
    """
    Resolve a build-data product name to its build variant.

    Args:
        product: Product name from the build-data group configuration.

    Returns:
        The build variant associated with the product.

    Raises:
        ValueError: If the product does not have a defined build variant.
    """
    normalized_product = product.strip().lower()
    try:
        return BuildVariant(normalized_product)
    except ValueError:
        message = f"No build variant found for product {product}; add it to the BuildVariant enum"
        logger.error(message)
        raise ValueError(message) from None
