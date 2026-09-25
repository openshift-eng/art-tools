"""
Canonical product identifiers used across ART tooling and BigQuery.
"""

from enum import Enum


class ProductId(Enum):
    """
    Canonical product identifiers used across ART tooling and BigQuery.

    The historical BigQuery field is named ``build_variant``. The Python
    identifier is ProductId because these values identify products rather than
    separate build configurations.
    """

    OCP = "ocp"
    MICROSHIFT = "microshift"
    OKD = "okd"
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
    MIRROR_GUI = "mirror_gui"
    RHOSDT = "openshift-opentelemetry-operator"
    ZERO_TRUST = "zero-trust-workload-identity-manager"
    SUPPLEMENTAL_TOOLS = "supplemental-tools"
    AGENT_INSTALLER = "openshift_agent_installer"
