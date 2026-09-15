from enum import Enum


class BuildVariant(Enum):
    OCP = "ocp"
    OKD = "okd"
    ACM = "acm"
    CERT_MANAGER = "cert-manager"
    COO = "coo"
    EXTERNAL_SECRETS = "external-secrets"
    MCE = "mce"
    LOGGING = "logging"
    MTA = "mta"
    MTC = "mtc"
    OADP = "oadp"
    OC_MIRROR = "oc-mirror"
    ZERO_TRUST = "zero-trust"
