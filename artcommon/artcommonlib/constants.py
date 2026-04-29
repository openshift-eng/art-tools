# constants shared across multiple sub-projects

RHCOS_RELEASES_BASE_URL = (
    "https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/releases"
)
RHCOS_RELEASES_STREAM_URL = (
    "https://releases-rhcos--prod-pipeline.apps.int.prod-stable-spoke1-dc-iad2.itup.redhat.com/storage/prod/streams"
)
BREW_HUB = "https://brewhub.engineering.redhat.com/brewhub"
BREW_DOWNLOAD_URL = "https://download.devel.redhat.com/brewroot"
RELEASE_SCHEDULES = "https://pp.engineering.redhat.com/api/v7/releases"
DEFAULT_PLASHET_BASE_URL = "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets"

# Environment variables to disable Git stdin prompts for username, password, etc
GIT_NO_PROMPTS = {
    "GIT_SSH_COMMAND": "ssh -oBatchMode=yes",
    "GIT_TERMINAL_PROMPT": "0",
}

ACTIVE_OCP_VERSIONS = [
    "4.12",
    "4.13",
    "4.14",
    "4.15",
    "4.16",
    "4.17",
    "4.18",
    "4.19",
    "4.20",
    "4.21",
    "4.22",
    "4.23",
    "5.0",
]

# Last known minor version for each OCP major release
# Update these values as new versions are released
# Note: This is OCP-specific; art-tools also works with other products (OADP, MTA, etc.)
# which have their own versioning schemes
# Use None for major versions where the maximum minor is not yet known
LAST_OCP_MINOR_VERSION = {
    3: 11,  # OCP 3.11 was the last 3.x release
    4: 22,  # Current highest known 4.x minor (update as versions are released)
    5: None,  # OCP 5.x max minor not yet known - allows infinite growth
}

# Konflux DB related vars
GOOGLE_CLOUD_PROJECT = 'openshift-art'
DATASET_ID = 'events'
BUILDS_TABLE_ID = 'builds'
BUNDLES_TABLE_ID = 'bundles'
FBCS_TABLE_ID = 'fbcs'
TASKRUN_TABLE_ID = 'taskruns'

SHIPMENT_DATA_URL_TEMPLATE = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data"

# Redis related vars
REDIS_HOST = 'master.redis.gwprhd.use1.cache.amazonaws.com'
REDIS_PORT = '6379'

# Telemetry
OTEL_EXPORTER_OTLP_ENDPOINT = "http://otel-collector-psi-rhv.hosts.prod.psi.rdu2.redhat.com:4317"

# Registry paths for authentication and image operations
REGISTRY_QUAY_OCP_RELEASE_DEV = "quay.io/openshift-release-dev"
REGISTRY_QUAY_OPENSHIFT = "quay.io/openshift"
REGISTRY_QUAY_CI = "quay.io/openshift/ci"
REGISTRY_CI_OPENSHIFT = "registry.ci.openshift.org"
REGISTRY_REDHAT_IO = "registry.redhat.io"
REGISTRY_BREW = "brew.registry.redhat.io"
KONFLUX_DEFAULT_IMAGE_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images"
KONFLUX_DEFAULT_IMAGE_SHARE_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images-share"
KONFLUX_DEFAULT_FBC_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"

KONFLUX_DEFAULT_BUILD_PRIORITY = 5

# Golang builder image metadata key
GOLANG_BUILDER_IMAGE_NAME = 'openshift-golang-builder'
# Golang rpm package name
GOLANG_RPM_PACKAGE_NAME = 'golang'

# Product-based mappings for Konflux tenant namespaces and kubeconfigs
PRODUCT_NAMESPACE_MAP = {
    "cert-manager": "art-oap-tenant",
    "external-secrets": "art-oap-tenant",
    "installer-ove-ui": "art-installer-agent-tenant",
    "logging": "art-logging-tenant",
    "mta": "art-mta-tenant",
    "oadp": "art-oadp-tenant",
    "ocp": "ocp-art-tenant",
    "openshift-logging": "art-logging-tenant",
    "quay": "art-quay-tenant",
    "rhmtc": "art-mtc-tenant",
    "zero-trust": "art-oap-tenant",
}

PRODUCT_KUBECONFIG_MAP = {
    "cert-manager": "OAP_KONFLUX_SA_KUBECONFIG",
    "external-secrets": "OAP_KONFLUX_SA_KUBECONFIG",
    "installer-ove-ui": "ASSISTED_INSTALLER_SA_KUBECONFIG",
    "logging": "LOGGING_KONFLUX_SA_KUBECONFIG",
    "mta": "MTA_KONFLUX_SA_KUBECONFIG",
    "oadp": "OADP_KONFLUX_SA_KUBECONFIG",
    "ocp": "KONFLUX_SA_KUBECONFIG",
    "openshift-logging": "LOGGING_KONFLUX_SA_KUBECONFIG",
    "quay": "QUAY_KONFLUX_SA_KUBECONFIG",
    "rhmtc": "MTC_KONFLUX_SA_KUBECONFIG",
    "zero-trust": "OAP_KONFLUX_SA_KUBECONFIG",
}

# Default namespace for Konflux operations
KONFLUX_DEFAULT_NAMESPACE = "ocp-art-tenant"
COREOS_RHEL10_STREAMS = [
    "rhel-coreos-10",
    "rhel-coreos-10-extensions",
]
# Legacy constant removed - use get_art_prod_image_repo_for_version() from artcommonlib.util instead
