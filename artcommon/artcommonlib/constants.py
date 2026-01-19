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

ACTIVE_OCP_VERSIONS = ["4.12", "4.13", "4.14", "4.15", "4.16", "4.17", "4.18", "4.19", "4.20", "4.21", "4.22"]

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

KONFLUX_ART_IMAGES_SHARE = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images-share"

KONFLUX_DEFAULT_BUILD_PRIORITY = 5

# Golang builder image metadata key
GOLANG_BUILDER_IMAGE_NAME = 'openshift-golang-builder'
# Golang rpm package name
GOLANG_RPM_PACKAGE_NAME = 'golang'

# Product-based mappings for Konflux tenant namespaces and kubeconfigs
PRODUCT_NAMESPACE_MAP = {
    "oadp": "art-oadp-tenant",
    "mta": "art-mta-tenant",
    "rhmtc": "art-mtc-tenant",
    "logging": "art-logging-tenant",
    "ocp": "ocp-art-tenant",
}

PRODUCT_KUBECONFIG_MAP = {
    "oadp": "OADP_KONFLUX_SA_KUBECONFIG",
    "mta": "MTA_KONFLUX_SA_KUBECONFIG",
    "rhmtc": "MTC_KONFLUX_SA_KUBECONFIG",
    "logging": "LOGGING_KONFLUX_SA_KUBECONFIG",
    "ocp": "KONFLUX_SA_KUBECONFIG",
}

# Default namespace for Konflux operations
KONFLUX_DEFAULT_NAMESPACE = "ocp-art-tenant"
COREOS_RHEL10_STREAMS = [
    "rhel-coreos-10",
    "rhel-coreos-10-extensions",
]
ART_PROD_IMAGE_REPO = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
