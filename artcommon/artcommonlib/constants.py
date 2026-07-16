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

# Last standard (non-bridge) minor for each OCP major release, used when crossing
# major version boundaries (e.g. 5.0 -> 4.22, 4.22 -> 5.0).
# Bridge releases such as 4.23 are active compat siblings of a new major (5.0)
# and must NOT be included here or in lineage calculations. Keep bridge versions
# in ACTIVE_OCP_VERSIONS only.
# Use None for major versions where the maximum minor is not yet known.
LAST_OCP_MINOR_VERSION = {
    3: 11,  # OCP 3.11 was the last 3.x release
    4: 22,  # Last standard 4.x before OCP 5.0 (excludes bridge 4.23)
    5: None,  # OCP 5.x max minor not yet known - allows infinite growth
}

# OCP 5.x compat bridge on 4.x: 5.0 -> 4.23, 5.1 -> 4.24, ...
OCP5_BRIDGE_MINOR_BASE = 23

# Konflux DB related vars
GOOGLE_CLOUD_PROJECT = 'openshift-art'
DATASET_ID = 'events'
BUILDS_TABLE_ID = 'builds'
BUNDLES_TABLE_ID = 'bundles'
FBCS_TABLE_ID = 'fbcs'
TASKRUN_TABLE_ID = 'taskruns'

SHIPMENT_DATA_URL_TEMPLATE = "https://gitlab.cee.redhat.com/hybrid-platforms/art/ocp-shipment-data"
SHIPMENT_CONFIG_KINDS = ("image", "extras", "metadata", "fbc", "microshift-bootc")

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
# Label and env var for injecting exact golang NVR into builder images
GOLANG_NVR_LABEL = 'io.openshift.build.golang-nvr'
GOLANG_NVR_ENV = '__doozer_golang_nvr'

# Product-based mappings for Konflux tenant namespaces and kubeconfigs
PRODUCT_NAMESPACE_MAP = {
    "acm": "art-acm-tenant",
    "mce": "art-acm-tenant",
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
    "supplemental-tools": "ocp-art-tenant",
    "zero-trust": "art-oap-tenant",
}

# Konflux silent base-image workflow: ReleasePlan metadata.name and Application (Snapshot/Releases spec.application).
# Must stay aligned with konflux-release-data ReleasePlan resources per product tenant.
# Konflux Application: OCP uses art-images-base; layered products use <product>-images-base (no stream suffix).
PRODUCT_BASE_IMAGE_KONFLUX_RELEASE_MAP = {
    "ocp": ("ocp-art-images-base-silent", "art-images-base"),
    "rhmtc": ("mtc-images-base-silent", "mtc-images-base"),
    "mta": ("mta-images-base-silent", "mta-images-base"),
    "oadp": ("oadp-images-base-silent", "oadp-images-base"),
    "logging": ("logging-images-base-silent", "logging-images-base"),
    "openshift-logging": ("logging-images-base-silent", "logging-images-base"),
}

# Pre-release lifecycle (software_lifecycle.phase) — registry-ocp-art-base-ec-prod via ART-19498.
PRODUCT_BASE_IMAGE_KONFLUX_EC_RELEASE_MAP = {
    "ocp": ("ocp-art-images-base-silent-ec", "art-images-base"),
}

PRODUCT_KUBECONFIG_MAP = {
    "acm": "ACM_KONFLUX_SA_KUBECONFIG",
    "mce": "ACM_KONFLUX_SA_KUBECONFIG",
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
    "supplemental-tools": "KONFLUX_SA_KUBECONFIG",
    "zero-trust": "OAP_KONFLUX_SA_KUBECONFIG",
}

# Default namespace for Konflux operations
KONFLUX_DEFAULT_NAMESPACE = "ocp-art-tenant"

# Base URL for fetching ReleasePlanAdmission YAMLs from konflux-release-data (GitLab raw)
OCP_RPA_BASE_URL = (
    "https://gitlab.cee.redhat.com/releng/konflux-release-data/-/raw/main/"
    "config/kflux-ocp-p01.7ayg.p1/product/ReleasePlanAdmission/ocp-art"
)
OCP_RPA_KINDS = {
    "image": "ocp-art-advisory",
    "metadata": "ocp-art-advisory",
    "extras": "ocp-art-advisory",
    "microshift-bootc": "ocp-art-advisory",
}
OCP_RPA_ENVS = ["stage", "prod"]

COREOS_RHEL10_STREAMS = [
    "rhel-coreos-10",
    "rhel-coreos-10-extensions",
]
# Legacy constant removed - use get_art_prod_image_repo_for_version() from artcommonlib.util instead
