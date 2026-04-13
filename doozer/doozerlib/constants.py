RC_BASE_URL = "https://{arch}.ocp.releases.ci.openshift.org"
RC_BASE_PRIV_URL = "https://{arch}.ocp.internal.releases.ci.openshift.org"

BREWWEB_URL = "https://brewweb.engineering.redhat.com/brew"
DISTGIT_GIT_URL = "git+https://pkgs.devel.redhat.com/git"

# Environment variables that should be set for doozer interaction with db for storing and retrieving build records.
# DB ENV VARS
DB_HOST = "DOOZER_DB_HOST"
DB_PORT = "DOOZER_DB_PORT"
DB_USER = "DOOZER_DB_USER"
DB_PWD_NAME = "DOOZER_DB_PASSWORD"
DB_NAME = "DOOZER_DB_NAME"

# default db parameters
default_db_params = {
    DB_NAME: "doozer_build",
    DB_HOST: "localhost",
    DB_PORT: "3306",
}

# TODO: once brew outage is resolved, change to 6 hours again (currently set to 100)
BREW_BUILD_TIMEOUT = 100 * 60 * 60  # how long we wait before canceling a task

# In Brew, 'ADD tls-ca-bundle.pem /tmp/tls-ca-bundle.pem' is injected during build and 'sslcacert=/tmp/tls-ca-bundle.pem'
# is specified for CA cert, so that 'sslverify=true' can be used in yum repos.
# To achieve the same in Konflux, we need to 'ADD tls-ca-bundle.pem /tmp/tls-ca-bundle.pem' in every Dockerfile stage
# and set 'sslcacert=/tmp/Current-IT-Root-CAs.pem'
KONFLUX_REPO_CA_BUNDLE_TMP_PATH = "/tmp/art"
KONFLUX_REPO_CA_BUNDLE_FILENAME = "Current-IT-Root-CAs.pem"
KONFLUX_REPO_CA_BUNDLE_HOST = "https://certs.corp.redhat.com/certs"
WORKING_SUBDIR_KONFLUX_BUILD_SOURCES = "konflux_build_sources"
WORKING_SUBDIR_KONFLUX_FBC_SOURCES = "konflux_fbc_sources"
WORKING_SUBDIR_KONFLUX_OKD_SOURCES = "konflux_okd_sources"
KONFLUX_DEFAULT_PIPELINERUN_SERVICE_ACCOUNT = "appstudio-pipeline"
KONFLUX_DEFAULT_PIPELINERUN_TIMEOUT = "1h0m0s"
KONFLUX_DEFAULT_PIPRLINE_DOCKER_BUILD_BUNDLE_PULLSPEC = "quay.io/konflux-ci/tekton-catalog/pipeline-docker-build:devel"
KONFLUX_DEFAULT_IMAGE_REPO = (
    "quay.io/redhat-user-workloads/ocp-art-tenant/art-images"  # FIXME: If we change clusters this URL will change
)
KONFLUX_DEFAULT_FBC_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"
# Legacy constants removed - use get_art_prod_image_repo_for_version() from artcommonlib.util instead
DELIVERY_IMAGE_REGISTRY = "registry.redhat.io"
KONFLUX_UI_HOST = "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com"
KONFLUX_UI_DEFAULT_WORKSPACE = "ocp-art"  # associated with ocp-art-tenant
KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-konflux-template-push.yaml?ref=main"
KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-bundle-konflux-template-push.yaml?ref=main"
KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-fbc-konflux-template-push.yaml?ref=main"
ART_FBC_GIT_REPO = "https://github.com/openshift-priv/art-fbc.git"
REGISTRY_PROXY_BASE_URL = "registry-proxy.engineering.redhat.com"
BREW_REGISTRY_BASE_URL = "brew.registry.redhat.io"

ART_BUILD_HISTORY_URL = 'https://art-build-history-art-build-history.apps.artc2023.pc3z.p1.openshiftapps.com'

# Enterprise Contract (EC) verification pipeline constants
KONFLUX_EC_PIPELINE_GIT_URL = "https://github.com/konflux-ci/build-definitions"
KONFLUX_EC_PIPELINE_REVISION = "main"
KONFLUX_EC_PIPELINE_PATH = "pipelines/enterprise-contract.yaml"

# Shared namespace for all EC policies in konflux-release-data
KONFLUX_EC_POLICY_NAMESPACE = "rhtap-releng-tenant"

# Product-to-EC-policy mappings (bare policy names; prepend KONFLUX_EC_POLICY_NAMESPACE at lookup time).
# Policy YAMLs live in konflux-release-data under config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/.
PRODUCT_EC_POLICY_MAP = {
    "ocp": "registry-ocp-art-stage",
    "logging": "registry-art-logging-stage",
    "mta": "registry-art-mta-stage",
    "rhmtc": "registry-art-mtc-stage",
    "oadp": "registry-art-oadp-stage",
}

PRODUCT_FBC_EC_POLICY_MAP = {
    "logging": "fbc-ocp-art-stage",
    "mta": "fbc-ocp-art-stage",
    "rhmtc": "fbc-ocp-art-stage",
    "oadp": "fbc-art-oadp-stage",
}

KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION = f"{KONFLUX_EC_POLICY_NAMESPACE}/registry-ocp-art-stage"
# PreGA (PREVIEW assembly) EC policy: same as stage but allows unsigned RPMs
# https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-ec-stage.yaml
KONFLUX_PREGA_EC_POLICY_CONFIGURATION = f"{KONFLUX_EC_POLICY_NAMESPACE}/registry-ocp-art-ec-stage"
# Base image EC policy (base_only images use a dedicated prod policy for all assembly types)
# https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-base-prod.yaml
KONFLUX_BASE_IMAGE_EC_POLICY_CONFIGURATION = f"{KONFLUX_EC_POLICY_NAMESPACE}/registry-ocp-art-base-prod"


def get_ec_policy_for_product(product: str) -> str:
    """Resolve the registry EC policy for a given product, falling back to OCP default."""
    policy_name = PRODUCT_EC_POLICY_MAP.get(product)
    if policy_name:
        return f"{KONFLUX_EC_POLICY_NAMESPACE}/{policy_name}"
    return KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION


def get_fbc_ec_policy_for_product(product: str) -> str:
    """Resolve the FBC EC policy for a given product.

    Only products in PRODUCT_FBC_EC_POLICY_MAP should call this; callers gate
    on map membership first. The fallback is purely defensive.
    """
    policy_name = PRODUCT_FBC_EC_POLICY_MAP.get(product)
    if policy_name:
        return f"{KONFLUX_EC_POLICY_NAMESPACE}/{policy_name}"
    return KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION
