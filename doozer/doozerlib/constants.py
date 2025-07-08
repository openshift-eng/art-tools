RC_BASE_URL = "https://{arch}.ocp.releases.ci.openshift.org"
RC_BASE_PRIV_URL = "https://{arch}.ocp.internal.releases.ci.openshift.org"

GITHUB_TOKEN = "GITHUB_TOKEN"
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
KONFLUX_DEFAULT_PIPELINERUN_SERVICE_ACCOUNT = "appstudio-pipeline"
KONFLUX_DEFAULT_PIPELINERUN_TIMEOUT = "1h0m0s"
KONFLUX_DEFAULT_PIPRLINE_DOCKER_BUILD_BUNDLE_PULLSPEC = "quay.io/konflux-ci/tekton-catalog/pipeline-docker-build:devel"
KONFLUX_DEFAULT_IMAGE_REPO = (
    "quay.io/redhat-user-workloads/ocp-art-tenant/art-images"  # FIXME: If we change clusters this URL will change
)
KONFLUX_PUBLIC_QUAY_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-images-public"
KONFLUX_DEFAULT_FBC_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"
ART_PROD_IMAGE_REPO = "quay.io/openshift-release-dev/ocp-v4.0-art-dev"
ART_PROD_PRIV_IMAGE_REPO = "quay.io/openshift-release-dev/ocp-v4.0-art-dev-priv"
DELIVERY_IMAGE_REGISTRY = "registry.redhat.io"
KONFLUX_UI_HOST = "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com"
KONFLUX_UI_DEFAULT_WORKSPACE = "ocp-art"  # associated with ocp-art-tenant
KONFLUX_DEFAULT_NAMESPACE = f"{KONFLUX_UI_DEFAULT_WORKSPACE}-tenant"
MAX_KONFLUX_BUILD_QUEUE_SIZE = 100  # how many concurrent Konflux pipeline can we spawn per OCP version?
KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL = (
    "https://github.com/openshift-priv/art-konflux-template/raw/refs/heads/main/.tekton/art-konflux-template-push.yaml"
)
KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL = "https://github.com/openshift-priv/art-konflux-template/raw/refs/heads/main/.tekton/art-bundle-konflux-template-push.yaml"
KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL = "https://github.com/openshift-priv/art-konflux-template/raw/refs/heads/main/.tekton/art-fbc-konflux-template-push.yaml"
ART_FBC_GIT_REPO = "git@github.com:openshift-priv/art-fbc.git"
REGISTRY_PROXY_BASE_URL = "registry-proxy.engineering.redhat.com"
BREW_REGISTRY_BASE_URL = "brew.registry.redhat.io"

ART_BUILD_HISTORY_URL = 'https://art-build-history-art-build-history.apps.artc2023.pc3z.p1.openshiftapps.com'
