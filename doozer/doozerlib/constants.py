RC_BASE_URL = "https://{arch}.ocp.releases.ci.openshift.org"
RC_BASE_PRIV_URL = "https://openshift-release{arch}-priv.apps.ci.l2s4.p1.openshiftapps.com"

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
    DB_PORT: "3306"
}

# TODO: once brew outage is resolved, change to 6 hours again (currently set to 100)
BREW_BUILD_TIMEOUT = 100 * 60 * 60  # how long we wait before canceling a task

# In Brew, 'ADD tls-ca-bundle.pem /tmp/tls-ca-bundle.pem' is injected during build and 'sslcacert=/tmp/tls-ca-bundle.pem'
# is specified for CA cert, so that 'sslverify=true' can be used in yum repos.
# To achieve the same in Konflux, we need to 'ADD tls-ca-bundle.pem /tmp/tls-ca-bundle.pem' in every Dockerfile stage
# and set 'sslcacert=/tmp/Current-IT-Root-CAs.pem'
KONFLUX_REPO_CA_BUNDLE_TMP_PATH = "/tmp"
KONFLUX_REPO_CA_BUNDLE_FILENAME = "Current-IT-Root-CAs.pem"
KONFLUX_REPO_CA_BUNDLE_HOST = "https://certs.corp.redhat.com/certs"
WORKING_SUBDIR_KONFLUX_BUILD_SOURCES = "konflux_build_sources"
KONFLUX_DEFAULT_PIPELINERUN_SERVICE_ACCOUNT = "appstudio-pipeline"
KONFLUX_DEFAULT_PIPELINERUN_TIMEOUT = "1h0m0s"
KONFLUX_DEFAULT_PIPRLINE_DOCKER_BUILD_BUNDLE_PULLSPEC = "quay.io/konflux-ci/tekton-catalog/pipeline-docker-build:devel"
KONFLUX_DEFAULT_DEST_IMAGE_REPO = "quay.io/yuxzhu/test-ocp-v4-art-konflux-dev"  # FIXME: This is a temporary repo.


REGISTRY_PROXY_BASE_URL = "registry-proxy.engineering.redhat.com"
BREW_REGISTRY_BASE_URL = "brew.registry.redhat.io"
