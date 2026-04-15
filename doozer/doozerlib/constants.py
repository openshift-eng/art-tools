RC_BASE_URL = "https://{arch}.ocp.releases.ci.openshift.org"
RC_BASE_PRIV_URL = "https://{arch}.ocp.internal.releases.ci.openshift.org"

BREWWEB_URL = "https://brewweb.engineering.redhat.com/brew"
DISTGIT_GIT_URL = "git+https://pkgs.devel.redhat.com/git"

# Doozer used to be part of OIT
OIT_COMMENT_PREFIX = '#oit##'
OIT_BEGIN = '##OIT_BEGIN'
OIT_END = '##OIT_END'

CONTAINER_YAML_HEADER = """
# This file is managed by Doozer: https://github.com/openshift-eng/art-tools/tree/main/doozer
# operated by the OpenShift Automated Release Tooling team (#forum-ocp-art on CoreOS Slack).

# Any manual changes will be overwritten by Doozer on the next build.
#
# See https://source.redhat.com/groups/public/container-build-system/container_build_system_wiki/odcs_integration_with_osbs
# for more information on maintaining this file and the format and examples

---
"""

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
KONFLUX_DEFAULT_IMAGE_REPO = (
    "quay.io/redhat-user-workloads/ocp-art-tenant/art-images"  # FIXME: If we change clusters this URL will change
)
KONFLUX_DEFAULT_FBC_REPO = "quay.io/redhat-user-workloads/ocp-art-tenant/art-fbc"
# Legacy constants removed - use get_art_prod_image_repo_for_version() from artcommonlib.util instead
DELIVERY_IMAGE_REGISTRY = "registry.redhat.io"
KONFLUX_UI_HOST = "https://konflux-ui.apps.kflux-ocp-p01.7ayg.p1.openshiftapps.com"
KONFLUX_DEFAULT_IMAGE_BUILD_PLR_TEMPLATE_URL = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-konflux-template-push.yaml?ref=main"
KONFLUX_DEFAULT_BUNDLE_BUILD_PLR_TEMPLATE_URL = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-bundle-konflux-template-push.yaml?ref=main"
KONFLUX_DEFAULT_FBC_BUILD_PLR_TEMPLATE_URL = "https://api.github.com/repos/openshift-priv/art-konflux-template/contents/.tekton/art-fbc-konflux-template-push.yaml?ref=main"
ART_FBC_GIT_REPO = "https://github.com/openshift-priv/art-fbc.git"
REGISTRY_PROXY_BASE_URL = "registry-proxy.engineering.redhat.com"
BREW_REGISTRY_BASE_URL = "brew.registry.redhat.io"

ART_BUILD_HISTORY_URL = 'https://art-build-history-art-build-history.apps.artc2023.pc3z.p1.openshiftapps.com'

# Enterprise Contract (EC) verification pipeline constants
# TODO: Expand EC verification to layered products (logging, oadp, mta, rhmtc, quay, cert-manager, etc.)
# Currently scoped to OCP only.
KONFLUX_EC_PIPELINE_GIT_URL = "https://github.com/konflux-ci/build-definitions"
KONFLUX_EC_PIPELINE_REVISION = "main"
KONFLUX_EC_PIPELINE_PATH = "pipelines/enterprise-contract.yaml"
KONFLUX_DEFAULT_EC_POLICY_CONFIGURATION = "rhtap-releng-tenant/registry-ocp-art-stage"
# PreGA (PREVIEW assembly) EC policy: same as stage but allows unsigned RPMs
# https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-ec-stage.yaml
KONFLUX_PREGA_EC_POLICY_CONFIGURATION = "rhtap-releng-tenant/registry-ocp-art-ec-stage"
# Base image EC policy (base_only images use a dedicated prod policy for all assembly types)
# https://gitlab.cee.redhat.com/releng/konflux-release-data/-/blob/main/config/kflux-ocp-p01.7ayg.p1/product/EnterpriseContractPolicy/registry-ocp-art-base-prod.yaml
KONFLUX_BASE_IMAGE_EC_POLICY_CONFIGURATION = "rhtap-releng-tenant/registry-ocp-art-base-prod"
