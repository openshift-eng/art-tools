PLASHET_REMOTES = [
    {
        'url': 'https://ocp-artifacts-art--runtime-int.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com/pub/RHOCP/plashets',
        'host': 'ocp-artifacts',
    },
]

PLASHET_REMOTE_BASE_DIR = "/mnt/data/pub/RHOCP/plashets"

SPMM_UTILS_REMOTE_HOST = "exd-ocp-buildvm-bot-prod@spmm-util"

RELEASE_IMAGE_REPO = "quay.io/openshift-release-dev/ocp-release"

GIT_AUTHOR = "AOS Automation Release Team <noreply@redhat.com>"

NIGHTLY_PAYLOAD_REPOS = {
    "x86_64": "registry.ci.openshift.org/ocp/release",
    "s390x": "registry.ci.openshift.org/ocp-s390x/release-s390x",
    "ppc64le": "registry.ci.openshift.org/ocp-ppc64le/release-ppc64le",
    "aarch64": "registry.ci.openshift.org/ocp-arm64/release-arm64",
}

# Maps the name of a release component tag to the filename element to include
# when creating artifacts on mirror.openshift.com.
MIRROR_CLIENTS = {
    "cli": "openshift-client",
    "installer": "openshift-installer",
    "operator-registry": "opm",
}

OCP_BUILD_DATA_URL = 'https://github.com/openshift-eng/ocp-build-data'

# This is the URL that buildvm itself uses to resolve Jenkins
# an alternative to JENKINS_URL env var set by Jenkins
JENKINS_SERVER_URL = 'https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com'

# This is the URL that humans behind a VPN use to browse Jenkins UI
# It shall be used to print clickable logs that redirect the user to the triggered job page
JENKINS_UI_URL = 'https://art-jenkins.apps.prod-stable-spoke1-dc-iad2.itup.redhat.com'

MIRROR_BASE_URL = 'https://mirror.openshift.com'

UMB_BROKERS = {
    "prod": "stomp+ssl://umb.api.redhat.com:61612",
    "stage": "stomp+ssl://umb.stage.api.redhat.com:61612",
    "qa": "stomp+ssl://umb.qa.api.redhat.com:61612",
    "dev": "stomp+ssl://umb.dev.api.redhat.com:61612",
}

GITHUB_OWNER = "openshift-eng"

KONFLUX_IMAGE_BUILD_PLR_TEMPLATE_URL_FORMAT = "https://api.github.com/repos/{owner}/art-konflux-template/contents/.tekton/art-konflux-template-push.yaml?ref={branch_name}"  # Konflux PipelineRun (PLR) template for image builds
KONFLUX_BUNDLE_BUILD_PLR_TEMPLATE_URL_FORMAT = "https://api.github.com/repos/{owner}/art-konflux-template/contents/.tekton/art-bundle-konflux-template-push.yaml?ref={branch_name}"  # Konflux PipelineRun (PLR) template for bundle builds
KONFLUX_FBC_BUILD_PLR_TEMPLATE_URL_FORMAT = "https://api.github.com/repos/{owner}/art-konflux-template/contents/.tekton/art-fbc-konflux-template-push.yaml?ref={branch_name}"  # Konflux PipelineRun (PLR) template for FBC builds

# OKD build triggering is enabled only for these OCP versions
OKD_ENABLED_VERSIONS = ['4.21', '4.22']
