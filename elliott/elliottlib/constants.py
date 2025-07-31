"""
This file contains constants that are used to manage OCP Image and RPM builds
"""

from artcommonlib.constants import BREW_DOWNLOAD_URL, BREW_HUB, RHCOS_RELEASES_BASE_URL

CINCINNATI_BASE_URL = "https://api.openshift.com/api/upgrades_info/v1/graph"
BREW_DOWNLOAD_TEMPLATE = BREW_DOWNLOAD_URL + "/packages/{name}/{version}/{release}/files/{file_path}"
CGIT_URL = "https://pkgs.devel.redhat.com/cgit"
RESULTSDB_API_URL = "https://resultsdb-api.engineering.redhat.com"

VALID_BUG_STATES = ['NEW', 'ASSIGNED', 'POST', 'MODIFIED', 'ON_QA', 'VERIFIED', 'RELEASE_PENDING', 'CLOSED']
TRACKER_BUG_KEYWORDS = ['Security', 'SecurityTracking']
BUGZILLA_PRODUCT_OCP = 'OpenShift Container Platform'
BUG_SEVERITY_NUMBER_MAP = {
    "unspecified": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "urgent": 4,
}

# Golang builder needs special treatment when associating security tracking bugs with builds:
GOLANG_BUILDER_CVE_COMPONENT = 'openshift-golang-builder-container'

BUG_LOOKUP_CHUNK_SIZE = 100
BUG_ATTACH_CHUNK_SIZE = 100

# When severity isn't set on all tracking and flaw bugs, default to "Low"
# https://jira.coreos.com/browse/ART-1192
SECURITY_IMPACT = ["Low", "Low", "Moderate", "Important", "Critical"]
security_impact_map = {'Critical': 4, 'Important': 3, 'Moderate': 2, 'Low': 1}
errata_url = "https://errata.devel.redhat.com"
# errata_url = "https://errata.stage.engineering.redhat.com"

errata_active_advisory_labels = [
    "NEW_FILES",
    "QE",
    "REL_PREP",
    "PUSH_READY",
    "IN_PUSH",
]

errata_inactive_advisory_labels = [
    "SHIPPED_LIVE",
    "DROPPED_NO_SHIP",
]

errata_states = errata_active_advisory_labels + errata_inactive_advisory_labels

errata_shipped_advisory_label = "SHIPPED_LIVE"

# These are the types of advisories that may have definitions in group.yml
# TODO: this should probably be user-definable in some way
standard_advisory_types = [
    "rpm",
    "image",
    "extras",
    "metadata",
    "microshift",
    "prerelease",
    "advance",
    "bootimage",
]


# Valid external test (RPMDiff) statuses defined in Errata Tool.
# Note those status names are partially different from the status names in RPMDiff tool.
# https://code.engineering.redhat.com/gerrit/gitweb?p=errata-rails.git;a=blob;f=app/models/external_test_run.rb;h=eb9489c24ec70a8e6b4e54cfe6b385fb51e330b8;hb=refs/heads/master
ET_EXTERNAL_TEST_STATUSES = {
    "PASSED",
    "INELIGIBLE",
    "INFO",
    "WAIVED",
    "NEEDS_INSPECTION",
    "FAILED",
    "PENDING",
    "QUEUED_FOR_TEST",
    "RUNNING",
}

ET_COMPLETED_EXTERNAL_TEST_STATUSES = {
    "PASSED",
    "INELIGIBLE",
    "INFO",
    "WAIVED",
    "NEEDS_INSPECTION",
    "FAILED",
}

# External test statuses that are considered "good" by Errata Tool:
# https://code.engineering.redhat.com/gerrit/gitweb?p=errata-rails.git;a=blob;f=app/models/external_test_run.rb;h=eb9489c24ec70a8e6b4e54cfe6b385fb51e330b8;hb=refs/heads/master#l74
ET_GOOD_EXTERNAL_TEST_STATUSES = {
    "PASSED",
    "INELIGIBLE",
    "INFO",
    "WAIVED",
}

ET_BAD_EXTERNAL_TEST_STATUSES = {
    "NEEDS_INSPECTION",
    "FAILED",
}

ADVISORY_TYPES = ('rhba', 'rhea', 'rhsa')

SFM2_ERRATA_ALERTS_URL = "https://sfm2.prodsec.redhat.com/api/public/errata/{id}/alerts"

######################################################################
# API endpoints with string formatting placeholders as
# necessary. Index of all available endpoints is available in the
# online documentation.
#
# https://errata.devel.redhat.com/developer-guide/api-http-api.html#api-index-by-url
errata_add_bug_url = errata_url + "/api/v1/erratum/{id}/add_bug"
errata_add_build_url = errata_url + "/api/v1/erratum/{id}/add_build"
errata_add_builds_url = errata_url + "/api/v1/erratum/{id}/add_builds"
errata_add_comment_url = errata_url + "/api/v1/erratum/{id}/add_comment"
errata_bug_refresh_url = errata_url + "/api/v1/bug/refresh"
errata_change_state_url = errata_url + "/api/v1/erratum/{id}/change_state"
errata_drop_url = errata_url + "/errata/drop_errata/{id}"
errata_filter_list_url = errata_url + "/filter/{id}.json"
errata_get_build_url = errata_url + "/api/v1/build/{id}"
errata_get_builds_url = errata_url + "/api/v1/erratum/{id}/builds"
errata_get_comment_url = errata_url + "/api/v1/comments/{id}"
errata_get_comments_url = errata_url + "/api/v1/comments"
errata_get_erratum_url = errata_url + "/api/v1/erratum/{id}"
errata_post_erratum_url = errata_url + "/api/v1/erratum"
errata_get_advisories_for_bug_url = errata_url + "/bugs/{id}/advisories.json"

JIRA_API_FIELD = "https://issues.redhat.com/rest/api/2/field"

JIRA_SECURITY_ALLOWLIST = {
    "Red Hat Employee",
    "Restricted",
    "Red Hat Partner",
    "Red Hat Engineering Authorized",
    "Embargoed Security Issue",
}
