from enum import Enum
from typing import Dict, List, Union


class VerifyIssueCode(Enum):
    def __str__(self):
        return self.value

    # Bug validations

    INVALID_TARGET_RELEASE = "invalid_target_release"
    INVALID_TRACKER_BUGS = "invalid_tracker_bugs"
    INVALID_BUG_STATUS = "invalid_bug_status"

    BUGS_MULTIPLE_ADVISORIES = "bugs_multiple_advisories"

    PARENT_BUG_WRONG_STATUS = "parent_bug_wrong_status"
    PARENT_BUG_NOT_SHIPPING = "parent_bug_not_shipping"

    # Advisory validations

    WRONG_ADVISORY_TYPE = "wrong_advisory_type"

    MISSING_FLAW_BUGS = "missing_flaw_bugs"
    EXTRA_FLAW_BUGS = "extra_flaw_bugs"

    EXTRA_CVE_EXCLUSIONS = "extra_cve_exclusions"
    MISSING_CVE_EXCLUSIONS = "missing_cve_exclusions"

    EXTRA_CVE_NAMES_IN_ADVISORY = "extra_cve_names_in_advisory"
    MISSING_CVE_NAMES_IN_ADVISORY = "missing_cve_names_in_advisory"

    MISSING_BUGS_IN_ADVISORY = "missing_bugs_in_advisory"
    EXTRA_BUGS_IN_ADVISORY = "extra_bugs_in_advisory"

    TRACKER_BUGS_NO_BUILDS = "tracker_bugs_no_builds"

    # Other

    VALIDATION_ERROR = "validation_error"


class VerifyIssue:
    def __init__(self, code: VerifyIssueCode, message: str,
                 bugs: List[str] = None, advisory: Union[int, str] = None, data: Dict = None):
        """
        :param code: The code of the issue.
        :param message: The description of the issue.
        :param bugs: The list of bugs related to the issue, used for bug validations.
        :param advisory: The advisory related to the issue, used for advisory validations.
        """
        self.code = code
        self.message = message
        self.bugs = bugs
        self.advisory = advisory

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message

    def to_dict(self):
        d = {
            "code": self.code.value,
            "message": self.message,
        }
        if self.bugs:
            d["bugs"] = self.bugs
        if self.advisory:
            d["advisory"] = self.advisory
        return d
