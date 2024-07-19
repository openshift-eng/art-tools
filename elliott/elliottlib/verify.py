from enum import Enum
from typing import Iterable


def stringify(i, sort=True):
    """
    Convert iterable containing strings to a string without single quotes
    This is useful when dumping data to JSON/YAML for humans to read
    Bad for serialization
    """
    if not isinstance(i, Iterable):
        return str(i)
    # sort it so it's deterministic unless told not to
    i = sorted(i) if sort else i
    return str(i).replace("'", "")


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
    def __init__(self, code: VerifyIssueCode, message: str):
        """
        :param code: The code of the issue.
        :param message: The description of the issue with all the necessary details.
        """
        self.code = code
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message

    def to_dict(self):
        return {
            "code": self.code.value,
            "message": self.message,
        }
