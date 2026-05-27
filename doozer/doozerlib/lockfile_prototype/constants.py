"""
Constants and enums for the lockfile prototype package.
"""

from enum import Enum

DEFAULT_RPM_LOCKFILE_NAME = "rpms.lock.yaml"
DEFAULT_RPM_INFILE_NAME = "rpms.in.yaml"
DOCKERFILE_NAME = "Dockerfile"

DIGEST_PREFIX = "@sha256:"
BASEARCH_VAR = "$basearch"

LOCKFILE_VERSION = 1
LOCKFILE_VENDOR = "redhat"

MAX_RESOLUTION_RETRIES = 5
DEFAULT_PLATFORM = "linux/amd64"

SYSTEM_PYTHON = "/usr/bin/python3"
RPM_LOCKFILE_ENTRY_POINT = "from rpm_lockfile import main; main()"

# Shell subshell expressions that evaluate to the current architecture
ARCH_SUBSHELL_KEYWORDS = ("$(arch)", "$(uname -m)", "$(uname -p)")

# Shell variable names that hold the current architecture
ARCH_VAR_NAMES = ("HOSTTYPE", "ARCH")

# All arch keywords: subshells + variables in both $VAR and ${VAR} forms
ARCH_KEYWORDS = ARCH_SUBSHELL_KEYWORDS + tuple(form for name in ARCH_VAR_NAMES for form in (f"${name}", f"${{{name}}}"))

# RPM pseudo-packages that appear in rpmdb but are not installable via DNF
RPM_PSEUDO_PACKAGES = frozenset({"gpg-pubkey"})


class LockfileBackend(str, Enum):
    ART_INTERNAL = "art-internal"
    RPM_LOCKFILE_PROTOTYPE = "rpm-lockfile-prototype"
