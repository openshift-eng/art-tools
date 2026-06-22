"""
Constants and enums for the lockfile prototype package.
"""

import re
from enum import Enum
from pathlib import Path

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
ARCH_SUBSHELL_KEYWORDS = ("$(arch)", "$(uname -m)", "$(uname -p)", "$(go env GOARCH)")

# Shell variable names that hold the current architecture
ARCH_VAR_NAMES = ("HOSTTYPE", "ARCH", "GOARCH")

# All arch keywords: subshells + variables in both $VAR and ${VAR} forms
ARCH_KEYWORDS = ARCH_SUBSHELL_KEYWORDS + tuple(form for name in ARCH_VAR_NAMES for form in (f"${name}", f"${{{name}}}"))

# RPM pseudo-packages that appear in rpmdb but are not installable via DNF
RPM_PSEUDO_PACKAGES = frozenset({"gpg-pubkey"})

VALID_PKG_NAME = re.compile(r"^[a-zA-Z0-9*][a-zA-Z0-9._+\-*]*$")


# rpm-lockfile-prototype stores extracted RPMDBs here. There is no env var
# to override this path — only RPM_LOCKFILE_PROTOTYPE_DNF_CACHE controls
# the DNF repodata cache, not the RPMDB cache.
RPMDB_CACHE_PATH = Path.home() / ".cache" / "rpm-lockfile-prototype" / "rpmdbs"

# Patterns in rpm-lockfile-prototype stderr that indicate a broken RPMDB
# cache entry. When matched, doozer clears the cached entry and retries.
RPMDB_CACHE_ERROR_PATTERNS = [
    "database disk image is malformed",
    "failed loading RPMDB",
]


class LockfileBackend(str, Enum):
    ART_INTERNAL = "art-internal"
    RPM_LOCKFILE_PROTOTYPE = "rpm-lockfile-prototype"
