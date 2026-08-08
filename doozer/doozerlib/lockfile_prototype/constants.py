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
MAX_REINSTALL_STRIP_RETRIES = 5
DEFAULT_PLATFORM = "linux/amd64"

SYSTEM_PYTHON = "/usr/bin/python3"
RPM_LOCKFILE_ENTRY_POINT = "from rpm_lockfile import main; main()"

# RPM pseudo-packages that appear in rpmdb but are not installable via DNF
RPM_PSEUDO_PACKAGES = frozenset({"gpg-pubkey"})

VALID_PKG_NAME = re.compile(r"^[a-zA-Z0-9*][a-zA-Z0-9._+\-*]*$")

# rpm-lockfile-prototype stores extracted RPMDBs under
# $XDG_CACHE_HOME/rpm-lockfile-prototype/rpmdbs (defaulting to ~/.cache).
# We set XDG_CACHE_HOME in the subprocess env (see resolver.py) so that
# the cache lands on persistent workspace storage in Jenkins instead of ~/.cache.
RPMDB_CACHE_SUBDIR = Path("rpm-lockfile-prototype") / "rpmdbs"

# Persistent cache root on Jenkins agents. This volume survives across job
# runs, unlike the per-job workspace which is cleaned up after each build.
JENKINS_CACHE_DIR = Path("/mnt/jenkins-workspace/rpm-lockfile-cache")

# Patterns in rpm-lockfile-prototype stderr that indicate a broken RPMDB
# cache entry. When matched, doozer clears the cached entry and retries.
RPMDB_CACHE_ERROR_PATTERNS = [
    "database disk image is malformed",
    "failed loading RPMDB",
    "No such file or directory",
]


class LockfileBackend(str, Enum):
    ART_INTERNAL = "art-internal"
    RPM_LOCKFILE_PROTOTYPE = "rpm-lockfile-prototype"
