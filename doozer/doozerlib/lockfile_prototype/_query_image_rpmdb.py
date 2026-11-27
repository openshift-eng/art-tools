"""
Print the RPM inventory of an image as tab-separated package names and EVRs.

Run with system Python so rpm-lockfile-prototype can import the DNF bindings.
Example: /usr/bin/python3 _query_image_rpmdb.py /tmp/rpmdb registry.example.com/base:tag s390x
"""

import subprocess
import sys
from pathlib import Path

from rpm_lockfile import containers, utils


def _query_image_rpmdb(root: Path, image: str, arch: str) -> None:
    """
    Extract the image's RPM database and print its installed packages.
    """
    containers.setup_rpmdb(root, image, arch)
    subprocess.run(
        [
            "rpm",
            "--dbpath",
            str(root / utils.RPMDB_PATH),
            "-qa",
            "--qf",
            r"%{NAME}\t%{EPOCHNUM}:%{VERSION}-%{RELEASE}\n",
        ],
        check=True,
    )


if __name__ == "__main__":
    root, image, arch = sys.argv[1:]
    _query_image_rpmdb(Path(root), image, arch)
