import sys
from importlib.metadata import PackageNotFoundError, version
from typing import cast

if sys.version_info < (3, 11):
    sys.exit("Sorry, Python < 3.11 is no longer supported.")

from elliottlib.runtime import Runtime

__version__ = "0.0.0"

try:
    __version__ = cast(str, version("rh-art-tools"))
except PackageNotFoundError:
    # package is not installed
    pass
