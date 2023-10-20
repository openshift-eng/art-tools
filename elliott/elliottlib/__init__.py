import sys
from importlib.metadata import PackageNotFoundError, version
from typing import cast

if sys.version_info < (3, 8):
    sys.exit('Sorry, Python < 3.8 is no longer supported.')

from elliottlib.runtime import Runtime

__version__ = "0.0.0"

try:
    __version__ = cast(str, version("rh-elliott"))
except PackageNotFoundError:
    # package is not installed
    pass
