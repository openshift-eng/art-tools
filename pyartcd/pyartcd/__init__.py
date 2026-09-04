import os
import sys
import typing
from importlib.metadata import PackageNotFoundError, version
from typing import cast

StrOrBytesPath = typing.Union[str, bytes, os.PathLike]

if sys.version_info < (3, 11):
    sys.exit('Sorry, Python < 3.11 is not supported.')

__version__ = "0.0.0"


try:
    __version__ = cast(str, version("rh-art-tools"))
except PackageNotFoundError:
    # package is not installed
    pass
