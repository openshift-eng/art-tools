from typing import Optional, Sequence

from pyartcd.cli import cli
from pyartcd.pipelines import *  # noqa: F401, F403
from pyartcd.pipelines.scheduled import *  # noqa: F401, F403


def main(args: Optional[Sequence[str]] = None):
    # pylint: disable=no-value-for-parameter
    cli()


if __name__ == "__main__":
    main()
