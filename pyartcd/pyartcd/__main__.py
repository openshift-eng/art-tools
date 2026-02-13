from typing import Optional, Sequence

from pyartcd.cli import cli


def main(args: Optional[Sequence[str]] = None):
    # pylint: disable=no-value-for-parameter
    cli()


if __name__ == "__main__":
    main()
