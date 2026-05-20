"""
Reusable Click callback validators for pyartcd CLI options.
"""

import click
from artcommonlib.util import normalize_release_date


def validate_release_date(ctx, param, value):
    """
    Click callback that validates and converts release date to YYYY-Mon-DD format.
    Delegates to artcommonlib.util.normalize_release_date for the actual conversion.
    """
    if value is None:
        return None
    try:
        normalized = normalize_release_date(value)
        if normalized != value:
            click.echo(f"Converted release date from {value} to {normalized} (elliott format)")
        return normalized
    except ValueError as e:
        raise click.BadParameter(str(e)) from e
