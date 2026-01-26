import click


def stringify(val):
    """
    Accepts either str or bytes and returns a str
    """
    try:
        val = val.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        pass
    return val


def red_prefix(msg, file=None):
    """Print out a message prefix in bold red letters, like for "Error: "
    messages"""
    click.secho(stringify(msg), nl=False, bold=True, fg="red", file=file)


def red_print(msg, file=None):
    """Print out a message in red text"
    messages"""
    click.secho(stringify(msg), nl=True, bold=False, fg="red", file=file)


def green_prefix(msg, file=None):
    """Print out a message prefix in bold green letters, like for "Success: "
    messages"""
    click.secho(stringify(msg), nl=False, bold=True, fg="green", file=file)


def green_print(msg, file=None):
    """Print out a message in green text"""
    click.secho(stringify(msg), nl=True, bold=False, fg="green", file=file)


def yellow_prefix(msg, file=None):
    """Print out a message prefix in bold yellow letters, like for "Success: "
    messages"""
    click.secho(stringify(msg), nl=False, bold=True, fg="yellow", file=file)


def yellow_print(msg, file=None):
    """Print out a message in yellow text"""
    click.secho(stringify(msg), nl=True, bold=False, fg="yellow", file=file)


def cprint(msg, file=None):
    """Wrapper for click.echo"""
    click.echo(stringify(msg), file=file)


def color_print(msg, color="white", nl=True, file=None):
    """Print out a message in given color"""
    click.secho(stringify(msg), nl=nl, bold=False, fg=color, file=file)
