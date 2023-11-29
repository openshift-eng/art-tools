import click
import io
from doozerlib.cli import cli, pass_runtime


@cli.command("rpms:print", short_help="Print data for each rpm metadata")
@click.option("--short", default=False, is_flag=True, help="Suppress all output other than the data itself")
@click.option("--include-disabled", default=False, is_flag=True, help="Include disabled RPMs")
@click.option("--output", "-o", default=None, help="Write data to FILE instead of STDOUT")
@click.argument("pattern", default="{build}", nargs=1)
@pass_runtime
def rpms_print(runtime, short, include_disabled, output, pattern):
    # If user omitted braces, add them.
    if "{" not in pattern:
        pattern = "{%s}" % pattern.strip()

    runtime.initialize(mode="rpms", disabled=include_disabled, clone_distgits=False)
    rpms = list(runtime.rpm_metas())

    if short:
        echo_verbose = lambda _: None
    else:
        echo_verbose = click.echo

    echo_verbose("")
    echo_verbose("------------------------------------------")

    for rpm in rpms:
        s = pattern
        s = s.replace("{name}", rpm.name)
        s = s.replace("{component}", rpm.get_component_name())

        if output is None:
            # Print to stdout
            click.echo(s)
        else:
            # Write to a file
            with io.open(output, 'a', encoding="utf-8") as out_file:
                out_file.write("{}\n".format(s))
