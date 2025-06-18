import json

import click
import yaml

from doozerlib.cli import cli, pass_runtime
from doozerlib.runtime import Runtime


@cli.command('config:read-rpms', help="Prints the configuration for the RPMS provided by --rpms.")
@click.option('--yaml', 'as_yaml', required=False, default=False, is_flag=True, help='Format the output as yaml')
@pass_runtime
def config_read_rpms(runtime: Runtime, as_yaml: bool):
    if runtime.group == "openshift-4.20":
        runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False, build_system="konflux")
    else:
        runtime.initialize(mode='rpms', clone_source=False, clone_distgits=False)
    config = runtime.get_rpm_config()

    if as_yaml:
        click.echo(yaml.safe_dump(yaml.safe_load((json.dumps(config)))))
    else:
        click.echo(json.dumps(config, indent=4))
