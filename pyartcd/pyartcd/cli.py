import asyncio
import logging
import os
import sys
from functools import update_wrapper
from pathlib import Path
from typing import Optional

import click
from artcommonlib import logutil
from opentelemetry import context, trace

from pyartcd import __version__
from pyartcd.runtime import Runtime
from pyartcd.telemetry import initialize_telemetry

pass_runtime = click.make_pass_decorator(Runtime)


def click_coroutine(f):
    """A wrapper to allow to use asyncio with click.
    https://github.com/pallets/click/issues/85
    """

    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return update_wrapper(wrapper, f)


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('artcd v{}'.format(__version__))
    click.echo('Python v{}'.format(sys.version))
    ctx.exit()


# ============================================================================
# GLOBAL OPTIONS: parameters for all commands
# ============================================================================
@click.group(context_settings=dict(help_option_names=['-h', '--help']))
@click.option(
    '--version',
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Print version information and quit",
)
@click.option('--enable-telemetry', is_flag=True, help="[Experimental] Enable OpenTelemetry support")
@click.option("--config", "-c", metavar='PATH', help="Configuration file ('~/.config/artcd.toml' by default)")
@click.option(
    "--working-dir",
    "-C",
    metavar='PATH',
    default=None,
    help="Existing directory in which file operations should be performed (current directory by default)",
)
@click.option("--dry-run", is_flag=True, help="don't actually execute the pipeline; just print what would be done")
@click.option("--verbosity", "-v", count=True, help="[MULTIPLE] increase output verbosity")
@click.pass_context
def cli(
    ctx: click.Context,
    config: Optional[str],
    working_dir: Optional[str],
    dry_run: bool,
    verbosity: int,
    enable_telemetry: bool,
):
    # Initialize telemetry if needed
    if enable_telemetry or os.environ.get("TELEMETRY_ENABLED") == "1":
        initialize_telemetry()
        tracer = trace.get_tracer("pyartcd")
        cmd_name = ctx.invoked_subcommand or "root"
        span = tracer.start_span(f"artcd.{cmd_name}")
        span.set_attributes(
            {
                "artcd.command": cmd_name,
                "artcd.dry_run": dry_run,
                "artcd.version": __version__,
                "jenkins.build_url": os.environ.get("BUILD_URL", ""),
                "jenkins.job_name": os.environ.get("JOB_NAME", ""),
                "jenkins.build_user_email": os.environ.get("BUILD_USER_EMAIL", ""),
            }
        )
        token = context.attach(trace.set_span_in_context(span))
        ctx.call_on_close(span.end)
        ctx.call_on_close(lambda: context.detach(token))
        # Update TRACEPARENT so child subprocesses (e.g. doozer) use the artcd span
        # as their parent rather than the Jenkins-generated fake span ID that was in
        # the original TRACEPARENT.  Without this, all doozer spans land as siblings
        # of the artcd root span instead of children.
        span_ctx = span.get_span_context()
        os.environ["TRACEPARENT"] = f"00-{span_ctx.trace_id:032x}-{span_ctx.span_id:016x}-{span_ctx.trace_flags:02x}"
    config_filename = Path(config) if config else Path("~/.config/artcd.toml").expanduser()
    working_dir_path = Path(working_dir) if working_dir else Path.cwd()
    # configure logging
    if not verbosity:
        level = logging.WARNING
    elif verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logutil.setup_logging(level, str(working_dir_path / "debug.log"))
    ctx.obj = Runtime.from_config_file(config_filename, working_dir=working_dir_path, dry_run=dry_run)
