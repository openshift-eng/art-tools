import asyncio
import contextvars
import functools
import logging
import shlex
from typing import List, Tuple, Union
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from pyartcd.telemetry import start_as_current_span_async


logger = logging.getLogger(__name__)
TRACER = trace.get_tracer(__name__)


async def to_thread(func, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.

    This function is a backport of asyncio.to_thread from Python 3.9.

    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propogated,
    allowing context variables from the main thread to be accessed in the
    separate thread.

    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = asyncio.get_event_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def limit_concurrency(limit=5):
    """A decorator to limit the number of parallel tasks with asyncio.

    It should be noted that when the decorator function is executed, the created Semaphore is bound to the default event loop.
    https://stackoverflow.com/a/66289885
    """
    # use asyncio.BoundedSemaphore(5) instead of Semaphore to prevent accidentally increasing the original limit (stackoverflow.com/a/48971158/6687477)
    sem = asyncio.BoundedSemaphore(limit)

    def executor(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            async with sem:
                return await func(*args, **kwargs)

        return wrapper

    return executor


@start_as_current_span_async(TRACER, "cmd_gather_async")
async def cmd_gather_async(cmd: Union[List[str], str], check: bool = True, **kwargs) -> Tuple[int, str, str]:
    """ Runs a command asynchronously and returns rc,stdout,stderr as a tuple
    :param cmd <string|list>: A shell command
    :param check: If check is True and the exit code was non-zero, it raises a ChildProcessError
    :param kwargs: Other arguments passing to asyncio.subprocess.create_subprocess_exec
    :return: rc,stdout,stderr
    """

    if isinstance(cmd, str):
        cmd_list = shlex.split(cmd)
    else:
        cmd_list = cmd

    # Remove any empty tokens from the command list
    cmd_list = [token for token in cmd_list if token]

    span = trace.get_current_span()
    span.set_attribute("pyartcd.param.cmd", cmd_list)

    logger.info("Executing:cmd_gather_async %s", cmd_list)
    # capture stdout and stderr if they are not set in kwargs
    if "stdout" not in kwargs:
        kwargs["stdout"] = asyncio.subprocess.PIPE
    if "stderr" not in kwargs:
        kwargs["stderr"] = asyncio.subprocess.PIPE

    # Propagate trace context to subprocess
    env = kwargs.get("env", {})
    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)
    if "traceparent" in carrier:
        env["TRACEPARENT"] = carrier["traceparent"]
        kwargs["env"] = env

    proc = await asyncio.subprocess.create_subprocess_exec(cmd_list[0], *cmd_list[1:], **kwargs)
    stdout, stderr = await proc.communicate()
    stdout = stdout.decode() if stdout else ""
    stderr = stderr.decode() if stderr else ""
    span.set_attribute("pyartcd.result.exit_code", proc.returncode)
    if proc.returncode != 0:
        msg = f"Process {cmd_list!r} exited with code {proc.returncode}.\nstdout>>{stdout}<<\nstderr>>{stderr}<<\n"
        if check:
            raise ChildProcessError(msg)
        else:
            logger.warning(msg)
    span.set_status(trace.StatusCode.OK)
    return proc.returncode, stdout, stderr


@start_as_current_span_async(TRACER, "cmd_assert_async")
async def cmd_assert_async(cmd: Union[List[str], str], check: bool = True, **kwargs) -> int:
    """ Runs a command and optionally raises an exception if the return code of the command indicates failure.
    :param cmd <string|list>: A shell command
    :param check: If check is True and the exit code was non-zero, it raises a ChildProcessError
    :param kwargs: Other arguments passing to asyncio.subprocess.create_subprocess_exec
    :return: return code of the command
    """
    if isinstance(cmd, str):
        cmd_list = shlex.split(cmd)
    else:
        cmd_list = cmd

    # Remove any empty tokens from the command list
    cmd_list = [token for token in cmd_list if token]

    span = trace.get_current_span()
    span.set_attribute("pyartcd.param.cmd", cmd_list)

    # Propagate trace context to subprocess
    env = kwargs.get("env", {})
    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)
    if "traceparent" in carrier:
        env["TRACEPARENT"] = carrier["traceparent"]
        logger.warning("Pass TRACEPARENT %s", env["TRACEPARENT"])
        kwargs["env"] = env

    logger.info("Executing:cmd_assert_async %s", cmd_list)
    proc = await asyncio.subprocess.create_subprocess_exec(cmd_list[0], *cmd_list[1:], **kwargs)
    returncode = await proc.wait()
    span.set_attribute("pyartcd.result.exit_code", returncode)
    if returncode != 0:
        msg = f"Process {cmd_list!r} exited with code {returncode}."
        if check:
            raise ChildProcessError(msg)
        else:
            logger.warning(msg)
    span.set_status(trace.StatusCode.OK)
    return returncode
