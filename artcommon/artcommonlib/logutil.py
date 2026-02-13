import logging
import re


class EntityLoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "[%s] %s" % (self.extra["entity"], msg), kwargs


class RedactingFilter(logging.Filter):
    """A filter to remove sensitive information like passwords, tokens, and keys from logs
    https://docs.python.org/3/library/logging.html#filter-objects
    """

    PATTERNS = {
        "requests_gssapi.gssapi_": [
            (re.compile(r"Authorization header:(.+)", re.IGNORECASE), "Authorization header: <redacted>"),
        ],
    }

    def filter(self, record: logging.LogRecord):
        if record.name in self.PATTERNS.keys():
            self._redact(record)
        return True

    def _redact(self, record: logging.LogRecord):
        msg = isinstance(record.msg, str) and record.msg or str(record.msg)
        patterns = self.PATTERNS[record.name]
        for pattern, replacement in patterns:
            m = pattern.search(msg)
            if m:
                record.msg = pattern.sub(replacement, msg)
                break


def setup_logging(log_level: int, debug_log_path: str):
    """
    Setup logging with common configurations for art-tools.

    This function:
    - configure the root logger to log messages with levels of DEBUG and higher to the specified debug.log file
    - configure the root logger to log messages to the console with the specified log level
    - configure the root logger to redact sensitive information from logs using RedactingFilter
    - configure the log level for some third-party libraries to avoid leaking sensitive information

    :param log_level: The log level to use for console logging
    :param debug_log_path: The path to the debug log file

    :return: The root logger
    """
    # set up the root logger to log messages with levels of DEBUG and higher to debug.log
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(name)-12s %(levelname)s (%(thread)d) %(message)s",
        filename=debug_log_path,
        force=True,
    )

    root_logger = logging.getLogger()

    # define a Handler which writes $log_level messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(log_level)
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)s %(message)s")
    console.setFormatter(formatter)

    # add the handler to the root logger
    root_logger.addHandler(console)

    # add a RedactingFilter to redact sensitive information from logs
    redacting_filter = RedactingFilter()
    for handler in root_logger.handlers:
        handler.addFilter(redacting_filter)

    # configure the log level for some third-party libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("requests_kerberos").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(module_name=None):
    """
    Returns a logger appropriate for use in the art-tools package.
    Modules should request a logger using their __name__
    """

    logger_name = "art_tools"

    if module_name:
        logger_name = "{}.{}".format(logger_name, module_name)

    return logging.getLogger(logger_name)
