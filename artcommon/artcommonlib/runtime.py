import logging
import os
import warnings
from abc import ABC, abstractmethod

from artcommonlib import logutil
from artcommonlib.konflux.konflux_db import KonfluxDb

# an abstract class intended to be used by anything looking for config specific to a single
# ocp-build-data group.


class GroupRuntime(ABC):
    def __init__(self, **kwargs):
        self.debug = False
        self.quiet = False
        self._logger = None
        self.debug_log_path = None
        self.working_dir = None
        self.initialized = False
        self.build_system = None
        self.konflux_db = None

        for key, val in kwargs.items():
            self.__dict__[key] = val

    def initialize(self, build_system=None, disable_konflux_db_cache=False):
        self.initialize_logging()
        self.initialize_konflux_db(disable_konflux_db_cache)
        if build_system:
            self.build_system = build_system

    def initialize_logging(self):
        self.debug_log_path = os.path.join(self.working_dir, "debug.log")

        if self.initialized or self._logger:
            return

        # Three flags control the output modes of the command:
        # --verbose prints logs to CLI as well as to files
        # --debug increases the log level to produce more detailed internal
        #         behavior logging
        # --quiet opposes both verbose and debug
        if self.debug:
            log_level = logging.DEBUG
        elif self.quiet:
            log_level = logging.WARN
        else:
            log_level = logging.INFO

        logutil.setup_logging(log_level, self.debug_log_path)
        self._logger = logging.getLogger("artcommonlib")

    def initialize_konflux_db(self, disable_konflux_db_cache=False):
        if self.konflux_db:
            return  # already initialized
        try:
            self.konflux_db = KonfluxDb(enable_cache=disable_konflux_db_cache is False)
            self._logger.info("Konflux DB initialized ")

        except Exception as err:
            self._logger.warning("Cannot connect to the Konflux DB: %s", str(err))

    @property
    @abstractmethod
    def group_config(self):
        pass

    @property
    def logger(self):
        """Get the runtime logger.
        Your module should generally use `logging.getLogger(__name__)` instead of using this one.
        """

        warnings.warn(
            "Use `logging.getLogger(__name__)` for your module instead of reusing `runtime.logger`", DeprecationWarning
        )
        return self._logger
