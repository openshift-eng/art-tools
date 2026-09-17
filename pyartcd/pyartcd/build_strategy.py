"""Build strategy values used by Konflux pipelines."""

from enum import Enum


class BuildStrategy(Enum):
    """Image build selection strategies."""

    ALL = "all"
    ONLY = "only"
    EXCEPT = "except"
    NONE = "none"

    def __str__(self):
        return self.value
