"""Common tooling exceptions. Store them in this central place to
avoid circular imports
"""


class DoozerFatalError(Exception):
    """A broad exception for errors during Brew CRUD operations"""

    pass


class ParentRebaseFailedError(Exception):
    """Raised when a child image cannot be rebased because a group-member parent image failed first."""

    def __init__(self, distgit_key: str, failed_parents: list[str]):
        self.distgit_key = distgit_key
        self.failed_parents = failed_parents
        super().__init__(
            f"Couldn't rebase {distgit_key} because the following parent images failed to rebase: "
            f"{', '.join(failed_parents)}"
        )
