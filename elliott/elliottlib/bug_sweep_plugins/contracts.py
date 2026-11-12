"""Contracts shared by bug-sweep plugins and their orchestration."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from elliottlib.bzutil import Bug, BugTracker
from elliottlib.runtime import Runtime


class PluginDisposition(StrEnum):
    """
    Decision a plugin makes for an issue it owns in this sweep.

    INCLUDE bypasses ordinary status and cutoff checks. SUPPRESS and REVIEW block
    ordinary-sweep fallback; REVIEW also marks an issue for human follow-up.
    """

    INCLUDE = "include"
    SUPPRESS = "suppress"
    REVIEW = "review"


@dataclass(frozen=True)
class PluginDecision:
    """
    A plugin decision describes an issue object in the plugin's declared domain.

    Retaining the issue object lets downstream code reuse the tracker result without
    fetching the issue again. Reasons are suitable for logs and reports. Plugins
    must not include raw external data or exception messages in them.
    """

    issue: Bug
    disposition: PluginDisposition
    reason: str

    @property
    def issue_key(self) -> str:
        """
        Return the issue's stable identifier as a string.
        """
        return str(self.issue.id)


@dataclass(frozen=True)
class PluginResult:
    """
    Issue decisions and whether the plugin completely discovered its domain.

    A complete result describes exactly the issues listed in ``decisions``. Issues
    outside the plugin's domain remain eligible for the ordinary sweep. Warnings
    must omit raw external data and exception messages.
    """

    plugin_name: str
    decisions: tuple[PluginDecision, ...]
    domain_search_complete: bool
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class PluginContext:
    """
    Read-only inputs available to a plugin for one assembly sweep. The cutoff
    timestamp is optional because not all sweeps have a time-based basis.
    """

    runtime: Runtime
    bug_tracker: BugTracker
    excluded_issue_keys: frozenset[str]
    assembly_timestamp: float | None = None


class BugSweepPlugin(Protocol):
    """
    Contract implemented by a plugin that contributes to a release bug sweep.

    Each returned decision claims its issue for this sweep. INCLUDE bypasses the
    normal Jira status and basis-time filters. SUPPRESS and REVIEW prevent the issue
    from falling back into the ordinary status/time sweep. Plugins return no
    decision for issues outside their domain. They do not perform Jira status
    changes, advisory categorization, or attachment deduplication.
    """

    @property
    def name(self) -> str:
        """
        Return the stable CLI and log name for this plugin.
        """
        ...

    def evaluate(self, context: PluginContext) -> PluginResult:
        """
        Return decisions for every issue claimed by the plugin.
        """
        ...
