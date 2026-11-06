"""Contracts shared by eligibility plugins and the bug sweep."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from elliottlib.bzutil import BugTracker
from elliottlib.runtime import Runtime


class PluginDisposition(StrEnum):
    """
    Decision a plugin makes for an issue it owns in this sweep.

    INCLUDE bypasses only ordinary status and cutoff checks. SUPPRESS and REVIEW
    both block ordinary-sweep fallback; REVIEW marks an issue for human follow-up.
    """

    INCLUDE = "include"
    SUPPRESS = "suppress"
    REVIEW = "review"


@dataclass(frozen=True)
class PluginDecision:
    """
    A plugin decision claims an issue and prevents ordinary-sweep fallback.

    Reasons are suitable for logs and reports. Plugins must not include raw
    external data or exception messages in them.
    """

    issue_key: str
    disposition: PluginDisposition
    reason: str


@dataclass(frozen=True)
class PluginResult:
    """
    Issue decisions and whether the plugin completely discovered its domain.

    A complete result owns exactly the issues listed in ``decisions``. Issues not
    listed are outside the plugin's domain and remain eligible for the normal sweep.
    Warnings must use sanitized diagnostic messages and omit exception text.
    """

    plugin_name: str
    decisions: tuple[PluginDecision, ...]
    domain_search_complete: bool
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class PluginEvaluation:
    """
    Validated plugin outputs reduced to the two sets consumed by the sweep.

    ``claimed_issue_keys`` blocks all plugin-owned issues from ordinary status and
    cutoff selection. ``included_issue_keys`` is fetched and added after those
    filters. The original results remain available for diagnostics.
    """

    claimed_issue_keys: frozenset[str]
    included_issue_keys: frozenset[str]
    results: tuple[PluginResult, ...]


@dataclass(frozen=True)
class PluginContext:
    """
    Read-only inputs available to an eligibility plugin for one assembly sweep.
    """

    runtime: Runtime
    bug_tracker: BugTracker
    excluded_issue_keys: frozenset[str]


class EligibilityPlugin(Protocol):
    """
    Contract implemented by a release-specific evidence eligibility plugin.

    Every returned decision claims its issue for this sweep. INCLUDE bypasses only
    the normal Jira status and basis-time filters. SUPPRESS and REVIEW prevent that
    issue from falling back into the normal status/time sweep. Plugins return no
    decision for issues outside their domain. They do not perform Jira status
    changes, advisory categorization, or attachment deduplication.
    Assembly-level includes are applied after plugin evaluation and explicit excludes
    are applied last. Targeted assemblies skip plugins and use their explicit issue
    list as the complete sweep input.
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
