"""Configured JQL rules for a Jira bug-sweep plugin."""

import re
from collections.abc import Mapping
from datetime import datetime, timezone

from elliottlib.bug_sweep_plugins.contracts import (
    PluginContext,
    PluginDecision,
    PluginDisposition,
    PluginResult,
)
from elliottlib.bzutil import JIRABug, JIRABugTracker

JQL_SWEEP_PLUGIN_NAME = "jql_sweep"

_ASSEMBLY_TIMESTAMP_PATTERN = re.compile(r"\{\{\s*assembly\.timestamp\s*\}\}|\{\s*assembly\.timestamp\s*\}")
_ORDER_BY_PATTERN = re.compile(r"\border\s+by\b", re.IGNORECASE)


class JqlSweepPlugin:
    """Apply configured Jira JQL rules to the find-bugs sweep."""

    def __init__(self, configuration: Mapping[str, object]) -> None:
        """
        Validate and retain the candidate, include, and exclude JQL rules.
        """
        self._candidate_jql = self._require_jql(configuration.get("candidate"), "candidate")
        self._include_rules = self._parse_rules(configuration.get("include", []), "include")
        self._exclude_rules = self._parse_rules(configuration.get("exclude", []), "exclude")

    @property
    def name(self) -> str:
        """
        Return the configuration name for this plugin.
        """
        return JQL_SWEEP_PLUGIN_NAME

    def evaluate(self, context: PluginContext) -> PluginResult:
        """
        Evaluate candidate, include, and exclude JQL rules for one assembly.
        """
        jira_tracker = context.bug_tracker
        if not isinstance(jira_tracker, JIRABugTracker):
            raise TypeError("The sweep plugin requires the Jira bug tracker")

        candidate_jql = self._render_jql(self._candidate_jql, context.assembly_timestamp)
        include_rules = tuple(
            (name, self._render_jql(jql, context.assembly_timestamp)) for name, jql in self._include_rules
        )
        exclude_rules = tuple(
            (name, self._render_jql(jql, context.assembly_timestamp)) for name, jql in self._exclude_rules
        )
        candidate_bugs = jira_tracker.search_jql(candidate_jql, verbose=context.runtime.debug)
        candidate_bugs_by_key = {str(bug.id): bug for bug in candidate_bugs}
        candidate_keys = set(candidate_bugs_by_key)

        include_matches = (
            self._matching_issue_keys(
                jira_tracker,
                context.runtime.debug,
                candidate_jql,
                include_rules,
            )
            if candidate_keys
            else {}
        )
        exclude_matches = (
            self._matching_issue_keys(
                jira_tracker,
                context.runtime.debug,
                candidate_jql,
                exclude_rules,
            )
            if candidate_keys
            else {}
        )

        decisions = tuple(
            self._decision_for(candidate_bugs_by_key[issue_key], include_matches, exclude_matches)
            for issue_key in sorted(candidate_keys)
        )
        return PluginResult(
            plugin_name=self.name,
            decisions=decisions,
            domain_search_complete=True,
        )

    @staticmethod
    def _require_jql(value: object, field_name: str) -> str:
        """
        Validate one JQL expression and disallow clauses that cannot be nested.
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"JQL sweep field {field_name!r} must be a non-empty JQL string")
        jql = value.strip()
        if _ORDER_BY_PATTERN.search(jql):
            raise ValueError(f"JQL field {field_name!r} must not contain ORDER BY")
        return jql

    @classmethod
    def _parse_rules(cls, value: object, field_name: str) -> tuple[tuple[str, str], ...]:
        """
        Validate named include or exclude rules from bug.yml.
        """
        if not isinstance(value, list):
            raise ValueError(f"JQL sweep field {field_name!r} must be a list")

        rules: list[tuple[str, str]] = []
        rule_names: set[str] = set()
        for rule in value:
            if not isinstance(rule, Mapping):
                raise ValueError(f"Every JQL sweep {field_name} rule must be a mapping")
            name = rule.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError(f"Every JQL sweep {field_name} rule must have a non-empty name")
            if name in rule_names:
                raise ValueError(f"Duplicate JQL sweep {field_name} rule name: {name!r}")
            rule_names.add(name)
            rules.append((name, cls._require_jql(rule.get("jql"), f"{field_name}.{name}.jql")))
        return tuple(rules)

    def _matching_issue_keys(
        self,
        jira_tracker: JIRABugTracker,
        verbose: bool,
        candidate_jql: str,
        rules: tuple[tuple[str, str], ...],
    ) -> dict[str, str]:
        """
        Return the first matching rule name for each issue in the candidate domain.
        """
        matches: dict[str, str] = {}
        for rule_name, rule_jql in rules:
            query = f"({candidate_jql}) AND ({rule_jql})"
            for bug in jira_tracker.search_jql(query, verbose=verbose):
                matches.setdefault(str(bug.id), rule_name)
        return matches

    @staticmethod
    def _render_jql(jql: str, assembly_timestamp: float | None) -> str:
        """
        Substitute the sweep cutoff timestamp into an assembly timestamp template.
        """
        if "assembly.timestamp" not in jql:
            return jql
        if assembly_timestamp is None or assembly_timestamp <= 0:
            raise ValueError("JQL uses assembly.timestamp but the sweep has no cutoff timestamp")

        formatted_timestamp = datetime.fromtimestamp(assembly_timestamp, tz=timezone.utc).strftime("\"%Y/%m/%d %H:%M\"")
        rendered_jql = _ASSEMBLY_TIMESTAMP_PATTERN.sub(formatted_timestamp, jql)
        if "assembly.timestamp" in rendered_jql:
            raise ValueError("Unsupported assembly timestamp template in JQL")
        return rendered_jql

    def _decision_for(
        self,
        issue: JIRABug,
        include_matches: dict[str, str],
        exclude_matches: dict[str, str],
    ) -> PluginDecision:
        """
        Apply exclusion vetoes and then include rules to one candidate issue.
        """
        issue_key = str(issue.id)
        exclude_rule = exclude_matches.get(issue_key)
        if exclude_rule is not None:
            return PluginDecision(issue, PluginDisposition.SUPPRESS, f"Excluded by rule: {exclude_rule}")

        include_rule = include_matches.get(issue_key)
        if include_rule is not None:
            return PluginDecision(issue, PluginDisposition.INCLUDE, f"Included by rule: {include_rule}")

        if not self._include_rules:
            return PluginDecision(issue, PluginDisposition.INCLUDE, "Matched the candidate JQL")

        return PluginDecision(issue, PluginDisposition.SUPPRESS, "No include rule matched")
