"""Execute plugins and translate their results into the sweep's eligibility overlay."""

from collections.abc import Iterable

from elliottlib.bug_sweep_plugins.contracts import (
    EligibilityPlugin,
    PluginContext,
    PluginDisposition,
    PluginEvaluation,
    PluginResult,
)
from elliottlib.exceptions import ElliottFatalError


def evaluate_plugins(plugins: Iterable[EligibilityPlugin], context: PluginContext) -> PluginEvaluation:
    """
    Validate plugin results and reduce them to the inputs consumed by the main sweep.

    A plugin must completely discover its domain, and an issue may have only one
    owner. These checks prevent partial searches or conflicting plugins from silently
    falling through to ordinary status and cutoff selection.
    """
    selected_plugins = tuple(plugins)
    results: list[PluginResult] = []
    issue_owners: dict[str, str] = {}
    plugin_names: set[str] = set()
    for plugin in selected_plugins:
        if plugin.name in plugin_names:
            raise ValueError(f"Evidence plugin {plugin.name!r} was selected more than once")
        plugin_names.add(plugin.name)

        result = plugin.evaluate(context)
        if result.plugin_name != plugin.name:
            raise ValueError(f"Plugin {plugin.name!r} returned a result named {result.plugin_name!r}")
        if not result.domain_search_complete:
            raise ElliottFatalError(
                f"Evidence plugin {plugin.name!r} could not completely discover its issue domain; "
                "refusing to run the ordinary bug sweep with partial ownership"
            )
        decision_keys = [decision.issue_key for decision in result.decisions]
        if any(not issue_key for issue_key in decision_keys):
            raise ValueError(f"Evidence plugin {plugin.name!r} returned an empty issue key")
        if len(decision_keys) != len(set(decision_keys)):
            raise ValueError(f"Evidence plugin {plugin.name!r} returned duplicate issue decisions")
        if any(not isinstance(decision.disposition, PluginDisposition) for decision in result.decisions):
            raise ValueError(f"Evidence plugin {plugin.name!r} returned an invalid decision disposition")
        for issue_key in decision_keys:
            owner = issue_owners.get(issue_key)
            if owner:
                raise ValueError(
                    f"Issue {issue_key} is claimed by multiple evidence plugins: {owner!r} and {plugin.name!r}"
                )
            issue_owners[issue_key] = plugin.name
        results.append(result)

    return PluginEvaluation(
        claimed_issue_keys=frozenset(issue_owners),
        included_issue_keys=frozenset(
            decision.issue_key
            for result in results
            for decision in result.decisions
            if decision.disposition is PluginDisposition.INCLUDE
        ),
        results=tuple(results),
    )
