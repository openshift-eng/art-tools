"""Execute plugins and translate their results into bug-sweep inputs."""

from collections.abc import Iterable

from elliottlib.bug_sweep_plugins.contracts import (
    BugSweepPlugin,
    PluginContext,
    PluginDisposition,
    PluginResult,
)
from elliottlib.bzutil import Bug
from elliottlib.exceptions import ElliottFatalError


def evaluate_plugins(plugins: Iterable[BugSweepPlugin], context: PluginContext) -> tuple[PluginResult, ...]:
    """
    Run plugins, validate their outputs, and return their results in configuration order.

    A plugin must completely discover its domain, and an issue may have only one
    owner. These checks prevent partial searches or conflicting plugins from silently
    falling through to ordinary status and cutoff selection. The sweep integration
    decides how to consume the validated results.
    """
    selected_plugins = tuple(plugins)
    results: list[PluginResult] = []
    issue_owners: dict[str, str] = {}
    plugin_names: set[str] = set()
    for plugin in selected_plugins:
        plugin_name = plugin.name
        if not isinstance(plugin_name, str) or not plugin_name:
            raise ValueError("Every bug-sweep plugin must have a non-empty name")
        if plugin_name in plugin_names:
            raise ValueError(f"Bug-sweep plugin {plugin_name!r} was selected more than once")
        plugin_names.add(plugin_name)

        result = plugin.evaluate(context)
        if result.plugin_name != plugin_name:
            raise ValueError(f"Plugin {plugin_name!r} returned a result named {result.plugin_name!r}")
        if not result.domain_search_complete:
            raise ElliottFatalError(
                f"Bug-sweep plugin {plugin_name!r} could not completely discover its issue domain; "
                "refusing to run the ordinary bug sweep with partial ownership"
            )
        if any(not isinstance(decision.issue, Bug) for decision in result.decisions):
            raise ValueError(f"Bug-sweep plugin {plugin_name!r} returned a decision without a bug object")
        decision_keys = [decision.issue_key for decision in result.decisions]
        if any(not issue_key for issue_key in decision_keys):
            raise ValueError(f"Bug-sweep plugin {plugin_name!r} returned an empty issue key")
        if len(decision_keys) != len(set(decision_keys)):
            raise ValueError(f"Bug-sweep plugin {plugin_name!r} returned duplicate issue decisions")
        if any(not isinstance(decision.disposition, PluginDisposition) for decision in result.decisions):
            raise ValueError(f"Bug-sweep plugin {plugin_name!r} returned an invalid decision disposition")
        for issue_key in decision_keys:
            if issue_key in issue_owners:
                owner = issue_owners[issue_key]
                raise ValueError(
                    f"Issue {issue_key} is claimed by multiple bug-sweep plugins: {owner!r} and {plugin_name!r}"
                )
            issue_owners[issue_key] = plugin_name
        results.append(result)

    return tuple(results)
