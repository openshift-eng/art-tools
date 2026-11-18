"""Explicit registry for bug-sweep plugins used by find-bugs."""

from collections.abc import Callable, Iterable, Mapping

from elliottlib.bug_sweep_plugins.contracts import BugSweepPlugin
from elliottlib.bug_sweep_plugins.jql_sweep import JQL_SWEEP_PLUGIN_NAME, JqlSweepPlugin

PluginConfiguration = Mapping[str, object]


# Add a factory here when implementing a new plugin.
_PLUGIN_FACTORIES: dict[str, Callable[[PluginConfiguration], BugSweepPlugin]] = {
    JQL_SWEEP_PLUGIN_NAME: JqlSweepPlugin,
}


def available_plugin_names() -> tuple[str, ...]:
    """
    Return the stable names accepted by the find-bugs command.
    """
    return tuple(sorted(_PLUGIN_FACTORIES))


def create_plugins(configurations: Iterable[PluginConfiguration]) -> tuple[BugSweepPlugin, ...]:
    """
    Instantiate configured plugins and reject missing, unknown, or duplicate names.
    """
    selected_configurations = tuple(configurations)
    if any(not isinstance(configuration, Mapping) for configuration in selected_configurations):
        raise ValueError("Every bug-sweep plugin configuration must be a mapping")
    selected_names = tuple(configuration.get("name") for configuration in selected_configurations)
    if any(not isinstance(name, str) or not name for name in selected_names):
        raise ValueError("Every bug-sweep plugin configuration must include a non-empty name")
    if len(selected_names) != len(set(selected_names)):
        raise ValueError("A bug-sweep plugin may be selected only once")

    unknown_names = set(selected_names) - set(_PLUGIN_FACTORIES)
    if unknown_names:
        raise ValueError(f"Unknown bug-sweep plugin(s): {', '.join(sorted(unknown_names))}")

    return tuple(
        _PLUGIN_FACTORIES[name](configuration) for name, configuration in zip(selected_names, selected_configurations)
    )
