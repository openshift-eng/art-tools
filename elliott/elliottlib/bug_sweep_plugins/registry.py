"""Explicit registry for eligibility plugins used by find-bugs."""

from collections.abc import Callable, Iterable

from elliottlib.bug_sweep_plugins.contracts import EligibilityPlugin


def _create_rhcos_rpm_plugin() -> EligibilityPlugin:
    """
    Load the RHCOS plugin only when a sweep explicitly selects it.
    """
    from elliottlib.bug_sweep_plugins.rhcos_rpm import RHCOSRpmEvidencePlugin

    return RHCOSRpmEvidencePlugin()


_PLUGIN_FACTORIES: dict[str, Callable[[], EligibilityPlugin]] = {
    "rhcos-rpm": _create_rhcos_rpm_plugin,
}


def available_plugin_names() -> tuple[str, ...]:
    """
    Return the stable names accepted by the find-bugs command.
    """
    return tuple(sorted(_PLUGIN_FACTORIES))


def create_plugins(names: Iterable[str]) -> tuple[EligibilityPlugin, ...]:
    """
    Instantiate requested plugins and reject unknown or duplicate names.
    """
    selected_names = tuple(names)
    if len(selected_names) != len(set(selected_names)):
        raise ValueError("An evidence plugin may be selected only once")

    unknown_names = set(selected_names) - set(_PLUGIN_FACTORIES)
    if unknown_names:
        raise ValueError(f"Unknown evidence plugin(s): {', '.join(sorted(unknown_names))}")

    return tuple(_PLUGIN_FACTORIES[name]() for name in selected_names)
