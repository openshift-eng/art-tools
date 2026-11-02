"""Helpers for reading OLM package.yaml channel metadata for bundle builds.

ART historically used only ``channels[0]``. Layered products such as COO may list
multiple peer channels (e.g. ``stable`` and ``fast``) that should all receive the
same bundle. Resolution stays additive: a single-entry ``channels`` list yields
the same primary channel / default / CSV as before.
"""

from __future__ import annotations

from typing import Any, NamedTuple, Sequence


class PackageChannels(NamedTuple):
    """Resolved channel fields from an OLM package.yaml mapping."""

    package_name: str
    primary_channel: str
    additional_channels: tuple[str, ...]
    default_channel: str
    csv_name: str

    @property
    def all_channels(self) -> tuple[str, ...]:
        return (self.primary_channel, *self.additional_channels)


def resolve_package_channels(package_yaml: dict[str, Any]) -> PackageChannels:
    """Resolve package name, channels, default channel, and CSV from package.yaml.

    Backwards compatible for the common ART case of a single channel entry:
    primary/default are ``channels[0].name`` and CSV is ``channels[0].currentCSV``.

    When multiple channels are listed:
    - all unique ``channels[].name`` values are preserved in package.yaml order
    - ``defaultChannel`` is honored when present and names a listed channel;
      otherwise default remains ``channels[0].name`` (historical behavior)
    - CSV comes from the default channel's ``currentCSV`` when set, else
      ``channels[0].currentCSV``
    """
    package_name = str(package_yaml['packageName'])
    channels = package_yaml.get('channels') or []
    if not channels:
        raise ValueError('package.yaml must define at least one channel')

    names: list[str] = []
    for entry in channels:
        name = str(entry['name'])
        if name not in names:
            names.append(name)

    primary_channel = names[0]
    additional_channels = tuple(names[1:])

    default_channel = primary_channel
    configured_default = package_yaml.get('defaultChannel')
    if configured_default is not None:
        configured_default = str(configured_default)
        if configured_default in names:
            default_channel = configured_default

    csv_name = str(channels[0]['currentCSV'])
    for entry in channels:
        if str(entry['name']) == default_channel and entry.get('currentCSV') is not None:
            csv_name = str(entry['currentCSV'])
            break

    return PackageChannels(
        package_name=package_name,
        primary_channel=primary_channel,
        additional_channels=additional_channels,
        default_channel=default_channel,
        csv_name=csv_name,
    )


def format_bundle_channels_label(
    primary_channel: str,
    additional_channels: Sequence[str] | None = None,
    *,
    package_default_channel: str | None = None,
    operator_channel_stable: str | None = None,
    stable_channel_name: str = 'stable',
) -> tuple[str, str]:
    """Build ``channels.v1`` and ``channel.default.v1`` label values.

    For a single primary channel this preserves historical ART-3107 behavior:

    - unset ``operator_channel_stable`` → channels label is exactly ``primary_channel``
    - ``default`` / ``extra`` → ``','.join((primary_channel, stable_channel_name))``
      (including a possible duplicate when primary is already the stable name)

    Multi-channel package.yaml entries append additional names after the primary.
    ``package_default_channel`` (from package.yaml ``defaultChannel``) is used as the
    starting default when multiple channels are present. When ART-3107 is enabled
    with multiple channels, ``stable_channel_name`` is appended only if missing
    (avoids ``stable,fast,stable``).
    """
    additional = list(additional_channels or ())

    if not additional:
        # Exact historical single-channel path (ignore package_default_channel so
        # a stale defaultChannel cannot change single-channel ART behavior).
        channels_label = primary_channel
        default_channel = primary_channel
        if operator_channel_stable in ('default', 'extra'):
            channels_label = ','.join((primary_channel, stable_channel_name))
            if operator_channel_stable == 'default':
                default_channel = stable_channel_name
        return channels_label, default_channel

    channel_names = [primary_channel]
    for name in additional:
        if name not in channel_names:
            channel_names.append(name)

    default_channel = package_default_channel or primary_channel
    if default_channel not in channel_names:
        default_channel = primary_channel

    if operator_channel_stable in ('default', 'extra'):
        if stable_channel_name not in channel_names:
            channel_names.append(stable_channel_name)
        if operator_channel_stable == 'default':
            default_channel = stable_channel_name

    return ','.join(channel_names), default_channel
