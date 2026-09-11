"""Shared utilities for verify-* elliott subcommands.

Provides:
- get_assembly_advisory_ids(): unified advisory ID lookup from assembly config
- VerifyResultBase: abstract base for verify result dataclasses
- render_verify_result(): generic JSON/text rendering
- verify_output_option: shared --output click option
- handle_verify_result(): echo result and exit on failure
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass

import click
from artcommonlib.assembly import assembly_config_struct


def get_assembly_advisory_ids(
    runtime,
    include_types: tuple[str, ...] | None = None,
    exclude_types: tuple[str, ...] = (),
) -> dict[str, int]:
    """Get advisory IDs from assembly config, filtered by impetus type.

    Reads the assembly's group config from releases.yml and returns
    advisory IDs filtered by impetus type.

    Args:
        runtime: Elliott runtime (must be initialized).
        include_types: If set, only include these impetus types.
        exclude_types: Impetus types to skip.

    Returns:
        dict mapping impetus name to advisory ID.
    """
    releases_config = runtime.get_releases_config()
    group_config = assembly_config_struct(releases_config, runtime.assembly, "group", {})
    advisories = group_config.get("advisories", {})
    result = {}
    for impetus, ad_id in advisories.items():
        if not ad_id:
            continue
        if include_types is not None and impetus not in include_types:
            continue
        if impetus in exclude_types:
            continue
        result[impetus] = int(ad_id)
    return result


@dataclass
class VerifyResultBase(ABC):
    """Abstract base for verify-* command top-level result dataclasses.

    Subclasses must implement:
    - ``passed``: whether the verification succeeded
    - ``to_dict()``: JSON-serializable dict representation
    - ``render_text()``: human-readable text representation

    The ``failed`` property defaults to ``not self.passed`` but can be
    overridden for tri-state results (e.g. complete / pending / failed).
    """

    @property
    @abstractmethod
    def passed(self) -> bool:
        """Whether the verification succeeded."""
        ...

    @property
    def failed(self) -> bool:
        """Whether the verification failed.

        Override for tri-state results where ``not passed`` does not
        imply ``failed`` (e.g. a pending state).
        """
        return not self.passed

    @abstractmethod
    def to_dict(self) -> dict:
        """Return a JSON-serializable dict representation."""
        ...

    @abstractmethod
    def render_text(self) -> str:
        """Return a human-readable text representation."""
        ...


def render_verify_result(result: VerifyResultBase, output: str) -> str:
    """Render a verify result in the requested format.

    Args:
        result: A VerifyResultBase subclass instance.
        output: ``"json"`` or ``"text"``.

    Returns:
        Formatted string.
    """
    if output == "json":
        return json.dumps(result.to_dict(), indent=2)
    return result.render_text()


verify_output_option = click.option(
    "-o",
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="Output format.",
)


def handle_verify_result(result: VerifyResultBase, output: str) -> None:
    """Render and echo a verify result, exiting with code 1 on failure.

    Args:
        result: A VerifyResultBase subclass instance.
        output: ``"json"`` or ``"text"``.
    """
    click.echo(render_verify_result(result, output))
    if not result.passed:
        raise SystemExit(1)
