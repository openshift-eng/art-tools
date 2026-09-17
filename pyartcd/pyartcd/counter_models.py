"""Data models used by Konflux build and rebase counter utilities."""

from collections.abc import Awaitable, Callable
from pathlib import Path

from artcommonlib.variants import BuildVariant
from pydantic import BaseModel, ConfigDict, Field

from pyartcd.build_strategy import BuildStrategy


class BaseCounterModel(BaseModel):
    """Validated fields shared by counter update models."""

    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)

    group: str
    build_variant: BuildVariant
    jenkins_url: str | None
    increment_counter: Callable[..., Awaitable[None]]


class RebaseCounterContext(BaseCounterModel):
    """Validated configuration for updating Konflux rebase counters."""

    assembly: str
    image_build_strategy: BuildStrategy = Field(strict=True)
    group_images: list[str]
    requested_images: list[str]
    images_excluded: list[str]
    state_path: Path | None
    reset_counter: Callable[[str], Awaitable[None]]
    rebase_state_key: str = "images:konflux:rebase"
    failed_images: list[str] = Field(default_factory=list)
    skipped_due_to_parent: list[str] | None = None


class FailedImageCategories(BaseModel):
    """Validated failed images grouped by the counter category they should update."""

    model_config = ConfigDict(validate_assignment=True)

    build: list[str]
    its: list[str]
    release: list[str]


class FailedImageCounterContext(BaseCounterModel):
    """Validated inputs for incrementing categorized failed-image counters."""

    failure_categories: FailedImageCategories
    failed_entries: dict[str, dict]
    return_exceptions: bool = False


class BuildFailCounterContext(BaseCounterModel):
    """Validated inputs for updating Konflux build-failure counters."""

    assembly: str
    built_images: list[str]
    failed_images: list[str]
    failed_entries: dict[str, dict]
    reset_counter: Callable[[str], Awaitable[None]]
    build_only: bool = False
