"""
Data models for release readiness checks.

Defines status enums, check results, and the aggregated readiness report.
"""

from enum import Enum

from pydantic import BaseModel, Field

NIGHTLY_AGE_YELLOW_HOURS = 36
NIGHTLY_AGE_RED_HOURS = 48
NIGHTLY_HISTORY_COUNT = 5

MAX_DETAIL_ITEMS = 5
MAX_ERROR_LENGTH = 120


class Status(str, Enum):
    """
    Readiness check status levels.
    """

    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"

    def __init__(self, value):
        self._emoji = {"GREEN": "✅", "YELLOW": "⚠️", "RED": "❌"}[value]
        self._overall_emoji = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}[value]

    @property
    def emoji(self) -> str:
        return self._emoji

    @property
    def overall_emoji(self) -> str:
        return self._overall_emoji


class NightlyPhase(str, Enum):
    """
    Release controller nightly phases.
    """

    ACCEPTED = "Accepted"
    REJECTED = "Rejected"
    READY = "Ready"

    def __init__(self, value):
        self._emoji = {"Accepted": "✅", "Rejected": "❌", "Ready": "⏳"}[value]

    @property
    def emoji(self) -> str:
        return self._emoji

    @classmethod
    def emoji_for(cls, phase: str) -> str:
        """
        Convert a phase string to its emoji, returning ❓ for unknown phases.
        """

        try:
            return cls(phase).emoji
        except ValueError:
            return "❓"


class CheckResult(BaseModel):
    """
    Result of a single readiness check.
    """

    name: str
    status: Status
    summary: str
    details: list[str] = Field(default_factory=list)


class ReadinessReport(BaseModel):
    """
    Aggregated readiness report for an OCP version.
    """

    group: str
    timestamp: str
    overall_status: Status
    overall_emoji: str = ""
    checks: list[CheckResult] = Field(default_factory=list)
    dev_cut_off: str | None = None
