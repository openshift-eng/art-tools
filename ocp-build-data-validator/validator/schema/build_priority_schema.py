"""Validate the central build priority policy using the runtime model."""

from artcommonlib.build_priority import BuildPriorityPolicy
from pydantic import ValidationError


def validate(file, data):
    try:
        BuildPriorityPolicy.model_validate(data)
    except ValidationError as exc:
        return str(exc)
    return ''
