from enum import Enum


class TaskStates(Enum):
    FREE = 0
    OPEN = 1
    CLOSED = 2
    CANCELED = 3
    ASSIGNED = 4
    FAILED = 5


class BuildStates(Enum):
    BUILDING = 0
    COMPLETE = 1
    DELETED = 2
    FAILED = 3
    CANCELED = 4
