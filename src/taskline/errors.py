from collections.abc import Sequence
from typing import Any


class DuplicateTask(Exception):
    """A task name was registered twice in one process."""


class GracePeriodTooShort(Exception):
    """A task's cancel grace period is shorter than the lease length."""


class SubmissionError(ValueError):
    """A job was submitted with inputs or references that do not fit its definition."""


class InvalidInputs(SubmissionError):
    """A job was submitted with values its declared input types do not accept.

    problems names each input at fault. The refused values themselves are never in the message.
    """

    def __init__(self, message: str, problems: Sequence[Any]) -> None:
        super().__init__(message)
        self.problems = tuple(problems)


class CatalogueError(Exception):
    """A module that defines jobs could not be imported."""


class MissingInputs(SubmissionError):
    """A job was submitted without some of its declared inputs."""


class UnknownInputs(SubmissionError):
    """A job was submitted with inputs it does not declare."""


class MissingReferences(SubmissionError):
    """A job was submitted without a reference kind one of its tasks requires."""


class DuplicateJob(Exception):
    """A different job is already registered in the catalogue under this name."""


class MissingValue(Exception):
    """A step needs a named value that is not in the job's values."""


class InvalidStepResult(Exception):
    """A task's return value cannot be used as the step's declared output."""


class ValuesTooLarge(Exception):
    """The values passed between a job's steps are larger than the configured limit."""


class NotMigrated(Exception):
    """The database has no jobs schema, or one older than this Taskline needs."""


class UnknownTask(Exception):
    """A worker received a message for a task it has no definition for."""


class NonRetryable(Exception):
    """Raised inside a task to fail it at once, whatever attempts it has left."""


class TaskCancelled(Exception):
    """Raised inside a task to stop because a cancel was requested.

    Taskline raises it for an async task that is cancelled at an await. A plain task raises
    it itself after seeing cancel_requested(). Either way the task is recorded as cancelled.
    """
