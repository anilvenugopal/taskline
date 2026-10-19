class DuplicateTask(Exception):
    """A task name was registered twice in one process."""


class GracePeriodTooShort(Exception):
    """A task's cancel grace period is shorter than the lease length."""


class SubmissionError(ValueError):
    """A job was submitted with inputs or references that do not fit its definition."""


class MissingInputs(SubmissionError):
    """A job was submitted without some of its declared inputs."""


class UnknownInputs(SubmissionError):
    """A job was submitted with inputs it does not declare."""


class MissingReferences(SubmissionError):
    """A job was submitted without a reference kind one of its tasks requires."""


class MissingValue(Exception):
    """A step needs a named value that is not in the job's values."""


class InvalidStepResult(Exception):
    """A task's return value cannot be used as the step's declared output."""


class ValuesTooLarge(Exception):
    """The values passed between a job's steps are larger than the configured limit."""


class UnknownTask(Exception):
    """A worker received a message for a task it has no definition for."""


class NonRetryable(Exception):
    """Raised inside a task to fail it at once, whatever attempts it has left."""


class TaskCancelled(Exception):
    """Raised inside a task to stop because a cancel was requested.

    Taskline raises it for an async task that is cancelled at an await. A plain task raises
    it itself after seeing cancel_requested(). Either way the task is recorded as cancelled.
    """
