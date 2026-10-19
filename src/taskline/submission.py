import contextvars
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from taskline.store import ReferencePlan


@dataclass(frozen=True)
class Submission:
    """Details a producer attaches to a run when it submits.

    producer_code, correlation_id and parent_task_instance_id left empty are taken
    from the lineage of the task that is submitting, when there is one.
    """

    producer_code: str | None = None
    job_name: str | None = None
    trigger_code: str = "api"
    triggered_by: str | None = None
    correlation_id: str | None = None
    schedule_id: uuid.UUID | None = None
    parent_task_instance_id: uuid.UUID | None = None
    references: list[ReferencePlan] = field(default_factory=list)


_current_submission: contextvars.ContextVar[Submission | None] = contextvars.ContextVar(
    "taskline_current_submission", default=None
)


@contextmanager
def bound_submission(submission: Submission) -> Iterator[None]:
    token = _current_submission.set(submission)
    try:
        yield
    finally:
        _current_submission.reset(token)


def current_submission() -> Submission | None:
    return _current_submission.get()
