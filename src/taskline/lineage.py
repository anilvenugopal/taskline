import contextvars
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass


@dataclass(frozen=True)
class LineageContext:
    """Identifies the task attempt that is running."""

    job_run_id: uuid.UUID
    task_instance_id: uuid.UUID
    step_index: int
    step_count: int
    task_name: str
    attempt: int
    max_attempts: int
    correlation_id: str | None
    producer_code: str
    worker_id: str | None  # None before any worker has claimed the task


_current_lineage: contextvars.ContextVar[LineageContext | None] = contextvars.ContextVar(
    "taskline_current_lineage", default=None
)


@contextmanager
def bound_lineage(lineage: LineageContext) -> Iterator[None]:
    """Make lineage the current attempt's lineage for code running inside the block."""
    token = _current_lineage.set(lineage)
    try:
        yield
    finally:
        _current_lineage.reset(token)


def current_lineage() -> LineageContext | None:
    """The lineage of the task running in this context, or None outside a task."""
    return _current_lineage.get()
