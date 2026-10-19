import contextvars
from collections.abc import Iterator
from contextlib import contextmanager

from taskline.lease import Lease

_current_lease: contextvars.ContextVar[Lease | None] = contextvars.ContextVar(
    "taskline_current_lease", default=None
)


@contextmanager
def bound_lease(lease: Lease) -> Iterator[None]:
    """Make lease the current attempt's lease for code running inside the block."""
    token = _current_lease.set(lease)
    try:
        yield
    finally:
        _current_lease.reset(token)


def cancel_requested() -> bool:
    """True once a cancel has been requested for the task running in this context."""
    lease = _current_lease.get()
    return lease is not None and lease.cancel_requested


def report_progress(pct: float | None = None, note: str | None = None) -> None:
    """Record progress for the task running in this context; does nothing outside a task."""
    lease = _current_lease.get()
    if lease is not None:
        lease.set_progress(pct, note)
