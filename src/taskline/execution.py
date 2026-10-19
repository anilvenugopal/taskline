import asyncio
import contextvars
import functools
import inspect
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from taskiq.exceptions import NoResultError

from taskline.errors import TaskCancelled


@dataclass
class AttemptState:
    """What the middleware and the task wrapper share about one running attempt."""

    claimed: bool
    stop_reason: str | None = None  # "cancelled" or "lost", set by the lease callbacks
    asyncio_task: asyncio.Task | None = None  # the asyncio task running an async task function
    cancels_sent: int = 0  # cancellations requested on asyncio_task that have not been cleared

    def on_cancel(self) -> None:
        if self.stop_reason is None:
            self.stop_reason = "cancelled"
        self._stop_job()

    def on_lost(self) -> None:
        self.stop_reason = "lost"
        self._stop_job()

    def _stop_job(self) -> None:
        if self.asyncio_task is not None and self.asyncio_task.cancel():
            self.cancels_sent += 1


_current_attempt: contextvars.ContextVar[AttemptState | None] = contextvars.ContextVar(
    "taskline_current_attempt", default=None
)


@contextmanager
def bound_attempt(state: AttemptState) -> Iterator[None]:
    token = _current_attempt.set(state)
    try:
        yield
    finally:
        _current_attempt.reset(token)


def current_attempt() -> AttemptState | None:
    return _current_attempt.get()


def guarded(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a task function so a worker can skip it, stop it, or discard its result.

    Outside a worker (no attempt bound) the function runs unchanged.
    """
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def run_async(*args: Any, **kwargs: Any) -> Any:
            state = _current_attempt.get()
            if state is None:
                return await fn(*args, **kwargs)
            if not state.claimed:
                raise NoResultError
            current = asyncio.current_task()
            state.asyncio_task = current
            stopped_by_lease = False
            try:
                result = await fn(*args, **kwargs)
            except asyncio.CancelledError:
                if state.cancels_sent == 0:
                    raise  # cancelled by something other than the lease, such as shutdown
                stopped_by_lease = True
            finally:
                state.asyncio_task = None
                while state.cancels_sent:
                    current.uncancel()
                    state.cancels_sent -= 1
            if stopped_by_lease:
                if state.stop_reason == "lost":
                    raise NoResultError from None
                raise TaskCancelled from None
            if state.stop_reason == "lost":
                raise NoResultError
            return result

        return run_async

    @functools.wraps(fn)
    def run_plain(*args: Any, **kwargs: Any) -> Any:
        state = _current_attempt.get()
        if state is None:
            return fn(*args, **kwargs)
        if not state.claimed:
            raise NoResultError
        result = fn(*args, **kwargs)
        if state.stop_reason == "lost":
            raise NoResultError
        return result

    return run_plain
