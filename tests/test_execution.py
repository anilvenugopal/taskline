import asyncio
import inspect

import pytest
from taskiq.exceptions import NoResultError

from taskline.errors import TaskCancelled
from taskline.execution import AttemptState, bound_attempt, guarded


def start(state: AttemptState, job, *args) -> asyncio.Task:
    """Run an async job as its own asyncio task with the attempt bound, as a worker does."""
    with bound_attempt(state):
        return asyncio.create_task(job(*args))


async def test_jobs_run_unchanged_outside_a_worker():
    @guarded
    async def async_job(x):
        return x + 1

    @guarded
    def plain_job(x):
        return x + 1

    assert await async_job(1) == 2
    assert plain_job(1) == 2


async def test_an_unclaimed_task_is_skipped_without_running_the_job():
    ran = []

    @guarded
    async def async_job():
        ran.append("async")

    @guarded
    def plain_job():
        ran.append("plain")

    with bound_attempt(AttemptState(claimed=False)):
        with pytest.raises(NoResultError):
            await async_job()
        with pytest.raises(NoResultError):
            plain_job()

    assert ran == []


async def test_a_cancel_stops_an_async_job_at_its_await_and_runs_its_cleanup():
    state = AttemptState(claimed=True)
    started = asyncio.Event()
    cleaned = []

    @guarded
    async def job():
        try:
            started.set()
            await asyncio.sleep(30)
        finally:
            cleaned.append(True)

    task = start(state, job)
    await started.wait()

    state.on_cancel()

    with pytest.raises(TaskCancelled):
        await task
    assert cleaned == [True]
    assert state.stop_reason == "cancelled"
    assert state.asyncio_task is None
    assert state.cancels_sent == 0


async def test_a_lost_lease_stops_an_async_job_and_gives_no_result():
    state = AttemptState(claimed=True)
    started = asyncio.Event()
    cleaned = []

    @guarded
    async def job():
        try:
            started.set()
            await asyncio.sleep(30)
        finally:
            cleaned.append(True)

    task = start(state, job)
    await started.wait()

    state.on_lost()

    with pytest.raises(NoResultError):
        await task
    assert cleaned == [True]
    assert state.stop_reason == "lost"


async def test_a_cancellation_that_did_not_come_from_the_lease_passes_through():
    state = AttemptState(claimed=True)
    started = asyncio.Event()

    @guarded
    async def job():
        started.set()
        await asyncio.sleep(30)

    task = start(state, job)
    await started.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert task.cancelled()


async def test_an_async_job_that_finishes_anyway_after_a_cancel_keeps_its_result():
    state = AttemptState(claimed=True)

    @guarded
    async def job():
        state.on_cancel()  # the lease asks while the job is running without awaiting
        return "done"

    task = start(state, job)

    assert await task == "done"
    assert not task.cancelled()
    assert task.cancelling() == 0
    assert state.stop_reason == "cancelled"


async def test_a_plain_job_keeps_its_result_after_a_cancel_request():
    state = AttemptState(claimed=True)

    @guarded
    def job():
        state.on_cancel()
        return "done"

    with bound_attempt(state):
        assert job() == "done"


async def test_a_plain_job_result_is_discarded_after_a_lost_lease():
    state = AttemptState(claimed=True)

    @guarded
    def job():
        state.on_lost()
        return "done"

    with bound_attempt(state), pytest.raises(NoResultError):
        job()


async def test_job_errors_pass_through_unchanged():
    @guarded
    async def async_job():
        raise ValueError("bad input")

    @guarded
    def plain_job():
        raise ValueError("bad input")

    with bound_attempt(AttemptState(claimed=True)):
        with pytest.raises(ValueError, match="bad input"):
            await async_job()
        with pytest.raises(ValueError, match="bad input"):
            plain_job()


async def test_the_wrapped_function_keeps_its_name_and_signature():
    async def parse(path: str, pages: int = 5) -> int:
        return pages

    def split(path: str) -> list[str]:
        return [path]

    for original in (parse, split):
        wrapped = guarded(original)
        assert wrapped.__name__ == original.__name__
        assert inspect.signature(wrapped) == inspect.signature(original)
        assert inspect.iscoroutinefunction(wrapped) == inspect.iscoroutinefunction(original)
