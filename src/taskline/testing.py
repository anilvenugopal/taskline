"""Test data for programs that read Taskline: runs, tasks, references, events and workers in known
states, written into a migrated database without a worker or Valkey.

Every state is reached by the same Store operations a real worker uses (queue, claim, heartbeat,
finish, retry, cancel), so the rows and the event log are what Taskline would have written. The
functions are synchronous, for synchronous tests; each call uses its own short-lived connection.
They are not for async tests, where the Store can be used directly.
"""

import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from sqlalchemy import Engine, text
from taskiq import TaskiqMessage

from taskline.engines import sync_engine
from taskline.queries import REAPER_QUEUE
from taskline.spec import LABEL_MAX_ATTEMPTS, LABEL_QUEUE
from taskline.store import ReferencePlan, RunPlan, StepPlan, Store

__all__ = ["STATES", "RunHandle", "add_run", "add_worker", "clear"]  # as in docs/public_contract.md

STATES = ("queued", "running", "cancel_requested", "retrying", "succeeded", "failed", "cancelled")
WORKER_STATUSES = ("alive", "stopped", "dead")
DEFAULT_WORKER = "test-host:4242:aaaaaa"
LEASE_SECONDS = 60
RETRY_DELAY_SECONDS = 300  # a retrying task is due this long from now


@dataclass(frozen=True)
class RunHandle:
    """What add_run made: the run, its tasks in step order, and the worker that ran them."""

    run_id: uuid.UUID
    task_ids: tuple[uuid.UUID, ...]
    worker_id: str


def add_run(
    url_or_engine: str | Engine,
    *,
    state: str = "succeeded",
    job_name: str = "ingest_document",
    steps: int | Sequence[str] = 3,
    at_step: int = 0,
    attempts: int = 1,
    max_attempts: int = 3,
    queue: str = "default",
    producer: str = "test.producer",
    trigger: str = "api",
    triggered_by: str | None = None,
    correlation_id: str | None = None,
    references: Sequence[ReferencePlan] = (),
    parent: RunHandle | None = None,
    parent_step: int = 0,
    error_type: str = "ValueError",
    error_detail: str = "boom",
    error_traceback: str = "Traceback (most recent call last):\n  ...\nValueError: boom",
    progress_pct: float = 40,
    progress_note: str = "working",
    worker: str = DEFAULT_WORKER,
    age: timedelta = timedelta(0),
) -> RunHandle:
    """Write one run in the given state, and return a handle to it.

    The state is about the step at_step (counting from 0). Steps before it succeeded, and steps
    after it are planned, except that they are upstream_failed after a failure, cancelled after a
    cancel, and succeeded when the whole run succeeded.

    states:  queued, running (with progress), cancel_requested (a running task told to stop),
             retrying (an attempt failed, the next is due in 5 minutes), succeeded, failed
             (for good, with the error given), cancelled.
    attempts is the attempt number the state belongs to: 2 means the step was tried and retried
    once before, so its log holds the retry. It cannot exceed max_attempts.
    age moves the run's creation, queue, start and finish times, and its events, into the past,
    so lists have runs at different times. Heartbeats and leases stay as they are.
    The producer and the reference kinds are registered if they are not already.
    """
    _require_sync_context()
    names = [f"step_{i}" for i in range(steps)] if isinstance(steps, int) else list(steps)
    if state not in STATES:
        raise ValueError(f"unknown state {state!r}; expected one of {STATES}")
    if not names:
        raise ValueError("a run needs at least one step")
    if not 0 <= at_step < len(names):
        raise ValueError(f"at_step {at_step} is not a step of a run with {len(names)} steps")
    if not 1 <= attempts <= max_attempts:
        raise ValueError(f"attempts must be from 1 to max_attempts ({max_attempts})")
    if parent is not None and not 0 <= parent_step < len(parent.task_ids):
        raise ValueError(f"parent_step {parent_step} is not a step of the parent run")

    plan = RunPlan(
        job_run_id=uuid.uuid7(),
        job_name=job_name,
        job_trigger_code=trigger,
        producer_code=producer,
        producer_host="test-host",
        triggered_by=triggered_by,
        correlation_id=correlation_id,
        parent_task_instance_id=None if parent is None else parent.task_ids[parent_step],
        steps=[StepPlan(uuid.uuid7(), i, name, queue, max_attempts) for i, name in enumerate(names)],
        references=list(references),
    )
    error = (error_type, error_detail, error_traceback)

    _register_kinds(url_or_engine, producer, {r.reference_kind_code for r in references})
    _run_async(
        lambda: _build(
            _database_url(url_or_engine), plan, state, at_step, attempts, worker, error,
            progress_pct, progress_note,
        )
    )  # fmt: skip
    if age:
        _shift_times(url_or_engine, plan.job_run_id, age)
    return RunHandle(plan.job_run_id, tuple(s.task_instance_id for s in plan.steps), worker)


def add_worker(
    url_or_engine: str | Engine,
    *,
    queue: str = "default",
    status: str = "alive",
    reaper: bool = False,
    worker_id: str | None = None,
) -> str:
    """Register a worker (or a reaper) in the given status; returns its id.

    A worker's id is host:pid:suffix and a reaper's is reaper:host:pid:suffix. A stopped worker is
    stopped through the Store. A dead one is written directly, because only a clock can make it so.
    """
    _require_sync_context()
    if status not in WORKER_STATUSES:
        raise ValueError(f"unknown worker status {status!r}; expected one of {WORKER_STATUSES}")
    if worker_id is None:
        prefix = "reaper:" if reaper else ""
        worker_id = f"{prefix}test-host:{random.randint(1000, 9999)}:{uuid.uuid4().hex[:6]}"
    queue_name = REAPER_QUEUE if reaper else queue

    async def register(store: Store) -> None:
        await store.register_worker(worker_id, queue_name)
        if status == "stopped":
            await store.stop_worker(worker_id)

    _run_async(lambda: _with_store(_database_url(url_or_engine), register))
    if status == "dead":
        with sync_engine(url_or_engine) as engine, engine.begin() as conn:
            conn.execute(
                text("UPDATE jobs.worker SET worker_status_code = 'dead' WHERE worker_id = :id"),
                {"id": worker_id},
            )
    return worker_id


def clear(url_or_engine: str | Engine) -> None:
    """Remove every run, task, reference, event, worker and schedule. Registered producers and
    reference kinds, and the instance, stay."""
    _require_sync_context()
    with sync_engine(url_or_engine) as engine, engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE jobs.task_event, jobs.job_reference, jobs.schedule_fire, "
                "jobs.schedule_reference, jobs.task_instance, jobs.job_run, jobs.worker, "
                "jobs.schedule RESTART IDENTITY CASCADE"
            )
        )


async def _build(
    url: str,
    plan: RunPlan,
    state: str,
    at_step: int,
    attempts: int,
    worker: str,
    error: tuple[str, str, str],
    progress_pct: float,
    progress_note: str,
) -> None:
    async def build(store: Store) -> None:
        await store.register_worker(worker, plan.steps[0].queue_name)
        await store.register_run(plan)

        def task(step: int) -> uuid.UUID:
            return plan.steps[step].task_instance_id

        async def reach(step: int, attempt_number: int, claimed: bool) -> int:
            """Queue a step, retry it until it is on the wanted attempt, and optionally claim it."""
            spec = plan.steps[step]
            message = TaskiqMessage(  # what the middleware stores when it queues a task
                task_id=str(spec.task_instance_id),
                task_name=spec.task_name,
                labels={LABEL_QUEUE: spec.queue_name, LABEL_MAX_ATTEMPTS: str(spec.max_attempts)},
                args=[],
                kwargs={},
            ).model_dump(mode="json")
            await store.mark_queued(task(step), message)
            for _ in range(attempt_number - 1):
                earlier = await store.claim(task(step), worker, LEASE_SECONDS)
                if earlier is None:
                    raise RuntimeError(f"could not claim step {step} of {plan.job_name}")
                await store.schedule_retry(task(step), earlier.attempt, 0, *error)
                await store.take_due_retries(100)
            if not claimed:
                return 0
            claim = await store.claim(task(step), worker, LEASE_SECONDS)
            if claim is None:
                raise RuntimeError(f"could not claim step {step} of {plan.job_name}")
            return claim.attempt

        for step in range(len(plan.steps)):
            if step < at_step or (step > at_step and state == "succeeded"):
                attempt = await reach(step, 1, claimed=True)
                await store.finish_succeeded(task(step), attempt)
            elif step == at_step:
                if state == "queued":
                    await reach(step, attempts, claimed=False)
                    break
                attempt = await reach(step, attempts, claimed=True)
                if state in ("running", "cancel_requested"):
                    await store.heartbeat(task(step), attempt, LEASE_SECONDS, progress_pct, progress_note)
                    if state == "cancel_requested":
                        await store.cancel_run(plan.job_run_id)
                    break
                if state == "retrying":
                    await store.schedule_retry(task(step), attempt, RETRY_DELAY_SECONDS, *error)
                    break
                if state == "failed":
                    await store.finish_failed(task(step), attempt, *error)
                    await store.fail_downstream(plan.job_run_id, step)
                    break
                if state == "cancelled":
                    await store.cancel_run(plan.job_run_id)
                    await store.finish_cancelled(task(step), attempt)
                    break
                await store.finish_succeeded(task(step), attempt)  # succeeded
        await store.refresh_run_status(plan.job_run_id)

    await _with_store(url, build)


async def _with_store(url: str, work: Callable[[Store], Awaitable[Any]]) -> Any:
    store = Store(url)
    try:
        return await work(store)
    finally:
        await store.close()


def _require_sync_context() -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    raise RuntimeError(
        "taskline.testing is for synchronous tests; in an async test, use the Store directly"
    )


def _run_async(make: Callable[[], Awaitable[Any]]) -> Any:
    _require_sync_context()
    return asyncio.run(make())


def _database_url(url_or_engine: str | Engine) -> str:
    if isinstance(url_or_engine, Engine):
        return url_or_engine.url.render_as_string(hide_password=False)
    return url_or_engine


def _register_kinds(url_or_engine: str | Engine, producer: str, reference_kinds: set[str]) -> None:
    """Add the producer and reference kinds if missing; names already registered are left alone."""
    with sync_engine(url_or_engine) as engine, engine.begin() as conn:
        conn.execute(
            text("INSERT INTO jobs.producer_kind (code, name) VALUES (:c, :c) ON CONFLICT DO NOTHING"),
            {"c": producer},
        )
        for code in reference_kinds:
            conn.execute(
                text("INSERT INTO jobs.reference_kind (code, name) VALUES (:c, :c) ON CONFLICT DO NOTHING"),
                {"c": code},
            )


def _shift_times(url_or_engine: str | Engine, run_id: uuid.UUID, age: timedelta) -> None:
    shifted = "make_interval(secs => CAST(:secs AS double precision))"
    params = {"id": run_id, "secs": age.total_seconds()}
    with sync_engine(url_or_engine) as engine, engine.begin() as conn:
        for sql in (
            f"UPDATE jobs.job_run SET created_at = created_at - {shifted}, "
            f"finished_at = finished_at - {shifted} WHERE job_run_id = :id",
            f"UPDATE jobs.task_instance SET queued_at = queued_at - {shifted}, "
            f"started_at = started_at - {shifted}, finished_at = finished_at - {shifted} "
            "WHERE job_run_id = :id",
            f"UPDATE jobs.task_event SET occurred_at = occurred_at - {shifted} WHERE task_instance_id "
            "IN (SELECT task_instance_id FROM jobs.task_instance WHERE job_run_id = :id)",
            f"UPDATE jobs.job_reference SET created_at = created_at - {shifted} WHERE job_run_id = :id",
        ):
            conn.execute(text(sql), params)
