import asyncio
import threading
import time
import uuid
from types import SimpleNamespace

import pytest
from helpers import make_plan, rows, wait_until
from taskiq import InMemoryBroker, TaskiqMessage
from taskiq_pipelines import Pipeline, PipelineMiddleware

from taskline.context import report_progress
from taskline.execution import current_attempt, guarded
from taskline.hook_registry import EVENTS, HookRegistry
from taskline.lineage import current_lineage
from taskline.middleware import TasklineMiddleware
from taskline.submission import Submission, bound_submission


@pytest.fixture
async def app(store):
    registry = HookRegistry()
    seen = []
    for event in EVENTS:
        registry.on(event, seen.append)
    ended = []
    middleware = TasklineMiddleware(
        store,
        registry,
        worker_id="worker-1",
        heartbeat_interval=0.05,
        lease_intervals=4,
        end_process=lambda: ended.append(True),
    )
    broker = InMemoryBroker(await_inplace=True).with_middlewares(middleware, PipelineMiddleware())
    await store.register_worker("worker-1", "default")

    ran, cleaned, lineages = [], [], []
    release = threading.Event()

    def job(**labels):
        return lambda fn: broker.task(task_name=fn.__name__, **labels)(guarded(fn))

    @job()
    async def ok():
        ran.append("ok")
        return 1

    @job()
    async def double(value):
        ran.append("double")
        return value * 2

    @job()
    async def boom():
        raise ValueError("bad input")

    @job()
    async def slow():
        try:
            await asyncio.sleep(30)
        finally:
            cleaned.append(True)

    @job(cancel_grace_seconds="0.3")
    def stubborn():
        while not release.is_set():
            time.sleep(0.02)

    @job()
    async def reports():
        lineages.append(current_lineage())
        report_progress(50, "half")
        await asyncio.sleep(0.4)

    await broker.startup()
    yield SimpleNamespace(
        broker=broker, middleware=middleware, seen=seen, ended=ended, ran=ran, cleaned=cleaned,
        lineages=lineages, release=release, ok=ok, double=double, boom=boom, slow=slow,
        stubborn=stubborn, reports=reports, registry=registry,
    )
    release.set()
    await broker.shutdown()


async def submit(task, *args):
    with bound_submission(Submission(producer_code="test.producer", correlation_id="c-1")):
        return await task.kiq(*args)


def events_seen(app) -> list[str]:
    """Hook events in order, leaving out task_queued, which the in-memory broker fires late."""
    return [payload.event for payload in app.seen if payload.event != "task_queued"]


def task_rows(url: str):
    return rows(url, "SELECT * FROM jobs.task_instance ORDER BY step_index")


def run_row(url: str):
    (run,) = rows(url, "SELECT * FROM jobs.job_run")
    return run


def event_codes(url: str) -> list[str]:
    result = rows(url, "SELECT task_event_code FROM jobs.task_event ORDER BY task_event_id")
    return [row.task_event_code for row in result]


def status_is(url: str, status: str):
    return lambda: any(t.task_status_code == status for t in task_rows(url))


async def test_a_successful_task_is_claimed_recorded_and_its_run_finished(app, empty_database_url):
    await submit(app.ok)

    (task,) = task_rows(empty_database_url)
    assert (task.task_status_code, task.attempt, task.worker_id) == ("succeeded", 1, "worker-1")
    assert task.finished_at is not None
    assert event_codes(empty_database_url) == ["queued", "claimed", "succeeded"]
    run = run_row(empty_database_url)
    assert run.job_run_status_code == "succeeded"
    assert run.finished_at is not None
    assert events_seen(app) == ["task_started", "task_succeeded", "run_finished"]
    finished = [p for p in app.seen if p.event == "run_finished"]
    assert finished[0].run_status == "succeeded"


async def test_a_failed_task_keeps_its_error_and_fails_its_run(app, empty_database_url):
    await submit(app.boom)

    (task,) = task_rows(empty_database_url)
    assert task.task_status_code == "failed"
    assert task.error_type == "ValueError"
    assert task.error_detail == "bad input"
    assert "ValueError: bad input" in task.error_traceback
    assert run_row(empty_database_url).job_run_status_code == "failed"
    failed = next(p for p in app.seen if p.event == "task_failed")
    assert (failed.error_type, failed.error_detail) == ("ValueError", "bad input")
    assert events_seen(app) == ["task_started", "task_failed", "run_finished"]


async def test_a_failed_step_leaves_the_later_steps_upstream_failed_and_never_runs_them(
    app, empty_database_url
):
    with bound_submission(Submission(producer_code="test.producer")):
        await Pipeline(app.broker, app.boom).call_next(app.double).kiq()

    first, second = task_rows(empty_database_url)
    assert (first.task_status_code, second.task_status_code) == ("failed", "upstream_failed")
    assert run_row(empty_database_url).job_run_status_code == "failed"
    assert app.ran == []


async def test_a_successful_chain_records_every_step_and_finishes_the_run_once(
    app, empty_database_url
):
    with bound_submission(Submission(producer_code="test.producer")):
        await Pipeline(app.broker, app.ok).call_next(app.double).kiq()

    first, second = task_rows(empty_database_url)
    assert (first.task_status_code, second.task_status_code) == ("succeeded", "succeeded")
    assert run_row(empty_database_url).job_run_status_code == "succeeded"
    assert app.ran == ["ok", "double"]
    assert events_seen(app).count("run_finished") == 1


async def test_a_hook_that_raises_does_not_change_the_outcome(app, empty_database_url):
    def broken(payload):
        raise RuntimeError("hook failed")

    app.registry.on("task_succeeded", broken)

    await submit(app.ok)

    (task,) = task_rows(empty_database_url)
    assert task.task_status_code == "succeeded"
    assert run_row(empty_database_url).job_run_status_code == "succeeded"


async def test_a_cancel_stops_an_async_job_and_records_the_task_and_run_cancelled(
    app, store, empty_database_url
):
    background = asyncio.create_task(submit(app.slow))
    await wait_until(status_is(empty_database_url, "running"))

    await store.cancel_run(run_row(empty_database_url).job_run_id)
    await background

    (task,) = task_rows(empty_database_url)
    assert task.task_status_code == "cancelled"
    assert app.cleaned == [True]
    assert run_row(empty_database_url).job_run_status_code == "cancelled"
    assert events_seen(app) == ["task_started", "task_cancelled", "run_finished"]
    assert event_codes(empty_database_url) == ["queued", "claimed", "cancel_requested", "cancelled"]


async def test_a_task_taken_by_another_worker_is_left_alone_when_the_lease_is_lost(
    app, store, empty_database_url
):
    await store.register_worker("worker-2", "default")
    background = asyncio.create_task(submit(app.slow))
    await wait_until(status_is(empty_database_url, "running"))
    (task,) = task_rows(empty_database_url)
    rows(empty_database_url, "UPDATE jobs.task_instance SET task_status_code = 'queued' RETURNING 1")
    await store.claim(task.task_instance_id, "worker-2", lease_seconds=60)

    await background

    (task,) = task_rows(empty_database_url)
    assert (task.task_status_code, task.attempt, task.worker_id) == ("running", 2, "worker-2")
    assert app.cleaned == [True]
    assert event_codes(empty_database_url) == ["queued", "claimed", "claimed"]
    assert events_seen(app) == ["task_started"]


async def test_a_duplicate_delivery_does_not_run_the_job_or_save_a_result(
    app, store, empty_database_url
):
    await store.register_worker("worker-2", "default")
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    message = TaskiqMessage(task_id=task_id.hex, task_name="ok", labels={}, args=[], kwargs={})
    await store.mark_queued(task_id, message.model_dump(mode="json"))
    await store.claim(task_id, "worker-2", lease_seconds=60)

    await app.broker.kick(app.broker.formatter.dumps(message))

    assert app.ran == []
    assert task_id.hex not in app.broker.result_backend.results
    (task,) = task_rows(empty_database_url)
    assert (task.task_status_code, task.worker_id) == ("running", "worker-2")
    assert event_codes(empty_database_url)[-1] == "duplicate_skipped"


async def test_a_task_that_ignores_its_cancel_is_recorded_cancelled_and_the_process_is_ended(
    app, store, empty_database_url
):
    background = asyncio.create_task(submit(app.stubborn))
    await wait_until(status_is(empty_database_url, "running"))

    await store.cancel_run(run_row(empty_database_url).job_run_id)
    await wait_until(lambda: app.ended, timeout=5)

    assert task_rows(empty_database_url)[0].task_status_code == "cancelled"
    app.release.set()
    await background
    assert task_rows(empty_database_url)[0].task_status_code == "cancelled"
    assert "task_succeeded" not in events_seen(app)
    assert app.ended == [True]


async def test_a_job_sees_its_lineage_and_its_progress_is_recorded(app, empty_database_url):
    background = asyncio.create_task(submit(app.reports))

    def progress_recorded():
        return any(t.progress_pct is not None for t in task_rows(empty_database_url))

    await wait_until(progress_recorded)
    await background

    (lineage,) = app.lineages
    assert (lineage.attempt, lineage.step_index, lineage.step_count) == (1, 0, 1)
    assert (lineage.worker_id, lineage.producer_code, lineage.correlation_id) == (
        "worker-1",
        "test.producer",
        "c-1",
    )
    (task,) = task_rows(empty_database_url)
    assert (float(task.progress_pct), task.progress_note) == (50, "half")


async def test_a_database_failure_at_claim_stops_the_job_before_it_runs(
    app, store, monkeypatch
):
    async def unreachable(*args, **kwargs):
        raise ConnectionError("database unreachable")

    monkeypatch.setattr(store, "claim", unreachable)
    message = TaskiqMessage(task_id=uuid.uuid7().hex, task_name="ok", labels={}, args=[], kwargs={})

    with pytest.raises(ConnectionError):
        await app.middleware.pre_execute(message)

    assert current_attempt() is None
    assert app.ran == []
