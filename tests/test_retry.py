import asyncio
import uuid
from types import SimpleNamespace

import pytest
from helpers import rows, wait_until
from pydantic import TypeAdapter, ValidationError
from taskiq import InMemoryBroker, TaskiqMessage
from taskiq.exceptions import NoResultError
from taskiq.kicker import AsyncKicker
from taskiq_pipelines import Pipeline, PipelineMiddleware

from taskline.errors import (
    InvalidStepResult,
    MissingValue,
    NonRetryable,
    TaskCancelled,
    ValuesTooLarge,
)
from taskline.execution import guarded
from taskline.hook_registry import EVENTS, HookRegistry
from taskline.lineage import LineageContext, bound_lineage
from taskline.middleware import ChainMiddleware, TasklineMiddleware
from taskline.retry import is_retryable, retry_delay, should_retry
from taskline.spec import TaskSpec
from taskline.stepping import stepped
from taskline.submission import Submission, bound_submission

FAST = {"retry_delay_seconds": 0.01, "retry_jitter": 0, "retry_max_delay_seconds": 0.05}


def lineage(attempt: int, max_attempts: int) -> LineageContext:
    return LineageContext(
        uuid.uuid7(), uuid.uuid7(), 0, 1, "t", attempt, max_attempts, None, "p", "w"
    )


def test_the_wait_grows_by_the_tasks_own_factor_and_is_capped():
    labels = TaskSpec(
        name="t", retry_delay_seconds=5, retry_backoff_factor=3, retry_max_delay_seconds=100,
        retry_jitter=0,
    ).labels()

    assert [retry_delay(attempt, labels) for attempt in (1, 2, 3, 4, 5)] == [5, 15, 45, 100, 100]


def test_the_jitter_stays_within_its_bounds_and_never_exceeds_the_cap():
    labels = TaskSpec(
        name="t", retry_delay_seconds=10, retry_max_delay_seconds=10, retry_jitter=0.2
    ).labels()
    first = TaskSpec(name="t", retry_delay_seconds=10, retry_max_delay_seconds=100,
                     retry_jitter=0.2).labels()

    capped = [retry_delay(1, labels) for _ in range(300)]
    spread = [retry_delay(1, first) for _ in range(300)]

    assert max(capped) <= 10
    assert all(8 <= value <= 12 for value in spread)
    assert max(spread) > 11 and min(spread) < 9


def test_a_message_without_retry_labels_uses_the_defaults():
    assert 4 <= retry_delay(1, {}) <= 6


@pytest.mark.parametrize(
    "error",
    [NonRetryable("x"), TaskCancelled(), MissingValue("x"), InvalidStepResult("x"), ValuesTooLarge("x")],
)
def test_errors_that_would_repeat_or_were_asked_for_are_not_retried(error):
    assert not is_retryable(error)


def test_a_validation_error_is_not_retried():
    with pytest.raises(ValidationError) as caught:
        TypeAdapter(int).validate_python("many")

    assert not is_retryable(caught.value)


def test_ordinary_errors_are_retried_and_no_error_means_no_retry():
    assert is_retryable(ConnectionError("down"))
    assert is_retryable(ValueError("bad"))
    assert not is_retryable(None)


def test_a_task_with_no_attempts_left_is_not_retried():
    assert should_retry(lineage(1, 3), ConnectionError())
    assert should_retry(lineage(2, 3), ConnectionError())
    assert not should_retry(lineage(3, 3), ConnectionError())
    assert not should_retry(lineage(1, 1), ConnectionError())


@pytest.fixture
async def app(store):
    registry = HookRegistry()
    seen = []
    for event in EVENTS:
        registry.on(event, seen.append)
    await store.register_worker("worker-1", "default")
    broker = InMemoryBroker(await_inplace=True).with_middlewares(
        TasklineMiddleware(store, registry, worker_id="worker-1", heartbeat_interval=0.05),
        ChainMiddleware(),
    )
    calls = {"flaky": 0}
    ran = []

    def job(max_attempts=3):
        def register(fn):
            spec = TaskSpec(name=fn.__name__, max_attempts=max_attempts, **FAST)
            return broker.task(task_name=fn.__name__, **spec.labels())(guarded(fn))

        return register

    @job()
    async def flaky():
        calls["flaky"] += 1
        if calls["flaky"] == 1:
            raise ConnectionError("network down")
        return 21

    @job(max_attempts=2)
    async def always_fails():
        raise ConnectionError("still down")

    @job()
    async def refuses():
        raise NonRetryable("do not repeat")

    @job()
    async def double(value):
        ran.append(f"double({value})")
        return value * 2

    @job()
    async def slow():
        await asyncio.sleep(30)

    def needs_an_int(n: int) -> int:
        return n

    typed_task = broker.task(
        task_name="typed",
        **TaskSpec(name="typed", max_attempts=3, **FAST).labels(),
    )(guarded(stepped(needs_an_int)))

    await broker.startup()
    yield SimpleNamespace(
        broker=broker, store=store, seen=seen, flaky=flaky, always_fails=always_fails,
        refuses=refuses, double=double, slow=slow, typed=typed_task, ran=ran,
    )
    await broker.shutdown()


async def submit(task, *args, **kwargs):
    with bound_submission(Submission(producer_code="test.producer")):
        return await task.kiq(*args, **kwargs)


async def resend_due_retries(app) -> int:
    """What the reaper does for due retries: queue them and send their stored messages."""
    await asyncio.sleep(0.1)
    resent = await app.store.take_due_retries(limit=10)
    for item in resent:
        message = TaskiqMessage.model_validate(item.message)
        await app.broker.kick(app.broker.formatter.dumps(message))
    return len(resent)


def task_rows(url: str):
    return rows(url, "SELECT * FROM jobs.task_instance ORDER BY step_index")


def run_status(url: str) -> str:
    return rows(url, "SELECT job_run_status_code FROM jobs.job_run")[0].job_run_status_code


def events_seen(app) -> list[str]:
    return [p.event for p in app.seen if p.event != "task_queued"]


async def test_a_failed_task_with_attempts_left_is_scheduled_for_retry_and_the_chain_is_not_failed(
    app, empty_database_url
):
    with bound_submission(Submission(producer_code="test.producer")):
        task = await Pipeline(app.broker, app.flaky).call_next(app.double).kiq()

    first, second = task_rows(empty_database_url)
    assert first.task_status_code == "retrying"
    assert first.attempt == 1
    assert (first.error_type, first.error_detail) == ("ConnectionError", "network down")
    assert first.retry_at is not None
    assert second.task_status_code == "planned"
    assert run_status(empty_database_url) == "running"
    assert events_seen(app) == ["task_started", "task_retrying"]
    assert not await app.broker.result_backend.is_result_ready(task.task_id)
    retrying = next(p for p in app.seen if p.event == "task_retrying")
    assert retrying.error_type == "ConnectionError"


async def test_the_retry_succeeds_on_the_second_attempt_and_the_chain_continues(
    app, empty_database_url
):
    with bound_submission(Submission(producer_code="test.producer")):
        await Pipeline(app.broker, app.flaky).call_next(app.double).kiq()

    assert await resend_due_retries(app) == 1

    first, second = task_rows(empty_database_url)
    assert (first.task_status_code, first.attempt) == ("succeeded", 2)
    assert (first.error_type, first.error_detail) == (None, None)
    assert second.task_status_code == "succeeded"
    assert app.ran == ["double(21)"]
    assert run_status(empty_database_url) == "succeeded"
    codes = [e.task_event_code for e in rows(
        empty_database_url,
        "SELECT task_event_code FROM jobs.task_event WHERE task_instance_id = :id "
        "ORDER BY task_event_id",
        id=first.task_instance_id,
    )]
    assert codes == ["queued", "claimed", "retry_scheduled", "requeued", "claimed", "succeeded"]
    assert events_seen(app).count("run_finished") == 1


async def test_the_last_attempt_fails_for_good_and_later_steps_are_upstream_failed(
    app, empty_database_url
):
    with bound_submission(Submission(producer_code="test.producer")):
        await Pipeline(app.broker, app.always_fails).call_next(app.double).kiq()
    assert await resend_due_retries(app) == 1

    first, second = task_rows(empty_database_url)
    assert (first.task_status_code, first.attempt) == ("failed", 2)
    assert second.task_status_code == "upstream_failed"
    assert run_status(empty_database_url) == "failed"
    assert events_seen(app).count("task_retrying") == 1
    assert events_seen(app).count("task_failed") == 1
    assert app.ran == []


async def test_a_non_retryable_error_fails_at_once_with_attempts_left(app, empty_database_url):
    await submit(app.refuses)

    (task,) = task_rows(empty_database_url)
    assert (task.task_status_code, task.attempt, task.error_type) == ("failed", 1, "NonRetryable")
    assert "task_retrying" not in events_seen(app)


async def test_a_value_that_does_not_fit_is_not_retried(app, empty_database_url):
    await submit(app.typed, {"x": "many"}, taskline_step={"inputs": {"n": "x"}})

    (task,) = task_rows(empty_database_url)
    assert (task.task_status_code, task.attempt, task.error_type) == ("failed", 1, "ValidationError")


async def test_a_cancel_is_not_retried_even_with_attempts_left(app, empty_database_url):
    background = asyncio.create_task(submit(app.slow))
    await wait_until(lambda: any(t.task_status_code == "running" for t in task_rows(empty_database_url)))

    await app.store.cancel_run(rows(empty_database_url, "SELECT job_run_id FROM jobs.job_run")[0].job_run_id)
    await background

    (task,) = task_rows(empty_database_url)
    assert task.task_status_code == "cancelled"
    assert "task_retrying" not in events_seen(app)


async def test_the_chain_middleware_ignores_the_no_result_signal(monkeypatch):
    calls = []

    async def record(self, message, result, exception):
        calls.append(exception)

    monkeypatch.setattr(PipelineMiddleware, "on_error", record)

    await ChainMiddleware().on_error(None, None, NoResultError())

    assert calls == []


async def test_the_chain_middleware_ignores_a_task_about_to_be_retried_but_fails_a_final_failure(
    monkeypatch,
):
    calls = []

    async def record(self, message, result, exception):
        calls.append(exception)

    monkeypatch.setattr(PipelineMiddleware, "on_error", record)
    middleware = ChainMiddleware()
    error = ConnectionError("down")

    with bound_lineage(lineage(1, 3)):
        await middleware.on_error(None, None, error)
    assert calls == []

    with bound_lineage(lineage(3, 3)):
        await middleware.on_error(None, None, error)
    assert calls == [error]


async def test_a_kicker_built_from_a_spec_carries_the_retry_settings():
    labels = TaskSpec(name="t", retry_delay_seconds=2).labels()

    assert AsyncKicker("t", InMemoryBroker(), labels).labels["retry_delay_seconds"] == "2"
