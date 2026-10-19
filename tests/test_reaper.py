import asyncio
import uuid

import pytest
from helpers import make_plan, rows, wait_until
from redis.asyncio import Redis
from taskiq import TaskiqMessage

from taskline.broker import build_broker
from taskline.config import get_config
from taskline.hook_registry import EVENTS, HookRegistry
from taskline.queries import REAPER_QUEUE, Queries
from taskline.reaper import Reaper


@pytest.fixture
def config(empty_database_url):
    return get_config().model_copy(
        update={
            "database_url": empty_database_url,
            "namespace": f"t{uuid.uuid4().hex[:8]}",
            "reaper_interval_seconds": 0.05,
            "handoff_grace_seconds": 120,
            "dead_worker_seconds": 60,
        }
    )


@pytest.fixture
async def valkey(config):
    client = Redis.from_url(config.valkey_url)
    yield client
    keys = [key async for key in client.scan_iter(match=f"{config.namespace}:*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()


@pytest.fixture
async def reaper(store, config, valkey):
    registry = HookRegistry()
    seen = []
    for event in EVENTS:
        registry.on(event, seen.append)
    broker = build_broker(store, config=config)
    await broker.startup()
    queries = Queries(config.database_url)
    instance = Reaper(store, queries, broker, registry, config)
    instance.seen = seen
    instance.broker = broker
    instance.stream = f"{config.namespace}:work"
    yield instance
    await queries.close()
    await broker.shutdown()


def message_for(task_id: uuid.UUID, queue: str = "work") -> dict:
    return TaskiqMessage(
        task_id=task_id.hex, task_name="step_0", labels={"queue_name": queue}, args=[], kwargs={}
    ).model_dump(mode="json")


async def queued_with_message(store, step_count: int = 1):
    plan = make_plan(step_count=step_count)
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    await store.mark_queued(task_id, message_for(task_id))
    return plan


def set_queue(url: str, queue: str = "work") -> None:
    rows(url, "UPDATE jobs.task_instance SET queue_name = :queue RETURNING 1", queue=queue)


def expire_lease(url: str, task_id, attempt: int | None = None) -> None:
    rows(
        url,
        "UPDATE jobs.task_instance SET lease_expires_at = now() - interval '1 minute', "
        "attempt = COALESCE(:attempt, attempt) WHERE task_instance_id = :id RETURNING 1",
        attempt=attempt,
        id=task_id,
    )


def statuses(url: str) -> list[str]:
    result = rows(url, "SELECT task_status_code FROM jobs.task_instance ORDER BY step_index")
    return [row.task_status_code for row in result]


def events_seen(reaper) -> list[str]:
    return [payload.event for payload in reaper.seen]


async def test_an_expired_lease_with_attempts_left_becomes_a_retry_and_is_sent_at_once(
    store, reaper, valkey, empty_database_url
):
    await store.register_worker("worker-1", "work")
    plan = await queued_with_message(store)
    set_queue(empty_database_url)
    task_id = plan.steps[0].task_instance_id
    await store.claim(task_id, "worker-1", lease_seconds=60)
    expire_lease(empty_database_url, task_id)

    report = await reaper.run_once()

    assert (report.retried, report.resent, report.failed) == (1, 1, 0)
    assert statuses(empty_database_url) == ["queued"]
    assert await valkey.xlen(reaper.stream) == 1
    retrying = next(p for p in reaper.seen if p.event == "task_retrying")
    assert retrying.error_type == "LeaseExpired"


async def test_a_due_retry_is_sent_and_a_failed_send_is_tried_again_next_pass(
    store, reaper, valkey, empty_database_url, monkeypatch
):
    await queued_with_message(store)
    set_queue(empty_database_url)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET task_status_code = 'retrying', retry_at = now() "
        "RETURNING 1",
    )
    real_kick = reaper.broker.kick
    state = {"fail": True}

    async def flaky_kick(message):
        if state["fail"]:
            raise ConnectionError("valkey unreachable")
        await real_kick(message)

    monkeypatch.setattr(reaper.broker, "kick", flaky_kick)

    first = await reaper.run_once()

    assert (first.resent, first.unsent) == (0, 1)
    assert statuses(empty_database_url) == ["retrying"]
    assert await valkey.xlen(reaper.stream) == 0

    state["fail"] = False
    second = await reaper.run_once()

    assert (second.resent, second.unsent) == (1, 0)
    assert statuses(empty_database_url) == ["queued"]
    assert await valkey.xlen(reaper.stream) == 1


async def test_an_expired_lease_on_the_last_attempt_fails_the_job_and_fires_the_hooks(
    store, reaper, empty_database_url
):
    await store.register_worker("worker-1", "work")
    plan = await queued_with_message(store, step_count=2)
    set_queue(empty_database_url)
    first = plan.steps[0].task_instance_id
    await store.claim(first, "worker-1", lease_seconds=60)
    expire_lease(empty_database_url, first, attempt=3)

    report = await reaper.run_once()

    assert (report.failed, report.retried) == (1, 0)
    assert statuses(empty_database_url) == ["failed", "upstream_failed"]
    (run,) = rows(empty_database_url, "SELECT job_run_status_code FROM jobs.job_run")
    assert run.job_run_status_code == "failed"
    assert events_seen(reaper) == ["task_failed", "run_finished"]
    failed = reaper.seen[0]
    assert failed.error_type == "LeaseExpired"
    assert reaper.seen[1].run_status == "failed"


async def test_a_lost_handoff_fails_the_planned_step_and_the_job(store, reaper, empty_database_url):
    plan = make_plan(step_count=3)
    await store.register_run(plan)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET task_status_code = 'succeeded', "
        "finished_at = now() - interval '10 minutes' WHERE step_index = 0 RETURNING 1",
    )

    report = await reaper.run_once()

    assert report.failed == 1
    assert statuses(empty_database_url) == ["succeeded", "failed", "upstream_failed"]
    (error,) = rows(
        empty_database_url, "SELECT error_type FROM jobs.task_instance WHERE step_index = 1"
    )
    assert error.error_type == "HandoffLost"
    assert events_seen(reaper) == ["task_failed", "run_finished"]


async def test_a_queued_task_is_never_touched_however_long_it_waits(
    store, reaper, valkey, empty_database_url
):
    await queued_with_message(store)
    set_queue(empty_database_url)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queued_at = now() - interval '10 days' RETURNING 1",
    )

    report = await reaper.run_once()

    assert (report.retried, report.failed, report.resent) == (0, 0, 0)
    assert statuses(empty_database_url) == ["queued"]
    events = rows(empty_database_url, "SELECT task_event_code FROM jobs.task_event")
    assert [e.task_event_code for e in events] == ["queued"]
    assert await valkey.xlen(reaper.stream) == 0
    assert any("messages may be missing" in warning for warning in report.warnings)


async def test_a_worker_that_stopped_pinging_is_marked_dead_and_the_reaper_stays_alive(
    store, reaper, empty_database_url
):
    await store.register_worker("silent", "work")
    await store.register_worker("fresh", "work")
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' "
        "WHERE worker_id = 'silent' RETURNING 1",
    )
    await store.register_worker(reaper.reaper_id, REAPER_QUEUE)

    report = await reaper.run_once()

    assert report.dead_workers == 1
    states = {
        row.worker_id: row.worker_status_code
        for row in rows(empty_database_url, "SELECT worker_id, worker_status_code FROM jobs.worker")
    }
    assert states == {"silent": "dead", "fresh": "alive", reaper.reaper_id: "alive"}


async def test_a_queue_with_waiting_tasks_and_no_worker_is_warned_about_once(
    store, reaper, valkey, empty_database_url
):
    plan = await queued_with_message(store)
    set_queue(empty_database_url)
    await reaper.broker.kick(
        reaper.broker.formatter.dumps(TaskiqMessage.model_validate(message_for(plan.steps[0].task_instance_id)))
    )

    first = await reaper.run_once()
    second = await reaper.run_once()

    assert any("no live worker" in warning for warning in first.warnings)
    assert not any("messages may be missing" in warning for warning in first.warnings)
    assert second.warnings == []


async def test_a_failing_check_does_not_stop_the_others(
    store, reaper, empty_database_url, monkeypatch
):
    await store.register_worker("silent", "work")
    rows(empty_database_url, "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' RETURNING 1")

    async def broken(limit):
        raise ConnectionError("database hiccup")

    monkeypatch.setattr(store, "reap_expired_leases", broken)

    report = await reaper.run_once()

    assert report.failed_checks == ["expired leases"]
    assert report.dead_workers == 1


async def test_the_loop_registers_the_reaper_repeats_its_passes_and_stops_cleanly(
    store, reaper, empty_database_url
):
    await store.register_worker("silent", "work")
    stop = asyncio.Event()
    running = asyncio.create_task(reaper.run(stop))

    def reaper_row():
        return rows(
            empty_database_url,
            "SELECT worker_status_code, queue_name FROM jobs.worker WHERE worker_id = :id",
            id=reaper.reaper_id,
        )

    await wait_until(lambda: reaper_row() != [])
    assert reaper_row()[0].queue_name == REAPER_QUEUE
    assert await reaper._queries.live_reapers(60) == 1
    rows(empty_database_url, "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' WHERE worker_id = 'silent' RETURNING 1")
    await wait_until(
        lambda: rows(empty_database_url, "SELECT worker_status_code FROM jobs.worker WHERE worker_id = 'silent'")[0].worker_status_code == "dead"
    )

    stop.set()
    await asyncio.wait_for(running, timeout=5)

    assert reaper_row()[0].worker_status_code == "stopped"
