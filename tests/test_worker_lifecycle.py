import asyncio

import pytest
from helpers import queued_task, rows, wait_until
from taskiq import InMemoryBroker

from taskline.middleware import TasklineMiddleware


@pytest.fixture
def make_broker(store):
    created = []

    def make(is_worker_process: bool = True, **options) -> InMemoryBroker:
        middleware = TasklineMiddleware(
            store, worker_id="worker-1", consume_queue="parse", heartbeat_interval=0.05, **options
        )
        broker = InMemoryBroker().with_middlewares(middleware)
        broker.is_worker_process = is_worker_process
        created.append(broker)
        return broker

    yield make


def worker_rows(url: str):
    return rows(
        url,
        "SELECT worker_status_code, queue_name, stopped_at, "
        "EXTRACT(EPOCH FROM now() - last_seen_at) AS seconds_since_seen FROM jobs.worker",
    )


async def test_a_worker_registers_at_startup_and_is_stopped_after_shutdown(
    make_broker, empty_database_url
):
    broker = make_broker()

    await broker.startup()

    (worker,) = worker_rows(empty_database_url)
    assert (worker.worker_status_code, worker.queue_name) == ("alive", "parse")
    assert worker.stopped_at is None

    await broker.shutdown()

    (worker,) = worker_rows(empty_database_url)
    assert worker.worker_status_code == "stopped"
    assert worker.stopped_at is not None


async def test_a_producer_only_process_registers_nothing(make_broker, empty_database_url):
    broker = make_broker(is_worker_process=False)

    await broker.startup()
    await broker.shutdown()

    assert worker_rows(empty_database_url) == []


async def test_the_ping_keeps_last_seen_fresh(make_broker, empty_database_url):
    broker = make_broker()
    await broker.startup()
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' RETURNING 1",
    )

    await wait_until(lambda: float(worker_rows(empty_database_url)[0].seconds_since_seen) < 5)

    await broker.shutdown()


async def test_the_ping_revives_a_worker_wrongly_marked_dead(make_broker, empty_database_url):
    broker = make_broker()
    await broker.startup()
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET worker_status_code = 'dead' RETURNING 1",
    )

    await wait_until(lambda: worker_rows(empty_database_url)[0].worker_status_code == "alive")

    await broker.shutdown()


async def test_the_ping_keeps_going_after_the_database_fails(
    make_broker, store, empty_database_url, monkeypatch
):
    real_touch = store.touch_worker
    calls = {"failed": 0, "succeeded": 0}

    async def flaky(worker_id):
        if calls["failed"] < 2:
            calls["failed"] += 1
            raise ConnectionError("database unreachable")
        calls["succeeded"] += 1
        return await real_touch(worker_id)

    monkeypatch.setattr(store, "touch_worker", flaky)
    broker = make_broker()
    await broker.startup()

    await wait_until(lambda: calls["succeeded"] >= 1)

    assert calls["failed"] == 2
    await broker.shutdown()


async def test_the_ping_is_skipped_while_the_queue_is_unreachable_and_resumes_after(
    make_broker, empty_database_url
):
    state = {"reachable": False}

    async def check_queue():
        if not state["reachable"]:
            raise ConnectionError("queue unreachable")

    broker = make_broker(queue_reachable=check_queue)
    await broker.startup()
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' RETURNING 1",
    )

    await asyncio.sleep(0.4)
    assert float(worker_rows(empty_database_url)[0].seconds_since_seen) > 3000

    state["reachable"] = True
    await wait_until(lambda: float(worker_rows(empty_database_url)[0].seconds_since_seen) < 5)

    await broker.shutdown()


async def test_shutdown_expires_the_leases_of_tasks_still_held_and_the_reaper_recovers_them(
    make_broker, store, empty_database_url
):
    broker = make_broker()
    await broker.startup()
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=300)

    await broker.shutdown()
    await asyncio.sleep(0.05)

    recovered = await store.reap_expired_leases(limit=10)

    assert [(r.task_instance_id, r.failed) for r in recovered] == [(task_id, False)]
    (task,) = rows(empty_database_url, "SELECT task_status_code FROM jobs.task_instance")
    assert task.task_status_code == "retrying"
