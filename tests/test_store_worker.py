import pytest
from helpers import queued_task, rows
from sqlalchemy.exc import IntegrityError


def worker_row(url: str, worker_id: str):
    (worker,) = rows(
        url,
        "SELECT worker_status_code, queue_name, stopped_at, "
        "EXTRACT(EPOCH FROM now() - last_seen_at) AS seconds_since_seen "
        "FROM jobs.worker WHERE worker_id = :id",
        id=worker_id,
    )
    return worker


def lease_row(url: str, task_id):
    (task,) = rows(
        url,
        "SELECT task_status_code, EXTRACT(EPOCH FROM lease_expires_at - now()) AS lease_left "
        "FROM jobs.task_instance WHERE task_instance_id = :id",
        id=task_id,
    )
    return task


async def test_register_creates_an_alive_worker(store, empty_database_url):
    await store.register_worker("worker-1", "ingest")

    worker = worker_row(empty_database_url, "worker-1")
    assert worker.worker_status_code == "alive"
    assert worker.queue_name == "ingest"
    assert worker.stopped_at is None


async def test_registering_again_revives_a_stopped_worker(store, empty_database_url):
    await store.register_worker("worker-1", "ingest")
    await store.stop_worker("worker-1")

    await store.register_worker("worker-1", "ingest")

    worker = worker_row(empty_database_url, "worker-1")
    assert worker.worker_status_code == "alive"
    assert worker.stopped_at is None


async def test_touch_refreshes_last_seen_and_revives_a_dead_worker(store, empty_database_url):
    await store.register_worker("worker-1", "ingest")
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET worker_status_code = 'dead', "
        "last_seen_at = now() - interval '1 hour' RETURNING 1",
    )

    assert await store.touch_worker("worker-1") is True

    worker = worker_row(empty_database_url, "worker-1")
    assert worker.worker_status_code == "alive"
    assert float(worker.seconds_since_seen) < 5


async def test_touch_does_not_revive_a_stopped_worker_or_know_a_stranger(store, empty_database_url):
    await store.register_worker("worker-1", "ingest")
    await store.stop_worker("worker-1")

    assert await store.touch_worker("worker-1") is False
    assert await store.touch_worker("nobody") is False

    assert worker_row(empty_database_url, "worker-1").worker_status_code == "stopped"


async def test_stop_expires_only_the_leases_of_its_own_running_tasks(store, empty_database_url):
    await store.register_worker("worker-1", "default")
    await store.register_worker("worker-2", "default")
    first = await queued_task(store, empty_database_url)
    second = await queued_task(store, empty_database_url)
    await store.claim(first, "worker-1", lease_seconds=300)
    await store.claim(second, "worker-2", lease_seconds=300)

    released = await store.stop_worker("worker-1")

    assert released == [first]
    stopped = lease_row(empty_database_url, first)
    assert stopped.task_status_code == "running"
    assert float(stopped.lease_left) <= 0
    untouched = lease_row(empty_database_url, second)
    assert untouched.task_status_code == "running"
    assert float(untouched.lease_left) > 250
    worker = worker_row(empty_database_url, "worker-1")
    assert worker.worker_status_code == "stopped"
    assert worker.stopped_at is not None
    assert worker_row(empty_database_url, "worker-2").worker_status_code == "alive"


async def test_a_worker_that_never_registered_cannot_claim(store, empty_database_url):
    task_id = await queued_task(store, empty_database_url)

    with pytest.raises(IntegrityError):
        await store.claim(task_id, "ghost", lease_seconds=30)

    assert lease_row(empty_database_url, task_id).task_status_code == "queued"
