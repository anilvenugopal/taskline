import asyncio
import time

from helpers import queued_task, rows, wait_until

from taskline.context import bound_lease, cancel_requested, report_progress
from taskline.lease import Lease


async def running_task(store, url: str):
    await store.register_worker("worker-1", "default")
    task_id = await queued_task(store, url)
    await store.claim(task_id, "worker-1", lease_seconds=60)
    return task_id


def task_row(url: str):
    (task,) = rows(
        url,
        "SELECT heartbeat_at, progress_pct, progress_note, "
        "EXTRACT(EPOCH FROM lease_expires_at - heartbeat_at) AS lease_seconds "
        "FROM jobs.task_instance",
    )
    return task


def make_lease(store, task_id, **options) -> Lease:
    return Lease(
        store, task_id, 1, heartbeat_interval=0.1, lease_seconds=30, **options
    )


def request_cancel(url: str) -> None:
    rows(url, "UPDATE jobs.task_instance SET cancel_requested_at = now() RETURNING 1")


class FailingStore:
    async def heartbeat(self, *args, **kwargs):
        raise ConnectionError("database unreachable")


async def test_heartbeats_renew_the_lease_until_stopped(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    claimed_at = task_row(empty_database_url).heartbeat_at
    lease = make_lease(store, task_id)

    lease.start()
    await wait_until(lambda: task_row(empty_database_url).heartbeat_at > claimed_at)
    await lease.stop()

    assert float(task_row(empty_database_url).lease_seconds) == 30
    stopped_at = task_row(empty_database_url).heartbeat_at
    await asyncio.sleep(0.4)
    assert task_row(empty_database_url).heartbeat_at == stopped_at


async def test_progress_is_sent_with_the_next_heartbeat_and_kept(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    lease = make_lease(store, task_id)
    lease.start()

    lease.set_progress(40, "halfway")
    await wait_until(lambda: task_row(empty_database_url).progress_pct is not None)
    await asyncio.sleep(0.3)
    await lease.stop()

    task = task_row(empty_database_url)
    assert float(task.progress_pct) == 40
    assert task.progress_note == "halfway"


async def test_a_cancel_request_is_noticed_once_and_heartbeats_continue(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    calls = []
    lease = make_lease(store, task_id, on_cancel=lambda: calls.append("cancel"))
    lease.start()

    request_cancel(empty_database_url)
    await wait_until(lambda: lease.cancel_requested)
    beat_at_cancel = task_row(empty_database_url).heartbeat_at
    await wait_until(lambda: task_row(empty_database_url).heartbeat_at > beat_at_cancel)
    await lease.stop()

    assert calls == ["cancel"]
    assert not lease.lost


async def test_a_task_taken_by_another_worker_is_noticed_as_lost(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    calls = []
    lease = make_lease(store, task_id, on_lost=lambda: calls.append("lost"))
    await store.register_worker("worker-2", "default")
    rows(empty_database_url, "UPDATE jobs.task_instance SET task_status_code = 'queued' RETURNING 1")
    await store.claim(task_id, "worker-2", lease_seconds=60)
    lease.start()

    await wait_until(lambda: lease.lost)
    await lease.stop()

    assert calls == ["lost"]
    assert not lease.cancel_requested


async def test_losing_the_database_marks_the_lease_lost_only_after_the_lease_length():
    calls = []
    lease = Lease(
        FailingStore(),
        task_instance_id=None,
        attempt=1,
        heartbeat_interval=0.05,
        lease_seconds=0.4,
        on_lost=lambda: calls.append("lost"),
    )
    started = time.monotonic()
    lease.start()

    await asyncio.sleep(0.2)
    assert not lease.lost

    await wait_until(lambda: lease.lost)
    assert time.monotonic() - started >= 0.4
    assert calls == ["lost"]
    await lease.stop()


async def test_the_grace_timer_starts_when_the_cancel_is_noticed(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    expired_at = []

    async def on_grace_expired():
        expired_at.append(time.monotonic())

    lease = make_lease(store, task_id, cancel_grace_seconds=0.3, on_grace_expired=on_grace_expired)
    lease.start()

    request_cancel(empty_database_url)
    await wait_until(lambda: lease.cancel_requested)
    noticed_at = time.monotonic()
    await wait_until(lambda: expired_at)
    await lease.stop()

    assert expired_at[0] - noticed_at >= 0.25


async def test_no_grace_period_means_nothing_is_ever_ended(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    called = []

    async def on_grace_expired():
        called.append(True)

    lease = make_lease(store, task_id, on_grace_expired=on_grace_expired)
    lease.start()

    request_cancel(empty_database_url)
    await wait_until(lambda: lease.cancel_requested)
    await asyncio.sleep(0.5)
    await lease.stop()

    assert called == []


async def test_stopping_the_lease_cancels_a_pending_grace_timer(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    called = []

    async def on_grace_expired():
        called.append(True)

    lease = make_lease(store, task_id, cancel_grace_seconds=0.4, on_grace_expired=on_grace_expired)
    lease.start()

    request_cancel(empty_database_url)
    await wait_until(lambda: lease.cancel_requested)
    await lease.stop()
    await asyncio.sleep(0.6)

    assert called == []


async def test_job_code_sees_cancel_and_reports_progress_through_the_lease(store, empty_database_url):
    task_id = await running_task(store, empty_database_url)
    lease = make_lease(store, task_id)
    lease.start()

    assert cancel_requested() is False
    report_progress(10, "ignored outside a task")

    with bound_lease(lease):
        report_progress(70, "page 350 of 500")
        request_cancel(empty_database_url)
        await wait_until(lambda: lease.cancel_requested)
        assert await asyncio.to_thread(cancel_requested) is True
        await wait_until(lambda: task_row(empty_database_url).progress_pct is not None)

    await lease.stop()

    assert cancel_requested() is False
    task = task_row(empty_database_url)
    assert (float(task.progress_pct), task.progress_note) == (70, "page 350 of 500")
