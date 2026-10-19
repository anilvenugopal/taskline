import uuid

from helpers import add_worker, queued_task, rows

from taskline.store import HeartbeatResult


def task_row(url: str):
    (task,) = rows(
        url,
        "SELECT task_status_code, attempt, worker_id, started_at, finished_at, heartbeat_at, "
        "lease_expires_at, retry_at, cancel_requested_at, progress_pct, progress_note, "
        "error_type, error_detail, error_traceback, "
        "EXTRACT(EPOCH FROM lease_expires_at - heartbeat_at) AS lease_seconds, "
        "EXTRACT(EPOCH FROM retry_at - now()) AS retry_in_seconds "
        "FROM jobs.task_instance",
    )
    return task


def events(url: str):
    return rows(
        url,
        "SELECT task_event_code, attempt, worker_id, detail FROM jobs.task_event "
        "ORDER BY task_event_id",
    )


async def test_claim_starts_an_attempt(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)

    claim = await store.claim(task_id, "worker-1", lease_seconds=30)

    assert claim.attempt == 1
    assert claim.max_attempts == 3
    assert claim.step_index == 0
    assert claim.step_count == 1
    assert claim.correlation_id == "upload-42"
    assert claim.producer_code == "test.producer"
    task = task_row(empty_database_url)
    assert task.task_status_code == "running"
    assert task.attempt == 1
    assert task.worker_id == "worker-1"
    assert task.started_at is not None
    assert float(task.lease_seconds) == 30
    assert [(e.task_event_code, e.attempt, e.worker_id) for e in events(empty_database_url)] == [
        ("queued", 0, None),
        ("claimed", 1, "worker-1"),
    ]


async def test_second_claim_is_skipped_and_logged(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    add_worker(empty_database_url, "worker-2")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)

    assert await store.claim(task_id, "worker-2", lease_seconds=30) is None

    task = task_row(empty_database_url)
    assert (task.task_status_code, task.attempt, task.worker_id) == ("running", 1, "worker-1")
    skipped = events(empty_database_url)[-1]
    assert skipped.task_event_code == "duplicate_skipped"
    assert skipped.worker_id == "worker-2"
    assert skipped.detail == {"status": "running", "held_by": "worker-1"}


async def test_a_finished_task_cannot_be_claimed(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    await store.finish_succeeded(task_id, 1)

    assert await store.claim(task_id, "worker-1", lease_seconds=30) is None

    assert task_row(empty_database_url).task_status_code == "succeeded"
    assert events(empty_database_url)[-1].detail["status"] == "succeeded"


async def test_claiming_an_unknown_task_returns_nothing_and_logs_nothing(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")

    assert await store.claim(uuid.uuid7(), "worker-1", lease_seconds=30) is None

    assert events(empty_database_url) == []


async def test_heartbeat_renews_the_lease_and_keeps_progress(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=5)

    result = await store.heartbeat(task_id, 1, lease_seconds=60, progress_pct=40, progress_note="halfway")

    assert result == HeartbeatResult(held=True, cancel_requested=False)
    task = task_row(empty_database_url)
    assert float(task.lease_seconds) == 60
    assert float(task.progress_pct) == 40
    assert task.progress_note == "halfway"

    await store.heartbeat(task_id, 1, lease_seconds=60)

    task = task_row(empty_database_url)
    assert float(task.progress_pct) == 40
    assert task.progress_note == "halfway"


async def test_heartbeat_reports_a_cancel_request(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET cancel_requested_at = now() RETURNING 1",
    )

    result = await store.heartbeat(task_id, 1, lease_seconds=30)

    assert result == HeartbeatResult(held=True, cancel_requested=True)


async def test_heartbeat_with_the_wrong_attempt_changes_nothing(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    before = task_row(empty_database_url)

    result = await store.heartbeat(task_id, 2, lease_seconds=60, progress_pct=90)

    assert result == HeartbeatResult(held=False, cancel_requested=False)
    after = task_row(empty_database_url)
    assert after.heartbeat_at == before.heartbeat_at
    assert after.lease_expires_at == before.lease_expires_at
    assert after.progress_pct is None


async def test_heartbeat_on_a_finished_task_is_not_held(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    await store.finish_succeeded(task_id, 1)

    result = await store.heartbeat(task_id, 1, lease_seconds=30)

    assert result == HeartbeatResult(held=False, cancel_requested=False)


async def test_finish_succeeded(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)

    assert await store.finish_succeeded(task_id, 1) is True

    task = task_row(empty_database_url)
    assert task.task_status_code == "succeeded"
    assert task.finished_at is not None
    assert task.lease_expires_at is None
    last = events(empty_database_url)[-1]
    assert (last.task_event_code, last.attempt, last.worker_id) == ("succeeded", 1, "worker-1")


async def test_finish_cancelled_ends_the_attempt_and_refuses_a_stale_one(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)

    assert await store.finish_cancelled(task_id, 2) is False
    assert task_row(empty_database_url).task_status_code == "running"

    assert await store.finish_cancelled(task_id, 1) is True

    task = task_row(empty_database_url)
    assert task.task_status_code == "cancelled"
    assert task.finished_at is not None
    assert task.lease_expires_at is None
    last = events(empty_database_url)[-1]
    assert (last.task_event_code, last.attempt, last.worker_id) == ("cancelled", 1, "worker-1")


async def test_finish_failed_keeps_the_error(store, empty_database_url):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)

    assert await store.finish_failed(task_id, 1, "ValueError", "bad input", "Traceback...") is True

    task = task_row(empty_database_url)
    assert task.task_status_code == "failed"
    assert task.finished_at is not None
    assert task.lease_expires_at is None
    assert (task.error_type, task.error_detail, task.error_traceback) == (
        "ValueError",
        "bad input",
        "Traceback...",
    )
    last = events(empty_database_url)[-1]
    assert last.task_event_code == "failed"
    assert last.detail == {"error_type": "ValueError", "error": "bad input"}


async def test_schedule_retry_then_success_clears_the_error_but_not_its_history(
    store, empty_database_url
):
    add_worker(empty_database_url, "worker-1")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)

    assert await store.schedule_retry(task_id, 1, 10, "TimeoutError", "too slow", "tb") is True

    task = task_row(empty_database_url)
    assert task.task_status_code == "retrying"
    assert task.lease_expires_at is None
    assert task.finished_at is None
    assert 0 < float(task.retry_in_seconds) <= 10
    assert (task.error_type, task.error_detail) == ("TimeoutError", "too slow")

    rows(empty_database_url, "UPDATE jobs.task_instance SET task_status_code = 'queued' RETURNING 1")
    second = await store.claim(task_id, "worker-1", lease_seconds=30)
    assert second.attempt == 2
    assert await store.finish_succeeded(task_id, 2) is True

    task = task_row(empty_database_url)
    assert task.task_status_code == "succeeded"
    assert (task.error_type, task.error_detail, task.error_traceback) == (None, None, None)
    retry_event = [e for e in events(empty_database_url) if e.task_event_code == "retry_scheduled"]
    assert len(retry_event) == 1
    assert retry_event[0].attempt == 1
    assert retry_event[0].detail["error"] == "too slow"


async def test_a_stale_attempt_cannot_touch_a_task_taken_by_another_worker(
    store, empty_database_url
):
    add_worker(empty_database_url, "worker-1")
    add_worker(empty_database_url, "worker-2")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    rows(empty_database_url, "UPDATE jobs.task_instance SET task_status_code = 'queued' RETURNING 1")
    second = await store.claim(task_id, "worker-2", lease_seconds=30)
    assert second.attempt == 2

    assert await store.heartbeat(task_id, 1, lease_seconds=30) == HeartbeatResult(False, False)
    assert await store.finish_succeeded(task_id, 1) is False
    assert await store.finish_failed(task_id, 1, "E", "late") is False
    assert await store.schedule_retry(task_id, 1, 5, "E", "late") is False

    task = task_row(empty_database_url)
    assert (task.task_status_code, task.attempt, task.worker_id) == ("running", 2, "worker-2")
    assert await store.finish_succeeded(task_id, 2) is True


async def test_finishing_a_task_that_is_not_running_is_refused(store, empty_database_url):
    task_id = await queued_task(store, empty_database_url)

    assert await store.finish_succeeded(task_id, 0) is False
    assert await store.finish_failed(task_id, 0, "E", "x") is False
    assert await store.schedule_retry(task_id, 0, 5, "E", "x") is False

    assert task_row(empty_database_url).task_status_code == "queued"
