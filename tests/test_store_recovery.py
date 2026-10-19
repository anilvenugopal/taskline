import asyncio

from helpers import make_plan, queued_task, rows


def task_row(url: str, task_id):
    (task,) = rows(
        url,
        "SELECT task_status_code, attempt, finished_at, retry_at, error_type, error_detail, "
        "EXTRACT(EPOCH FROM retry_at - now()) AS retry_in_seconds "
        "FROM jobs.task_instance WHERE task_instance_id = :id",
        id=task_id,
    )
    return task


def event_codes(url: str, task_id) -> list[str]:
    result = rows(
        url,
        "SELECT task_event_code FROM jobs.task_event WHERE task_instance_id = :id "
        "ORDER BY task_event_id",
        id=task_id,
    )
    return [row.task_event_code for row in result]


async def expired_running_task(store, url: str, attempt: int | None = None):
    """A task claimed by worker-1 whose lease ran out a minute ago."""
    await store.register_worker("worker-1", "default")
    task_id = await queued_task(store, url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    rows(
        url,
        "UPDATE jobs.task_instance SET lease_expires_at = now() - interval '1 minute', "
        "attempt = COALESCE(:attempt, attempt) WHERE task_instance_id = :id RETURNING 1",
        attempt=attempt,
        id=task_id,
    )
    return task_id


async def test_expired_lease_with_attempts_left_becomes_a_due_retry(store, empty_database_url):
    task_id = await expired_running_task(store, empty_database_url)

    recovered = await store.reap_expired_leases(limit=10)

    assert [(r.task_instance_id, r.failed) for r in recovered] == [(task_id, False)]
    task = task_row(empty_database_url, task_id)
    assert task.task_status_code == "retrying"
    assert task.finished_at is None
    assert float(task.retry_in_seconds) <= 0
    assert task.error_type == "LeaseExpired"
    assert event_codes(empty_database_url, task_id) == ["queued", "claimed", "lost"]


async def test_expired_lease_on_the_last_attempt_fails_the_task(store, empty_database_url):
    task_id = await expired_running_task(store, empty_database_url, attempt=3)

    recovered = await store.reap_expired_leases(limit=10)

    assert [(r.task_instance_id, r.failed) for r in recovered] == [(task_id, True)]
    task = task_row(empty_database_url, task_id)
    assert task.task_status_code == "failed"
    assert task.finished_at is not None
    assert task.error_type == "LeaseExpired"
    assert event_codes(empty_database_url, task_id) == ["queued", "claimed", "lost", "failed"]


async def test_a_live_lease_is_left_alone(store, empty_database_url):
    await store.register_worker("worker-1", "default")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=300)

    assert await store.reap_expired_leases(limit=10) == []

    assert task_row(empty_database_url, task_id).task_status_code == "running"


async def test_the_batch_limit_is_respected(store, empty_database_url):
    for _ in range(3):
        await expired_running_task(store, empty_database_url)

    assert len(await store.reap_expired_leases(limit=2)) == 2
    assert len(await store.reap_expired_leases(limit=2)) == 1


async def test_two_reapers_at_once_recover_each_task_exactly_once(store, empty_database_url):
    expected = {await expired_running_task(store, empty_database_url) for _ in range(8)}

    first, second = await asyncio.gather(
        store.reap_expired_leases(limit=100), store.reap_expired_leases(limit=100)
    )

    taken = [r.task_instance_id for r in (*first, *second)]
    assert len(taken) == len(set(taken)) == 8
    assert set(taken) == expected


async def test_a_due_retry_is_queued_again_with_its_message(store, empty_database_url):
    task_id = await expired_running_task(store, empty_database_url)
    await store.reap_expired_leases(limit=10)

    resend = await store.take_due_retries(limit=10)

    assert [(r.task_instance_id, r.message) for r in resend] == [(task_id, {"task_name": "step_0"})]
    task = task_row(empty_database_url, task_id)
    assert (task.task_status_code, task.attempt, task.retry_at) == ("queued", 1, None)
    assert event_codes(empty_database_url, task_id)[-1] == "requeued"
    claim = await store.claim(task_id, "worker-1", lease_seconds=30)
    assert claim.attempt == 2


async def test_a_retry_whose_message_could_not_be_sent_goes_back_to_retrying_due_at_once(
    store, empty_database_url
):
    task_id = await expired_running_task(store, empty_database_url)
    await store.reap_expired_leases(limit=10)
    await store.take_due_retries(limit=10)
    assert task_row(empty_database_url, task_id).task_status_code == "queued"

    assert await store.return_to_retrying(task_id) is True

    task = task_row(empty_database_url, task_id)
    assert task.task_status_code == "retrying"
    assert float(task.retry_in_seconds) <= 0
    assert [r.task_instance_id for r in await store.take_due_retries(limit=10)] == [task_id]


async def test_only_a_queued_task_can_be_returned_to_retrying(store, empty_database_url):
    task_id = await expired_running_task(store, empty_database_url)

    assert await store.return_to_retrying(task_id) is False

    assert task_row(empty_database_url, task_id).task_status_code == "running"


async def test_a_retry_that_is_not_due_yet_is_left(store, empty_database_url):
    await store.register_worker("worker-1", "default")
    task_id = await queued_task(store, empty_database_url)
    await store.claim(task_id, "worker-1", lease_seconds=30)
    await store.schedule_retry(task_id, 1, delay_seconds=600, error_type="E", error_detail="x")

    assert await store.take_due_retries(limit=10) == []

    assert task_row(empty_database_url, task_id).task_status_code == "retrying"


async def test_take_queued_returns_the_queued_tasks_of_one_queue_and_restamps_them(
    store, empty_database_url
):
    waiting = await queued_task(store, empty_database_url)
    other_queue = await queued_task(store, empty_database_url)
    running = await queued_task(store, empty_database_url)
    await store.register_worker("worker-1", "default")
    await store.claim(running, "worker-1", lease_seconds=30)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queue_name = 'elsewhere' WHERE task_instance_id = :id RETURNING 1",
        id=other_queue,
    )
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queued_at = now() - interval '1 day' "
        "WHERE task_instance_id = :id RETURNING 1",
        id=waiting,
    )

    taken = await store.take_queued("default")

    assert [(r.task_instance_id, r.message) for r in taken] == [(waiting, {"task_name": "step_0"})]
    (age,) = rows(
        empty_database_url,
        "SELECT EXTRACT(EPOCH FROM now() - queued_at) AS age FROM jobs.task_instance "
        "WHERE task_instance_id = :id",
        id=waiting,
    )
    assert float(age.age) < 60
    assert event_codes(empty_database_url, waiting)[-1] == "requeued"
    assert task_row(empty_database_url, waiting).task_status_code == "queued"


async def test_take_queued_can_leave_tasks_that_were_queued_a_moment_ago(store, empty_database_url):
    old = await queued_task(store, empty_database_url)
    await queued_task(store, empty_database_url)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queued_at = now() - interval '1 hour' "
        "WHERE task_instance_id = :id RETURNING 1",
        id=old,
    )

    taken = await store.take_queued("default", older_than_seconds=600)

    assert [r.task_instance_id for r in taken] == [old]


async def test_take_queued_skips_a_task_with_no_stored_message(store, empty_database_url):
    plan = make_plan()
    await store.register_run(plan)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET task_status_code = 'queued', queued_at = now() "
        "WHERE task_instance_id = :id RETURNING 1",
        id=plan.steps[0].task_instance_id,
    )

    assert await store.take_queued("default") == []


async def test_queued_tasks_are_never_taken_back_however_long_they_wait(store, empty_database_url):
    waiting = await queued_task(store, empty_database_url)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queued_at = now() - interval '10 days' "
        "WHERE task_instance_id = :id RETURNING 1",
        id=waiting,
    )

    assert await store.take_due_retries(limit=10) == []
    assert await store.reap_expired_leases(limit=10) == []
    assert await store.fail_lost_handoffs(stall_seconds=120, limit=10) == []

    assert task_row(empty_database_url, waiting).task_status_code == "queued"
    assert event_codes(empty_database_url, waiting) == ["queued"]


async def test_a_lost_handoff_fails_the_step_that_was_never_sent(store, empty_database_url):
    plan = make_plan(step_count=3)
    await store.register_run(plan)
    first, second, third = (step.task_instance_id for step in plan.steps)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET task_status_code = 'succeeded', "
        "finished_at = now() - interval '10 minutes' WHERE task_instance_id = :id RETURNING 1",
        id=first,
    )

    failed = await store.fail_lost_handoffs(stall_seconds=120, limit=10)

    assert [(r.task_instance_id, r.job_run_id, r.step_index, r.failed) for r in failed] == [
        (second, plan.job_run_id, 1, True)
    ]
    task = task_row(empty_database_url, second)
    assert task.task_status_code == "failed"
    assert task.error_type == "HandoffLost"
    assert task.finished_at is not None
    assert event_codes(empty_database_url, second) == ["failed"]
    assert task_row(empty_database_url, third).task_status_code == "planned"


async def test_a_recent_handoff_is_not_treated_as_lost(store, empty_database_url):
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET task_status_code = 'succeeded', finished_at = now() "
        "WHERE step_index = 0 RETURNING 1",
    )

    assert await store.fail_lost_handoffs(stall_seconds=120, limit=10) == []

    assert task_row(empty_database_url, plan.steps[1].task_instance_id).task_status_code == "planned"


async def test_only_stale_alive_workers_are_marked_dead(store, empty_database_url):
    for worker_id in ("stale", "fresh", "stopped"):
        await store.register_worker(worker_id, "default")
    await store.stop_worker("stopped")
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' "
        "WHERE worker_id IN ('stale', 'stopped') RETURNING 1",
    )

    assert await store.mark_dead_workers(stale_seconds=60) == ["stale"]

    statuses = {
        r.worker_id: r.worker_status_code
        for r in rows(empty_database_url, "SELECT worker_id, worker_status_code FROM jobs.worker")
    }
    assert statuses == {"stale": "dead", "fresh": "alive", "stopped": "stopped"}
