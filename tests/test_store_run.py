import json
import uuid

import pytest
from helpers import add_worker, make_plan, rows


def set_status(url: str, task_id, status: str) -> None:
    rows(
        url,
        "UPDATE jobs.task_instance SET task_status_code = :status "
        "WHERE task_instance_id = :id RETURNING 1",
        status=status,
        id=task_id,
    )


async def register_chain(store, url: str, statuses: list[str]):
    plan = make_plan(step_count=len(statuses))
    await store.register_run(plan)
    for step, status in zip(plan.steps, statuses, strict=True):
        set_status(url, step.task_instance_id, status)
    return plan


def task_statuses(url: str) -> list[str]:
    result = rows(
        url, "SELECT task_status_code FROM jobs.task_instance ORDER BY step_index"
    )
    return [row.task_status_code for row in result]


def run_row(url: str):
    (run,) = rows(url, "SELECT job_run_status_code, finished_at FROM jobs.job_run")
    return run


async def test_fail_downstream_marks_the_later_planned_steps(store, empty_database_url):
    plan = await register_chain(store, empty_database_url, ["failed", "planned", "planned", "planned"])

    marked = await store.fail_downstream(plan.job_run_id, after_step=0)

    assert set(marked) == {step.task_instance_id for step in plan.steps[1:]}
    assert task_statuses(empty_database_url) == [
        "failed",
        "upstream_failed",
        "upstream_failed",
        "upstream_failed",
    ]
    logged = rows(
        empty_database_url,
        "SELECT task_event_code, detail FROM jobs.task_event ORDER BY task_event_id",
    )
    assert [(e.task_event_code, e.detail) for e in logged] == [
        ("upstream_failed", {"after_step": 0})
    ] * 3


async def test_fail_downstream_leaves_steps_that_are_not_planned(store, empty_database_url):
    plan = await register_chain(store, empty_database_url, ["failed", "planned", "queued", "planned"])

    marked = await store.fail_downstream(plan.job_run_id, after_step=0)

    assert set(marked) == {plan.steps[1].task_instance_id, plan.steps[3].task_instance_id}
    assert task_statuses(empty_database_url) == [
        "failed",
        "upstream_failed",
        "queued",
        "upstream_failed",
    ]


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        (["succeeded", "succeeded"], "succeeded"),
        (["succeeded", "failed", "upstream_failed"], "failed"),
        (["succeeded", "cancelled", "upstream_failed"], "cancelled"),
        (["failed", "cancelled"], "cancelled"),
    ],
)
async def test_refresh_sets_the_final_status_once_every_task_has_ended(
    store, empty_database_url, statuses, expected
):
    plan = await register_chain(store, empty_database_url, statuses)

    assert await store.refresh_run_status(plan.job_run_id) == expected

    run = run_row(empty_database_url)
    assert run.job_run_status_code == expected
    assert run.finished_at is not None
    assert await store.refresh_run_status(plan.job_run_id) is None


@pytest.mark.parametrize(
    "statuses",
    [
        ["succeeded", "planned"],
        ["succeeded", "queued"],
        ["succeeded", "running"],
        ["failed", "retrying"],
    ],
)
async def test_refresh_leaves_a_run_running_while_any_task_is_unfinished(
    store, empty_database_url, statuses
):
    plan = await register_chain(store, empty_database_url, statuses)

    assert await store.refresh_run_status(plan.job_run_id) is None

    run = run_row(empty_database_url)
    assert run.job_run_status_code == "running"
    assert run.finished_at is None


async def test_cancel_run_cancels_unstarted_tasks_and_asks_running_ones_to_stop(
    store, empty_database_url
):
    plan = await register_chain(
        store, empty_database_url, ["succeeded", "running", "queued", "retrying", "planned"]
    )
    ids = [step.task_instance_id for step in plan.steps]

    result = await store.cancel_run(plan.job_run_id)

    assert set(result.cancelled) == {ids[2], ids[3], ids[4]}
    assert result.cancel_requested == [ids[1]]
    assert task_statuses(empty_database_url) == [
        "succeeded",
        "running",
        "cancelled",
        "cancelled",
        "cancelled",
    ]
    (running,) = rows(
        empty_database_url,
        "SELECT cancel_requested_at FROM jobs.task_instance WHERE step_index = 1",
    )
    assert running.cancel_requested_at is not None
    logged = rows(
        empty_database_url, "SELECT task_event_code FROM jobs.task_event ORDER BY task_event_id"
    )
    assert sorted(e.task_event_code for e in logged) == [
        "cancel_requested",
        "cancelled",
        "cancelled",
        "cancelled",
    ]


async def test_cancelling_again_changes_nothing(store, empty_database_url):
    plan = await register_chain(store, empty_database_url, ["running", "planned"])
    await store.cancel_run(plan.job_run_id)

    result = await store.cancel_run(plan.job_run_id)

    assert result.cancelled == []
    assert result.cancel_requested == []
    events = rows(empty_database_url, "SELECT 1 FROM jobs.task_event")
    assert len(events) == 2


async def test_a_cancelled_task_cannot_be_claimed_when_its_message_arrives(
    store, empty_database_url
):
    add_worker(empty_database_url, "worker-1")
    plan = await register_chain(store, empty_database_url, ["queued"])
    task_id = plan.steps[0].task_instance_id
    await store.cancel_run(plan.job_run_id)

    assert await store.claim(task_id, "worker-1", lease_seconds=30) is None

    assert task_statuses(empty_database_url) == ["cancelled"]
    last = rows(
        empty_database_url,
        "SELECT task_event_code, detail FROM jobs.task_event ORDER BY task_event_id DESC LIMIT 1",
    )[0]
    assert last.task_event_code == "duplicate_skipped"
    assert last.detail["status"] == "cancelled"


async def failed_chain(store, url: str, attempt: int, max_attempts: int, label: str | None):
    """A three-step run: step 1 succeeded, step 2 failed with its message stored, step 3 upstream_failed."""
    plan = await register_chain(store, url, ["succeeded", "failed", "upstream_failed"])
    labels = {} if label is None else {"max_attempts": label}
    rows(
        url,
        "UPDATE jobs.task_instance SET attempt = :attempt, max_attempts = :max_attempts, "
        "message = CAST(:message AS jsonb), error_type = 'ValueError', error_detail = 'bad', "
        "finished_at = now() WHERE task_instance_id = :id RETURNING 1",
        attempt=attempt,
        max_attempts=max_attempts,
        message=json.dumps({"task_name": "step_1", "labels": labels}),
        id=plan.steps[1].task_instance_id,
    )
    await store.refresh_run_status(plan.job_run_id)
    return plan


async def test_retrying_a_failed_run_resumes_it_from_the_failed_task(store, empty_database_url):
    plan = await failed_chain(store, empty_database_url, attempt=3, max_attempts=3, label="3")

    result = await store.retry_run(plan.job_run_id)

    assert result.outcome == "retried"
    assert [(item.task_instance_id, item.message["task_name"]) for item in result.resend] == [
        (plan.steps[1].task_instance_id, "step_1")
    ]
    assert task_statuses(empty_database_url) == ["succeeded", "queued", "planned"]
    (failed_step,) = rows(
        empty_database_url,
        "SELECT attempt, max_attempts, error_type, finished_at FROM jobs.task_instance "
        "WHERE step_index = 1",
    )
    assert (failed_step.attempt, failed_step.max_attempts) == (3, 6)
    assert (failed_step.error_type, failed_step.finished_at) == (None, None)
    assert run_row(empty_database_url).job_run_status_code == "running"
    assert run_row(empty_database_url).finished_at is None
    last = rows(
        empty_database_url,
        "SELECT task_event_code, detail FROM jobs.task_event ORDER BY task_event_id DESC LIMIT 1",
    )[0]
    assert (last.task_event_code, last.detail) == ("requeued", {"reason": "run_retry"})


async def test_a_second_retry_gives_the_original_number_of_attempts_not_a_growing_one(
    store, empty_database_url
):
    plan = await failed_chain(store, empty_database_url, attempt=6, max_attempts=6, label="3")

    await store.retry_run(plan.job_run_id)

    (failed_step,) = rows(empty_database_url, "SELECT max_attempts FROM jobs.task_instance WHERE step_index = 1")
    assert failed_step.max_attempts == 9


async def test_a_failed_task_without_the_label_keeps_its_own_max_attempts_as_the_set_size(
    store, empty_database_url
):
    plan = await failed_chain(store, empty_database_url, attempt=1, max_attempts=2, label=None)

    await store.retry_run(plan.job_run_id)

    (failed_step,) = rows(empty_database_url, "SELECT max_attempts FROM jobs.task_instance WHERE step_index = 1")
    assert failed_step.max_attempts == 3


async def test_only_a_failed_run_can_be_retried(store, empty_database_url):
    running = await register_chain(store, empty_database_url, ["queued"])

    assert (await store.retry_run(running.job_run_id)).outcome == "not_failed"
    assert (await store.retry_run(uuid.uuid7())).outcome == "not_found"
    assert task_statuses(empty_database_url) == ["queued"]


async def test_a_cancelled_run_cannot_be_retried(store, empty_database_url):
    plan = await register_chain(store, empty_database_url, ["queued"])
    await store.cancel_run(plan.job_run_id)
    await store.refresh_run_status(plan.job_run_id)

    assert (await store.retry_run(plan.job_run_id)).outcome == "not_failed"


async def test_a_run_whose_failed_task_was_never_sent_is_refused_and_left_unchanged(
    store, empty_database_url
):
    plan = await register_chain(store, empty_database_url, ["succeeded", "failed", "upstream_failed"])
    await store.refresh_run_status(plan.job_run_id)

    result = await store.retry_run(plan.job_run_id)

    assert result.outcome == "never_sent"
    assert task_statuses(empty_database_url) == ["succeeded", "failed", "upstream_failed"]
    assert run_row(empty_database_url).job_run_status_code == "failed"
