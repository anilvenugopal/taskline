import uuid

from helpers import (
    failed_run,
    make_plan,
    message_for,
    rows,
    run_status,
    set_task,
    statuses,
    stream_task_ids,
    taskline,
)


async def test_cancel_cancels_an_unstarted_run(runtime, store, empty_database_url):
    plan = make_plan(step_count=2)
    await store.register_run(plan)

    result = taskline("cancel", str(plan.job_run_id))

    assert result.returncode == 0
    assert result.stdout.strip() == "cancelled 2 tasks, asked 0 running tasks to stop"
    assert statuses(empty_database_url) == ["cancelled", "cancelled"]
    assert run_status(empty_database_url) == "cancelled"


async def test_cancel_refuses_a_run_that_has_finished(runtime, store, empty_database_url):
    plan = await failed_run(store, empty_database_url)

    result = taskline("cancel", str(plan.job_run_id))

    assert result.returncode == 1
    assert "already failed" in result.stderr


async def test_cancel_reports_an_unknown_run_and_a_bad_id(runtime, store):
    unknown = taskline("cancel", str(uuid.uuid7()))
    malformed = taskline("cancel", "01900000")

    assert (unknown.returncode, "no such run" in unknown.stderr) == (1, True)
    assert (malformed.returncode, "full id" in malformed.stderr) == (2, True)


async def test_retry_resumes_a_failed_run_and_sends_the_failed_task(runtime, store, empty_database_url):
    plan = await failed_run(store, empty_database_url)

    result = taskline("retry", str(plan.job_run_id))

    assert result.returncode == 0
    assert result.stdout.strip() == f"retried run {plan.job_run_id}: sent 1 tasks"
    assert statuses(empty_database_url) == ["succeeded", "queued", "planned"]
    assert run_status(empty_database_url) == "running"
    assert stream_task_ids(runtime) == [str(plan.steps[1].task_instance_id)]


async def test_retry_refuses_a_run_that_has_not_failed_and_sends_nothing(runtime, store, empty_database_url):
    plan = make_plan()
    await store.register_run(plan)

    result = taskline("retry", str(plan.job_run_id))

    assert result.returncode == 1
    assert "has not failed" in result.stderr
    assert stream_task_ids(runtime) == []


async def test_retry_refuses_a_run_with_a_failed_task_that_was_never_sent(runtime, store, empty_database_url):
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    set_task(empty_database_url, plan.steps[0].task_instance_id, task_status_code="failed")
    set_task(empty_database_url, plan.steps[1].task_instance_id, task_status_code="upstream_failed")
    await store.refresh_run_status(plan.job_run_id)

    result = taskline("retry", str(plan.job_run_id))

    assert result.returncode == 1
    assert "never sent" in result.stderr
    assert statuses(empty_database_url) == ["failed", "upstream_failed"]


async def test_requeue_sends_the_queued_tasks_of_the_queue(runtime, store, empty_database_url):
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    first, second = (step.task_instance_id for step in plan.steps)
    set_task(empty_database_url, first, task_status_code="queued", queue_name="work", message=message_for(first))
    set_task(empty_database_url, second, task_status_code="planned", queue_name="work")

    result = taskline("requeue", "--queue", "work")

    assert result.returncode == 0
    assert result.stdout.strip() == "requeue work: sent 1 of 1 tasks"
    assert stream_task_ids(runtime) == [str(first)]


async def test_requeue_with_older_than_leaves_recent_tasks(runtime, store, empty_database_url):
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    set_task(
        empty_database_url, task_id, task_status_code="queued", queue_name="work", message=message_for(task_id)
    )
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queued_at = now() WHERE task_instance_id = :id RETURNING 1",
        id=task_id,
    )

    result = taskline("requeue", "--queue", "work", "--older-than", "600")

    assert result.stdout.strip() == "requeue work: sent 0 of 0 tasks"
    assert stream_task_ids(runtime) == []



async def test_cancel_from_the_command_line_is_recorded_with_a_cli_actor(runtime, store, empty_database_url):
    plan = make_plan(step_count=2)
    await store.register_run(plan)

    result = taskline("cancel", str(plan.job_run_id))

    assert result.returncode == 0
    actors = {r.detail["actor"] for r in rows(empty_database_url, "SELECT detail FROM jobs.task_event WHERE task_event_code = 'cancelled'")}
    assert len(actors) == 1 and actors.pop().startswith("cli")
