import pytest
from helpers import make_plan, rows
from sqlalchemy.exc import IntegrityError

from taskline.store import ReferencePlan, RunPlan


async def test_single_job_registers_one_planned_task(store, empty_database_url):
    plan = make_plan()

    await store.register_run(plan)

    (run,) = rows(empty_database_url, "SELECT * FROM jobs.job_run")
    assert run.job_run_id == plan.job_run_id
    assert run.job_name == "fetch_source"
    assert run.job_trigger_code == "api"
    assert run.job_run_status_code == "running"
    assert run.step_count == 1
    assert run.producer_code == "test.producer"
    assert run.producer_host == "host-1"
    assert run.triggered_by == "anil"
    assert run.correlation_id == "upload-42"
    (task,) = rows(empty_database_url, "SELECT * FROM jobs.task_instance")
    assert task.task_instance_id == plan.steps[0].task_instance_id
    assert task.task_status_code == "planned"
    assert task.attempt == 0
    assert task.max_attempts == 3
    assert task.queue_name == "default"


async def test_chain_registers_every_step_in_order(store, empty_database_url):
    plan = make_plan(step_count=3)

    await store.register_run(plan)

    tasks = rows(
        empty_database_url,
        "SELECT step_index, task_name, task_status_code FROM jobs.task_instance "
        "ORDER BY step_index",
    )
    assert [(t.step_index, t.task_name, t.task_status_code) for t in tasks] == [
        (0, "step_0", "planned"),
        (1, "step_1", "planned"),
        (2, "step_2", "planned"),
    ]
    (run,) = rows(empty_database_url, "SELECT step_count FROM jobs.job_run")
    assert run.step_count == 3


async def test_references_attach_to_run_or_step(store, empty_database_url):
    references = [
        ReferencePlan("source", "src-7", "subject", label="Zurich 10-K", url="http://x/src-7"),
        ReferencePlan("document", "doc-1", "produced", step_index=1),
    ]
    plan = make_plan(step_count=2, references=references)

    await store.register_run(plan)

    stored = {
        r.external_id: r
        for r in rows(empty_database_url, "SELECT * FROM jobs.job_reference")
    }
    assert stored["src-7"].task_instance_id is None
    assert stored["src-7"].label == "Zurich 10-K"
    assert stored["src-7"].url == "http://x/src-7"
    assert stored["src-7"].reference_relation_code == "subject"
    assert stored["doc-1"].task_instance_id == plan.steps[1].task_instance_id
    assert stored["doc-1"].reference_kind_code == "document"
    assert stored["doc-1"].reference_relation_code == "produced"


async def test_reference_ids_are_generated_by_the_database_as_uuid7(store, empty_database_url):
    await store.register_run(make_plan(references=[ReferencePlan("document", "doc-1", "subject")]))

    (reference,) = rows(empty_database_url, "SELECT job_reference_id FROM jobs.job_reference")
    assert reference.job_reference_id.version == 7


async def test_unregistered_producer_is_refused_and_nothing_is_kept(store, empty_database_url):
    with pytest.raises(IntegrityError):
        await store.register_run(make_plan(step_count=2, producer_code="nobody"))

    assert rows(empty_database_url, "SELECT 1 FROM jobs.job_run") == []
    assert rows(empty_database_url, "SELECT 1 FROM jobs.task_instance") == []


async def test_unregistered_reference_kind_is_refused_and_nothing_is_kept(store, empty_database_url):
    plan = make_plan(references=[ReferencePlan("spaceship", "x", "subject")])

    with pytest.raises(IntegrityError):
        await store.register_run(plan)

    assert rows(empty_database_url, "SELECT 1 FROM jobs.job_run") == []
    assert rows(empty_database_url, "SELECT 1 FROM jobs.task_instance") == []


async def test_registering_the_same_run_twice_fails(store):
    plan = make_plan()
    await store.register_run(plan)

    with pytest.raises(IntegrityError):
        await store.register_run(plan)


async def test_the_same_reference_cannot_be_attached_twice(store):
    same = ReferencePlan("document", "doc-1", "subject")
    plan = make_plan(references=[same, same])

    with pytest.raises(IntegrityError):
        await store.register_run(plan)


async def test_a_run_needs_at_least_one_step(store):
    plan = make_plan(step_count=1)
    empty = RunPlan(
        plan.job_run_id, plan.job_name, plan.job_trigger_code, plan.producer_code, steps=[]
    )

    with pytest.raises(ValueError, match="at least one step"):
        await store.register_run(empty)


async def test_a_reference_to_a_missing_step_is_refused(store):
    plan = make_plan(step_count=1, references=[ReferencePlan("document", "d", "subject", step_index=5)])

    with pytest.raises(ValueError, match="missing step 5"):
        await store.register_run(plan)


async def test_mark_queued_stores_message_and_logs_event(store, empty_database_url):
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id

    info = await store.mark_queued(task_id, {"task_name": "step_0", "args": [1]})

    assert info.task_instance_id == task_id
    assert info.job_run_id == plan.job_run_id
    assert (info.step_index, info.step_count, info.task_name) == (0, 1, "step_0")
    assert (info.attempt, info.max_attempts) == (0, 3)
    assert (info.correlation_id, info.producer_code) == ("upload-42", "test.producer")

    (task,) = rows(empty_database_url, "SELECT * FROM jobs.task_instance")
    assert task.task_status_code == "queued"
    assert task.message == {"task_name": "step_0", "args": [1]}
    assert task.queued_at is not None
    (event,) = rows(empty_database_url, "SELECT * FROM jobs.task_event")
    assert event.task_instance_id == task_id
    assert event.task_event_code == "queued"
    assert event.attempt == 0


async def test_mark_queued_refuses_a_cancelled_task(store, empty_database_url):
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET task_status_code = 'cancelled' "
        "WHERE task_instance_id = :id RETURNING 1",
        id=task_id,
    )

    assert await store.mark_queued(task_id, {"task_name": "step_0"}) is None

    (task,) = rows(empty_database_url, "SELECT task_status_code, message FROM jobs.task_instance")
    assert task.task_status_code == "cancelled"
    assert task.message is None
    assert rows(empty_database_url, "SELECT 1 FROM jobs.task_event") == []
