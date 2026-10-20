"""taskline.api: the commands, their results and refusals, the actor in the event log, and the
event-loop behaviour a host depends on. Shared helpers (failed_run, message_for, set_task, statuses,
run_status, stream_task_ids) move from test_cli_write.py to helpers.py."""

import asyncio
import uuid

import pytest
from helpers import (
    failed_run,
    make_plan,
    message_for,
    rows,
    run_status,
    set_task,
    statuses,
    stream_task_ids,
)

from taskline import Job, Reference, Step, TaskSpec, catalogue
from taskline.api import (
    REFUSAL_CODES,
    cancel_run,
    requeue,
    retry_run,
    shutdown,
    submit_job,
)
from taskline.runtime import get_broker

ACTOR = "lattice:user:42"
FETCH = TaskSpec(name="fetch_source", queue="work", max_attempts=2)
PARSE = TaskSpec(name="parse_pdf", queue="work", requires_references=("document",))
WORKER = "host-1:4242:abc123"


@pytest.fixture(autouse=True)
def fresh_catalogue():
    catalogue._reset()
    yield
    catalogue._reset()


def event_details(url, event):
    found = rows(
        url,
        "SELECT detail FROM jobs.task_event WHERE task_event_code = :event ORDER BY task_event_id",
        event=event,
    )
    return [r.detail for r in found]


async def running_run(store):
    """Step 1 running under a worker, step 2 planned."""
    await store.register_worker(WORKER, "work")
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    first = plan.steps[0].task_instance_id
    await store.mark_queued(first, {})
    await store.claim(first, WORKER, 60)
    return plan


# --- cancel_run ---------------------------------------------------------------------------------


async def test_cancel_run_cancels_unstarted_tasks_and_records_the_actor(runtime, store, empty_database_url):
    plan = make_plan(step_count=2)
    await store.register_run(plan)

    result = await cancel_run(plan.job_run_id, actor=ACTOR)

    assert (result.ok, result.refusal, result.cancelled, result.cancel_requested) == (True, None, 2, 0)
    assert statuses(empty_database_url) == ["cancelled", "cancelled"]
    assert run_status(empty_database_url) == "cancelled"
    assert event_details(empty_database_url, "cancelled") == [{"actor": ACTOR}] * 2


async def test_cancel_run_asks_a_running_task_to_stop_and_records_the_actor(runtime, store, empty_database_url):
    plan = await running_run(store)

    result = await cancel_run(plan.job_run_id, actor=ACTOR)

    assert (result.ok, result.cancelled, result.cancel_requested) == (True, 1, 1)
    assert run_status(empty_database_url) == "running"  # until the task stops
    assert event_details(empty_database_url, "cancel_requested") == [{"actor": ACTOR}]


async def test_cancel_run_refuses_a_run_that_has_finished_or_does_not_exist(runtime, store, empty_database_url):
    plan = await failed_run(store, empty_database_url)

    finished = await cancel_run(plan.job_run_id, actor=ACTOR)
    unknown = await cancel_run(make_plan().job_run_id, actor=ACTOR)

    assert not finished.ok
    assert (finished.refusal.code, "already failed" in finished.refusal.message) == ("not_running", True)
    assert (unknown.ok, unknown.refusal.code) == (False, "not_found")
    assert statuses(empty_database_url) == ["succeeded", "failed", "upstream_failed"]


# --- retry_run ----------------------------------------------------------------------------------


async def test_retry_run_resumes_a_failed_run_sends_its_task_and_records_the_actor(
    runtime, store, empty_database_url
):
    plan = await failed_run(store, empty_database_url)

    result = await retry_run(plan.job_run_id, actor=ACTOR)

    assert (result.ok, result.refusal, result.resent, result.unsent) == (True, None, 1, ())
    assert statuses(empty_database_url) == ["succeeded", "queued", "planned"]
    assert run_status(empty_database_url) == "running"
    assert stream_task_ids(runtime) == [str(plan.steps[1].task_instance_id)]
    assert event_details(empty_database_url, "requeued") == [{"reason": "run_retry", "actor": ACTOR}]


async def test_retry_run_refuses_what_cannot_be_retried_and_sends_nothing(runtime, store, empty_database_url):
    unfinished = make_plan()
    await store.register_run(unfinished)
    never_sent = make_plan(step_count=2)
    await store.register_run(never_sent)
    set_task(empty_database_url, never_sent.steps[0].task_instance_id, task_status_code="failed")
    set_task(empty_database_url, never_sent.steps[1].task_instance_id, task_status_code="upstream_failed")
    await store.refresh_run_status(never_sent.job_run_id)

    not_failed = await retry_run(unfinished.job_run_id, actor=ACTOR)
    unsendable = await retry_run(never_sent.job_run_id, actor=ACTOR)
    unknown = await retry_run(make_plan().job_run_id, actor=ACTOR)

    assert (not_failed.ok, not_failed.refusal.code) == (False, "not_failed")
    assert (unsendable.ok, unsendable.refusal.code) == (False, "never_sent")
    assert (unknown.ok, unknown.refusal.code) == (False, "not_found")
    assert stream_task_ids(runtime) == []


async def test_retry_run_lists_tasks_it_resumed_but_could_not_send(runtime, store, empty_database_url, monkeypatch):
    plan = await failed_run(store, empty_database_url)

    async def down(message):
        raise ConnectionError("valkey is down")

    monkeypatch.setattr(get_broker(), "kick", down)

    result = await retry_run(plan.job_run_id, actor=ACTOR)

    assert (result.ok, result.refusal, result.resent) == (True, None, 0)
    assert result.unsent == (plan.steps[1].task_instance_id,)
    assert statuses(empty_database_url) == ["succeeded", "queued", "planned"]  # requeue can send it


# --- requeue ------------------------------------------------------------------------------------


async def queued_on_work(store, url):
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    set_task(url, task_id, task_status_code="queued", queue_name="work", message=message_for(task_id))
    return task_id


async def test_requeue_sends_a_queues_queued_on_works_and_records_the_actor(runtime, store, empty_database_url):
    task_id = await queued_on_work(store, empty_database_url)

    result = await requeue("work", actor=ACTOR)

    assert (result.ok, result.refusal, result.resent, result.unsent) == (True, None, 1, ())
    assert stream_task_ids(runtime) == [str(task_id)]
    assert event_details(empty_database_url, "requeued") == [{"reason": "manual", "actor": ACTOR}]


async def test_requeue_with_older_than_leaves_recent_tasks(runtime, store, empty_database_url):
    await queued_on_work(store, empty_database_url)

    result = await requeue("work", older_than_seconds=600, actor=ACTOR)

    assert (result.resent, result.unsent) == (0, ())
    assert stream_task_ids(runtime) == []


async def test_requeue_lists_tasks_it_could_not_send(runtime, store, empty_database_url, monkeypatch):
    task_id = await queued_on_work(store, empty_database_url)

    async def down(message):
        raise ConnectionError("valkey is down")

    monkeypatch.setattr(get_broker(), "kick", down)

    result = await requeue("work", actor=ACTOR)

    assert (result.ok, result.resent, result.unsent) == (True, 0, (task_id,))


@pytest.mark.parametrize("kwargs", [{"queue": ""}, {"queue": "work", "older_than_seconds": -1}])
async def test_requeue_refuses_nonsense_arguments_by_raising(runtime, kwargs):
    with pytest.raises(ValueError):
        await requeue(actor=ACTOR, **kwargs)


# --- submit_job ---------------------------------------------------------------------------------


def register_ingest():
    return catalogue.register(
        Job("ingest", inputs=["url"], steps=[Step(FETCH, inputs={"url": "url"}, output="path")])
    )


async def test_submit_job_starts_a_registered_job_as_a_manual_run_by_the_actor(runtime, store, empty_database_url):
    register_ingest()

    result = await submit_job(
        "ingest",
        {"url": "https://example.test/a.pdf"},
        producer="test.producer",
        references=[Reference("document", "doc-1", "subject")],
        correlation_id="batch-7",
        actor=ACTOR,
    )

    assert (result.ok, result.refusal, result.problems) == (True, None, ())
    (run,) = rows(
        empty_database_url,
        "SELECT job_name, job_trigger_code, triggered_by, producer_code, correlation_id "
        "FROM jobs.job_run WHERE job_run_id = :id",
        id=result.run_id,
    )
    assert tuple(run) == ("ingest", "manual", ACTOR, "test.producer", "batch-7")
    assert rows(empty_database_url, "SELECT external_id FROM jobs.job_reference")[0][0] == "doc-1"
    assert [uuid.UUID(t) for t in stream_task_ids(runtime)] == [result.run_id]  # the stream drops dashes


async def test_submit_job_refuses_an_unregistered_job_without_recording_anything(runtime, store, empty_database_url):
    result = await submit_job("nothing", {}, producer="test.producer", actor=ACTOR)

    assert (result.ok, result.refusal.code, result.run_id) == (False, "unknown_job", None)
    assert rows(empty_database_url, "SELECT count(*) FROM jobs.job_run")[0][0] == 0


async def test_submit_job_names_each_invalid_input(runtime, store, empty_database_url):
    register_ingest()

    result = await submit_job("ingest", {"colour": "red"}, producer="test.producer", actor=ACTOR)

    assert (result.ok, result.refusal.code) == (False, "invalid_inputs")
    assert {(p.name, p.message) for p in result.problems} == {
        ("url", "is required and was not given"),
        ("colour", "is not an input of this job"),
    }
    assert rows(empty_database_url, "SELECT count(*) FROM jobs.job_run")[0][0] == 0
    assert stream_task_ids(runtime) == []


async def test_submit_job_refuses_missing_references_and_unregistered_names(runtime, store, empty_database_url):
    catalogue.register(Job("parse", inputs=["path"], steps=[Step(PARSE, inputs={"path": "path"})]))
    register_ingest()
    values = {"path": "/data/a.pdf"}

    no_reference = await submit_job("parse", values, producer="test.producer", actor=ACTOR)
    bad_producer = await submit_job("ingest", {"url": "u"}, producer="nobody.registered", actor=ACTOR)
    bad_kind = await submit_job(
        "ingest",
        {"url": "u"},
        producer="test.producer",
        references=[Reference("no_such_kind", "x", "subject")],
        actor=ACTOR,
    )

    assert no_reference.refusal.code == "missing_references"
    assert bad_producer.refusal.code == "unknown_producer"
    assert bad_kind.refusal.code == "unknown_reference_kind"
    assert rows(empty_database_url, "SELECT count(*) FROM jobs.job_run")[0][0] == 0
    assert stream_task_ids(runtime) == []


# --- the actor, the refusal codes and the event loop --------------------------------------------


@pytest.mark.parametrize("actor", ["", "  "])
async def test_every_command_needs_an_actor(runtime, actor):
    plan = make_plan()
    with pytest.raises(ValueError, match="actor"):
        await cancel_run(plan.job_run_id, actor=actor)
    with pytest.raises(ValueError, match="actor"):
        await retry_run(plan.job_run_id, actor=actor)
    with pytest.raises(ValueError, match="actor"):
        await requeue("work", actor=actor)
    with pytest.raises(ValueError, match="actor"):
        await submit_job("ingest", {}, producer="p", actor=actor)


def test_the_refusal_codes_are_the_documented_ones():
    assert set(REFUSAL_CODES) == {"cancel_run", "retry_run", "requeue", "submit_job"}


async def test_the_commands_work_again_after_shutdown(runtime, store, empty_database_url):
    task_id = await queued_on_work(store, empty_database_url)
    await requeue("work", actor=ACTOR)
    await shutdown()

    result = await requeue("work", actor=ACTOR)

    assert (result.ok, result.resent) == (True, 1)
    assert stream_task_ids(runtime) == [str(task_id)] * 2


async def test_a_fresh_asyncio_run_for_every_call_works(runtime, store, empty_database_url):
    task_id = await queued_on_work(store, empty_database_url)
    plan = make_plan()
    await store.register_run(plan)

    def calls_each_on_a_new_event_loop():
        requeues = [asyncio.run(requeue("work", actor=ACTOR)) for _ in range(4)]
        cancels = [asyncio.run(cancel_run(plan.job_run_id, actor=ACTOR)) for _ in range(2)]
        return requeues, cancels

    requeues, cancels = await asyncio.to_thread(calls_each_on_a_new_event_loop)

    # A send that fails on a dead loop would show up as unsent, not as an exception.
    assert [(r.ok, r.resent, r.unsent) for r in requeues] == [(True, 1, ())] * 4
    assert stream_task_ids(runtime) == [str(task_id)] * 4
    assert cancels[0].ok and not cancels[1].ok  # the second finds the run already cancelled
