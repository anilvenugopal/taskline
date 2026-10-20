"""taskline.testing: runs, tasks, references and workers in known states, read back through the
taskline_api views, the way a program that reads Taskline sees them."""

import asyncio
import uuid
from datetime import timedelta

import pytest
from helpers import rows, stream_task_ids
from sqlalchemy import create_engine, text

from taskline.api import requeue, retry_run
from taskline.migrate import migrate
from taskline.setup import Kind, register_producers
from taskline.store import ReferencePlan, Store
from taskline.testing import STATES, add_run, add_worker, clear


@pytest.fixture
def url(empty_database_url):
    migrate(empty_database_url)
    return empty_database_url


def run_row(url, handle):
    (row,) = rows(
        url,
        "SELECT status, current_step_index, progress_pct, cancel_requested, error_type, "
        "attempts_total, finished_at IS NOT NULL AS finished FROM taskline_api.run WHERE run_id = :id",
        id=handle.run_id,
    )
    return row


def task_states(url, handle):
    found = rows(
        url,
        "SELECT status, attempt FROM taskline_api.task WHERE run_id = :id ORDER BY step_index",
        id=handle.run_id,
    )
    return [tuple(r) for r in found]


def log(url, handle):
    found = rows(
        url,
        "SELECT event FROM taskline_api.event WHERE run_id = :id ORDER BY event_id",
        id=handle.run_id,
    )
    return [r[0] for r in found]


def test_the_states_are_the_documented_ones():
    assert STATES == (
        "queued", "running", "cancel_requested", "retrying", "succeeded", "failed", "cancelled",
    )  # fmt: skip


@pytest.mark.parametrize("state", STATES)
def test_every_state_can_be_written(url, state):
    handle = add_run(url, state=state)

    assert len(handle.task_ids) == 3
    assert run_row(url, handle).status in {"running", "succeeded", "failed", "cancelled"}


def test_a_queued_run(url):
    handle = add_run(url, state="queued")

    row = run_row(url, handle)
    assert (row.status, row.current_step_index, row.progress_pct, row.attempts_total) == (
        "running", 0, None, 0,
    )  # fmt: skip
    assert task_states(url, handle) == [("queued", 0), ("planned", 0), ("planned", 0)]
    assert log(url, handle) == ["queued"]


def test_a_running_run_has_progress_and_a_worker(url):
    handle = add_run(url, state="running", at_step=1, progress_pct=60, progress_note="page 6")

    row = run_row(url, handle)
    assert (row.status, row.current_step_index, float(row.progress_pct)) == ("running", 1, 60)
    assert task_states(url, handle) == [("succeeded", 1), ("running", 1), ("planned", 0)]
    assert log(url, handle) == ["queued", "claimed", "succeeded", "queued", "claimed"]
    ((worker_id, note),) = [
        tuple(r)
        for r in rows(
            url,
            "SELECT worker_id, progress_note FROM taskline_api.task WHERE run_id = :id AND step_index = 1",
            id=handle.run_id,
        )
    ]
    assert (worker_id, note) == (handle.worker_id, "page 6")


def test_a_run_with_a_cancel_waiting_for_a_running_task(url):
    handle = add_run(url, state="cancel_requested")

    assert run_row(url, handle).cancel_requested is True
    assert task_states(url, handle) == [("running", 1), ("cancelled", 0), ("cancelled", 0)]
    assert "cancel_requested" in log(url, handle)


def test_a_retrying_task_is_due_later_and_shows_its_error(url):
    handle = add_run(url, state="retrying")

    assert task_states(url, handle) == [("retrying", 1), ("planned", 0), ("planned", 0)]
    assert run_row(url, handle).error_type == "ValueError"
    assert rows(
        url,
        "SELECT next_retry_at > now() FROM taskline_api.task WHERE run_id = :id AND step_index = 0",
        id=handle.run_id,
    )[0][0]
    assert log(url, handle) == ["queued", "claimed", "retry_scheduled"]


def test_a_step_on_its_second_attempt_has_the_retry_in_its_log(url):
    running = add_run(url, state="running", attempts=2)
    queued = add_run(url, state="queued", attempts=2)

    assert task_states(url, running)[0] == ("running", 2)
    assert log(url, running) == ["queued", "claimed", "retry_scheduled", "requeued", "claimed"]
    assert task_states(url, queued)[0] == ("queued", 1)
    assert log(url, queued) == ["queued", "claimed", "retry_scheduled", "requeued"]


def test_a_succeeded_run_and_one_that_succeeded_after_a_retry(url):
    plain = add_run(url, state="succeeded")
    retried = add_run(url, state="succeeded", at_step=1, attempts=2)

    row = run_row(url, plain)
    assert (row.status, row.current_step_index, row.attempts_total, row.finished) == (
        "succeeded", None, 3, True,
    )  # fmt: skip
    assert task_states(url, plain) == [("succeeded", 1)] * 3
    assert task_states(url, retried) == [("succeeded", 1), ("succeeded", 2), ("succeeded", 1)]
    assert run_row(url, retried).attempts_total == 4


def test_a_failed_run_has_the_error_and_its_downstream_steps(url):
    handle = add_run(
        url, state="failed", at_step=1, error_type="KeyError", error_detail="no such key\nsecond line"
    )

    assert task_states(url, handle) == [("succeeded", 1), ("failed", 1), ("upstream_failed", 0)]
    row = run_row(url, handle)
    assert (row.status, row.current_step_index, row.error_type, row.finished) == (
        "failed", 1, "KeyError", True,
    )  # fmt: skip
    assert rows(
        url, "SELECT error_summary FROM taskline_api.run WHERE run_id = :id", id=handle.run_id
    )[0][0] == "no such key"
    assert log(url, handle)[-2:] == ["failed", "upstream_failed"]


def test_a_cancelled_run(url):
    handle = add_run(url, state="cancelled", at_step=1)

    assert task_states(url, handle) == [("succeeded", 1), ("cancelled", 1), ("cancelled", 0)]
    row = run_row(url, handle)
    assert (row.status, row.finished) == ("cancelled", True)
    assert log(url, handle)[-2:] == ["cancel_requested", "cancelled"]


def test_steps_can_be_a_count_or_a_list_of_task_names(url):
    named = add_run(url, steps=["fetch", "parse"], queue="parse")

    found = rows(
        url,
        "SELECT task_name, queue FROM taskline_api.task WHERE run_id = :id ORDER BY step_index",
        id=named.run_id,
    )
    assert [tuple(r) for r in found] == [("fetch", "parse"), ("parse", "parse")]
    assert rows(url, "SELECT step_count FROM taskline_api.run WHERE run_id = :id", id=named.run_id)[0][0] == 2
    assert len(add_run(url, steps=5).task_ids) == 5


def test_trigger_person_correlation_producer_job_and_parent_pass_through(url):
    parent = add_run(url, state="running", steps=2)
    child = add_run(
        url,
        state="queued",
        job_name="child_job",
        trigger="manual",
        triggered_by="lattice:user:42",
        correlation_id="batch-7",
        producer="lattice.taskline_ui",
        parent=parent,
        parent_step=1,
    )

    (row,) = rows(
        url,
        "SELECT job_name, trigger, triggered_by, correlation_id, producer, parent_run_id "
        "FROM taskline_api.run WHERE run_id = :id",
        id=child.run_id,
    )
    assert tuple(row) == (
        "child_job", "manual", "lattice:user:42", "batch-7", "lattice.taskline_ui", parent.run_id,
    )  # fmt: skip


def test_references_land_with_their_step_and_unknown_kinds_are_registered(url):
    register_producers(url, [Kind("test.producer", "A name chosen earlier")])
    handle = add_run(
        url,
        references=[
            ReferencePlan("strategiq.document", "doc-1", "subject", label="Doc 1"),
            ReferencePlan("source", "src-9", "produced", step_index=1),
        ],
        producer="test.producer",
    )

    found = rows(
        url,
        "SELECT kind, external_id, relation, label, step_index FROM taskline_api.reference "
        "WHERE run_id = :id ORDER BY external_id",
        id=handle.run_id,
    )
    assert [tuple(r) for r in found] == [
        ("strategiq.document", "doc-1", "subject", "Doc 1", None),
        ("source", "src-9", "produced", None, 1),
    ]
    assert {r[0] for r in rows(url, "SELECT code FROM jobs.reference_kind")} == {
        "strategiq.document",
        "source",
    }
    assert rows(url, "SELECT name FROM jobs.producer_kind")[0][0] == "A name chosen earlier"


def test_age_moves_a_run_into_the_past_but_not_a_running_tasks_heartbeat(url):
    recent = add_run(url, state="running")
    old = add_run(
        url,
        state="running",
        age=timedelta(hours=1),
        references=[ReferencePlan("document", "d-1", "subject")],
    )

    (row,) = rows(
        url,
        "SELECT created_at < now() - interval '59 minutes', started_at < now() - interval '59 minutes' "
        "FROM taskline_api.run WHERE run_id = :id",
        id=old.run_id,
    )
    assert tuple(row) == (True, True)
    assert rows(
        url,
        "SELECT max(occurred_at) < now() - interval '59 minutes' FROM taskline_api.event WHERE run_id = :id",
        id=old.run_id,
    )[0][0]
    assert rows(
        url,
        "SELECT created_at < now() - interval '59 minutes' FROM taskline_api.reference WHERE run_id = :id",
        id=old.run_id,
    )[0][0]
    assert rows(
        url,
        "SELECT heartbeat_at > now() - interval '1 minute' FROM taskline_api.task "
        "WHERE run_id = :id AND step_index = 0",
        id=old.run_id,
    )[0][0]
    newest_first = rows(url, "SELECT run_id FROM taskline_api.run ORDER BY created_at DESC")
    assert [r[0] for r in newest_first] == [recent.run_id, old.run_id]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"state": "paused"},
        {"steps": []},
        {"steps": 0},
        {"at_step": 3},
        {"at_step": -1},
        {"attempts": 0},
        {"attempts": 4},
        {"max_attempts": 2, "attempts": 3},
    ],
)
def test_bad_arguments_raise_and_write_nothing(url, kwargs):
    with pytest.raises(ValueError):
        add_run(url, **kwargs)

    assert rows(url, "SELECT count(*) FROM jobs.job_run")[0][0] == 0
    assert rows(url, "SELECT count(*) FROM jobs.producer_kind")[0][0] == 0


def test_a_parent_step_outside_the_parent_is_refused(url):
    parent = add_run(url, steps=2)

    with pytest.raises(ValueError, match="parent_step"):
        add_run(url, parent=parent, parent_step=2)


def test_workers_are_reported_by_status_and_role(url):
    alive = add_worker(url, queue="parse")
    stopped = add_worker(url, queue="parse", status="stopped")
    dead = add_worker(url, queue="parse", status="dead")
    reaper = add_worker(url, reaper=True)
    named = add_worker(url, worker_id="host-9:77:zz", queue="fetch")

    found = {
        r.worker_id: r
        for r in rows(url, "SELECT worker_id, role, queue, status, host, pid FROM taskline_api.worker")
    }
    assert (found[alive].role, found[alive].queue, found[alive].status) == ("worker", "parse", "alive")
    assert found[stopped].status == "stopped"
    assert found[dead].status == "dead"
    assert (found[reaper].role, found[reaper].queue, found[reaper].status) == ("reaper", None, "alive")
    assert (found[named].host, found[named].pid, found[named].queue) == ("host-9", 77, "fetch")
    with pytest.raises(ValueError, match="worker status"):
        add_worker(url, status="sleeping")


def test_clear_removes_runs_and_workers_but_keeps_kinds_and_the_instance(url):
    add_run(url, state="failed", references=[ReferencePlan("document", "d-1", "subject")])
    add_worker(url)

    clear(url)

    counts = rows(
        url,
        "SELECT (SELECT count(*) FROM jobs.job_run), (SELECT count(*) FROM jobs.task_instance), "
        "(SELECT count(*) FROM jobs.task_event), (SELECT count(*) FROM jobs.job_reference), "
        "(SELECT count(*) FROM jobs.worker), (SELECT count(*) FROM jobs.producer_kind), "
        "(SELECT count(*) FROM jobs.reference_kind), (SELECT count(*) FROM jobs.instance)",
    )[0]
    assert tuple(counts) == (0, 0, 0, 0, 0, 1, 1, 1)
    add_run(url, state="queued")
    assert rows(url, "SELECT min(event_id) FROM taskline_api.event")[0][0] == 1


def test_an_engine_is_accepted_and_left_open(url):
    engine = create_engine(url)
    try:
        handle = add_run(engine, state="failed")
        add_worker(engine)
        clear(engine)
        with engine.connect() as conn:  # still usable: the helpers did not close it
            assert conn.execute(text("SELECT count(*) FROM jobs.job_run")).scalar_one() == 0
        assert handle.run_id
    finally:
        engine.dispose()


async def test_the_helpers_refuse_to_run_inside_an_event_loop_and_write_nothing(url):
    with pytest.raises(RuntimeError, match="synchronous"):
        add_run(url)
    with pytest.raises(RuntimeError, match="synchronous"):
        add_worker(url)
    with pytest.raises(RuntimeError, match="synchronous"):
        clear(url)

    assert rows(url, "SELECT count(*) FROM jobs.producer_kind")[0][0] == 0


async def test_a_failed_run_written_by_the_helper_can_be_resumed_by_the_real_store(url):
    handle = await asyncio.to_thread(add_run, url, state="failed", at_step=1)
    store = Store(url)
    try:
        result = await store.retry_run(handle.run_id)
    finally:
        await store.close()

    assert result.outcome == "retried"
    assert len(result.resend) == 1


async def test_the_message_of_a_helper_made_run_can_really_be_sent(runtime, url):
    """The stored message is what the middleware stores, so retry and requeue can send it to Valkey."""
    failed = await asyncio.to_thread(add_run, url, state="failed", at_step=1, queue="work")
    queued = await asyncio.to_thread(add_run, url, state="queued", queue="work")

    resumed = await retry_run(failed.run_id, actor="test")
    resent = await requeue("work", actor="test")

    assert (resumed.ok, resumed.resent, resumed.unsent) == (True, 1, ())
    assert (resent.ok, resent.resent, resent.unsent) == (True, 2, ())  # the resumed task, and the queued one
    sent = {uuid.UUID(t) for t in stream_task_ids(runtime)}
    assert sent == {failed.task_ids[1], queued.task_ids[0]}
