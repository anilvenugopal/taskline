"""The taskline_api views, read the way another program reads them. Each test builds its data with
the real Store operations, so it is what a worker would have written."""

import dataclasses
import uuid

from helpers import make_plan, rows

from taskline.migrate import migrate
from taskline.setup import schema_version
from taskline.store import ReferencePlan, StepPlan

WORKER = "host-1:4242:abc123"

# The leading columns of an index for each filter the contract promises to keep fast.
FILTER_INDEXES = {
    "job_run": [
        ["job_run_status_code", "created_at"],
        ["created_at"],
        ["job_run_status_code", "finished_at"],
        ["job_name", "created_at"],
        ["producer_code", "created_at"],
        ["correlation_id"],
        ["schedule_id", "created_at"],
    ],
    "job_reference": [["reference_kind_code", "external_id", "job_run_id"]],
    "task_instance": [["worker_id"], ["job_run_id"]],
}


async def start(store, plan, step_index):
    """Queue and claim one step of a registered run; returns its task id and attempt."""
    task_id = plan.steps[step_index].task_instance_id
    await store.mark_queued(task_id, {"labels": {"max_attempts": "3"}})
    claim = await store.claim(task_id, WORKER, 60)
    return task_id, claim.attempt


async def failed_run(store):
    """A three-step run whose first step succeeded and whose second failed for good."""
    await store.register_worker(WORKER, "default")
    plan = make_plan(step_count=3)
    await store.register_run(plan)
    task_id, attempt = await start(store, plan, 0)
    await store.finish_succeeded(task_id, attempt)
    task_id, attempt = await start(store, plan, 1)
    await store.finish_failed(task_id, attempt, "ValueError", "boom\nsecond line", "the traceback")
    await store.fail_downstream(plan.job_run_id, 1)
    await store.refresh_run_status(plan.job_run_id)
    return plan


async def test_a_failed_run_shows_its_failed_step_and_the_first_line_of_its_error(
    store, empty_database_url
):
    plan = await failed_run(store)

    (row,) = rows(
        empty_database_url,
        "SELECT status, trigger, producer, job_name, step_count, current_step_index, "
        "current_step_name, error_type, error_summary, attempts_total, cancel_requested, "
        "finished_at IS NOT NULL AS finished, duration_seconds >= 0 AS timed "
        "FROM taskline_api.run WHERE run_id = :id",
        id=plan.job_run_id,
    )

    assert tuple(row) == (
        "failed", "api", "test.producer", "fetch_source", 3, 1,
        "step_1", "ValueError", "boom", 2, False, True, True,
    )  # fmt: skip


async def test_a_running_run_shows_progress_and_a_pending_cancel(store, empty_database_url):
    await store.register_worker(WORKER, "default")
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    task_id, attempt = await start(store, plan, 0)
    await store.heartbeat(task_id, attempt, 60, progress_pct=40, progress_note="page 4")
    await store.cancel_run(plan.job_run_id)

    (row,) = rows(
        empty_database_url,
        "SELECT status, current_step_index, progress_pct, cancel_requested, finished_at, "
        "duration_seconds FROM taskline_api.run WHERE run_id = :id",
        id=plan.job_run_id,
    )

    assert row.status == "running"
    assert row.current_step_index == 0
    assert float(row.progress_pct) == 40
    assert row.cancel_requested is True
    assert row.finished_at is None
    assert row.duration_seconds is None


async def test_a_run_started_by_a_task_shows_its_parent_run_and_its_trigger(
    store, empty_database_url
):
    parent = make_plan(step_count=1)
    await store.register_run(parent)
    child = dataclasses.replace(
        make_plan(step_count=1),
        job_run_id=uuid.uuid7(),
        steps=[StepPlan(uuid.uuid7(), 0, "child", "default")],
        parent_task_instance_id=parent.steps[0].task_instance_id,
        job_trigger_code="manual",
    )
    await store.register_run(child)

    sql = "SELECT trigger, parent_run_id FROM taskline_api.run WHERE run_id = :id"
    (child_row,) = rows(empty_database_url, sql, id=child.job_run_id)
    (parent_row,) = rows(empty_database_url, sql, id=parent.job_run_id)

    assert tuple(child_row) == ("manual", parent.job_run_id)
    assert tuple(parent_row) == ("api", None)


async def test_the_task_view_names_what_the_tables_hold(store, empty_database_url):
    plan = await failed_run(store)

    found = rows(
        empty_database_url,
        "SELECT step_index, task_name, queue, status, attempt, max_attempts, worker_id, "
        "error_type, error_detail, error_traceback FROM taskline_api.task "
        "WHERE run_id = :id ORDER BY step_index",
        id=plan.job_run_id,
    )

    assert [tuple(r) for r in found] == [
        (0, "step_0", "default", "succeeded", 1, 3, WORKER, None, None, None),
        (1, "step_1", "default", "failed", 1, 3, WORKER, "ValueError", "boom\nsecond line", "the traceback"),
        (2, "step_2", "default", "upstream_failed", 0, 3, None, None, None, None),
    ]  # fmt: skip


async def test_the_reference_view_has_no_step_for_a_reference_to_the_whole_run(
    store, empty_database_url
):
    plan = make_plan(
        step_count=2,
        references=[
            ReferencePlan("document", "doc-1", "subject", label="Doc 1"),
            ReferencePlan("source", "src-9", "produced", step_index=1),
        ],
    )
    await store.register_run(plan)

    found = rows(
        empty_database_url,
        "SELECT kind, external_id, relation, label, step_index FROM taskline_api.reference "
        "WHERE run_id = :id ORDER BY external_id",
        id=plan.job_run_id,
    )

    assert [tuple(r) for r in found] == [
        ("document", "doc-1", "subject", "Doc 1", None),
        ("source", "src-9", "produced", None, 1),
    ]


async def test_the_event_view_carries_the_run_the_step_and_the_task(store, empty_database_url):
    plan = await failed_run(store)

    found = rows(
        empty_database_url,
        "SELECT event, step_index, task_name FROM taskline_api.event "
        "WHERE run_id = :id ORDER BY event_id",
        id=plan.job_run_id,
    )
    events = [tuple(r) for r in found]

    assert events[0] == ("queued", 0, "step_0")
    assert ("claimed", 0, "step_0") in events
    assert ("failed", 1, "step_1") in events


async def test_the_queue_view_counts_waiting_tasks_and_live_workers_but_not_the_reaper(
    store, empty_database_url
):
    await store.register_worker("host-1:1:aa", "default")
    await store.register_worker("reaper:host-1:2:bb", "reaper")
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    await store.mark_queued(plan.steps[0].task_instance_id, {})

    found = rows(
        empty_database_url,
        "SELECT queue, queued, running, retrying, oldest_queued_seconds IS NOT NULL AS has_age, "
        "live_workers FROM taskline_api.queue ORDER BY queue",
    )

    assert [tuple(r) for r in found] == [("default", 1, 0, 0, True, 1)]


async def test_the_worker_view_reads_host_and_pid_from_worker_ids(store, empty_database_url):
    await store.register_worker(WORKER, "default")
    await store.register_worker("reaper:host-2:99:def456", "reaper")
    await store.register_worker("odd-id", "default")
    plan = make_plan(step_count=1)
    await store.register_run(plan)
    task_id, _ = await start(store, plan, 0)

    found = {
        r.worker_id: r
        for r in rows(
            empty_database_url,
            "SELECT worker_id, role, queue, host, pid, status, running_tasks, current_task_id "
            "FROM taskline_api.worker",
        )
    }

    worker = found[WORKER]
    assert (worker.role, worker.queue, worker.host, worker.pid, worker.status) == (
        "worker", "default", "host-1", 4242, "alive",
    )  # fmt: skip
    assert (worker.running_tasks, worker.current_task_id) == (1, task_id)
    reaper = found["reaper:host-2:99:def456"]
    assert (reaper.role, reaper.queue, reaper.host, reaper.pid) == ("reaper", None, "host-2", 99)
    assert (reaper.running_tasks, reaper.current_task_id) == (0, None)
    odd = found["odd-id"]  # an id that does not fit gives an empty pid, not an error for everyone
    assert (odd.host, odd.pid) == ("odd-id", None)


def test_the_instance_view_agrees_with_schema_version(empty_database_url):
    migrate(empty_database_url, instance_name="ops")

    (row,) = rows(
        empty_database_url,
        "SELECT name, taskline_version, schema_version FROM taskline_api.instance",
    )
    found = schema_version(empty_database_url)

    assert tuple(row) == (found.instance_name, found.taskline_version, found.schema_version)
    assert found.schema_version == "0003_api_views"


def index_columns(url, table):
    """The columns of every index on a jobs table, each as a list in index order."""
    found = rows(
        url,
        "SELECT array_agg(a.attname::text ORDER BY k.ord) FROM pg_index i "
        "JOIN pg_class t ON t.oid = i.indrelid "
        "JOIN pg_namespace n ON n.oid = t.relnamespace "
        "CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum "
        "WHERE n.nspname = 'jobs' AND t.relname = :table GROUP BY i.indexrelid",
        table=table,
    )
    return [list(r[0]) for r in found]


def test_every_filter_the_contract_keeps_fast_has_an_index(empty_database_url):
    migrate(empty_database_url)

    for table, wanted in FILTER_INDEXES.items():
        have = index_columns(empty_database_url, table)
        for columns in wanted:
            assert any(h[: len(columns)] == columns for h in have), (
                f"{table} has no index starting with {columns}"
            )
