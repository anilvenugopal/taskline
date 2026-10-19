import dataclasses
import uuid

import pytest
from helpers import make_plan, rows

from taskline.queries import REAPER_QUEUE, Queries
from taskline.store import ReferencePlan


@pytest.fixture
async def queries(store, empty_database_url):
    instance = Queries(empty_database_url)
    yield instance
    await instance.close()


async def add_task(store, url: str, queue: str, status: str) -> uuid.UUID:
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    rows(
        url,
        "UPDATE jobs.task_instance SET task_status_code = :status, queue_name = :queue, "
        "queued_at = now() WHERE task_instance_id = :id RETURNING 1",
        status=status,
        queue=queue,
        id=task_id,
    )
    return task_id


async def test_queue_counts_split_waiting_running_and_retrying_per_queue(
    store, queries, empty_database_url
):
    for status in ("queued", "queued", "running", "retrying"):
        await add_task(store, empty_database_url, "parse", status)
    await add_task(store, empty_database_url, "fetch", "queued")
    await add_task(store, empty_database_url, "fetch", "succeeded")

    counts = {c.queue_name: c for c in await queries.queue_counts()}

    assert (counts["parse"].queued, counts["parse"].running, counts["parse"].retrying) == (2, 1, 1)
    assert (counts["fetch"].queued, counts["fetch"].running, counts["fetch"].retrying) == (1, 0, 0)


async def test_the_age_of_the_oldest_waiting_task_is_reported(store, queries, empty_database_url):
    old = await add_task(store, empty_database_url, "parse", "queued")
    await add_task(store, empty_database_url, "parse", "queued")
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET queued_at = now() - interval '2 hours' "
        "WHERE task_instance_id = :id RETURNING 1",
        id=old,
    )

    (counts,) = await queries.queue_counts()

    assert 7100 < counts.oldest_queued_seconds < 7300


async def test_a_queue_with_no_waiting_task_has_no_oldest_age(store, queries, empty_database_url):
    await add_task(store, empty_database_url, "parse", "running")

    (counts,) = await queries.queue_counts()

    assert counts.oldest_queued_seconds is None


async def test_live_workers_are_counted_per_queue_and_a_reaper_is_left_out(
    store, queries, empty_database_url
):
    await store.register_worker("a", "parse")
    await store.register_worker("b", "parse")
    await store.register_worker("c", "ocr")
    await store.register_worker("gone", "parse")
    await store.stop_worker("gone")
    await store.register_worker("the-reaper", REAPER_QUEUE)

    counts = {c.queue_name: c for c in await queries.queue_counts()}

    assert set(counts) == {"parse", "ocr"}
    assert counts["parse"].live_workers == 2
    assert counts["ocr"].live_workers == 1
    assert counts["ocr"].queued == 0


async def test_task_info_gives_the_details_a_hook_needs(store, queries):
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    second = plan.steps[1].task_instance_id

    info = await queries.task_info(second)

    assert info.task_instance_id == second
    assert info.job_run_id == plan.job_run_id
    assert (info.step_index, info.step_count, info.task_name) == (1, 2, "step_1")
    assert (info.correlation_id, info.producer_code) == ("upload-42", "test.producer")
    assert await queries.task_info(uuid.uuid7()) is None


async def test_task_error_gives_the_recorded_error(store, queries, empty_database_url):
    task_id = await add_task(store, empty_database_url, "parse", "failed")
    rows(
        empty_database_url,
        "UPDATE jobs.task_instance SET error_type = 'ValueError', error_detail = 'bad' "
        "WHERE task_instance_id = :id RETURNING 1",
        id=task_id,
    )

    assert await queries.task_error(task_id) == ("ValueError", "bad")
    assert await queries.task_error(uuid.uuid7()) == (None, None)


async def test_live_reapers_counts_only_recent_alive_reapers(store, queries, empty_database_url):
    await store.register_worker("recent", REAPER_QUEUE)
    await store.register_worker("stale", REAPER_QUEUE)
    await store.register_worker("a-worker", "parse")
    rows(
        empty_database_url,
        "UPDATE jobs.worker SET last_seen_at = now() - interval '1 hour' "
        "WHERE worker_id = 'stale' RETURNING 1",
    )

    assert await queries.live_reapers(within_seconds=60) == 1


async def add_run(store, url: str, **changes):
    plan = dataclasses.replace(make_plan(), **changes)
    await store.register_run(plan)
    return plan


async def test_runs_are_listed_newest_first_and_limited(store, queries):
    first = await add_run(store, "", job_name="first")
    second = await add_run(store, "", job_name="second")
    third = await add_run(store, "", job_name="third")

    listed = await queries.list_runs(limit=2)

    assert [run.job_run_id for run in listed] == [third.job_run_id, second.job_run_id]
    assert first.job_run_id not in [run.job_run_id for run in listed]


async def test_runs_can_be_filtered_by_status_producer_job_and_correlation(
    store, queries, empty_database_url
):
    rows(
        empty_database_url,
        "INSERT INTO jobs.producer_kind (code, name) VALUES ('other.producer', 'Other') RETURNING 1",
    )
    await add_run(store, "", job_name="ingest", correlation_id="c1")
    other = await add_run(store, "", job_name="export", correlation_id="c2", producer_code="other.producer")
    rows(
        empty_database_url,
        "UPDATE jobs.job_run SET job_run_status_code = 'failed' WHERE job_run_id = :id RETURNING 1",
        id=other.job_run_id,
    )

    assert [r.job_name for r in await queries.list_runs(status="failed")] == ["export"]
    assert [r.job_name for r in await queries.list_runs(producer="other.producer")] == ["export"]
    assert [r.job_name for r in await queries.list_runs(job_name="ingest")] == ["ingest"]
    assert [r.job_name for r in await queries.list_runs(correlation_id="c2")] == ["export"]
    assert await queries.list_runs(status="failed", job_name="ingest") == []
    assert len(await queries.list_runs()) == 2


async def test_runs_can_be_found_through_the_object_they_refer_to(store, queries):
    await add_run(store, "", job_name="about_doc_1", references=[ReferencePlan("document", "doc-1", "subject")])
    await add_run(store, "", job_name="about_doc_2", references=[ReferencePlan("document", "doc-2", "subject")])
    await add_run(store, "", job_name="about_nothing")

    found = await queries.list_runs(reference=("document", "doc-1"))

    assert [run.job_name for run in found] == ["about_doc_1"]
    assert await queries.list_runs(reference=("source", "doc-1")) == []


async def test_a_run_is_shown_with_its_tasks_references_and_optionally_its_events(
    store, queries
):
    plan = await add_run(
        store,
        "",
        references=[
            ReferencePlan("document", "doc-1", "subject", label="Zurich 10-K"),
            ReferencePlan("source", "src-9", "produced", step_index=1),
        ],
        steps=make_plan(step_count=2).steps,
    )
    await store.mark_queued(plan.steps[0].task_instance_id, {"task_name": "step_0"})

    detail = await queries.get_run(plan.job_run_id)

    assert detail.run.job_name == "fetch_source"
    assert [(t.step_index, t.task_name, t.status) for t in detail.tasks] == [
        (0, "step_0", "queued"),
        (1, "step_1", "planned"),
    ]
    assert {(r.external_id, r.step_index) for r in detail.references} == {("doc-1", None), ("src-9", 1)}
    assert detail.events == []

    with_events = await queries.get_run(plan.job_run_id, include_events=True)

    assert [(e.event, e.task_name) for e in with_events.events] == [("queued", "step_0")]


async def test_an_unknown_run_is_none(queries):
    assert await queries.get_run(uuid.uuid7()) is None
