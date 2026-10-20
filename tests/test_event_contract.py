"""The meaning of each event is written in docs/public_contract.md ("Events", "Event sequences").
This test reads those tables and fails when the log Taskline writes no longer matches them, so a
reader such as Taskline UI's run timeline can rely on the document instead of on the code."""

import asyncio

from helpers import failed_run, make_plan, queued_task, rows, set_task
from test_public_contract import CONTRACT, read_sections

from taskline.testing import add_run


def documented() -> tuple[list[str], dict[str, str]]:
    sections = read_sections(CONTRACT.read_text())
    events = [row["event"] for row in sections["Events"]]
    cases = {row["case"]: row["events"] for row in sections["Event sequences"]}
    return events, cases


def log(url: str, task_id) -> str:
    result = rows(
        url,
        "SELECT task_event_code, attempt FROM jobs.task_event WHERE task_instance_id = :id "
        "ORDER BY occurred_at, task_event_id",
        id=task_id,
    )
    return ", ".join(f"{r.task_event_code}/{'-' if r.attempt is None else r.attempt}" for r in result)


def made_by_add_run(url: str) -> dict[str, str]:
    """Cases written by Taskline's own test helper, which drives the Store through its transitions."""
    succeeded = add_run(url, state="succeeded", steps=1)
    pending = add_run(url, state="retrying", attempts=2, steps=1)
    failed = add_run(url, state="failed", attempts=3, steps=2)
    asked = add_run(url, state="cancel_requested", steps=3)
    return {
        "succeeds first time": log(url, succeeded.task_ids[0]),
        "fails twice, retry pending": log(url, pending.task_ids[0]),
        "fails on its last attempt": log(url, failed.task_ids[0]),
        "a later step after a failure": log(url, failed.task_ids[1]),
        "running task asked to stop": log(url, asked.task_ids[0]),
        "step cancelled before it started": log(url, asked.task_ids[1]),
    }


async def test_the_log_matches_the_documented_events_and_sequences(store, empty_database_url):
    url = empty_database_url
    found = await asyncio.to_thread(made_by_add_run, url)   # the helper refuses to run on a loop

    # a worker disappears: its lease ran out a minute ago and the reaper takes the task back
    await store.register_worker("worker-1", "default")
    lost = await queued_task(store, url)
    await store.claim(lost, "worker-1", lease_seconds=30)
    rows(url, "UPDATE jobs.task_instance SET lease_expires_at = now() - interval '1 minute' "
              "WHERE task_instance_id = :id RETURNING 1", id=lost)
    await store.reap_expired_leases(limit=10)
    found["worker disappears"] = log(url, lost)

    # an operator retries a failed run: what the retry adds to the failed step's log
    plan = await failed_run(store, url)
    await store.retry_run(plan.job_run_id, actor="lattice:user:1")
    found["operator retries a failed run"] = log(url, plan.steps[1].task_instance_id)

    # an operator sends a queue's tasks again
    queued = await queued_task(store, url)
    await store.take_queued("default", None, actor="lattice:user:2")
    found["operator sends a queue again"] = log(url, queued)

    # a step whose previous step succeeded long ago but never sent it
    handoff = make_plan(step_count=2)
    await store.register_run(handoff)
    set_task(url, handoff.steps[0].task_instance_id, task_status_code="succeeded", queue_name="work")
    rows(url, "UPDATE jobs.task_instance SET finished_at = now() - interval '1 hour' "
              "WHERE task_instance_id = :id RETURNING 1", id=handoff.steps[0].task_instance_id)
    await store.fail_lost_handoffs(stall_seconds=60, limit=10)
    found["step never handed over"] = log(url, handoff.steps[1].task_instance_id)

    # a second delivery of a message the worker already holds
    held = await queued_task(store, url)
    await store.claim(held, "worker-1", lease_seconds=30)
    await store.claim(held, "worker-1", lease_seconds=30)
    found["duplicate delivery"] = log(url, held)

    _, cases = documented()
    assert found == cases


def test_every_event_in_the_status_values_is_described_once_and_nothing_else_is():
    events, _ = documented()
    (row,) = [r for r in read_sections(CONTRACT.read_text())["Status values"] if r["set"] == "event"]
    assert sorted(events) == sorted(row["values"].split(", "))
