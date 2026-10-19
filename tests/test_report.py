import json
import uuid
from datetime import UTC, datetime

from taskline.queries import (
    EventRow,
    QueueCounts,
    ReferenceRow,
    RunDetail,
    RunSummary,
    TaskRow,
)
from taskline.report import (
    as_json,
    format_age,
    format_queues,
    format_run,
    format_runs,
    format_table,
    format_time,
)

RUN_ID = uuid.UUID("01900000-0000-7000-8000-000000000001")
WHEN = datetime(2026, 3, 1, 12, 30, 5, tzinfo=UTC)


def summary(**changes) -> RunSummary:
    values = {
        "job_run_id": RUN_ID,
        "job_name": "fetch_source",
        "status": "failed",
        "producer_code": "test.producer",
        "correlation_id": "upload-42",
        "triggered_by": None,
        "step_count": 2,
        "created_at": WHEN,
        "finished_at": None,
    }
    return RunSummary(**{**values, **changes})


def task(**changes) -> TaskRow:
    values = {
        "task_instance_id": uuid.uuid7(),
        "step_index": 0,
        "task_name": "fetch",
        "queue_name": "default",
        "status": "succeeded",
        "attempt": 1,
        "max_attempts": 3,
        "worker_id": "w1",
        "started_at": WHEN,
        "finished_at": WHEN,
        "progress_pct": None,
        "progress_note": None,
        "error_type": None,
        "error_detail": None,
    }
    return TaskRow(**{**values, **changes})


def test_ages_are_shown_in_the_two_largest_units():
    assert format_age(None) == "-"
    assert format_age(42.9) == "42s"
    assert format_age(125) == "2m 5s"
    assert format_age(7260) == "2h 1m"
    assert format_age(2 * 86400 + 3 * 3600) == "2d 3h"


def test_times_are_shown_in_utc_and_a_missing_time_is_a_dash():
    assert format_time(WHEN) == "2026-03-01 12:30:05"
    assert format_time(None) == "-"


def test_a_table_pads_each_column_to_its_widest_cell():
    text = format_table(["A", "BB"], [["xxx", 1], ["y", 22]])

    assert text.splitlines() == ["A    BB", "xxx  1", "y    22"]


def test_a_list_of_runs_has_one_line_per_run_and_says_so_when_empty():
    text = format_runs([summary(), summary(job_name="export", correlation_id=None)])

    lines = text.splitlines()
    assert lines[0].split() == ["RUN", "JOB", "STATUS", "PRODUCER", "CORRELATION", "STARTED", "ENDED"]
    assert str(RUN_ID) in lines[1]
    assert "upload-42" in lines[1]
    assert "export" in lines[2]
    assert format_runs([]) == "no runs"


def test_a_run_shows_its_tasks_with_errors_and_progress_and_its_references():
    detail = RunDetail(
        run=summary(),
        tasks=[
            task(status="failed", attempt=3, error_type="ValueError", error_detail="bad page"),
            task(step_index=1, task_name="parse", status="running", progress_pct=40, progress_note="page 4"),
        ],
        references=[
            ReferenceRow("document", "doc-1", "subject", "Zurich 10-K", None, None),
            ReferenceRow("source", "src-9", "produced", None, None, 1),
        ],
        events=[],
    )

    text = format_run(detail)

    assert text.startswith(f"Run {RUN_ID}  fetch_source  failed")
    assert "3/3" in text
    assert "ValueError: bad page" in text
    assert "40% page 4" in text
    assert "subject  document  doc-1  Zurich 10-K  (run)" in text
    assert "produced  source  src-9  (step 2)" in text
    assert "Events:" not in text


def test_events_are_shown_only_when_the_run_carries_them():
    event = EventRow(0, "fetch", "queued", None, None, {"queue": "default"}, WHEN)
    detail = RunDetail(run=summary(), tasks=[task()], references=[], events=[event])

    text = format_run(detail)

    assert "Events:" in text
    assert '{"queue": "default"}' in text


def test_queues_show_their_load_and_whether_a_reaper_is_alive():
    counts = [QueueCounts("parse", 2, 1, 0, 125.0, 3), QueueCounts("ocr", 0, 0, 0, None, 1)]

    with_reaper = format_queues(counts, 2)
    without_reaper = format_queues(counts, 0)

    assert "parse" in with_reaper
    assert "2m 5s" in with_reaper
    assert with_reaper.splitlines()[-1] == "reaper: alive (2)"
    assert without_reaper.splitlines()[-1] == "reaper: NONE ALIVE"
    assert format_queues([], 0).startswith("no queues")


def test_json_output_turns_dataclasses_ids_and_times_into_plain_values():
    detail = RunDetail(run=summary(), tasks=[task()], references=[], events=[])

    data = json.loads(as_json(detail))

    assert data["run"]["job_run_id"] == str(RUN_ID)
    assert data["run"]["created_at"] == str(WHEN)
    assert data["tasks"][0]["task_name"] == "fetch"
    assert json.loads(as_json({"queues": [QueueCounts("q", 1, 0, 0, None, 0)], "live_reapers": 1})) == {
        "queues": [
            {
                "queue_name": "q",
                "queued": 1,
                "running": 0,
                "retrying": 0,
                "oldest_queued_seconds": None,
                "live_workers": 0,
            }
        ],
        "live_reapers": 1,
    }
