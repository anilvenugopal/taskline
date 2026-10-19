import json
import uuid

from helpers import make_plan, taskline

from taskline.store import ReferencePlan


async def test_runs_lists_a_registered_run_as_text_and_as_json(runtime, store):
    plan = make_plan()
    await store.register_run(plan)

    text_result = taskline("runs")
    json_result = taskline("runs", "--json")

    assert text_result.returncode == 0
    assert str(plan.job_run_id) in text_result.stdout
    assert [run["job_run_id"] for run in json.loads(json_result.stdout)] == [str(plan.job_run_id)]


async def test_runs_with_no_match_says_so(runtime, store):
    result = taskline("runs", "--status", "failed")

    assert result.returncode == 0
    assert result.stdout.strip() == "no runs"


async def test_runs_can_be_narrowed_by_reference(runtime, store):
    plan = make_plan(references=[ReferencePlan("document", "doc-1", "subject")])
    await store.register_run(plan)

    found = taskline("runs", "--reference", "document:doc-1", "--json")
    missing = taskline("runs", "--reference", "document:other", "--json")

    assert [run["job_run_id"] for run in json.loads(found.stdout)] == [str(plan.job_run_id)]
    assert json.loads(missing.stdout) == []


async def test_a_malformed_reference_exits_with_code_2(runtime, store):
    result = taskline("runs", "--reference", "document")

    assert result.returncode == 2
    assert "kind:id" in result.stderr


async def test_show_prints_the_run_its_tasks_and_with_events_the_event_log(runtime, store):
    plan = make_plan(step_count=2)
    await store.register_run(plan)
    await store.mark_queued(plan.steps[0].task_instance_id, {"task_name": "step_0"})

    plain = taskline("show", str(plan.job_run_id))
    with_events = taskline("show", str(plan.job_run_id), "--events")
    as_json = taskline("show", str(plan.job_run_id), "--json")

    assert plain.returncode == 0
    assert "step_0" in plain.stdout
    assert "Events:" not in plain.stdout
    assert "Events:" in with_events.stdout
    assert json.loads(as_json.stdout)["run"]["job_run_id"] == str(plan.job_run_id)


async def test_show_of_an_unknown_run_exits_with_code_1(runtime, store):
    result = taskline("show", str(uuid.uuid7()))

    assert result.returncode == 1
    assert "no such run" in result.stderr


async def test_show_of_something_that_is_not_a_run_id_exits_with_code_2(runtime, store):
    result = taskline("show", "01900000")

    assert result.returncode == 2
    assert "full id" in result.stderr


async def test_queues_reports_load_and_a_missing_reaper(runtime, store):
    plan = make_plan()
    await store.register_run(plan)
    await store.mark_queued(plan.steps[0].task_instance_id, {"task_name": "step_0"})

    text_result = taskline("queues")
    json_result = taskline("queues", "--json")

    assert text_result.returncode == 0
    assert "reaper: NONE ALIVE" in text_result.stdout
    data = json.loads(json_result.stdout)
    assert data["live_reapers"] == 0
    assert [(q["queue_name"], q["queued"]) for q in data["queues"]] == [("default", 1)]
