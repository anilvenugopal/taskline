import asyncio

import pytest
from helpers import default_worker, rows, wait_until
from redis import Redis
from sqlalchemy.exc import IntegrityError

from taskline import Job, Reference, Step, TaskSpec, submit, task
from taskline.config import get_config
from taskline.errors import MissingInputs, MissingReferences, UnknownInputs

FETCH = TaskSpec(name="fetch_source", queue="work", max_attempts=2)
PARSE = TaskSpec(name="parse_pdf", queue="work", requires_references=("document",))


def statuses(url: str) -> list[str]:
    result = rows(url, "SELECT task_status_code FROM jobs.task_instance ORDER BY step_index")
    return [row.task_status_code for row in result]


def all_succeeded(url: str, count: int):
    return lambda: statuses(url) == ["succeeded"] * count


def stream_exists(namespace: str) -> bool:
    client = Redis.from_url(get_config().valkey_url)
    try:
        return bool(client.exists(f"{namespace}:work"))
    finally:
        client.close()


async def test_a_two_task_job_runs_with_named_values_and_is_recorded(
    runtime, store, empty_database_url
):
    received = {}

    @task(FETCH)
    def fetch(url: str) -> str:
        received["url"] = url
        return "/data/doc42.pdf"

    @task(PARSE)
    async def parse(path: str, dpi: int, project_id: int) -> str:
        received["parse"] = (path, dpi, project_id, type(project_id).__name__)
        return "/data/doc42.parsed"

    job = Job(
        "ingest_document",
        inputs=["url", "project_id"],
        steps=[
            Step(FETCH, inputs={"url": "url"}, output="source_path"),
            Step(
                PARSE,
                inputs={"path": "source_path", "project_id": "project_id"},
                arguments={"dpi": 300},
            ),
        ],
    )

    async with default_worker():
        await submit(
            job,
            url="http://host/doc42.pdf",
            project_id="7",
            producer="test.producer",
            correlation_id="upload-42",
            triggered_by="anil",
            references=[Reference("document", "doc-1", "subject", label="Zurich 10-K")],
        )
        await wait_until(all_succeeded(empty_database_url, 2), timeout=20)

    assert received["url"] == "http://host/doc42.pdf"
    assert received["parse"] == ("/data/doc42.pdf", 300, 7, "int")
    (run,) = rows(empty_database_url, "SELECT * FROM jobs.job_run")
    assert run.job_name == "ingest_document"
    assert (run.producer_code, run.correlation_id, run.triggered_by) == (
        "test.producer",
        "upload-42",
        "anil",
    )
    assert run.job_trigger_code == "api"
    assert run.job_run_status_code == "succeeded"
    (reference,) = rows(empty_database_url, "SELECT * FROM jobs.job_reference")
    assert (reference.external_id, reference.label) == ("doc-1", "Zurich 10-K")


async def test_a_single_task_is_submitted_alone_as_a_job_of_one_task(
    runtime, store, empty_database_url
):
    received = {}

    @task(PARSE)
    def parse(path: str) -> str:
        received["path"] = path
        return "done"

    async with default_worker():
        await submit(
            PARSE,
            path="/data/x.pdf",
            producer="test.producer",
            references=[Reference("document", "doc-1", "subject")],
        )
        await wait_until(all_succeeded(empty_database_url, 1), timeout=20)

    assert received == {"path": "/data/x.pdf"}
    (run,) = rows(empty_database_url, "SELECT job_name, step_count FROM jobs.job_run")
    assert (run.job_name, run.step_count) == ("parse_pdf", 1)


async def test_submit_returns_the_id_of_the_job_run(runtime, store, empty_database_url):
    @task(FETCH)
    def fetch(url: str) -> str:
        return url

    async with default_worker():
        run_id = await submit(FETCH, url="http://x", producer="test.producer")
        await wait_until(all_succeeded(empty_database_url, 1), timeout=20)

    (run,) = rows(empty_database_url, "SELECT job_run_id FROM jobs.job_run")
    assert run.job_run_id == run_id


async def test_missing_inputs_are_refused_and_nothing_is_recorded(runtime, store, empty_database_url):
    job = Job("ingest", inputs=["url", "project_id"], steps=[Step(FETCH, inputs={"url": "url"})])

    with pytest.raises(MissingInputs, match="'project_id'"):
        await submit(job, url="http://x", producer="test.producer")

    assert rows(empty_database_url, "SELECT 1 FROM jobs.job_run") == []


async def test_unknown_inputs_are_refused_and_nothing_is_recorded(runtime, store, empty_database_url):
    job = Job("ingest", inputs=["url"], steps=[Step(FETCH, inputs={"url": "url"})])

    with pytest.raises(UnknownInputs, match="'colour'"):
        await submit(job, url="http://x", colour="red", producer="test.producer")

    assert rows(empty_database_url, "SELECT 1 FROM jobs.job_run") == []


async def test_missing_required_references_are_refused_and_nothing_is_recorded_or_sent(
    runtime, store, empty_database_url
):
    job = Job("ingest", inputs=["path"], steps=[Step(PARSE, inputs={"path": "path"})])

    with pytest.raises(MissingReferences) as error:
        await submit(job, path="/d/x.pdf", producer="test.producer")

    assert "parse_pdf" in str(error.value)
    assert "'document'" in str(error.value)
    assert rows(empty_database_url, "SELECT 1 FROM jobs.job_run") == []
    assert not stream_exists(runtime)


async def test_a_value_that_does_not_fit_a_parameter_fails_that_task_and_the_run(
    runtime, store, empty_database_url
):
    @task(FETCH)
    def fetch(url: str) -> str:
        return "many"

    @task(PARSE)
    def parse(pages: int) -> str:
        return "unreachable"

    job = Job(
        "ingest",
        inputs=["url"],
        steps=[
            Step(FETCH, inputs={"url": "url"}, output="page_count"),
            Step(PARSE, inputs={"pages": "page_count"}),
        ],
    )

    async with default_worker():
        await submit(
            job,
            url="http://x",
            producer="test.producer",
            references=[Reference("document", "doc-1", "subject")],
        )
        await wait_until(lambda: statuses(empty_database_url) == ["succeeded", "failed"], timeout=20)

    failed = rows(
        empty_database_url,
        "SELECT error_type, error_detail FROM jobs.task_instance WHERE step_index = 1",
    )[0]
    assert failed.error_type == "ValidationError"
    assert "pages" in failed.error_detail
    (run,) = rows(empty_database_url, "SELECT job_run_status_code FROM jobs.job_run")
    assert run.job_run_status_code == "failed"


async def test_an_unregistered_producer_is_refused_and_nothing_is_sent(runtime, store):
    @task(FETCH)
    def fetch(url: str) -> str:
        return url

    with pytest.raises(IntegrityError):
        await submit(FETCH, url="http://x", producer="nobody")

    assert not stream_exists(runtime)


async def test_two_submissions_at_once_are_both_recorded(runtime, store, empty_database_url):
    @task(FETCH)
    def fetch(url: str) -> str:
        return url

    async with default_worker():
        first, second = await asyncio.gather(
            submit(FETCH, url="http://a", producer="test.producer"),
            submit(FETCH, url="http://b", producer="test.producer"),
        )
        await wait_until(all_succeeded(empty_database_url, 2), timeout=20)

    assert first != second
    runs = rows(empty_database_url, "SELECT job_run_id FROM jobs.job_run")
    assert {run.job_run_id for run in runs} == {first, second}
