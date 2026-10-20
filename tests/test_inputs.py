"""Typed job inputs, the job listing a UI draws its form from, and loading definition modules."""

import sys
from datetime import date
from enum import StrEnum

import pytest
from helpers import default_worker, rows, statuses, wait_until
from pydantic import BaseModel, Field, model_validator

from taskline import Job, Step, TaskSpec, catalogue, task
from taskline.api import JobInfo, StepInfo, list_jobs, load_catalogue, submit_job
from taskline.config import get_config
from taskline.errors import CatalogueError, DuplicateJob, InvalidInputs
from taskline.submitting import submit

ACTOR = "lattice:user:42"
FETCH = TaskSpec(name="fetch_source", queue="work")
PARSE = TaskSpec(name="parse_pdf", queue="parse", requires_references=("document",))


class Kind(StrEnum):
    pdf = "pdf"
    html = "html"


class IngestInputs(BaseModel):
    url: str = Field(description="Where to fetch the document")
    project_id: int = Field(description="The project to file it under")
    kind: Kind = Kind.pdf
    tags: list[str] = []
    due: date | None = None


def ingest_job(**changes) -> Job:
    return Job(
        "ingest",
        inputs=IngestInputs,
        description="Fetch a document and file it",
        steps=[Step(FETCH, inputs={"url": "url", "project_id": "project_id"}, output="path")],
        **changes,
    )


@pytest.fixture(autouse=True)
def fresh_catalogue():
    catalogue._reset()
    yield
    catalogue._reset()


# --- declaring inputs ---------------------------------------------------------------------------


def test_a_model_gives_the_job_its_input_names_description_and_schema():
    job = ingest_job()

    assert job.inputs == ("url", "project_id", "kind", "tags", "due")
    assert job.description == "Fetch a document and file it"
    schema = job.input_schema
    assert schema["required"] == ["url", "project_id"]
    assert schema["properties"]["url"] == {
        "description": "Where to fetch the document", "title": "Url", "type": "string",
    }  # fmt: skip
    assert schema["properties"]["project_id"]["type"] == "integer"
    assert schema["properties"]["kind"] == {"$ref": "#/$defs/Kind", "default": "pdf"}
    assert schema["properties"]["tags"]["default"] == []
    assert schema["additionalProperties"] is False


def test_names_only_gives_an_untyped_schema_that_requires_every_name():
    job = Job("plain", inputs=["url", "n"], steps=[Step(FETCH, inputs={"url": "url"})])

    assert job.inputs == ("url", "n")
    assert job.input_schema == {
        "title": "plain",
        "type": "object",
        "properties": {"url": {}, "n": {}},
        "required": ["url", "n"],
        "additionalProperties": False,
    }


def test_an_aliased_field_is_refused_when_the_job_is_defined():
    class Aliased(BaseModel):
        from_url: str = Field(alias="from")

    with pytest.raises(ValueError, match="alias"):
        Job("aliased", inputs=Aliased, steps=[Step(FETCH)])


def test_a_model_field_with_a_reserved_name_is_refused():
    class Reserved(BaseModel):
        producer: str

    with pytest.raises(ValueError, match="producer"):
        Job("reserved", inputs=Reserved, steps=[Step(FETCH)])


def test_a_step_can_only_take_inputs_the_model_declares():
    with pytest.raises(ValueError, match="neither a job input"):
        Job("bad", inputs=IngestInputs, steps=[Step(FETCH, inputs={"url": "no_such_input"})])


def test_defining_the_same_job_twice_is_harmless_but_a_changed_one_is_refused():
    catalogue.register(ingest_job())

    class IngestInputs2(IngestInputs):
        extra: str

    catalogue.register(ingest_job())  # an equal job built again, as when a module is imported twice
    with pytest.raises(DuplicateJob):
        catalogue.register(
            Job("ingest", inputs=IngestInputs2, steps=[Step(FETCH, inputs={"url": "url"})])
        )


# --- submitting with typed inputs ---------------------------------------------------------------


async def test_submit_job_coerces_values_to_the_declared_types_and_fills_defaults(
    runtime, store, empty_database_url
):
    catalogue.register(ingest_job())

    result = await submit_job(
        "ingest",
        {"url": "https://example.test/a.pdf", "project_id": "7", "due": "2026-10-20"},
        producer="test.producer",
        actor=ACTOR,
    )

    assert (result.ok, result.problems) == (True, ())
    (message,) = rows(
        empty_database_url,
        "SELECT message -> 'args' -> 0 AS values FROM jobs.task_instance WHERE task_instance_id = :id",
        id=result.run_id,
    )
    assert message.values == {
        "url": "https://example.test/a.pdf",
        "project_id": 7,  # "7" became the integer the job declares
        "kind": "pdf",  # a default, filled in
        "tags": [],
        "due": "2026-10-20",  # a date, sent as JSON
    }


async def test_a_real_worker_receives_the_models_coerced_values(runtime, store, empty_database_url):
    received = {}

    @task(FETCH)
    def fetch(url: str, project_id: int, kind: str, due: date | None) -> str:
        received.update(url=url, project_id=project_id, kind=kind, due=due)
        return "/data/a.pdf"

    catalogue.register(
        Job(
            "ingest_typed",
            inputs=IngestInputs,
            steps=[
                Step(
                    FETCH,
                    inputs={"url": "url", "project_id": "project_id", "kind": "kind", "due": "due"},
                    output="path",
                )
            ],
        )
    )

    async with default_worker():
        result = await submit_job(
            "ingest_typed",
            {"url": "https://example.test/a.pdf", "project_id": "7", "due": "2026-10-20"},
            producer="test.producer",
            actor=ACTOR,
        )
        await wait_until(lambda: statuses(empty_database_url) == ["succeeded"])

    assert result.ok
    assert received == {
        "url": "https://example.test/a.pdf",
        "project_id": 7,  # the integer the job declares, not the text it was given as
        "kind": "pdf",  # a default the caller never gave
        "due": date(2026, 10, 20),
    }


async def test_submit_job_names_every_input_the_values_get_wrong(runtime, store, empty_database_url):
    catalogue.register(ingest_job())

    result = await submit_job(
        "ingest",
        {"project_id": "seven", "kind": "docx", "tags": ["a", 3], "colour": "red"},
        producer="test.producer",
        actor=ACTOR,
    )

    assert (result.ok, result.refusal.code, result.run_id) == (False, "invalid_inputs", None)
    problems = {(p.name, p.message) for p in result.problems}
    assert ("url", "is required and was not given") in problems
    assert ("colour", "is not an input of this job") in problems
    assert any(name == "project_id" and "valid integer" in message for name, message in problems)
    assert any(name == "kind" and "'pdf' or 'html'" in message for name, message in problems)
    assert any(name == "tags" and "valid string" in message for name, message in problems)
    assert "seven" not in result.refusal.message  # the refused values are never repeated
    assert rows(empty_database_url, "SELECT count(*) FROM jobs.job_run")[0][0] == 0


async def test_a_rule_across_several_inputs_is_reported_as_about_the_inputs_together(runtime, store):
    class Range(BaseModel):
        low: int
        high: int

        @model_validator(mode="after")
        def ordered(self):
            if self.low > self.high:
                raise ValueError("low must not exceed high")
            return self

    catalogue.register(Job("ranged", inputs=Range, steps=[Step(FETCH, inputs={"url": "low"})]))

    result = await submit_job("ranged", {"low": 5, "high": 1}, producer="test.producer", actor=ACTOR)

    assert [(p.name, "low must not exceed high" in p.message) for p in result.problems] == [
        ("(inputs)", True)
    ]


async def test_submit_with_a_model_job_raises_invalid_inputs_naming_the_input(runtime, store):
    job = ingest_job()

    with pytest.raises(InvalidInputs) as raised:
        await submit(job, producer="test.producer", url="u", project_id="seven")

    assert [p.name for p in raised.value.problems] == ["project_id"]


# --- listing jobs and loading definition modules ------------------------------------------------


def test_list_jobs_describes_each_job_by_name():
    catalogue.register(ingest_job())
    catalogue.register(
        Job("parse", inputs=["path"], steps=[Step(FETCH, inputs={"url": "path"}), Step(PARSE)])
    )

    found = list_jobs()

    assert [job.name for job in found] == ["ingest", "parse"]
    ingest, parse = found
    assert isinstance(ingest, JobInfo)
    assert ingest.description == "Fetch a document and file it"
    assert ingest.steps == (StepInfo(0, "fetch_source", "work"),)
    assert ingest.required_reference_kinds == ()
    assert ingest.input_schema["required"] == ["url", "project_id"]
    assert parse.steps == (StepInfo(0, "fetch_source", "work"), StepInfo(1, "parse_pdf", "parse"))
    assert parse.required_reference_kinds == ("document",)


def test_changing_a_listed_schema_does_not_change_the_registered_job():
    job = catalogue.register(ingest_job())

    list_jobs()[0].input_schema["properties"].clear()

    assert "url" in job.input_schema["properties"]


def test_with_no_jobs_registered_the_list_is_empty():
    assert list_jobs() == []


def write_module(tmp_path, monkeypatch, name, body):
    (tmp_path / f"{name}.py").write_text(body)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, name, raising=False)


def test_loading_definition_modules_registers_their_jobs(tmp_path, monkeypatch):
    write_module(
        tmp_path, monkeypatch, "defs_a",
        "from taskline import Job, Step, TaskSpec, catalogue\n"
        "catalogue.register(Job('from_a', inputs=['x'], steps=[Step(TaskSpec(name='t_a'), inputs={'x': 'x'})]))\n",
    )  # fmt: skip
    write_module(
        tmp_path, monkeypatch, "defs_b",
        "from taskline import Job, Step, TaskSpec, catalogue\n"
        "catalogue.register(Job('from_b', steps=[Step(TaskSpec(name='t_b'))]))\n",
    )  # fmt: skip

    loaded = load_catalogue(["defs_a", "defs_b"])
    again = load_catalogue("defs_a, defs_b")

    assert (loaded, again) == (["defs_a", "defs_b"], ["defs_a", "defs_b"])
    assert [job.name for job in list_jobs()] == ["from_a", "from_b"]


def test_loading_uses_the_modules_named_in_the_settings(tmp_path, monkeypatch):
    write_module(
        tmp_path, monkeypatch, "defs_c",
        "from taskline import Job, Step, TaskSpec, catalogue\n"
        "catalogue.register(Job('from_c', steps=[Step(TaskSpec(name='t_c'))]))\n",
    )  # fmt: skip
    monkeypatch.setenv("TASKLINE_CATALOGUE_MODULES", "defs_c")
    get_config.cache_clear()
    try:
        assert load_catalogue() == ["defs_c"]
    finally:
        get_config.cache_clear()

    assert [job.name for job in list_jobs()] == ["from_c"]


def test_a_module_that_cannot_be_imported_is_named_in_the_error():
    with pytest.raises(CatalogueError, match="no_such_defs_module"):
        load_catalogue(["no_such_defs_module"])
