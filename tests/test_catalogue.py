import pytest

from taskline import Job, Step, TaskSpec, catalogue
from taskline.errors import DuplicateJob

FETCH = TaskSpec(name="fetch_source")


@pytest.fixture(autouse=True)
def fresh_catalogue():
    catalogue._reset()
    yield
    catalogue._reset()


def job(name="ingest", inputs=("url",)):
    return Job(name, inputs=list(inputs), steps=[Step(FETCH, inputs={"url": "url"})])


def test_a_registered_job_is_found_by_name_and_register_returns_it():
    registered = catalogue.register(job())

    assert catalogue.get("ingest") is registered


def test_an_unknown_name_is_none():
    assert catalogue.get("nothing") is None


def test_registering_the_same_definition_again_is_harmless():
    first = catalogue.register(job())

    assert catalogue.register(job()) == first  # a module imported twice builds an equal job


def test_a_different_job_with_the_same_name_is_refused():
    catalogue.register(job())

    with pytest.raises(DuplicateJob, match="ingest"):
        catalogue.register(job(inputs=("url", "project_id")))
