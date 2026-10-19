import subprocess
import sys

import pytest

from taskline.job import Job, Step
from taskline.spec import TaskSpec

FETCH = TaskSpec(name="fetch_source", queue="fetch")
PARSE = TaskSpec(name="parse_pdf", queue="parse", requires_references=("document",))
EXTRACT = TaskSpec(name="extract_claims", queue="extract", requires_references=("project",))
ARCHIVE = TaskSpec(name="archive_source", queue="fetch")


def ingest_document() -> Job:
    return Job(
        "ingest_document",
        inputs=["url", "project_id"],
        steps=[
            Step(FETCH, inputs={"url": "url"}, output="source_path"),
            Step(PARSE, inputs={"path": "source_path"}, output="parsed_path", arguments={"dpi": 300}),
            Step(EXTRACT, inputs={"parsed_path": "parsed_path", "project_id": "project_id"}),
        ],
    )


def test_a_job_of_named_inputs_and_wired_steps_loads():
    job = ingest_document()

    assert job.name == "ingest_document"
    assert job.inputs == ("url", "project_id")
    assert [step.task.name for step in job.steps] == ["fetch_source", "parse_pdf", "extract_claims"]
    assert job.steps[1].arguments == {"dpi": 300}
    assert job.steps[1].produced_names == ("parsed_path",)


def test_a_step_can_use_the_output_of_any_earlier_step_not_only_the_previous_one():
    job = Job(
        "ingest_and_archive",
        inputs=["url"],
        steps=[
            Step(FETCH, inputs={"url": "url"}, output="source_path"),
            Step(PARSE, inputs={"path": "source_path"}, output="parsed_path"),
            Step(ARCHIVE, inputs={"path": "source_path"}),
        ],
    )

    assert len(job.steps) == 3


def test_a_step_with_several_outputs_makes_each_available_to_later_steps():
    job = Job(
        "parse_then_extract",
        inputs=["path"],
        steps=[
            Step(PARSE, inputs={"path": "path"}, outputs=["text_path", "page_count"]),
            Step(EXTRACT, inputs={"parsed_path": "text_path", "pages": "page_count"}),
        ],
    )

    assert job.steps[0].produced_names == ("text_path", "page_count")


def test_a_name_that_exists_nowhere_is_refused_naming_the_step_and_the_parameter():
    with pytest.raises(ValueError) as error:
        Job("broken", inputs=["url"], steps=[Step(FETCH, inputs={"url": "adress"})])

    message = str(error.value)
    assert "step 1 (fetch_source)" in message
    assert "'url'" in message
    assert "'adress'" in message


def test_using_the_output_of_a_later_step_is_refused():
    with pytest.raises(ValueError, match="step 1 .*'parsed_path'"):
        Job(
            "backwards",
            inputs=["url"],
            steps=[
                Step(ARCHIVE, inputs={"path": "parsed_path"}),
                Step(PARSE, inputs={"path": "url"}, output="parsed_path"),
            ],
        )


def test_an_output_named_like_a_job_input_is_refused():
    with pytest.raises(ValueError, match="already a job input or an output"):
        Job("clash", inputs=["url"], steps=[Step(FETCH, inputs={"url": "url"}, output="url")])


def test_two_steps_with_the_same_output_name_are_refused():
    with pytest.raises(ValueError, match="step 2 .*'result'"):
        Job(
            "clash",
            inputs=["url"],
            steps=[
                Step(FETCH, inputs={"url": "url"}, output="result"),
                Step(PARSE, inputs={"path": "result"}, output="result"),
            ],
        )


def test_an_input_declared_twice_is_refused():
    with pytest.raises(ValueError, match="declared twice"):
        Job("twice", inputs=["url", "url"], steps=[Step(FETCH, inputs={"url": "url"})])


@pytest.mark.parametrize("name", ["producer", "references", "correlation_id", "triggered_by"])
def test_an_input_cannot_be_named_like_a_parameter_of_submit(name):
    with pytest.raises(ValueError, match="parameter of submit"):
        Job("clash", inputs=[name], steps=[Step(FETCH, inputs={"url": name})])


def test_a_step_cannot_give_both_output_and_outputs():
    with pytest.raises(ValueError, match="not both"):
        Step(FETCH, output="a", outputs=["b", "c"])


def test_a_parameter_cannot_be_both_an_input_and_a_constant():
    with pytest.raises(ValueError, match="both an input and an argument"):
        Step(PARSE, inputs={"dpi": "x"}, arguments={"dpi": 300})


def test_a_job_needs_a_name_and_at_least_one_step():
    with pytest.raises(ValueError, match="needs a name"):
        Job("", steps=[Step(FETCH)])
    with pytest.raises(ValueError, match="at least one step"):
        Job("empty", steps=[])


def test_the_same_task_can_be_used_in_two_steps():
    job = Job(
        "twice",
        inputs=["path"],
        steps=[
            Step(ARCHIVE, inputs={"path": "path"}),
            Step(ARCHIVE, inputs={"path": "path"}),
        ],
    )

    assert [step.task.name for step in job.steps] == ["archive_source", "archive_source"]


def test_a_single_task_job_is_named_after_the_task_and_takes_its_parameters_as_inputs():
    job = Job.single(PARSE, inputs=["path", "dpi"])

    assert job.name == "parse_pdf"
    assert job.inputs == ("path", "dpi")
    assert job.steps[0].inputs == {"path": "path", "dpi": "dpi"}


def test_the_required_references_of_a_job_are_those_of_all_its_tasks():
    assert ingest_document().required_references() == {"document", "project"}


def test_importing_a_job_definition_does_not_pull_in_taskiq_or_the_database_code():
    code = (
        "import sys, taskline.job; "
        "heavy = [m for m in ('taskiq', 'sqlalchemy', 'redis') if m in sys.modules]; "
        "sys.exit(len(heavy))"
    )

    assert subprocess.run([sys.executable, "-c", code], capture_output=True).returncode == 0
