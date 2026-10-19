import inspect

import pytest
from pydantic import ValidationError
from taskiq import InMemoryBroker
from taskiq.kicker import AsyncKicker
from taskiq_pipelines import Pipeline, PipelineMiddleware

from taskline.config import get_config
from taskline.errors import InvalidStepResult, MissingValue, ValuesTooLarge
from taskline.execution import guarded
from taskline.stepping import stepped


@pytest.fixture
def small_limit(monkeypatch):
    monkeypatch.setenv("TASKLINE_MAX_VALUES_BYTES", "1000")
    get_config.cache_clear()
    yield
    get_config.cache_clear()


def test_named_values_are_mapped_to_parameters_and_the_output_is_added():
    def parse(path: str, dpi: int) -> str:
        return f"{path}.parsed@{dpi}"

    run = stepped(parse)
    values = {"url": "http://x", "source_path": "/d/x.pdf"}

    result = run(
        values,
        {"inputs": {"path": "source_path"}, "arguments": {"dpi": 300}, "output": "parsed_path"},
    )

    assert result == {
        "url": "http://x",
        "source_path": "/d/x.pdf",
        "parsed_path": "/d/x.pdf.parsed@300",
    }
    assert values == {"url": "http://x", "source_path": "/d/x.pdf"}


def test_values_the_step_does_not_map_pass_through_untouched():
    run = stepped(lambda path: path.upper())

    result = run({"a": 1, "b": "two", "p": "x"}, {"inputs": {"path": "p"}, "output": "q"})

    assert result == {"a": 1, "b": "two", "p": "x", "q": "X"}


def test_text_is_converted_to_the_parameters_type_when_that_is_safe():
    def receives(project_id: int) -> str:
        return type(project_id).__name__

    run = stepped(receives)

    result = run({"pid": "7"}, {"inputs": {"project_id": "pid"}, "output": "kind"})

    assert result["kind"] == "int"


def test_a_value_that_cannot_be_converted_fails_naming_the_parameter():
    def receives(project_id: int) -> int:
        return project_id

    run = stepped(receives)

    with pytest.raises(ValidationError, match="project_id"):
        run({"pid": "seven"}, {"inputs": {"project_id": "pid"}, "output": "x"})


def test_a_step_with_no_output_leaves_the_values_unchanged():
    run = stepped(lambda path: "ignored")

    assert run({"p": "x"}, {"inputs": {"path": "p"}}) == {"p": "x"}


def test_several_outputs_are_taken_from_a_returned_dictionary():
    run = stepped(lambda path: {"text_path": path + ".txt", "page_count": 12, "extra": "dropped"})

    result = run({"p": "x"}, {"inputs": {"path": "p"}, "outputs": ["text_path", "page_count"]})

    assert result == {"p": "x", "text_path": "x.txt", "page_count": 12}


def test_a_missing_output_key_fails_with_its_name():
    run = stepped(lambda path: {"text_path": "x"})

    with pytest.raises(InvalidStepResult, match="'page_count'"):
        run({"p": "x"}, {"inputs": {"path": "p"}, "outputs": ["text_path", "page_count"]})


def test_several_outputs_need_a_dictionary():
    run = stepped(lambda path: "not a dictionary")

    with pytest.raises(InvalidStepResult, match="must return a dictionary"):
        run({"p": "x"}, {"inputs": {"path": "p"}, "outputs": ["a", "b"]})


def test_an_output_that_cannot_be_sent_as_json_fails_at_once_naming_it():
    run = stepped(lambda path: object())

    with pytest.raises(InvalidStepResult, match="'handle'"):
        run({"p": "x"}, {"inputs": {"path": "p"}, "output": "handle"})


def test_values_over_the_size_limit_fail_naming_the_largest(small_limit):
    run = stepped(lambda path: "x" * 2000)

    with pytest.raises(ValuesTooLarge) as error:
        run({"p": "short"}, {"inputs": {"path": "p"}, "output": "blob"})

    assert "'blob'" in str(error.value)
    assert "limit of 1000" in str(error.value)


def test_a_value_the_step_needs_but_that_is_missing_is_caught():
    run = stepped(lambda path: path)

    with pytest.raises(MissingValue, match="'path'.*'nowhere'"):
        run({"p": "x"}, {"inputs": {"path": "nowhere"}})


def test_a_later_step_can_use_the_output_of_any_earlier_step():
    fetch = stepped(lambda url: "/d/x.pdf")
    parse = stepped(lambda path: path + ".parsed")
    archive = stepped(lambda path: "archived:" + path)

    values = fetch({"url": "u"}, {"inputs": {"url": "url"}, "output": "source_path"})
    values = parse(values, {"inputs": {"path": "source_path"}, "output": "parsed_path"})
    values = archive(values, {"inputs": {"path": "source_path"}, "output": "archive"})

    assert values["archive"] == "archived:/d/x.pdf"
    assert values["parsed_path"] == "/d/x.pdf.parsed"


async def test_an_async_function_stays_async_and_a_plain_one_stays_plain():
    async def fetch(url: str) -> str:
        return url + "!"

    def parse(path: str) -> str:
        return path + "?"

    async_run, plain_run = stepped(fetch), stepped(parse)

    assert inspect.iscoroutinefunction(async_run)
    assert not inspect.iscoroutinefunction(plain_run)
    result = await async_run({"u": "x"}, {"inputs": {"url": "u"}, "output": "o"})
    assert result == {"u": "x", "o": "x!"}


def test_the_wrapper_keeps_the_functions_name_and_documentation():
    def parse_pdf(path: str) -> str:
        """Parse a PDF."""
        return path

    run = stepped(parse_pdf)

    assert run.__name__ == "parse_pdf"
    assert run.__doc__ == "Parse a PDF."


async def test_a_two_step_job_runs_through_the_chain_library_with_the_wiring_as_a_named_argument():
    broker = InMemoryBroker(await_inplace=True).with_middlewares(PipelineMiddleware())

    def fetch(url: str) -> str:
        return "/data/" + url.split("/")[-1]

    async def parse(path: str, dpi: int) -> dict:
        return {"text_path": path + ".txt", "page_count": dpi // 100}

    broker.task(task_name="fetch")(guarded(stepped(fetch)))
    broker.task(task_name="parse")(guarded(stepped(parse)))
    await broker.startup()

    first = {"inputs": {"url": "url"}, "output": "source_path"}
    second = {
        "inputs": {"path": "source_path"},
        "arguments": {"dpi": 300},
        "outputs": ["text_path", "page_count"],
    }
    pipeline = Pipeline(broker, AsyncKicker("fetch", broker, {})).call_next(
        AsyncKicker("parse", broker, {}), taskline_step=second
    )

    task = await pipeline.kiq({"url": "http://host/doc42.pdf"}, taskline_step=first)
    result = await task.wait_result(timeout=5)

    assert result.return_value == {
        "url": "http://host/doc42.pdf",
        "source_path": "/data/doc42.pdf",
        "text_path": "/data/doc42.pdf.txt",
        "page_count": 3,
    }
    await broker.shutdown()
