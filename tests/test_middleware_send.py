import dataclasses
from types import SimpleNamespace

import pytest
from helpers import rows
from sqlalchemy.exc import IntegrityError
from taskiq import InMemoryBroker
from taskiq_pipelines import Pipeline, PipelineMiddleware

from taskline.hook_registry import HookRegistry
from taskline.lineage import LineageContext, bound_lineage
from taskline.middleware import MissingProducer, TasklineMiddleware, UnsupportedChain
from taskline.store import ReferencePlan
from taskline.submission import Submission, bound_submission


class ProducerOnlyMiddleware(TasklineMiddleware):
    """Taskline's sending side only, so that jobs run inline are not claimed or recorded."""

    async def pre_execute(self, message):
        return message

    async def post_execute(self, message, result):
        return None


@pytest.fixture
async def app(store):
    """An in-memory broker with taskline's middleware, two jobs, and a hook recorder."""
    registry = HookRegistry()
    queued = []
    registry.on("task_queued", queued.append)
    broker = InMemoryBroker(await_inplace=True).with_middlewares(
        ProducerOnlyMiddleware(store, registry), PipelineMiddleware()
    )
    ran = []

    @broker.task(task_name="first", queue_name="parse", max_attempts="3")
    async def first():
        ran.append("first")
        return 1

    @broker.task(task_name="second")
    async def second(value):
        ran.append("second")
        return value + 1

    await broker.startup()
    yield SimpleNamespace(broker=broker, first=first, second=second, ran=ran, queued=queued)
    await broker.shutdown()


def submission(**overrides) -> Submission:
    return Submission(producer_code="test.producer", **overrides)


def tasks(url: str):
    return rows(url, "SELECT * FROM jobs.task_instance ORDER BY step_index")


def runs(url: str):
    return rows(url, "SELECT * FROM jobs.job_run ORDER BY created_at")


async def test_a_single_job_is_registered_queued_and_stored(app, empty_database_url):
    references = [ReferencePlan("document", "doc-1", "subject", label="Zurich 10-K")]
    with bound_submission(
        submission(correlation_id="upload-42", triggered_by="anil", references=references)
    ):
        await app.first.kiq()

    (run,) = runs(empty_database_url)
    assert run.job_name == "first"
    assert (run.producer_code, run.correlation_id, run.triggered_by) == (
        "test.producer",
        "upload-42",
        "anil",
    )
    assert run.job_trigger_code == "api"
    assert run.producer_host
    assert run.step_count == 1
    (task,) = tasks(empty_database_url)
    assert task.task_status_code == "queued"
    assert (task.task_name, task.queue_name, task.max_attempts) == ("first", "parse", 3)
    assert task.message["task_name"] == "first"
    (reference,) = rows(empty_database_url, "SELECT * FROM jobs.job_reference")
    assert (reference.external_id, reference.label) == ("doc-1", "Zurich 10-K")
    events = rows(empty_database_url, "SELECT task_event_code FROM jobs.task_event")
    assert [e.task_event_code for e in events] == ["queued"]


async def test_a_chain_registers_every_step_up_front_and_queues_each_in_turn(app, empty_database_url):
    with bound_submission(submission()):
        await Pipeline(app.broker, app.first).call_next(app.second).kiq()

    (run,) = runs(empty_database_url)
    assert run.job_name == "first > second"
    assert run.step_count == 2
    first, second = tasks(empty_database_url)
    assert run.job_run_id == first.task_instance_id
    assert (first.task_name, second.task_name) == ("first", "second")
    assert (first.task_status_code, second.task_status_code) == ("queued", "queued")
    assert second.message["args"] == [1]
    assert app.ran == ["first", "second"]


async def test_a_submission_without_a_producer_is_refused_and_nothing_runs(app, empty_database_url):
    with pytest.raises(MissingProducer):
        await app.first.kiq()

    assert runs(empty_database_url) == []
    assert tasks(empty_database_url) == []
    assert app.ran == []


async def test_an_unregistered_producer_is_refused_and_nothing_runs(app, empty_database_url):
    with bound_submission(Submission(producer_code="nobody")), pytest.raises(IntegrityError):
        await app.first.kiq()

    assert runs(empty_database_url) == []
    assert app.ran == []


async def test_a_job_submitted_from_a_running_task_inherits_its_producer_correlation_and_parent(
    app, empty_database_url
):
    with bound_submission(submission(correlation_id="upload-42")):
        await app.first.kiq()
    (parent,) = tasks(empty_database_url)
    lineage = LineageContext(
        job_run_id=parent.job_run_id,
        task_instance_id=parent.task_instance_id,
        step_index=0,
        step_count=1,
        task_name="first",
        attempt=1,
        max_attempts=3,
        correlation_id="upload-42",
        producer_code="test.producer",
        worker_id="worker-1",
    )

    with bound_lineage(lineage):
        await app.second.kiq(5)

    _, child = runs(empty_database_url)
    assert child.producer_code == "test.producer"
    assert child.correlation_id == "upload-42"
    assert child.parent_task_instance_id == parent.task_instance_id


async def test_an_explicit_submission_overrides_what_the_running_task_would_pass_on(
    app, store, empty_database_url
):
    rows(
        empty_database_url,
        "INSERT INTO jobs.producer_kind (code, name) VALUES ('other.producer', 'Other') RETURNING 1",
    )
    with bound_submission(submission(correlation_id="upload-42")):
        await app.first.kiq()
    (parent,) = tasks(empty_database_url)
    lineage = LineageContext(
        parent.job_run_id, parent.task_instance_id, 0, 1, "first", 1, 3,
        "upload-42", "test.producer", "worker-1",
    )

    with bound_lineage(lineage), bound_submission(
        Submission(producer_code="other.producer", correlation_id="other-corr")
    ):
        await app.second.kiq(5)

    _, child = runs(empty_database_url)
    assert (child.producer_code, child.correlation_id) == ("other.producer", "other-corr")


async def test_the_queued_hook_fires_once_for_each_queued_task(app):
    with bound_submission(submission(correlation_id="upload-42")):
        await Pipeline(app.broker, app.first).call_next(app.second).kiq()

    # The in-memory broker runs a task inside the send, so hooks can arrive out of step order.
    first, second = sorted(app.queued, key=lambda payload: payload.lineage.step_index)
    assert (first.event, first.lineage.task_name, first.lineage.step_index) == ("task_queued", "first", 0)
    assert (second.lineage.task_name, second.lineage.step_index, second.lineage.step_count) == ("second", 1, 2)
    assert first.lineage.attempt == 0
    assert first.lineage.worker_id is None
    assert dataclasses.asdict(first.lineage)["correlation_id"] == "upload-42"


async def test_a_chain_with_a_step_that_is_not_sequential_is_refused(app, empty_database_url):
    with (
        bound_submission(submission()),
        pytest.raises(UnsupportedChain, match="only sequential"),
    ):
        await Pipeline(app.broker, app.first).map(app.second).kiq()

    assert runs(empty_database_url) == []
    assert app.ran == []
