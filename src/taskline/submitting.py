import uuid
from collections.abc import Sequence
from typing import Any

from taskiq.kicker import AsyncKicker
from taskiq_pipelines import Pipeline

from taskline.errors import InvalidInputs, MissingInputs, MissingReferences, UnknownInputs
from taskline.inputs import check_inputs
from taskline.job import STEP_ARGUMENT, Job
from taskline.runtime import get_broker, start_runtime
from taskline.spec import TaskSpec
from taskline.store import ReferencePlan
from taskline.submission import Submission, bound_submission


async def submit(
    target: Job | TaskSpec,
    *,
    producer: str,
    references: Sequence[ReferencePlan] = (),
    correlation_id: str | None = None,
    triggered_by: str | None = None,
    **inputs: Any,
) -> uuid.UUID:
    """Start a job, or a single task as a job of one task, and return the job run's id.

    The keyword arguments other than the four named ones are the job's inputs. The call
    returns once the first task is queued; it does not wait for the job to finish.
    """
    job = target if isinstance(target, Job) else Job.single(target, inputs=list(inputs))
    submission = Submission(
        producer_code=producer,
        job_name=job.name,
        triggered_by=triggered_by,
        correlation_id=correlation_id,
        references=list(references),
    )
    return await start_job(job, inputs, submission)


async def start_job(job: Job, values: dict[str, Any], submission: Submission) -> uuid.UUID:
    """Check the inputs and references, then send the job's first task.

    What is sent is what the check returns: for a job with a model, the model's validated values.
    """
    values = _check_inputs(job, values)
    _check_references(job, submission.references)

    await start_runtime()
    broker = get_broker()
    first, *later = job.steps
    pipeline = Pipeline(broker, _kicker(broker, first.task))
    for step in later:
        pipeline.call_next(_kicker(broker, step.task), **{STEP_ARGUMENT: step.wiring()})

    with bound_submission(submission):
        await pipeline.kiq(dict(values), **{STEP_ARGUMENT: first.wiring()})
    return uuid.UUID(pipeline.steps[0].task_id)


def _kicker(broker: Any, spec: TaskSpec) -> AsyncKicker:
    return AsyncKicker(spec.name, broker, spec.labels())


def _check_inputs(job: Job, values: dict[str, Any]) -> dict[str, Any]:
    """The values to send; raises when they do not fit the job's declared inputs."""
    if job.input_model is not None:
        clean, problems = check_inputs(job, values)
        if problems:
            detail = "; ".join(f"{problem.name} {problem.message}" for problem in problems)
            raise InvalidInputs(f"job {job.name!r}: {detail}", problems)
        return clean

    missing = [name for name in job.inputs if name not in values]
    if missing:
        raise MissingInputs(f"job {job.name!r} needs the inputs {missing}")
    unknown = [name for name in values if name not in job.inputs]
    if unknown:
        raise UnknownInputs(f"job {job.name!r} does not declare the inputs {unknown}")
    return dict(values)


def _check_references(job: Job, references: Sequence[ReferencePlan]) -> None:
    given = {reference.reference_kind_code for reference in references}
    missing = [
        f"task {step.task.name!r} requires a reference of kind {kind!r}"
        for step in job.steps
        for kind in step.task.requires_references
        if kind not in given
    ]
    if missing:
        raise MissingReferences(f"job {job.name!r}: " + "; ".join(missing))
