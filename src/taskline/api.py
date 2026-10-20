"""What a person does to Taskline: cancel a run, resume a failed one, send queued tasks again, and
submit a job by name. These are plain async functions that never print or exit; the command line
calls them and prints their results.

A request Taskline refuses (cancelling a finished run, submitting inputs a job does not take) comes
back as a result with a refusal: a code from REFUSAL_CODES and a message. Anything unexpected raises.

Every command takes an actor, an opaque text such as "lattice:user:42". It is stored in the event log
(and as triggered_by of a submitted run), so who did it can be seen in Taskline itself.

The functions use the database and Valkey of the process's settings and keep their connections open
between calls. Call them from one long-lived event loop and call shutdown() when the host stops.
Calling them with a fresh asyncio.run each time also works, but opens new connections every time.
"""

import copy
import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from taskiq import TaskiqMessage

from taskline import catalogue
from taskline.errors import CatalogueError
from taskline.inputs import InputProblem, check_inputs
from taskline.runtime import (
    bind_to_running_loop,
    get_broker,
    get_store,
    shutdown_runtime,
    start_runtime,
)
from taskline.store import ReferencePlan, Resend
from taskline.submission import Submission
from taskline.submitting import start_job

__all__ = [
    "REFUSAL_CODES",
    "CancelResult",
    "CatalogueError",
    "InputProblem",
    "JobInfo",
    "Refusal",
    "RequeueResult",
    "RetryResult",
    "StepInfo",
    "SubmitResult",
    "cancel_run",
    "list_jobs",
    "load_catalogue",
    "requeue",
    "retry_run",
    "shutdown",
    "submit_job",
]

log = logging.getLogger(__name__)

REFUSAL_CODES = {
    "cancel_run": ("not_found", "not_running"),
    "retry_run": ("not_found", "not_failed", "never_sent"),
    "requeue": (),
    "submit_job": (
        "unknown_job",
        "invalid_inputs",
        "missing_references",
        "unknown_producer",
        "unknown_reference_kind",
    ),
}


@dataclass(frozen=True)
class Refusal:
    code: str  # one of REFUSAL_CODES for the command
    message: str  # a sentence for a person


@dataclass(frozen=True)
class StepInfo:
    index: int  # position in the job, from 0, as in the step_index of the views
    task_name: str
    queue: str


@dataclass(frozen=True)
class JobInfo:
    """What a UI needs to offer a job: its name, steps, and the form of its inputs."""

    name: str
    description: str
    steps: tuple[StepInfo, ...]
    required_reference_kinds: tuple[str, ...]  # a submission must give a reference of each kind
    input_schema: dict[str, Any]  # JSON Schema of the inputs: properties, required, additionalProperties


@dataclass(frozen=True)
class CancelResult:
    ok: bool
    refusal: Refusal | None = None
    cancelled: int = 0  # tasks that had not started, cancelled at once
    cancel_requested: int = 0  # running tasks asked to stop


@dataclass(frozen=True)
class RetryResult:
    ok: bool
    refusal: Refusal | None = None
    resent: int = 0  # tasks put back on their queue
    unsent: tuple[uuid.UUID, ...] = ()  # tasks resumed in the database whose message could not be sent


@dataclass(frozen=True)
class RequeueResult:
    ok: bool
    refusal: Refusal | None = None
    resent: int = 0
    unsent: tuple[uuid.UUID, ...] = ()


@dataclass(frozen=True)
class SubmitResult:
    ok: bool
    refusal: Refusal | None = None
    run_id: uuid.UUID | None = None
    problems: tuple[InputProblem, ...] = ()  # set with the refusal invalid_inputs


async def cancel_run(run_id: uuid.UUID, *, actor: str) -> CancelResult:
    """Cancel the tasks of a running run that have not started, and ask running tasks to stop.

    Refused with not_found if there is no such run, and not_running if it has already finished.
    A running task stops at its next heartbeat; until then the run shows cancel_requested.
    """
    _check_actor(actor)
    bind_to_running_loop()
    store = get_store()
    status = await store.run_status(run_id)
    if status is None:
        return CancelResult(False, Refusal("not_found", f"no such run: {run_id}"))
    if status != "running":
        return CancelResult(
            False, Refusal("not_running", f"run {run_id} is already {status}; nothing to cancel")
        )
    result = await store.cancel_run(run_id, actor=actor)
    await store.refresh_run_status(run_id)
    return CancelResult(True, None, len(result.cancelled), len(result.cancel_requested))


async def retry_run(run_id: uuid.UUID, *, actor: str) -> RetryResult:
    """Resume a failed run from its failed task, inside the same run.

    Refused with not_found, not_failed (the run has not failed), or never_sent (a failed task has no
    stored message, so nothing can be resent). If the run is resumed but a message cannot reach the
    queue, the result is ok and lists those tasks in unsent; requeue sends them.
    """
    _check_actor(actor)
    bind_to_running_loop()
    result = await get_store().retry_run(run_id, actor=actor)
    if result.outcome != "retried":
        messages = {
            "not_found": f"no such run: {run_id}",
            "not_failed": f"run {run_id} has not failed, so it cannot be retried",
            "never_sent": f"run {run_id} has a failed task that was never sent, so it cannot be retried",
        }
        return RetryResult(False, Refusal(result.outcome, messages[result.outcome]))
    unsent = await _send(result.resend)
    return RetryResult(True, None, len(result.resend) - len(unsent), tuple(unsent))


async def requeue(
    queue: str, *, older_than_seconds: float | None = None, actor: str
) -> RequeueResult:
    """Send again the stored messages of a queue's queued tasks, for example after Valkey was wiped.

    Only tasks queued at least older_than_seconds ago when it is given. Never refused; tasks whose
    message could not be sent are listed in unsent.
    """
    _check_actor(actor)
    if not queue:
        raise ValueError("requeue needs a queue name")
    if older_than_seconds is not None and older_than_seconds < 0:
        raise ValueError("older_than_seconds cannot be negative")
    bind_to_running_loop()
    resend = await get_store().take_queued(queue, older_than_seconds, actor=actor)
    unsent = await _send(resend)
    return RequeueResult(True, None, len(resend) - len(unsent), tuple(unsent))


async def submit_job(
    job_name: str,
    values: Mapping[str, Any],
    *,
    producer: str,
    references: Sequence[ReferencePlan] = (),
    correlation_id: str | None = None,
    actor: str,
) -> SubmitResult:
    """Start a registered job by name, as a manual run triggered by the actor.

    Refused before anything is queued with unknown_job (not in the catalogue), invalid_inputs (the
    problems name each input), missing_references (a task of the job requires a reference kind not
    given), unknown_producer, or unknown_reference_kind. Returns the new run's id.
    """
    _check_actor(actor)
    bind_to_running_loop()
    job = catalogue.get(job_name)
    if job is None:
        return _refused_submit(
            "unknown_job", f"no job named {job_name!r} is registered; load the module that defines it"
        )

    clean, problems = check_inputs(job, values)
    if problems:
        detail = "; ".join(f"{p.name} {p.message}" for p in problems)
        return _refused_submit("invalid_inputs", f"job {job.name!r}: {detail}", tuple(problems))

    given = {reference.reference_kind_code for reference in references}
    missing = sorted(job.required_references() - given)
    if missing:
        return _refused_submit(
            "missing_references", f"job {job.name!r} needs references of the kinds {missing}"
        )

    producer_unknown, unknown_kinds = await get_store().unknown_kinds(producer, sorted(given))
    if producer_unknown:
        return _refused_submit("unknown_producer", f"the producer {producer!r} is not registered")
    if unknown_kinds:
        return _refused_submit(
            "unknown_reference_kind", f"the reference kinds {unknown_kinds} are not registered"
        )

    submission = Submission(
        producer_code=producer,
        job_name=job.name,
        trigger_code="manual",
        triggered_by=actor,
        correlation_id=correlation_id,
        references=list(references),
    )
    run_id = await start_job(job, clean, submission)
    return SubmitResult(True, None, run_id)


def list_jobs() -> list[JobInfo]:
    """Every registered job, by name, with what a UI needs to offer it.

    The schemas are copies, so changing one does not change the job.
    """
    return [
        JobInfo(
            name=job.name,
            description=job.description,
            steps=tuple(
                StepInfo(index, step.task.name, step.task.queue) for index, step in enumerate(job.steps)
            ),
            required_reference_kinds=tuple(sorted(job.required_references())),
            input_schema=copy.deepcopy(job.input_schema),
        )
        for job in catalogue.jobs()
    ]


def load_catalogue(modules: Sequence[str] | str | None = None) -> list[str]:
    """Import the modules that define jobs, so that importing them registers the jobs.

    Without modules, the ones in TASKLINE_CATALOGUE_MODULES (comma-separated) are loaded. They
    should define task specs and jobs and import no task code. A module that cannot be imported
    raises CatalogueError naming it. Returns the modules loaded; loading twice is harmless.
    """
    return catalogue.load(modules)


async def shutdown() -> None:
    """Close the connections the commands opened. Call it when the host stops."""
    await shutdown_runtime()


def _check_actor(actor: str) -> None:
    if not actor or not actor.strip():
        raise ValueError("a command needs an actor, for example 'lattice:user:42'")


def _refused_submit(code: str, message: str, problems: tuple[InputProblem, ...] = ()) -> SubmitResult:
    return SubmitResult(False, Refusal(code, message), None, problems)


async def _send(resend: list[Resend]) -> list[uuid.UUID]:
    """Send stored messages to their queues; returns the ids of the tasks that could not be sent."""
    unsent: list[uuid.UUID] = []
    await start_runtime()
    broker = get_broker()
    for item in resend:
        try:
            message = TaskiqMessage.model_validate(item.message)
            await broker.kick(broker.formatter.dumps(message))
        except Exception as error:
            log.warning("taskline: could not send task %s: %s", item.task_instance_id, error)
            unsent.append(item.task_instance_id)
    return unsent
