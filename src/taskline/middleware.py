import asyncio
import logging
import os
import socket
import traceback
import uuid
from collections.abc import Awaitable, Callable
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult
from taskiq.exceptions import NoResultError
from taskiq.labels import parse_label
from taskiq_pipelines import PipelineMiddleware
from taskiq_pipelines.constants import CURRENT_STEP, PIPELINE_DATA

from taskline.config import get_config
from taskline.context import bound_lease
from taskline.errors import TaskCancelled
from taskline.execution import AttemptState, bound_attempt
from taskline.hook_registry import HookPayload, HookRegistry, hooks
from taskline.lease import Lease
from taskline.lineage import LineageContext, bound_lineage, current_lineage
from taskline.retry import retry_delay, should_retry
from taskline.spec import LABEL_CANCEL_GRACE, LABEL_MAX_ATTEMPTS, LABEL_QUEUE
from taskline.store import RunPlan, StepPlan, Store, TaskInfo
from taskline.submission import current_submission

log = logging.getLogger(__name__)


class MissingProducer(Exception):
    """A run was submitted with no producer and no running task to inherit one from."""


class UnsupportedChain(Exception):
    """A chain contains a step other than a plain sequential one."""


def lineage_from_info(info: TaskInfo, worker_id: str | None = None) -> LineageContext:
    return LineageContext(
        job_run_id=info.job_run_id,
        task_instance_id=info.task_instance_id,
        step_index=info.step_index,
        step_count=info.step_count,
        task_name=info.task_name,
        attempt=info.attempt,
        max_attempts=info.max_attempts,
        correlation_id=info.correlation_id,
        producer_code=info.producer_code,
        worker_id=worker_id,
    )


@dataclass
class _Attempt:
    """What pre_execute set up for one message, for post_execute to finish."""

    scope: ExitStack
    state: AttemptState
    lineage: LineageContext | None = None
    lease: Lease | None = None


def _exit_process() -> None:
    os._exit(1)


class TasklineMiddleware(TaskiqMiddleware):
    def __init__(
        self,
        store: Store,
        hook_registry: HookRegistry = hooks,
        default_queue: str = "default",
        worker_id: str | None = None,
        consume_queue: str | None = None,
        heartbeat_interval: float | None = None,
        lease_intervals: int | None = None,
        end_process: Callable[[], None] = _exit_process,
        queue_reachable: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__()
        config = get_config()
        self._store = store
        self._hooks = hook_registry
        self._default_queue = default_queue
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self._heartbeat_interval = heartbeat_interval or config.heartbeat_interval_seconds
        self._lease_seconds = self._heartbeat_interval * (lease_intervals or config.lease_intervals)
        self._consume_queue = consume_queue or config.consume_queue
        self._end_process = end_process
        self._queue_reachable = queue_reachable
        self._queued: dict[str, TaskInfo] = {}
        self._attempts: dict[str, _Attempt] = {}
        self._alive_task: asyncio.Task | None = None

    async def startup(self) -> None:
        if not self.broker.is_worker_process:
            return
        await self._store.register_worker(self.worker_id, self._consume_queue)
        self._alive_task = asyncio.create_task(self._keep_alive())

    async def shutdown(self) -> None:
        if self._alive_task is None:
            return
        self._alive_task.cancel()
        await asyncio.gather(self._alive_task, return_exceptions=True)
        self._alive_task = None
        for attempt in self._attempts.values():
            if attempt.lease is not None:
                await attempt.lease.stop()
        await self._store.stop_worker(self.worker_id)

    async def _keep_alive(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                if self._queue_reachable is not None:
                    await self._queue_reachable()
                await self._store.touch_worker(self.worker_id)
            except Exception:
                log.exception("could not record that worker %s is alive", self.worker_id)

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        if int(message.labels.get(CURRENT_STEP, 0)) == 0:
            await self._store.register_run(self._plan_run(message))
        info = await self._store.mark_queued(uuid.UUID(message.task_id), message.model_dump(mode="json"))
        if info is not None:
            self._queued[message.task_id] = info
        return message

    async def post_send(self, message: TaskiqMessage) -> None:
        info = self._queued.pop(message.task_id, None)
        if info is not None:
            await self._hooks.fire(HookPayload("task_queued", lineage_from_info(info)))

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        task_id = uuid.UUID(message.task_id)
        claim = await self._store.claim(task_id, self.worker_id, self._lease_seconds)

        scope = ExitStack()
        if claim is None:
            state = AttemptState(claimed=False)
            scope.enter_context(bound_attempt(state))
            self._attempts[message.task_id] = _Attempt(scope, state)
            return message

        state = AttemptState(claimed=True)
        lineage = LineageContext(
            job_run_id=claim.job_run_id,
            task_instance_id=task_id,
            step_index=claim.step_index,
            step_count=claim.step_count,
            task_name=message.task_name,
            attempt=claim.attempt,
            max_attempts=claim.max_attempts,
            correlation_id=claim.correlation_id,
            producer_code=claim.producer_code,
            worker_id=self.worker_id,
        )
        grace = message.labels.get(LABEL_CANCEL_GRACE)
        lease = Lease(
            self._store,
            task_id,
            claim.attempt,
            heartbeat_interval=self._heartbeat_interval,
            lease_seconds=self._lease_seconds,
            on_cancel=state.on_cancel,
            on_lost=state.on_lost,
            cancel_grace_seconds=float(grace) if grace is not None else None,
            on_grace_expired=lambda: self._grace_expired(task_id, claim.attempt),
        )
        lease.start()
        scope.enter_context(bound_lineage(lineage))
        scope.enter_context(bound_lease(lease))
        scope.enter_context(bound_attempt(state))
        self._attempts[message.task_id] = _Attempt(scope, state, lineage, lease)

        await self._hooks.fire(HookPayload("task_started", lineage))
        return message

    async def _grace_expired(self, task_id: uuid.UUID, attempt: int) -> None:
        """The task did not stop within its cancel grace period: record it, end the process."""
        if await self._store.finish_cancelled(task_id, attempt):
            log.error("task %s ignored its cancel request; ending this worker process", task_id)
            self._end_process()

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult[Any]) -> None:
        attempt = self._attempts.pop(message.task_id, None)
        if attempt is None:
            return
        try:
            if attempt.lease is not None:
                await attempt.lease.stop()
            if attempt.lineage is not None and not isinstance(result.error, NoResultError):
                await self._record_outcome(attempt.lineage, result, message.labels)
        finally:
            attempt.scope.close()

    async def _record_outcome(
        self, lineage: LineageContext, result: TaskiqResult[Any], labels: dict[str, Any]
    ) -> None:
        task_id, attempt = lineage.task_instance_id, lineage.attempt
        error = result.error
        if not result.is_err:
            if not await self._store.finish_succeeded(task_id, attempt):
                return
            payload = HookPayload("task_succeeded", lineage)
        elif isinstance(error, TaskCancelled):
            if not await self._store.finish_cancelled(task_id, attempt):
                return
            payload = HookPayload("task_cancelled", lineage)
        else:
            error_type, detail = type(error).__name__, str(error)
            formatted = "".join(traceback.format_exception(error))
            if should_retry(lineage, error):
                delay = retry_delay(attempt, labels)
                if await self._store.schedule_retry(
                    task_id, attempt, delay, error_type, detail, formatted
                ):
                    await self._hooks.fire(
                        HookPayload(
                            "task_retrying", lineage, error_type=error_type, error_detail=detail
                        )
                    )
                return
            if not await self._store.finish_failed(task_id, attempt, error_type, detail, formatted):
                return
            payload = HookPayload(
                "task_failed", lineage, error_type=error_type, error_detail=detail
            )

        if result.is_err:
            await self._store.fail_downstream(lineage.job_run_id, lineage.step_index)
        await self._hooks.fire(payload)
        run_status = await self._store.refresh_run_status(lineage.job_run_id)
        if run_status is not None and run_status != "running":
            await self._hooks.fire(HookPayload("run_finished", lineage, run_status=run_status))

    def _plan_run(self, message: TaskiqMessage) -> RunPlan:
        steps = self._plan_steps(message)
        submission = current_submission()
        lineage = current_lineage()

        producer_code = (submission and submission.producer_code) or (
            lineage and lineage.producer_code
        )
        if not producer_code:
            raise MissingProducer(
                f"{message.task_name} was submitted without a producer; submit it with "
                "taskline's submit function and name the producer"
            )
        names = [step.task_name for step in steps]
        return RunPlan(
            job_run_id=steps[0].task_instance_id,
            job_name=(submission and submission.job_name) or " > ".join(names),
            job_trigger_code=submission.trigger_code if submission else "api",
            producer_code=producer_code,
            steps=steps,
            producer_host=socket.gethostname(),
            triggered_by=submission.triggered_by if submission else None,
            correlation_id=(submission and submission.correlation_id)
            or (lineage.correlation_id if lineage else None),
            parent_task_instance_id=(submission and submission.parent_task_instance_id)
            or (lineage.task_instance_id if lineage else None),
            schedule_id=submission.schedule_id if submission else None,
            references=list(submission.references) if submission else [],
        )

    def _plan_steps(self, message: TaskiqMessage) -> list[StepPlan]:
        if PIPELINE_DATA not in message.labels:
            return [self._step(uuid.UUID(message.task_id), 0, message.task_name, message.labels)]

        plan = parse_label(
            message.labels[PIPELINE_DATA], (message.labels_types or {}).get(PIPELINE_DATA)
        )
        dumped_steps = self.broker.serializer.loadb(plan)
        steps = []
        for index, dumped in enumerate(dumped_steps):
            data: dict[str, Any] = dumped["step_data"]
            if dumped["step_type"] != "sequential":
                raise UnsupportedChain(
                    f"step {index + 1} is a {dumped['step_type']!r} step; only sequential "
                    "chains are supported"
                )
            steps.append(
                self._step(uuid.UUID(dumped["task_id"]), index, data["task_name"], data["labels"])
            )
        return steps

    def _step(
        self, task_id: uuid.UUID, index: int, task_name: str, labels: dict[str, Any]
    ) -> StepPlan:
        return StepPlan(
            task_instance_id=task_id,
            step_index=index,
            task_name=task_name,
            queue_name=str(labels.get(LABEL_QUEUE, self._default_queue)),
            max_attempts=int(labels.get(LABEL_MAX_ATTEMPTS, 1)),
        )


class ChainMiddleware(PipelineMiddleware):
    """The chain library's middleware, except that it does not fail the chain on a task that
    is about to be retried, or on the "no result" signal of a duplicate delivery or a lost
    lease. It fails the chain only when a task has failed for good."""

    async def on_error(
        self, message: TaskiqMessage, result: TaskiqResult[Any], exception: BaseException
    ) -> None:
        if isinstance(exception, NoResultError):
            return
        lineage = current_lineage()
        if lineage is not None and should_retry(lineage, exception):
            return
        await super().on_error(message, result, exception)
