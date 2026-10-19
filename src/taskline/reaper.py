import asyncio
import logging
import os
import socket
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field

from taskiq import TaskiqMessage

from taskline.broker import TasklineBroker
from taskline.config import TasklineConfig, get_config
from taskline.hook_registry import HookPayload, HookRegistry, hooks
from taskline.middleware import lineage_from_info
from taskline.queries import REAPER_QUEUE, Queries
from taskline.store import RecoveredTask, Store

log = logging.getLogger(__name__)

WARN_EVERY_SECONDS = 600


@dataclass
class ReaperReport:
    retried: int = 0  # expired leases turned into retries
    failed: int = 0  # tasks failed: an expired lease on the last attempt, or a lost hand-off
    resent: int = 0  # due retries sent to the queue
    unsent: int = 0  # due retries that could not be sent and will be tried again
    dead_workers: int = 0
    warnings: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)


class Reaper:
    """Finds work that has gone wrong and fixes its records. It never touches a queued task.

    Any number of reapers can run at once; each check takes rows another reaper does not hold.
    """

    def __init__(
        self,
        store: Store,
        queries: Queries,
        broker: TasklineBroker,
        hook_registry: HookRegistry = hooks,
        config: TasklineConfig | None = None,
    ) -> None:
        self._store = store
        self._queries = queries
        self._broker = broker
        self._hooks = hook_registry
        self._config = config or get_config()
        self.reaper_id = f"reaper:{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self._warned: dict[tuple[str, str], float] = {}

    async def run(self, stop: asyncio.Event) -> None:
        """Pass after pass, every interval, until stop is set."""
        await self._store.register_worker(self.reaper_id, REAPER_QUEUE)
        try:
            while not stop.is_set():
                await self.run_once()
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self._config.reaper_interval_seconds)
        finally:
            await self._store.stop_worker(self.reaper_id)

    async def run_once(self) -> ReaperReport:
        report = ReaperReport()
        checks: list[tuple[str, Callable[[ReaperReport], Awaitable[None]]]] = [
            ("alive ping", self._ping),
            ("expired leases", self._expired_leases),
            ("due retries", self._due_retries),
            ("lost hand-offs", self._lost_handoffs),
            ("dead workers", self._dead_workers),
            ("queue report", self._queue_report),
        ]
        for name, check in checks:
            try:
                await check(report)
            except Exception:
                log.exception("reaper check %r failed", name)
                report.failed_checks.append(name)
        return report

    async def _ping(self, report: ReaperReport) -> None:
        await self._store.touch_worker(self.reaper_id)

    async def _expired_leases(self, report: ReaperReport) -> None:
        for task in await self._store.reap_expired_leases(self._config.reaper_batch_size):
            if task.failed:
                report.failed += 1
                await self._after_failure(task)
            else:
                report.retried += 1
                await self._notify_retry(task)

    async def _due_retries(self, report: ReaperReport) -> None:
        for item in await self._store.take_due_retries(self._config.reaper_batch_size):
            try:
                message = TaskiqMessage.model_validate(item.message)
                await self._broker.kick(self._broker.formatter.dumps(message))
            except Exception:
                log.exception("could not send the retry of task %s", item.task_instance_id)
                await self._store.return_to_retrying(item.task_instance_id)
                report.unsent += 1
            else:
                report.resent += 1

    async def _lost_handoffs(self, report: ReaperReport) -> None:
        failed = await self._store.fail_lost_handoffs(
            self._config.handoff_grace_seconds, self._config.reaper_batch_size
        )
        for task in failed:
            report.failed += 1
            await self._after_failure(task)

    async def _dead_workers(self, report: ReaperReport) -> None:
        dead = await self._store.mark_dead_workers(self._config.dead_worker_seconds)
        for worker_id in dead:
            log.warning("worker %s stopped pinging and is marked dead", worker_id)
        report.dead_workers = len(dead)

    async def _queue_report(self, report: ReaperReport) -> None:
        for counts in await self._queries.queue_counts():
            waiting = counts.queued + counts.retrying
            if waiting and not counts.live_workers:
                self._warn(
                    report, counts.queue_name, "no_worker",
                    f"queue {counts.queue_name!r} has {waiting} tasks waiting and no live worker",
                )
            expected = counts.queued + counts.running
            held = await self._broker.queue_length(counts.queue_name)
            if held < expected:
                self._warn(
                    report, counts.queue_name, "missing_messages",
                    f"queue {counts.queue_name!r}: Postgres has {expected} tasks that should have "
                    f"messages but Valkey holds {held}; messages may be missing",
                )

    def _warn(self, report: ReaperReport, queue: str, kind: str, message: str) -> None:
        now = time.monotonic()
        if now - self._warned.get((queue, kind), float("-inf")) < WARN_EVERY_SECONDS:
            return
        self._warned[(queue, kind)] = now
        log.warning(message)
        report.warnings.append(message)

    async def _after_failure(self, task: RecoveredTask) -> None:
        await self._store.fail_downstream(task.job_run_id, task.step_index)
        info = await self._queries.task_info(task.task_instance_id)
        error_type, detail = await self._queries.task_error(task.task_instance_id)
        if info is not None:
            lineage = lineage_from_info(info)
            await self._hooks.fire(
                HookPayload("task_failed", lineage, error_type=error_type, error_detail=detail)
            )
        status = await self._store.refresh_run_status(task.job_run_id)
        if info is not None and status is not None and status != "running":
            await self._hooks.fire(HookPayload("run_finished", lineage_from_info(info), run_status=status))

    async def _notify_retry(self, task: RecoveredTask) -> None:
        info = await self._queries.task_info(task.task_instance_id)
        if info is None:
            return
        error_type, detail = await self._queries.task_error(task.task_instance_id)
        await self._hooks.fire(
            HookPayload(
                "task_retrying", lineage_from_info(info), error_type=error_type, error_detail=detail
            )
        )
