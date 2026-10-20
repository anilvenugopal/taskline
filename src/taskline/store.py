import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine


@dataclass(frozen=True)
class ReferencePlan:
    reference_kind_code: str
    external_id: str
    reference_relation_code: str
    label: str | None = None
    url: str | None = None
    step_index: int | None = None  # None: the reference is about the whole run


@dataclass(frozen=True)
class StepPlan:
    task_instance_id: uuid.UUID
    step_index: int
    task_name: str
    queue_name: str
    max_attempts: int = 1


@dataclass(frozen=True)
class RunPlan:
    job_run_id: uuid.UUID
    job_name: str
    job_trigger_code: str
    producer_code: str
    steps: list[StepPlan]
    producer_host: str | None = None
    triggered_by: str | None = None
    correlation_id: str | None = None
    parent_task_instance_id: uuid.UUID | None = None
    schedule_id: uuid.UUID | None = None
    references: list[ReferencePlan] = field(default_factory=list)


@dataclass(frozen=True)
class Claim:
    attempt: int
    max_attempts: int
    job_run_id: uuid.UUID
    step_index: int
    step_count: int
    correlation_id: str | None
    producer_code: str


@dataclass(frozen=True)
class TaskInfo:
    task_instance_id: uuid.UUID
    job_run_id: uuid.UUID
    step_index: int
    step_count: int
    task_name: str
    attempt: int
    max_attempts: int
    correlation_id: str | None
    producer_code: str


@dataclass(frozen=True)
class HeartbeatResult:
    held: bool  # False: the task is no longer running under the caller's attempt
    cancel_requested: bool


@dataclass(frozen=True)
class CancelResult:
    cancelled: list[uuid.UUID]  # not running: cancelled at once
    cancel_requested: list[uuid.UUID]  # running: will stop at their next heartbeat


@dataclass(frozen=True)
class RecoveredTask:
    task_instance_id: uuid.UUID
    job_run_id: uuid.UUID
    step_index: int
    failed: bool  # False: the task is retrying


@dataclass(frozen=True)
class Resend:
    task_instance_id: uuid.UUID
    message: dict[str, Any]


@dataclass(frozen=True)
class RetryResult:
    # "retried", "not_found", "not_failed" (the run is not in the failed state) or
    # "never_sent" (a failed task has no stored message, so nothing can be resent)
    outcome: str
    resend: list[Resend] = field(default_factory=list)


def _actor_detail(actor: str | None) -> dict[str, str]:
    """The event detail that names who did something; empty when nobody is named."""
    return {} if actor is None else {"actor": actor}


class Store:
    def __init__(self, database_url: str) -> None:
        self._engine = create_async_engine(database_url, pool_pre_ping=True)

    async def close(self) -> None:
        await self._engine.dispose()

    async def register_run(self, plan: RunPlan) -> None:
        """Record a run, its steps (all planned) and its references in one transaction."""
        if not plan.steps:
            raise ValueError("a run needs at least one step")
        step_ids = {step.step_index: step.task_instance_id for step in plan.steps}
        if len(step_ids) != len(plan.steps):
            raise ValueError("step_index values must be unique")
        for reference in plan.references:
            if reference.step_index is not None and reference.step_index not in step_ids:
                raise ValueError(f"reference points at missing step {reference.step_index}")

        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO jobs.job_run (job_run_id, job_name, job_trigger_code, step_count, "
                    "correlation_id, producer_code, producer_host, triggered_by, "
                    "parent_task_instance_id, schedule_id) VALUES (:job_run_id, :job_name, "
                    ":job_trigger_code, :step_count, :correlation_id, :producer_code, "
                    ":producer_host, :triggered_by, :parent_task_instance_id, :schedule_id)"
                ),
                {
                    "job_run_id": plan.job_run_id,
                    "job_name": plan.job_name,
                    "job_trigger_code": plan.job_trigger_code,
                    "step_count": len(plan.steps),
                    "correlation_id": plan.correlation_id,
                    "producer_code": plan.producer_code,
                    "producer_host": plan.producer_host,
                    "triggered_by": plan.triggered_by,
                    "parent_task_instance_id": plan.parent_task_instance_id,
                    "schedule_id": plan.schedule_id,
                },
            )
            await conn.execute(
                text(
                    "INSERT INTO jobs.task_instance (task_instance_id, job_run_id, step_index, "
                    "task_name, queue_name, task_status_code, max_attempts) VALUES "
                    "(:task_instance_id, :job_run_id, :step_index, :task_name, :queue_name, "
                    "'planned', :max_attempts)"
                ),
                [
                    {
                        "task_instance_id": step.task_instance_id,
                        "job_run_id": plan.job_run_id,
                        "step_index": step.step_index,
                        "task_name": step.task_name,
                        "queue_name": step.queue_name,
                        "max_attempts": step.max_attempts,
                    }
                    for step in plan.steps
                ],
            )
            if plan.references:
                await conn.execute(
                    text(
                        "INSERT INTO jobs.job_reference (job_run_id, task_instance_id, "
                        "reference_kind_code, external_id, reference_relation_code, label, url) "
                        "VALUES (:job_run_id, :task_instance_id, :reference_kind_code, "
                        ":external_id, :reference_relation_code, :label, :url)"
                    ),
                    [
                        {
                            "job_run_id": plan.job_run_id,
                            "task_instance_id": step_ids.get(reference.step_index),
                            "reference_kind_code": reference.reference_kind_code,
                            "external_id": reference.external_id,
                            "reference_relation_code": reference.reference_relation_code,
                            "label": reference.label,
                            "url": reference.url,
                        }
                        for reference in plan.references
                    ],
                )

    async def mark_queued(
        self, task_instance_id: uuid.UUID, message: dict[str, Any]
    ) -> TaskInfo | None:
        """Set a planned or queued task to queued and keep its message for the reaper.

        Returns the task's details, or None when the task is in any other state, for
        example cancelled.
        """
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "WITH queued AS (UPDATE jobs.task_instance SET "
                        "task_status_code = 'queued', message = CAST(:message AS jsonb), "
                        "queued_at = now() WHERE task_instance_id = :task_instance_id "
                        "AND task_status_code IN ('planned', 'queued') "
                        "RETURNING task_instance_id, job_run_id, step_index, task_name, "
                        "attempt, max_attempts) "
                        "SELECT q.*, r.step_count, r.correlation_id, r.producer_code "
                        "FROM queued q JOIN jobs.job_run r ON r.job_run_id = q.job_run_id"
                    ),
                    {"task_instance_id": task_instance_id, "message": json.dumps(message)},
                )
            ).first()
            if row is None:
                return None
            await self._add_event(conn, task_instance_id, "queued", attempt=row.attempt)
            return TaskInfo(
                task_instance_id=row.task_instance_id,
                job_run_id=row.job_run_id,
                step_index=row.step_index,
                step_count=row.step_count,
                task_name=row.task_name,
                attempt=row.attempt,
                max_attempts=row.max_attempts,
                correlation_id=row.correlation_id,
                producer_code=row.producer_code,
            )

    async def claim(
        self, task_instance_id: uuid.UUID, worker_id: str, lease_seconds: float
    ) -> Claim | None:
        """Start an attempt on a queued task, or return None if it cannot be claimed.

        A task that exists but is not queued (running, finished, cancelled) gets a
        duplicate_skipped event naming its current status and holder.
        """
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET task_status_code = 'running', "
                        "attempt = attempt + 1, worker_id = :worker_id, "
                        "started_at = COALESCE(started_at, now()), heartbeat_at = now(), "
                        "lease_expires_at = now() + make_interval(secs => "
                        "CAST(:lease_seconds AS double precision)), retry_at = NULL, "
                        "progress_pct = NULL, progress_note = NULL "
                        "WHERE task_instance_id = :task_instance_id "
                        "AND task_status_code = 'queued' "
                        "RETURNING attempt, max_attempts, job_run_id, step_index"
                    ),
                    {
                        "task_instance_id": task_instance_id,
                        "worker_id": worker_id,
                        "lease_seconds": lease_seconds,
                    },
                )
            ).first()

            if row is None:
                current = (
                    await conn.execute(
                        text(
                            "SELECT task_status_code, worker_id, attempt FROM jobs.task_instance "
                            "WHERE task_instance_id = :task_instance_id"
                        ),
                        {"task_instance_id": task_instance_id},
                    )
                ).first()
                if current is not None:
                    await self._add_event(
                        conn,
                        task_instance_id,
                        "duplicate_skipped",
                        attempt=current.attempt,
                        worker_id=worker_id,
                        detail={"status": current.task_status_code, "held_by": current.worker_id},
                    )
                return None

            run = (
                await conn.execute(
                    text(
                        "SELECT correlation_id, step_count, producer_code "
                        "FROM jobs.job_run WHERE job_run_id = :id"
                    ),
                    {"id": row.job_run_id},
                )
            ).one()
            await self._add_event(
                conn, task_instance_id, "claimed", attempt=row.attempt, worker_id=worker_id
            )
            return Claim(
                attempt=row.attempt,
                max_attempts=row.max_attempts,
                job_run_id=row.job_run_id,
                step_index=row.step_index,
                step_count=run.step_count,
                correlation_id=run.correlation_id,
                producer_code=run.producer_code,
            )

    async def heartbeat(
        self,
        task_instance_id: uuid.UUID,
        attempt: int,
        lease_seconds: float,
        progress_pct: float | None = None,
        progress_note: str | None = None,
    ) -> HeartbeatResult:
        """Renew the lease of a running attempt, optionally recording progress."""
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET heartbeat_at = now(), "
                        "lease_expires_at = now() + make_interval(secs => "
                        "CAST(:lease_seconds AS double precision)), "
                        "progress_pct = COALESCE(:progress_pct, progress_pct), "
                        "progress_note = COALESCE(:progress_note, progress_note) "
                        "WHERE task_instance_id = :task_instance_id "
                        "AND task_status_code = 'running' AND attempt = :attempt "
                        "RETURNING cancel_requested_at IS NOT NULL AS cancel_requested"
                    ),
                    {
                        "task_instance_id": task_instance_id,
                        "attempt": attempt,
                        "lease_seconds": lease_seconds,
                        "progress_pct": progress_pct,
                        "progress_note": progress_note,
                    },
                )
            ).first()
        if row is None:
            return HeartbeatResult(held=False, cancel_requested=False)
        return HeartbeatResult(held=True, cancel_requested=row.cancel_requested)

    async def finish_succeeded(self, task_instance_id: uuid.UUID, attempt: int) -> bool:
        """End a running attempt as succeeded. False if the caller no longer holds the task."""
        return await self._end_attempt(
            task_instance_id,
            attempt,
            assignments=(
                "task_status_code = 'succeeded', finished_at = now(), error_type = NULL, "
                "error_detail = NULL, error_traceback = NULL"
            ),
            params={},
            event_code="succeeded",
            detail=None,
        )

    async def finish_failed(
        self,
        task_instance_id: uuid.UUID,
        attempt: int,
        error_type: str,
        error_detail: str,
        error_traceback: str | None = None,
    ) -> bool:
        """End a running attempt as failed. False if the caller no longer holds the task."""
        return await self._end_attempt(
            task_instance_id,
            attempt,
            assignments=(
                "task_status_code = 'failed', finished_at = now(), error_type = :error_type, "
                "error_detail = :error_detail, error_traceback = :error_traceback"
            ),
            params={
                "error_type": error_type,
                "error_detail": error_detail,
                "error_traceback": error_traceback,
            },
            event_code="failed",
            detail={"error_type": error_type, "error": error_detail},
        )

    async def finish_cancelled(self, task_instance_id: uuid.UUID, attempt: int) -> bool:
        """End a running attempt as cancelled. False if the caller no longer holds the task."""
        return await self._end_attempt(
            task_instance_id,
            attempt,
            assignments="task_status_code = 'cancelled', finished_at = now()",
            params={},
            event_code="cancelled",
            detail=None,
        )

    async def schedule_retry(
        self,
        task_instance_id: uuid.UUID,
        attempt: int,
        delay_seconds: float,
        error_type: str,
        error_detail: str,
        error_traceback: str | None = None,
    ) -> bool:
        """End a running attempt as retrying, due in delay_seconds. False if not held."""
        return await self._end_attempt(
            task_instance_id,
            attempt,
            assignments=(
                "task_status_code = 'retrying', retry_at = now() + make_interval(secs => "
                "CAST(:delay_seconds AS double precision)), error_type = :error_type, "
                "error_detail = :error_detail, error_traceback = :error_traceback"
            ),
            params={
                "delay_seconds": delay_seconds,
                "error_type": error_type,
                "error_detail": error_detail,
                "error_traceback": error_traceback,
            },
            event_code="retry_scheduled",
            detail={
                "retry_in_seconds": delay_seconds,
                "error_type": error_type,
                "error": error_detail,
            },
        )

    async def fail_downstream(
        self, job_run_id: uuid.UUID, after_step: int
    ) -> list[uuid.UUID]:
        """Mark the planned steps after a failed step as upstream_failed; returns their ids."""
        async with self._engine.begin() as conn:
            marked = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET task_status_code = 'upstream_failed', "
                        "finished_at = now() WHERE job_run_id = :job_run_id "
                        "AND step_index > :after_step AND task_status_code = 'planned' "
                        "RETURNING task_instance_id"
                    ),
                    {"job_run_id": job_run_id, "after_step": after_step},
                )
            ).all()
            for row in marked:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "upstream_failed",
                    detail={"after_step": after_step},
                )
            return [row.task_instance_id for row in marked]

    async def refresh_run_status(self, job_run_id: uuid.UUID) -> str | None:
        """Recompute a run's status from its tasks; returns the new status if it changed."""
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "UPDATE jobs.job_run r SET job_run_status_code = s.status, "
                        "finished_at = CASE WHEN s.status = 'running' THEN NULL ELSE now() END "
                        "FROM (SELECT CASE "
                        "WHEN bool_or(task_status_code IN "
                        "('planned', 'queued', 'running', 'retrying')) THEN 'running' "
                        "WHEN bool_or(task_status_code = 'cancelled') THEN 'cancelled' "
                        "WHEN bool_or(task_status_code IN ('failed', 'upstream_failed')) "
                        "THEN 'failed' "
                        "ELSE 'succeeded' END AS status "
                        "FROM jobs.task_instance WHERE job_run_id = :job_run_id) s "
                        "WHERE r.job_run_id = :job_run_id AND r.job_run_status_code <> s.status "
                        "RETURNING r.job_run_status_code"
                    ),
                    {"job_run_id": job_run_id},
                )
            ).first()
        return None if row is None else row.job_run_status_code

    async def run_status(self, job_run_id: uuid.UUID) -> str | None:
        """The status of a run, or None if there is no such run."""
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text("SELECT job_run_status_code FROM jobs.job_run WHERE job_run_id = :id"),
                    {"id": job_run_id},
                )
            ).first()
        return None if row is None else row.job_run_status_code

    async def unknown_kinds(
        self, producer: str, reference_kinds: Sequence[str]
    ) -> tuple[bool, list[str]]:
        """Whether the producer is unregistered, and which of the reference kinds are."""
        async with self._engine.connect() as conn:
            producer_known = (
                await conn.execute(
                    text("SELECT 1 FROM jobs.producer_kind WHERE code = :code"), {"code": producer}
                )
            ).first() is not None
            known = {
                row[0]
                for row in await conn.execute(
                    text("SELECT code FROM jobs.reference_kind WHERE code = ANY(:codes)"),
                    {"codes": list(reference_kinds)},
                )
            }
        return not producer_known, [kind for kind in reference_kinds if kind not in known]

    async def cancel_run(self, job_run_id: uuid.UUID, actor: str | None = None) -> CancelResult:
        """Cancel the tasks of a run that have not started; ask running ones to stop.

        The actor, when given, is recorded in the detail of the events written.
        """
        detail = _actor_detail(actor) or None  # no actor: no detail, as before
        async with self._engine.begin() as conn:
            cancelled = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET task_status_code = 'cancelled', "
                        "finished_at = now(), lease_expires_at = NULL "
                        "WHERE job_run_id = :job_run_id "
                        "AND task_status_code IN ('planned', 'queued', 'retrying') "
                        "RETURNING task_instance_id, attempt"
                    ),
                    {"job_run_id": job_run_id},
                )
            ).all()
            requested = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET cancel_requested_at = now() "
                        "WHERE job_run_id = :job_run_id AND task_status_code = 'running' "
                        "AND cancel_requested_at IS NULL "
                        "RETURNING task_instance_id, attempt"
                    ),
                    {"job_run_id": job_run_id},
                )
            ).all()
            for row in cancelled:
                await self._add_event(
                    conn, row.task_instance_id, "cancelled", attempt=row.attempt, detail=detail
                )
            for row in requested:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "cancel_requested",
                    attempt=row.attempt,
                    detail=detail,
                )
            return CancelResult(
                cancelled=[row.task_instance_id for row in cancelled],
                cancel_requested=[row.task_instance_id for row in requested],
            )

    async def retry_run(self, job_run_id: uuid.UUID, actor: str | None = None) -> RetryResult:
        """Resume a failed run in place from its failed tasks.

        The actor, when given, is recorded in the detail of the events written.

        Each failed task is queued again with a new set of attempts: as many as the
        task's own max_attempts label, counted from the attempts already used. Steps
        marked upstream_failed return to planned, and the run is running again.
        Returns the stored messages to send.
        """
        async with self._engine.begin() as conn:
            run = (
                await conn.execute(
                    text(
                        "SELECT job_run_status_code FROM jobs.job_run "
                        "WHERE job_run_id = :job_run_id FOR UPDATE"
                    ),
                    {"job_run_id": job_run_id},
                )
            ).first()
            if run is None:
                return RetryResult("not_found")
            if run.job_run_status_code != "failed":
                return RetryResult("not_failed")

            unsent = (
                await conn.execute(
                    text(
                        "SELECT 1 FROM jobs.task_instance WHERE job_run_id = :job_run_id "
                        "AND task_status_code = 'failed' AND message IS NULL"
                    ),
                    {"job_run_id": job_run_id},
                )
            ).first()
            if unsent is not None:
                return RetryResult("never_sent")

            queued = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET task_status_code = 'queued', "
                        "max_attempts = attempt + COALESCE("
                        "CAST(message -> 'labels' ->> 'max_attempts' AS int), max_attempts), "
                        "queued_at = now(), finished_at = NULL, retry_at = NULL, "
                        "cancel_requested_at = NULL, error_type = NULL, error_detail = NULL, "
                        "error_traceback = NULL WHERE job_run_id = :job_run_id "
                        "AND task_status_code = 'failed' "
                        "RETURNING task_instance_id, message, attempt"
                    ),
                    {"job_run_id": job_run_id},
                )
            ).all()
            await conn.execute(
                text(
                    "UPDATE jobs.task_instance SET task_status_code = 'planned', finished_at = NULL "
                    "WHERE job_run_id = :job_run_id AND task_status_code = 'upstream_failed'"
                ),
                {"job_run_id": job_run_id},
            )
            await conn.execute(
                text(
                    "UPDATE jobs.job_run SET job_run_status_code = 'running', finished_at = NULL "
                    "WHERE job_run_id = :job_run_id"
                ),
                {"job_run_id": job_run_id},
            )
            for row in queued:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "requeued",
                    attempt=row.attempt,
                    detail={"reason": "run_retry", **_actor_detail(actor)},
                )
            return RetryResult("retried", [Resend(row.task_instance_id, row.message) for row in queued])

    async def register_worker(self, worker_id: str, queue_name: str) -> None:
        """Record a worker process as alive. A worker must be registered before it claims."""
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO jobs.worker (worker_id, queue_name, worker_status_code) "
                    "VALUES (:worker_id, :queue_name, 'alive') "
                    "ON CONFLICT (worker_id) DO UPDATE SET queue_name = EXCLUDED.queue_name, "
                    "worker_status_code = 'alive', started_at = now(), last_seen_at = now(), "
                    "stopped_at = NULL"
                ),
                {"worker_id": worker_id, "queue_name": queue_name},
            )

    async def touch_worker(self, worker_id: str) -> bool:
        """Show a worker is alive; revives one marked dead. False if unknown or stopped."""
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "UPDATE jobs.worker SET last_seen_at = now(), "
                        "worker_status_code = 'alive' WHERE worker_id = :worker_id "
                        "AND worker_status_code <> 'stopped' RETURNING 1"
                    ),
                    {"worker_id": worker_id},
                )
            ).first()
        return row is not None

    async def stop_worker(self, worker_id: str) -> list[uuid.UUID]:
        """Mark a worker stopped and expire the leases of its running tasks.

        Returns the ids of those tasks; the reaper takes them back.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE jobs.worker SET worker_status_code = 'stopped', stopped_at = now() "
                    "WHERE worker_id = :worker_id"
                ),
                {"worker_id": worker_id},
            )
            released = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET lease_expires_at = now() "
                        "WHERE worker_id = :worker_id AND task_status_code = 'running' "
                        "RETURNING task_instance_id"
                    ),
                    {"worker_id": worker_id},
                )
            ).all()
        return [row.task_instance_id for row in released]

    async def reap_expired_leases(self, limit: int) -> list[RecoveredTask]:
        """Take back running tasks whose lease has expired.

        With attempts left a task becomes retrying and is due at once; on its last
        attempt it fails with LeaseExpired.
        """
        error_type = "LeaseExpired"
        error_detail = "the worker stopped sending heartbeats"
        async with self._engine.begin() as conn:
            taken = (
                await conn.execute(
                    text(
                        "WITH due AS (SELECT task_instance_id FROM jobs.task_instance "
                        "WHERE task_status_code = 'running' AND lease_expires_at < now() "
                        "ORDER BY lease_expires_at LIMIT :limit FOR UPDATE SKIP LOCKED) "
                        "UPDATE jobs.task_instance t SET "
                        "task_status_code = CASE WHEN t.attempt < t.max_attempts "
                        "THEN 'retrying' ELSE 'failed' END, "
                        "retry_at = CASE WHEN t.attempt < t.max_attempts THEN now() END, "
                        "finished_at = CASE WHEN t.attempt < t.max_attempts THEN NULL "
                        "ELSE now() END, "
                        "lease_expires_at = NULL, error_type = :error_type, "
                        "error_detail = :error_detail "
                        "FROM due WHERE t.task_instance_id = due.task_instance_id "
                        "RETURNING t.task_instance_id, t.job_run_id, t.step_index, t.attempt, "
                        "t.worker_id, t.task_status_code"
                    ),
                    {"limit": limit, "error_type": error_type, "error_detail": error_detail},
                )
            ).all()
            for row in taken:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "lost",
                    attempt=row.attempt,
                    worker_id=row.worker_id,
                    detail={"next": row.task_status_code},
                )
                if row.task_status_code == "failed":
                    await self._add_event(
                        conn,
                        row.task_instance_id,
                        "failed",
                        attempt=row.attempt,
                        worker_id=row.worker_id,
                        detail={"error_type": error_type, "error": error_detail},
                    )
            return [
                RecoveredTask(
                    row.task_instance_id,
                    row.job_run_id,
                    row.step_index,
                    failed=row.task_status_code == "failed",
                )
                for row in taken
            ]

    async def take_due_retries(self, limit: int) -> list[Resend]:
        """Queue retrying tasks whose retry time has come; returns the messages to send."""
        async with self._engine.begin() as conn:
            taken = (
                await conn.execute(
                    text(
                        "WITH due AS (SELECT task_instance_id FROM jobs.task_instance "
                        "WHERE task_status_code = 'retrying' AND retry_at <= now() "
                        "AND message IS NOT NULL ORDER BY retry_at LIMIT :limit "
                        "FOR UPDATE SKIP LOCKED) "
                        "UPDATE jobs.task_instance t SET task_status_code = 'queued', "
                        "queued_at = now(), retry_at = NULL "
                        "FROM due WHERE t.task_instance_id = due.task_instance_id "
                        "RETURNING t.task_instance_id, t.message, t.attempt"
                    ),
                    {"limit": limit},
                )
            ).all()
            for row in taken:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "requeued",
                    attempt=row.attempt,
                    detail={"reason": "retry_due"},
                )
            return [Resend(row.task_instance_id, row.message) for row in taken]

    async def take_queued(
        self,
        queue_name: str,
        older_than_seconds: float | None = None,
        actor: str | None = None,
    ) -> list[Resend]:
        """Restamp the queued tasks of a queue as queued now; returns their messages to send.

        Only tasks queued at least older_than_seconds ago are taken when it is given. The actor,
        when given, is recorded in the detail of the events written.
        """
        async with self._engine.begin() as conn:
            taken = (
                await conn.execute(
                    text(
                        "WITH waiting AS (SELECT task_instance_id FROM jobs.task_instance "
                        "WHERE task_status_code = 'queued' AND queue_name = :queue_name "
                        "AND message IS NOT NULL AND (CAST(:older_than AS double precision) IS NULL "
                        "OR queued_at <= now() - make_interval(secs => "
                        "CAST(:older_than AS double precision))) "
                        "ORDER BY queued_at FOR UPDATE SKIP LOCKED) "
                        "UPDATE jobs.task_instance t SET queued_at = now() "
                        "FROM waiting WHERE t.task_instance_id = waiting.task_instance_id "
                        "RETURNING t.task_instance_id, t.message, t.attempt"
                    ),
                    {"queue_name": queue_name, "older_than": older_than_seconds},
                )
            ).all()
            for row in taken:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "requeued",
                    attempt=row.attempt,
                    detail={"reason": "manual", **_actor_detail(actor)},
                )
            return [Resend(row.task_instance_id, row.message) for row in taken]

    async def return_to_retrying(self, task_instance_id: uuid.UUID) -> bool:
        """Undo take_due_retries for a task whose message could not be sent.

        The task goes back to retrying, due at once, so the next pass sends it again.
        """
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "UPDATE jobs.task_instance SET task_status_code = 'retrying', "
                        "retry_at = now() WHERE task_instance_id = :task_instance_id "
                        "AND task_status_code = 'queued' RETURNING 1"
                    ),
                    {"task_instance_id": task_instance_id},
                )
            ).first()
        return row is not None

    async def fail_lost_handoffs(self, stall_seconds: float, limit: int) -> list[RecoveredTask]:
        """Fail planned steps whose previous step succeeded long ago but which were never sent."""
        error_type = "HandoffLost"
        error_detail = "the previous step succeeded but this step was never sent"
        async with self._engine.begin() as conn:
            taken = (
                await conn.execute(
                    text(
                        "WITH due AS (SELECT t.task_instance_id FROM jobs.task_instance t "
                        "JOIN jobs.task_instance previous "
                        "ON previous.job_run_id = t.job_run_id "
                        "AND previous.step_index = t.step_index - 1 "
                        "WHERE t.task_status_code = 'planned' "
                        "AND previous.task_status_code = 'succeeded' "
                        "AND previous.finished_at < now() - "
                        "make_interval(secs => CAST(:stall_seconds AS double precision)) "
                        "ORDER BY previous.finished_at LIMIT :limit "
                        "FOR UPDATE OF t SKIP LOCKED) "
                        "UPDATE jobs.task_instance t SET task_status_code = 'failed', "
                        "finished_at = now(), error_type = :error_type, "
                        "error_detail = :error_detail "
                        "FROM due WHERE t.task_instance_id = due.task_instance_id "
                        "RETURNING t.task_instance_id, t.job_run_id, t.step_index"
                    ),
                    {
                        "stall_seconds": stall_seconds,
                        "limit": limit,
                        "error_type": error_type,
                        "error_detail": error_detail,
                    },
                )
            ).all()
            for row in taken:
                await self._add_event(
                    conn,
                    row.task_instance_id,
                    "failed",
                    attempt=0,
                    detail={"error_type": error_type, "error": error_detail},
                )
            return [
                RecoveredTask(row.task_instance_id, row.job_run_id, row.step_index, failed=True)
                for row in taken
            ]

    async def mark_dead_workers(self, stale_seconds: float) -> list[str]:
        """Mark alive workers not seen for stale_seconds as dead; returns their ids."""
        async with self._engine.begin() as conn:
            marked = (
                await conn.execute(
                    text(
                        "UPDATE jobs.worker SET worker_status_code = 'dead' "
                        "WHERE worker_status_code = 'alive' AND last_seen_at < now() - "
                        "make_interval(secs => CAST(:stale_seconds AS double precision)) "
                        "RETURNING worker_id"
                    ),
                    {"stale_seconds": stale_seconds},
                )
            ).all()
        return [row.worker_id for row in marked]

    async def _end_attempt(
        self,
        task_instance_id: uuid.UUID,
        attempt: int,
        assignments: str,
        params: dict[str, Any],
        event_code: str,
        detail: dict[str, Any] | None,
    ) -> bool:
        async with self._engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        f"UPDATE jobs.task_instance SET lease_expires_at = NULL, {assignments} "
                        "WHERE task_instance_id = :task_instance_id "
                        "AND task_status_code = 'running' AND attempt = :attempt "
                        "RETURNING worker_id"
                    ),
                    {"task_instance_id": task_instance_id, "attempt": attempt, **params},
                )
            ).first()
            if row is None:
                return False
            await self._add_event(
                conn,
                task_instance_id,
                event_code,
                attempt=attempt,
                worker_id=row.worker_id,
                detail=detail,
            )
            return True

    @staticmethod
    async def _add_event(
        conn: AsyncConnection,
        task_instance_id: uuid.UUID,
        task_event_code: str,
        attempt: int | None = None,
        worker_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        await conn.execute(
            text(
                "INSERT INTO jobs.task_event (task_instance_id, task_event_code, attempt, "
                "worker_id, detail) VALUES (:task_instance_id, :task_event_code, :attempt, "
                ":worker_id, CAST(:detail AS jsonb))"
            ),
            {
                "task_instance_id": task_instance_id,
                "task_event_code": task_event_code,
                "attempt": attempt,
                "worker_id": worker_id,
                "detail": None if detail is None else json.dumps(detail),
            },
        )
