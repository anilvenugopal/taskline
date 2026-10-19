import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from taskline.store import TaskInfo

# The queue name under which a reaper registers itself in the worker table.
REAPER_QUEUE = "reaper"


@dataclass(frozen=True)
class QueueCounts:
    queue_name: str
    queued: int
    running: int
    retrying: int
    oldest_queued_seconds: float | None
    live_workers: int


@dataclass(frozen=True)
class RunSummary:
    job_run_id: uuid.UUID
    job_name: str
    status: str
    producer_code: str
    correlation_id: str | None
    triggered_by: str | None
    step_count: int
    created_at: datetime
    finished_at: datetime | None


@dataclass(frozen=True)
class TaskRow:
    task_instance_id: uuid.UUID
    step_index: int
    task_name: str
    queue_name: str
    status: str
    attempt: int
    max_attempts: int
    worker_id: str | None
    started_at: datetime | None
    finished_at: datetime | None
    progress_pct: float | None
    progress_note: str | None
    error_type: str | None
    error_detail: str | None


@dataclass(frozen=True)
class ReferenceRow:
    reference_kind_code: str
    external_id: str
    relation: str
    label: str | None
    url: str | None
    step_index: int | None  # None: the reference is about the whole run


@dataclass(frozen=True)
class EventRow:
    step_index: int
    task_name: str
    event: str
    attempt: int | None
    worker_id: str | None
    detail: dict[str, Any] | None
    occurred_at: datetime


@dataclass(frozen=True)
class RunDetail:
    run: RunSummary
    tasks: list[TaskRow]
    references: list[ReferenceRow]
    events: list[EventRow]


_RUN_COLUMNS = (
    "r.job_run_id, r.job_name, r.job_run_status_code, r.producer_code, r.correlation_id, "
    "r.triggered_by, r.step_count, r.created_at, r.finished_at"
)


def _run_summary(row: Any) -> RunSummary:
    return RunSummary(
        job_run_id=row.job_run_id,
        job_name=row.job_name,
        status=row.job_run_status_code,
        producer_code=row.producer_code,
        correlation_id=row.correlation_id,
        triggered_by=row.triggered_by,
        step_count=row.step_count,
        created_at=row.created_at,
        finished_at=row.finished_at,
    )


class Queries:
    """Read-only questions about the jobs schema. Nothing here changes any row."""

    def __init__(self, database_url: str) -> None:
        self._engine = create_async_engine(database_url, pool_pre_ping=True)

    async def close(self) -> None:
        await self._engine.dispose()

    async def queue_counts(self) -> list[QueueCounts]:
        """For each queue with open tasks or live workers: its load and its live workers."""
        async with self._engine.connect() as conn:
            load = (
                await conn.execute(
                    text(
                        "SELECT queue_name, "
                        "count(*) FILTER (WHERE task_status_code = 'queued') AS queued, "
                        "count(*) FILTER (WHERE task_status_code = 'running') AS running, "
                        "count(*) FILTER (WHERE task_status_code = 'retrying') AS retrying, "
                        "EXTRACT(EPOCH FROM now() - min(queued_at) "
                        "FILTER (WHERE task_status_code = 'queued')) AS oldest_queued_seconds "
                        "FROM jobs.task_instance "
                        "WHERE task_status_code IN ('queued', 'running', 'retrying') "
                        "GROUP BY queue_name"
                    )
                )
            ).all()
            workers = (
                await conn.execute(
                    text(
                        "SELECT queue_name, count(*) AS live FROM jobs.worker "
                        "WHERE worker_status_code = 'alive' AND queue_name <> :reaper "
                        "GROUP BY queue_name"
                    ),
                    {"reaper": REAPER_QUEUE},
                )
            ).all()

        live = {row.queue_name: row.live for row in workers}
        counts = {
            row.queue_name: QueueCounts(
                row.queue_name,
                row.queued,
                row.running,
                row.retrying,
                None if row.oldest_queued_seconds is None else float(row.oldest_queued_seconds),
                live.get(row.queue_name, 0),
            )
            for row in load
        }
        for queue_name, count in live.items():
            counts.setdefault(queue_name, QueueCounts(queue_name, 0, 0, 0, None, count))
        return sorted(counts.values(), key=lambda c: c.queue_name)

    async def list_runs(
        self,
        *,
        status: str | None = None,
        producer: str | None = None,
        job_name: str | None = None,
        correlation_id: str | None = None,
        reference: tuple[str, str] | None = None,
        limit: int = 20,
    ) -> list[RunSummary]:
        """Recent job runs, newest first, narrowed by any of the filters given."""
        conditions: list[str] = []
        params: dict[str, Any] = {"limit": limit}
        if status is not None:
            conditions.append("r.job_run_status_code = :status")
            params["status"] = status
        if producer is not None:
            conditions.append("r.producer_code = :producer")
            params["producer"] = producer
        if job_name is not None:
            conditions.append("r.job_name = :job_name")
            params["job_name"] = job_name
        if correlation_id is not None:
            conditions.append("r.correlation_id = :correlation_id")
            params["correlation_id"] = correlation_id
        if reference is not None:
            conditions.append(
                "EXISTS (SELECT 1 FROM jobs.job_reference f WHERE f.job_run_id = r.job_run_id "
                "AND f.reference_kind_code = :reference_kind AND f.external_id = :reference_id)"
            )
            params["reference_kind"], params["reference_id"] = reference
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        async with self._engine.connect() as conn:
            result = (
                await conn.execute(
                    text(
                        f"SELECT {_RUN_COLUMNS} FROM jobs.job_run r {where} "
                        "ORDER BY r.created_at DESC, r.job_run_id DESC LIMIT :limit"
                    ),
                    params,
                )
            ).all()
        return [_run_summary(row) for row in result]

    async def get_run(
        self, job_run_id: uuid.UUID, include_events: bool = False
    ) -> RunDetail | None:
        """A run with its tasks and references, and optionally its full event log."""
        async with self._engine.connect() as conn:
            run = (
                await conn.execute(
                    text(f"SELECT {_RUN_COLUMNS} FROM jobs.job_run r WHERE r.job_run_id = :id"),
                    {"id": job_run_id},
                )
            ).first()
            if run is None:
                return None
            tasks = (
                await conn.execute(
                    text(
                        "SELECT task_instance_id, step_index, task_name, queue_name, "
                        "task_status_code, attempt, max_attempts, worker_id, started_at, "
                        "finished_at, progress_pct, progress_note, error_type, error_detail "
                        "FROM jobs.task_instance WHERE job_run_id = :id ORDER BY step_index"
                    ),
                    {"id": job_run_id},
                )
            ).all()
            references = (
                await conn.execute(
                    text(
                        "SELECT f.reference_kind_code, f.external_id, f.reference_relation_code, "
                        "f.label, f.url, t.step_index FROM jobs.job_reference f "
                        "LEFT JOIN jobs.task_instance t ON t.task_instance_id = f.task_instance_id "
                        "WHERE f.job_run_id = :id ORDER BY f.created_at, f.job_reference_id"
                    ),
                    {"id": job_run_id},
                )
            ).all()
            events = []
            if include_events:
                events = (
                    await conn.execute(
                        text(
                            "SELECT t.step_index, t.task_name, e.task_event_code, e.attempt, "
                            "e.worker_id, e.detail, e.occurred_at FROM jobs.task_event e "
                            "JOIN jobs.task_instance t ON t.task_instance_id = e.task_instance_id "
                            "WHERE t.job_run_id = :id ORDER BY e.task_event_id"
                        ),
                        {"id": job_run_id},
                    )
                ).all()

        return RunDetail(
            run=_run_summary(run),
            tasks=[
                TaskRow(
                    task_instance_id=row.task_instance_id,
                    step_index=row.step_index,
                    task_name=row.task_name,
                    queue_name=row.queue_name,
                    status=row.task_status_code,
                    attempt=row.attempt,
                    max_attempts=row.max_attempts,
                    worker_id=row.worker_id,
                    started_at=row.started_at,
                    finished_at=row.finished_at,
                    progress_pct=None if row.progress_pct is None else float(row.progress_pct),
                    progress_note=row.progress_note,
                    error_type=row.error_type,
                    error_detail=row.error_detail,
                )
                for row in tasks
            ],
            references=[
                ReferenceRow(
                    reference_kind_code=row.reference_kind_code,
                    external_id=row.external_id,
                    relation=row.reference_relation_code,
                    label=row.label,
                    url=row.url,
                    step_index=row.step_index,
                )
                for row in references
            ],
            events=[
                EventRow(
                    step_index=row.step_index,
                    task_name=row.task_name,
                    event=row.task_event_code,
                    attempt=row.attempt,
                    worker_id=row.worker_id,
                    detail=row.detail,
                    occurred_at=row.occurred_at,
                )
                for row in events
            ],
        )

    async def task_info(self, task_instance_id: uuid.UUID) -> TaskInfo | None:
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT t.task_instance_id, t.job_run_id, t.step_index, t.task_name, "
                        "t.attempt, t.max_attempts, r.step_count, r.correlation_id, "
                        "r.producer_code FROM jobs.task_instance t "
                        "JOIN jobs.job_run r ON r.job_run_id = t.job_run_id "
                        "WHERE t.task_instance_id = :id"
                    ),
                    {"id": task_instance_id},
                )
            ).first()
        if row is None:
            return None
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

    async def task_error(self, task_instance_id: uuid.UUID) -> tuple[str | None, str | None]:
        """The error type and message recorded on a task instance."""
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT error_type, error_detail FROM jobs.task_instance "
                        "WHERE task_instance_id = :id"
                    ),
                    {"id": task_instance_id},
                )
            ).first()
        return (None, None) if row is None else (row.error_type, row.error_detail)

    async def live_reapers(self, within_seconds: float) -> int:
        """How many reapers have pinged within the given time."""
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT count(*) AS live FROM jobs.worker WHERE queue_name = :reaper "
                        "AND worker_status_code = 'alive' AND last_seen_at > now() - "
                        "make_interval(secs => CAST(:within AS double precision))"
                    ),
                    {"reaper": REAPER_QUEUE, "within": within_seconds},
                )
            ).one()
        return row.live
