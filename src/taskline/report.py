import dataclasses
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from taskline.queries import QueueCounts, RunDetail, RunSummary


def as_json(value: Any) -> str:
    """JSON for dataclasses, lists and dictionaries; ids and times become text."""

    def plain(item: Any) -> Any:
        if dataclasses.is_dataclass(item) and not isinstance(item, type):
            return {field.name: plain(getattr(item, field.name)) for field in dataclasses.fields(item)}
        if isinstance(item, (list, tuple)):
            return [plain(element) for element in item]
        if isinstance(item, dict):
            return {key: plain(element) for key, element in item.items()}
        return item

    return json.dumps(plain(value), default=str, indent=2)


def format_time(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def format_age(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def format_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[str(cell) for cell in row] for row in rows]
    widths = [
        max([len(header)] + [len(row[index]) for row in cells])
        for index, header in enumerate(headers)
    ]
    lines = ["  ".join(header.ljust(width) for header, width in zip(headers, widths, strict=True))]
    lines += ["  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)) for row in cells]
    return "\n".join(line.rstrip() for line in lines)


def format_runs(runs: Sequence[RunSummary]) -> str:
    if not runs:
        return "no runs"
    return format_table(
        ["RUN", "JOB", "STATUS", "PRODUCER", "CORRELATION", "STARTED", "ENDED"],
        [
            [
                run.job_run_id,
                run.job_name,
                run.status,
                run.producer_code,
                run.correlation_id or "-",
                format_time(run.created_at),
                format_time(run.finished_at),
            ]
            for run in runs
        ],
    )


def format_run(detail: RunDetail) -> str:
    run = detail.run
    lines = [
        f"Run {run.job_run_id}  {run.job_name}  {run.status}",
        f"Producer: {run.producer_code}   Triggered by: {run.triggered_by or '-'}   "
        f"Correlation: {run.correlation_id or '-'}",
        f"Started: {format_time(run.created_at)}   Ended: {format_time(run.finished_at)}   "
        f"Steps: {run.step_count}",
        "",
        format_table(
            ["STEP", "TASK", "STATUS", "TRY", "WORKER", "STARTED", "ENDED", "NOTE"],
            [
                [
                    task.step_index + 1,
                    task.task_name,
                    task.status,
                    f"{task.attempt}/{task.max_attempts}",
                    task.worker_id or "-",
                    format_time(task.started_at),
                    format_time(task.finished_at),
                    _note(task),
                ]
                for task in detail.tasks
            ],
        ),
    ]
    if detail.references:
        lines += ["", "References:"]
        for reference in detail.references:
            where = "run" if reference.step_index is None else f"step {reference.step_index + 1}"
            label = f"  {reference.label}" if reference.label else ""
            lines.append(
                f"  {reference.relation}  {reference.reference_kind_code}  "
                f"{reference.external_id}{label}  ({where})"
            )
    if detail.events:
        lines += ["", "Events:"]
        lines.append(
            format_table(
                ["TIME", "STEP", "TASK", "EVENT", "TRY", "WORKER", "DETAIL"],
                [
                    [
                        format_time(event.occurred_at),
                        event.step_index + 1,
                        event.task_name,
                        event.event,
                        "-" if event.attempt is None else event.attempt,
                        event.worker_id or "-",
                        json.dumps(event.detail) if event.detail else "",
                    ]
                    for event in detail.events
                ],
            )
        )
    return "\n".join(lines)


def _note(task: Any) -> str:
    if task.error_type:
        return f"{task.error_type}: {task.error_detail}"
    if task.progress_pct is not None:
        return f"{task.progress_pct:g}%" + (f" {task.progress_note}" if task.progress_note else "")
    return ""


def format_queues(counts: Sequence[QueueCounts], live_reapers: int) -> str:
    reaper = f"reaper: alive ({live_reapers})" if live_reapers else "reaper: NONE ALIVE"
    if not counts:
        return f"no queues have open tasks or live workers\n{reaper}"
    table = format_table(
        ["QUEUE", "WAITING", "RUNNING", "RETRYING", "OLDEST WAITING", "LIVE WORKERS"],
        [
            [
                queue.queue_name,
                queue.queued,
                queue.running,
                queue.retrying,
                format_age(queue.oldest_queued_seconds),
                queue.live_workers,
            ]
            for queue in counts
        ],
    )
    return f"{table}\n{reaper}"
