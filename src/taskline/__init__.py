"""Taskline's public names, loaded on first use so that importing a light module such as
taskline.spec or taskline.job does not import taskiq or the database code."""

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from taskline.context import cancel_requested, report_progress
    from taskline.declaration import task
    from taskline.errors import NonRetryable, TaskCancelled
    from taskline.hook_registry import hooks
    from taskline.job import Job, Step
    from taskline.lineage import current_lineage
    from taskline.spec import TaskSpec
    from taskline.store import ReferencePlan as Reference
    from taskline.submitting import submit

_EXPORTS = {
    "cancel_requested": ("taskline.context", "cancel_requested"),
    "report_progress": ("taskline.context", "report_progress"),
    "task": ("taskline.declaration", "task"),
    "NonRetryable": ("taskline.errors", "NonRetryable"),
    "TaskCancelled": ("taskline.errors", "TaskCancelled"),
    "hooks": ("taskline.hook_registry", "hooks"),
    "Job": ("taskline.job", "Job"),
    "Step": ("taskline.job", "Step"),
    "current_lineage": ("taskline.lineage", "current_lineage"),
    "TaskSpec": ("taskline.spec", "TaskSpec"),
    "Reference": ("taskline.store", "ReferencePlan"),
    "submit": ("taskline.submitting", "submit"),
}

__all__ = [
    "Job",
    "NonRetryable",
    "Reference",
    "Step",
    "TaskCancelled",
    "TaskSpec",
    "cancel_requested",
    "current_lineage",
    "hooks",
    "report_progress",
    "submit",
    "task",
]


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module 'taskline' has no attribute {name!r}")
    module_name, attribute = _EXPORTS[name]
    value = getattr(importlib.import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*globals(), *_EXPORTS])
