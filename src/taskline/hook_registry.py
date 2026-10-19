import inspect
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from taskline.lineage import LineageContext

log = logging.getLogger(__name__)

EVENTS = (
    "task_queued",
    "task_started",
    "task_succeeded",
    "task_failed",
    "task_retrying",
    "task_cancelled",
    "run_finished",
)


@dataclass(frozen=True)
class HookPayload:
    event: str
    lineage: LineageContext
    error_type: str | None = None  # task_failed, task_retrying
    error_detail: str | None = None  # task_failed, task_retrying
    run_status: str | None = None  # run_finished


Hook = Callable[[HookPayload], Awaitable[None] | None]


class HookRegistry:
    """Functions the host registers to run at task events.

    Hooks run inline, in registration order: those for every task first, then those
    for the task named in the payload's lineage. A hook that raises is logged and
    skipped; it never changes a task's outcome.
    """

    def __init__(self) -> None:
        self._hooks: dict[tuple[str, str | None], list[Hook]] = defaultdict(list)

    def on(self, event: str, hook: Hook, task: str | None = None) -> None:
        if event not in EVENTS:
            raise ValueError(f"unknown hook event {event!r}; expected one of {EVENTS}")
        self._hooks[(event, task)].append(hook)

    async def fire(self, payload: HookPayload) -> None:
        task = payload.lineage.task_name
        for hook in (*self._hooks[(payload.event, None)], *self._hooks[(payload.event, task)]):
            try:
                outcome = hook(payload)
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception:
                log.exception("hook %r failed on %s", hook, payload.event)


hooks = HookRegistry()
