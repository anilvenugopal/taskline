from collections.abc import Callable
from typing import Any

from taskline.config import get_config
from taskline.errors import DuplicateTask, GracePeriodTooShort
from taskline.execution import guarded
from taskline.runtime import get_broker
from taskline.spec import TaskSpec
from taskline.stepping import stepped


def task(spec: TaskSpec) -> Callable[[Callable[..., Any]], Any]:
    """Declare a function as the implementation of the task described by spec.

    The task is registered for workers under spec.name, whatever the function is called.
    The function itself is returned unchanged, so it can still be called directly. Raises
    GracePeriodTooShort or DuplicateTask when the declaration is not allowed, which stops
    the module, and so the worker that loads it, from starting.
    """

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        _check_grace_period(spec)
        broker = get_broker()
        if spec.name in broker.local_task_registry:
            raise DuplicateTask(f"task {spec.name!r} is already declared in this process")
        broker.register_task(guarded(stepped(function)), task_name=spec.name, **spec.labels())
        return function

    return decorate


def _check_grace_period(spec: TaskSpec) -> None:
    if spec.cancel_grace_seconds is None:
        return
    config = get_config()
    lease_seconds = config.heartbeat_interval_seconds * config.lease_intervals
    if spec.cancel_grace_seconds < lease_seconds:
        raise GracePeriodTooShort(
            f"task {spec.name!r}: cancel grace period {spec.cancel_grace_seconds:g}s is "
            f"shorter than the lease length {lease_seconds:g}s"
        )
