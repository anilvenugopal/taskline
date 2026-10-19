import asyncio
import importlib
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from taskiq.receiver import Receiver

from taskline.config import get_config
from taskline.hook_registry import hooks
from taskline.queries import Queries
from taskline.reaper import Reaper
from taskline.runtime import get_broker, get_store, reset_runtime, shutdown_runtime, start_runtime
from taskline.spec import LABEL_CANCEL_GRACE, LABEL_QUEUE

log = logging.getLogger(__name__)


class StartupError(Exception):
    """A worker or reaper cannot start; the message says why."""


def import_modules(value: str) -> list[str]:
    """Import the comma-separated modules; an import error names the module that failed."""
    names = [name.strip() for name in value.split(",") if name.strip()]
    for name in names:
        try:
            importlib.import_module(name)
        except Exception as error:
            raise StartupError(
                f"cannot import module {name!r}: {type(error).__name__}: {error}"
            ) from error
    return names


def check_worker(broker: Any, queue: str, concurrency: int, modules: list[str]) -> list[str]:
    """Raise StartupError if a worker for this queue cannot run safely; else the tasks it serves."""
    if concurrency < 1:
        raise StartupError("concurrency must be at least 1")
    served = {
        name: task
        for name, task in broker.local_task_registry.items()
        if task.labels.get(LABEL_QUEUE) == queue
    }
    if not served:
        raise StartupError(
            f"no task is declared for queue {queue!r} in the modules loaded "
            f"({', '.join(modules) or 'none'})"
        )
    if concurrency > 1:
        with_grace = sorted(name for name, task in served.items() if LABEL_CANCEL_GRACE in task.labels)
        if with_grace:
            raise StartupError(
                f"tasks {with_grace} have a cancel grace period, so queue {queue!r} must run one "
                f"task at a time, but the concurrency is {concurrency}"
            )
    return sorted(served)


def stop_on_signals(loop: asyncio.AbstractEventLoop) -> asyncio.Event:
    """An event set by the first SIGINT or SIGTERM; a second signal exits at once."""
    stop = asyncio.Event()
    received = 0

    def handle() -> None:
        nonlocal received
        received += 1
        if received == 1:
            log.info("stopping: letting running tasks finish")
            stop.set()
        else:
            log.error("second stop signal: exiting at once")
            os._exit(1)

    for signal_number in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signal_number, handle)
    return stop


async def run_worker(
    queue: str,
    concurrency: int = 1,
    job_modules: str | None = None,
    hook_modules: str | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """Run one worker process for a queue until stopped."""
    os.environ["TASKLINE_CONSUME_QUEUE"] = queue
    get_config.cache_clear()
    reset_runtime()
    config = get_config()

    loaded_hooks = import_modules(config.hook_modules if hook_modules is None else hook_modules)
    loaded = import_modules(config.job_modules if job_modules is None else job_modules)
    broker = get_broker()
    served = check_worker(broker, queue, concurrency, loaded)
    log.info(
        "worker for queue %r (stream %s), concurrency %d, tasks %s, hook modules %s",
        queue, broker.stream_name(queue), concurrency, served, loaded_hooks or "none",
    )

    stop = stop or stop_on_signals(asyncio.get_running_loop())
    broker.is_worker_process = True
    await start_runtime()
    receiver = Receiver(
        broker,
        executor=ThreadPoolExecutor(concurrency),
        max_async_tasks=concurrency,
        run_startup=False,
        wait_tasks_timeout=config.shutdown_wait_seconds,
    )
    try:
        await receiver.listen(stop)
    finally:
        await shutdown_runtime()


async def run_reaper(hook_modules: str | None = None, stop: asyncio.Event | None = None) -> None:
    """Run a reaper until stopped. It loads the hook modules only, never any task code."""
    get_config.cache_clear()
    reset_runtime()
    config = get_config()

    loaded = import_modules(config.hook_modules if hook_modules is None else hook_modules)
    store, broker = get_store(), get_broker()
    queries = Queries(config.database_url)
    await start_runtime()
    reaper = Reaper(store, queries, broker, hooks, config)
    log.info(
        "reaper %s: every %gs, batch %d, hook modules %s",
        reaper.reaper_id, config.reaper_interval_seconds, config.reaper_batch_size, loaded or "none",
    )

    stop = stop or stop_on_signals(asyncio.get_running_loop())
    try:
        await reaper.run(stop)
    finally:
        await queries.close()
        await shutdown_runtime()
