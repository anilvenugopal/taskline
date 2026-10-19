import asyncio
import os
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from taskiq.receiver import Receiver

from taskline.config import get_config
from taskline.runtime import get_broker, shutdown_runtime, start_runtime
from taskline.store import RunPlan, StepPlan


@asynccontextmanager
async def default_worker():
    """Run a worker in this process on the default broker; shuts the runtime down afterwards."""
    broker = get_broker()
    broker.is_worker_process = True
    await start_runtime()
    receiver = Receiver(broker, executor=ThreadPoolExecutor(2), max_async_tasks=5, run_startup=False)
    finish = asyncio.Event()
    listening = asyncio.create_task(receiver.listen(finish))
    try:
        yield
    finally:
        finish.set()
        await asyncio.wait_for(listening, timeout=15)
        await shutdown_runtime()


def taskline(*args: str) -> subprocess.CompletedProcess:
    """Run `python -m taskline ...` in the tests folder, so it cannot read the package's .env."""
    return subprocess.run(
        [sys.executable, "-m", "taskline", *args],
        cwd=Path(__file__).parent,
        env={**os.environ, "TASKLINE_VALKEY_URL": get_config().valkey_url},
        capture_output=True,
        text=True,
        timeout=60,
    )


def rows(url: str, sql: str, **params):
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            return conn.execute(text(sql), params).all()
    finally:
        engine.dispose()


async def wait_until(condition, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(0.02)


def make_plan(step_count: int = 1, references=(), producer_code: str = "test.producer") -> RunPlan:
    return RunPlan(
        job_run_id=uuid.uuid7(),
        job_name="fetch_source",
        job_trigger_code="api",
        producer_code=producer_code,
        producer_host="host-1",
        triggered_by="anil",
        correlation_id="upload-42",
        steps=[
            StepPlan(uuid.uuid7(), index, f"step_{index}", "default", max_attempts=3)
            for index in range(step_count)
        ],
        references=list(references),
    )


def add_worker(url: str, worker_id: str) -> None:
    rows(
        url,
        "INSERT INTO jobs.worker (worker_id, queue_name, worker_status_code) "
        "VALUES (:id, 'default', 'alive') RETURNING 1",
        id=worker_id,
    )


async def queued_task(store, url: str) -> uuid.UUID:
    """Register a one-step run and queue its task; returns the task id."""
    plan = make_plan()
    await store.register_run(plan)
    task_id = plan.steps[0].task_instance_id
    assert await store.mark_queued(task_id, {"task_name": "step_0"})
    return task_id
