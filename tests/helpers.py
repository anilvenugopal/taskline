import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path

from redis import Redis
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


def message_for(task_id, name: str = "echo") -> str:
    """A stored taskiq message for a task, as the Store keeps it."""
    return json.dumps(
        {
            "task_id": str(task_id),
            "task_name": name,
            "labels": {"queue_name": "work", "max_attempts": "3"},
            "args": [],
            "kwargs": {},
        }
    )


def stream_task_ids(namespace: str) -> list[str]:
    """The task ids of the messages on the 'work' stream of a Valkey namespace, in order."""
    client = Redis.from_url(get_config().valkey_url)
    try:
        entries = client.xrange(f"{namespace}:work")
    finally:
        client.close()
    return [json.loads(fields[b"data"])["task_id"] for _, fields in entries]


def set_task(url: str, task_id, **columns) -> None:
    assignments = ", ".join(f"{name} = :{name}" for name in columns)
    rows(
        url,
        f"UPDATE jobs.task_instance SET {assignments} WHERE task_instance_id = :id RETURNING 1",
        id=task_id,
        **columns,
    )


def statuses(url: str) -> list[str]:
    return [r.task_status_code for r in rows(url, "SELECT task_status_code FROM jobs.task_instance ORDER BY step_index")]


def run_status(url: str) -> str:
    return rows(url, "SELECT job_run_status_code FROM jobs.job_run")[0].job_run_status_code


async def failed_run(store, url: str):
    """Step 1 succeeded, step 2 failed with its message stored, step 3 upstream_failed."""
    plan = make_plan(step_count=3)
    await store.register_run(plan)
    for step, status in zip(plan.steps, ["succeeded", "failed", "upstream_failed"], strict=True):
        set_task(url, step.task_instance_id, task_status_code=status, queue_name="work")
    set_task(
        url,
        plan.steps[1].task_instance_id,
        attempt=3,
        max_attempts=3,
        message=message_for(plan.steps[1].task_instance_id),
    )
    await store.refresh_run_status(plan.job_run_id)
    return plan
