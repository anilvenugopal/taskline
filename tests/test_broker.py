import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from helpers import rows, wait_until
from redis.asyncio import Redis
from taskiq.receiver import Receiver
from taskiq_pipelines import Pipeline

from taskline.broker import RECOVERY_LOCK_SECONDS, build_broker
from taskline.config import get_config
from taskline.execution import guarded
from taskline.submission import Submission, bound_submission


@pytest.fixture
def config(empty_database_url):
    return get_config().model_copy(
        update={
            "database_url": empty_database_url,
            "namespace": f"t{uuid.uuid4().hex[:8]}",
            "result_ttl_seconds": 60,
            "heartbeat_interval_seconds": 0.1,
        }
    )


@pytest.fixture
async def valkey(config):
    client = Redis.from_url(config.valkey_url)
    yield client
    keys = [key async for key in client.scan_iter(match=f"{config.namespace}:*")]
    if keys:
        await client.delete(*keys)
    await client.aclose()


def define_tasks(broker, ran, with_mystery=True) -> SimpleNamespace:
    def task(**labels):
        return lambda fn: broker.task(task_name=fn.__name__, **labels)(guarded(fn))

    @task(queue_name="parse")
    async def echo():
        ran.append("echo")
        return "ok"

    @task(queue_name="parse")
    async def double(value):
        ran.append(f"double({value})")
        return value * 2

    @task(queue_name="parse")
    async def number():
        ran.append("number")
        return 21

    tasks = SimpleNamespace(echo=echo, double=double, number=number)
    if with_mystery:

        @task(queue_name="parse")
        async def mystery():
            ran.append("mystery")

        tasks.mystery = mystery
    return tasks


@asynccontextmanager
async def producer(store, config):
    broker = build_broker(store, config=config)
    ran = []
    tasks = define_tasks(broker, ran)
    await broker.startup()
    try:
        yield broker, tasks
    finally:
        await broker.shutdown()


@asynccontextmanager
async def worker(store, config, with_mystery=True):
    broker = build_broker(store, config=config, consume_queue="parse", worker_id="worker-1")
    ran = []
    define_tasks(broker, ran, with_mystery)
    broker.is_worker_process = True
    await broker.startup()
    receiver = Receiver(broker, executor=ThreadPoolExecutor(2), max_async_tasks=5, run_startup=False)
    finish = asyncio.Event()
    listening = asyncio.create_task(receiver.listen(finish))
    try:
        yield ran
    finally:
        finish.set()
        await asyncio.wait_for(listening, timeout=15)
        await broker.shutdown()


async def submit(task, *args):
    with bound_submission(Submission(producer_code="test.producer")):
        return await task.kiq(*args)


def task_statuses(url: str) -> list[str]:
    result = rows(url, "SELECT task_status_code FROM jobs.task_instance ORDER BY step_index")
    return [row.task_status_code for row in result]


def all_succeeded(url: str, count: int):
    return lambda: task_statuses(url) == ["succeeded"] * count


async def test_a_message_sent_before_any_worker_exists_is_delivered_to_the_first_worker(
    store, config, valkey, empty_database_url
):
    stream = f"{config.namespace}:parse"
    async with producer(store, config) as (_, tasks):
        await submit(tasks.echo)
        assert await valkey.xlen(stream) == 1
        assert await valkey.xinfo_groups(stream) == []

    async with worker(store, config) as ran:
        await wait_until(all_succeeded(empty_database_url, 1), timeout=15)

    assert ran == ["echo"]


async def test_streams_are_named_with_the_namespace_and_never_the_plain_queue_name(
    store, config, valkey
):
    async with producer(store, config) as (_, tasks):
        await submit(tasks.echo)

    assert await valkey.exists(f"{config.namespace}:parse") == 1
    assert await valkey.exists("parse") == 0


async def test_a_worker_in_another_namespace_never_receives_the_message(
    store, config, valkey, empty_database_url
):
    other = config.model_copy(update={"namespace": f"{config.namespace}x"})
    async with producer(store, config) as (_, tasks):
        await submit(tasks.echo)

    try:
        async with worker(store, other) as ran:
            await asyncio.sleep(2.5)
        assert ran == []
        assert task_statuses(empty_database_url) == ["queued"]
    finally:
        keys = [key async for key in valkey.scan_iter(match=f"{other.namespace}:*")]
        if keys:
            await valkey.delete(*keys)


async def test_acknowledged_messages_are_removed_from_the_stream(
    store, config, valkey, empty_database_url
):
    stream = f"{config.namespace}:parse"
    async with producer(store, config) as (_, tasks):
        await submit(tasks.echo)

    async with worker(store, config):
        await wait_until(all_succeeded(empty_database_url, 1), timeout=15)
        await asyncio.sleep(0.3)

    assert await valkey.xlen(stream) == 0
    assert (await valkey.xpending(stream, f"{config.namespace}:parse"))["pending"] == 0


async def test_a_message_for_an_unknown_task_fails_loudly_and_is_acknowledged(
    store, config, valkey, empty_database_url
):
    stream = f"{config.namespace}:parse"
    async with producer(store, config) as (_, tasks):
        await submit(tasks.mystery)

    async with worker(store, config, with_mystery=False):
        await wait_until(lambda: task_statuses(empty_database_url) == ["failed"], timeout=15)
        await asyncio.sleep(0.3)

    (task,) = rows(empty_database_url, "SELECT error_type, error_detail FROM jobs.task_instance")
    assert task.error_type == "UnknownTask"
    assert "mystery" in task.error_detail
    assert "parse" in task.error_detail
    assert await valkey.xlen(stream) == 0


async def test_a_job_of_two_tasks_runs_end_to_end_over_the_real_stream(
    store, config, empty_database_url
):
    async with producer(store, config) as (broker, tasks):
        with bound_submission(Submission(producer_code="test.producer")):
            await Pipeline(broker, tasks.number).call_next(tasks.double).kiq()

    async with worker(store, config) as ran:
        await wait_until(all_succeeded(empty_database_url, 2), timeout=20)

    assert ran == ["number", "double(21)"]
    (run,) = rows(empty_database_url, "SELECT job_run_status_code FROM jobs.job_run")
    assert run.job_run_status_code == "succeeded"


async def test_results_expire_and_task_ids_are_uuid7(store, config, valkey, empty_database_url):
    async with producer(store, config) as (_, tasks):
        await submit(tasks.echo)
    async with worker(store, config):
        await wait_until(all_succeeded(empty_database_url, 1), timeout=15)

    (task,) = rows(empty_database_url, "SELECT task_instance_id FROM jobs.task_instance")
    assert task.task_instance_id.version == 7
    assert 0 < await valkey.ttl(task.task_instance_id.hex) <= 60


async def test_the_recovery_lock_expires(store, config):
    broker = build_broker(store, config=config)

    assert broker.unacknowledged_lock_timeout == RECOVERY_LOCK_SECONDS == 30
