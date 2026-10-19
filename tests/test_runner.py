import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from helpers import rows, wait_until
from support_specs import ECHO, SLOW

from taskline import submit
from taskline.config import get_config
from taskline.queries import Queries
from taskline.runner import StartupError, check_worker, import_modules
from taskline.runtime import shutdown_runtime

TESTS_DIR = Path(__file__).parent


def fake_task(**labels):
    return SimpleNamespace(labels=labels)


def test_modules_are_imported_and_listed():
    assert import_modules("json, os.path") == ["json", "os.path"]
    assert import_modules("") == []
    assert import_modules(" , ") == []


def test_a_module_that_cannot_be_imported_is_named_in_the_error():
    with pytest.raises(StartupError, match="cannot import module 'no_such_module_anywhere'"):
        import_modules("json,no_such_module_anywhere")


def test_a_worker_serves_the_tasks_declared_for_its_queue():
    broker = SimpleNamespace(
        local_task_registry={
            "parse_pdf": fake_task(queue_name="parse"),
            "fetch_source": fake_task(queue_name="fetch"),
            "split_pages": fake_task(queue_name="parse"),
        }
    )

    assert check_worker(broker, "parse", 1, ["a"]) == ["parse_pdf", "split_pages"]


def test_a_worker_with_no_task_for_its_queue_refuses_to_start():
    broker = SimpleNamespace(local_task_registry={"x": fake_task(queue_name="fetch")})

    with pytest.raises(StartupError, match="no task is declared for queue 'parse'"):
        check_worker(broker, "parse", 1, ["strategiq.jobs"])


def test_a_task_with_a_grace_period_needs_a_queue_that_runs_one_task_at_a_time():
    broker = SimpleNamespace(
        local_task_registry={
            "parse_pdf": fake_task(queue_name="parse", cancel_grace_seconds="600"),
            "split_pages": fake_task(queue_name="parse"),
        }
    )

    assert check_worker(broker, "parse", 1, []) == ["parse_pdf", "split_pages"]
    with pytest.raises(StartupError, match=r"\['parse_pdf'\].*concurrency is 2"):
        check_worker(broker, "parse", 2, [])


def test_a_grace_period_on_another_queue_does_not_matter():
    broker = SimpleNamespace(
        local_task_registry={
            "a": fake_task(queue_name="fetch"),
            "b": fake_task(queue_name="parse", cancel_grace_seconds="600"),
        }
    )

    assert check_worker(broker, "fetch", 4, []) == ["a"]


def test_concurrency_must_be_at_least_one():
    with pytest.raises(StartupError, match="at least 1"):
        check_worker(SimpleNamespace(local_task_registry={}), "q", 0, [])


@pytest.fixture
def processes():
    started = []
    yield started
    for process in started:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)


def start_command(processes, *args: str, **env: str) -> subprocess.Popen:
    """Run `python -m taskline ...` in the tests folder, so it cannot read the package's .env."""
    settings = {"TASKLINE_VALKEY_URL": get_config().valkey_url, **env}
    process = subprocess.Popen(
        [sys.executable, "-m", "taskline", *args],
        cwd=TESTS_DIR,
        env={**os.environ, **settings},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    processes.append(process)
    return process


def worker_states(url: str) -> dict[str, str]:
    result = rows(url, "SELECT queue_name, worker_status_code FROM jobs.worker")
    return {row.queue_name: row.worker_status_code for row in result}


def statuses(url: str) -> list[str]:
    result = rows(url, "SELECT task_status_code FROM jobs.task_instance ORDER BY step_index")
    return [row.task_status_code for row in result]


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--queue", "nothing", "--modules", "support_tasks"], "no task is declared for queue 'nothing'"),
        (
            ["--queue", "work", "--concurrency", "2", "--modules", "support_tasks"],
            "cancel grace period",
        ),
        (["--queue", "work", "--modules", "no_such_module_anywhere"], "cannot import module"),
    ],
)
def test_a_worker_that_cannot_start_exits_with_code_2_and_says_why(
    runtime, store, processes, args, message
):
    process = start_command(processes, "worker", *args)

    output, _ = process.communicate(timeout=30)

    assert process.returncode == 2
    assert message in output


def test_an_invalid_setting_is_reported_in_one_line_naming_the_setting(runtime, store, processes):
    process = start_command(
        processes, "worker", "--queue", "work", "--modules", "support_tasks",
        TASKLINE_RESULT_TTL_SECONDS="soon",
    )

    output, _ = process.communicate(timeout=30)

    assert process.returncode == 2
    assert "the settings are not valid: TASKLINE_RESULT_TTL_SECONDS" in output
    assert "Traceback" not in output


async def test_a_worker_process_runs_a_job_and_stops_cleanly_on_sigterm(
    runtime, store, processes, empty_database_url
):
    worker = start_command(processes, "worker", "--queue", "work", "--modules", "support_tasks")
    await wait_until(lambda: worker_states(empty_database_url).get("work") == "alive", timeout=30)

    await submit(ECHO, text="hello", producer="test.producer")
    await wait_until(lambda: statuses(empty_database_url) == ["succeeded"], timeout=30)

    worker.send_signal(signal.SIGTERM)
    output, _ = await asyncio.to_thread(worker.communicate, None, 30)
    await shutdown_runtime()

    assert worker.returncode == 0
    assert worker_states(empty_database_url)["work"] == "stopped"
    assert "worker for queue 'work'" in output
    assert "'echo'" in output


async def test_a_reaper_process_registers_is_seen_as_alive_and_stops_cleanly(
    runtime, store, processes, empty_database_url
):
    reaper = start_command(processes, "reaper")
    queries = Queries(empty_database_url)
    try:
        await wait_until(lambda: worker_states(empty_database_url).get("reaper") == "alive", timeout=30)
        assert await queries.live_reapers(within_seconds=60) == 1
    finally:
        await queries.close()

    reaper.send_signal(signal.SIGTERM)
    output, _ = await asyncio.to_thread(reaper.communicate, None, 30)

    assert reaper.returncode == 0
    assert worker_states(empty_database_url)["reaper"] == "stopped"
    assert "reaper reaper:" in output


async def test_a_second_stop_signal_exits_at_once_and_leaves_the_running_task_for_the_reaper(
    runtime, store, processes, empty_database_url
):
    worker = start_command(processes, "worker", "--queue", "work", "--modules", "support_tasks")
    await wait_until(lambda: worker_states(empty_database_url).get("work") == "alive", timeout=30)
    await submit(SLOW, producer="test.producer")
    await wait_until(lambda: statuses(empty_database_url) == ["running"], timeout=30)

    worker.send_signal(signal.SIGTERM)
    await asyncio.sleep(1.5)
    assert worker.poll() is None

    worker.send_signal(signal.SIGTERM)
    await asyncio.to_thread(worker.wait, 10)
    await shutdown_runtime()

    assert worker.returncode == 1
    assert statuses(empty_database_url) == ["running"]
