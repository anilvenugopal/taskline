from concurrent.futures import ThreadPoolExecutor

import pytest

from taskline.config import get_config
from taskline.declaration import task
from taskline.errors import DuplicateTask, GracePeriodTooShort
from taskline.runtime import get_broker, get_store, reset_runtime
from taskline.spec import TaskSpec


def declared() -> dict:
    return get_broker().local_task_registry


def test_a_task_is_registered_under_the_spec_name_whatever_the_function_is_called(runtime):
    spec = TaskSpec(name="parse_pdf", queue="parse", max_attempts=3, cancel_grace_seconds=600)

    @task(spec)
    def do_the_parsing(path):
        return path.upper()

    registered = declared()["parse_pdf"]
    assert registered.task_name == "parse_pdf"
    assert "do_the_parsing" not in declared()
    assert do_the_parsing("a.pdf") == "A.PDF"
    assert registered.labels == spec.labels()
    assert registered.labels["queue_name"] == "parse"
    assert registered.labels["cancel_grace_seconds"] == "600"


def test_a_declared_plain_task_can_still_be_called_directly(runtime):
    @task(TaskSpec(name="parse_pdf"))
    def parse(path):
        return path.upper()

    assert parse("a.pdf") == "A.PDF"


async def test_a_declared_async_task_can_still_be_awaited_directly(runtime):
    @task(TaskSpec(name="fetch_source"))
    async def fetch(url):
        return url + "!"

    assert await fetch("http://x") == "http://x!"


def test_declaring_the_same_name_twice_is_refused(runtime):
    @task(TaskSpec(name="parse_pdf"))
    def first(path):
        return path

    registered = declared()["parse_pdf"]

    with pytest.raises(DuplicateTask, match="parse_pdf"):

        @task(TaskSpec(name="parse_pdf"))
        def second(path):
            return path

    assert declared()["parse_pdf"] is registered


def test_a_grace_period_shorter_than_the_lease_length_is_refused_and_nothing_is_registered(runtime):
    with pytest.raises(GracePeriodTooShort) as error:

        @task(TaskSpec(name="parse_pdf", cancel_grace_seconds=30))
        def parse(path):
            return path

    assert "30s" in str(error.value)
    assert "60s" in str(error.value)
    assert "parse_pdf" not in declared()


@pytest.mark.parametrize("grace", [None, 60, 600])
def test_a_grace_period_of_at_least_the_lease_length_or_none_is_accepted(runtime, grace):
    @task(TaskSpec(name="parse_pdf", cancel_grace_seconds=grace))
    def parse(path):
        return path

    assert "parse_pdf" in declared()


def test_the_grace_floor_follows_the_configured_lease_length(monkeypatch, runtime):
    monkeypatch.setenv("TASKLINE_HEARTBEAT_INTERVAL_SECONDS", "10")
    monkeypatch.setenv("TASKLINE_LEASE_INTERVALS", "2")
    get_config.cache_clear()

    @task(TaskSpec(name="long_enough", cancel_grace_seconds=20))
    def long_enough(path):
        return path

    with pytest.raises(GracePeriodTooShort, match="20s"):

        @task(TaskSpec(name="too_short", cancel_grace_seconds=15))
        def too_short(path):
            return path


def test_the_default_broker_and_store_are_each_created_once(runtime):
    assert get_broker() is get_broker()
    assert get_store() is get_store()


def test_sixteen_threads_asking_at_once_still_get_one_broker_and_one_store(runtime):
    with ThreadPoolExecutor(16) as pool:
        brokers = list(pool.map(lambda _: id(get_broker()), range(64)))
        stores = list(pool.map(lambda _: id(get_store()), range(64)))

    assert len(set(brokers)) == 1
    assert len(set(stores)) == 1


def test_resetting_the_runtime_gives_new_ones(runtime):
    broker, store = get_broker(), get_store()

    reset_runtime()

    assert get_broker() is not broker
    assert get_store() is not store
