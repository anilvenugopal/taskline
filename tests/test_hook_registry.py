import logging
import uuid

import pytest

from taskline.hook_registry import EVENTS, HookPayload, HookRegistry
from taskline.lineage import LineageContext


def make_lineage(task_name: str = "parse_pdf") -> LineageContext:
    return LineageContext(
        job_run_id=uuid.uuid7(),
        task_instance_id=uuid.uuid7(),
        step_index=1,
        step_count=3,
        task_name=task_name,
        attempt=1,
        max_attempts=3,
        correlation_id="upload-42",
        producer_code="test.producer",
        worker_id="worker-1",
    )


async def test_hooks_for_every_task_run_before_hooks_for_one_task():
    registry = HookRegistry()
    calls = []
    registry.on("task_started", lambda p: calls.append("one task"), task="parse_pdf")
    registry.on("task_started", lambda p: calls.append("every task"))

    await registry.fire(HookPayload("task_started", make_lineage("parse_pdf")))

    assert calls == ["every task", "one task"]


async def test_a_hook_for_one_task_does_not_run_for_another_task():
    registry = HookRegistry()
    calls = []
    registry.on("task_started", lambda p: calls.append("ran"), task="parse_pdf")

    await registry.fire(HookPayload("task_started", make_lineage("fetch_source")))

    assert calls == []


async def test_sync_and_async_hooks_both_run_in_registration_order():
    registry = HookRegistry()
    calls = []

    async def async_hook(payload):
        calls.append("async")

    registry.on("task_succeeded", lambda p: calls.append("sync"))
    registry.on("task_succeeded", async_hook)

    await registry.fire(HookPayload("task_succeeded", make_lineage()))

    assert calls == ["sync", "async"]


async def test_a_hook_that_raises_is_logged_and_the_others_still_run(caplog):
    registry = HookRegistry()
    calls = []

    def broken(payload):
        raise RuntimeError("boom")

    registry.on("task_failed", broken)
    registry.on("task_failed", lambda p: calls.append("after"))

    with caplog.at_level(logging.ERROR, logger="taskline.hook_registry"):
        await registry.fire(HookPayload("task_failed", make_lineage()))

    assert calls == ["after"]
    assert "failed on task_failed" in caplog.text


def test_an_unknown_event_is_refused():
    with pytest.raises(ValueError, match="unknown hook event"):
        HookRegistry().on("task_exploded", lambda p: None)


async def test_the_payload_carries_the_event_lineage_and_error():
    registry = HookRegistry()
    received = []
    registry.on("task_failed", received.append)
    lineage = make_lineage()

    await registry.fire(HookPayload("task_failed", lineage, error_type="ValueError", error_detail="bad"))

    (payload,) = received
    assert payload.event == "task_failed"
    assert payload.lineage is lineage
    assert (payload.error_type, payload.error_detail) == ("ValueError", "bad")


def test_every_event_can_be_registered():
    registry = HookRegistry()
    for event in EVENTS:
        registry.on(event, lambda p: None)
