import argparse
import sys
import uuid

from taskiq import TaskiqMessage

from taskline.config import get_config
from taskline.queries import Queries
from taskline.report import as_json, format_queues, format_run, format_runs
from taskline.runtime import get_broker, get_store, shutdown_runtime, start_runtime
from taskline.store import Resend


def parse_run_id(value: str) -> uuid.UUID | None:
    """The run id, or None after printing why the text is not one."""
    try:
        return uuid.UUID(value)
    except ValueError:
        print(f"taskline: {value!r} is not a run id; give the full id shown by `runs`", file=sys.stderr)
        return None


async def send_messages(resend: list[Resend]) -> list[uuid.UUID]:
    """Send stored messages to their queues; returns the ids of the tasks that could not be sent."""
    unsent: list[uuid.UUID] = []
    broker = get_broker()
    await start_runtime()
    for item in resend:
        try:
            message = TaskiqMessage.model_validate(item.message)
            await broker.kick(broker.formatter.dumps(message))
        except Exception as error:
            print(f"taskline: could not send task {item.task_instance_id}: {error}", file=sys.stderr)
            unsent.append(item.task_instance_id)
    return unsent


async def runs(args: argparse.Namespace) -> int:
    reference = None
    if args.reference is not None:
        kind, separator, external_id = args.reference.partition(":")
        if not separator or not kind or not external_id:
            print("taskline: --reference must look like kind:id", file=sys.stderr)
            return 2
        reference = (kind, external_id)

    queries = Queries(get_config().database_url)
    try:
        found = await queries.list_runs(
            status=args.status,
            producer=args.producer,
            job_name=args.job,
            correlation_id=args.correlation_id,
            reference=reference,
            limit=args.limit,
        )
    finally:
        await queries.close()
    print(as_json(found) if args.json else format_runs(found))
    return 0


async def show(args: argparse.Namespace) -> int:
    run_id = parse_run_id(args.run_id)
    if run_id is None:
        return 2

    queries = Queries(get_config().database_url)
    try:
        detail = await queries.get_run(run_id, include_events=args.events)
    finally:
        await queries.close()
    if detail is None:
        print(f"taskline: no such run: {run_id}", file=sys.stderr)
        return 1
    print(as_json(detail) if args.json else format_run(detail))
    return 0


async def queues(args: argparse.Namespace) -> int:
    config = get_config()
    queries = Queries(config.database_url)
    try:
        counts = await queries.queue_counts()
        live_reapers = await queries.live_reapers(config.dead_worker_seconds)
    finally:
        await queries.close()
    if args.json:
        print(as_json({"queues": counts, "live_reapers": live_reapers}))
    else:
        print(format_queues(counts, live_reapers))
    return 0


async def cancel(args: argparse.Namespace) -> int:
    run_id = parse_run_id(args.run_id)
    if run_id is None:
        return 2

    queries = Queries(get_config().database_url)
    try:
        detail = await queries.get_run(run_id)
    finally:
        await queries.close()
    if detail is None:
        print(f"taskline: no such run: {run_id}", file=sys.stderr)
        return 1
    if detail.run.status != "running":
        print(f"taskline: run {run_id} is already {detail.run.status}; nothing to cancel", file=sys.stderr)
        return 1

    try:
        result = await get_store().cancel_run(run_id)
        await get_store().refresh_run_status(run_id)
    finally:
        await shutdown_runtime()
    print(
        f"cancelled {len(result.cancelled)} tasks, "
        f"asked {len(result.cancel_requested)} running tasks to stop"
    )
    return 0


async def retry(args: argparse.Namespace) -> int:
    run_id = parse_run_id(args.run_id)
    if run_id is None:
        return 2

    try:
        result = await get_store().retry_run(run_id)
        if result.outcome != "retried":
            reasons = {
                "not_found": f"no such run: {run_id}",
                "not_failed": f"run {run_id} has not failed, so it cannot be retried",
                "never_sent": f"run {run_id} has a failed task that was never sent, so it cannot be retried",
            }
            print(f"taskline: {reasons[result.outcome]}", file=sys.stderr)
            return 1
        unsent = await send_messages(result.resend)
    finally:
        await shutdown_runtime()

    if unsent:
        print(
            f"taskline: {len(unsent)} tasks stay queued without a message; "
            "send them with `taskline requeue`",
            file=sys.stderr,
        )
        return 1
    print(f"retried run {run_id}: sent {len(result.resend)} tasks")
    return 0


async def requeue(args: argparse.Namespace) -> int:
    try:
        resend = await get_store().take_queued(args.queue, args.older_than)
        unsent = await send_messages(resend)
    finally:
        await shutdown_runtime()

    print(f"requeue {args.queue}: sent {len(resend) - len(unsent)} of {len(resend)} tasks")
    return 1 if unsent else 0
