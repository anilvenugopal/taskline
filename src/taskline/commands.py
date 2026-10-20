import argparse
import getpass
import sys
import uuid

from taskline import api
from taskline.config import get_config
from taskline.queries import Queries
from taskline.report import as_json, format_queues, format_run, format_runs


def cli_actor() -> str:
    """Who the command line says it is, so its changes can be told apart in the event log."""
    try:
        return f"cli:{getpass.getuser()}"
    except Exception:
        return "cli"


def parse_run_id(value: str) -> uuid.UUID | None:
    """The run id, or None after printing why the text is not one."""
    try:
        return uuid.UUID(value)
    except ValueError:
        print(f"taskline: {value!r} is not a run id; give the full id shown by `runs`", file=sys.stderr)
        return None


def refused(refusal: api.Refusal) -> int:
    print(f"taskline: {refusal.message}", file=sys.stderr)
    return 1


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
    try:
        result = await api.cancel_run(run_id, actor=cli_actor())
    finally:
        await api.shutdown()
    if not result.ok:
        return refused(result.refusal)
    print(f"cancelled {result.cancelled} tasks, asked {result.cancel_requested} running tasks to stop")
    return 0


async def retry(args: argparse.Namespace) -> int:
    run_id = parse_run_id(args.run_id)
    if run_id is None:
        return 2
    try:
        result = await api.retry_run(run_id, actor=cli_actor())
    finally:
        await api.shutdown()
    if not result.ok:
        return refused(result.refusal)
    if result.unsent:
        print(
            f"taskline: {len(result.unsent)} tasks stay queued without a message; "
            "send them with `taskline requeue`",
            file=sys.stderr,
        )
        return 1
    print(f"retried run {run_id}: sent {result.resent} tasks")
    return 0


async def requeue(args: argparse.Namespace) -> int:
    try:
        result = await api.requeue(args.queue, older_than_seconds=args.older_than, actor=cli_actor())
    except ValueError as error:
        print(f"taskline: {error}", file=sys.stderr)
        return 2
    finally:
        await api.shutdown()
    print(f"requeue {args.queue}: sent {result.resent} of {result.resent + len(result.unsent)} tasks")
    return 1 if result.unsent else 0
