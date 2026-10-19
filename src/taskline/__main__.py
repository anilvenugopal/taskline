import argparse
import asyncio
import sys
from typing import NoReturn

from pydantic import ValidationError

from taskline.migrate import migrate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="taskline")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("migrate", help="create or upgrade the jobs schema")

    worker = commands.add_parser("worker", help="run one worker process for a queue")
    worker.add_argument("--queue", required=True, help="the queue to read")
    worker.add_argument("--concurrency", type=int, default=1, help="tasks run at once (default 1)")
    worker.add_argument("--modules", help="comma-separated task modules (default: TASKLINE_JOB_MODULES)")
    worker.add_argument(
        "--hook-modules", help="comma-separated hook modules (default: TASKLINE_HOOK_MODULES)"
    )

    reaper = commands.add_parser("reaper", help="run a reaper")
    reaper.add_argument(
        "--hook-modules", help="comma-separated hook modules (default: TASKLINE_HOOK_MODULES)"
    )

    runs = commands.add_parser("runs", help="list recent job runs, newest first")
    runs.add_argument("--status", help="running, succeeded, failed or cancelled")
    runs.add_argument("--producer", help="the producer that submitted the run")
    runs.add_argument("--job", help="the job's name")
    runs.add_argument("--correlation-id", help="the correlation id")
    runs.add_argument("--reference", help="runs that refer to an object, as kind:id")
    runs.add_argument("--limit", type=int, default=20, help="how many runs (default 20)")
    runs.add_argument("--json", action="store_true", help="print JSON")

    show = commands.add_parser("show", help="show one job run with its tasks and references")
    show.add_argument("run_id", help="the full run id, as shown by `runs`")
    show.add_argument("--events", action="store_true", help="also print the event log")
    show.add_argument("--json", action="store_true", help="print JSON")

    queues = commands.add_parser("queues", help="show each queue's load and live workers")
    queues.add_argument("--json", action="store_true", help="print JSON")

    cancel = commands.add_parser("cancel", help="cancel a running job run")
    cancel.add_argument("run_id", help="the full run id, as shown by `runs`")

    retry = commands.add_parser("retry", help="resume a failed job run from its failed task")
    retry.add_argument("run_id", help="the full run id, as shown by `runs`")

    requeue = commands.add_parser("requeue", help="send the stored messages of a queue's queued tasks again")
    requeue.add_argument("--queue", required=True, help="the queue to resend")
    requeue.add_argument(
        "--older-than",
        type=float,
        metavar="SECONDS",
        help="only tasks that have been queued at least this long",
    )
    return parser


def fail(message: str) -> NoReturn:
    print(f"taskline: {message}", file=sys.stderr)
    sys.exit(2)


def main() -> None:
    args = build_parser().parse_args()

    if args.command == "migrate":
        applied = migrate()
        print("applied:", ", ".join(applied) if applied else "schema is up to date")
        return

    from taskline import commands
    from taskline.logs import configure_logging
    from taskline.runner import StartupError, run_reaper, run_worker

    try:
        if args.command in ("runs", "show", "queues", "cancel", "retry", "requeue"):
            sys.exit(asyncio.run(getattr(commands, args.command)(args)))
        configure_logging()
        if args.command == "worker":
            asyncio.run(run_worker(args.queue, args.concurrency, args.modules, args.hook_modules))
        else:
            asyncio.run(run_reaper(args.hook_modules))
    except StartupError as error:
        fail(str(error))
    except ValidationError as error:
        problems = "; ".join(
            f"TASKLINE_{str(problem['loc'][0]).upper()}: {problem['msg']}" for problem in error.errors()
        )
        fail(f"the settings are not valid: {problems}")


if __name__ == "__main__":
    main()
