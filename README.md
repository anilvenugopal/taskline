# Taskline

Taskline is a job execution engine for Python applications, built on [taskiq](https://taskiq-python.github.io/). It runs background jobs and chains of jobs, and keeps a durable record of every run in Postgres.

How it works is described in [docs/architecture.md](docs/architecture.md). What it must do is in [docs/product_requirement.md](docs/product_requirement.md). Progress is tracked in [docs/product_development_plan.md](docs/product_development_plan.md).

Taskline is under development. This README describes only what exists today: the containers it runs on.

## Requirements

- Python 3.14 and [uv](https://docs.astral.sh/uv/)
- podman and podman-compose

## Start the containers

From this directory:

```
cp .env.example .env
podman-compose up -d
```

This starts Valkey, which carries taskline's messages. Use it when a host application provides the Postgres database.

To run taskline on its own, also start the standalone Postgres:

```
podman-compose --profile standalone up -d
```

## Ports

All ports are published on `127.0.0.1` only. Change them in `.env`.

| Port | Service | Setting |
|---|---|---|
| 6380 | Valkey | `TASKLINE_VALKEY_PORT` |
| 5435 | Standalone Postgres | `TASKLINE_POSTGRES_PORT` |

## Check that it works

```
podman-compose ps
podman-compose exec valkey valkey-cli ping
PGPASSWORD=taskline psql -h 127.0.0.1 -p 5435 -U taskline -d taskline -c "select 1"
```

`ps` should show the services as healthy, and `ping` should answer `PONG`. The `psql` check applies only when the standalone Postgres is running.

## Create the database schema

Taskline keeps its tables in a schema named `jobs`. It reads the database URL from `TASKLINE_DATABASE_URL` in `.env`.

```
uv run taskline migrate
```

This creates the `jobs` schema and a table `jobs._schema_version`, then applies any schema files not yet recorded there. Running it again applies nothing. A host application can do the same from its own code:

```python
from taskline.migrate import migrate

migrate()
```

## Run a worker and a reaper

A worker is one process that reads one queue and runs one task at a time:

```
uv run taskline worker --queue parse --modules myapp.tasks.parse --hook-modules myapp.hooks
```

To run more work at once, start more worker processes. `--concurrency N` runs N tasks at once in one process; it must stay 1 for a queue whose tasks have a cancel grace period. The worker refuses to start, with a one-line message and exit code 2, when a module cannot be imported, when no task is declared for its queue, or when a grace period meets a concurrency above one. Stop it with SIGTERM: it waits up to `TASKLINE_SHUTDOWN_WAIT_SECONDS` for running tasks, and a second signal exits at once.

A reaper takes tasks back from workers that died, sends due retries, fails steps that were never sent, and warns about queues that look wrong. It never touches a queued task. It loads only the hook modules:

```
uv run taskline reaper --hook-modules myapp.hooks
```

The module lists can also be set with `TASKLINE_JOB_MODULES` and `TASKLINE_HOOK_MODULES`.

## Look at runs and queues

```
uv run taskline runs --status failed --producer strategiq.ingest --limit 10
uv run taskline runs --reference document:doc-1
uv run taskline show <full run id> --events
uv run taskline queues
```

`runs` lists job runs newest first (default 20) and can be narrowed with `--status`, `--producer`, `--job`, `--correlation-id` and `--reference kind:id`. `show` takes the full run id as printed by `runs` and prints the run, its tasks and its references; `--events` adds the event log. `queues` prints each queue's waiting, running and retrying tasks, the age of the oldest waiting task, and its live workers, then whether a reaper is alive. All three accept `--json`. Exit codes: 0 success, 1 run not found, 2 invalid argument or setting.

## Change a run

```
uv run taskline cancel <full run id>
uv run taskline retry <full run id>
uv run taskline requeue --queue parse --older-than 600
```

`cancel` cancels the tasks of a running run that have not started and asks running tasks to stop. `retry` resumes a failed run from its failed task, inside the same run: that task gets a new set of attempts equal to its own `max_attempts`, the later tasks are planned again, and tasks that already succeeded are not run again. It refuses a run that has not failed, and a run whose failed task was never sent. If a message cannot be sent, the task stays queued and `requeue` sends it. `requeue` sends the stored messages of a queue's queued tasks again, for example after Valkey was wiped; `--older-than` leaves out tasks queued more recently. A second delivery of a task is harmless, because only one worker can claim it. These commands exit 1 when they cannot do what was asked.

## Run the tests

The tests need the standalone Postgres running. Each test creates its own empty database on that server and drops it afterwards. Run them from this directory so that `.env` is found:

```
uv run pytest
```

## Stop and reset

Stop the containers and keep their data:

```
podman-compose --profile standalone down
```

Stop the containers and delete their data:

```
podman-compose --profile standalone down -v
```
