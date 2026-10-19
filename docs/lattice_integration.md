# What Lattice needs from Taskline

Status: **for the Taskline project to review and plan.** Written 18 October 2026
by the Lattice project, against Taskline as it is in
`strategiq/packages/taskline` today.

## Purpose

Lattice will ship a Taskline UI: an app named `lattice-taskline`. It is the
admin UI in Taskline's FR-22 (runs, run detail, workers), FR-35 (schedules) and
FR-44 (several instances together). That answers Taskline's open question 2:
**the UI is not part of the Taskline package.** It is a separate Lattice app
(Lattice ADR 0006). It can run inside a business host such as strategiq, or on
its own as an Ops console.

Taskline stays its own project and package. This document lists what the
Lattice app needs from it, so Taskline can plan those pieces in its own
architecture and keep the right to change its internals.

**The rule behind every item:** the Lattice app uses only Taskline's
**published contract**, never its internal modules or private tables. Taskline
can then change anything not in the contract without breaking the UI.

Priorities:
- **Must:** the UI can't be built without it.
- **Should:** the UI works without it, but worse.
- **Later:** needed for a later feature.

## How Lattice works (the parts that matter here)

- **Data:** one Postgres database per host. Lattice and each app own a schema;
  Taskline's is `jobs`. Lattice is **sync** today, with SQLAlchemy Core and
  psycopg 3. It will add an async engine for the Taskline app.
- **No ORM:** every SQL statement is a named query in a `.sql` file.
- **Lists:** wrap a SQL query with search, filters, sorting and paging, in the
  database. A list's rows must therefore come from SQL the app can select from.
- **Migrations:** `lattice migrate` applies each app's schema files. An app
  whose library migrates itself (Taskline) has `lattice migrate` call that
  library's migrate function instead.
- **Standards Lattice follows** (Lattice ADR 0011 (`~/projects/lattice/docs/adr/0011-integration-standards.md`, or `docs/adr/0011-integration-standards.md` in the `anilvenugopal/lattice` repository),
  accepted; read it with this document):
  - events between apps follow **CloudEvents** (`type`, `source`, `subject`,
    `id`, `time`, `data`);
  - references to records are **URNs**: `urn:lattice:{app}.{kind}:{id}`;
  - calls between processes are **HTTP + JSON, described with OpenAPI**.

## 1. Packaging and versioning (Must)

| # | Need | Why | Done when |
|---|---|---|---|
| 1.1 | Taskline is installable as its own package, from its own repository or a package index, at Python 3.14 | Lattice pins it like any dependency, outside the strategiq workspace | `uv add taskline@<version>` works in another repository |
| 1.2 | Semantic versioning, and a changelog that marks contract changes | The Lattice app declares `taskline>=X,<Y` | A breaking contract change bumps the major version |
| 1.3 | **A written public contract**: the Python names, SQL views and event types below, listed in one document | So both projects know what may change freely and what may not | `docs/public_contract.md` exists, and a test fails when a listed name or view column disappears |
| 1.4 | psycopg 3 and SQLAlchemy 2.x compatible with Lattice's, with no deprecation warnings under `-W error::DeprecationWarning` | They share one environment; Lattice's tests fail on deprecation warnings | Taskline's tests pass with that flag |

## 2. Reading: SQL views as the read contract (Must)

**Why views, not only Python queries.** Lattice's lists search, filter, sort,
page and count in the database, by wrapping a SQL `SELECT`. Taskline's
`Queries.list_runs` returns a fixed page of 20 with no offset, total count,
sorting or search. Rebuilding those features in Python for every list would
duplicate what Lattice's grid does already.

Read-only **views**, in a dedicated schema such as `taskline_api`, give the UI
everything it needs. They also leave Taskline free to change its tables
underneath. Views are a standard way to publish a database interface.

| # | View | Columns (at least) |
|---|---|---|
| 2.1 | `taskline_api.run` | `run_id`, `job_name`, `status`, `producer`, `correlation_id`, `triggered_by`, `step_count`, `current_step` (index and task name of the running or failed step), `progress_pct` (of the current step), `error_type`, `error_summary` (the failing task's, one line), `attempts_total`, `created_at`, `started_at`, `finished_at`, `duration_seconds`, `schedule_id`, `parent_run_id` |
| 2.2 | `taskline_api.task` | `task_id`, `run_id`, `step_index`, `task_name`, `queue`, `status`, `attempt`, `max_attempts`, `worker_id`, `queued_at`, `started_at`, `finished_at`, `progress_pct`, `progress_note`, `error_type`, `error_detail`, `next_retry_at` |
| 2.3 | `taskline_api.reference` | `run_id`, `step_index` (null: the whole run), `kind`, `external_id`, `relation`, `label`, `url` |
| 2.4 | `taskline_api.event` | `event_id` (**monotonic**, for reading new events since a cursor), `run_id`, `task_id`, `step_index`, `task_name`, `event`, `attempt`, `worker_id`, `detail`, `occurred_at` |
| 2.5 | `taskline_api.queue` | `queue`, `queued`, `running`, `retrying`, `oldest_queued_seconds`, `live_workers` |
| 2.6 | `taskline_api.worker` | `worker_id`, `queue`, `host`, `pid`, `status`, `started_at`, `last_heartbeat_at`, `current_task_id` |
| 2.7 | `taskline_api.schedule` (Later, with FR-35) | `schedule_id`, `job_name`, `cron`, `timezone`, `paused`, `next_run_at`, `last_run_id`, `last_run_status` |

Rules for the views:

- **Stable:** columns are added freely; removing or renaming one is a major
  version change.
- **Timestamps:** `timestamptz`, in UTC.
- **Status values:** a documented, closed set (`queued`, `running`, `succeeded`,
  `failed`, `cancelled`, …).
- **Indexed for the obvious filters:** status, `created_at`, `job_name`,
  `producer`, `correlation_id`, and references by (`kind`, `external_id`). Lists
  on a busy instance must not read every run.
- **Created by Taskline's migrations,** and kept in step with its tables.
- **Readable by a separate database role,** so the UI's connection doesn't
  need write access to `jobs`.

`Queries` can stay for the CLI. The UI reads the views.

## 3. Writing: a Python command API (Must)

Today, cancel, retry and requeue live in `taskline.commands` as CLI functions
that parse arguments and print. The UI needs them as plain async functions,
with results it can show.

| # | Command | Returns |
|---|---|---|
| 3.1 | `cancel_run(run_id, *, actor)` | A result: done, or refused with a reason (not running, already finished) |
| 3.2 | `retry_run(run_id, *, actor)` | A result: done, or refused with a reason (not failed, failed task never sent) |
| 3.3 | `requeue(queue, *, older_than_seconds=None, actor)` | How many were resent, and which could not be sent |
| 3.4 | `submit_job(job_name, values, *, references=(), correlation_id=None, actor)` | The new run id, or a validation error naming the input |
| 3.5 | Schedules (Later, with FR-35): `create`, `pause`, `resume`, `delete` | A result each |

Requirements:

- **In a module of the public contract,** such as `taskline.api`. No
  `argparse`, no printing, no `sys.exit`. The CLI calls these same functions.
- **`actor`** (a string such as `"lattice:user:42"`) is stored in Taskline's
  event log, so an operator's cancel is auditable in Taskline itself. Lattice
  also writes its own audit event.
- **Typed results:** refusals are results with a reason, not exceptions, and
  unexpected failures raise. The UI shows the reason as a message.
- **Connection:** commands accept an `AsyncEngine` (or a URL), so a host can
  share one pool and doesn't need one per call. The same applies to any
  `Queries` the UI uses.

## 4. The job catalogue (Should)

The UI's **Run job** page builds its form from a job's declared inputs, using
Lattice's JSON Schema forms (ADR 0007).

| # | Need |
|---|---|
| 4.1 | `list_jobs()`: every declared job with `name`, `description`, steps (task names and queues), and required reference kinds |
| 4.2 | Typed inputs: each job input has a type, whether it is required, a default and a description, exposed as **JSON Schema** (Pydantic's `model_json_schema()` is fine) |
| 4.3 | A job is known to a process without starting a worker: importing the job modules registers them |

Today a job's inputs are names only, so this is a change to job declaration.

## 5. Events for other apps (Should)

Lattice's Incidents app should open an incident when a job fails. Notifications
will tell people about it. Taskline's hooks fire **inside worker and reaper
processes**, not in the web process where Lattice apps run, so an in-process
hook can't reach them.

| # | Need |
|---|---|
| 5.1 | The run-level outcomes (`run.succeeded`, `run.failed`, `run.cancelled`) are readable from `taskline_api.event` by a monotonic `event_id`. Lattice then reads "events since my last cursor", the standard outbox-reader pattern, in its own process |
| 5.2 | The event fields map one to one onto **CloudEvents**: `type` = `taskline.run.failed`, `source` = the instance, `subject` = the run id, `id` = `event_id`, `time` = `occurred_at`, `data` = the run's references and error summary. A view `taskline_api.cloud_event` with exactly those columns is ideal |
| 5.3 | Events are kept long enough for a reader that was down for a while (say 7 days), and the retention is documented |

## 6. References (Should)

Taskline already stores references (kind, external id, label, url) and finds
runs by reference. To make them Lattice references:

| # | Need |
|---|---|
| 6.1 | Dotted kind codes such as `strategiq.document` work in `jobs.reference_kind`, and a host can register its kinds from code (a function in the contract), so `lattice migrate` can keep them in step with the apps' reference kinds |
| 6.2 | A run's references can be read as Lattice URNs. Either Taskline stores the URN, or the view adds a `urn` column: `urn:lattice:{kind}:{external_id}` |
| 6.3 | Finding runs by reference is indexed (§2) and works for references on any step, not only the run |

## 7. Several instances (Later, FR-44)

The Ops console lists runs from several Taskline databases together.

| # | Need |
|---|---|
| 7.1 | Each instance has a name, readable from `taskline_api.instance` (`name`, `taskline_version`, `schema_version`) |
| 7.2 | Ids are unique per instance (UUIDs already are). Lattice prefixes them with the instance name, so nothing more is needed |
| 7.3 | The views are identical across instances at the same major version |

## 8. Migrations (Must)

| # | Need |
|---|---|
| 8.1 | `taskline.migrate.migrate(url_or_engine)` stays callable from a host, idempotent, with its advisory lock (it is today) |
| 8.2 | Migrations create and update the `taskline_api` views |
| 8.3 | A way to ask the schema version, so Lattice can refuse to start against a database older than the views it expects, with a clear message |

## 9. Testing support (Should)

The Lattice app's tests need Taskline data in known states, without workers or
Valkey.

| # | Need |
|---|---|
| 9.1 | `taskline.testing`: functions that write runs, tasks, references and events in given states (queued, running with progress, failed with an error, retried, cancelled), straight into a migrated database |
| 9.2 | They are part of the public contract, so Lattice's tests don't reach into internals |

## Out of scope for Taskline

These belong to the Lattice app, not to Taskline:

- users, permissions and audit in the UI (Taskline's non-goal stands);
- pages, lists, forms, charts and the stepper or graph views of runs;
- the Ops console host and its connections to several instances;
- turning Taskline events into incidents or notifications.

## Summary for planning

| Priority | Items |
|---|---|
| **Must** | 1 Packaging, versioning and a written contract · 2 Read views (`run`, `task`, `reference`, `event`, `queue`, `worker`) · 3 Command API (`cancel`, `retry`, `requeue`, `submit_job`, with `actor`) · 8 Migrations that maintain the views |
| **Should** | 4 Job catalogue with JSON Schema inputs · 5 CloudEvents-shaped run events with a monotonic id · 6 References as URNs · 9 Test data helpers |
| **Later** | 2.7 and 3.5 Schedules (FR-35) · 7 Instance identity (FR-44) |
