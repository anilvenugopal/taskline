# Taskline's response to "What Taskline UI needs from Taskline"

Status: **steps 8 to 12 of Taskline's plan are done.** Versions 0.2.0 (commands and views) and 0.3.0 (typed
job inputs and the job listing) are ready; each becomes installable from the git repository when the
maintainer pushes its tag. Written 19 and 20 October 2026 for the Lattice project, in answer to
`docs/lattice_integration.md`.

Taskline accepts almost all of it. This note says what you can use today, what differs from your list and why,
what we need you to do, and what comes next. The authoritative list of everything you may rely on is
[docs/public_contract.md](public_contract.md); Taskline has a test that fails if a listed name, view column or
column type disappears.

## Where each of your items stands

| Your item | Status | What you get |
|---|---|---|
| 1 Packaging, versioning, contract | **Done**; versions 0.2.0 and 0.3.0 are ready and become installable when tagged | A written contract with a test that fails when a listed name or column goes. Semantic versioning and a changelog that marks contract changes. Installs from a git tag. Tests pass with deprecation warnings as errors. Dependencies carry version ceilings |
| 2 Read views | **Done** | Schema `taskline_api`: `run`, `task`, `reference`, `event`, `queue`, `worker`, `instance`. `schedule` arrives with the scheduler |
| 3 Command API | **Done** | Async functions `cancel_run`, `retry_run`, `requeue`, `submit_job` with an `actor`, typed results and refusal codes |
| 4 Job catalogue and typed inputs | **Done** (0.3.0) | Pydantic-model inputs with JSON Schema, `list_jobs()`, `load_catalogue()` |
| 5 Events | **Not provided** (see "What differs") | Poll `taskline_api.run` |
| 6 References | **Done**, with one difference | Dotted kind codes work; `register_reference_kinds`; runs found by reference are indexed. No `urn` column |
| 7 Several instances | **Done** | `taskline_api.instance`: `name`, `taskline_version`, `schema_version` |
| 8 Migrations | **Done** | `migrate()` takes a URL or a synchronous engine; `schema_version()` |
| 9 Testing support | **Done** | `taskline.testing` |

## What you can use today

**Migrate, name the instance, give your role read access, register your names.** All synchronous, safe to repeat,
and meant to run from `lattice migrate`:

```python
from taskline.migrate import migrate
from taskline.setup import Kind, register_producers, register_reference_kinds, schema_version

migrate(engine, instance_name="strategiq", reader_role="taskline_reader")
register_producers(engine, [Kind("taskline_ui.console", "Taskline UI")])
register_reference_kinds(engine, [Kind("strategiq.document", "Document")])
schema_version(engine)  # SchemaVersion(taskline_version, schema_version, instance_name); raises NotMigrated
```

`reader_role` must already exist; Taskline never creates a role. It is granted `USAGE` on `taskline_api` and
`SELECT` on its views, and nothing else. The views run with their owner's rights, so your role needs no access
to Taskline's own tables. Kind codes may contain dots. After upgrading Taskline, **run `migrate` before starting
workers or a reaper**; they read through the views too.

**The views.** Columns follow your list, with these differences:
- `run` has two columns, `current_step_index` and `current_step_name`, instead of one `current_step`.
- Added: `trigger` (api, schedule or manual), `cancel_requested` on `run` and `task`, and on `task` the
  `error_traceback`, `heartbeat_at`, `lease_expires_at` and `cancel_requested_at`.
- `worker` has a `role` (`worker` or `reaper`), so a missing reaper can be seen, and `running_tasks`.
- A run is **never `queued`**: it is `running` from the moment it is submitted. Run statuses are `running`,
  `succeeded`, `failed`, `cancelled`. Task statuses are `planned`, `queued`, `running`, `retrying`, `succeeded`,
  `failed`, `cancelled`, `upstream_failed`.
- Timestamps are `timestamptz` in UTC. The stored job values are not exposed.
- PostgreSQL 18 or later is required (the schema uses the built-in `uuidv7()`).

**Speed.** We measured on 1,000,000 runs, 3,000,000 tasks and 1,000,000 references; each operation must stay under
a second, and a release test enforces it. Median of three, in milliseconds:

| What a list does | Time |
|---|---|
| first page of 50 (newest first) | 1.7 |
| page 100 (skip 5,000 rows) | 442 |
| count of all runs | 38 |
| count by status | 21 |
| page by status, job name, correlation id, or reference | 1.4 to 1.7 |
| count by job name | 26 |
| runs failed in the last seven days (your incident poll) | 135 |
| the `queue` view | 112 |

Fast filters are `status`, `created_at`, `finished_at`, `job_name`, `producer`, `correlation_id`, `schedule_id`,
`run_id` and a reference (by `kind` and `external_id`). **Filtering or sorting by a column worked out from the
tasks** (current step, error, attempt total, duration) reads every run's tasks and takes about 1.2 seconds at that
size; use the `task` view for such questions. **Paging by offset** costs about 80 microseconds per skipped row, so
beyond a few hundred pages continue after the last row seen:
`WHERE (created_at, run_id) < (:last_created_at, :last_run_id) ORDER BY created_at DESC, run_id DESC`.

**Commands.** Plain async functions in `taskline.api`; none prints or exits. A request Taskline refuses comes back as
a result with `ok = False` and a refusal (a `code` and a `message` for a person); anything unexpected raises.

```python
from taskline.api import cancel_run, retry_run, requeue, submit_job, shutdown
from taskline import Reference
from taskline.catalogue import register  # a job is found by name once registered

result = await submit_job(
    "ingest_document", {"url": "https://example.test/a.pdf"},
    producer="taskline_ui.console", references=[Reference("strategiq.document", "doc-1", "subject")],
    correlation_id="batch-7", actor="lattice:user:42",
)
if not result.ok:
    show(result.refusal.message, result.problems)  # problems name each invalid input

await cancel_run(run_id, actor="lattice:user:42")
await retry_run(run_id, actor="lattice:user:42")
await requeue("parse", older_than_seconds=600, actor="lattice:user:42")
await shutdown()  # when your host stops
```

| Command | Result fields | Refusal codes |
|---|---|---|
| `cancel_run(run_id, actor=)` | `ok, refusal, cancelled, cancel_requested` | `not_found`, `not_running` |
| `retry_run(run_id, actor=)` | `ok, refusal, resent, unsent` | `not_found`, `not_failed`, `never_sent` |
| `requeue(queue, older_than_seconds=, actor=)` | `ok, refusal, resent, unsent` | none |
| `submit_job(job_name, values, producer=, references=, correlation_id=, actor=)` | `ok, refusal, run_id, problems` | `unknown_job`, `invalid_inputs`, `missing_references`, `unknown_producer`, `unknown_reference_kind` |

- **The actor** is an opaque text such as `lattice:user:42`. Taskline writes it into the `detail` of the events the
  command produces (`cancel_requested`, `cancelled`, `requeued`; visible in `taskline_api.event.detail`), and as
  `triggered_by` of a submitted run, which is recorded with trigger `manual`. An empty actor raises `ValueError`.
- **`unsent`:** `retry_run` and `requeue` can succeed in the database but fail to put a message on the queue (Valkey
  down). The result is then `ok` with those task ids in `unsent`, and `requeue` sends them.
- **`submit_job` checks everything before it queues anything**, so a refusal leaves nothing behind. `invalid_inputs`
  names each input at fault (see "Jobs and their inputs").
- **Event loops.** The commands keep their Postgres and Valkey connections open between calls, and those belong to the
  event loop that opened them. Call the commands from one long-lived loop (an async request handler is ideal) and
  call `shutdown()` when the host stops. A fresh `asyncio.run` per call also works (a runtime built on an ended
  loop is rebuilt), at the cost of a new connection each time. One event loop at a time per process.
- `cancel_run` needs only Postgres; the others also need Valkey (`TASKLINE_VALKEY_URL`, `TASKLINE_NAMESPACE`).

**Jobs and their inputs (0.3.0).** A job's inputs can be a Pydantic model, which gives each input a type, a
required flag, a default and a description. `list_jobs()` and `load_catalogue()` are plain synchronous functions:

```python
from taskline.api import list_jobs, load_catalogue

load_catalogue()   # imports the modules in TASKLINE_CATALOGUE_MODULES, or load_catalogue(["myapp.jobs"])
for job in list_jobs():
    job.name, job.description
    job.steps                      # StepInfo(index, task_name, queue), index from 0 as in the views' step_index
    job.required_reference_kinds   # a submission must give a reference of each kind
    job.input_schema               # JSON Schema: the form to draw
```

- **Draw the form from `input_schema`.** It is a JSON Schema object: `properties` (each with `type`, `title`, a
  `description` and a `default` where the job declares them; enums and nested types through `$defs`/`$ref`),
  `required`, and `additionalProperties: false`. A job declared with names only has untyped properties (`{}`) and
  every name required, so old jobs still get a plain form.
- **Definition modules.** Keep task specs and jobs in modules that import no task code, name them in
  `TASKLINE_CATALOGUE_MODULES` (or pass them to `load_catalogue`), and a web process knows every job without the code
  that runs it. A module that cannot be imported raises `CatalogueError` naming it. Jobs register with
  `taskline.catalogue.register(job)`, which returns the job.
- **`submit_job` refusals for inputs.** `invalid_inputs` carries `problems`, a list of `InputProblem(name, message)`:
  one for each wrong input, in Pydantic's English message ("Input should be a valid integer…"), "is required and was not
  given" for a missing one, "is not an input of this job" for an extra one, and the name `(inputs)` for a rule across
  several inputs. **The refused values are never repeated** in a message. Show the messages next to the fields.
- **What the job receives** is the model's result: values coerced to the declared types (`"7"` becomes `7`), defaults
  filled in, everything as JSON. The types live on the job, not on the task functions; a task whose own type hints
  disagree still fails when it starts.
- A field with an alias is refused when the job is defined, because a form gives values by field name.

**Test data.** `taskline.testing` writes runs in known states with no worker or Valkey, through the same store
operations a worker uses, so rows and event logs are what Taskline writes. Synchronous, about 80 ms per run:

```python
from datetime import timedelta
from taskline.testing import add_run, add_worker, clear

failed = add_run(engine, state="failed", at_step=1, correlation_id="batch-7")
running = add_run(engine, state="running", progress_pct=60)
old = add_run(engine, state="succeeded", age=timedelta(days=2))
add_worker(engine, queue="parse", status="dead")
clear(engine)  # between tests; keeps registered producers and reference kinds
```
States: `queued`, `running`, `cancel_requested`, `retrying`, `succeeded`, `failed`, `cancelled`. A step can be on a
later attempt (`attempts=2`), a run can have a parent, references and a trigger. The functions refuse to run
inside an async test.

## What differs from your list, and why

1. **No event feed (your item 5).** Taskline publishes no CloudEvents view. A cursor over a database sequence can
   silently miss an event: ids are handed out when a row is written, but become visible when its transaction
   commits, so two runs finishing at once can commit out of order and a reader that has seen the higher id never
   sees the lower one. Instead, your Incidents app reads `taskline_api.run` on a timer:
   ```sql
   SELECT run_id, job_name, error_type, error_summary, finished_at
   FROM taskline_api.run
   WHERE status = 'failed' AND finished_at > now() - interval '7 days'
   ```
   and keeps its own record of the run ids it has handled. Asking "what is the state now" instead of "what is new
   since" means a row that becomes visible late is found on the next read. A feed can be added later without
   breaking you if a real need appears. (`taskline_api.event` is one run's log; its ids are not a safe cursor.)
2. **No `urn` column (your 6.2).** `urn:lattice:` is Lattice's vocabulary, and Taskline knows nothing of Lattice.
   Build `'urn:lattice:' || kind || ':' || external_id` in your own query; our kinds are already dotted and
   app-prefixed.
3. **`submit_job` takes a required `producer`.** Taskline requires every run to name its producer. Register Taskline
   UI's own (`register_producers`, above). Runs you submit are recorded with trigger `manual`.
4. **One version number.** There is no separate contract version: the package version is the contract's version.
   Until 1.0, a minor release may break the contract and a patch release never does, so pin `taskline>=0.2,<0.3`.
   `taskline_api.instance.taskline_version` is the version that last migrated the database, so Lattice can refuse to
   start against a database older than it needs.
5. **Commands use Taskline's own connection, not yours.** They read `TASKLINE_DATABASE_URL`,
   `TASKLINE_VALKEY_URL` and `TASKLINE_NAMESPACE` from the process's environment, and open their own pool. Your
   pool is synchronous and ours is asynchronous, so they cannot be shared. `retry_run`, `requeue` and `submit_job`
   also need Valkey; `cancel_run` needs only Postgres. Acting on several instances from one console is deferred;
   if needed we would add a small service per instance.
6. **Your speed concern was right, and we changed the design.** Our first `run` view took 1.2 seconds to count all
   runs at a million. It is now 38 ms. Storing the computed values on the run row, which you suggested as the
   fallback, turned out not to be needed.

## What we need from you

- **Pin** `taskline>=0.3,<0.4` for typed inputs and the job listing (`>=0.2,<0.3` has the views and commands only).
  Until a version's tag is pushed, install from the git repository.
- **Check your environment resolves** with Taskline's dependency ranges (`taskiq>=0.13,<0.14`,
  `taskiq-redis>=1.2,<2`, `taskiq-pipelines>=0.1.4,<0.2`, `redis>=8,<9`, `pydantic>=2,<3`,
  `pydantic-settings>=2,<3`, `sqlalchemy>=2.0`, `psycopg>=3.2`) and tell us if a ceiling conflicts.
- **Call the commands from one long-lived event loop** (an async request handler is ideal), and call
  `taskline.api.shutdown()` when your host stops. We found that a command called with a fresh `asyncio.run(...)`
  each time could fail its Valkey send on the second call; that is fixed in 0.2.0, but a single loop is still the
  cheaper and recommended way.
- **Read through the views** with a role granted by `migrate(reader_role=...)`. Do not read the `jobs` tables.
- **Write your incident logic as a poll** (change 1), and build URNs in your own queries (change 2).

## Please correct on your side

- ADR 0006 still names `lattice-taskline`, says the UI uses `taskline.queries`, and lists events `job.failed` and
  `job.succeeded`. The agreed design is `taskline-ui`, the `taskline_api` views, and polling `run`.
- ADR 0011 gives the CloudEvents `source` as `/taskline/{instance}`; with no event feed from Taskline this no
  longer applies to it.
- Your copy, `docs/integrations/taskline-requirements.md`, is an older version of `docs/lattice_integration.md`
  than the one in Taskline's repository.

## What comes next

| Step | What | You can then |
|---|---|---|
| 11 (done) | `cancel_run`, `retry_run`, `requeue`, `submit_job`; **version 0.2.0 prepared** | Build the whole UI against a released version, once tagged |
| 12 (done) | Job catalogue and typed inputs (`list_jobs()`, JSON Schema); version 0.3.0 | Draw the Run job form |
| 13 | The scheduler, and the `schedule` view and schedule commands | Build the schedules page |

You can build the read side and the actions now, against the views, the commands and `taskline.testing`.

## Where to read more

- [docs/public_contract.md](public_contract.md): every name, view column and type, status value and refusal code.
- [CHANGELOG.md](../CHANGELOG.md): every contract change, marked.
- [docs/architecture.md](architecture.md) sections 10.2, 11.2 and 11.3: why the views and commands are shaped this way.
