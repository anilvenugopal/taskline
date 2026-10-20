# Taskline: public contract

This is everything another package may rely on. Anything not listed here is internal and may
change in any release. `tests/test_public_contract.py` reads this file and fails when a listed
name, view column or column type disappears or changes, and when a view has a column that is
not listed here.

The contract's version is the package's version.

A heading marked `[planned: step N]` describes something not built yet. The test skips it, and
starts enforcing it when step N removes the marker.

## Rules

- Taskline never imports Lattice or Taskline UI. They use only what is listed here.
- Adding a name, a view or a view column is a minor change. Removing or renaming one, changing
  a column's type, or removing a status value or refusal code is a breaking change.
- Taskline follows semantic versioning. Before 1.0, a minor release may break the contract and
  a patch release never does. The changelog marks every contract change.
- A host that needs a given contract checks `taskline_api.instance.taskline_version`, the version
  that last migrated the database, for example "at least 0.2 and below 0.3".
- Requires Python 3.14 or later and PostgreSQL 18 or later.
- Timestamps are `timestamptz`, in UTC.

## Python names

### taskline

| name | kind |
|---|---|
| Job | class |
| Step | class |
| TaskSpec | class |
| Reference | class |
| NonRetryable | exception |
| TaskCancelled | exception |
| task | decorator |
| submit | async function |
| hooks | registry |
| current_lineage | function |
| cancel_requested | function |
| report_progress | function |

### taskline.api (run time, async)

| name | kind | parameters |
|---|---|---|
| cancel_run | async function | run_id, actor |
| retry_run | async function | run_id, actor |
| requeue | async function | queue, older_than_seconds, actor |
| submit_job | async function | job_name, values, producer, references, correlation_id, actor |
| shutdown | async function | none |
| REFUSAL_CODES | constant | |
| CancelResult | class | ok, refusal, cancelled, cancel_requested |
| RetryResult | class | ok, refusal, resent, unsent |
| RequeueResult | class | ok, refusal, resent, unsent |
| SubmitResult | class | ok, refusal, run_id, problems |
| Refusal | class | code, message |
| InputProblem | class | name, message |

Every command takes an `actor`, an opaque text such as `lattice:user:42`. It is stored in the `detail` of the
events the command writes (`cancel_requested`, `cancelled`, `requeued`), and as `triggered_by` of a submitted
run, which is recorded with trigger `manual`. An empty actor raises `ValueError`.

A request Taskline refuses returns a result with `ok = False` and a `Refusal` (a `code` from "Refusal codes"
below, and a `message` for a person). Anything unexpected raises. `submit_job` checks everything before it
queues anything, and `invalid_inputs` lists each input at fault in `problems`. `retry_run` and `requeue` return
`ok` with the ids of any tasks whose message could not be sent in `unsent`; `requeue` sends them.

The commands use the database and Valkey of the process's settings and keep their connections open between
calls. Call them from one long-lived event loop (an async request handler is ideal), and call `shutdown()` when
the host stops. Calling a command with a fresh `asyncio.run` each time also works, but opens new connections
every time. Use one event loop at a time per process. `cancel_run` needs only Postgres; the others also need Valkey.

### taskline.api catalogue part (sync) [planned: step 12]

| name | kind | parameters |
|---|---|---|
| list_jobs | function (sync) | none |
| load_catalogue | function (sync) | modules |
| JobInfo | class | name, description, steps, required_reference_kinds, input_schema |
| StepInfo | class | index, task_name, queue |

### taskline.setup (deployment time, sync)

| name | kind | parameters |
|---|---|---|
| register_producers | function | url_or_engine, producers |
| register_reference_kinds | function | url_or_engine, kinds |
| schema_version | function | url_or_engine |
| Kind | class | code, name, description |
| SchemaVersion | class | taskline_version, schema_version, instance_name |
| NotMigrated | exception | |

### taskline.migrate

| name | kind | parameters |
|---|---|---|
| migrate | function (sync) | url_or_engine, instance_name, reader_role |

With `reader_role`, that database role is granted `USAGE` on `taskline_api` and `SELECT` on its views,
and nothing else. The role must already exist; Taskline never creates one.

### taskline.catalogue

| name | kind | parameters |
|---|---|---|
| register | function | job |

`register(job)` adds a job to the process's catalogue so `submit_job` can find it by name, and returns the job,
so a module can say `INGEST = register(Job(...))`. Registering the same definition again is harmless; a
different job under the same name raises `DuplicateJob`.

### taskline.testing

| name | kind | parameters |
|---|---|---|
| add_run | function (sync) | url_or_engine, state, job_name, steps, at_step, attempts, max_attempts, queue, producer, trigger, triggered_by, correlation_id, references, parent, parent_step, error_type, error_detail, error_traceback, progress_pct, progress_note, worker, age |
| add_worker | function (sync) | url_or_engine, queue, status, reaper, worker_id |
| clear | function (sync) | url_or_engine |
| RunHandle | class | run_id, task_ids, worker_id |
| STATES | constant | |

These write test data into a migrated database through the same store operations a worker uses, with no
worker or Valkey. They are for synchronous tests; in an async test they raise `RuntimeError`.

`add_run` writes one run in a state about the step `at_step` (from 0): steps before it succeeded, and steps
after it are planned, or upstream_failed after a failure, cancelled after a cancel, succeeded when the whole run
succeeded. The states are those in `STATES`. `attempts` is the attempt number the state belongs to; 2 means the
step was retried once before, and its log shows it. `age` moves the run's creation, queue, start and finish
times, and its events, into the past. The producer and reference kinds are registered if missing. Each call
takes under 0.1 second, so write tens or hundreds of runs, not thousands.

`add_worker` registers a worker (or, with `reaper=True`, a reaper) as `alive`, `stopped` or `dead`.
`clear` removes every run, task, reference, event, worker and schedule, and keeps the registered producers,
reference kinds and the instance.

## Status values

| set | values |
|---|---|
| run status | running, succeeded, failed, cancelled |
| task status | planned, queued, running, retrying, succeeded, failed, cancelled, upstream_failed |
| worker status | alive, stopped, dead |
| worker role | worker, reaper |
| reference relation | subject, produced, caused_by |
| run trigger | api, schedule, manual |
| event | queued, claimed, succeeded, failed, retry_scheduled, lost, requeued, cancel_requested, cancelled, upstream_failed, duplicate_skipped |

A run is never `queued`: it is `running` from the moment it is submitted.

## Refusal codes

| command | codes |
|---|---|
| cancel_run | not_found, not_running |
| retry_run | not_found, not_failed, never_sent |
| requeue | none (it returns what it resent and what it could not send) |
| submit_job | unknown_job, invalid_inputs, missing_references, unknown_producer, unknown_reference_kind |

## Views in schema taskline_api

Read-only. Columns may be added at any time.

### taskline_api.run

| column | type | meaning |
|---|---|---|
| run_id | uuid | the run |
| job_name | text | the job |
| status | text | run status |
| trigger | text | run trigger |
| producer | text | the application component that submitted it |
| correlation_id | text | free-form label, inherited by runs it starts |
| triggered_by | text | the actor or person who triggered it, if any |
| step_count | integer | steps in the job |
| current_step_index | integer | the lowest step that has not succeeded; null when all succeeded |
| current_step_name | text | that step's task name |
| progress_pct | numeric | progress of the current step |
| cancel_requested | boolean | a running task has been asked to stop and has not stopped yet |
| error_type | text | error type of the current step, if it has one |
| error_summary | text | first line of that error, at most 200 characters |
| attempts_total | integer | attempts of all tasks added together |
| created_at | timestamp with time zone | when it was submitted |
| started_at | timestamp with time zone | when its first task started; null until then |
| finished_at | timestamp with time zone | set when the status changes to succeeded, failed or cancelled; null while running |
| duration_seconds | double precision | finished_at minus started_at; null until finished |
| schedule_id | uuid | the schedule that created it, if any |
| parent_run_id | uuid | the run of the task that started it, if any |

### taskline_api.task

| column | type | meaning |
|---|---|---|
| task_id | uuid | the task instance |
| run_id | uuid | its run |
| step_index | integer | position in the job |
| task_name | text | the task |
| queue | text | its queue |
| status | text | task status |
| attempt | integer | attempts started so far |
| max_attempts | integer | attempts allowed |
| worker_id | text | the worker that holds or last held it |
| queued_at | timestamp with time zone | when it was last queued |
| started_at | timestamp with time zone | when it first started |
| finished_at | timestamp with time zone | when it ended |
| heartbeat_at | timestamp with time zone | last heartbeat |
| lease_expires_at | timestamp with time zone | when its lease ends |
| progress_pct | numeric | reported progress |
| progress_note | text | reported note |
| cancel_requested_at | timestamp with time zone | when a cancel was requested |
| error_type | text | last error type |
| error_detail | text | last error message |
| error_traceback | text | last traceback |
| next_retry_at | timestamp with time zone | when a retrying task is due |

### taskline_api.reference

| column | type | meaning |
|---|---|---|
| run_id | uuid | the run |
| step_index | integer | the step; null when the reference is about the whole run |
| kind | text | registered reference kind, for example strategiq.document |
| external_id | text | the host's id |
| relation | text | reference relation |
| label | text | display text |
| url | text | link back to the host |
| created_at | timestamp with time zone | when it was attached |

### taskline_api.event

Shows one run's log. The ids increase but can become visible out of order, so this view is not a
cursor for reading new events.

| column | type | meaning |
|---|---|---|
| event_id | bigint | the event |
| run_id | uuid | its run |
| task_id | uuid | its task |
| step_index | integer | the task's step |
| task_name | text | the task |
| event | text | event |
| attempt | integer | the attempt it belongs to |
| worker_id | text | the worker involved |
| detail | jsonb | extra facts; for commands this holds the actor |
| occurred_at | timestamp with time zone | when it happened |

### taskline_api.queue

One row for each queue with open tasks or live workers. Reapers are not counted as workers.

| column | type | meaning |
|---|---|---|
| queue | text | the queue |
| queued | bigint | tasks waiting |
| running | bigint | tasks running |
| retrying | bigint | tasks waiting for a retry |
| oldest_queued_seconds | double precision | age of the oldest waiting task |
| live_workers | bigint | workers alive on the queue |

### taskline_api.worker

| column | type | meaning |
|---|---|---|
| worker_id | text | the worker |
| role | text | worker role |
| queue | text | the queue it reads; null for a reaper |
| host | text | machine it runs on |
| pid | integer | process id |
| status | text | worker status |
| started_at | timestamp with time zone | when it started |
| last_heartbeat_at | timestamp with time zone | when it last pinged |
| stopped_at | timestamp with time zone | when it stopped cleanly |
| running_tasks | bigint | tasks it holds now |
| current_task_id | uuid | the oldest task it holds; null if none |

### taskline_api.instance

One row.

| column | type | meaning |
|---|---|---|
| name | text | the instance name |
| taskline_version | text | the Taskline version that last ran `taskline migrate` on the database |
| schema_version | text | latest migration file applied |

## Speed of the views

On a database of a million runs, listing, paging, counting and filtering the `run` view by these columns stays
well under a second: `status`, `created_at`, `finished_at`, `job_name`, `producer`, `correlation_id`,
`schedule_id`, `run_id`, and a reference (`taskline_api.reference` by `kind` and `external_id`). Filtering or
sorting by any other column of `run` (the current step, the error, the attempt total, the duration) reads the
tasks of every run and takes over a second at that size; use the `task` view for such questions.

Paging by offset costs about 80 microseconds for each row skipped: page 100 takes about 0.4 seconds at a
million runs. To go deeper, continue after the last row seen, with
`WHERE (created_at, run_id) < (:last_created_at, :last_run_id) ORDER BY created_at DESC, run_id DESC`.

## Reading run outcomes

Taskline publishes no event feed. To learn that runs have finished, read `taskline_api.run` on a
timer for the statuses you care about with `finished_at` inside a window (for example the last
seven days), and keep your own record of the run ids you have handled. Do not rely on "everything
since my last read": a row can become visible late.

## Settings a host sets

| setting | meaning |
|---|---|
| TASKLINE_DATABASE_URL | the instance's Postgres |
| TASKLINE_VALKEY_URL | the instance's Valkey (needed by retry_run, requeue and submit_job) |
| TASKLINE_NAMESPACE | prefix of every Valkey stream and group |
| TASKLINE_INSTANCE_NAME | the name recorded by `taskline migrate`; defaults to the namespace. A host calling `migrate()` passes `instance_name` instead |
| TASKLINE_API_READER_ROLE | database role that may read taskline_api and nothing else |
| TASKLINE_CATALOGUE_MODULES | modules that define task specs and jobs, loaded by load_catalogue |

## Not in the contract

The `jobs` schema and its tables, the `Store`, `Queries`, `Reaper` and middleware classes, the
output of the command line (except `taskline migrate`), log lines, and any name not listed above.
