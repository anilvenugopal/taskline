# Taskline: Product Requirements

Status: draft 1, 2026-10-06. Owner: Anil Venugopal.

## 1. Purpose

Taskline is a job execution engine for Python applications. It runs background work reliably and keeps a complete, queryable history of what ran, when, on which worker, with what result, and what it depended on. It is built on taskiq and Valkey (Redis-compatible), and stores its record in Postgres.

The first host is StrategIQ, which needs it for ingestion, parsing, extraction and export pipelines. Taskline must be usable by other applications without changes.

Taskline has no user interface of its own. Its admin UI, Taskline UI, is a separate package built on Lattice. It uses only Taskline's public contract (`docs/public_contract.md`).

## 2. Who uses it

| User | What they need |
|---|---|
| Application developer | Declare a task in a few lines, combine tasks into jobs, set retries and timeouts, attach hooks, test tasks without a full stack |
| Operator | See what is running and what failed, cancel or retry a run, know a worker is dead before users complain, run workers and the reaper |
| Analyst or support person | Open a run, see each step, its status, progress, error and full event log, find all work for one correlation id |

## 3. Goals

1. No task is silently lost: a task is either finished, failed with a recorded reason, or being retried.
2. No task runs twice at the same time, even when messages are duplicated or workers freeze.
3. Every run's history is reconstructable from the database alone.
4. Taskline has no dependency on any host application.
5. A person can understand the whole system from its documentation, and diagnose a stuck task without reading code.

## 4. Non-goals (version 1)

- Parallel fan-out steps (map, filter) inside chains.
- Multi-tenant isolation or user permissions. Users and permissions belong to whatever UI sits on top of Taskline.
- Exactly-once execution. Taskline gives at-least-once delivery with at-most-one concurrent execution, and requires any task allowed more than one attempt to be safe to run again.
- The taskiq-admin dashboard. Rejected so there is one record of truth.
- A web UI in this package.
- An event feed or message bus for other applications. They read run state from the published views.

## 5. Functional requirements

Numbering is stable; requirements are referenced from tests and the development plan.

**Tasks, jobs and submission**
- FR-1. A task is either an `async` function or a plain function, registered with a name, queue, maximum attempts (default one) and optional timeout. An `async` task runs on the worker's event loop; a plain task is run by taskline on a helper thread, so long blocking work does not stop the heartbeat. A task that allows more than one attempt accepts being run again after an error or after its worker dies.
- FR-2. A job is one or more steps run in order, each using a task. A job declares named inputs, optionally with a type, required flag, default and description (FR-58). It is defined once by name, registered in a catalogue, and submitted by name with its inputs.
- FR-41. Each step maps named values to its task's parameters, taken from the job's inputs and from the outputs of any earlier step, and declares its output (one value) or outputs (a dictionary of several). Inputs and outputs have unique names. The wiring is checked when the job definition loads, and the inputs given to `submit` are checked at submit time.
- FR-42. A step can have constant arguments in the job definition. A task can be submitted alone as a job of one task, named after it, whose inputs are the arguments given.
- FR-3. Submission can carry a correlation id, trigger (api, schedule, manual) and the person who triggered it. A job started from inside a running task inherits its correlation id and records its parent.
- FR-4. Each queue is isolated: workers consume one queue, so a long task on one queue never delays another.

**Producer and context**
- FR-24. Every run records its producer: a registered application component, plus host, process, and the person who triggered it if any. A submission without a registered producer is refused.
- FR-25. A run, or a single task, can carry external references: a registered kind, the host's external id, an optional label and link, and a relation (subject, produced, caused_by). Unregistered kinds are refused.
- FR-26. A running task can add references to itself (for example the documents it created).
- FR-27. A task can declare required reference kinds; a submission of a job containing that task and missing one is refused before anything is queued.
- FR-28. Runs can be found by producer, and by reference kind and id, in both directions: from a host object to the runs that concern or produced it, and from a run to its objects.
- FR-29. A run started from inside a running task inherits the parent's producer and correlation id unless overridden, and records the parent task.
- FR-30. Hosts register producers and reference kinds through taskline's API; taskline contains no host-specific names.

**Scheduling**
- FR-31. Schedules are stored in Postgres: one job (one task, or several in order), arguments, cron expression or fixed interval, time zone, optional start time and end time, enabled flag, producer and references for the runs it creates.
- FR-38. A schedule can be entered ahead of its first run. Nothing fires before its start time or at or after its end time, and a schedule ends on its own at its end time without anyone changing a flag.
- FR-32. A scheduler process submits due schedules with trigger `schedule`; each created run records its schedule.
- FR-33. A schedule fires at most once per intended time, even with several scheduler processes or restarts (unique key on schedule and fire time).
- FR-34. Each schedule has an overlap policy (`allow`, `skip_if_running`) and a missed-fire policy (`skip`, `run_once`); skipped fires are recorded with the reason.
- FR-35. Schedules can be created, listed, paused, resumed and deleted from the command line and the command functions (FR-56). A job's definition can declare a default schedule that `taskline schedules sync` creates only if absent and never overwrites.
- FR-36. Policies and trigger types are lookup tables, like every other closed value set.

**Lifecycle and recording**
- FR-5. Before a task runs, taskline records its identity, chain position, attempt number, queue, worker and start time.
- FR-6. After a task ends, taskline records the outcome, error type, message and traceback. How long an attempt took is worked out from its events.
- FR-7. Every state change appends an event to a per-task log.
- FR-8. When a chain step fails for good, later steps are marked `upstream_failed` and the run is marked failed. When all steps succeed the run is marked succeeded.
- FR-9. Application code can register hooks for: task queued, started, succeeded, failed, retrying, cancelled, run finished; globally or per task. A failing hook never changes a task's outcome.
- FR-10. Running tasks can read their lineage (run id, task position, attempt, correlation id), and the same fields appear on their log lines.

**Heartbeats and recovery**
- FR-11. A running task holds a lease that a heartbeat renews at a configurable interval. Time comes from the database.
- FR-12. Tasks can report progress (percent and a note), stored with the heartbeat.
- FR-13. Heartbeat and completion writes are fenced by the attempt number; a worker that lost its lease must stop and cannot write results.
- FR-14. The reaper acts on expired leases, due retries, and chain steps whose hand-off was lost. A task whose lease expired is retried if it has attempts left, and fails with the reason `LeaseExpired` on its last attempt. A due retry is sent to its queue. A chain step whose previous step succeeded but which was never sent fails with the reason "hand-off lost", and the run fails. The reaper never changes a task that is `queued`, however long it has waited.
- FR-15. A duplicate message delivery must not execute the task a second time.
- FR-16. A task that fails with attempts left is retried with the same values. The wait uses exponential backoff with jitter, set per task by its first delay, backoff factor, maximum delay and jitter. A task can raise `NonRetryable` to fail at once. Cancellation, and errors that would repeat identically (a value that does not fit a parameter, a missing value, an unusable output, an oversized values dictionary), are never retried.
- FR-17. A run can be cancelled. Queued, planned and retrying tasks are cancelled at once. A running `async` task is stopped at its next `await`. A running plain task is told through `cancel_requested()` and stops when it checks it or returns. Until a running task stops, it shows "cancel requested".
- FR-18. On clean worker shutdown, its running tasks are released for immediate recovery.
- FR-19. Worker processes register themselves and heartbeat; workers that stop are marked dead.
- FR-45. A message sent before any worker has joined a queue is delivered to the first worker that does.
- FR-46. A message for a task that the receiving worker does not know fails that task instance with the reason `UnknownTask`; it is never dropped silently.
- FR-47. A worker's alive ping succeeds only if Valkey answers, so a worker that cannot consume is marked dead.
- FR-48. The reaper only reports, and changes nothing, when a queue has tasks waiting and no live worker, or when Valkey holds fewer messages for a queue than Postgres expects. Each warning is given at most once per ten minutes per queue. The reaper registers itself in the worker table under the queue `reaper` and pings like a worker, so a missing reaper can be seen.
- FR-50. A person can run `taskline requeue --queue <name>` to send again the messages of that queue's queued tasks, for example after Valkey was wiped. `--older-than <seconds>` leaves out tasks queued more recently. It is never run automatically.
- FR-51. The modules that register hooks are listed in their own setting. Workers load them together with the task modules, and the reaper loads only them, so it fires hooks for the failures it causes without loading any task code. With none listed, the reaper fires no hooks and the failures are still recorded.
- FR-37. Workers for a queue can be added and removed at any time without losing or duplicating work. Taskline reports, per queue, the number of tasks waiting, the number running, the age of the oldest waiting task, and the number of live workers. Taskline does not start or stop workers itself.
- FR-49. A person can resume a failed run from its failed task, inside the same run. The failed task is queued again with a fresh set of attempts equal to its original maximum, the later tasks return from `upstream_failed` to `planned`, and the stored message is sent again, so the job continues with the same values and the earlier tasks are not run again. The run's history shows the failure, the resume and what followed.
- FR-39. A task can declare a cancel grace period. If it has not stopped when the grace period is over, taskline records it as cancelled and ends that worker process. The grace period has no default, cannot be shorter than the lease length, and starts when the worker learns of the cancel. A task with a grace period must be on a queue whose workers run one task at a time; taskline refuses to start a worker that breaks this. A task without a grace period is never ended: a plain task that does not check `cancel_requested()` keeps running until it returns.
- FR-40. A queue has a concurrency setting, the number of tasks one worker process runs at the same time. The default is one.
- FR-43. Several applications can each run their own taskline instance on shared Postgres and Valkey servers. An instance has its own database and a Valkey namespace that prefixes every stream and consumer group name, so instances cannot read each other's messages or tables.
- FR-44. Several instances can be read together by a separate UI. Each instance has a name and reports the Taskline version that last migrated it (FR-60), and instances at the same version publish identical views. The instances do not depend on one another.

**Operation**
- FR-20. `taskline migrate` creates and upgrades the `jobs` schema without any host migration tool.
- FR-21. A command line to start a worker for a queue, start the reaper, list runs, show one run with its tasks and events, and cancel a run. The command line calls the command functions of FR-56 and reads through the views of FR-54.
- FR-22. Taskline publishes what an admin UI needs: the read views (FR-54) and the command functions (FR-56). It contains no UI. The UI is the separate package Taskline UI, which shows the run list with filters, run detail with the task timeline, progress, errors and event log, the queues and workers, and the cancel and retry actions.
- FR-23. Taskline's own `.env.example`, container definitions and README allow running it standalone.

**Public contract and packaging**
- FR-52. Taskline's public contract is written in `docs/public_contract.md`: the Python names, the SQL views and their columns, the status values and the refusal codes. Anything not listed is internal and may change in any release. A test fails when a listed name, view column or column type disappears or changes, and when a view has a column the document does not list.
- FR-53. Releases use semantic versioning, and a changelog marks contract changes. The package version is the contract's version. A breaking contract change raises the major version. Before 1.0, a minor release may break the contract and a patch release never does. Taskline can be installed from its own repository, works with psycopg 3 and SQLAlchemy 2, and its tests pass with deprecation warnings treated as errors.

**Read views**
- FR-54. A schema `taskline_api` holds read-only views `run`, `task`, `reference`, `event`, `queue`, `worker` and `instance` (and `schedule` with FR-31). Timestamps are `timestamptz` in UTC. Status values are closed sets listed in the contract. `taskline migrate` creates and updates the views. Taskline publishes no event feed: another application learns that a run has finished by reading `run`.
- FR-55. The views are indexed for the filters status, created time, finish time, job name, producer, correlation id, reference kind and id, and schedule. A list over the `run` view, with its filters, paging and count, stays fast on a million runs; this is shown by a test before the first contract release. A database role named in the settings can read the views and nothing in `jobs`.

**Commands**
- FR-56. `taskline.api` offers the commands `cancel_run`, `retry_run`, `requeue` and `submit_job` (schedule commands with FR-35) as async functions that never print or exit. `submit_job` takes the job's name and values and a producer. The command line calls the same functions.
- FR-57. Every command takes an `actor`, recorded in the event log (and as `triggered_by` for a submission). A request Taskline refuses, such as cancelling a finished run or submitting invalid inputs, returns a result with a refusal code from a closed set and a message. Anything unexpected raises.

**Catalogue**
- FR-58. A job's inputs can be described by a Pydantic model (type, required, default, description) or, as before, by names only. Jobs are registered in a catalogue by importing their definition modules, which need no task code. `list_jobs()` returns each job's name, description, steps and queues, required reference kinds and the JSON Schema of its inputs. A submission with invalid inputs is refused, naming each input.

**Registration, identity and testing**
- FR-59. A host registers producers and reference kinds with idempotent functions that can run during migration. Codes may contain dots. This completes FR-30.
- FR-60. Each database has an instance name. `taskline_api.instance` reports the name, the Taskline version that last migrated the database, and the schema version, and `schema_version()` returns them to a host, so a host can refuse to start against a database older than it needs.
- FR-61. `taskline.testing` writes runs, tasks, references and events in given states into a migrated database through the same store operations the workers use, without a worker or Valkey.

## 6. Non-functional requirements

- NFR-1. Taskline never imports from any host application, Lattice or Taskline UI. An automated test enforces it.
- NFR-2. Closed value sets are lookup tables, not database enums.
- NFR-3. Recovery from a killed worker is demonstrated by an automated test using a real process, real Valkey and real Postgres.
- NFR-4. Defaults: heartbeat every 15 seconds, lease 60 seconds (four heartbeats), both configurable globally and per task. The time to detect a dead worker is at most one lease plus one reaper interval.
- NFR-6. Documentation explains the architecture and every failure scenario in plain language.

## 7. Acceptance for version 1

1. Every FR above has at least one automated test that fails if the feature is removed.
2. The crash-recovery test passes: kill a worker mid-task, the task finishes on another worker and its event log shows the full story.
3. The duplicate-delivery test passes: the same message sent twice executes once.
4. A chain with a failing middle step ends with the correct step and run statuses.
5. The import-boundary test passes.
6. An operator can answer "what is happening with correlation id X" using the command line alone.
7. A schedule started twice by two scheduler processes, and one restarted mid-minute, still produces exactly one run per intended time.
8. Given a host object (for example a document id), an operator can find the run that produced it; given a run, the operator can see who submitted it and which host objects it concerns.
9. A host can read runs through `taskline_api` with a database role that cannot read the `jobs` schema.
10. A run cancelled through `cancel_run` with an actor shows that actor in the run's event log.
11. The contract test fails when a listed name or view column is removed.
12. A job with typed inputs refuses a submission with a wrong value and names the input.
13. The `run` view's list, page count and filters stay fast on a million seeded runs (FR-55). This passes before release 0.2.0.

## 8. Open questions

1. Which retention policy for old `job_run`, `task_instance` and `task_event` rows? Whatever is chosen is its own command (`taskline prune`), not part of the reaper.
2. Resolved: the admin UI is a separate package, Taskline UI, not part of this one (FR-22).