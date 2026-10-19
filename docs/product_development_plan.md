# Taskline: Product Development Plan

This file tracks progress. Update it at the end of every step. A step is done only when its tests pass and you have reviewed it.

Last updated: 2026-10-06.

## Steps

| # | Step | What it delivers | Requirements | Status |
|---|---|---|---|---|
| 0 | Documents | `product_requirement.md`, `architecture.md`, this plan | all | In review |
| 1 | Package layout and own infrastructure | Taskline `.env.example`, `compose.yaml` (Valkey; optional standalone Postgres), README, test fixtures that start the stack | FR-23 | Done: files written, README commands verified |
| 2 | Database definition and migrate command | `0001_schema.sql` reviewed and agreed table by table; then `taskline migrate`, a schema-version table, and a test that applies the schema to an empty database | FR-20, FR-24 to FR-31, FR-38, NFR-2 | Done: schema reviewed, `taskline migrate` and `taskline.migrate.migrate()` written, 3 tests pass, command verified twice against the standalone database |
| 3 | Store tests | Tests for every store operation against real Postgres: claim, fencing, heartbeat, finish, retry, cancel, chain bookkeeping, reaper queries | FR-5 to FR-8, FR-13, FR-14 | Done: registering a run, queueing, claim, heartbeat, finish, retry, chain bookkeeping, run status, cancel, workers, recovery (58 tests pass). The read-side `Queries` class is built later with the command line and admin UI |
| 4 | Heartbeat lease | The lease: a heartbeat loop on the event loop that renews the lease, reports progress, and handles a cancel request and a lost lease for both `async` and plain tasks; `cancel_requested()`; the cancel grace period that ends a one-task-per-process worker | FR-11 to FR-13, FR-17, FR-39 | Done: `Lease`, `cancel_requested()`, `report_progress()`, `Store.finish_cancelled`, timing settings (68 tests pass). The callbacks that stop an `async` task or end a worker process are supplied in steps 5 and 6 |
| 5 | Lifecycle middleware and hooks | Before and after capture, claim, outcome recording, hook firing, worker registration | FR-5 to FR-7, FR-9, FR-10, FR-15, FR-18, FR-19 | In progress. 5a done: hooks, lineage context, log filter. 5b done: execution wrapper, `TaskCancelled`. 5c done: producer side, `pre_send` and `post_send`, submission details. 5d done: worker side, `pre_execute` and `post_execute`. 5e done: worker registration, alive ping, shutdown (115 tests pass). Step 5 complete |
| 6 | Task decorator, jobs, retries | 6a broker setup on Valkey, a namespace for every stream and group, uuid7 ids, a consumer group that starts at the beginning of the stream, loud `UnknownTask` failures, and an alive ping that needs Valkey; 6b `@task` and task specs (name, queue, attempts, timeout, grace period, required references; checked at import); 6c job definitions and `submit`, in three parts: 6c-1 `Job` and `Step` with named inputs and outputs and their checks at load time; 6c-2 mapped execution, where a wrapper picks a step's inputs from the job's values and adds its outputs; 6c-3 `submit`, required-reference checks and the public names; 6d retries, backoff and chain failure handling | FR-1 to FR-4, FR-8, FR-16, FR-45 to FR-47 | In progress. 6a done: broker on real Valkey with the namespace, start-of-stream consumer group, acknowledged messages removed, recovery lock expiry, `UnknownTask`, uuid7 ids, alive ping that needs Valkey. 6b done: `TaskSpec`, `@task`, the process-wide broker and store, grace period and duplicate checks 6c-1 done: `Job` and `Step` with load-time wiring checks. 6c-2 done: the step runner, with type checking, output checks and the size limit 6c-3 done: `submit`, input and reference checks, public names, runtime start and shutdown 6d done: retry decisions with per-task backoff, `NonRetryable`, chain middleware that fails a job only on a final failure (217 tests pass). Step 6 complete |
| 7 | Reaper and command line | 7a the reaper and `Queries`; 7b the `worker` and `reaper` commands, with module loading, hook modules and the startup checks (a worker refuses to start when its modules define no task for its queue, or a task with a grace period is on a queue with concurrency above one, and it logs the tasks it serves); 7c `runs`, `show` and `queues`; 7d `cancel`, `retry` (resume a failed run from its failed task) and `requeue` | FR-14, FR-21, FR-37, FR-48 to FR-51 | In progress. 7a done: the reaper (expired leases, due retries, lost hand-offs, dead workers, warnings, self-registration), `Queries`, settings. 7b done: the `worker` and `reaper` commands, module loading, startup checks, graceful and forced shutdown, tested with real separate processes. 7c done: `runs` (filters for status, producer, job, correlation id, reference; newest first), `show` (full run id; `--events` adds the event log) and `queues` (load per queue, live workers, whether a reaper is alive), each with `--json`, tested through the real command (270 tests pass). 7d done: `cancel`, `retry` (resumes a failed run from its failed task with a fresh set of attempts; refuses a run that has not failed or whose failed task was never sent) and `requeue` (`--older-than`), tested through the real command (287 tests pass). Step 7 complete |
| 8 | Scheduler | `schedule` and `schedule_fire` tables, Postgres schedule source for taskiq's scheduler, overlap and missed-fire policies, `taskline scheduler` and `schedules` commands | FR-31 to FR-36 | Not started |
| 9 | Failure tests | Kill a real worker mid-task; duplicate delivery; lost message; frozen worker; Postgres outage at claim; two schedulers at once; import-boundary test | NFR-1, NFR-3, FR-33 | Not started |
| 10 | Admin UI | Runs list, run detail, workers page, schedules page, cancel and retry, over one or several instances | FR-22, FR-35, FR-44 | Not started |
| 11 | Hardening and docs | Retention policy, performance check, README walkthrough | NFR-4 to NFR-6 | Not started |

## Rules for every step

1. The step is explained to the owner before work starts, in plain language, saying what is built and why.
2. Files are written as files the owner can open and read, not hidden in commands.
3. Tests are written with the step, not afterwards.
4. This plan and `architecture.md` are updated at the end of the step: what was built, what was learned, what changed.
5. Assumptions listed in `architecture.md` section 14 are proved or corrected by a test, and the document is updated.

## Current repository state (honest)

- The package contains only `sql/0001_schema.sql`, a draft that has not been applied or reviewed. All Python code is written step by step from here.
- The `jobs` schema was dropped from the dev database during the restructure. Nothing depends on it yet.
- Taskline's own containers (`compose.yaml`): Valkey on port 6380, and a standalone Postgres on port 5435 behind the `standalone` profile. Currently stopped.
- The application's containers (`infra/compose.yaml`, repository root): Postgres on port 5434 and SeaweedFS (S3 on port 8333). Currently running.
- The taskiq-admin container image was pulled while investigating and is not used.

## Change log

| Date | Change |
|---|---|
| 2026-10-06 | Plan created. Taskline restructured from `src/strategiq/jobs` into the standalone `packages/taskline`. |
| 2026-10-06 | Scheduler added to version 1 (new step 8, FR-31 to FR-36). Producer mandatory; references to deleted host objects stay as history. |
| 2026-10-06 | Producer and external references added to the design (architecture 10.1, FR-24 to FR-30). Schema SQL not yet updated; that happens in step 2. |
| 2026-10-18 | Moved from `strategiq/packages/taskline` to its own repository, `anilvenugopal/taskline`, unchanged. Added `docs/lattice_integration.md` for review. |
