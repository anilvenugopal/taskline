# Changelog

Newest first. A **Contract** entry changes what `docs/public_contract.md` lists. Before 1.0 a minor release
may include breaking contract changes and a patch release never does.

## 0.3.0, 2026-10-20

Typed job inputs and the job listing Taskline UI draws its "Run job" form from.

### Contract
- A job's inputs can be a Pydantic model: each input has a type, a required flag, a default and a description.
  Names-only inputs keep working. `Job` gains `description`, and every job has an `input_schema`, a JSON Schema
  of its inputs. `submit_job` validates the values, names each wrong input in `problems` (`(inputs)` for a rule
  across several), never repeats a refused value, and sends the model's result: coerced to the declared types
  with defaults filled in. A value that is not an input is always refused.
- Added `taskline.api.list_jobs` (with `JobInfo` and `StepInfo`) and `load_catalogue` (with `CatalogueError`),
  and the setting `TASKLINE_CATALOGUE_MODULES`, so a program that never runs a task can list and submit jobs.
- Listed `DuplicateJob` in `taskline.catalogue` and the parameters of `Job` in the contract; both already existed.
- Documented what each `taskline_api.event` value means: when it is written, what `attempt`, `worker_id` and
  `detail` hold, and the event sequences of common cases ("Events" and "Event sequences" in the contract).
  This describes behaviour that already existed, so nothing changes for a reader. `attempt` is the number of
  attempts started when the event was written, so a task not yet started is at 0. A test
  (`tests/test_event_contract.py`) runs each case and fails when the log differs from the document.
- Documented `taskline.Reference`: its parameters, and that it is the value of the `references` parameter of
  `submit_job` and `add_run`. The contract test now checks the parameters. Nothing changes in the code.

## 0.2.0, 2026-10-19

The first contract release: Taskline UI can build against it.

### Contract
- Added `taskline.api`: the async commands `cancel_run`, `retry_run`, `requeue` and `submit_job`, and
  `shutdown`, with the results `CancelResult`, `RetryResult`, `RequeueResult` and `SubmitResult`, `Refusal`,
  `InputProblem` and `REFUSAL_CODES`. Every command takes an `actor`, written into the event log. A request
  Taskline refuses comes back as a result with a refusal code, not an exception.
- Added `taskline.catalogue.register`, so `submit_job` can find a job by name.
- Added `taskline.testing` (`add_run`, `add_worker`, `clear`, `RunHandle`, `STATES`): synchronous test-data
  helpers that write runs, tasks, references, events and workers in known states through the real store
  operations, with no worker or Valkey.
- Added the `taskline_api` views `run`, `task`, `reference`, `event`, `queue`, `worker` and `instance`, and
  `migrate(reader_role=...)`, which grants a database role read access to them and nothing else.
- Added `taskline.setup`: `register_producers`, `register_reference_kinds`, `schema_version`, `Kind`,
  `SchemaVersion` and `NotMigrated`.
- `taskline.migrate.migrate` takes a database URL or a synchronous engine, and an `instance_name`. Its first
  parameter is renamed `url_or_engine` (breaking for callers that pass it by keyword).

### Other
- The commands can be called with a fresh `asyncio.run` each time: a runtime built on an event loop that has
  ended is rebuilt. (Before, the Valkey connection of a finished loop made the second send fail.) One
  long-lived loop is still the recommended way.
- `taskline cancel`, `retry` and `requeue` now call the same functions, print the same messages, and record
  themselves as `cli:<user>` in the event log.
- The contract test now requires each contract module's `__all__` to equal the names the contract lists for
  it, so a name cannot be dropped from the document or added to a module without the other.
- New migration `0003_api_views`: the views, and indexes for the filters a list uses. `taskline runs`,
  `show` and `queues`, and the reaper's queue check, now read through the views. **After upgrading, run
  `taskline migrate` before starting workers or a reaper.**
- New table `jobs.instance` (migration `0002_instance`) holds the instance name and the Taskline version
  that last migrated the database.
- Dependencies now have version ceilings, and `pydantic`, which was imported but not declared, is declared.
- The tests treat deprecation warnings as errors. New tests enforce the public contract and the import
  boundary, and `pytest -m release` checks that the built wheel installs and migrates.
